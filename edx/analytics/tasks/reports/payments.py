"""Collect information about payments from third-party sources for financial reporting."""

import csv
import datetime
import logging
import requests
import StringIO

import luigi

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.hive import HivePartition
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)

try:
    import paypalrestsdk
    paypalrestsdk_available = True
except ImportError:
    log.warn('Unable to import paypalrestsdk client libraries')
    paypalrestsdk_available = False


class PullFromCybersourceTaskMixin(OverwriteOutputMixin):

    host = luigi.Parameter(
        default_from_config={'section': 'cybersource', 'name': 'host'}
    )
    merchant_id = luigi.Parameter(
        default_from_config={'section': 'cybersource', 'name': 'merchant_id'}
    )
    username = luigi.Parameter(
        default_from_config={'section': 'cybersource', 'name': 'username'}
    )
    # Making this 'insignificant' means it won't be echoed in log files.
    password = luigi.Parameter(
        default_from_config={'section': 'cybersource', 'name': 'password'},
        significant=False,
    )


class SinglePullFromCybersourceTask(PullFromCybersourceTaskMixin, luigi.Task):
    """
    A task that reads out of a remote Cybersource account and writes to a file in TSV format.

    A complication is that this needs to be performed with more than one account.

    Inputs also include the interval over which to request daily dumps.

    Output should be incremental.  That is, this task can be run periodically with
    contiguous time intervals requested, and the output should properly accumulate.
    Output is therefore in the form {output_root}/dt={date}/cybersource_{merchant}.tsv
    """
    output_root = luigi.Parameter()
    run_date = luigi.DateParameter(default=datetime.date.today())

    REPORT_NAME = 'PaymentBatchDetailReport'
    REPORT_FORMAT = 'csv'

    def requires(self):
        pass

    def run(self):
        self.remove_output_on_overwrite()
        auth = (self.username, self.password)
        response = requests.get(self.query_url, auth=auth)
        data = StringIO.StringIO(response.content)
        _download_header = data.readline()
        reader = csv.reader(data, delimiter=',')
        with self.output().open('w') as output_file:
            for row in reader:
                output_file.write('\t'.join(row))
                output_file.write('\n')

    def output(self):
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "cybersource_{}.tsv".format(self.merchant_id)
        url_with_filename = url_path_join(self.output_root, partition_path_spec, filename)
        return get_target_from_url(url_with_filename)

    @property
    def query_url(self):
        slashified_date = self.run_date.strftime('%Y/%m/%d')  # pylint: disable=no-member
        url = 'https://{host}/DownloadReport/{date}/{merchant_id}/{report_name}.{report_format}'.format(
            host=self.host,
            date=slashified_date,
            merchant_id=self.merchant_id,
            report_name=self.REPORT_NAME,
            report_format=self.REPORT_FORMAT
        )
        return url


class IntervalPullFromCybersourceTask(PullFromCybersourceTaskMixin, luigi.Task):
    """Determines a set of dates to pull, and requires them."""

    interval = luigi.DateIntervalParameter()
    output_root = luigi.Parameter()

    required_tasks = None

    def _get_required_tasks(self):
        """Internal method to actually calculate required tasks once."""
        start_date = self.interval.date_a  # pylint: disable=no-member
        end_date = self.interval.date_b  # pylint: disable=no-member
        args = {
            'host': self.host,
            'merchant_id': self.merchant_id,
            'username': self.username,
            'password': self.password,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
        }

        current_date = start_date
        while current_date < end_date:
            args['run_date'] = current_date
            task = SinglePullFromCybersourceTask(**args)
            if not task.complete():
                yield task
            current_date += datetime.timedelta(days=1)

    def requires(self):
        if not self.required_tasks:
            self.required_tasks = [task for task in self._get_required_tasks()]

        return self.required_tasks

    def output(self):
        return [task.output() for task in self.requires()]


class PullFromPaypalTaskMixin(OverwriteOutputMixin):

    client_mode = luigi.Parameter(
        default_from_config={'section': 'paypal', 'name': 'client_mode'}
    )
    client_id = luigi.Parameter(
        default_from_config={'section': 'paypal', 'name': 'client_id'}
    )
    # Making this 'insignificant' means it won't be echoed in log files.
    client_secret = luigi.Parameter(
        default_from_config={'section': 'paypal', 'name': 'client_secret'},
        significant=False,
    )


class SinglePullFromPaypalTask(PullFromPaypalTaskMixin, luigi.Task):
    """
    A task that reads out of a remote Cybersource account and writes to a file in TSV format.

    A complication is that this needs to be performed with more than one account.

    Inputs also include the interval over which to request daily dumps.

    Output should be incremental.  That is, this task can be run periodically with
    contiguous time intervals requested, and the output should properly accumulate.
    Output is therefore in the form {output_root}/dt={date}/cybersource_{merchant}.tsv
    """
    output_root = luigi.Parameter()
    run_date = luigi.DateParameter(default=datetime.date.today())

    def initialize(self):
        if not paypalrestsdk_available:
            raise ImportError('paypalrestsdk client library not available')

        paypalrestsdk.configure({
            'mode': self.client_mode,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        })

    def requires(self):
        pass

    def run(self):
        self.initialize()
        self.remove_output_on_overwrite()
        all_payments = []

        end_date = self.run_date + datetime.timedelta(days=1)
        # Maximum number to request at any time is 20.
        request_args = {
            'start_time': "{}T00:00:00Z".format(self.run_date.isoformat()),  # pylint: disable=no-member
            'end_time': "{}T00:00:00Z".format(end_date.isoformat()),
            'count': 10
        }
        print "request_args = {}".format(request_args)

        payment_history = paypalrestsdk.Payment.all(request_args)
        if payment_history.payments is None:
            # TODO: log error
            pass
        else:
            print "Found {} payments".format(payment_history.count)
            if payment_history.count != len(payment_history.payments):
                # TODO: log error
                print "BADDD!!!!"
            for payment in payment_history.payments:
                all_payments.append(payment)

        while payment_history.next_id is not None:
            request_args['start_id'] = payment_history.next_id
            payment_history = paypalrestsdk.Payment.all(request_args)
            print "Found {} payments".format(payment_history.count)
            for payment in payment_history.payments:
                all_payments.append(payment)

        with self.output().open('w') as output_file:
            for payment in all_payments:
                self.output_payment(payment, output_file)

    def output_payment(self, payment, output_file):
        """
  batch_id              ?
  merchant_id           ?
  batch_date            payment.update_time
  request_id            ?
  merchant_ref_number   ?
  trans_ref_no          payment.id
  payment_method        payment.payer.payment_method
  currency              payment.transactions[0].amount.currency
  amount                payment.transactions[0].amount.total
  transaction_type      payment.intent
        """
        row = []
        row.append("unknown")
        row.append("unknown")
        row.append(payment.update_time)
        row.append("unknown")
        row.append("unknown")
        row.append(payment.id)
        row.append(payment.payer.payment_method)
        row.append(payment.transactions[0].amount.currency)
        row.append(payment.transactions[0].amount.total)
        row.append(payment.intent)

        output_file.write('\t'.join(row))
        output_file.write('\n')

    def output(self):
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "paypal_{}.tsv".format(self.client_mode)
        url_with_filename = url_path_join(self.output_root, partition_path_spec, filename)
        return get_target_from_url(url_with_filename)


class IntervalPullFromPaypalTask(PullFromPaypalTaskMixin, luigi.Task):
    """Determines a set of dates to pull, and requires them."""

    interval = luigi.DateIntervalParameter()
    output_root = luigi.Parameter()

    required_tasks = None

    def _get_required_tasks(self):
        """Internal method to actually calculate required tasks once."""
        start_date = self.interval.date_a  # pylint: disable=no-member
        end_date = self.interval.date_b  # pylint: disable=no-member
        args = {
            'client_mode': self.client_mode,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
        }

        current_date = start_date
        while current_date < end_date:
            args['run_date'] = current_date
            task = SinglePullFromPaypalTask(**args)
            if not task.complete():
                yield task
            current_date += datetime.timedelta(days=1)

    def requires(self):
        if not self.required_tasks:
            self.required_tasks = [task for task in self._get_required_tasks()]

        return self.required_tasks

    def output(self):
        return [task.output() for task in self.requires()]
