"""Collect information about payments from third-party sources for financial reporting."""

import csv
import datetime
import json
import logging
import requests
import StringIO

import luigi

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.hive import HivePartition
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)


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
        if response.status_code != requests.codes.ok:
            raise Exception("Encountered status {} on request to Cybersource for {}".format(response.status_code, self.run_date))

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
    A task that reads out of a remote Paypal account and writes to a file in TSV format.

    A complication is that this needs to be performed with more than one account.

    Inputs also include the interval over which to request daily dumps.

    Output should be incremental.  That is, this task can be run periodically with
    contiguous time intervals requested, and the output should properly accumulate.
    Output is therefore in the form {output_root}/dt={date}/paypal_{merchant}.tsv
    """
    output_root = luigi.Parameter()
    run_date = luigi.DateParameter(default=datetime.date.today())

    @property
    def _root_url(self):
        """Returns appropriate Paypal API root URL, depending on mode."""
        if self.client_mode == 'sandbox':
            return "https://api.sandbox.paypal.com"
        else:
            return "https://api.paypal.com"

    def get_access_token(self):
        """Requests an access token from Paypal API."""
        url = "{}/v1/oauth2/token".format(self._root_url)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Accept-Language": "en_US",
            "User-Agent": "edx-analytics-pipeline",
        }
        data = "grant_type=client_credentials"
        auth = (self.client_id, self.client_secret)

        response = requests.post(url, auth=auth, headers=headers, data=data)
        if response.status_code != requests.codes.ok:
            raise Exception("Encountered status {} on request to Paypal for access token for {}".format(response.status_code, self.run_date))
        reply = json.loads(response.content)
        access_token = reply.get("access_token")
        if access_token is None:
            raise Exception("Failed to get access token on request to Paypal for {}".format(self.run_date))
        access_token_type = reply.get("token_type")
        return (access_token_type, access_token)
        
    def requires(self):
        pass

    def get_payment_history(self, request_params):
        """
        Queries Paypal payment REST API for payment history.

        Request parameters can be used to specify start and end times, and paging count.

        Returns information about payments as a dict.
        """
        url = "{}/v1/payments/payment".format(self._root_url)
        auth = (self.client_id, self.client_secret)
        headers = {
            "Authorization": ("%s %s" % self.get_access_token()),
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Language": "en_US",
            "User-Agent": "edx-analytics-pipeline",
        }

        response = requests.get(url, headers=headers, params=request_params)
        if response.status_code != requests.codes.ok:
            raise Exception("Encountered status {} on request to Paypal for payments for {}".format(response.status_code, self.run_date))
        payment_history = json.loads(response.content)
        return payment_history

    def run(self):
        self.remove_output_on_overwrite()
        all_payments = []

        end_date = self.run_date + datetime.timedelta(days=1)
        # Maximum number to request at any time is 20.
        request_params = {
            'start_time': "{}T00:00:00Z".format(self.run_date.isoformat()),  # pylint: disable=no-member
            'end_time': "{}T00:00:00Z".format(end_date.isoformat()),
            'count': 10
        }
        print "request_params = {}".format(request_params)

        payment_history = self.get_payment_history(request_params)

        if payment_history.get('payments') is None:
            # TODO: log error
            pass
        else:
            print "Found {} payments".format(payment_history.get('count'))
            if payment_history.get('count') != len(payment_history.get('payments')):
                # TODO: log error
                print "BADDD!!!!"
            for payment in payment_history.get('payments'):
                all_payments.append(payment)

        while payment_history.get('next_id') is not None:
            request_params['start_id'] = payment_history.get('next_id')
            payment_history = self.get_payment_history(request_params)
            print "Found {} payments".format(payment_history.get('count'))
            for payment in payment_history.get('payments'):
                all_payments.append(payment)

        with self.output().open('w') as output_file:
            for payment in all_payments:
                self.output_payment_from_dict(payment, output_file)

    def output_payment_from_dict(self, payment, output_file):
        """
        Extracts relevant payment information and writes to output file as TSV.

        Rows:
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
        row.append(payment.get('update_time'))
        row.append("unknown")
        row.append("unknown")
        row.append(payment.get('id'))
        row.append(payment.get('payer',{}).get('payment_method'))
        row.append(payment.get('transactions')[0].get('amount').get('currency'))
        row.append(payment.get('transactions')[0].get('amount').get('total'))
        row.append(payment.get('intent'))

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
