import base64
from datetime import datetime, timedelta
from enum import Enum, auto
import gzip
import itertools
import json
from logging import getLogger, INFO
import re

import boto3
from botocore.exceptions import ClientError


logger = getLogger(__name__)
logger.setLevel(INFO)

cw = boto3.client('cloudwatch')
dynamodb = boto3.client('dynamodb')


class ClientsCounter:
    """接続ユーザー数をカウントする.

    Attributes
    ----------
    command : CountCommand
        接続ユーザー数カウント時に利用するコマンド。
        設定時にはクラス名を直接セットする。

    Examples
    --------
    >>> # clients_counter.command object is required implementation class of CountCommand
    >>> clients_counter = ClientsCounter(
    ...     command_class=CountCommandFromCloudWatchAlarmToDynamoDB,
    ...     parameter='is_required_by_command_class'
    ... )
    >>> clients_counter.count()

    """
    def __init__(self, command_class=None, **kwargs):
        self._kwargs = kwargs
        self.command = command_class or CountCommand

    @property
    def command(self):
        return self._command

    @command.setter
    def command(self, command_class):
        self._command = command_class(**self._kwargs)

    def count(self):
        if self.command.check_event_source():
            self.command.count()


class CountCommand:
    def __init__(self, **kwargs):
        pass

    def count(self):
        raise NotImplementedError()

    def check_event_source(self):
        raise NotImplementedError()


class CountCommandFromCloudWatchLogsToDynamoDB:
    def __init__(self, **kwargs):
        self.event = kwargs['event']
        self.table = CounterTable(kwargs.get('table_name'), kwargs.get('primary_key_column_name'))

    def count(self):
        results = []

        for state in itertools.chain.from_iterable(self.analyze_log_text()):
            if state == LogState.JOINED:
                result = self.table.update_item(1)
                logger.info(json.dumps({
                    'connected_count': result,
                    'joined_count': 1,
                    'left_count': 0,
                }))
                results.append(result)
            elif state == LogState.LEFT:
                result = self.table.update_item(-1)
                logger.info(json.dumps({
                    'connected_count': result,
                    'joined_count': 0,
                    'left_count': 1,
                }))
                results.append(result)

        return results

    def analyze_log_text(self):
        decoded = self.decode_event_records()
        logger.info(json.dumps(decoded))

        results_list = []
        for record in decoded['Records']:
            results = []
            for log_event in record['kinesis']['data']['logEvents']:
                if 'joined' in log_event['message']:
                    results.append(LogState.JOINED)
                elif 'left' in log_event['message']:
                    results.append(LogState.LEFT)
                elif log_event['message'].startswith('CWL CONTROL MESSAGE'):
                    logger.info(log_event['message'])
                    results.append(LogState.INITIAL_ACTIVATION_OF_KINESIS_DATA_STREAM)
                else:
                    raise UnknownLogState
            results_list.append(results)

        return results_list

    def decode_event_records(self):
        decoded = self.event | {
            'Records': [
                r | {
                    'kinesis': {'data': json.loads(gzip.decompress(base64.b64decode(r['kinesis']['data'].encode())))}
                } for r in self.event['Records']
            ]
        }

        return decoded

    def check_event_source(self):
        try:
            return len(self.event['Records']) >= 1 and self.event['Records'][0]['kinesis']
        except (KeyError, TypeError):
            raise UnknownEventSource()


class CountCommandFromCloudWatchAlarmToDynamoDB(CountCommand):
    def __init__(self, **kwargs):
        self.event = kwargs['event']
        self.joined_alarm_name = kwargs.get('joined_alarm_name')
        self.left_alarm_name = kwargs.get('left_alarm_name')
        self.na = NotificationAnalysis()
        self.table = CounterTable(kwargs.get('table_name'), kwargs.get('primary_key_column_name'))

    def count(self):
        message = json.loads(self.event['Records'][0]['Sns']['Message'])
        metric = int(self.na.extract_datapoint(message['NewStateReason']))

        if message['AlarmName'] == self.joined_alarm_name:
            result_metric = self.table.update_item(metric)
            logger.info(json.dumps({
                'connected_count': result_metric,
                'joined_count': metric,
                'left_count': 0,
            }))
            return result_metric
        elif message['AlarmName'] == self.left_alarm_name:
            result_metric = self.table.update_item(-metric)
            logger.info(json.dumps({
                'connected_count': result_metric,
                'joined_count': 0,
                'left_count': metric,
            }))
            return result_metric
        else:
            raise AlarmNotFoundError()

    def check_event_source(self):
        try:
            return self.event['Records'][0]['Sns']['Message']
        except (KeyError, TypeError):
            raise UnknownEventSource()



class CountCommandFromCloudWatchAlarmToCloudWatchLogs(CountCommand):
    def __init__(self, **kwargs):
        self.event = kwargs['event']
        self.joined_alarm_name = kwargs.get('joined_alarm_name')
        self.left_alarm_name = kwargs.get('left_alarm_name')
        self.metric_namespace = kwargs.get('metric_namespace')
        self.metric_name = kwargs.get('metric_name')
        self.na = NotificationAnalysis()

    def get_metric_data(self):  # pragma: no cover
        now_time = datetime.now()

        return cw.get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'm1',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': self.metric_namespace,
                            'MetricName': self.metric_name,
                        },
                        'Period': 60,
                        'Stat': 'Sum',
                    },
                },
                {
                    'Id': 'e1',
                    'Expression': 'FILL(m1, REPEAT)',
                },
            ],
            StartTime=now_time - timedelta(hours=24),
            EndTime=now_time
        )

    def get_previous_metric(self):
        needed_metrics = cw.list_metrics(Namespace=self.metric_namespace, MetricName=self.metric_name)['Metrics']
        if needed_metrics:
            metric_data = self.get_metric_data()
            return metric_data['MetricDataResults'][0]['Values'][-1]
        else:
            logger.info(f'{self.metric_namespace}/{self.metric_name} metric is not found.')
            return 0.0

    def count(self):
        message = json.loads(self.event['Records'][0]['Sns']['Message'])
        previous_metric = int(self.get_previous_metric())
        metric = int(self.na.extract_datapoint(message['NewStateReason']))

        if message['AlarmName'] == self.joined_alarm_name:
            return {
                'previous_count': previous_metric,
                'connected_count': previous_metric + metric,
                'joined_count': metric,
                'left_count': 0,
            }
        elif message['AlarmName'] == self.left_alarm_name:
            return {
                'previous_count': previous_metric,
                'connected_count': previous_metric - metric,
                'joined_count': 0,
                'left_count': metric,
            }
        else:
            raise AlarmNotFoundError()

    def check_event_source(self):
        try:
            return self.event['Records'][0]['Sns']['Message']
        except (KeyError, TypeError):
            raise UnknownEventSource()


class CounterTable:
    def __init__(self, table_name, primary_key_column_name):
        self.table_name = table_name
        self.primary_key_column_name = primary_key_column_name

    def update_item(self, count):
        result = dynamodb.update_item(
            TableName=self.table_name,
            ReturnValues='UPDATED_NEW',
            Key={self.primary_key_column_name: {'S': 'counter'}},
            UpdateExpression=f'ADD #count :count',
            ExpressionAttributeNames={
                f'#count': 'count',
            },
            ExpressionAttributeValues={
                ':count': {
                    'N': str(count),
                },
            }
        )

        return int(result['Attributes']['count']['N'])


class NotificationAnalysis:
    def extract_datapoint(self, notification_log_from_sns):
        """
        SNSからの状態変化理由の文字列から現在のデータポイントを抽出する.

        Notes
        -----
        数値が上がる場合の理由文は以下のようになる。
        E.g.) 'Threshold Crossed: 1 out of the last 1 datapoints [1.0 (01/08/22 15:05:00)] was greater than or equal to the threshold (1.0) (minimum 1 datapoint for OK -> ALARM transition).'

        数値が下がる場合の理由文は以下のようになる。
        なお、現状では数値が下がる際の通知は届かない設定になっている。
        E.g.) 'Threshold Crossed: 1 out of the last 1 datapoints [0.0 (01/08/22 15:06:00)] was not greater than or equal to the threshold (1.0) (minimum 1 datapoint for ALARM -> OK transition).'
        """
        logger.info(notification_log_from_sns)
        return float(re.findall(r'\[([0-9.-]*)\s*\([^)]*\)\]', notification_log_from_sns)[0])


class LogState(Enum):
    JOINED = auto()
    LEFT = auto()
    INITIAL_ACTIVATION_OF_KINESIS_DATA_STREAM = auto()


class AlarmNotFoundError(RuntimeError):
    pass


class UnknownEventSource(RuntimeError):
    pass


class UnknownLogState(RuntimeError):
    pass
