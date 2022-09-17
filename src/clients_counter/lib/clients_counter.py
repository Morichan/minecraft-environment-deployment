from datetime import datetime, timedelta
import json
from logging import getLogger, INFO
import re

import boto3
from botocore.exceptions import ClientError


logger = getLogger(__name__)
logger.setLevel(INFO)

cw = boto3.client('cloudwatch')


class ClientsCounter:
    def __init__(self, joined_alarm_name, left_alarm_name, metric_namespace, metric_name):
        self.joined_alarm_name = joined_alarm_name
        self.left_alarm_name = left_alarm_name
        self.metric_namespace = metric_namespace
        self.metric_name = metric_name

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

    def create_log_data(self, event_message):
        message = json.loads(event_message)
        previous_metric = int(self.get_previous_metric())
        metric = int(self.extract_datapoint(message['NewStateReason']))

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


class AlarmNotFoundError(RuntimeError):
    pass
