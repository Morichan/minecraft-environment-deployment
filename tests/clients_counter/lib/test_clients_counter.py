import boto3
from datetime import datetime, timedelta
import json

import pytest
from moto import mock_cloudwatch, mock_dynamodb

with mock_cloudwatch(), mock_dynamodb():
    from lib.clients_counter import (
        ClientsCounterByDynamoDB,
        ClientsCounterByCloudWatch,
        NotificationAnalysis,
    )


def _create_cloudwatch_alarm_message(alarm_name, timestamp, value, namespace='test_namespace', metric_name='test_metric_name'):
    return json.dumps({
        'AlarmName': alarm_name,
        'AlarmDescription': None,
        'AWSAccountId': 'xxxxxxxxxxxx',
        'AlarmConfigurationUpdatedTimestamp': '2022-01-01T00:00:00.000+0000',
        'NewStateValue': 'ALARM',
        'NewStateReason': f'Threshold Crossed: 1 out of the last 1 datapoints [{value} ({timestamp.strftime("%d/%m/%y %H:%M:%S")})] was greater than or equal to the threshold (1.0) (minimum 1 datapoint for OK -> ALARM transition).',
        'StateChangeTime': timestamp.isoformat(),
        'Region': 'Asia Pacific (Tokyo)',
        'AlarmArn': f'arn:aws:cloudwatch:ap-northeast-1:xxxxxxxxxxxx:alarm:{alarm_name}',
        'OldStateValue': 'OK',
        'OKActions': [],
        'AlarmActions': [
            'arn:aws:sns:ap-northeast-1:xxxxxxxxxxxx:SampleSNSTopic',
        ],
        'InsufficientDataActions': [],
        'Trigger': {
            'MetricName': metric_name,
            'Namespace': namespace,
            'StatisticType': 'Statistic',
            'Statistic': 'Sum',
            'Unit': None,
            'Dimensions': [],
            'Period': 60,
            'EvaluationPeriods': 1,
            'ComparisonOperator': 'MoreThanOrEqualToThreshold',
            'Threshold': 1,
            'TreatMissingData': '',
            'EvaluateLowSampleCountPercentile': '',
        },
    })


@mock_dynamodb
class TestClientsCounterByDynamoDB:
    dynamodb = boto3.client('dynamodb')

    def _create_table(self, table_name, primary_key):
        self.dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{'AttributeName': primary_key, 'AttributeType': 'S'}],
            KeySchema=[{'AttributeName': primary_key, 'KeyType': 'HASH'}],
            BillingMode='PAY_PER_REQUEST'
        )

    def test_count_if_user_is_joined(self):
        self._create_table('TestTable', 'id')
        message = _create_cloudwatch_alarm_message('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounterByDynamoDB('TestTable', 'id', 'joined_alarm', 'left_alarm')

        actual = obj.count(message)

        assert actual == 1

    def test_count_if_user_is_left_when_one_is_already_joined(self):
        self._create_table('TestTable', 'id')
        message = _create_cloudwatch_alarm_message('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounterByDynamoDB('TestTable', 'id', 'joined_alarm', 'left_alarm')
        obj.count(message)
        message = _create_cloudwatch_alarm_message('left_alarm', datetime(2022, 8, 1, 15, 35), 1.0)

        actual = obj.count(message)

        assert actual == 0

    def test_not_count_if_cloudwatch_alarm_name_is_not_found(self, mocker):
        self._create_table('TestTable', 'id')
        message = _create_cloudwatch_alarm_message('unknown_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounterByDynamoDB('TestTable', 'id', 'joined_alarm', 'left_alarm')

        with pytest.raises(RuntimeError):
            obj.count(message)

    def test_add_counter(self):
        self._create_table('TestTable', 'id')
        obj = ClientsCounterByDynamoDB('TestTable', 'id', 'joined_alarm', 'left_alarm')

        actual = obj.update_item(1)

        assert actual == 1

    def test_add_counter_multiple(self):
        self._create_table('TestTable', 'id')
        obj = ClientsCounterByDynamoDB('TestTable', 'id', 'joined_alarm', 'left_alarm')

        obj.update_item(1)
        obj.update_item(2)
        actual = obj.update_item(3)

        assert actual == 6

    def test_subtract_counter(self):
        self._create_table('TestTable', 'id')
        obj = ClientsCounterByDynamoDB('TestTable', 'id', 'joined_alarm', 'left_alarm')

        actual = obj.update_item(-1)

        assert actual == -1

    def test_subtract_counter_multiple(self):
        self._create_table('TestTable', 'id')
        obj = ClientsCounterByDynamoDB('TestTable', 'id', 'joined_alarm', 'left_alarm')

        obj.update_item(3)
        obj.update_item(-2)
        actual = obj.update_item(-1)

        assert actual == 0

    @pytest.mark.asyncio
    async def test_count_asyncronously(self):
        import asyncio

        with mock_dynamodb():
            self._create_table('TestTable', 'id')
            obj = ClientsCounterByDynamoDB('TestTable', 'id', 'joined_alarm', 'left_alarm')
            async def async_obj_update_item(count):
                return await asyncio.get_event_loop().run_in_executor(None, obj.update_item, count)

            actuals = await asyncio.gather(
                async_obj_update_item(3),
                async_obj_update_item(-2),
                async_obj_update_item(4)
            )

            assert 5 in actuals


@mock_cloudwatch
class TestClientsCounterByCloudWatch:
    cw = boto3.client('cloudwatch')

    def _set_to_repeat_fill_metric_data(self, namespace, metric_name, start_timestamp, end_timestamp, values):
        """
        指定時刻間における指定時刻の値以外の値を、1分単位で以前の値に設定する (初期値は0).

        Examples
        --------
        >>> from datetime import datetime
        >>> start_timestamp   = datetime(2022, 8, 1, 15)      # 2022-08-01T15:00:00:000
        >>> start_1_timestamp = datetime(2022, 8, 1, 15, 5)   # 2022-08-01T15:05:00:000
        >>> end_1_timestamp   = datetime(2022, 8, 1, 15, 10)  # 2022-08-01T15:10:00:000
        >>> end_timestamp     = datetime(2022, 8, 1, 15, 15)  # 2022-08-01T15:15:00:000

        >>> values = [
        ...     {'timestamp': start_1_timestamp, 'value': 1},
        ...     {'timestamp': end_1_timestamp,   'value': 0},
        ... ]

        >>> t = TestClientsCounterByCloudWatch()
        >>> t._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', start_timestamp, end_timestamp, values)

        >>> data = t._get_metric_data('test_namespace', 'test_metric_name', start_timestamp, end_timestamp)
        >>> print([{'timestamp': str(t), 'value': v} for t, v in zip(data['MetricDataResults'][0]['Timestamps'], data['MetricDataResults'][0]['Values'])])
        [
            {'timestamp': '2022-08-01 15:00:00', 'value': 0.0},  # (start_timestamp)
            {'timestamp': '2022-08-01 15:01:00', 'value': 0.0},
            {'timestamp': '2022-08-01 15:02:00', 'value': 0.0},
            {'timestamp': '2022-08-01 15:03:00', 'value': 0.0},
            {'timestamp': '2022-08-01 15:04:00', 'value': 0.0},
            {'timestamp': '2022-08-01 15:05:00', 'value': 1.0},  # (start_1_timestamp)
            {'timestamp': '2022-08-01 15:06:00', 'value': 1.0},
            {'timestamp': '2022-08-01 15:07:00', 'value': 1.0},
            {'timestamp': '2022-08-01 15:08:00', 'value': 1.0},
            {'timestamp': '2022-08-01 15:09:00', 'value': 1.0},
            {'timestamp': '2022-08-01 15:10:00', 'value': 0.0},  # (end_1_timestamp)
            {'timestamp': '2022-08-01 15:11:00', 'value': 0.0},
            {'timestamp': '2022-08-01 15:12:00', 'value': 0.0},
            {'timestamp': '2022-08-01 15:13:00', 'value': 0.0},
            {'timestamp': '2022-08-01 15:14:00', 'value': 0.0},
            # {'timestamp': '2022-08-01 15:15:00', 'value': 0.0},  # (end_timestamp, ignore data)
        ]

        """
        current_timestamp = start_timestamp
        current_value = 0
        sorted_values = [s for s in sorted(values, key=lambda x: x['timestamp']) if start_timestamp <= s['timestamp'] < end_timestamp]
        metric_data = []

        while current_timestamp < end_timestamp:
            if sorted_values and current_timestamp == sorted_values[0]['timestamp']:
                current_value = sorted_values[0]['value']
                sorted_values.pop(0)
            metric_data.append({
                'MetricName': metric_name,
                'Timestamp': current_timestamp,
                'Value': current_value,
            })
            current_timestamp += timedelta(minutes=1)

        self.cw.put_metric_data(Namespace=namespace, MetricData=metric_data)

    def _get_metric_data(self, namespace, metric_name, start_time, end_time):
        return self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'm1',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': namespace,
                            'MetricName': metric_name,
                        },
                        'Period': 60,
                        'Stat': 'Sum',
                    },
                },
            ],
            StartTime=start_time,
            EndTime=end_time
        )

    def _mock_to_get_metric_data(self, mocker, namespace, metric_name, start_time, end_time):
        """
        lib.clients_counter.ClientsCounterByCloudWatch.get_metric_data メソッドをモック化する

        Notes
        -----
        moto ライブラリでは、 lib.clients_counter.ClientsCounterByCloudWatch.get_metric_data メソッド内で呼出している
        boto3.client('cloudwatch').get_metric_data メソッドにおける Expression に対応しておらず、
        モック化しない状態ではテストが通らない。
        そこで、 self._get_metric_data メソッドで Expression を無効化したメソッドを定義しておく。
        https://github.com/spulec/moto/issues/3323

        また、実行時間から相対的にデータを取得するため、その部分に対してもモック化が必要となる。
        """
        mocker.patch('lib.clients_counter.ClientsCounterByCloudWatch.get_metric_data', side_effect=lambda: self._get_metric_data(namespace, metric_name, start_time, end_time))

    def test_get_all_zero_metric_data_without_one_timestamp(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16), [{'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1}, {'timestamp': datetime(2022, 8, 1, 15, 31), 'value': 0}])
        obj = ClientsCounterByCloudWatch(None, None, 'test_namespace', 'test_metric_name')

        actual = obj.get_metric_data()
        actual_values = actual['MetricDataResults'][0]['Values']

        assert len(actual_values) == 60
        assert actual_values[30] == 1.0
        actual_values.pop(30)
        assert all(v == 0.0 for v in actual_values)

    def test_get_previous_metric_when_metric_has_been_to_count_up(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1},
        ])
        obj = ClientsCounterByCloudWatch(None, None, 'test_namespace', 'test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 1.0

    def test_get_previous_metric_when_metric_has_been_to_count_up_more_than_one(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1},
            {'timestamp': datetime(2022, 8, 1, 15, 31), 'value': 2},
            {'timestamp': datetime(2022, 8, 1, 15, 34), 'value': 1},
            {'timestamp': datetime(2022, 8, 1, 15, 59), 'value': 3},
        ])
        obj = ClientsCounterByCloudWatch(None, None, 'test_namespace', 'test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 3.0

    def test_get_previous_metric_when_metric_has_been_to_count_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1},
            {'timestamp': datetime(2022, 8, 1, 15, 31), 'value': 0},
        ])
        obj = ClientsCounterByCloudWatch(None, None, 'test_namespace', 'test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 0.0

    def test_get_previous_metric_when_metric_has_been_to_count_down_more_than_one(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 5},
            {'timestamp': datetime(2022, 8, 1, 15, 39), 'value': 1},
            {'timestamp': datetime(2022, 8, 1, 15, 40), 'value': 4},
            {'timestamp': datetime(2022, 8, 1, 15, 41), 'value': 2},
        ])
        obj = ClientsCounterByCloudWatch(None, None, 'test_namespace', 'test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 2.0

    def test_get_previous_metric_when_metric_has_not_been_to_count_up_or_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16), [])
        obj = ClientsCounterByCloudWatch(None, None, 'test_namespace', 'test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 0.0

    def test_get_previous_metric_when_metric_is_not_created(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        obj = ClientsCounterByCloudWatch(None, None, 'test_namespace', 'test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 0.0

    def test_create_log_data_for_cloudwatch_logs_metric_filter_if_function_is_called_by_joined_alarm_when_metric_has_not_been_to_count_up_or_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 31), datetime(2022, 8, 1, 15, 31))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [])
        message = _create_cloudwatch_alarm_message('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounterByCloudWatch('joined_alarm', 'left_alarm', 'test_namespace', 'test_metric_name')

        actual = obj.create_log_data(message)

        assert actual == {'previous_count': 0, 'connected_count': 1, 'joined_count': 1, 'left_count': 0}

    def test_create_log_data_for_cloudwatch_logs_metric_filter_if_function_is_called_by_joined_alarm_when_metric_has_been_to_count_up(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 31), datetime(2022, 8, 1, 15, 31))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1},
        ])
        message = _create_cloudwatch_alarm_message('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounterByCloudWatch('joined_alarm', 'left_alarm', 'test_namespace', 'test_metric_name')

        actual = obj.create_log_data(message)

        assert actual == {'previous_count': 1, 'connected_count': 2, 'joined_count': 1, 'left_count': 0}

    def test_create_log_data_for_cloudwatch_logs_metric_filter_if_function_is_called_by_joined_alarm_when_metric_has_been_to_count_up_more_than_one(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 31), datetime(2022, 8, 1, 15, 31))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 20), 'value': 4},
            {'timestamp': datetime(2022, 8, 1, 15, 25), 'value': 1},
            {'timestamp': datetime(2022, 8, 1, 15, 26), 'value': 3},
        ])
        message = _create_cloudwatch_alarm_message('joined_alarm', datetime(2022, 8, 1, 15, 31), 2.0)
        obj = ClientsCounterByCloudWatch('joined_alarm', 'left_alarm', 'test_namespace', 'test_metric_name')

        actual = obj.create_log_data(message)

        assert actual == {'previous_count': 3, 'connected_count': 5, 'joined_count': 2, 'left_count': 0}

    def test_create_log_data_for_cloudwatch_logs_metric_filter_if_function_is_called_by_left_alarm_when_metric_has_not_been_to_count_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 59), datetime(2022, 8, 1, 15, 59))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1},
        ])
        message = _create_cloudwatch_alarm_message('left_alarm', datetime(2022, 8, 1, 15, 59), 1.0)
        obj = ClientsCounterByCloudWatch('joined_alarm', 'left_alarm', 'test_namespace', 'test_metric_name')

        actual = obj.create_log_data(message)

        assert actual == {'previous_count': 1, 'connected_count': 0, 'joined_count': 0, 'left_count': 1}

    def test_create_log_data_for_cloudwatch_logs_metric_filter_if_function_is_called_by_left_alarm_when_metric_has_been_to_count_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 59), datetime(2022, 8, 1, 15, 59))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 3},
            {'timestamp': datetime(2022, 8, 1, 15, 31), 'value': 4},
        ])
        message = _create_cloudwatch_alarm_message('left_alarm', datetime(2022, 8, 1, 15, 59), 2.0)
        obj = ClientsCounterByCloudWatch('joined_alarm', 'left_alarm', 'test_namespace', 'test_metric_name')

        actual = obj.create_log_data(message)

        assert actual == {'previous_count': 4, 'connected_count': 2, 'joined_count': 0, 'left_count': 2}

    def test_create_log_data_for_cloudwatch_logs_metric_filter_if_function_is_called_by_left_alarm_when_metric_has_been_to_count_down_more_than_one(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 59), datetime(2022, 8, 1, 15, 59))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 20), 'value': 4},
            {'timestamp': datetime(2022, 8, 1, 15, 25), 'value': 1},
            {'timestamp': datetime(2022, 8, 1, 15, 26), 'value': 3},
        ])
        message = _create_cloudwatch_alarm_message('left_alarm', datetime(2022, 8, 1, 15, 59), 3.0)
        obj = ClientsCounterByCloudWatch('joined_alarm', 'left_alarm', 'test_namespace', 'test_metric_name')

        actual = obj.create_log_data(message)

        assert actual == {'previous_count': 3, 'connected_count': 0, 'joined_count': 0, 'left_count': 3}

    def test_not_create_log_data_if_cloudwatch_alarm_name_is_not_found(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 31), datetime(2022, 8, 1, 15, 31))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [])
        message = _create_cloudwatch_alarm_message('unknown_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounterByCloudWatch('not_found_alarm', 'not_exist_alarm', 'test_namespace', 'test_metric_name')

        with pytest.raises(RuntimeError):
            obj.create_log_data(message)


class TestNotificationAnalysis:
    def test_extract_datapoint_when_current_metric_datapoint_is_one(self):
        notification_log_from_sns = 'Threshold Crossed: 1 out of the last 1 datapoints [1.0 (13/08/22 16:10:00)] was greater than or equal to the threshold (1.0) (minimum 1 datapoint for OK -> ALARM transition).'
        obj = NotificationAnalysis()

        actual = obj.extract_datapoint(notification_log_from_sns)

        assert actual == 1.0

    def test_extract_datapoint_when_current_metric_datapoint_is_zero(self):
        notification_log_from_sns = 'Threshold Crossed: 1 out of the last 1 datapoints [0.0 (01/08/22 15:10:00)] was not greater than or equal to the threshold (1.0) (minimum 1 datapoint for ALARM -> OK transition).'
        obj = NotificationAnalysis()

        actual = obj.extract_datapoint(notification_log_from_sns)

        assert actual == 0.0
