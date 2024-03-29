import base64
import boto3
from datetime import datetime, timedelta
import gzip
import json

import pytest
from moto import mock_cloudwatch, mock_dynamodb

with mock_cloudwatch(), mock_dynamodb():
    from lib.clients_counter import (
        ClientsCounter,
        CountCommand,
        CountCommandFromCloudWatchLogsToDynamoDB,
        CountCommandFromCloudWatchAlarmToDynamoDB,
        CountCommandFromCloudWatchAlarmToCloudWatchLogs,
        CounterTable,
        NotificationAnalysis,
        LogState,
        UnknownEventSource,
        UnknownLogState,
    )


def _create_cloudwatch_log_event(timestamp, data_states, user_name, is_first=False):
    if is_first:
        data = [{
            'messageType': 'CONTROL_MESSAGE',
            'owner': 'CloudwatchLogs',
            'logGroup': '',
            'logStream': '',
            'subscriptionFilters': [],
            'logEvents': [{
                'id': '',
                'timestamp': round(timestamp.timestamp(), 3) * 1000,
                'message': 'CWL CONTROL MESSAGE: Checking health of destination Kinesis stream.'
            }],
        }]
    else:
        data = [{
            'messageType': 'DATA_MESSAGE',
            'owner': '000000000000',
            'logGroup': '/ecs/logs/minecraft-environment-deployment/minecraft-server',
            'logStream': 'minecraft/minecraft-server/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            'subscriptionFilters': [
                'minecraft-environment-deployment-MinecraftECSConnectedCountSubscriptionFilter-xxxxxxxxxxxx',
            ],
            'logEvents': [{
                'id': '33333333333333333333333333333333333333333333333333333333',
                'timestamp': round(timestamp.timestamp(), 3) * 1000,
                'message': f'[{timestamp.strftime("%H:%M:%S")}] [Server thread/INFO]: {user_name} {log_event_state} the game',
            } for log_event_state in data_state],
        } for data_state in data_states]

    return {
        'Records': [{
            'kinesis': {
                'kinesisSchemaVersion': '1.0',
                'partitionKey': '3e21f5e8240cbb048271af4fdb892a1c',
                'sequenceNumber': '49634156167133626984422846403173197967042654711234691106',
                'data': base64.b64encode(gzip.compress(json.dumps(d).encode())).decode(),
                'approximateArrivalTimestamp': round(timestamp.timestamp(), 3),
            },
            'eventSource': 'aws:kinesis',
            'eventVersion': '1.0',
            'eventID': 'shardId-000000000002:49634156167133626984422846403173197967042654711234691106',
            'eventName': 'aws:kinesis:record',
            'invokeIdentityArn': 'arn:aws:iam::485332844223:role/minecraft-environment-dep-MinecraftECSConnectedSom-11P9E4LPBFG0O',
            'awsRegion': 'ap-northeast-1',
            'eventSourceARN': 'arn:aws:kinesis:ap-northeast-1:485332844223:stream/minecraft-environment-deployment-MinecraftECSLogKinesisDataStream-Ckgt1t0sQ1pd',
        } for d in data],
    }


def _create_cloudwatch_alarm_event(alarm_name, timestamp, value, namespace='test_namespace', metric_name='test_metric_name'):
    return {
        'Records': [{
            'Sns': {
                'Message': json.dumps({
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
                }),
            },
        }],
    }


def _create_table(table_name, primary_key):
    with mock_dynamodb():
        dynamodb = boto3.client('dynamodb')

        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{'AttributeName': primary_key, 'AttributeType': 'S'}],
            KeySchema=[{'AttributeName': primary_key, 'KeyType': 'HASH'}],
            BillingMode='PAY_PER_REQUEST'
        )


@mock_cloudwatch
@mock_dynamodb
class TestClientsCounter:
    def test_count_to_use_send_message_from_cloudwatch_logs_to_dynamodb(self, mocker):
        m_check = mocker.spy(CountCommandFromCloudWatchLogsToDynamoDB, 'check_event_source')
        m_count = mocker.spy(CountCommandFromCloudWatchLogsToDynamoDB, 'count')
        _create_table('TestTable', 'id')
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), [['joined']], 'user')
        obj = ClientsCounter(event=event, table_name='TestTable', primary_key_column_name='id')
        obj.command = CountCommandFromCloudWatchLogsToDynamoDB

        obj.count()

        m_check.assert_called_once()
        m_count.assert_called_once()

    def test_not_count_to_use_send_message_from_cloudwatch_logs_to_dynamodb_if_checking_is_failed(self, mocker):
        m_check = mocker.patch('lib.clients_counter.CountCommandFromCloudWatchLogsToDynamoDB.check_event_source', return_value=False)
        m_count = mocker.spy(CountCommandFromCloudWatchLogsToDynamoDB, 'count')
        _create_table('TestTable', 'id')
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), [['joined']], 'user')
        obj = ClientsCounter(event=event, table_name='TestTable', primary_key_column_name='id')
        obj.command = CountCommandFromCloudWatchLogsToDynamoDB

        obj.count()

        m_check.assert_called_once()
        m_count.assert_not_called()

    def test_count_to_use_send_message_from_cloudwatch_alarm_to_dynamodb(self, mocker):
        m_check = mocker.spy(CountCommandFromCloudWatchAlarmToDynamoDB, 'check_event_source')
        m_count = mocker.spy(CountCommandFromCloudWatchAlarmToDynamoDB, 'count')
        _create_table('TestTable', 'id')
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounter(event=event, table_name='TestTable', primary_key_column_name='id', joined_alarm_name='joined_alarm', left_alarm_name='left_alarm')
        obj.command = CountCommandFromCloudWatchAlarmToDynamoDB

        obj.count()

        m_check.assert_called_once()
        m_count.assert_called_once()

    def test_not_count_to_use_send_message_from_cloudwatch_alarm_to_dynamodb_if_checking_is_failed(self, mocker):
        m_check = mocker.patch('lib.clients_counter.CountCommandFromCloudWatchAlarmToDynamoDB.check_event_source', return_value=False)
        m_count = mocker.spy(CountCommandFromCloudWatchAlarmToDynamoDB, 'count')
        _create_table('TestTable', 'id')
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounter(event=event, table_name='TestTable', primary_key_column_name='id', joined_alarm_name='joined_alarm', left_alarm_name='left_alarm')
        obj.command = CountCommandFromCloudWatchAlarmToDynamoDB

        obj.count()

        m_check.assert_called_once()
        m_count.assert_not_called()

    def test_count_to_use_send_message_from_cloudwatch_alarm_to_cloudwatch_logs(self, mocker):
        m_check = mocker.spy(CountCommandFromCloudWatchAlarmToCloudWatchLogs, 'check_event_source')
        m_count = mocker.spy(CountCommandFromCloudWatchAlarmToCloudWatchLogs, 'count')
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounter(event=event, joined_alarm_name='joined_alarm', left_alarm_name='left_alarm', metric_namespace='test_namespace', metric_name='test_metric_name')
        obj.command = CountCommandFromCloudWatchAlarmToCloudWatchLogs

        obj.count()

        m_check.assert_called_once()
        m_count.assert_called_once()

    def test_not_count_to_use_send_message_from_cloudwatch_alarm_to_cloudwatch_logs_if_checking_is_failed(self, mocker):
        m_check = mocker.patch('lib.clients_counter.CountCommandFromCloudWatchAlarmToCloudWatchLogs.check_event_source', return_value=False)
        m_count = mocker.spy(CountCommandFromCloudWatchAlarmToCloudWatchLogs, 'count')
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = ClientsCounter(event=event, joined_alarm_name='joined_alarm', left_alarm_name='left_alarm', metric_namespace='test_namespace', metric_name='test_metric_name')
        obj.command = CountCommandFromCloudWatchAlarmToCloudWatchLogs

        obj.count()

        m_check.assert_called_once()
        m_count.assert_not_called()

    def test_raise_error_if_command_is_set_count_command(self, mocker):
        obj = ClientsCounter(command_class=CountCommand)

        with pytest.raises(NotImplementedError):
            obj.count()


class TestCountCommand:
    def test_raise_error_to_some_methods(self):
        obj = CountCommand()

        with pytest.raises(NotImplementedError):
            obj.count()

        with pytest.raises(NotImplementedError):
            obj.check_event_source()


@mock_dynamodb
class TestCountCommandFromCloudWatchLogsToDynamoDB:
    def test_count_if_user_is_joined(self):
        _create_table('TestTable', 'id')
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), [['joined']], 'user')
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        actual = obj.count()

        assert actual == [1]

    def test_count_if_users_are_joined_and_left(self):
        _create_table('TestTable', 'id')
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), [['joined', 'joined', 'left'], ['left', 'joined'], ['joined'], ['left', 'left']], 'user')
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        actual = obj.count()

        assert actual == [1, 2, 1, 0, 1, 2, 1, 0]

    def test_check_event_source_is_kinesis_data_stream(self):
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), [['joined']], 'user')
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        actual = obj.check_event_source()

        assert actual

    def test_raise_error_if_event_source_is_unknown(self):
        event = {'Records': [{'UnknownEventSource': {}}]}
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        with pytest.raises(UnknownEventSource):
            obj.check_event_source()

    def test_decode_data_in_event_records(self):
        expected = {
            'Records': [
                {'kinesis': {'data': {'logEvents': [{'id': '1'}]}}},
                {'kinesis': {'data': {'logEvents': [{'id': '2'}]}}},
            ],
        }
        event = {
            'Records': [
                {'kinesis': {'data': 'H4sIAPDvRmMC/6tWyslPdy1LzSspVrJSiK5WykwB0kqGSrWxtQC0KFpkHAAAAA=='}},
                {'kinesis': {'data': 'H4sIAPjvRmMC/6tWyslPdy1LzSspVrJSiK5WykwB0kpGSrWxtQBkUvojHAAAAA=='}},
                # {'kinesis': {'data': base64.b64encode(gzip.compress(json.dumps({'logEvents': [{'id': '1'}]}).encode())).decode()}},
                # {'kinesis': {'data': base64.b64encode(gzip.compress(json.dumps({'logEvents': [{'id': '2'}]}).encode())).decode()}},
            ],
        }
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        actual = obj.decode_event_records()

        assert actual == expected

    def test_analyze_log_text_to_verify_that_user_is_joined(self):
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), [['joined']], 'user')
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        actual = obj.analyze_log_text()

        assert actual == [[LogState.JOINED]]

    def test_analyze_log_text_to_verify_that_user_is_left(self):
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), [['left']], 'user')
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        actual = obj.analyze_log_text()

        assert actual == [[LogState.LEFT]]

    def test_raise_error_if_log_text_is_unknown(self):
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), [['unknown']], 'user')
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        with pytest.raises(UnknownLogState):
            obj.analyze_log_text()

    def test_analyze_log_text_if_log_event_has_multi_log_events_and_multi_data(self):
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), [['joined', 'joined', 'left'], ['left', 'joined'], ['joined'], ['left', 'left']], 'user')
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        actual = obj.analyze_log_text()

        assert actual == [[LogState.JOINED, LogState.JOINED, LogState.LEFT], [LogState.LEFT, LogState.JOINED], [LogState.JOINED], [LogState.LEFT, LogState.LEFT]]

    def test_analyze_log_text_to_verify_that_kinesis_resource_is_created(self):
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), None, None, is_first=True)
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        actual = obj.analyze_log_text()

        assert actual == [[LogState.INITIAL_ACTIVATION_OF_KINESIS_DATA_STREAM]]

    def test_result_is_empty_if_kinesis_resource_is_created(self):
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), None, None, is_first=True)
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id')

        actual = obj.count()

        assert actual == []

    def test_ckeck_that_event_source_is_kinesis_data_stream(self):
        event = _create_cloudwatch_log_event(datetime(2022, 8, 1, 15, 31), None, None, is_first=True)
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=event)

        actual = obj.check_event_source()

        assert actual

    def test_raise_error_if_ckeck_that_event_source_is_null(self):
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event=None)

        with pytest.raises(UnknownEventSource):
            obj.check_event_source()

    def test_raise_error_if_ckeck_that_event_source_is_not_kinesis_data_stream(self):
        obj = CountCommandFromCloudWatchLogsToDynamoDB(event={})

        with pytest.raises(UnknownEventSource):
            obj.check_event_source()


@mock_dynamodb
class TestCountCommandFromCloudWatchAlarmToDynamoDB:
    def test_count_if_user_is_joined(self):
        _create_table('TestTable', 'id')
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = CountCommandFromCloudWatchAlarmToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id', joined_alarm_name='joined_alarm', left_alarm_name='left_alarm')

        actual = obj.count()

        assert actual == 1

    def test_count_if_user_is_left_when_one_is_already_joined(self):
        _create_table('TestTable', 'id')
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = CountCommandFromCloudWatchAlarmToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id', joined_alarm_name='joined_alarm', left_alarm_name='left_alarm')
        obj.count()
        event = _create_cloudwatch_alarm_event('left_alarm', datetime(2022, 8, 1, 15, 35), 1.0)
        obj.event = event

        actual = obj.count()

        assert actual == 0

    def test_not_count_if_cloudwatch_alarm_name_is_not_found(self, mocker):
        _create_table('TestTable', 'id')
        event = _create_cloudwatch_alarm_event('unknown_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = CountCommandFromCloudWatchAlarmToDynamoDB(event=event, table_name='TestTable', primary_key_column_name='id', joined_alarm_name='joined_alarm', left_alarm_name='left_alarm')

        with pytest.raises(RuntimeError):
            obj.count()

    def test_ckeck_that_event_source_is_sns_message(self):
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = CountCommandFromCloudWatchAlarmToDynamoDB(event=event)

        actual = obj.check_event_source()

        assert actual

    def test_raise_error_if_ckeck_that_event_source_is_null(self):
        obj = CountCommandFromCloudWatchAlarmToDynamoDB(event=None)

        with pytest.raises(UnknownEventSource):
            obj.check_event_source()

    def test_raise_error_if_ckeck_that_event_source_is_not_sns_message(self):
        obj = CountCommandFromCloudWatchAlarmToDynamoDB(event={})

        with pytest.raises(UnknownEventSource):
            obj.check_event_source()


@mock_cloudwatch
class TestCountCommandFromCloudWatchAlarmToCloudWatchLogs:
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

        >>> t = TestCountCommandFromCloudWatchAlarmToCloudWatchLogs()
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
        lib.clients_counter.CountCommandFromCloudWatchAlarmToCloudWatchLogs.get_metric_data メソッドをモック化する

        Notes
        -----
        moto ライブラリでは、 lib.clients_counter.CountCommandFromCloudWatchAlarmToCloudWatchLogs.get_metric_data メソッド内で呼出している
        boto3.client('cloudwatch').get_metric_data メソッドにおける Expression に対応しておらず、
        モック化しない状態ではテストが通らない。
        そこで、 self._get_metric_data メソッドで Expression を無効化したメソッドを定義しておく。
        https://github.com/spulec/moto/issues/3323

        また、実行時間から相対的にデータを取得するため、その部分に対してもモック化が必要となる。
        """
        mocker.patch('lib.clients_counter.CountCommandFromCloudWatchAlarmToCloudWatchLogs.get_metric_data', side_effect=lambda: self._get_metric_data(namespace, metric_name, start_time, end_time))

    def test_get_all_zero_metric_data_without_one_timestamp(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16), [{'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1}, {'timestamp': datetime(2022, 8, 1, 15, 31), 'value': 0}])
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=None, metric_namespace='test_namespace', metric_name='test_metric_name')

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
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=None, metric_namespace='test_namespace', metric_name='test_metric_name')

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
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=None, metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 3.0

    def test_get_previous_metric_when_metric_has_been_to_count_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1},
            {'timestamp': datetime(2022, 8, 1, 15, 31), 'value': 0},
        ])
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=None, metric_namespace='test_namespace', metric_name='test_metric_name')

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
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=None, metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 2.0

    def test_get_previous_metric_when_metric_has_not_been_to_count_up_or_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16), [])
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=None, metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 0.0

    def test_get_previous_metric_when_metric_is_not_created(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 15), datetime(2022, 8, 1, 16))
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=None, metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.get_previous_metric()

        assert actual == 0.0

    def test_count_for_cloudwatch_logs_metric_filter_if_function_is_called_by_joined_alarm_when_metric_has_not_been_to_count_up_or_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 31), datetime(2022, 8, 1, 15, 31))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [])
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=event, joined_alarm_name='joined_alarm', left_alarm_name='left_alarm', metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.count()

        assert actual == {'previous_count': 0, 'connected_count': 1, 'joined_count': 1, 'left_count': 0}

    def test_count_for_cloudwatch_logs_metric_filter_if_function_is_called_by_joined_alarm_when_metric_has_been_to_count_up(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 31), datetime(2022, 8, 1, 15, 31))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1},
        ])
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=event, joined_alarm_name='joined_alarm', left_alarm_name='left_alarm', metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.count()

        assert actual == {'previous_count': 1, 'connected_count': 2, 'joined_count': 1, 'left_count': 0}

    def test_count_for_cloudwatch_logs_metric_filter_if_function_is_called_by_joined_alarm_when_metric_has_been_to_count_up_more_than_one(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 31), datetime(2022, 8, 1, 15, 31))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 20), 'value': 4},
            {'timestamp': datetime(2022, 8, 1, 15, 25), 'value': 1},
            {'timestamp': datetime(2022, 8, 1, 15, 26), 'value': 3},
        ])
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 2.0)
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=event, joined_alarm_name='joined_alarm', left_alarm_name='left_alarm', metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.count()

        assert actual == {'previous_count': 3, 'connected_count': 5, 'joined_count': 2, 'left_count': 0}

    def test_count_for_cloudwatch_logs_metric_filter_if_function_is_called_by_left_alarm_when_metric_has_not_been_to_count_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 59), datetime(2022, 8, 1, 15, 59))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 1},
        ])
        event = _create_cloudwatch_alarm_event('left_alarm', datetime(2022, 8, 1, 15, 59), 1.0)
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=event, joined_alarm_name='joined_alarm', left_alarm_name='left_alarm', metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.count()

        assert actual == {'previous_count': 1, 'connected_count': 0, 'joined_count': 0, 'left_count': 1}

    def test_count_for_cloudwatch_logs_metric_filter_if_function_is_called_by_left_alarm_when_metric_has_been_to_count_down(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 59), datetime(2022, 8, 1, 15, 59))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 30), 'value': 3},
            {'timestamp': datetime(2022, 8, 1, 15, 31), 'value': 4},
        ])
        event = _create_cloudwatch_alarm_event('left_alarm', datetime(2022, 8, 1, 15, 59), 2.0)
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=event, joined_alarm_name='joined_alarm', left_alarm_name='left_alarm', metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.count()

        assert actual == {'previous_count': 4, 'connected_count': 2, 'joined_count': 0, 'left_count': 2}

    def test_count_for_cloudwatch_logs_metric_filter_if_function_is_called_by_left_alarm_when_metric_has_been_to_count_down_more_than_one(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 59), datetime(2022, 8, 1, 15, 59))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [
            {'timestamp': datetime(2022, 8, 1, 15, 20), 'value': 4},
            {'timestamp': datetime(2022, 8, 1, 15, 25), 'value': 1},
            {'timestamp': datetime(2022, 8, 1, 15, 26), 'value': 3},
        ])
        event = _create_cloudwatch_alarm_event('left_alarm', datetime(2022, 8, 1, 15, 59), 3.0)
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=event, joined_alarm_name='joined_alarm', left_alarm_name='left_alarm', metric_namespace='test_namespace', metric_name='test_metric_name')

        actual = obj.count()

        assert actual == {'previous_count': 3, 'connected_count': 0, 'joined_count': 0, 'left_count': 3}

    def test_not_count_if_cloudwatch_alarm_name_is_not_found(self, mocker):
        self._mock_to_get_metric_data(mocker, 'test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14, 31), datetime(2022, 8, 1, 15, 31))
        self._set_to_repeat_fill_metric_data('test_namespace', 'test_metric_name', datetime(2022, 8, 1, 14), datetime(2022, 8, 1, 16), [])
        event = _create_cloudwatch_alarm_event('unknown_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=event, joined_alarm_name='joined_alarm', left_alarm_name='left_alarm', metric_namespace='test_namespace', metric_name='test_metric_name')

        with pytest.raises(RuntimeError):
            obj.count()

    def test_ckeck_that_event_source_is_sns_message(self):
        event = _create_cloudwatch_alarm_event('joined_alarm', datetime(2022, 8, 1, 15, 31), 1.0)
        obj = CountCommandFromCloudWatchAlarmToDynamoDB(event=event)

        actual = obj.check_event_source()

        assert actual

    def test_raise_error_if_ckeck_that_event_source_is_null(self):
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event=None)

        with pytest.raises(UnknownEventSource):
            obj.check_event_source()

    def test_raise_error_if_ckeck_that_event_source_is_not_sns_message(self):
        obj = CountCommandFromCloudWatchAlarmToCloudWatchLogs(event={})

        with pytest.raises(UnknownEventSource):
            obj.check_event_source()


@mock_dynamodb
class TestCounterTable:
    def test_add_counter(self):
        _create_table('TestTable', 'id')
        obj = CounterTable('TestTable', 'id')

        actual = obj.update_item(1)

        assert actual == 1

    def test_add_counter_multiple(self):
        _create_table('TestTable', 'id')
        obj = CounterTable('TestTable', 'id')

        obj.update_item(1)
        obj.update_item(2)
        actual = obj.update_item(3)

        assert actual == 6

    def test_subtract_counter(self):
        _create_table('TestTable', 'id')
        obj = CounterTable('TestTable', 'id')

        actual = obj.update_item(-1)

        assert actual == -1

    def test_subtract_counter_multiple(self):
        _create_table('TestTable', 'id')
        obj = CounterTable('TestTable', 'id')

        obj.update_item(3)
        obj.update_item(-2)
        actual = obj.update_item(-1)

        assert actual == 0

    @pytest.mark.asyncio
    async def test_count_asyncronously(self):
        import asyncio

        with mock_dynamodb():
            _create_table('TestTable', 'id')
            obj = CounterTable('TestTable', 'id')
            async def async_obj_update_item(count):
                return await asyncio.get_event_loop().run_in_executor(None, obj.update_item, count)

            actuals = await asyncio.gather(
                async_obj_update_item(3),
                async_obj_update_item(-2),
                async_obj_update_item(4)
            )

            assert 5 in actuals


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
