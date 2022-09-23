import boto3
import json

import pytest
from moto import (
    mock_cloudformation,
    mock_dynamodb,
    mock_s3,
)

with mock_cloudformation(), mock_dynamodb():
    from lib.minecraft_switcher import (
        MinecraftSwitcher,
        UnnecessaryToUpdateStackError,
        NotFoundStackError,
        UserStillConnectedError,
    )


@mock_cloudformation
@mock_dynamodb
class TestMinecraftSwitcher:
    dynamodb = boto3.client('dynamodb')

    def _create_cfn_parameters(self, parameters, is_reused=None):
        return [({
            'ParameterKey': k,
            'ParameterValue': v
        } | ({'UsePreviousValue': is_reused} if is_reused is not None else {})) for k, v in parameters.items()]

    def _create_cfn_template_body(self, parameters):
        return {
            'AWSTemplateFormatVersion': '2010-09-09',
            'Parameters': {k: {'Type': 'String'} for k in parameters.keys()},
            'Resources': {
                'SampleBucket': {
                    'Type': 'AWS::S3::Bucket',
                    'Properties': {
                        'BucketName': 'sample.bucket',
                    },
                },
            },
        }

    def _create_cfn_stack(self, stack_name, parameters={}):
        cfn = boto3.client('cloudformation')

        with mock_s3():
            # create_stackはS3を参照しており、2回以上実行すると409を返してエラーになってしまう
            # https://github.com/spulec/moto/issues/4925
            cfn.create_stack(
                StackName=stack_name,
                TemplateBody=json.dumps(self._create_cfn_template_body(parameters)),
                Parameters=self._create_cfn_parameters(parameters)
            )

    def _create_table(self, table_name, primary_key):
        self.dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{'AttributeName': primary_key, 'AttributeType': 'S'}],
            KeySchema=[{'AttributeName': primary_key, 'KeyType': 'HASH'}],
            BillingMode='PAY_PER_REQUEST'
        )

    def test_not_set_param_if_stack_name_is_null(self):
        with pytest.raises(RuntimeError):
            MinecraftSwitcher(None, None, None)

    def test_not_set_param_if_stack_name_is_empty(self):
        with pytest.raises(RuntimeError):
            MinecraftSwitcher('', None, None)

    def test_not_set_param_if_stack_name_is_not_string(self):
        with pytest.raises(RuntimeError):
            MinecraftSwitcher(['not', 'string', 'info'], None, None)

    def test_get_cloudformation_one_parameter(self):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue'}
        self._create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name, None, None)
        expected = self._create_cfn_parameters(stack_params)

        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_get_cloudformation_multi_parameters(self):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        self._create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name, None, None)
        expected = self._create_cfn_parameters(stack_params)

        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_not_get_cloudformation_empty_parameters(self):
        stack_name = 'SampleStack'
        stack_params = {}
        self._create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name, None, None)
        expected = self._create_cfn_parameters(stack_params)

        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_not_get_cloudformation_if_try_to_search_different_stack_name(self):
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        self._create_cfn_stack('SampleStack', stack_params)
        obj = MinecraftSwitcher('NotFoundStackName', None, None)

        actual = obj.get_cloudformation_parameters()

        assert actual is None

    def test_update_cloudformation_to_override_all_parameters(self):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {'SampleKey': 'Overrode', 'ExampleKey': 'Rewritten'}
        self._create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name, None, None)
        expected = self._create_cfn_parameters(overrode_params)

        obj.update_cloudformation_stack(overrode_params)
        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_update_cloudformation_to_override_any_parameters(self):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {'SampleKey': 'Overrode'}
        ignore_overrode_params = {'ExampleKey': 'ExampleValue'}
        self._create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name, None, None)
        expected = self._create_cfn_parameters(overrode_params | ignore_overrode_params)

        obj.update_cloudformation_stack(overrode_params)
        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_not_update_cloudformation_not_to_override_any_parameters(self):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {}
        self._create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name, None, None)
        expected = self._create_cfn_parameters(stack_params)

        with pytest.raises(UnnecessaryToUpdateStackError):
            obj.update_cloudformation_stack(overrode_params)

    def test_not_update_cloudformation_not_to_override_same_parameters(self):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        self._create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name, None, None)
        expected = self._create_cfn_parameters(overrode_params)

        with pytest.raises(UnnecessaryToUpdateStackError):
            obj.update_cloudformation_stack(overrode_params)

    def test_raise_exception_when_updated_cloudformation_if_is_not_found(self):
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {}
        self._create_cfn_stack('SampleStack', stack_params)
        obj = MinecraftSwitcher('NotFoundStackName', None, None)
        expected = self._create_cfn_parameters(stack_params)

        with pytest.raises(NotFoundStackError):
            obj.update_cloudformation_stack(overrode_params)

    def test_not_update_cloudformation_if_there_is_connected_user(self):
        self._create_table('TestTable', 'id')
        self.dynamodb.put_item(
            TableName='TestTable',
            Item={'id': {'S': 'counter'}, 'count': {'N': '1'}}
        )
        self._create_cfn_stack('TestStack', {'TestKey': 'TestValue'})
        obj = MinecraftSwitcher('TestStack', 'TestTable', 'id')

        with pytest.raises(UserStillConnectedError):
            obj.update_cloudformation_stack({'TestKey': 'Overrode'})

    def test_update_cloudformation_if_there_is_not_connected_user(self):
        self._create_table('TestTable', 'id')
        self.dynamodb.put_item(
            TableName='TestTable',
            Item={'id': {'S': 'counter'}, 'count': {'N': '0'}}
        )
        self._create_cfn_stack('TestStack', {'TestKey': 'TestValue'})
        obj = MinecraftSwitcher('TestStack', 'TestTable', 'id')
        expected = self._create_cfn_parameters({'TestKey': 'Overrode'})

        obj.update_cloudformation_stack({'TestKey': 'Overrode'})
        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_update_cloudformation_if_there_is_not_data_from_table(self):
        self._create_table('TestTable', 'id')
        self._create_cfn_stack('TestStack', {'TestKey': 'TestValue'})
        obj = MinecraftSwitcher('TestStack', 'TestTable', 'id')
        expected = self._create_cfn_parameters({'TestKey': 'Overrode'})

        obj.update_cloudformation_stack({'TestKey': 'Overrode'})
        actual = obj.get_cloudformation_parameters()

        assert actual == expected
