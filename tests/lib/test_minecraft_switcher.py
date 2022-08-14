import boto3
import json

import pytest
from moto import mock_cloudformation, mock_s3

with mock_cloudformation():
    from src.lib.minecraft_switcher import (
        MinecraftSwitcher,
        UnnecessaryToUpdateStackError,
        NotFoundStackError,
    )


@pytest.fixture
def create_cfn_parameters():
    return lambda p, is_reused=None: [({
        'ParameterKey': k,
        'ParameterValue': v
    } | ({'UsePreviousValue': is_reused} if is_reused is not None else {})) for k, v in p.items()]


@pytest.fixture
def create_cfn_template_body():
    return lambda p: {
        'AWSTemplateFormatVersion': '2010-09-09',
        'Parameters': {k: {'Type': 'String'} for k in p.keys()},
        'Resources': {
            'SampleBucket': {
                'Type': 'AWS::S3::Bucket',
                'Properties': {
                    'BucketName': 'sample.bucket',
                },
            },
        },
    }


@pytest.fixture
@mock_cloudformation
def create_cfn_stack(create_cfn_parameters, create_cfn_template_body):
    cfn = boto3.client('cloudformation')

    def function(stack_name, parameters={}):
        with mock_s3():
            # create_stackはS3を参照しており、2回以上実行すると409を返してエラーになってしまう
            # https://github.com/spulec/moto/issues/4925
            cfn.create_stack(
                StackName=stack_name,
                TemplateBody=json.dumps(create_cfn_template_body(parameters)),
                Parameters=create_cfn_parameters(parameters)
            )

    return function


@mock_cloudformation
class TestMinecraftSwitcher:
    def test_not_set_param_if_stack_name_is_null(self):
        with pytest.raises(RuntimeError):
            MinecraftSwitcher(None)

    def test_not_set_param_if_stack_name_is_empty(self):
        with pytest.raises(RuntimeError):
            MinecraftSwitcher('')

    def test_not_set_param_if_stack_name_is_not_string(self):
        with pytest.raises(RuntimeError):
            MinecraftSwitcher(['not', 'string', 'info'])

    def test_get_cloudformation_one_parameter(self, create_cfn_stack, create_cfn_parameters):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue'}
        create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name)
        expected = create_cfn_parameters(stack_params)

        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_get_cloudformation_multi_parameters(self, create_cfn_stack, create_cfn_parameters):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name)
        expected = create_cfn_parameters(stack_params)

        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_not_get_cloudformation_empty_parameters(self, create_cfn_stack, create_cfn_parameters):
        stack_name = 'SampleStack'
        stack_params = {}
        create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name)
        expected = create_cfn_parameters(stack_params)

        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_not_get_cloudformation_if_try_to_search_different_stack_name(self, create_cfn_stack, create_cfn_parameters):
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        create_cfn_stack('SampleStack', stack_params)
        obj = MinecraftSwitcher('NotFoundStackName')

        actual = obj.get_cloudformation_parameters()

        assert actual is None

    def test_update_cloudformation_to_override_all_parameters(self, create_cfn_stack, create_cfn_parameters):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {'SampleKey': 'Overrode', 'ExampleKey': 'Rewritten'}
        create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name)
        expected = create_cfn_parameters(overrode_params)

        obj.update_cloudformation_stack(overrode_params)
        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_update_cloudformation_to_override_any_parameters(self, create_cfn_stack, create_cfn_parameters):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {'SampleKey': 'Overrode'}
        ignore_overrode_params = {'ExampleKey': 'ExampleValue'}
        create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name)
        expected = create_cfn_parameters(overrode_params | ignore_overrode_params)

        obj.update_cloudformation_stack(overrode_params)
        actual = obj.get_cloudformation_parameters()

        assert actual == expected

    def test_not_update_cloudformation_not_to_override_any_parameters(self, create_cfn_stack, create_cfn_parameters):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {}
        create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name)
        expected = create_cfn_parameters(stack_params)

        with pytest.raises(UnnecessaryToUpdateStackError):
            obj.update_cloudformation_stack(overrode_params)

    def test_not_update_cloudformation_not_to_override_same_parameters(self, create_cfn_stack, create_cfn_parameters):
        stack_name = 'SampleStack'
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        create_cfn_stack(stack_name, stack_params)
        obj = MinecraftSwitcher(stack_name)
        expected = create_cfn_parameters(overrode_params)

        with pytest.raises(UnnecessaryToUpdateStackError):
            obj.update_cloudformation_stack(overrode_params)

    def test_raise_exception_when_updated_cloudformation_if_is_not_found(self, create_cfn_stack, create_cfn_parameters):
        stack_params = {'SampleKey': 'SampleValue', 'ExampleKey': 'ExampleValue'}
        overrode_params = {}
        create_cfn_stack('SampleStack', stack_params)
        obj = MinecraftSwitcher('NotFoundStackName')
        expected = create_cfn_parameters(stack_params)

        with pytest.raises(NotFoundStackError):
            obj.update_cloudformation_stack(overrode_params)
