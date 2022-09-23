from logging import getLogger, INFO

import boto3
from botocore.exceptions import ClientError


logger = getLogger(__name__)
logger.setLevel(INFO)

cfn = boto3.client('cloudformation')
dynamodb = boto3.client('dynamodb')


class MinecraftSwitcher:
    """
    マインクラフトサーバーの起動／停止スイッチクラス.

    Notes
    -----
    実態はCloudFormationスタックのパラメータ上書きによるUpdateStackを実行している。

    Examples
    --------
    >>> # Set properties
    >>> stack_name = 'SampleStack'
    >>> overrode_parameters = {'SampleKey': 'Overrode'}
    >>> table_name = 'SampleTable'
    >>> primary_key_column_name = 'id'
    >>> capabilities = []

    >>> # Generate object and update
    >>> switcher = MinecraftSwitcher(stack_name, table_name, primary_key_column_name)
    >>> switcher.update_cloudformation_stack(overrode_parameters, capabilities)

    """
    def __init__(self, stack_name, table_name, primary_key_column_name):
        if not(type(stack_name) is str and len(stack_name)):
            raise

        self._stack_name = stack_name
        self._table_name = table_name
        self._primary_key_column_name = primary_key_column_name

    def get_cloudformation_parameters(self):
        """
        CloudFormationスタックのパラメーターを取得する.

        Notes
        -----
        self.update_cloudformation_stask メソッド内部で利用しているため、こちらのメソッドの実行は基本的には必要ない。

        Returns
        -------
        parameters : list
            CloudFormationスタックのパラメーターが、CloudFormationで定義されている辞書のリスト。

        """
        try:
            stacks = cfn.describe_stacks(StackName=self._stack_name)['Stacks']
        except ClientError:
            logger.warning(f'Stack is not found (stack_name={self._stack_name})')
            return None

        # 同名スタックは共存しないため、スタックが見つかった場合は1つのみを取得する (空文字で検索した場合を除く)
        return stacks[0]['Parameters']

    def update_cloudformation_stack(self, overrode_parameters, capabilities=[]):
        """
        CloudFormationスタックを更新する.

        Parameters
        ----------
        overrode_parameters : dict
            上書きするCloudFormationパラメーターのキーとバリューの辞書。
            上書きが不要な場合は、値を設定しなければ既存の設定済みパラメーターから自動で適用する。

        capabilities : list
            上書き対象のCloudFormationをUpdateStackする際に必要になる権限のリスト。

        Raises
        ------
        NotFoundStackError
            self._stack_name を元にスタックを検索したが見つからなかった場合に投げる。

        UnnecessaryToUpdateStackError
            アップデート実行時における上書きパラメーターが、全て更新前の設定値と同じなどの理由により、UpdateStack処理を実行する必要が無い場合に投げる。

        UserStillConnectedError
            アップデート実行時、まだユーザーが接続している場合に投げる。

        """
        if self._is_connected_any_users(self._table_name, self._primary_key_column_name):
            raise UserStillConnectedError()

        params = self.get_cloudformation_parameters()

        if params is None:
            raise NotFoundStackError()

        params = [self._override_param_if_need(p, overrode_parameters) for p in params]

        if not all((p.get('UsePreviousValue') for p in params)):
            cfn.update_stack(
                StackName=self._stack_name,
                Parameters=params,
                UsePreviousTemplate=True,
                Capabilities=capabilities
            )
        else:
            logger.warning(f'Stack is unnecessary to update (stack_name={self._stack_name})')
            raise UnnecessaryToUpdateStackError()

    def _override_param_if_need(self, param, overrode_parameters):
        overrode = param.copy()
        key = param['ParameterKey']

        if key in overrode_parameters.keys() and overrode['ParameterValue'] != overrode_parameters[key]:
            overrode['ParameterValue'] = overrode_parameters[key]
        else:
            del overrode['ParameterValue']
            overrode |= {'UsePreviousValue': True}

        return overrode

    def _is_connected_any_users(self, table_name, primary_key_column_name):
        if table_name is None:
            return False

        result = dynamodb.get_item(
            TableName=self._table_name,
            Key={primary_key_column_name: {'S': 'counter'}}
        )

        return int(result.get('Item', {}).get('count', {}).get('N', '0')) > 0


class NotFoundStackError(RuntimeError):
    pass


class UnnecessaryToUpdateStackError(RuntimeError):
    pass


class UserStillConnectedError(RuntimeError):
    pass
