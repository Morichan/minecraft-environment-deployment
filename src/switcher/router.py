from logging import getLogger, INFO
import os

from fastapi import APIRouter

from lib.minecraft_switcher import (
    MinecraftSwitcher,
    UnnecessaryToUpdateStackError,
    NotFoundStackError,
)


logger = getLogger(__name__)
logger.setLevel(INFO)

stack_name = os.getenv('STACK_NAME')
switched_parameter = os.getenv('SWITCHED_PARAMETER')
changed_task_count_parameter = os.getenv('CHANGED_TASK_COUNT_PARAMETER')

router = APIRouter()


@router.get('/health')
def health():
    return {'message': 'OK'}


@router.get('/switch/on')
def switch_on():
    return _switch('on', 1)


@router.get('/switch/off')
def switch_on():
    return _switch('off', 0)


def _switch(on_off, task_count):
    is_on = on_off == 'on'
    switcher = MinecraftSwitcher(stack_name)

    try:
        switcher.update_cloudformation_stack({
            switched_parameter: 'true' if is_on else 'false',
            changed_task_count_parameter: str(task_count),
        }, ['CAPABILITY_NAMED_IAM'])
        return {'message': f'Try to switch {on_off}, so please wait.'}
    except UnnecessaryToUpdateStackError:
        return {'message': f'Stack is unnecessary to switch {on_off} ({stack_name=}).'}
    except NotFoundStackError:
        return {'message': f'Stack is not found ({stack_name=}).'}
