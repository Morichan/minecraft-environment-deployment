import json
from logging import getLogger, INFO
import os

from lib.clients_counter import ClientsCounter


logger = getLogger(__name__)
logger.setLevel(INFO)

joined_alarm_name = os.getenv('JOINED_ALARM_NAME')
left_alarm_name = os.getenv('LEFT_ALARM_NAME')
metric_namespace = os.getenv('METRIC_NAMESPACE')
metric_name = os.getenv('METRIC_NAME')


def handler(event, context):
    clients_counter = ClientsCounter(
        joined_alarm_name,
        left_alarm_name,
        metric_namespace,
        metric_name
    )

    log_data = clients_counter.create_log_data(event['Records'][0]['Sns']['Message'])

    logger.info(json.dumps(log_data))
