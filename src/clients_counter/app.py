import json
from logging import getLogger, INFO
import os

from lib.clients_counter import ClientsCounterByDynamoDB


logger = getLogger(__name__)
logger.setLevel(INFO)

table_name = os.getenv('TABLE_NAME')
primary_key_column_name = os.getenv('PRIMARY_KEY_COLUMN_NAME', 'id')
joined_alarm_name = os.getenv('JOINED_ALARM_NAME')
left_alarm_name = os.getenv('LEFT_ALARM_NAME')


def handler(event, context):
    clients_counter = ClientsCounterByDynamoDB(
        table_name,
        primary_key_column_name,
        joined_alarm_name,
        left_alarm_name
    )

    clients_counter.count(event['Records'][0]['Sns']['Message'])
