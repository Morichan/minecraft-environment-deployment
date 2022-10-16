from logging import getLogger, INFO
import os

from lib.clients_counter import (
    ClientsCounter,
    CountCommandFromCloudWatchLogsToDynamoDB,
)


logger = getLogger(__name__)
logger.setLevel(INFO)

table_name = os.getenv('TABLE_NAME')
primary_key_column_name = os.getenv('PRIMARY_KEY_COLUMN_NAME', 'id')


def handler(event, context):
    clients_counter = ClientsCounter(
        command_class=CountCommandFromCloudWatchLogsToDynamoDB,
        event=event,
        table_name=table_name,
        primary_key_column_name=primary_key_column_name
    )

    clients_counter.count()
