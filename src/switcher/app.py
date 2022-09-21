from logging import getLogger, INFO

from fastapi import FastAPI
from mangum import Mangum

from router import router


logger = getLogger(__name__)
logger.setLevel(INFO)

app = FastAPI()
app.include_router(router)


def handler(event, context):
    if 'Records' in event:
        logger.info(f'{event=}')
        event = {
            'httpMethod': 'GET',
            'resource': '/',
            'path': '/switch/off',
            'body': None,
            'multiValueQueryStringParameters': None,
            'requestContext': {},
        }

    return Mangum(app)(event, context)
