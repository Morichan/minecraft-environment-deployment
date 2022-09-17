from logging import getLogger, INFO

from fastapi import FastAPI
from mangum import Mangum

from router import router


logger = getLogger(__name__)
logger.setLevel(INFO)

app = FastAPI()
app.include_router(router)

handler = Mangum(app)
