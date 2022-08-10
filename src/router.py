from logging import getLogger, INFO

from fastapi import APIRouter


logger = getLogger(__name__)
logger.setLevel(INFO)

router = APIRouter()


@router.get('/health')
def health():
    return {'message': 'OK'}
