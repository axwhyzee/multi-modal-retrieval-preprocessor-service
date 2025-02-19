import os
from typing import Any, Dict, TypeAlias

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

Config: TypeAlias = Dict[str, Any]


def get_redis_connection_params() -> Config:
    return {
        "host": os.environ["REDIS_HOST"],
        "port": os.environ["REDIS_PORT"],
        "username": os.environ["REDIS_USERNAME"],
        "password": os.environ["REDIS_PASSWORD"],
        "decode_responses": True,
    }


def get_storage_service_api_url() -> str:
    return os.environ["STORAGE_SERVICE_API_URL"].rstrip("/") + "/"
