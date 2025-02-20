import logging
from abc import ABC, abstractmethod
from io import BytesIO
from urllib.parse import urljoin

import requests

from config import get_storage_service_api_url

logger = logging.getLogger(__name__)


class FailedToStore(Exception): ...


class AbstractRepository(ABC):
    @abstractmethod
    def store(self, obj: bytes, obj_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, obj_id: str) -> bytes:
        raise NotImplementedError


class ObjectRepository(ABC):
    STORAGE_SERVICE_API_URL = get_storage_service_api_url()

    def store(self, obj: bytes, obj_id: str) -> None:
        logger.info(f"Storing <{obj_id}>")
        r = requests.post(
            urljoin(self.STORAGE_SERVICE_API_URL, "add"),
            data={"doc_id": obj_id},
            files={"file": (obj_id, BytesIO(obj))},
        )
        if r.status_code != 200:
            raise FailedToStore(f"Failed to store object {obj_id}")

    def get(self, obj_id: str) -> bytes:
        logger.info(f"Fetching <{obj_id}>")
        return requests.get(
            urljoin(self.STORAGE_SERVICE_API_URL, f'get/{obj_id.lstrip("/")}')
        ).content
