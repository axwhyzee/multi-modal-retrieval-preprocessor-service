import json
import logging
from abc import ABC, abstractmethod
from typing import Callable

import redis
import redis.client

from config import get_redis_connection_params
from domain.events import EVENTS, Event

logger = logging.getLogger(__name__)


class AbstractPubsub(ABC):
    @abstractmethod
    def listen(self, callback: Callable[[Event], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def subscribe(self, channel: str) -> None:
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, *_): ...


class RedisPubsub(AbstractPubsub):
    def __init__(self):
        self._r = redis.Redis(**get_redis_connection_params())
        self._pubsub = self._r.pubsub(ignore_subscribe_messages=True)

    def listen(self, callback: Callable[[Event], None]) -> None:
        for message in self._pubsub.listen():
            data = json.loads(message["data"])
            event_cls = EVENTS[message["channel"]]
            event = event_cls(**data)
            logger.info(f"Received: {event}")
            callback(event)

    def subscribe(self, channel: str) -> None:
        self._pubsub.subscribe(channel)

    def __exit__(self, *_):
        self._r.close()
        self._pubsub.close()
