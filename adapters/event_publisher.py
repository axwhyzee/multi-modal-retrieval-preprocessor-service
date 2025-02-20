import json
from abc import ABC, abstractmethod
from dataclasses import asdict

import redis

from config import get_redis_connection_params
from domain.events import CHANNELS, Event


class AbstractPublisher(ABC):
    @abstractmethod
    def publish(self, event: Event) -> None:
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, *_):
        raise NotImplementedError

    def __enter__(self):
        return self


class RedisPublisher(AbstractPublisher):
    def __init__(self):
        self._client = redis.Redis(**get_redis_connection_params())

    def publish(self, event: Event) -> None:
        self._client.publish(
            CHANNELS[event.__class__], json.dumps(asdict(event))
        )

    def __exit__(self, *_):
        self._client.close()
