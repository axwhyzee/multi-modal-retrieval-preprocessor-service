import logging
from typing import Callable

from adapters.pubsub import RedisPubsub
from domain.events import CHANNELS, DocumentUploaded, Event
from services.handlers import EVENT_HANDLERS


def main():
    callback: Callable[[Event], None]
    callback = lambda event: EVENT_HANDLERS[event.__class__](event)
    with RedisPubsub() as pubsub:
        pubsub.subscribe(CHANNELS[DocumentUploaded])
        pubsub.listen(callback)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
