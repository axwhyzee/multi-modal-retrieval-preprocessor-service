import logging
from typing import Callable

from event_core.adapters.pubsub.event_consumer import RedisConsumer
from event_core.domain.events import Event, ObjectStored

from services.handlers import EVENT_HANDLERS


def main():
    callback: Callable[[Event], None]
    callback = lambda event: EVENT_HANDLERS[event.__class__](event)
    with RedisConsumer() as consumer:
        consumer.subscribe(ObjectStored)
        consumer.listen(callback)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
