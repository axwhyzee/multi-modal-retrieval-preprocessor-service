import logging
from pathlib import Path

from event_core.adapters.pubsub import RedisConsumer
from event_core.adapters.services.api.storage import add, get
from event_core.adapters.services.mapping import RedisMapper
from event_core.domain.events import DocStored

from domain.model import Obj, document_factory

mapper = RedisMapper()


def _generate_key(parent_key: Path, obj: Obj, obj_seq: int) -> str:
    return str(
        parent_key.parent
        / parent_key.stem
        / f"{obj_seq}__{obj.type}{obj.file_ext}"
    )


def _handle_doc_callback(event: DocStored) -> None:
    parent_key = Path(event.parent_key)
    data = get(event.key)
    with document_factory(data, parent_key.suffix) as document:
        for obj_seq, obj in document.generate_objs():
            key = _generate_key(parent_key, obj, obj_seq)
            add(obj.data, key, str(parent_key), obj.type)
            mapper.set(key, str(parent_key))  # obj -> parent


def main():
    with RedisConsumer() as consumer:
        consumer.subscribe(DocStored)
        consumer.listen(_handle_doc_callback)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
