import logging
from pathlib import Path

from event_core.adapters.pubsub import RedisConsumer
from event_core.adapters.storage import add, get
from event_core.domain.events import DocStored

from domain.model import document_factory


def _handle_doc_callback(event: DocStored) -> None:
    doc_path = Path(event.obj_path)
    doc_data = get(event.obj_path)
    document = document_factory(doc_data, doc_path.suffix)
    for i, obj in enumerate(document.generate_objs()):
        obj_path = f'{doc_path.with_suffix("")}__{i}__{obj.type}{obj.file_ext}'
        add(obj.data, f"META/{obj_path}", obj.type)


def main():
    with RedisConsumer() as consumer:
        consumer.subscribe(DocStored)
        consumer.listen(_handle_doc_callback)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
