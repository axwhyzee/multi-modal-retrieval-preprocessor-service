import logging
from pathlib import Path

from event_core.adapters.pubsub import RedisConsumer
from event_core.adapters.services.api.storage import add, get
from event_core.adapters.services.mapping import RedisMapper
from event_core.domain.events import DocStored

from domain.model import Obj, document_factory

mapper = RedisMapper()


def _generate_obj_path(doc_path: Path, obj: Obj, obj_seq: int) -> str:
    return str(
        doc_path.parent
        / doc_path.stem
        / f"{obj_seq}__{obj.type}{obj.file_ext}"
    )


def _handle_doc_callback(event: DocStored) -> None:
    doc_path = Path(event.obj_path)
    doc_data = get(event.obj_path)
    with document_factory(doc_data, doc_path.suffix) as document:
        for obj_seq, obj in document.generate_objs():
            obj_path = _generate_obj_path(doc_path, obj, obj_seq)
            add(obj.data, f"{obj_path}", obj.type)
            mapper.set(obj_path, str(doc_path))  # obj -> doc (parent)


def main():
    with RedisConsumer() as consumer:
        consumer.subscribe(DocStored)
        consumer.listen(_handle_doc_callback)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
