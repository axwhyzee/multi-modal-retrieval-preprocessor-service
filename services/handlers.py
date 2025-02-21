from pathlib import Path
from typing import Callable, Dict, Type

from event_core.adapters.pubsub.event_publisher import RedisPublisher
from event_core.adapters.repository.client import StorageClient
from event_core.domain.events import Event, ObjectStored, ObjectType

from domain.model import document_factory

publisher = RedisPublisher()
repo = StorageClient()


def handle_doc_stored(event: ObjectStored) -> None:
    if event.obj_type != ObjectType.DOC:
        pass

    doc_path = Path(event.obj_path)
    doc_data = repo.get(event.obj_path)
    document = document_factory(doc_data, doc_path.suffix)

    for i, obj in enumerate(document.generate_objs()):
        obj_path = f'{doc_path.with_suffix("")}__{i}__{obj.type}{obj.file_ext}'
        repo.add(obj.data, f"META/{obj_path}")
        publisher.publish(
            ObjectStored(obj_path=str(obj_path), obj_type=obj.type)
        )


EVENT_HANDLERS: Dict[Type[Event], Callable] = {
    ObjectStored: handle_doc_stored,
}
