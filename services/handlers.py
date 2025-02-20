from pathlib import Path
from typing import Callable, Dict, Type

from adapters.event_publisher import RedisPublisher
from adapters.repository import ObjectRepository
from domain.events import DocumentUploaded, Event, ObjectPersisted
from domain.model import document_factory

publisher = RedisPublisher()
repo = ObjectRepository()


def handle_uploaded_doc(event: DocumentUploaded) -> None:
    doc_path = Path(event.document_path)
    doc_data = repo.get(event.document_path)
    document = document_factory(doc_data, doc_path.suffix)

    for i, obj in enumerate(document.generate_objs()):
        obj_path = (
            f'{doc_path.with_suffix("")}__{i}__{obj.obj_type}{obj.file_ext}'
        )
        repo.store(obj.data, f"META/{obj_path}")
        publisher.publish(
            ObjectPersisted(
                document_path=str(doc_path),
                object_path=str(obj_path),
            )
        )


EVENT_HANDLERS: Dict[Type[Event], Callable] = {
    DocumentUploaded: handle_uploaded_doc,
}
