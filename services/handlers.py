from pathlib import Path

import requests

from adapters.event_publisher import RedisPublisher
from adapters.repository import ObjectRepository
from domain.events import DocumentUploaded, ObjectPersisted
from services.processor import processor_factory

publisher = RedisPublisher()
repo = ObjectRepository()


def _add_stem_suffix(path: Path, stem_suffix: str) -> Path:
    return Path(f'{path.with_suffix("")}__{stem_suffix}{path.suffix}')


def handle_uploaded_doc(event: DocumentUploaded) -> None:
    doc_path = Path(event.document_path)
    file_data = requests.get(event.document_path).content
    processor = processor_factory(file_data, doc_path.suffix)

    # store doc thumbnail
    doc_thumbnail_path = _add_stem_suffix(doc_path, "thumbnail")
    repo.store(processor.generate_thumbnail(), str(doc_thumbnail_path))

    # store doc chunks
    for chunk_seq, chunk_data in enumerate(processor.split_chunks()):
        chunk_path = _add_stem_suffix(doc_path, f"chunk__{chunk_seq}")
        repo.store(chunk_data, str(chunk_path))

        # store chunk thumbnail
        if processor.has_chunk_thumbnails:
            chunk_thumbnail_path = _add_stem_suffix(chunk_path, "thumbnail")
            repo.store(chunk_data, str(chunk_thumbnail_path))

        publisher.publish(
            ObjectPersisted(
                document_path=str(doc_path),
                object_path=str(chunk_path),
                object_thumbnail_path=str(chunk_thumbnail_path),
            )
        )
