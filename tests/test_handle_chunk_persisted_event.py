from pathlib import Path
from typing import cast

from event_core.adapters.services.meta import FakeMetaMapping, Meta
from event_core.adapters.services.storage import FakeStorageClient, Payload
from event_core.domain.events import DocStored
from event_core.domain.types import Modal, ObjectType

from app import _handle_doc_callback
from bootstrap import DIContainer
from domain.model import IMG_EXT


def test_handle_mp4_doc_stored(
    mp4_file_path: Path, container: DIContainer
) -> None:
    modal = Modal.VIDEO
    doc_key = str(mp4_file_path)
    doc_stored_event = DocStored(key=doc_key, modal=modal)

    meta = cast(FakeMetaMapping, container.meta())
    storage = cast(FakeStorageClient, container.storage())
    storage[doc_key] = Payload(
        data=mp4_file_path.read_bytes(),
        obj_type=ObjectType.DOC,
        modal=modal,
    )  # storage should already have doc object

    _handle_doc_callback(doc_stored_event)

    obj_key_prefix = mp4_file_path.parent / mp4_file_path.stem
    chunk_key = str(obj_key_prefix / f"1__CHUNK{IMG_EXT}")
    chunk_thumb_key = str(obj_key_prefix / f"1__CHUNK_THUMBNAIL{IMG_EXT}")
    doc_thumb_key = str(obj_key_prefix / f"0__DOCUMENT_THUMBNAIL{IMG_EXT}")

    assert Meta.DOC_THUMB in meta
    assert Meta.CHUNK_THUMB in meta
    assert Meta.PARENT in meta
    assert meta[Meta.DOC_THUMB][doc_key] == doc_thumb_key
    assert meta[Meta.CHUNK_THUMB][chunk_key] == chunk_thumb_key
    assert meta[Meta.PARENT][chunk_key] == doc_key
    assert doc_key in storage
    assert doc_thumb_key in storage
    assert chunk_key in storage
    assert chunk_thumb_key in storage


def test_handle_txt_doc_stored(
    txt_file_path: Path, container: DIContainer
) -> None:
    modal = Modal.TEXT
    doc_key = str(txt_file_path)
    doc_stored_event = DocStored(key=doc_key, modal=modal)

    meta = cast(FakeMetaMapping, container.meta())
    storage = cast(FakeStorageClient, container.storage())
    storage[doc_key] = Payload(
        data=txt_file_path.read_bytes(),
        obj_type=ObjectType.DOC,
        modal=modal,
    )  # storage should already have doc object

    _handle_doc_callback(doc_stored_event)

    obj_key_prefix = txt_file_path.parent / txt_file_path.stem
    chunk_key = str(obj_key_prefix / "1__CHUNK.txt")

    assert Meta.PARENT in meta
    assert meta[Meta.PARENT][chunk_key] == doc_key
    assert doc_key in storage
    assert chunk_key in storage


def test_handle_jpg_doc_stored(
    jpg_file_path: Path, container: DIContainer
) -> None:
    modal = Modal.IMAGE
    doc_key = str(jpg_file_path)
    doc_stored_event = DocStored(key=doc_key, modal=modal)

    meta = cast(FakeMetaMapping, container.meta())
    storage = cast(FakeStorageClient, container.storage())
    storage[doc_key] = Payload(
        data=jpg_file_path.read_bytes(),
        obj_type=ObjectType.DOC,
        modal=modal,
    )  # storage should already have doc object

    _handle_doc_callback(doc_stored_event)

    obj_key_prefix = jpg_file_path.parent / jpg_file_path.stem
    chunk_key = str(obj_key_prefix / "1__CHUNK.jpg")
    doc_thumb_key = str(obj_key_prefix / f"0__DOCUMENT_THUMBNAIL{IMG_EXT}")

    assert Meta.DOC_THUMB in meta
    assert Meta.PARENT in meta
    assert meta[Meta.DOC_THUMB][doc_key] == doc_thumb_key
    assert meta[Meta.PARENT][chunk_key] == doc_key
    assert doc_key in storage
    assert doc_thumb_key in storage
    assert chunk_key in storage
