from pathlib import Path
from typing import cast

import pytest
from event_core.adapters.services.mapping import FakeMapper
from event_core.adapters.services.storage import FakeStorageClient
from event_core.domain.events import DocStored
from event_core.domain.types import Modal, ObjectType

from app import _handle_doc_callback
from bootstrap import MODULES, DIContainer
from domain.model import IMG_EXT


@pytest.fixture
def container() -> DIContainer:
    container = DIContainer()
    container.storage_client.override(FakeStorageClient())
    container.mapper.override(FakeMapper())
    container.wire(modules=MODULES)
    return container


def test_handle_mp4_doc_stored(
    mp4_file_path: Path, container: DIContainer
) -> None:
    modal = Modal.VIDEO
    doc_key = str(mp4_file_path)
    doc_stored_event = DocStored(key=doc_key, modal=modal)

    mapper = cast(FakeMapper, container.mapper())
    storage = cast(FakeStorageClient, container.storage_client())
    storage.add(
        data=mp4_file_path.read_bytes(),
        key=doc_key,
        obj_type=ObjectType.DOC,
        modal=modal,
    )  # storage should already have doc object

    _handle_doc_callback(doc_stored_event)

    obj_key_prefix = mp4_file_path.parent / mp4_file_path.stem
    chunk_key = str(obj_key_prefix / f"1__CHUNK{IMG_EXT}")
    chunk_thumb_key = str(obj_key_prefix / f"1__CHUNK_THUMBNAIL{IMG_EXT}")
    doc_thumb_key = str(obj_key_prefix / f"0__DOCUMENT_THUMBNAIL{IMG_EXT}")

    assert "DOCUMENT__DOCUMENT_THUMBNAIL" in mapper._namespaces
    assert "CHUNK__CHUNK_THUMBNAIL" in mapper._namespaces
    assert "CHUNK__DOCUMENT" in mapper._namespaces
    assert (
        mapper._namespaces["DOCUMENT__DOCUMENT_THUMBNAIL"][doc_key]
        == doc_thumb_key
    )
    assert (
        mapper._namespaces["CHUNK__CHUNK_THUMBNAIL"][chunk_key]
        == chunk_thumb_key
    )
    assert mapper._namespaces["CHUNK__DOCUMENT"][chunk_key] == doc_key
    assert doc_key in storage._objects
    assert doc_thumb_key in storage._objects
    assert chunk_key in storage._objects
    assert chunk_thumb_key in storage._objects


def test_handle_txt_doc_stored(
    txt_file_path: Path, container: DIContainer
) -> None:
    modal = Modal.TEXT
    doc_key = str(txt_file_path)
    doc_stored_event = DocStored(key=doc_key, modal=modal)

    mapper = cast(FakeMapper, container.mapper())
    storage = cast(FakeStorageClient, container.storage_client())
    storage.add(
        data=txt_file_path.read_bytes(),
        key=doc_key,
        obj_type=ObjectType.DOC,
        modal=modal,
    )  # storage should already have doc object

    _handle_doc_callback(doc_stored_event)

    obj_key_prefix = txt_file_path.parent / txt_file_path.stem
    chunk_key = str(obj_key_prefix / "1__CHUNK.txt")

    assert "CHUNK__DOCUMENT" in mapper._namespaces
    assert mapper._namespaces["CHUNK__DOCUMENT"][chunk_key] == doc_key
    assert doc_key in storage._objects
    assert chunk_key in storage._objects


def test_handle_jpg_doc_stored(
    jpg_file_path: Path, container: DIContainer
) -> None:
    modal = Modal.IMAGE
    doc_key = str(jpg_file_path)
    doc_stored_event = DocStored(key=doc_key, modal=modal)

    mapper = cast(FakeMapper, container.mapper())
    storage = cast(FakeStorageClient, container.storage_client())
    storage.add(
        data=jpg_file_path.read_bytes(),
        key=doc_key,
        obj_type=ObjectType.DOC,
        modal=modal,
    )  # storage should already have doc object

    _handle_doc_callback(doc_stored_event)

    obj_key_prefix = jpg_file_path.parent / jpg_file_path.stem
    chunk_key = str(obj_key_prefix / "1__CHUNK.jpg")
    doc_thumb_key = str(obj_key_prefix / f"0__DOCUMENT_THUMBNAIL{IMG_EXT}")

    assert "DOCUMENT__DOCUMENT_THUMBNAIL" in mapper._namespaces
    assert "CHUNK__DOCUMENT" in mapper._namespaces
    assert (
        mapper._namespaces["DOCUMENT__DOCUMENT_THUMBNAIL"][doc_key]
        == doc_thumb_key
    )
    assert mapper._namespaces["CHUNK__DOCUMENT"][chunk_key] == doc_key
    assert doc_key in storage._objects
    assert doc_thumb_key in storage._objects
    assert chunk_key in storage._objects
