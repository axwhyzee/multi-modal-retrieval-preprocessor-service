from pathlib import Path
from typing import cast

from event_core.adapters.services.meta import FakeMetaMapping, Meta
from event_core.adapters.services.storage import FakeStorageClient, Payload
from event_core.domain.events import DocStored
from event_core.domain.types import Asset

from app import _handle_doc_callback
from bootstrap import DIContainer
from processors.common import IMG_EXT


def test_handle_mp4_doc_stored(
    vid_file_path: Path, container: DIContainer
) -> None:
    doc_key = str(vid_file_path)
    doc_stored_event = DocStored(key=doc_key)

    meta = cast(FakeMetaMapping, container.meta())
    storage = cast(FakeStorageClient, container.storage())
    storage[doc_key] = Payload(
        data=vid_file_path.read_bytes(),
        type=Asset.DOC,
    )  # storage should already have doc object

    _handle_doc_callback(doc_stored_event)

    obj_key_prefix = vid_file_path.parent / vid_file_path.stem
    chunk_key = str(obj_key_prefix / f"1__IMAGE_ELEMENT{IMG_EXT}")
    chunk_thumb_key = str(obj_key_prefix / f"1__ELEMENT_THUMBNAIL{IMG_EXT}")
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
    doc_key = str(txt_file_path)
    doc_stored_event = DocStored(key=doc_key)

    meta = cast(FakeMetaMapping, container.meta())
    storage = cast(FakeStorageClient, container.storage())
    storage[doc_key] = Payload(
        data=txt_file_path.read_bytes(),
        type=Asset.DOC,
    )  # storage should already have doc object

    _handle_doc_callback(doc_stored_event)

    obj_key_prefix = txt_file_path.parent / txt_file_path.stem
    chunk_key = str(obj_key_prefix / "1__TEXT_ELEMENT.txt")

    assert Meta.PARENT in meta
    assert meta[Meta.PARENT][chunk_key] == doc_key
    assert doc_key in storage
    assert chunk_key in storage


def test_handle_jpg_doc_stored(
    img_file_path: Path, container: DIContainer
) -> None:
    doc_key = str(img_file_path)
    doc_stored_event = DocStored(key=doc_key)

    meta = cast(FakeMetaMapping, container.meta())
    storage = cast(FakeStorageClient, container.storage())
    storage[doc_key] = Payload(
        data=img_file_path.read_bytes(),
        type=Asset.DOC,
    )  # storage should already have doc object

    _handle_doc_callback(doc_stored_event)

    obj_key_prefix = img_file_path.parent / img_file_path.stem
    chunk_key = str(obj_key_prefix / "1__IMAGE_ELEMENT.jpg")
    doc_thumb_key = str(obj_key_prefix / f"0__DOCUMENT_THUMBNAIL{IMG_EXT}")

    assert Meta.DOC_THUMB in meta
    assert Meta.PARENT in meta
    assert meta[Meta.DOC_THUMB][doc_key] == doc_thumb_key
    assert meta[Meta.PARENT][chunk_key] == doc_key
    assert doc_key in storage
    assert doc_thumb_key in storage
    assert chunk_key in storage
