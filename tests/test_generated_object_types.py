from collections import Counter, defaultdict
from typing import Dict

import pytest
from event_core.domain.types import ObjectType

from domain.model import THUMB_EXT, AbstractDoc, FileExt


def test_text_doc_generates_chunks_without_thumbnail(
    txt_doc: AbstractDoc,
) -> None:
    chunks = list(txt_doc._chunk())
    for _, obj in chunks:
        assert obj.type == ObjectType.CHUNK
    assert len(chunks) >= 1


def test_text_doc_generates_correct_object_types(txt_doc: AbstractDoc) -> None:
    obj_types: Dict[ObjectType, int] = defaultdict(int)

    for _, obj in txt_doc.generate_objs():
        obj_types[obj.type] += 1

    assert set(obj_types.keys()) == {
        ObjectType.CHUNK,
        ObjectType.DOC_THUMBNAIL,
    }
    assert obj_types[ObjectType.CHUNK] >= 1
    assert obj_types[ObjectType.DOC_THUMBNAIL] == 1


def test_img_doc_generates_one_chunk_without_thumbnail(
    jpg_doc: AbstractDoc,
) -> None:
    chunks = list(jpg_doc._chunk())
    for _, obj in chunks:
        assert obj.type == ObjectType.CHUNK

    assert len(chunks) == 1


def test_img_doc_generates_correct_object_types(jpg_doc: AbstractDoc) -> None:
    obj_types: Dict[ObjectType, int] = defaultdict(int)

    for _, obj in jpg_doc.generate_objs():
        obj_types[obj.type] += 1

    assert set(obj_types.keys()) == {
        ObjectType.CHUNK,
        ObjectType.DOC_THUMBNAIL,
    }
    assert obj_types[ObjectType.CHUNK] == 1
    assert obj_types[ObjectType.DOC_THUMBNAIL] == 1


def test_vid_doc_generates_chunks_with_thumbnail(mp4_doc: AbstractDoc) -> None:
    obj_types = Counter([obj.type for _, obj in mp4_doc._chunk()])
    assert obj_types[ObjectType.CHUNK] == obj_types[ObjectType.CHUNK_THUMBNAIL]
    assert obj_types[ObjectType.CHUNK] >= 1


def test_vid_doc_generates_correct_object_types(mp4_doc: AbstractDoc) -> None:
    obj_types: Dict[ObjectType, int] = defaultdict(int)

    for _, obj in mp4_doc.generate_objs():
        obj_types[obj.type] += 1

    assert set(obj_types.keys()) == {
        ObjectType.CHUNK,
        ObjectType.CHUNK_THUMBNAIL,
        ObjectType.DOC_THUMBNAIL,
    }
    assert obj_types[ObjectType.CHUNK] == obj_types[ObjectType.CHUNK_THUMBNAIL]
    assert obj_types[ObjectType.CHUNK] >= 1
    assert obj_types[ObjectType.DOC_THUMBNAIL] == 1


@pytest.mark.parametrize(
    "fixture_doc,chunk_file_ext",
    (
        ("txt_doc", FileExt.TXT),
        ("jpg_doc", FileExt.JPG),
        ("mp4_doc", FileExt.PNG),
    ),
)
def test_doc_generates_obj_with_correct_file_ext(
    fixture_doc: str, request: pytest.FixtureRequest, chunk_file_ext: FileExt
) -> None:
    doc: AbstractDoc = request.getfixturevalue(fixture_doc)
    for _, obj in doc.generate_objs():
        if obj.type == ObjectType.CHUNK:
            assert obj.file_ext == chunk_file_ext
        elif obj.type in (
            ObjectType.CHUNK_THUMBNAIL,
            ObjectType.DOC_THUMBNAIL,
        ):
            assert obj.file_ext == THUMB_EXT
