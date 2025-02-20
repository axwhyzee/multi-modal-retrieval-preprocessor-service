from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict

import pytest

from domain.model import THUMB_EXT, AbstractDoc, FileExt, ObjType


def test_text_doc_generates_chunks_without_thumbnail(
    txt_doc: AbstractDoc,
) -> None:
    chunks = list(txt_doc._chunk())
    for obj in chunks:
        assert obj.obj_type == ObjType.CHUNK
    assert len(chunks) >= 1


def test_text_doc_generates_correct_object_types(txt_doc: AbstractDoc) -> None:
    obj_types: Dict[ObjType, int] = defaultdict(int)

    for obj in txt_doc.generate_objs():
        obj_types[obj.obj_type] += 1

    assert set(obj_types.keys()) == {
        ObjType.CHUNK,
        ObjType.DOC_THUMB,
    }
    assert obj_types[ObjType.CHUNK] >= 1
    assert obj_types[ObjType.DOC_THUMB] == 1


def test_img_doc_generates_one_chunk_without_thumbnail(
    jpg_doc: AbstractDoc,
) -> None:
    chunks = list(jpg_doc._chunk())
    for obj in chunks:
        assert obj.obj_type == ObjType.CHUNK

    assert len(chunks) == 1


def test_img_doc_generates_correct_object_types(jpg_doc: AbstractDoc) -> None:
    obj_types: Dict[ObjType, int] = defaultdict(int)

    for obj in jpg_doc.generate_objs():
        obj_types[obj.obj_type] += 1

    assert set(obj_types.keys()) == {
        ObjType.CHUNK,
        ObjType.DOC_THUMB,
    }
    assert obj_types[ObjType.CHUNK] == 1
    assert obj_types[ObjType.DOC_THUMB] == 1


def test_vid_doc_generates_chunks_with_thumbnail(mp4_doc: AbstractDoc) -> None:
    obj_types = Counter([obj.obj_type for obj in mp4_doc._chunk()])
    assert obj_types[ObjType.CHUNK] == obj_types[ObjType.CHUNK_THUMB]
    assert obj_types[ObjType.CHUNK] >= 1


def test_vid_doc_generates_correct_object_types(mp4_doc: AbstractDoc) -> None:
    obj_types: Dict[ObjType, int] = defaultdict(int)

    for obj in mp4_doc.generate_objs():
        obj_types[obj.obj_type] += 1

    assert set(obj_types.keys()) == {
        ObjType.CHUNK,
        ObjType.CHUNK_THUMB,
        ObjType.DOC_THUMB,
    }
    assert obj_types[ObjType.CHUNK] == obj_types[ObjType.CHUNK_THUMB]
    assert obj_types[ObjType.CHUNK] >= 1
    assert obj_types[ObjType.DOC_THUMB] == 1


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
    for obj in doc.generate_objs():
        if obj.obj_type == ObjType.CHUNK:
            assert obj.file_ext == chunk_file_ext
        elif obj.obj_type in (ObjType.CHUNK_THUMB, ObjType.DOC_THUMB):
            assert obj.file_ext == THUMB_EXT
