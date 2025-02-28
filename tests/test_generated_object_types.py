from collections import Counter, defaultdict
from typing import Dict

import pytest
from event_core.domain.types import ObjectType

from domain.model import IMG_EXT, AbstractDoc, FileExt


def test_text_doc_generates_only_chunks(
    txt_doc: AbstractDoc,
) -> None:
    obj_types = Counter([obj.type for obj in txt_doc.generate_objs()])
    assert obj_types[ObjectType.CHUNK] >= 1
    assert obj_types[ObjectType.CHUNK_THUMBNAIL] == 0
    assert obj_types[ObjectType.DOC_THUMBNAIL] == 0


def test_img_doc_generates_one_chunk_and_one_doc_thumbnail(
    jpg_doc: AbstractDoc,
) -> None:
    obj_types = Counter([obj.type for obj in jpg_doc.generate_objs()])
    assert obj_types[ObjectType.CHUNK] == 1
    assert obj_types[ObjectType.CHUNK_THUMBNAIL] == 0
    assert obj_types[ObjectType.DOC_THUMBNAIL] == 1


def test_vid_doc_generates_chunks_with_thumbnails(
    mp4_doc: AbstractDoc,
) -> None:
    obj_types = Counter([obj.type for obj in mp4_doc.generate_objs()])
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
    for obj in doc.generate_objs():
        if obj.type == ObjectType.CHUNK:
            assert obj.file_ext == chunk_file_ext
        elif obj.type in (
            ObjectType.CHUNK_THUMBNAIL,
            ObjectType.DOC_THUMBNAIL,
        ):
            assert obj.file_ext == IMG_EXT
