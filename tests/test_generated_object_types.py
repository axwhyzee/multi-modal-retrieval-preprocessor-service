from collections import Counter

import pytest
from event_core.domain.types import FileExt, UnitType

from config import IMG_EXT
from processors.base import AbstractProcessor


def test_text_doc_generates_only_chunks(
    txt_processor: AbstractProcessor,
) -> None:
    types = Counter([unit.type for unit in txt_processor()])
    assert types[UnitType.CHUNK] >= 1
    assert types[UnitType.CHUNK_THUMBNAIL] == 0
    assert types[UnitType.DOC_THUMBNAIL] == 0


def test_img_doc_generates_one_chunk_and_one_doc_thumbnail(
    img_processor: AbstractProcessor,
) -> None:
    types = Counter([unit.type for unit in img_processor()])
    assert types[UnitType.CHUNK] == 1
    assert types[UnitType.CHUNK_THUMBNAIL] == 1
    assert types[UnitType.DOC_THUMBNAIL] == 1


def test_vid_doc_generates_chunks_with_thumbnails(
    vid_processor: AbstractProcessor,
) -> None:
    types = Counter([unit.type for unit in vid_processor()])
    assert types[UnitType.CHUNK] == types[UnitType.CHUNK_THUMBNAIL]
    assert types[UnitType.CHUNK] >= 1
    assert types[UnitType.DOC_THUMBNAIL] == 1


@pytest.mark.parametrize(
    "fixture_processor,chunk_file_ext",
    (
        ("txt_processor", FileExt.TXT),
        ("img_processor", FileExt.JPG),
        ("vid_processor", FileExt.PNG),
    ),
)
def test_doc_generates_obj_with_correct_file_ext(
    fixture_processor: str,
    request: pytest.FixtureRequest,
    chunk_file_ext: FileExt,
) -> None:
    processor: AbstractProcessor = request.getfixturevalue(fixture_processor)
    for unit in processor():
        if unit.type == UnitType.CHUNK:
            assert unit.file_ext == chunk_file_ext
        elif unit.type in (
            UnitType.CHUNK_THUMBNAIL,
            UnitType.DOC_THUMBNAIL,
        ):
            assert unit.file_ext == IMG_EXT
