import re
from io import BytesIO
from pathlib import Path

import pytest
from event_core.domain.types import Asset, Element
from PIL import Image

from config import THUMB_HEIGHT, THUMB_WIDTH
from processors.base import AbstractProcessor


def _to_alnum(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", s)


@pytest.mark.parametrize(
    "fixture_processor", ("img_processor", "vid_processor")
)
def test_img_and_video_docs_generate_doc_thumbnail(
    fixture_processor: str, request: pytest.FixtureRequest
) -> None:
    processor: AbstractProcessor = request.getfixturevalue(fixture_processor)
    doc_thumb_data = None
    for unit in processor():
        if unit.type == Asset.DOC_THUMBNAIL:
            doc_thumb_data = unit.data
            break

    assert doc_thumb_data is not None
    doc_thumb_image = Image.open(BytesIO(doc_thumb_data))
    assert doc_thumb_image.width == THUMB_WIDTH
    assert doc_thumb_image.height == THUMB_HEIGHT


def test_text_doc_original_text_equals_chunks(
    txt_processor: AbstractProcessor, txt_file_path: Path
) -> None:
    original = txt_file_path.read_text()
    text_chunks = [
        unit.data.decode("utf-8")
        for unit in txt_processor()
        if unit.type == Element.TEXT
    ]
    concat_text = "".join(text_chunks)
    assert _to_alnum(original) == _to_alnum(concat_text)
