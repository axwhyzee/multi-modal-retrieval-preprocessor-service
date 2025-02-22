import re
from io import BytesIO
from pathlib import Path

import pytest
from PIL import Image

from domain.model import THUMB_HEIGHT, THUMB_WIDTH, AbstractDoc


def _to_alnum(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", s)


@pytest.mark.parametrize("fixture_doc", ("txt_doc", "jpg_doc", "mp4_doc"))
def test_doc_generates_doc_thumbnail(
    fixture_doc: str, request: pytest.FixtureRequest
) -> None:
    doc: AbstractDoc = request.getfixturevalue(fixture_doc)
    thumb = doc._get_thumb()
    thumb_image = Image.open(BytesIO(thumb))
    assert thumb_image.width == THUMB_WIDTH
    assert thumb_image.height == THUMB_HEIGHT


def test_text_doc_original_text_equals_chunks(
    txt_doc: AbstractDoc, txt_file_path: Path
) -> None:
    original = txt_file_path.read_text()
    text_chunks = [obj.data.decode("utf-8") for _, obj in txt_doc._chunk()]
    concat_text = "".join(text_chunks)
    assert _to_alnum(original) == _to_alnum(concat_text)
