import re
from io import BytesIO
from pathlib import Path

import pytest
from event_core.domain.types import ObjectType
from PIL import Image

from domain.model import THUMB_HEIGHT, THUMB_WIDTH, AbstractDoc


def _to_alnum(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", s)


@pytest.mark.parametrize("fixture_doc", ("jpg_doc", "mp4_doc"))
def test_img_and_video_docs_generate_doc_thumbnail(
    fixture_doc: str, request: pytest.FixtureRequest
) -> None:
    doc: AbstractDoc = request.getfixturevalue(fixture_doc)
    doc_thumb_data = None
    for obj in doc.generate_objs():
        if obj.type == ObjectType.DOC_THUMBNAIL:
            doc_thumb_data = obj.data
            break

    assert doc_thumb_data is not None
    doc_thumb_image = Image.open(BytesIO(doc_thumb_data))
    assert doc_thumb_image.width == THUMB_WIDTH
    assert doc_thumb_image.height == THUMB_HEIGHT


def test_text_doc_original_text_equals_chunks(
    txt_doc: AbstractDoc, txt_file_path: Path
) -> None:
    original = txt_file_path.read_text()
    text_chunks = [
        obj.data.decode("utf-8")
        for obj in txt_doc.generate_objs()
        if obj.type == ObjectType.CHUNK
    ]
    concat_text = "".join(text_chunks)
    assert _to_alnum(original) == _to_alnum(concat_text)
