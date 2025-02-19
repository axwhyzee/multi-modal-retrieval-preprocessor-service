import re
import tempfile
from pathlib import Path

import pytest
from PIL import Image

from services.processor import (
    THUMBNAIL_FILE_EXTENSION,
    AbstractFileProcessor,
    FileExtension,
)


def _remove_non_alphanumeric(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", s)


def _is_image_data_valid(data: bytes, file_ext: FileExtension) -> bool:
    with tempfile.NamedTemporaryFile(
        delete_on_close=False, suffix=file_ext
    ) as temp_file:
        temp_file.write(data)
        temp_file.close()
        try:
            Image.open(temp_file.name)
            return True
        except IOError:
            return False


def test_text_splits_into_one_or_more_chunks(
    txt_processor: AbstractFileProcessor, txt_file_path: Path
) -> None:
    chunks = list(txt_processor.split_chunks())
    combined_chunks = "".join([chunk.decode() for chunk in chunks])
    original_text = txt_file_path.read_text()
    assert len(chunks) >= 1
    assert _remove_non_alphanumeric(combined_chunks) == _remove_non_alphanumeric(
        original_text
    )


def test_image_splits_into_one_chunk(jpg_processor: AbstractFileProcessor) -> None:
    chunks = jpg_processor.split_chunks()
    assert len(list(chunks)) == 1


def test_video_splits_into_one_or_more_chunks(
    mp4_processor: AbstractFileProcessor,
) -> None:
    chunks = mp4_processor.split_chunks()
    assert len(list(chunks)) >= 1


@pytest.mark.parametrize(
    "fixture_processor",
    (
        "txt_processor",
        "jpg_processor",
        "mp4_processor",
    ),
)
def test_generate_thumbnail(
    request: pytest.FixtureRequest, fixture_processor: str
) -> None:
    processor: AbstractFileProcessor = request.getfixturevalue(fixture_processor)
    data = processor.generate_thumbnail()
    assert _is_image_data_valid(data, THUMBNAIL_FILE_EXTENSION)
