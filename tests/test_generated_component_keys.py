from pathlib import Path

import pytest
from event_core.domain.types import Asset, Element, FileExt

from app import _generate_key
from processors.common import Unit


@pytest.mark.parametrize(
    "key,unit,expected",
    (
        (
            Path("test.mp4"),
            Unit(
                seq=0,
                data=b"",
                type=Asset.DOC_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/0__DOCUMENT_THUMBNAIL.png",
        ),
        (
            Path("test.mp4"),
            Unit(seq=1, data=b"", type=Element.IMAGE, file_ext=FileExt.PNG),
            "test/1__IMAGE.png",
        ),
        (
            Path("test.mp4"),
            Unit(
                seq=1,
                data=b"",
                type=Asset.ELEM_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/1__ELEMENT_THUMBNAIL.png",
        ),
        (
            Path("test.jpg"),
            Unit(
                seq=0,
                data=b"",
                type=Asset.DOC_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/0__DOCUMENT_THUMBNAIL.png",
        ),
        (
            Path("test.jpg"),
            Unit(seq=1, data=b"", type=Element.IMAGE, file_ext=FileExt.JPG),
            "test/1__IMAGE.jpg",
        ),
        (
            Path("test.jpg"),
            Unit(
                seq=1,
                data=b"",
                type=Asset.ELEM_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/1__ELEMENT_THUMBNAIL.png",
        ),
        (
            Path("test.txt"),
            Unit(
                seq=0,
                data=b"",
                type=Asset.DOC_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/0__DOCUMENT_THUMBNAIL.png",
        ),
        (
            Path("test.txt"),
            Unit(seq=1, data=b"", type=Element.TEXT, file_ext=FileExt.TXT),
            "test/1__TEXT.txt",
        ),
    ),
)
def test_generate_key(key: Path, unit: Unit, expected: str) -> None:
    assert _generate_key(key, unit) == expected
