from pathlib import Path

import pytest

from app import _generate_key
from domain.model import FileExt, Obj, ObjectType


@pytest.mark.parametrize(
    "key,obj,expected",
    (
        (
            Path("test.mp4"),
            Obj(
                seq=0,
                data=b"",
                type=ObjectType.DOC_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/0__DOCUMENT_THUMBNAIL.png",
        ),
        (
            Path("test.mp4"),
            Obj(seq=1, data=b"", type=ObjectType.CHUNK, file_ext=FileExt.PNG),
            "test/1__CHUNK.png",
        ),
        (
            Path("test.mp4"),
            Obj(
                seq=1,
                data=b"",
                type=ObjectType.CHUNK_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/1__CHUNK_THUMBNAIL.png",
        ),
        (
            Path("test.jpg"),
            Obj(
                seq=0,
                data=b"",
                type=ObjectType.DOC_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/0__DOCUMENT_THUMBNAIL.png",
        ),
        (
            Path("test.jpg"),
            Obj(seq=1, data=b"", type=ObjectType.CHUNK, file_ext=FileExt.JPG),
            "test/1__CHUNK.jpg",
        ),
        (
            Path("test.jpg"),
            Obj(
                seq=1,
                data=b"",
                type=ObjectType.CHUNK_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/1__CHUNK_THUMBNAIL.png",
        ),
        (
            Path("test.txt"),
            Obj(
                seq=0,
                data=b"",
                type=ObjectType.DOC_THUMBNAIL,
                file_ext=FileExt.PNG,
            ),
            "test/0__DOCUMENT_THUMBNAIL.png",
        ),
        (
            Path("test.txt"),
            Obj(seq=1, data=b"", type=ObjectType.CHUNK, file_ext=FileExt.TXT),
            "test/1__CHUNK.txt",
        ),
    ),
)
def test_generate_key(key: Path, obj: Obj, expected: str) -> None:
    assert _generate_key(key, obj) == expected
