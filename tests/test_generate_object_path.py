from pathlib import Path

import pytest

from domain.model import FileExt, Obj, ObjectType
from app import _generate_obj_path


@pytest.mark.parametrize(
    "doc_path,obj,obj_seq,expected",
    (
        (
            Path("test.mp4"),
            Obj(data=b"", type=ObjectType.DOC_THUMBNAIL, file_ext=FileExt.PNG),
            0,
            "test/0__DOCUMENT_THUMBNAIL.png",
        ),
        (
            Path("test.mp4"),
            Obj(data=b"", type=ObjectType.CHUNK, file_ext=FileExt.PNG),
            1,
            "test/1__CHUNK.png",
        ),
        (
            Path("test.mp4"),
            Obj(
                data=b"", type=ObjectType.CHUNK_THUMBNAIL, file_ext=FileExt.PNG
            ),
            1,
            "test/1__CHUNK_THUMBNAIL.png",
        ),
        (
            Path("test.jpg"),
            Obj(data=b"", type=ObjectType.DOC_THUMBNAIL, file_ext=FileExt.PNG),
            0,
            "test/0__DOCUMENT_THUMBNAIL.png",
        ),
        (
            Path("test.jpg"),
            Obj(data=b"", type=ObjectType.CHUNK, file_ext=FileExt.JPG),
            1,
            "test/1__CHUNK.jpg",
        ),
        (
            Path("test.jpg"),
            Obj(
                data=b"", type=ObjectType.CHUNK_THUMBNAIL, file_ext=FileExt.PNG
            ),
            1,
            "test/1__CHUNK_THUMBNAIL.png",
        ),
        (
            Path("test.txt"),
            Obj(data=b"", type=ObjectType.DOC_THUMBNAIL, file_ext=FileExt.PNG),
            0,
            "test/0__DOCUMENT_THUMBNAIL.png",
        ),
        (
            Path("test.txt"),
            Obj(data=b"", type=ObjectType.CHUNK, file_ext=FileExt.TXT),
            1,
            "test/1__CHUNK.txt",
        ),
    ),
)
def test_generate_obj_path(
    doc_path: Path, obj: Obj, obj_seq: int, expected: str
) -> None:
    assert _generate_obj_path(doc_path, obj, obj_seq) == expected
