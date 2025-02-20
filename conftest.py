from pathlib import Path

import pytest

from domain.model import AbstractDoc, document_factory

TEST_DATA_DIR_PATH = Path("tests/data")


def _get_doc(path: Path) -> AbstractDoc:
    data = path.read_bytes()
    return document_factory(data, path.suffix)


@pytest.fixture
def jpg_file_path() -> Path:
    return TEST_DATA_DIR_PATH / "test.jpg"


@pytest.fixture
def mp4_file_path() -> Path:
    return TEST_DATA_DIR_PATH / "test.mp4"


@pytest.fixture
def txt_file_path() -> Path:
    return TEST_DATA_DIR_PATH / "test.txt"


@pytest.fixture
def jpg_doc(jpg_file_path: Path) -> AbstractDoc:
    return _get_doc(jpg_file_path)


@pytest.fixture
def mp4_doc(mp4_file_path: Path) -> AbstractDoc:
    return _get_doc(mp4_file_path)


@pytest.fixture
def txt_doc(txt_file_path: Path) -> AbstractDoc:
    return _get_doc(txt_file_path)
