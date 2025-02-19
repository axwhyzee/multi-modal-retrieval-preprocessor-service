from pathlib import Path

import pytest

from services.processor import AbstractFileProcessor, processor_factory

TEST_DATA_DIR_PATH = Path("tests/data")


def _get_processor(path: Path) -> AbstractFileProcessor:
    data = path.read_bytes()
    return processor_factory(data, path.suffix)


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
def jpg_processor(jpg_file_path: Path) -> AbstractFileProcessor:
    return _get_processor(jpg_file_path)


@pytest.fixture
def mp4_processor(mp4_file_path: Path) -> AbstractFileProcessor:
    return _get_processor(mp4_file_path)


@pytest.fixture
def txt_processor(txt_file_path: Path) -> AbstractFileProcessor:
    return _get_processor(txt_file_path)
