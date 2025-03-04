from pathlib import Path
from typing import Iterator, cast

import pytest
from event_core.adapters.services.meta import FakeMetaMapping
from event_core.adapters.services.storage import FakeStorageClient
from event_core.domain.types import FileExt, path_to_ext

from bootstrap import MODULES, DIContainer
from processors import PROCESSORS_BY_EXT
from processors.base import AbstractProcessor

TEST_DATA_DIR_PATH = Path("tests/data")


def _get_processor(path: Path) -> Iterator[AbstractProcessor]:
    file_ext = path_to_ext(path)
    doc_data = path.read_bytes()
    with PROCESSORS_BY_EXT[file_ext](doc_data) as processor:
        yield processor


@pytest.fixture
def img_file_path() -> Path:
    return TEST_DATA_DIR_PATH / "test.jpg"


@pytest.fixture
def vid_file_path() -> Path:
    return TEST_DATA_DIR_PATH / "test.mp4"


@pytest.fixture
def txt_file_path() -> Path:
    return TEST_DATA_DIR_PATH / "test.txt"


@pytest.fixture
def img_processor(img_file_path: Path) -> Iterator[AbstractProcessor]:
    yield from _get_processor(img_file_path)


@pytest.fixture
def vid_processor(vid_file_path: Path) -> Iterator[AbstractProcessor]:
    yield from _get_processor(vid_file_path)


@pytest.fixture
def txt_processor(txt_file_path: Path) -> Iterator[AbstractProcessor]:
    yield from _get_processor(txt_file_path)


@pytest.fixture
def container() -> DIContainer:
    container = DIContainer()
    container.storage.override(FakeStorageClient())
    container.meta.override(FakeMetaMapping())
    container.wire(modules=MODULES)
    return container
