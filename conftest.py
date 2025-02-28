from pathlib import Path
from typing import Iterator, cast

import pytest
from event_core.adapters.services.mapping import FakeMapper
from event_core.adapters.services.storage import FakeStorageClient
from event_core.domain.types import MODAL_FACTORY, FileExt

from bootstrap import MODULES, DIContainer
from domain.model import DOC_FACTORY, AbstractDoc

TEST_DATA_DIR_PATH = Path("tests/data")


def _get_doc(path: Path) -> Iterator[AbstractDoc]:
    file_ext = cast(FileExt, FileExt._value2member_map_[path.suffix])
    modal = MODAL_FACTORY[file_ext]
    with DOC_FACTORY[modal](data=path.read_bytes(), file_ext=file_ext) as doc:
        yield doc


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
def jpg_doc(jpg_file_path: Path) -> Iterator[AbstractDoc]:
    yield from _get_doc(jpg_file_path)


@pytest.fixture
def mp4_doc(mp4_file_path: Path) -> Iterator[AbstractDoc]:
    yield from _get_doc(mp4_file_path)


@pytest.fixture
def txt_doc(txt_file_path: Path) -> Iterator[AbstractDoc]:
    yield from _get_doc(txt_file_path)


@pytest.fixture
def container() -> DIContainer:
    container = DIContainer()
    container.storage_client.override(FakeStorageClient())
    container.mapper.override(FakeMapper())
    container.wire(modules=MODULES)
    return container
