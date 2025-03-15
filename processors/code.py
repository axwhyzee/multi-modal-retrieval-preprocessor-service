from typing import Iterator

from event_core.domain.types import Element, FileExt

from processors.base import AbstractProcessor
from processors.common import Unit
from processors.text import TextProcessor


class CodeProcessor(AbstractProcessor):
    def __init__(
        self, data: bytes, file_ext: FileExt = FileExt.MD, *args, **kwargs
    ):
        super().__init__(data, file_ext, *args, **kwargs)

    def __call__(self) -> Iterator[Unit]:
        for unit in TextProcessor(self._data)():
            unit.type = Element.CODE
            yield unit
