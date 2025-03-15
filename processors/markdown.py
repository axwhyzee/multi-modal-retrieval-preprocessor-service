from typing import Iterator

from event_core.domain.types import Element, FileExt

from processors.base import AbstractProcessor
from processors.code import CodeProcessor
from processors.common import Unit


class MarkdownProcessor(AbstractProcessor):
    def __init__(
        self, data: bytes, file_ext: FileExt = FileExt.MD, *args, **kwargs
    ):
        super().__init__(data, file_ext, *args, **kwargs)

    def __call__(self) -> Iterator[Unit]:
        blocks = self._data.decode("utf-8").split("```")
        seq = 0
        for i, block in enumerate(blocks):
            if not block.strip():
                continue
            type_ = Element.CODE if i % 2 else Element.TEXT
            for unit in CodeProcessor(
                block.encode("utf-8"), FileExt.PY
            )():  # any file ext will do
                unit.seq = (seq := seq + 1)
                unit.type = type_
                yield unit
