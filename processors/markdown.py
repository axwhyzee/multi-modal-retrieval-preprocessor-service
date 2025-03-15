from typing import Iterator

from event_core.domain.types import Element, FileExt

from processors.base import AbstractProcessor
from processors.code import CodeProcessor
from processors.common import Unit
from processors.text import TextProcessor


class MarkdownProcessor(AbstractProcessor):
    def __init__(
        self, data: bytes, file_ext: FileExt = FileExt.MD, *args, **kwargs
    ):
        super().__init__(data, file_ext, *args, **kwargs)

    def __call__(self) -> Iterator[Unit]:
        blocks = self._data.decode("utf-8").split("```")
        seq = 0
        for i, block in enumerate(blocks):
            block = block.strip()
            if not block:
                continue

            enc_block = block.encode("utf-8")
            if i % 2:
                # code block
                for unit in CodeProcessor(
                    enc_block, FileExt.PY
                )():  # any file ext will do
                    unit.seq = (seq := seq + 1)
                    unit.type = Element.CODE
                    yield unit
            else:
                # text block
                for unit in TextProcessor(enc_block)():
                    unit.seq = (seq := seq + 1)
                    unit.type = Element.TEXT
                    yield unit
