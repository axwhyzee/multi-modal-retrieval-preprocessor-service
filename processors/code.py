from typing import Iterator

from event_core.domain.types import Element, FileExt
from langchain_text_splitters import RecursiveCharacterTextSplitter

from config import CODE_CHUNK_SIZE
from processors.base import AbstractProcessor
from processors.common import Unit


class CodeProcessor(AbstractProcessor):

    def __call__(self) -> Iterator[Unit]:

        splitter = RecursiveCharacterTextSplitter(
            chunk_size=CODE_CHUNK_SIZE,
            chunk_overlap=0,
            length_function=len,
            separators=["\n\n", "\n"],
            is_separator_regex=False,
            strip_whitespace=False,
        )
        chunks = splitter.split_text(self._data.decode("utf-8"))
        for i, chunk in enumerate(chunks, start=1):
            yield Unit(
                seq=i,
                data=chunk.encode("utf-8"),
                type=Element.CODE,
                file_ext=self._file_ext,
            )
