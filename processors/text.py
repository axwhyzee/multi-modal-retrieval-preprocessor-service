from typing import Iterator

from event_core.domain.types import FileExt, UnitType
from langchain_text_splitters import RecursiveCharacterTextSplitter

from config import TEXT_CHUNK_MIN_SIZE, TEXT_CHUNK_OVERLAP, TEXT_CHUNK_SIZE
from processors.base import AbstractProcessor
from processors.common import Unit


class TextProcessor(AbstractProcessor):

    def __init__(
        self, data: bytes, file_ext: FileExt = FileExt.TXT, *args, **kwargs
    ):
        super().__init__(data, file_ext, *args, **kwargs)
        self._text = data.decode("utf-8")

    def __call__(self) -> Iterator[Unit]:
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=TEXT_CHUNK_SIZE - TEXT_CHUNK_MIN_SIZE,
            chunk_overlap=TEXT_CHUNK_OVERLAP,
            length_function=len,
            separators=["\n\n", "\n", ". ", ",", " ", ""],
            is_separator_regex=False,
            keep_separator=False,
        )
        texts = splitter.split_text(self._text)
        texts.append("")  # ensure last chunk is consumed even if < min width
        accumulated = ""
        for seq, doc in enumerate(texts, start=1):
            accumulated += doc + "\n"
            if len(accumulated) >= TEXT_CHUNK_MIN_SIZE:
                yield Unit(
                    seq=seq,
                    data=accumulated.encode("utf-8"),
                    type=UnitType.CHUNK,
                    file_ext=FileExt.TXT,
                )
                accumulated = ""
