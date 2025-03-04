from abc import ABC, abstractmethod
from typing import Iterator

from event_core.domain.types import FileExt

from processors.common import Unit


class AbstractProcessor(ABC):
    """A processor generates units from a document,
    depending on the modal of the document, such as image,
    text, and even multi-modal documents like PDFs, which
    is a combination of img + text.

    The types of generated units, as aforementioned,
    depends on the modal of the document. E.g.,

    1) Video documents
        Videos can be chunked into frames, where each
        frame is an image, and therefore has an associated
        frame thumbnail. A video document also has a video
        thumbnail.

    2) Text documents
        Text documents can be chunked into text snippets,
        where each snippet does not have a thumbnail,
        unlike video frames. A text document also does not
        have a thumbnail.

    3) Image documents
        Image documents have a thumbnail, but cannot be
        chunked further since an image is already the
        smallest representable unit. Hence, a chunk is
        the same image as the original doc, but downsized.

    All concrete processors implement a `__call__()`
    method to provide a unified entrypoint to creating
    these units.
    """

    def __init__(self, data: bytes, file_ext: FileExt):
        self._data = data
        self._file_ext = file_ext

    @abstractmethod
    def __call__(self) -> Iterator[Unit]:
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, *_): ...
