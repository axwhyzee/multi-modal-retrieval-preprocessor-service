from typing import Iterator

from event_core.domain.types import Asset, Element

from config import IMG_EXT
from processors.base import AbstractProcessor
from processors.common import Unit, resize_to_chunk, resize_to_thumb


class ImageProcessor(AbstractProcessor):

    def __call__(self) -> Iterator[Unit]:
        yield Unit(
            seq=0,
            data=resize_to_thumb(self._data),
            type=Asset.DOC_THUMBNAIL,
            file_ext=IMG_EXT,
        )
        yield Unit(
            seq=1,
            data=resize_to_chunk(self._data),
            type=Element.IMAGE,
            file_ext=self._file_ext,
        )
        yield Unit(
            seq=1,
            data=resize_to_thumb(self._data),
            type=Asset.ELEM_THUMBNAIL,
            file_ext=IMG_EXT,
        )
