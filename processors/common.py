from dataclasses import dataclass
from io import BytesIO

from event_core.domain.types import FileExt, UnitType
from PIL import Image, ImageOps

from config import (
    IMG_CHUNK_HEIGHT,
    IMG_CHUNK_WIDTH,
    IMG_EXT,
    THUMB_HEIGHT,
    THUMB_WIDTH,
)


@dataclass
class Unit:
    """A doc is composed of units, like thumbnails, chunks"""

    seq: int
    data: bytes
    type: UnitType
    file_ext: FileExt


def _resize_img(data: bytes, width: int, height: int) -> bytes:
    image = Image.open(BytesIO(data))
    image = ImageOps.fit(
        image,
        (width, height),
        method=0,
        bleed=0.0,
        centering=(0.5, 0.5),
    )  # type: ignore
    image_fmt = ext_to_pil_fmt(IMG_EXT)
    thumb = BytesIO()
    image.save(thumb, format=image_fmt)
    return thumb.getvalue()


def resize_to_chunk(data: bytes) -> bytes:
    return _resize_img(data, IMG_CHUNK_WIDTH, IMG_CHUNK_HEIGHT)


def resize_to_thumb(data: bytes) -> bytes:
    return _resize_img(data, THUMB_WIDTH, THUMB_HEIGHT)


def ext_to_pil_fmt(file_ext: FileExt) -> str:
    return file_ext.value.strip(".").upper()  # .png -> PNG
