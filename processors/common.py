from dataclasses import dataclass
from io import BytesIO
from typing import Optional

from event_core.domain.types import FileExt, RepoObject
from PIL import Image, ImageOps

from config import (
    IMG_EXT,
    THUMB_HEIGHT,
    THUMB_WIDTH,
)


@dataclass
class Unit:
    """A doc is composed of units, like thumbnails, chunks"""

    seq: int
    data: bytes
    type: RepoObject
    file_ext: FileExt
    meta: Optional[dict] = None


def resize_to_thumb(data: bytes) -> bytes:
    image = Image.open(BytesIO(data))
    image = ImageOps.fit(
        image,
        (THUMB_WIDTH, THUMB_HEIGHT),
        method=0,
        bleed=0.0,
        centering=(0.5, 0.5),
    )  # type: ignore
    image_fmt = ext_to_pil_fmt(IMG_EXT)
    image_bytes = BytesIO()
    image.save(image_bytes, format=image_fmt)
    return image_bytes.getvalue()


def ext_to_pil_fmt(file_ext: FileExt) -> str:
    return file_ext.value.strip(".").upper()  # .png -> PNG
