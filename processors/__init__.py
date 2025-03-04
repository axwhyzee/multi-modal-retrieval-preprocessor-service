from functools import partial
from typing import Callable, Dict

from event_core.domain.types import FileExt

from processors.base import AbstractProcessor
from processors.image import ImageProcessor
from processors.pdf import PdfProcessor
from processors.text import TextProcessor
from processors.video import VideoProcessor

PROCESSORS_BY_EXT: Dict[FileExt, Callable[..., AbstractProcessor]] = {
    FileExt.TXT: partial(TextProcessor, file_ext=FileExt.TXT),
    FileExt.JPEG: partial(ImageProcessor, file_ext=FileExt.JPEG),
    FileExt.JPG: partial(ImageProcessor, file_ext=FileExt.JPG),
    FileExt.PNG: partial(ImageProcessor, file_ext=FileExt.PNG),
    FileExt.MP4: partial(VideoProcessor, file_ext=FileExt.MP4),
    FileExt.PDF: partial(PdfProcessor, file_ext=FileExt.PDF),
}
