from typing import Dict, Type

from event_core.domain.types import FileExt

from processors.base import AbstractProcessor
from processors.image import ImageProcessor
from processors.pdf import PdfProcessor
from processors.text import TextProcessor
from processors.video import VideoProcessor

PROCESSORS_BY_EXT: Dict[FileExt, Type[AbstractProcessor]] = {
    FileExt.TXT: TextProcessor,
    FileExt.JPEG: ImageProcessor,
    FileExt.JPG: ImageProcessor,
    FileExt.PNG: ImageProcessor,
    FileExt.MP4: VideoProcessor,
    FileExt.PDF: PdfProcessor,
}
