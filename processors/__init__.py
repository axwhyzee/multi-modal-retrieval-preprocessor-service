from functools import partial
from typing import Callable, Dict, Iterator

from event_core.domain.types import FileExt

from processors.base import AbstractProcessor
from processors.code import CodeProcessor
from processors.common import Unit
from processors.image import ImageProcessor
from processors.markdown import MarkdownProcessor
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
    FileExt.MD: partial(MarkdownProcessor, file_ext=FileExt.MD),
    FileExt.PY: partial(CodeProcessor, file_ext=FileExt.PY),
}


def extract_elems_and_assets(data: bytes, file_ext: FileExt) -> Iterator[Unit]:
    with PROCESSORS_BY_EXT[file_ext](data) as processor:
        yield from processor()
