import re
from io import BytesIO
from typing import Iterator, Tuple

import fitz  # type: ignore
from event_core.domain.types import FileExt, UnitType, path_to_ext
from pdf2image import convert_from_bytes

from config import IMG_EXT
from processors.base import AbstractProcessor
from processors.common import (
    Unit,
    ext_to_pil_fmt,
    resize_to_chunk,
    resize_to_thumb,
)
from processors.exceptions import EmptyPDF
from processors.text import TextProcessor


def _text_from_pdf(data: bytes) -> str:
    pdf = fitz.open(stream=data, filetype="pdf")
    text = ""
    for page in pdf:
        page_text = page.get_text("text")
        text += page_text + "\n"
    return text


def _imgs_from_pdf(data: bytes) -> Iterator[Tuple[bytes, FileExt]]:
    pdf = fitz.open(stream=data, filetype="pdf")
    for page_idx in range(len(pdf)):
        page = pdf.load_page(page_idx)
        imgs = page.get_images(full=True)

        for img in imgs:
            xref = img[0]
            base_img = pdf.extract_image(xref)
            img_bytes = base_img["image"]
            img_ext = path_to_ext("_." + base_img["ext"])
            yield img_bytes, img_ext


def _get_pdf_thumbnail(data: bytes) -> bytes:
    images = convert_from_bytes(data, first_page=1, last_page=1)
    if not images:
        raise EmptyPDF
    thumb_io = BytesIO()
    img = images[0]
    img.save(thumb_io, format=ext_to_pil_fmt(IMG_EXT))
    thumb_io.seek(0)
    return thumb_io.getvalue()


class PdfProcessor(AbstractProcessor):

    def __init__(
        self, data: bytes, file_ext: FileExt = FileExt.PDF, *args, **kwargs
    ):
        super().__init__(data, file_ext, *args, **kwargs)

    def __call__(self) -> Iterator[Unit]:
        imgs = _imgs_from_pdf(self._data)
        pdf_text = _text_from_pdf(self._data)
        pdf_text = pdf_text.replace("\n", " ")
        pdf_text = re.sub(
            r"\s{2,}", " ", pdf_text
        )  # sub >=2 whitespaces with single whitespace

        # doc thumbnail
        doc_thumb = _get_pdf_thumbnail(self._data)
        doc_thumb = resize_to_thumb(doc_thumb)
        yield Unit(
            seq=0,
            data=doc_thumb,
            type=UnitType.DOC_THUMBNAIL,
            file_ext=IMG_EXT,
        )

        seq = 1

        # text chunks
        text_processor = TextProcessor(data=pdf_text.encode("utf-8"))
        for seq, unit in enumerate(text_processor(), start=seq):
            yield Unit(
                seq=seq,
                data=unit.data,
                type=UnitType.CHUNK,
                file_ext=FileExt.TXT,
            )

        # image chunks
        for seq, (img, img_ext) in enumerate(imgs, start=seq + 1):
            yield Unit(
                seq=seq,
                data=resize_to_chunk(img),
                type=UnitType.CHUNK,
                file_ext=img_ext,
            )
            yield Unit(
                seq=seq,
                data=resize_to_thumb(img),
                type=UnitType.CHUNK_THUMBNAIL,
                file_ext=IMG_EXT,
            )
