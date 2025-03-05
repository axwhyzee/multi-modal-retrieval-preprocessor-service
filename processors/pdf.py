import re
from io import BytesIO
from typing import Iterator

import fitz  # type: ignore
from event_core.domain.types import FileExt, UnitType
from pdf2image import convert_from_bytes

from config import PDF_X_CUTS, PDF_Y_CUTS, IMG_EXT
from processors.base import AbstractProcessor
from processors.common import (
    IMG_EXT,
    Unit,
    ext_to_pil_fmt,
    resize_to_chunk,
    resize_to_thumb,
)
from processors.exceptions import EmptyPDF
from processors.text import TextProcessor


PAGE_MAT = fitz.Matrix(PDF_X_CUTS, PDF_Y_CUTS)


def _text_from_pdf(data: bytes) -> str:
    pdf = fitz.open(stream=data, filetype="pdf")
    text = ""
    for page in pdf:
        text += page.get_text("text") + "\n"
    return text


def _imgs_from_pdf(data: bytes) -> Iterator[bytes]:
    """
    Split each page into PDF_X_CUTS * PDF_Y_CUTS number of 
    quadrants. For each quandrant, save entire quadrant as 
    image if and only if it contains at least 1 image.
    """

    pdf = fitz.open(stream=data, filetype="pdf")
    for page in pdf:
        rect = page.rect
        step_x, step_y = rect.tl + rect.br
        step_x /= PDF_X_CUTS
        step_y /= PDF_Y_CUTS

        quads_w_imgs = set()
        for img in page.get_images(full=True):
            xref = img[0]
            for bbox in page.get_image_rects(xref):
                [x1, y1, x2, y2] = bbox
                quads_w_imgs.add(
                    (
                        (x1 // step_x * step_x, y1 // step_y * step_y),
                        (
                            (x2 // step_x + 1) * step_x,
                            (y2 // step_y + 1) * step_y,
                        ),
                    )
                )

        for quad in quads_w_imgs:
            clip = fitz.Rect(*quad)
            pix = page.get_pixmap(matrix=PAGE_MAT, clip=clip)
            yield pix.tobytes(IMG_EXT.lstrip("."))


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
        pdf_text = re.sub(r"\s{2,}", " ", pdf_text)  # truncate contiguous " "

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
        for seq, img in enumerate(imgs, start=seq + 1):
            yield Unit(
                seq=seq,
                data=resize_to_chunk(img),
                type=UnitType.CHUNK,
                file_ext=IMG_EXT,
            )
            yield Unit(
                seq=seq,
                data=resize_to_thumb(img),
                type=UnitType.CHUNK_THUMBNAIL,
                file_ext=IMG_EXT,
            )
