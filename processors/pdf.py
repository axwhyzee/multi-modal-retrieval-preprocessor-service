import base64
from io import BytesIO
from typing import Iterator, cast

from event_core.adapters.services.meta import Meta
from event_core.domain.types import Asset, Element, FileExt
from pdf2image import convert_from_bytes
from unstructured.documents.elements import CoordinatesMetadata, ElementType
from unstructured.partition.pdf import partition_pdf

from processors.base import AbstractProcessor
from processors.common import (
    IMG_EXT,
    Unit,
    ext_to_pil_fmt,
    resize_to_thumb,
)
from processors.exceptions import EmptyPDF
from processors.text import TextProcessor

IMAGE_TYPES = {
    ElementType.IMAGE: Element.IMAGE,
    ElementType.TABLE: Element.PLOT,
    ElementType.FIGURE: Element.PLOT,
    ElementType.PICTURE: Element.IMAGE,
}

MIME_TYPE_TO_EXT = {
    "image/jpeg": FileExt.JPEG,
    "image/jpg": FileExt.JPG,
    "image/png": FileExt.PNG,
}

MIN_TEXT_CHUNKSIZE = 16


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
        # doc thumbnail
        doc_thumb = _get_pdf_thumbnail(self._data)
        doc_thumb = resize_to_thumb(doc_thumb)
        yield Unit(
            seq=0,
            data=doc_thumb,
            type=Asset.DOC_THUMBNAIL,
            file_ext=IMG_EXT,
        )

        # extract elements
        seq = 1
        chunks = partition_pdf(
            file=BytesIO(self._data),
            infer_table_structure=True,
            strategy="hi_res",
            extract_image_block_types=list(IMAGE_TYPES.keys()),
            extract_image_block_to_payload=True,
        )

        for chunk in chunks:
            # image and plot elements
            if elem_type := IMAGE_TYPES.get(chunk.category):
                meta = chunk.metadata
                ext = MIME_TYPE_TO_EXT[cast(str, meta.image_mime_type)]
                img = base64.b64decode(cast(str, meta.image_base64))

                yield Unit(
                    seq=seq,
                    data=img,
                    type=elem_type,
                    file_ext=ext,
                    meta={
                        Meta.PAGE: meta.page_number,
                        Meta.COORDS: cast(
                            CoordinatesMetadata, meta.coordinates
                        ).points,
                    },
                )
                yield Unit(
                    seq=seq,
                    data=resize_to_thumb(img),
                    type=Asset.ELEM_THUMBNAIL,
                    file_ext=IMG_EXT,
                )
                seq += 1

            # text elements
            if len(chunk.text) < MIN_TEXT_CHUNKSIZE:
                continue

            text_processor = TextProcessor(data=chunk.text.encode("utf-8"))
            for unit in text_processor():
                unit.seq = seq
                unit.meta = {
                    Meta.PAGE: chunk.metadata.page_number,
                    Meta.COORDS: cast(
                        CoordinatesMetadata, chunk.metadata.coordinates
                    ).points,
                }
                seq += 1
                yield unit
