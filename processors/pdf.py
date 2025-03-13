import tempfile
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterator

from event_core.domain.types import Asset, Element, FileExt
from pdf2image import convert_from_bytes
from unstructured.documents.elements import ElementType
from unstructured.partition.pdf import partition_pdf

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

FILE_PREFIX_TO_ELEMENT: Dict[str, Element] = {
    ElementType.FIGURE.lower(): Element.PLOT,
    ElementType.TABLE.lower(): Element.PLOT,
    ElementType.IMAGE.lower(): Element.IMAGE,
    ElementType.PICTURE.lower(): Element.IMAGE,
}


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

        seq = 1
        with tempfile.TemporaryDirectory() as temp_dir:
            chunks = partition_pdf(
                file=BytesIO(self._data),
                infer_table_structure=True,
                strategy="hi_res",
                extract_image_block_types=[
                    ElementType.IMAGE,
                    ElementType.TABLE,
                    ElementType.FIGURE,
                    ElementType.PICTURE,
                ],
                extract_image_block_output_dir=temp_dir,
            )

            # images
            for img_path in Path(temp_dir).iterdir():
                pre = img_path.stem.split("-")[0]
                img = img_path.read_bytes()
                yield Unit(
                    seq=seq,
                    data=resize_to_chunk(img),
                    type=FILE_PREFIX_TO_ELEMENT[pre],
                    file_ext=IMG_EXT,
                )
                yield Unit(
                    seq=seq,
                    data=resize_to_thumb(img),
                    type=Asset.ELEM_THUMBNAIL,
                    file_ext=IMG_EXT,
                )
                seq += 1

        # text chunks
        text = "\n".join([chunk.text for chunk in chunks])
        text_processor = TextProcessor(data=text.encode("utf-8"))
        for seq, unit in enumerate(text_processor(), start=seq):
            unit.seq = seq
            yield unit
