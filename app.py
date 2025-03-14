import logging
from pathlib import Path
from typing import Dict, Union

from dependency_injector.wiring import Provide, inject
from event_core.adapters.pubsub import RedisConsumer
from event_core.adapters.services.meta import AbstractMetaMapping, Meta
from event_core.adapters.services.storage import Payload, StorageClient
from event_core.domain.events import DocStored
from event_core.domain.types import (
    Asset,
    Element,
    FileExt,
    path_to_ext,
)

from bootstrap import DIContainer, bootstrap
from processors import PROCESSORS_BY_EXT
from processors.common import Unit, resize_to_thumb

logger = logging.getLogger(__name__)

DEFAULT_THUMBNAILS: Dict[FileExt, Path] = {
    FileExt.TXT: Path("assets/icons/txt.png"),
    FileExt.MD: Path("assets/icons/md.png"),
    FileExt.PY: Path("assets/icons/py.jpeg"),
}


def _generate_key(key: Union[str, Path], unit: Unit) -> str:
    if isinstance(key, str):
        key = Path(key)
    return str(
        key.parent / key.stem / f"{unit.seq}__{unit.type}{unit.file_ext}"
    )


@inject
def _handle_doc_callback(
    event: DocStored,
    storage: StorageClient = Provide[DIContainer.storage],
    meta: AbstractMetaMapping = Provide[DIContainer.meta],
) -> None:
    """
    Perform the following preprocessing steps:

    1. Generate units from document
    2. Store units
    3. Map out unit metas
    """
    chunks_by_seq: Dict[int, str] = {}
    thumbs_by_seq: Dict[int, str] = {}

    doc_key = event.key
    doc_ext = path_to_ext(doc_key)
    doc_data = storage[doc_key]

    # map doc key to default thumbnail key if applicable
    if default_thumb_key := (DEFAULT_THUMBNAILS.get(doc_ext)):
        meta[Meta.DOC_THUMB][doc_key] = str(default_thumb_key)

    with PROCESSORS_BY_EXT[doc_ext](doc_data) as processor:
        try:
            for unit in processor():
                unit_key = _generate_key(doc_key, unit)
                storage[unit_key] = Payload(data=unit.data, type=unit.type)
                if unit.type == Asset.DOC_THUMBNAIL:
                    meta[Meta.DOC_THUMB][doc_key] = unit_key
                elif unit.type == Asset.ELEM_THUMBNAIL:
                    thumbs_by_seq[unit.seq] = unit_key
                elif isinstance(unit.type, Element):
                    meta[Meta.PARENT][unit_key] = doc_key
                    chunks_by_seq[unit.seq] = unit_key
                else:
                    logger.warning(f"Unrecognized unit type: {unit.type}")

                # add meta
                if unit.meta:
                    for meta_key, meta_val in unit.meta.items():
                        meta[meta_key][unit_key] = meta_val

        except Exception as e:
            logger.warning(f"Failed to process {doc_key}. Error: {e}")

    # map chunks to chunk thumbnails
    for thumb_seq, thumb_key in thumbs_by_seq.items():
        chunk_key = chunks_by_seq[thumb_seq]
        meta[Meta.CHUNK_THUMB][chunk_key] = thumb_key


@inject
def _insert_default_thumbnails(
    storage: StorageClient = Provide[DIContainer.storage],
) -> None:
    for thumb_path in DEFAULT_THUMBNAILS.values():
        payload = Payload(
            data=resize_to_thumb(thumb_path.read_bytes()),
            type=Asset.DOC_THUMBNAIL,
        )
        storage[str(thumb_path)] = payload


def main():
    _insert_default_thumbnails()
    logger.info("Listening to event broker")
    with RedisConsumer() as consumer:
        consumer.subscribe(DocStored)
        consumer.listen(_handle_doc_callback)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bootstrap()
    main()
