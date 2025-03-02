import logging
from pathlib import Path
from typing import Dict, Union, cast

from dependency_injector.wiring import Provide, inject
from event_core.adapters.pubsub import RedisConsumer
from event_core.adapters.services.meta import AbstractMetaMapping, Meta
from event_core.adapters.services.storage import Payload, StorageClient
from event_core.domain.events import DocStored
from event_core.domain.types import Modal, ObjectType

from bootstrap import DIContainer, bootstrap
from domain.model import DOC_FACTORY, FileExt, Obj, img_thumbnail

DEFAULT_MODAL_THUMBNAILS: Dict[Modal, Path] = {
    Modal.TEXT: Path("assets/icons/txt.png"),
}


def _generate_key(key: Union[str, Path], obj: Obj) -> str:
    if isinstance(key, str):
        key = Path(key)
    return str(key.parent / key.stem / f"{obj.seq}__{obj.type}{obj.file_ext}")


def _file_ext_from_key(key: str) -> FileExt:
    suffix = Path(key).suffix
    file_ext = cast(FileExt, FileExt._value2member_map_[suffix])
    return file_ext


@inject
def _handle_doc_callback(
    event: DocStored,
    storage: StorageClient = Provide[DIContainer.storage],
    meta: AbstractMetaMapping = Provide[DIContainer.meta],
) -> None:
    """
    Perform the following preprocessing steps:

    1. Generate objects from document
    2. Store objects
    3. Map out object metas
    """
    key = event.key
    modal = event.modal
    file_ext = _file_ext_from_key(key)
    doc_data = storage[key]

    # map to default thumbnail if applicable
    if default_thumb_key := (DEFAULT_MODAL_THUMBNAILS.get(modal)):
        meta[Meta.DOC_THUMB][key] = str(default_thumb_key)

    chunks_by_seq: Dict[int, str] = {}
    thumbs_by_seq: Dict[int, str] = {}

    with DOC_FACTORY[modal](doc_data, file_ext) as document:
        for obj in document.generate_objs():
            obj_key = _generate_key(key, obj)
            storage[obj_key] = Payload(
                data=obj.data, obj_type=obj.type, modal=modal
            )
            match obj.type:
                case ObjectType.DOC_THUMBNAIL:
                    # map doc to doc thumbnail
                    meta[Meta.DOC_THUMB][key] = obj_key
                case ObjectType.CHUNK:
                    # map chunk to parent doc
                    meta[Meta.PARENT][obj_key] = key
                    chunks_by_seq[obj.seq] = obj_key
                case ObjectType.CHUNK_THUMBNAIL:
                    thumbs_by_seq[obj.seq] = obj_key

    # map chunks to chunk thumbnails
    for thumb_seq, thumb_key in thumbs_by_seq.items():
        chunk_key = chunks_by_seq[thumb_seq]
        meta[Meta.CHUNK_THUMB][chunk_key] = thumb_key


@inject
def _insert_default_thumbnails(
    storage: StorageClient = Provide[DIContainer.storage],
) -> None:
    for icon_path in DEFAULT_MODAL_THUMBNAILS.values():
        payload = Payload(
            data=img_thumbnail(icon_path.read_bytes()),
            obj_type=ObjectType.DOC_THUMBNAIL,
            modal=Modal.IMAGE,
        )
        storage[str(icon_path)] = payload


def main():
    _insert_default_thumbnails()
    with RedisConsumer() as consumer:
        consumer.subscribe(DocStored)
        consumer.listen(_handle_doc_callback)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bootstrap()
    main()
