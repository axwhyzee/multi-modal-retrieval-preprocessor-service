import logging
from pathlib import Path
from typing import Dict, Union, cast

from dependency_injector.wiring import Provide, inject
from event_core.adapters.pubsub import RedisConsumer
from event_core.adapters.services.mapping import AbstractMapper
from event_core.adapters.services.storage import StorageClient
from event_core.domain.events import DocStored
from event_core.domain.types import Modal, ObjectType

from bootstrap import DIContainer, bootstrap
from domain.model import DOC_FACTORY, FileExt, Obj

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
    storage_client: StorageClient = Provide[DIContainer.storage_client],
    mapper: AbstractMapper = Provide[DIContainer.mapper],
) -> None:
    key = event.key
    modal = event.modal
    file_ext = _file_ext_from_key(key)
    data = storage_client.get(key)

    # default doc thumbnail
    if default_thumb_key := (DEFAULT_MODAL_THUMBNAILS.get(modal)):
        mapper.set(
            from_type=ObjectType.DOC,
            to_type=ObjectType.DOC_THUMBNAIL,
            key=obj_key,
            val=str(default_thumb_key),
        )

    chunks_by_seq: Dict[int, str] = {}
    thumbs_by_seq: Dict[int, str] = {}

    with DOC_FACTORY[event.modal](data, file_ext) as document:
        for obj in document.generate_objs():
            obj_key = _generate_key(key, obj)
            storage_client.add(obj.data, obj_key, obj.type, modal)

            # map doc to doc thumbnail
            if obj.type == ObjectType.DOC_THUMBNAIL:
                mapper.set(
                    from_type=ObjectType.DOC,
                    to_type=ObjectType.DOC_THUMBNAIL,
                    key=key,
                    val=obj_key,
                )
            # map chunk to parent doc
            elif obj.type == ObjectType.CHUNK:
                mapper.set(
                    from_type=ObjectType.CHUNK,
                    to_type=ObjectType.DOC,
                    key=obj_key,
                    val=key,
                )
                chunks_by_seq[obj.seq] = obj_key
            elif obj.type == ObjectType.CHUNK_THUMBNAIL:
                thumbs_by_seq[obj.seq] = obj_key

    # map chunks to chunk thumbnails
    for thumb_seq, thumb_key in thumbs_by_seq.items():
        mapper.set(
            from_type=ObjectType.CHUNK,
            to_type=ObjectType.CHUNK_THUMBNAIL,
            key=chunks_by_seq[thumb_seq],
            val=thumb_key,
        )


@inject
def _insert_default_thumbnails(
    storage_client: StorageClient = Provide[DIContainer.storage_client],
) -> None:
    for icon_path in DEFAULT_MODAL_THUMBNAILS.values():
        storage_client.add(
            icon_path.read_bytes(),
            str(icon_path),
            ObjectType.DOC_THUMBNAIL,
            Modal.IMAGE,
        )


def main():
    _insert_default_thumbnails()
    with RedisConsumer() as consumer:
        consumer.subscribe(DocStored)
        consumer.listen(_handle_doc_callback)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bootstrap()
    main()
