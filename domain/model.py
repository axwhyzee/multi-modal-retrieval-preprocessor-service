import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterator, Type

import cv2
from event_core.domain.types import FileExt, Modal, ObjectType
from langchain_text_splitters import RecursiveCharacterTextSplitter
from PIL import Image, ImageOps
from scenedetect import AdaptiveDetector, detect, video_splitter  # type: ignore

from domain.exceptions import (
    FrameReadError,
    UnableToOpenVideo,
    VideoSplitterUnavailable,
)

THUMB_WIDTH = 300
THUMB_HEIGHT = 200
IMG_CHUNK_WIDTH = 400
IMG_CHUNK_HEIGHT = 400
IMG_EXT = FileExt.PNG


@dataclass
class Obj:
    seq: int
    data: bytes
    type: ObjectType
    file_ext: FileExt


def _crop_img(data: bytes, width: int, height: int) -> bytes:
    image = Image.open(BytesIO(data))
    image = ImageOps.fit(
        image,
        (width, height),
        method=0,
        bleed=0.0,
        centering=(0.5, 0.5),
    )  # type: ignore
    image_fmt = IMG_EXT.lstrip(".").upper()  # .png -> PNG
    thumb = BytesIO()
    image.save(thumb, format=image_fmt)
    return thumb.getvalue()


def _img_downsize(data: bytes) -> bytes:
    return _crop_img(data, IMG_CHUNK_WIDTH, IMG_CHUNK_HEIGHT)


def img_thumbnail(data: bytes) -> bytes:
    return _crop_img(data, THUMB_WIDTH, THUMB_HEIGHT)


def _extract_first_frame(
    video_path: str, frame_ext: FileExt = IMG_EXT
) -> bytes:
    cap = cv2.VideoCapture(video_path)
    try:
        if not cap.isOpened():
            raise UnableToOpenVideo(f"Video {video_path} could not be opened")

        success, frame = cap.read()
        if not success:
            raise FrameReadError(
                f"Could not read first frame of video {video_path}"
            )
        _, buffer = cv2.imencode(frame_ext, frame)
        return buffer.tobytes()
    finally:
        cap.release()


class AbstractDoc(ABC):
    def __init__(self, data: bytes, file_ext: FileExt):
        self._data = data
        self._file_ext = file_ext

    @abstractmethod
    def generate_objs(self) -> Iterator[Obj]:
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, *_): ...


class TextDoc(AbstractDoc):
    CHUNK_SIZE = 300
    CHUNK_OVERLAP = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._text = self._data.decode("utf-8")

    def generate_objs(self) -> Iterator[Obj]:
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.CHUNK_SIZE,
            chunk_overlap=self.CHUNK_OVERLAP,
            length_function=len,
            separators=["\n\n", "\n", ". ", ",", " ", ""],
            is_separator_regex=False,
            keep_separator=False,
        )
        texts = splitter.create_documents([self._text])
        for i, doc in enumerate(texts, start=1):
            text = doc.page_content.encode("utf-8")
            yield Obj(
                seq=i, data=text, type=ObjectType.CHUNK, file_ext=FileExt.TXT
            )


class ImageDoc(AbstractDoc):

    def generate_objs(self) -> Iterator[Obj]:
        yield Obj(
            seq=0,
            data=img_thumbnail(self._data),
            type=ObjectType.DOC_THUMBNAIL,
            file_ext=IMG_EXT,
        )
        yield Obj(
            seq=1,
            data=_img_downsize(self._data),
            type=ObjectType.CHUNK,
            file_ext=self._file_ext,
        )


class VideoDoc(AbstractDoc):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # persist video to temp file
        self._temp_file = tempfile.NamedTemporaryFile(suffix=self._file_ext)
        self._temp_file_path = self._temp_file.name
        with open(self._temp_file_path, "wb") as vid_file:
            vid_file.write(self._data)

    def generate_objs(self) -> Iterator[Obj]:
        yield Obj(
            seq=0,
            data=self._get_thumb(),
            type=ObjectType.DOC_THUMBNAIL,
            file_ext=IMG_EXT,
        )
        yield from self._chunk()

    def _chunk(self) -> Iterator[Obj]:
        scene_list = detect(self._temp_file_path, AdaptiveDetector())

        with tempfile.TemporaryDirectory() as temp_dir:
            if video_splitter.is_ffmpeg_available():
                video_splitter.split_video_ffmpeg(
                    self._temp_file_path, scene_list, output_dir=temp_dir
                )
            elif video_splitter.is_mkvmerge_available():
                video_splitter.split_video_mkvmerge(
                    self._temp_file_path, scene_list, output_dir=temp_dir
                )
            else:
                raise VideoSplitterUnavailable(
                    "Neither ffmpeg nor mkvmerge found. Try:\n"
                    "`>> brew install ffmpeg` or\n"
                    "`>> brew install mkvtoolnix`"
                )
            video_paths = Path(temp_dir).iterdir()
            for i, video_path in enumerate(video_paths, start=1):
                frame = _extract_first_frame(str(video_path), IMG_EXT)
                frame_thumb = img_thumbnail(frame)
                yield Obj(
                    seq=i,
                    data=_img_downsize(frame),
                    type=ObjectType.CHUNK,
                    file_ext=IMG_EXT,
                )
                yield Obj(
                    seq=i,
                    data=frame_thumb,
                    type=ObjectType.CHUNK_THUMBNAIL,
                    file_ext=IMG_EXT,
                )

    def _get_thumb(self) -> bytes:
        frame = _extract_first_frame(self._temp_file_path)
        frame_thumb = img_thumbnail(frame)
        return frame_thumb

    def __exit__(self, *_):
        self._temp_file.close()


DOC_FACTORY: Dict[Modal, Type[AbstractDoc]] = {
    Modal.TEXT: TextDoc,
    Modal.IMAGE: ImageDoc,
    Modal.VIDEO: VideoDoc,
}
