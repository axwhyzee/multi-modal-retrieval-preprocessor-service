import subprocess
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterator, Type, Union

import cv2
from event_core.domain.types import ObjectType
from langchain_text_splitters import RecursiveCharacterTextSplitter
from PIL import Image, ImageOps
from scenedetect import AdaptiveDetector, detect, video_splitter  # type: ignore

from domain.exceptions import (
    FrameReadError,
    UnableToOpenVideo,
    UnrecognizedFileExt,
    VideoSplitterUnavailable,
)


class FileExt(StrEnum):
    PNG = ".png"
    JPG = ".jpg"
    JPEG = ".jpeg"
    MP4 = ".mp4"
    TXT = ".txt"


THUMB_WIDTH = 200
THUMB_HEIGHT = 300
THUMB_EXT = FileExt.PNG


@dataclass
class RepoObject:
    data: bytes
    type: ObjectType
    file_ext: FileExt


def _get_img_thumb(data: bytes) -> bytes:
    image = Image.open(BytesIO(data))
    image = ImageOps.fit(
        image,
        (THUMB_WIDTH, THUMB_HEIGHT),
        method=0,
        bleed=0.0,
        centering=(0.5, 0.5),
    )  # type: ignore
    image_fmt = THUMB_EXT.lstrip(".").upper()  # .png -> PNG
    thumb = BytesIO()
    image.save(thumb, format=image_fmt)
    return thumb.getvalue()


def _extract_first_frame(video_path: str, frame_ext: FileExt) -> bytes:
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

    def generate_objs(self) -> Iterator[RepoObject]:
        yield RepoObject(
            data=self._get_thumb(),
            type=ObjectType.DOC_THUMBNAIL,
            file_ext=THUMB_EXT,
        )
        yield from self._chunk()

    @abstractmethod
    def _chunk(self) -> Iterator[RepoObject]:
        raise NotImplementedError

    @abstractmethod
    def _get_thumb(self) -> bytes:
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, *_): ...


class TextDoc(AbstractDoc):
    THUMB_MAX_CHARS = 200
    CHUNK_SIZE = 1000
    CHUNK_OVERLAP = 50

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._text = self._data.decode("utf-8")

    def _chunk(self) -> Iterator[RepoObject]:
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.CHUNK_SIZE,
            chunk_overlap=self.CHUNK_OVERLAP,
            length_function=len,
        )
        texts = splitter.create_documents([self._text])
        for doc in texts:
            text = doc.page_content.encode("utf-8")
            yield RepoObject(
                data=text, type=ObjectType.CHUNK, file_ext=self._file_ext
            )

    def _get_thumb(self) -> bytes:
        if len(text := self._text) > self.THUMB_MAX_CHARS:
            text = text[: self.THUMB_MAX_CHARS] + " ..."

        with tempfile.NamedTemporaryFile(suffix=THUMB_EXT) as temp_file:
            subprocess.run(
                [
                    "magick",
                    "-size",
                    f"{THUMB_WIDTH}x{THUMB_HEIGHT}",
                    "-background",
                    "white",
                    "-fill",
                    "black",
                    f"caption:{text}",
                    temp_file.name,
                ],
                check=True,
            )
            return temp_file.read()


class ImageDoc(AbstractDoc):

    def _chunk(self) -> Iterator[RepoObject]:
        yield RepoObject(
            data=self._data, type=ObjectType.CHUNK, file_ext=self._file_ext
        )

    def _get_thumb(self) -> bytes:
        return _get_img_thumb(self._data)


class VideoDoc(AbstractDoc):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # persist video to temp file
        self._temp_file = tempfile.NamedTemporaryFile(suffix=self._file_ext)
        self._temp_file_path = self._temp_file.name
        with open(self._temp_file_path, "wb") as vid_file:
            vid_file.write(self._data)

    def _chunk(self) -> Iterator[RepoObject]:
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
            for video_path in Path(temp_dir).iterdir():
                frame = _extract_first_frame(str(video_path), THUMB_EXT)
                frame_thumb = _get_img_thumb(frame)
                yield RepoObject(
                    data=frame, type=ObjectType.CHUNK, file_ext=THUMB_EXT
                )
                yield RepoObject(
                    data=frame_thumb,
                    type=ObjectType.CHUNK_THUMBNAIL,
                    file_ext=THUMB_EXT,
                )

    def _get_thumb(self) -> bytes:
        frame = _extract_first_frame(self._temp_file_path, THUMB_EXT)
        frame_thumb = _get_img_thumb(frame)
        return frame_thumb

    def __exit__(self, *_):
        self._temp_file.close()


_DOCS: Dict[FileExt, Type[AbstractDoc]] = {
    FileExt.TXT: TextDoc,
    FileExt.JPEG: ImageDoc,
    FileExt.JPG: ImageDoc,
    FileExt.PNG: ImageDoc,
    FileExt.MP4: VideoDoc,
}


def document_factory(
    file_data: bytes, file_ext: Union[str, FileExt]
) -> AbstractDoc:
    if isinstance(file_ext, str):
        if file_ext not in FileExt._value2member_map_:
            raise UnrecognizedFileExt(f"Unrecognized extension <{file_ext}>")
        file_ext = FileExt(file_ext)
    return _DOCS[file_ext](file_data, file_ext)
