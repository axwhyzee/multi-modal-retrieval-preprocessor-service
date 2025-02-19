import subprocess
import tempfile
from abc import ABC, abstractmethod
from enum import StrEnum
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterator, Type, Union

import cv2
from langchain_text_splitters import RecursiveCharacterTextSplitter
from PIL import Image, ImageOps
from scenedetect import AdaptiveDetector, detect, video_splitter  # type: ignore

from services.exceptions import (
    FrameReadError,
    UnableToOpenVideo,
    UnrecognizedFileExtension,
    VideoSplitterUnavailable,
)


class FileExtension(StrEnum):
    PNG = ".png"
    JPG = ".jpg"
    JPEG = ".jpeg"
    MP4 = ".mp4"
    TXT = ".txt"


THUMBNAIL_WIDTH = 200
THUMBNAIL_HEIGHT = 300
THUMBNAIL_FILE_EXTENSION = FileExtension.PNG


class AbstractFileProcessor(ABC):
    def __init__(self, file_data: bytes, file_ext: FileExtension):
        self.file_data = file_data
        self.file_ext = file_ext
        self.has_chunk_thumbnails: bool

    @abstractmethod
    def split_chunks(self) -> Iterator[bytes]:
        raise NotImplementedError

    @abstractmethod
    def generate_thumbnail(self) -> bytes:
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, *_): ...


class TextFileProcessor(AbstractFileProcessor):
    THUMBNAIL_MAX_CHARS = 200
    CHUNK_SIZE = 1000
    CHUNK_OVERLAP = 50

    def __init__(self, file_data: bytes, *args, **kwargs):
        super().__init__(file_data, *args, **kwargs)
        self.text = file_data.decode()
        self.has_chunk_thumbnails = False

    def split_chunks(self) -> Iterator[bytes]:
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.CHUNK_SIZE,
            chunk_overlap=self.CHUNK_OVERLAP,
            length_function=len,
            is_separator_regex=False,
        )

        texts = text_splitter.create_documents([self.text])
        for text in texts:
            yield text.page_content.encode("utf-8")

    def generate_thumbnail(self) -> bytes:
        preview_text = self.text
        if len(preview_text) >= self.THUMBNAIL_MAX_CHARS:
            preview_text += " ..."
        with tempfile.NamedTemporaryFile(suffix=THUMBNAIL_FILE_EXTENSION) as temp_file:
            subprocess.run(
                [
                    "magick",
                    "-size",
                    f"{THUMBNAIL_WIDTH}x{THUMBNAIL_HEIGHT}",
                    "-background",
                    "white",
                    "-fill",
                    "black",
                    f"caption:{preview_text}",
                    temp_file.name,
                ],
                check=True,
            )
            return temp_file.read()


class ImageFileProcessor(AbstractFileProcessor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.has_chunk_thumbnails = True

    def split_chunks(self) -> Iterator[bytes]:
        yield self.file_data

    def generate_thumbnail(self) -> bytes:
        image = Image.open(BytesIO(self.file_data))
        image = ImageOps.fit(
            image,
            (THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT),
            method=0,
            bleed=0.0,
            centering=(0.5, 0.5),
        )  # type: ignore
        image_fmt = THUMBNAIL_FILE_EXTENSION.lstrip(".").upper()  # .png -> PNG
        thumbnail = BytesIO()
        image.save(thumbnail, format=image_fmt)
        return thumbnail.getvalue()


class VideoFileProcessor(AbstractFileProcessor):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.has_chunk_thumbnails = True
        self._temp_file = tempfile.NamedTemporaryFile(suffix=self.file_ext)
        self.file_path = self._temp_file.name
        with open(self.file_path, "wb") as vid_file:
            vid_file.write(self.file_data)

    def split_chunks(self) -> Iterator[bytes]:
        scene_list = detect(self.file_path, AdaptiveDetector())

        with tempfile.TemporaryDirectory() as temp_dir:
            if video_splitter.is_ffmpeg_available():
                video_splitter.split_video_ffmpeg(
                    self.file_path, scene_list, output_dir=temp_dir
                )
            elif video_splitter.is_mkvmerge_available():
                video_splitter.split_video_mkvmerge(
                    self.file_path, scene_list, output_dir=temp_dir
                )
            else:
                raise VideoSplitterUnavailable(
                    "Neither ffmpeg nor mkvmerge found. Try:\n"
                    "`>> brew install ffmpeg` or\n"
                    "`>> brew install mkvtoolnix`"
                )
            for vid_snippet_path in Path(temp_dir).iterdir():
                yield vid_snippet_path.read_bytes()

    def generate_thumbnail(self) -> bytes:
        cap = cv2.VideoCapture(str(self.file_path))
        try:
            if not cap.isOpened():
                raise UnableToOpenVideo(f"Video {self.file_path} could not be opened")

            success, frame = cap.read()
            if not success:
                raise FrameReadError(
                    f"Could not read first frame of video {self.file_path}"
                )

            _, buffer = cv2.imencode(THUMBNAIL_FILE_EXTENSION, frame)
            return buffer.tobytes()
        finally:
            cap.release()

    def __exit__(self, *_):
        if (temp_file := self._temp_file) and not temp_file.closed:
            temp_file.close()


_FILE_PROCESSORS: Dict[FileExtension, Type[AbstractFileProcessor]] = {
    FileExtension.TXT: TextFileProcessor,
    FileExtension.JPEG: ImageFileProcessor,
    FileExtension.JPG: ImageFileProcessor,
    FileExtension.PNG: ImageFileProcessor,
    FileExtension.MP4: VideoFileProcessor,
}


def processor_factory(
    file_data: bytes, file_ext: Union[str, FileExtension]
) -> AbstractFileProcessor:
    if isinstance(file_ext, str):
        if file_ext not in FileExtension._value2member_map_:
            raise UnrecognizedFileExtension(f"Unrecognized extension <{file_ext}>")
        file_ext = FileExtension(file_ext)
    return _FILE_PROCESSORS[file_ext](file_data, file_ext)
