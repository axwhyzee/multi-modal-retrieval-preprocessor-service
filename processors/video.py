import tempfile
from pathlib import Path
from typing import Iterator

import cv2
from event_core.adapters.services.meta import Meta
from event_core.domain.types import Asset, Element, FileExt
from scenedetect import AdaptiveDetector, detect, video_splitter  # type: ignore

from config import IMG_EXT
from processors.base import AbstractProcessor
from processors.common import Unit, resize_to_thumb
from processors.exceptions import (
    FrameReadError,
    UnableToOpenVideo,
    VideoSplitterUnavailable,
)


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


class VideoProcessor(AbstractProcessor):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # persist video to temp file
        self._temp_file = tempfile.NamedTemporaryFile(suffix=self._file_ext)
        self._temp_file_path = self._temp_file.name
        with open(self._temp_file_path, "wb") as vid_file:
            vid_file.write(self._data)

    def __call__(self) -> Iterator[Unit]:
        yield Unit(
            seq=0,
            data=self._get_thumb(),
            type=Asset.DOC_THUMBNAIL,
            file_ext=IMG_EXT,
        )
        yield from self._chunk()

    def _process_scene(
        self, seq: int, seconds: float, video_path: str
    ) -> Iterator[Unit]:
        frame = _extract_first_frame(video_path, IMG_EXT)
        yield Unit(
            seq=seq,
            data=frame,
            type=Element.IMAGE,
            file_ext=IMG_EXT,
            meta={Meta.FRAME_SECONDS: seconds},
        )
        yield Unit(
            seq=seq,
            data=resize_to_thumb(frame),
            type=Asset.ELEM_THUMBNAIL,
            file_ext=IMG_EXT,
        )

    def _chunk(self) -> Iterator[Unit]:
        scene_list = detect(self._temp_file_path, AdaptiveDetector())

        with tempfile.TemporaryDirectory() as temp_dir:
            # split video into scenes
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

            # process scenes
            if video_paths := list(Path(temp_dir).iterdir()):
                for seq, video_path in enumerate(video_paths, start=1):
                    # E.g., tmpazhewchn-Scene-001.mp4
                    scene_idx = int(video_path.stem.rsplit("-", 1)[1]) - 1
                    scene_seconds = scene_list[scene_idx][0].get_seconds()
                    yield from self._process_scene(
                        seq, scene_seconds, str(video_path)
                    )
            else:
                # single scene video
                yield from self._process_scene(1, 0, self._temp_file_path)

    def _get_thumb(self) -> bytes:
        frame = _extract_first_frame(self._temp_file_path)
        frame_thumb = resize_to_thumb(frame)
        return frame_thumb

    def __exit__(self, *_):
        self._temp_file.close()
