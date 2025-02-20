from enum import StrEnum


class FileExt(StrEnum):
    PNG = ".png"
    JPG = ".jpg"
    JPEG = ".jpeg"
    MP4 = ".mp4"
    TXT = ".txt"


class ObjType(StrEnum):
    CHUNK = "CHUNK"
    CHUNK_THUMB = "CHUNK_THUMBNAIL"
    DOC_THUMB = "DOC_THUMBNAIL"
