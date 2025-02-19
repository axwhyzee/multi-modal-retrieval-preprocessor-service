import json
from dataclasses import asdict, dataclass
from typing import Any, Dict


@dataclass
class Event:
    def jsonify(self) -> str:
        return json.dumps(asdict(self))

    def __post_init__(self):
        self.channel: str = self.__class__.__name__


@dataclass
class DocumentUploaded(Event):
    document_path: str


@dataclass
class ObjectPersisted(Event):
    document_path: str
    object_path: str
    object_thumbnail_path: str
