from dataclasses import dataclass
from typing import Dict, Type


class Event: ...


@dataclass
class DocumentUploaded(Event):
    document_path: str


@dataclass
class ObjectPersisted(Event):
    document_path: str
    object_path: str


CHANNELS: Dict[Type[Event], str] = {}
EVENTS: Dict[str, Type[Event]] = {}


def _register_event(event: Type[Event]) -> None:
    CHANNELS[event] = event.__name__
    EVENTS[event.__name__] = event


_register_event(ObjectPersisted)
_register_event(DocumentUploaded)
