"""Actor identity: distinguish self / owner / other by UID.

UID is the stable key. Display name travels alongside for humans and logs;
it can change, so it must never be the sole key.
"""

from dataclasses import dataclass
from enum import Enum


class ActorKind(str, Enum):
    SELF = "self"
    OWNER = "owner"
    OTHER = "other"


@dataclass(frozen=True)
class Actor:
    uid: int
    display_name: str
    kind: ActorKind


def classify_actor(uid: int, display_name: str, *, bot_uid: int, owner_uid: int) -> Actor:
    """Classify a Discord user ID. Self wins on misconfiguration (bot == owner)."""
    if uid == bot_uid:
        kind = ActorKind.SELF
    elif uid == owner_uid:
        kind = ActorKind.OWNER
    else:
        kind = ActorKind.OTHER
    return Actor(uid=uid, display_name=display_name, kind=kind)
