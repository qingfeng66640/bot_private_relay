"""Module-level runtime state for bot private relay.

Service instances are not unique in Neo-MoFox, so all mutable runtime state lives
here and nowhere else.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class PresenceRecord:
    """Presence state for a partner bot."""

    bot_id: str
    bot_name: str = ""
    status: str = "offline"
    last_seen: float = field(default_factory=time.time)
    is_known_partner: bool = False


@dataclass(slots=True)
class RelaySession:
    """Minimal Phase 1 relay session state."""

    conversation_id: str
    peer_bot_id: str
    channel: str
    intent: str
    state: str | None = None
    terminal: bool = False
    expect_reply: bool = False
    reply_budget: int = 0
    allowed_responders: list[str] = field(default_factory=list)
    updated_at: float = field(default_factory=time.time)


DEDUP_CACHE: dict[str, float] = {}
PRESENCE_TABLE: dict[str, PresenceRecord] = {}
SESSION_TABLE: dict[str, RelaySession] = {}
AUDIT_LOG: list[dict[str, object]] = []


def reset_state() -> None:
    """Clear all module-level state for plugin-local tests."""

    DEDUP_CACHE.clear()
    PRESENCE_TABLE.clear()
    SESSION_TABLE.clear()
    AUDIT_LOG.clear()


def remember_message(message_id: str, ttl_seconds: int = 3600) -> bool:
    """Record a message id if it has not been seen recently.

    Args:
        message_id: Relay message id.
        ttl_seconds: Expiry window for dedup records.

    Returns:
        ``True`` if this is a new message, otherwise ``False``.
    """

    now = time.time()
    expired = [key for key, seen_at in DEDUP_CACHE.items() if now - seen_at > ttl_seconds]
    for key in expired:
        DEDUP_CACHE.pop(key, None)
    if message_id in DEDUP_CACHE:
        return False
    DEDUP_CACHE[message_id] = now
    return True


def upsert_presence(record: PresenceRecord) -> None:
    """Store presence state."""

    PRESENCE_TABLE[record.bot_id] = record


def save_session(session: RelaySession) -> None:
    """Store relay session state."""

    session.updated_at = time.time()
    SESSION_TABLE[session.conversation_id] = session


def get_session(conversation_id: str) -> RelaySession | None:
    """Return relay session state by id."""

    return SESSION_TABLE.get(conversation_id)


def audit(event: str, **data: object) -> None:
    """Append a lightweight audit entry."""

    AUDIT_LOG.append({"event": event, "time": time.time(), **data})
