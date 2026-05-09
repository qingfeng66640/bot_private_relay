"""Stateless service wrappers for relay state."""

from __future__ import annotations

from src.core.components.base import BaseService

from . import store


class RelayStateService(BaseService):
    """Expose relay state without owning any instance-local runtime state."""

    service_name = "relay_state"
    service_description = "Bot private relay state access"
    version = "0.1.0"

    def presence_snapshot(self) -> dict[str, store.PresenceRecord]:
        """Return current presence table."""

        return dict(store.PRESENCE_TABLE)

    def session_snapshot(self) -> dict[str, store.RelaySession]:
        """Return current session table."""

        return dict(store.SESSION_TABLE)

    def memory_candidate_snapshot(self) -> dict[str, store.RelayMemoryCandidate]:
        """Return projected memory candidates."""

        return dict(store.RELAY_MEMORY_CANDIDATES)
