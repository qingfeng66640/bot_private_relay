"""Memory-candidate projection for relay messages."""

from __future__ import annotations

from src.app.plugin_system.base import BaseService

from . import store


class MemoryBridgeService(BaseService):
    """Expose projected memory candidates within plugin boundary only."""

    service_name = "relay_memory_bridge"
    service_description = "Bot private relay memory bridge"
    version = "0.1.0"

    def list_candidates(self) -> dict[str, store.RelayMemoryCandidate]:
        """Return projected memory candidates."""

        return dict(store.RELAY_MEMORY_CANDIDATES)
