"""Stateless service wrappers for relay state."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

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

    def audit_snapshot(self) -> list[dict[str, object]]:
        """Return audit log snapshot."""

        return list(store.AUDIT_LOG)

    def transaction_log_snapshot(self) -> dict[str, store.RelayTransactionRecord]:
        """Return transaction log snapshot."""

        return dict(store.TRANSACTION_LOG)

    def export_debug_snapshot(self, output_dir: str | Path) -> Path:
        """Persist a plugin-local debug snapshot.

        This is a plugin-local optional persistence helper only. It does not
        replace future framework-approved persistence paths.
        """

        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        target = path / "relay_debug_snapshot.json"
        payload = {
            "presence": {
                key: asdict(value) for key, value in store.PRESENCE_TABLE.items()
            },
            "sessions": {
                key: asdict(value) for key, value in store.SESSION_TABLE.items()
            },
            "transactions": {
                key: asdict(value) for key, value in store.TRANSACTION_LOG.items()
            },
            "memory_candidates": {
                key: asdict(value) for key, value in store.RELAY_MEMORY_CANDIDATES.items()
            },
            "audit": list(store.AUDIT_LOG),
        }
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return target
