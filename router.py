"""Router endpoints for bot_private_relay management."""

from __future__ import annotations

from src.app.plugin_system.base import BaseRouter

from .service import RelayStateService


class BotPrivateRelayRouter(BaseRouter):
    """Minimal management router for plugin-local inspection."""

    router_name = "bot_private_relay"
    router_description = "Bot private relay management router"
    custom_route_path = "/router/bot_private_relay"

    def register_endpoints(self) -> None:
        """Register plugin-local management endpoints."""

        @self.app.get("/health")
        async def health() -> dict[str, object]:
            return {"ok": True, "plugin": "bot_private_relay"}

        @self.app.get("/stats")
        async def stats() -> dict[str, object]:
            service = RelayStateService(self.plugin)
            return {
                "presence_count": len(service.presence_snapshot()),
                "session_count": len(service.session_snapshot()),
                "memory_candidate_count": len(service.memory_candidate_snapshot()),
                "audit_count": len(service.audit_snapshot()),
            }
