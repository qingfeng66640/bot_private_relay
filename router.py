"""Router endpoints for bot_private_relay management."""

from __future__ import annotations

from src.app.plugin_system.base import BaseRouter

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
            return {
                "ok": True,
                "plugin": "bot_private_relay",
                "platform": "bot_relay",
                "debug_surface": "limited",
            }
