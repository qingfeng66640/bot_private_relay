"""bot_private_relay 管理的路由端点。"""

# =============================================================================
# BotPrivateRelayRouter - 管理端点
# =============================================================================
# 提供 HTTP REST 端点用于简单的运行状态查询。
# 挂载在 /router/bot_private_relay 路径下。
#
# 端点列表：
# - GET /health  → 健康检查
# - GET /stats   → 运行统计
# =============================================================================

from __future__ import annotations

from src.core.components.base.router import BaseRouter as BaseRouterComponent

class BotPrivateRelayRouter(BaseRouterComponent):
    """用于插件本地检查的最简管理路由。"""

    router_name = "bot_private_relay"
    router_description = "Bot 私有中继管理路由"
    custom_route_path = "/router/bot_private_relay"

    def register_endpoints(self) -> None:
        """注册插件本地管理端点。

        注册两个基础管理端点，用于快速检查插件运行状态。
        """

        @self.app.get("/health")
        async def health() -> dict[str, object]:
            """健康检查端点：返回插件是否正常加载。"""
            return {"ok": True, "plugin": "bot_private_relay"}

        @self.app.get("/stats")
        async def stats() -> dict[str, object]:
            """统计端点：返回插件基本运行信息。"""
            return {
                "ok": True,
                "plugin": "bot_private_relay",
                "platform": "bot_relay",
                "debug_surface": "limited",
            }
