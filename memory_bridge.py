"""Memory-candidate projection for relay messages."""

# =============================================================================
# MemoryBridgeService - 记忆候选桥接服务
# =============================================================================
# 将 relay 对话中的高价值消息投影为"记忆候选"（RelayMemoryCandidate），
# 供外部记忆系统（如长期记忆插件）消费。
#
# memories 的筛选在 session.py 的 maybe_create_memory_candidate() 中进行：
# - 只处理 social 和 transaction channel 的消息
# - 消息长度 >= 12 字符
# - 根据消息长度计算 score（分值越高越值得记忆）
# =============================================================================

from __future__ import annotations

from src.app.plugin_system.base import BaseService

from . import store


class MemoryBridgeService(BaseService):
    """Expose projected memory candidates within plugin boundary only.

    服务名：relay_memory_bridge
    用途：对外暴露 relay 对话中产生的记忆候选数据，供其他插件或框架组件读取。
    """

    service_name = "relay_memory_bridge"
    service_description = "Bot private relay memory bridge"
    version = "0.1.0"

    def list_candidates(self) -> dict[str, store.RelayMemoryCandidate]:
        """Return projected memory candidates.

        返回当前所有的记忆候选。调用方可以遍历这些候选，
        决定哪些值得长期存储。
        """

        return dict(store.RELAY_MEMORY_CANDIDATES)
