"""系统通道短路处理。

本模块是内部辅助模块，刻意不作为独立的注册组件。
"""

# =============================================================================
# SystemChannelHandler - 系统通道短路处理器
# =============================================================================
# 负责处理 system channel 的消息，这些消息不需要进入 LLM 对话流程。
# 系统消息直接在这里被"消费"，不会传递到 Chatter 层。
#
# 支持的系统消息类型：
# - presence_update  → 更新伙伴在线状态
# - cancel/close/error/ack/heartbeat/typing → 记录审计日志
#
# 这是一个内部辅助类，不是独立的注册组件。
# =============================================================================

from __future__ import annotations

from . import store
from .envelope import RelayEnvelope
from .presence import PresenceManager


class SystemChannelHandler:
    """处理中继系统信封，不进入 LLM 流程。"""

    def __init__(self, presence_manager: PresenceManager) -> None:
        self.presence_manager = presence_manager

    def handle(self, envelope: RelayEnvelope) -> bool:
        """处理系统信封。

        处理系统信道的消息。返回 True 表示消息被消费，不需要进一步处理。

        处理逻辑：
        - presence_update：调用 PresenceManager 更新伙伴的在线状态
        - 其他系统意图（cancel/close/error/ack/heartbeat/typing）：记录审计日志

        Returns:
            如果信封被系统短路路径消费，返回 ``True``。
        """

        if envelope.channel != "system":
            return False

        if envelope.intent == "presence_update":
            # 在线状态更新 → 更新 PresenceManager 中的伙伴状态记录
            self.presence_manager.update_from_envelope(envelope)

        elif envelope.intent in {"cancel", "close", "error", "ack", "heartbeat", "typing"}:
            # 其他系统事件 → 记录审计日志
            store.audit("system_event", intent=envelope.intent, from_bot=envelope.from_bot)

        # 所有系统消息都被短路消费，不会继续传递
        return True
