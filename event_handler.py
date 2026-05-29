"""Event handlers for bot private relay."""

# =============================================================================
# 事件处理器模块
# =============================================================================
# 包含两个 EventHandler：
#
# 1. LoopGuardEventHandler（权重 200）
#    - 防护 relay 消息循环和出站泄漏
#    - 消息去重（remember_message）
#    - TTL/hop 超限拦截
#    - terminal + expect_reply 矛盾检测
#    - bot_internal 消息 budget 耗尽拦截
#    - 出站消息只有 bot_relay adapter 可以发送
#    - 将普通聊天消息记录为 proactive 决策线索
#
# 2. GroupReplySuppressionEventHandler（权重 300）
#    - 在群聊中静默特定 bot 的消息
#    - 消息被移入历史但不触发 Chatter 回复
# =============================================================================

from __future__ import annotations

from typing import Any, cast

from src.app.plugin_system.base import BaseEventHandler
from src.core.components.types import EventType
from src.core.models.message import Message
from src.kernel.event import EventDecision

from . import store
from .config import BotPrivateRelayConfig


def _normalized_set(values: list[str]) -> set[str]:
    """Return non-empty lower-case values for config matching.

    将配置列表中的字符串标准化（去空格、转小写、去空值），
    用于平台名和聊天类型的模糊匹配。
    """

    return {str(value).strip().lower() for value in values if str(value).strip()}


def _message_exists_in_history(context: Any, message: Message) -> bool:
    """Return whether the message is already present in stream history.

    检查消息是否已经在聊天流的历史消息列表中（防止重复添加）。
    """

    message_id = str(message.message_id or "")
    return bool(
        message_id
        and any(str(getattr(item, "message_id", "") or "") == message_id for item in getattr(context, "history_messages", []))
    )


def _add_history_once(context: Any, message: Message) -> None:
    """Move a suppressed message into history without duplicating it.

    将被静默的消息移入历史记录。如果消息已存在则跳过（防重复）。
    这样消息仍然可见，但不会触发 Chatter 的回复逻辑。
    """

    if _message_exists_in_history(context, message):
        return
    add_history_message = getattr(context, "add_history_message", None)
    if callable(add_history_message):
        add_history_message(message)


# =============================================================================
# LoopGuardEventHandler - 循环守卫
# =============================================================================
class LoopGuardEventHandler(BaseEventHandler):
    """Protect relay flows against loops and outbound leakage.

    核心职责：防止 relay 消息进入无限循环、消息泄漏到非 bot_relay 平台。
    拦截 ON_MESSAGE_RECEIVED 和 ON_MESSAGE_SENT 两个事件。
    """

    handler_name = "loop_guard"
    handler_description = "Loop guard for bot private relay"
    weight = 200                     # 权重 200，中等优先级
    intercept_message = True         # 可拦截消息
    init_subscribe = [EventType.ON_MESSAGE_RECEIVED, EventType.ON_MESSAGE_SENT]

    async def execute(self, event_name: str, params: dict[str, Any]) -> tuple[EventDecision, dict[str, Any]]:
        """Validate relay messages while preserving param keys.

        根据事件类型分发到不同的处理器。
        """

        if event_name == EventType.ON_MESSAGE_RECEIVED:
            return self._handle_received(params)
        if event_name == EventType.ON_MESSAGE_SENT:
            return self._handle_sent(params)
        return EventDecision.PASS, params

    def _handle_received(self, params: dict[str, Any]) -> tuple[EventDecision, dict[str, Any]]:
        """处理入站消息的循环防护。

        检查项：
        1. 消息去重 → STOP（防止重复处理同一条消息）
        2. hop > ttl → STOP（防止无限循环）
        3. terminal=True 但 expect_reply=True → STOP（协议矛盾）
        4. bot_internal 消息 reply_budget <= 0 → STOP（预算耗尽）
        """

        message = cast(Message | None, params.get("message"))
        if message is None:
            return EventDecision.PASS, params

        # 检查是否是 relay_envelope 格式的消息
        relay_envelope = message.extra.get("relay_envelope") if hasattr(message, "extra") else None
        if not isinstance(relay_envelope, dict):
            # 非 relay 消息 → 可能需要记录为 proactive 线索
            self._record_proactive_chat_hint(message)
            return EventDecision.PASS, params

        # ── 消息去重：检查 message_id 是否已处理过 ──
        message_id = str(relay_envelope.get("message_id") or message.message_id or "")
        if message_id and not store.remember_message(message_id):
            # 重复消息 → 拦截
            return EventDecision.STOP, params

        # ── TTL 检查：hop 超过 ttl → 拦截 ──
        hop = int(relay_envelope.get("hop", 0) or 0)
        ttl = int(relay_envelope.get("ttl", 0) or 0)
        if hop > ttl:
            return EventDecision.STOP, params

        # ── relay_context 检查 ──
        relay_context = message.extra.get("relay_context", {}) if hasattr(message, "extra") else {}
        if isinstance(relay_context, dict):
            # terminal=True 但 expect_reply=True → 矛盾，拦截
            if relay_context.get("terminal") is True and relay_context.get("expect_reply") is True:
                return EventDecision.STOP, params

            # bot_internal 且 reply_budget <= 0 且 expect_reply 不为 False → 拦截
            if message.extra.get("bot_internal") is True and int(relay_context.get("reply_budget", 0) or 0) <= 0 and relay_context.get("expect_reply") is not False:
                return EventDecision.STOP, params

        return EventDecision.SUCCESS, params

    def _handle_sent(self, params: dict[str, Any]) -> tuple[EventDecision, dict[str, Any]]:
        """处理出站消息的泄漏防护。

        关键规则：bot_relay 平台的消息只能通过 bot_private_relay:adapter:bot_relay 发送。
        如果其他 adapter 尝试发送 bot_relay 平台的消息 → 拦截。
        """

        message = cast(Message | None, params.get("message"))
        adapter_signature = str(params.get("adapter_signature") or "")

        if message is None:
            return EventDecision.PASS, params

        # 非 bot_relay 平台的消息 → 放行
        if message.platform != "bot_relay":
            return EventDecision.PASS, params

        # bot_relay 平台的消息，如果不是通过 relay adapter 发送 → 拦截
        if adapter_signature != "bot_private_relay:adapter:bot_relay":
            self._set_continue_send(params, False)
            return EventDecision.STOP, params

        # ── 检查 relay_context 是否存在 ──
        relay_context = message.extra.get("relay_context", {}) if hasattr(message, "extra") else {}
        if not isinstance(relay_context, dict):
            self._set_continue_send(params, False)
            return EventDecision.STOP, params

        # ── 将 relay_context 注入到出站 envelope 中 ──
        envelope = params.get("envelope")
        if isinstance(envelope, dict):
            message_info = envelope.setdefault("message_info", {})
            if isinstance(message_info, dict):
                extra = message_info.setdefault("extra", {})
                if isinstance(extra, dict):
                    extra["relay_context"] = relay_context
                    extra["bot_internal"] = True

        self._set_continue_send(params, True)
        return EventDecision.STOP, params

    def _record_proactive_chat_hint(self, message: Message) -> None:
        """Record ordinary chat messages as proactive decision context.

        将普通聊天消息记录为 proactive 决策的上下文线索。
        proactive 系统使用这些线索来判断是否有合适的时机主动联系伙伴 bot。

        跳过 bot_relay 平台的消息（这些已经是 relay 消息，不需要作为线索）。
        """

        if message.platform == "bot_relay":
            return

        # 如果 proactive 未启用，跳过
        config = getattr(self.plugin, "config", None)
        if isinstance(config, BotPrivateRelayConfig) and not config.proactive.enabled:
            return

        # 跳过空消息
        text = str(message.processed_plain_text or message.content or "").strip()
        if not text:
            return

        # 保存为 proactive 聊天线索
        store.save_proactive_chat_hint(
            store.ProactiveChatHint(
                message_id=message.message_id,
                platform=message.platform,
                chat_type=message.chat_type,
                stream_id=message.stream_id,
                sender_id=message.sender_id,
                sender_name=message.sender_name or message.sender_cardname or "",
                text=text,
            )
        )

    @staticmethod
    def _set_continue_send(params: dict[str, Any], value: bool) -> None:
        """Update continue_send without changing the event param signature.

        设置 continue_send 标志，控制是否继续发送流程。
        """

        if "continue_send" in params:
            params["continue_send"] = value


# =============================================================================
# GroupReplySuppressionEventHandler - 群聊静默
# =============================================================================
class GroupReplySuppressionEventHandler(BaseEventHandler):
    """Receive configured bot messages in groups without triggering replies.

    在某些群聊场景中，我们希望接收特定 bot 的消息，但不触发自动回复。
    此处理器在 Chatter 处理消息前将其从 unread_messages 中移除。

    适用场景：例如在 QQ 群中有多个 bot，避免 bot 之间互相回复造成刷屏。
    """

    handler_name = "group_reply_suppression"
    handler_description = "Suppress default chatter replies to configured group bot senders"
    weight = 300                     # 权重 300，高优先级（在 Chatter 处理前执行）
    intercept_message = False        # 不拦截消息（消息仍会被处理，只是不触发回复）
    init_subscribe = [EventType.ON_CHATTER_STEP]  # 在 Chatter 执行步骤时触发

    async def execute(self, event_name: str, params: dict[str, Any]) -> tuple[EventDecision, dict[str, Any]]:
        """Filter blocked bot messages before chatter consumes unread messages.

        执行逻辑：
        1. 读取配置中的 block_bot_ids 列表
        2. 遍历 unread_messages，将匹配的消息移到 suppressed 列表
        3. 将 suppressed 消息添加到历史记录（不丢失数据）
        4. 更新上下文中的 unread_messages 和 triggering_user_id
        """

        if event_name != EventType.ON_CHATTER_STEP:
            return EventDecision.PASS, params

        # ── 加载配置 ──
        config = getattr(self.plugin, "config", None)
        if not isinstance(config, BotPrivateRelayConfig):
            return EventDecision.PASS, params

        suppression = config.group_reply_suppression
        blocked_bot_ids = {str(bot_id).strip() for bot_id in suppression.blocked_bot_ids if str(bot_id).strip()}

        # 如果未启用或没有需要静默的 bot，直接放行
        if not suppression.enabled or not blocked_bot_ids:
            return EventDecision.PASS, params

        # ── 获取上下文中的未读消息 ──
        context = params.get("context")
        unread_messages = getattr(context, "unread_messages", None)
        if context is None or not isinstance(unread_messages, list) or not unread_messages:
            return EventDecision.PASS, params

        # ── 分离需要静默的消息 ──
        platforms = _normalized_set(suppression.platforms)
        chat_types = _normalized_set(suppression.chat_types)
        kept: list[Message] = []
        suppressed: list[Message] = []

        for message in unread_messages:
            if isinstance(message, Message) and self._should_suppress(message, platforms, chat_types, blocked_bot_ids):
                suppressed.append(message)
            else:
                kept.append(message)

        # 没有需要静默的消息 → 放行
        if not suppressed:
            return EventDecision.PASS, params

        # ── 更新上下文 ──
        context.unread_messages = kept

        for message in suppressed:
            _add_history_once(context, message)  # 移入历史
            store.audit(
                "group_reply_suppressed",
                stream_id=message.stream_id,
                message_id=message.message_id,
                sender_id=message.sender_id,
                reason_code="configured_group_bot",
            )

        # ── 更新触发用户 ID ──
        if kept:
            context.triggering_user_id = kept[-1].sender_id
            params["continue"] = True
            return EventDecision.SUCCESS, params

        # 所有消息都被静默 → 不触发 Chatter
        context.triggering_user_id = None
        params["continue"] = False
        return EventDecision.SUCCESS, params

    @staticmethod
    def _should_suppress(
        message: Message,
        platforms: set[str],
        chat_types: set[str],
        blocked_bot_ids: set[str],
    ) -> bool:
        """Return whether the message should be removed before chatter execution.

        判断条件（全部满足才静默）：
        1. 消息平台在启用列表中
        2. 聊天类型在启用列表中
        3. 发送者 ID 在静默列表中
        """

        platform = str(message.platform or "").strip().lower()
        chat_type = str(message.chat_type or "").strip().lower()
        sender_id = str(message.sender_id or "").strip()
        return platform in platforms and chat_type in chat_types and sender_id in blocked_bot_ids
