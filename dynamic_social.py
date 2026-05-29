"""Dynamic social quota and proactive-contact tools."""

# =============================================================================
# 动态社交联系模块
# =============================================================================
# 提供两个核心功能：
#
# 1. DynamicSocialLimiter - 动态社交配额限制器
#    对主动联系其他 bot 的行为实施频率限制，防止刷屏。
#    支持三种限制维度：
#    - 每日上限（max_per_day）
#    - 每小时上限（max_per_hour）
#    - 冷却时间（cooldown_seconds）
#    配额数据存储在 store 模块的全局字典中。
#
# 2. RelaySocialContactTool - 社交联系 Tool
#    供外部插件（如 todo_plugin）使用的 Tool，允许通过 relay 的
#    social channel 主动联系其他 bot。此 Tool 通过 plugin.py 中的
#    on_plugin_loaded 钩子注册到 todo_plugin 的 registry 中。
# =============================================================================

from __future__ import annotations

import time
from typing import Annotated
from uuid import uuid4

from src.app.plugin_system.base import BaseTool
from src.core.models.message import Message, MessageType
from src.core.models.stream import ChatStream
from src.core.transport.message_send import get_message_sender

from . import store
from .config import BotPrivateRelayConfig


# 全局注册的 relay 配置，供其他插件（如 todo_plugin）中的 Tool 使用
_REGISTERED_RELAY_CONFIG: BotPrivateRelayConfig | None = None


def register_relay_config(config: BotPrivateRelayConfig) -> None:
    """Expose relay config for tools invoked from other plugins.

    将 relay 配置注册到模块全局变量。因为 Tool 从外部插件调用时，
    无法通过 self.plugin.config 获取配置，所以需要一个全局注册机制。
    """

    global _REGISTERED_RELAY_CONFIG
    _REGISTERED_RELAY_CONFIG = config


# =============================================================================
# DynamicSocialLimiter - 动态社交配额限制器
# =============================================================================
class DynamicSocialLimiter:
    """Enforce per-bot proactive social quotas in runtime memory.

    对每个目标 bot 的主动社交联系行为实施频率限制。
    所有限制数据存储在 store 模块的全局字典中（运行时内存），
    插件重启后重置。
    """

    def __init__(self, config: BotPrivateRelayConfig) -> None:
        self.config = config

    def allow(self, *, target_bot_id: str, source: str) -> tuple[bool, str]:
        """Consume one proactive social quota if allowed.

        检查是否允许向目标 bot 发送主动社交消息。如果允许，消耗一次配额。

        检查顺序（短路逻辑）：
        1. dynamic_social 是否启用
        2. 触发来源是否被允许（impulse/event/user_command）
        3. 目标 bot 是否在允许列表中（或 default_allow_all_bots=True）
        4. 冷却时间是否已过
        5. 每日配额是否耗尽
        6. 每小时配额是否耗尽

        Args:
            target_bot_id: 目标 bot 的 ID。
            source: 触发来源：impulse（突发奇想）/ event（事件）/ user_command（用户指令）。

        Returns:
            (是否允许, 状态码)
        """

        dynamic = self.config.dynamic_social

        # ── 1. 动态社交是否启用 ──
        if not dynamic.enabled:
            return False, "dynamic_social_disabled"

        # ── 2. 触发来源检查 ──
        if source == "impulse" and not dynamic.impulse_enabled:
            return False, "impulse_disabled"
        if source == "event" and not dynamic.event_triggers_enabled:
            return False, "event_trigger_disabled"
        if source == "user_command" and not dynamic.user_command_triggers_enabled:
            return False, "user_command_trigger_disabled"

        # ── 3. 目标 bot 是否允许 ──
        if not dynamic.default_allow_all_bots and self.config.partner_by_id(target_bot_id) is None:
            return False, "target_not_allowed"

        # ── 4. 加载配额配置（优先使用每目标的覆盖配置） ──
        quota = self.config.social_quota_by_id(target_bot_id)
        max_per_day = quota.max_per_day if quota is not None else dynamic.default_max_per_day
        max_per_hour = quota.max_per_hour if quota is not None else dynamic.default_max_per_hour
        cooldown_seconds = quota.cooldown_seconds if quota is not None else dynamic.default_cooldown_seconds

        now = time.time()

        # ── 5. 冷却时间检查 ──
        cooldown_until = store.DYNAMIC_SOCIAL_COOLDOWNS.get(target_bot_id, 0.0)
        if cooldown_until > now:
            return False, "social_cooldown_active"

        # ── 6. 每日配额检查 ──
        day_key = time.strftime("%Y-%m-%d", time.localtime(now))
        day_count_key = (target_bot_id, day_key)
        if store.DYNAMIC_SOCIAL_DAILY_COUNTS.get(day_count_key, 0) >= max_per_day:
            return False, "daily_social_quota_exhausted"

        # ── 7. 每小时配额检查 ──
        hour_key = time.strftime("%Y-%m-%dT%H", time.localtime(now))
        hour_count_key = (target_bot_id, hour_key)
        if store.DYNAMIC_SOCIAL_HOURLY_COUNTS.get(hour_count_key, 0) >= max_per_hour:
            return False, "hourly_social_quota_exhausted"

        # ── 所有检查通过 → 消耗配额 ──
        store.DYNAMIC_SOCIAL_DAILY_COUNTS[day_count_key] = store.DYNAMIC_SOCIAL_DAILY_COUNTS.get(day_count_key, 0) + 1
        store.DYNAMIC_SOCIAL_HOURLY_COUNTS[hour_count_key] = store.DYNAMIC_SOCIAL_HOURLY_COUNTS.get(hour_count_key, 0) + 1
        if cooldown_seconds > 0:
            store.DYNAMIC_SOCIAL_COOLDOWNS[target_bot_id] = now + cooldown_seconds

        return True, "ok"


# =============================================================================
# RelaySocialContactTool - 社交联系 Tool
# =============================================================================
class RelaySocialContactTool(BaseTool):
    """Contact another bot through the relay social channel only.

    通过 bot_relay 的 social channel 联系另一个 bot。
    主要供 todo_plugin 在执行 bot 待办事项时使用。
    也支持其他插件通过此 Tool 发起社交联系。

    注意：此 Tool 仅使用 social channel，不适用于 transaction/system。
    """

    tool_name = "relay_social_contact"
    tool_description = (
        "通过 bot_relay 的 social channel 联系另一个 bot。"
        "用于 todo 插件执行 bot 待办时联系其他 bot；禁止用于 transaction/system。"
    )
    chatter_allow = ["bot_relay_chatter"]
    associated_platforms = ["bot_relay"]

    async def execute(
        self,
        target_bot_id: Annotated[str, "目标 bot_id"],
        message: Annotated[str, "要通过社交链路发送给对端 bot 的自然语言消息"],
        reason: Annotated[str, "主动联系原因，例如 todo_execution、event、impulse"] = "todo_execution",
        conversation_id: Annotated[str, "可选：沿用来源 relay 事务的 conversation_id"] = "",
        trace_id: Annotated[str, "可选：沿用来源 relay 事务的 trace_id"] = "",
    ) -> tuple[bool, str]:
        """Send a proactive social message to a target bot.

        执行流程：
        1. 获取配置（从 self.plugin.config 或全局注册的 _REGISTERED_RELAY_CONFIG）
        2. 验证参数（target_bot_id 和 message 不能为空）
        3. 解析伙伴信息
        4. 检查配额（通过 DynamicSocialLimiter）
        5. 构建 relay_context
        6. 创建 Message 并发送
        """

        # ── 获取配置 ──
        config = getattr(self.plugin, "config", None)
        if not isinstance(config, BotPrivateRelayConfig):
            config = _REGISTERED_RELAY_CONFIG
        if not isinstance(config, BotPrivateRelayConfig):
            return False, "relay_config_unavailable"

        # ── 参数验证 ──
        target_bot_id = target_bot_id.strip()
        message = message.strip()
        if not target_bot_id or not message:
            return False, "invalid_social_contact_payload"

        # ── 解析伙伴信息 ──
        partner = config.partner_by_id(target_bot_id)
        if partner is None:
            if not config.dynamic_social.default_allow_all_bots:
                return False, "unknown_social_target"
            partner_name = target_bot_id
        else:
            partner_name = partner.bot_name

        # ── 配额检查 ──
        ok, code = DynamicSocialLimiter(config).allow(
            target_bot_id=target_bot_id,
            source=self._source_from_reason(reason)
        )
        if not ok:
            return False, code

        # ── 构建 relay_context ──
        stream_id = ChatStream.generate_stream_id("bot_relay", user_id=target_bot_id)
        relay_context = {
            "channel": "social",
            "intent": "say",
            "peer_bot_id": target_bot_id,
            "peer_bot_name": partner_name,
            "conversation_id": conversation_id.strip() or uuid4().hex,
            "trace_id": trace_id.strip(),
            "phase": "opening",
            "terminal": False,
            "expect_reply": True,
            "reply_budget": config.relay.default_reply_budget,
            "allowed_responders": [target_bot_id],
            "dynamic_social": True,
            "dynamic_social_reason": reason,
        }

        # ── 构建并发送消息 ──
        relay_message = Message(
            message_id=f"relay-social-contact-{uuid4().hex}",
            content=message,
            processed_plain_text=message,
            message_type=MessageType.TEXT,
            platform="bot_relay",
            chat_type="private",
            stream_id=stream_id,
            target_user_id=target_bot_id,
            target_user_name=partner_name,
            relay_context=relay_context,
        )
        sent = await get_message_sender().send_message(
            relay_message,
            "bot_private_relay:adapter:bot_relay",
        )
        if not sent:
            return False, "social_contact_send_failed"
        return True, f"social contact sent to {target_bot_id}"

    @staticmethod
    def _source_from_reason(reason: str) -> str:
        """Map free-form reasons to quota source buckets.

        将自由文本的原因映射到配额来源桶。
        - impulse / 突发奇想 → "impulse"
        - event / 事件 → "event"
        - user_command / command / 用户指令 → "user_command"
        - 其他 → "event"（默认）
        """

        normalized = reason.strip().lower()
        if normalized in {"impulse", "突发奇想"}:
            return "impulse"
        if normalized in {"event", "事件"}:
            return "event"
        if normalized in {"user_command", "command", "用户指令"}:
            return "user_command"
        return "event"
