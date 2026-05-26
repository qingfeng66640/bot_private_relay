"""Dynamic social quota and proactive-contact tools."""

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


_REGISTERED_RELAY_CONFIG: BotPrivateRelayConfig | None = None


def register_relay_config(config: BotPrivateRelayConfig) -> None:
    """Expose relay config for tools invoked from other plugins."""

    global _REGISTERED_RELAY_CONFIG
    _REGISTERED_RELAY_CONFIG = config


class DynamicSocialLimiter:
    """Enforce per-bot proactive social quotas in runtime memory."""

    def __init__(self, config: BotPrivateRelayConfig) -> None:
        self.config = config

    def allow(self, *, target_bot_id: str, source: str) -> tuple[bool, str]:
        """Consume one proactive social quota if allowed."""

        dynamic = self.config.dynamic_social
        if not dynamic.enabled:
            return False, "dynamic_social_disabled"
        if source == "impulse" and not dynamic.impulse_enabled:
            return False, "impulse_disabled"
        if source == "event" and not dynamic.event_triggers_enabled:
            return False, "event_trigger_disabled"
        if source == "user_command" and not dynamic.user_command_triggers_enabled:
            return False, "user_command_trigger_disabled"
        if not dynamic.default_allow_all_bots and self.config.partner_by_id(target_bot_id) is None:
            return False, "target_not_allowed"

        quota = self.config.social_quota_by_id(target_bot_id)
        max_per_day = quota.max_per_day if quota is not None else dynamic.default_max_per_day
        max_per_hour = quota.max_per_hour if quota is not None else dynamic.default_max_per_hour
        cooldown_seconds = quota.cooldown_seconds if quota is not None else dynamic.default_cooldown_seconds
        now = time.time()
        cooldown_until = store.DYNAMIC_SOCIAL_COOLDOWNS.get(target_bot_id, 0.0)
        if cooldown_until > now:
            return False, "social_cooldown_active"

        day_key = time.strftime("%Y-%m-%d", time.localtime(now))
        hour_key = time.strftime("%Y-%m-%dT%H", time.localtime(now))
        day_count_key = (target_bot_id, day_key)
        hour_count_key = (target_bot_id, hour_key)
        if store.DYNAMIC_SOCIAL_DAILY_COUNTS.get(day_count_key, 0) >= max_per_day:
            return False, "daily_social_quota_exhausted"
        if store.DYNAMIC_SOCIAL_HOURLY_COUNTS.get(hour_count_key, 0) >= max_per_hour:
            return False, "hourly_social_quota_exhausted"

        store.DYNAMIC_SOCIAL_DAILY_COUNTS[day_count_key] = store.DYNAMIC_SOCIAL_DAILY_COUNTS.get(day_count_key, 0) + 1
        store.DYNAMIC_SOCIAL_HOURLY_COUNTS[hour_count_key] = store.DYNAMIC_SOCIAL_HOURLY_COUNTS.get(hour_count_key, 0) + 1
        if cooldown_seconds > 0:
            store.DYNAMIC_SOCIAL_COOLDOWNS[target_bot_id] = now + cooldown_seconds
        return True, "ok"


class RelaySocialContactTool(BaseTool):
    """Contact another bot through the relay social channel only."""

    tool_name = "relay_social_contact"
    tool_description = (
        "通过 bot_relay 的 social channel 联系另一个 bot。"
        "用于 todo 插件执行 bot 待办时联系其他 bot；禁止用于 transaction/system。"
    )
    associated_platforms = ["bot_relay"]

    async def execute(
        self,
        target_bot_id: Annotated[str, "目标 bot_id"],
        message: Annotated[str, "要通过社交链路发送给对端 bot 的自然语言消息"],
        reason: Annotated[str, "主动联系原因，例如 todo_execution、event、impulse"] = "todo_execution",
        conversation_id: Annotated[str, "可选：沿用来源 relay 事务的 conversation_id"] = "",
        trace_id: Annotated[str, "可选：沿用来源 relay 事务的 trace_id"] = "",
    ) -> tuple[bool, str]:
        """Send a proactive social message to a target bot."""

        config = getattr(self.plugin, "config", None)
        if not isinstance(config, BotPrivateRelayConfig):
            config = _REGISTERED_RELAY_CONFIG
        if not isinstance(config, BotPrivateRelayConfig):
            return False, "relay_config_unavailable"
        target_bot_id = target_bot_id.strip()
        message = message.strip()
        if not target_bot_id or not message:
            return False, "invalid_social_contact_payload"
        partner = config.partner_by_id(target_bot_id)
        if partner is None:
            if not config.dynamic_social.default_allow_all_bots:
                return False, "unknown_social_target"
            partner_name = target_bot_id
        else:
            partner_name = partner.bot_name

        ok, code = DynamicSocialLimiter(config).allow(target_bot_id=target_bot_id, source=self._source_from_reason(reason))
        if not ok:
            return False, code

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
        """Map free-form reasons to quota source buckets."""

        normalized = reason.strip().lower()
        if normalized in {"impulse", "突发奇想"}:
            return "impulse"
        if normalized in {"event", "事件"}:
            return "event"
        if normalized in {"user_command", "command", "用户指令"}:
            return "user_command"
        return "event"
