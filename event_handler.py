"""Event handlers for bot private relay."""

from __future__ import annotations

from typing import Any, cast

from src.app.plugin_system.base import BaseEventHandler
from src.core.components.types import EventType
from src.core.models.message import Message
from src.kernel.event import EventDecision

from . import store
from .config import BotPrivateRelayConfig


def _normalized_set(values: list[str]) -> set[str]:
    """Return non-empty lower-case values for config matching."""

    return {str(value).strip().lower() for value in values if str(value).strip()}


def _message_exists_in_history(context: Any, message: Message) -> bool:
    """Return whether the message is already present in stream history."""

    message_id = str(message.message_id or "")
    return bool(
        message_id
        and any(str(getattr(item, "message_id", "") or "") == message_id for item in getattr(context, "history_messages", []))
    )


def _add_history_once(context: Any, message: Message) -> None:
    """Move a suppressed message into history without duplicating it."""

    if _message_exists_in_history(context, message):
        return
    add_history_message = getattr(context, "add_history_message", None)
    if callable(add_history_message):
        add_history_message(message)


class LoopGuardEventHandler(BaseEventHandler):
    """Protect relay flows against loops and outbound leakage."""

    handler_name = "loop_guard"
    handler_description = "Loop guard for bot private relay"
    weight = 200
    intercept_message = True
    init_subscribe = [EventType.ON_MESSAGE_RECEIVED, EventType.ON_MESSAGE_SENT]

    async def execute(self, event_name: str, params: dict[str, Any]) -> tuple[EventDecision, dict[str, Any]]:
        """Validate relay messages while preserving param keys."""

        if event_name == EventType.ON_MESSAGE_RECEIVED:
            return self._handle_received(params)
        if event_name == EventType.ON_MESSAGE_SENT:
            return self._handle_sent(params)
        return EventDecision.PASS, params

    def _handle_received(self, params: dict[str, Any]) -> tuple[EventDecision, dict[str, Any]]:
        message = cast(Message | None, params.get("message"))
        if message is None:
            return EventDecision.PASS, params
        relay_envelope = message.extra.get("relay_envelope") if hasattr(message, "extra") else None
        if not isinstance(relay_envelope, dict):
            self._record_proactive_chat_hint(message)
            return EventDecision.PASS, params
        message_id = str(relay_envelope.get("message_id") or message.message_id or "")
        if message_id and not store.remember_message(message_id):
            return EventDecision.STOP, params
        hop = int(relay_envelope.get("hop", 0) or 0)
        ttl = int(relay_envelope.get("ttl", 0) or 0)
        if hop > ttl:
            return EventDecision.STOP, params
        relay_context = message.extra.get("relay_context", {}) if hasattr(message, "extra") else {}
        if isinstance(relay_context, dict):
            if relay_context.get("terminal") is True and relay_context.get("expect_reply") is True:
                return EventDecision.STOP, params
            if message.extra.get("bot_internal") is True and int(relay_context.get("reply_budget", 0) or 0) <= 0 and relay_context.get("expect_reply") is not False:
                return EventDecision.STOP, params
        return EventDecision.SUCCESS, params

    def _handle_sent(self, params: dict[str, Any]) -> tuple[EventDecision, dict[str, Any]]:
        message = cast(Message | None, params.get("message"))
        adapter_signature = str(params.get("adapter_signature") or "")
        if message is None:
            return EventDecision.PASS, params
        if message.platform != "bot_relay":
            return EventDecision.PASS, params
        if adapter_signature != "bot_private_relay:adapter:bot_relay":
            self._set_continue_send(params, False)
            return EventDecision.STOP, params
        relay_context = message.extra.get("relay_context", {}) if hasattr(message, "extra") else {}
        if not isinstance(relay_context, dict):
            self._set_continue_send(params, False)
            return EventDecision.STOP, params
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
        """Record ordinary chat messages as proactive decision context."""

        if message.platform == "bot_relay":
            return
        config = getattr(self.plugin, "config", None)
        if isinstance(config, BotPrivateRelayConfig) and not config.proactive.enabled:
            return
        text = str(message.processed_plain_text or message.content or "").strip()
        if not text:
            return
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
        """Update continue_send without changing the event param signature."""

        if "continue_send" in params:
            params["continue_send"] = value


class GroupReplySuppressionEventHandler(BaseEventHandler):
    """Receive configured bot messages in groups without triggering replies."""

    handler_name = "group_reply_suppression"
    handler_description = "Suppress default chatter replies to configured group bot senders"
    weight = 300
    intercept_message = False
    init_subscribe = [EventType.ON_CHATTER_STEP]

    async def execute(self, event_name: str, params: dict[str, Any]) -> tuple[EventDecision, dict[str, Any]]:
        """Filter blocked bot messages before chatter consumes unread messages."""

        if event_name != EventType.ON_CHATTER_STEP:
            return EventDecision.PASS, params

        config = getattr(self.plugin, "config", None)
        if not isinstance(config, BotPrivateRelayConfig):
            return EventDecision.PASS, params
        suppression = config.group_reply_suppression
        blocked_bot_ids = {str(bot_id).strip() for bot_id in suppression.blocked_bot_ids if str(bot_id).strip()}
        if not suppression.enabled or not blocked_bot_ids:
            return EventDecision.PASS, params

        context = params.get("context")
        unread_messages = getattr(context, "unread_messages", None)
        if context is None or not isinstance(unread_messages, list) or not unread_messages:
            return EventDecision.PASS, params

        platforms = _normalized_set(suppression.platforms)
        chat_types = _normalized_set(suppression.chat_types)
        kept: list[Message] = []
        suppressed: list[Message] = []
        for message in unread_messages:
            if isinstance(message, Message) and self._should_suppress(message, platforms, chat_types, blocked_bot_ids):
                suppressed.append(message)
            else:
                kept.append(message)

        if not suppressed:
            return EventDecision.PASS, params

        context.unread_messages = kept
        for message in suppressed:
            _add_history_once(context, message)
            store.audit(
                "group_reply_suppressed",
                stream_id=message.stream_id,
                message_id=message.message_id,
                sender_id=message.sender_id,
                reason_code="configured_group_bot",
            )

        if kept:
            context.triggering_user_id = kept[-1].sender_id
            params["continue"] = True
            return EventDecision.SUCCESS, params

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
        """Return whether the message should be removed before chatter execution."""

        platform = str(message.platform or "").strip().lower()
        chat_type = str(message.chat_type or "").strip().lower()
        sender_id = str(message.sender_id or "").strip()
        return platform in platforms and chat_type in chat_types and sender_id in blocked_bot_ids
