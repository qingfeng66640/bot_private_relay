"""Loop guard event handler for bot private relay."""

from __future__ import annotations

from typing import Any, cast

from src.app.plugin_system.base import BaseEventHandler
from src.core.components.types import EventType
from src.core.models.message import Message
from src.kernel.event import EventDecision

from . import store


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

    @staticmethod
    def _set_continue_send(params: dict[str, Any], value: bool) -> None:
        """Update continue_send without changing the event param signature."""

        if "continue_send" in params:
            params["continue_send"] = value
