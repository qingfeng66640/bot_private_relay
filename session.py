"""Minimal Phase 1 session helpers."""

from __future__ import annotations

from mofox_wire import MessageEnvelope

from .envelope import RelayEnvelope
from . import store


class SessionManager:
    """Provide transaction semantics without owning runtime state."""

    def build_outbound_envelope(
        self,
        *,
        message_envelope: MessageEnvelope,
        from_bot: str,
        from_bot_name: str,
        to_bot: str,
        to_bot_name: str,
        default_ttl: int = 4,
        default_reply_budget: int = 3,
    ) -> RelayEnvelope:
        """Build the Phase 1 outbound relay envelope."""

        text = _extract_text(message_envelope)
        extra = _extract_extra(message_envelope)
        relay_context = extra.get("relay_context") if isinstance(extra, dict) else None
        context = relay_context if isinstance(relay_context, dict) else {}
        intent = str(context.get("intent") or "notify")
        channel = str(context.get("channel") or "transaction")
        conversation_id = str(context.get("conversation_id") or "")
        envelope = RelayEnvelope(
            conversation_id=conversation_id or RelayEnvelope().conversation_id,
            from_bot=from_bot,
            from_bot_name=from_bot_name,
            to_bot=to_bot,
            to_bot_name=to_bot_name,
            channel=channel if channel in {"system", "transaction", "social"} else "transaction",
            intent=intent,
            ttl=default_ttl,
            payload={"text": text, "structured": context.get("structured", {})},
            allowed_responders=[to_bot] if intent == "request" else [],
            reply_budget=default_reply_budget if intent == "request" else 0,
            terminal=intent == "notify",
            expect_reply=intent == "request",
            state="pending_reply" if intent == "request" else "closed",
        )
        store.save_session(
            store.RelaySession(
                conversation_id=envelope.conversation_id,
                peer_bot_id=to_bot,
                channel=envelope.channel,
                intent=envelope.intent,
                state=envelope.state,
                terminal=envelope.terminal,
                expect_reply=envelope.expect_reply,
                reply_budget=envelope.reply_budget,
                allowed_responders=list(envelope.allowed_responders),
            )
        )
        return envelope

    def relay_context_from_envelope(self, envelope: RelayEnvelope) -> dict[str, object]:
        """Build Message.extra relay_context from an envelope."""

        return {
            "conversation_id": envelope.conversation_id,
            "trace_id": envelope.trace_id,
            "channel": envelope.channel,
            "intent": envelope.intent,
            "peer_bot_id": envelope.from_bot,
            "peer_bot_name": envelope.from_bot_name,
            "state": envelope.state,
            "phase": envelope.phase,
            "terminal": envelope.terminal,
            "expect_reply": envelope.expect_reply,
            "reply_budget": envelope.reply_budget,
        }


def _extract_text(message_envelope: MessageEnvelope) -> str:
    segments = message_envelope.get("message_segment") or []
    if isinstance(segments, dict):
        segments = [segments]
    text_parts: list[str] = []
    for segment in segments:
        if isinstance(segment, dict) and segment.get("type") == "text":
            text_parts.append(str(segment.get("data", "")))
    return "".join(text_parts)


def _extract_extra(message_envelope: MessageEnvelope) -> dict[str, object]:
    message_info = message_envelope.get("message_info") or {}
    extra = message_info.get("extra") if isinstance(message_info, dict) else None
    return extra if isinstance(extra, dict) else {}
