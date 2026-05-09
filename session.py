"""Relay session helpers and Phase 2 transaction state machine."""

from __future__ import annotations

from uuid import uuid4

from mofox_wire import MessageEnvelope

from .envelope import RelayEnvelope
from . import store


class SessionManager:
    """Provide transaction semantics without owning runtime state."""

    _TRANSITIONS = {
        "created": {"notify": "closed", "request": "pending_reply", "invite": "pending_reply"},
        "pending_reply": {
            "accept": "accepted",
            "decline": "closed",
            "reschedule": "reschedule_requested",
            "ack": "closed",
            "close": "closed",
            "cancel": "closed",
            "confirm": "confirmed",
        },
        "accepted": {"confirm": "closed", "decline": "closed", "cancel": "closed"},
        "reschedule_requested": {"accept": "accepted", "decline": "closed", "close": "closed", "cancel": "closed"},
        "confirmed": {},
        "closed": {},
    }
    _SOCIAL_END_PHASES = {"ending", "closed"}

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
        explicit_intent = context.get("intent")
        inferred_session = self._find_session_for_outbound(context=context, message_envelope=message_envelope, to_bot=to_bot)
        intent = str(explicit_intent or self._infer_intent_from_session(inferred_session) or "notify")
        channel = str(context.get("channel") or "transaction")
        conversation_id = str(context.get("conversation_id") or (inferred_session.conversation_id if inferred_session else ""))
        reply_budget = default_reply_budget if intent == "request" else 0
        allowed_responders = [to_bot] if intent == "request" else []
        terminal = intent == "notify"
        expect_reply = intent == "request"
        state = "pending_reply" if intent == "request" else "closed"
        if inferred_session is not None and not explicit_intent:
            reply_budget = inferred_session.reply_budget
            allowed_responders = list(inferred_session.allowed_responders)
            state = inferred_session.state or state
            terminal = inferred_session.terminal
            expect_reply = inferred_session.expect_reply
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
            allowed_responders=allowed_responders,
            reply_budget=reply_budget,
            terminal=terminal,
            expect_reply=expect_reply,
            state=state,
        )
        if inferred_session is None:
            envelope.state = "pending_reply" if intent == "request" else envelope.state
            envelope.expect_reply = intent == "request"
            envelope.terminal = intent == "notify"
            envelope.reply_budget = default_reply_budget if intent == "request" else envelope.reply_budget
            envelope.allowed_responders = [to_bot] if intent == "request" else envelope.allowed_responders
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
        store.save_transaction_record(
            store.RelayTransactionRecord(
                conversation_id=envelope.conversation_id,
                trace_id=envelope.trace_id,
                from_bot=from_bot,
                to_bot=to_bot,
                current_state=envelope.state or "",
                final_intent=envelope.intent if envelope.terminal else None,
                topic=text,
                summary=text,
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

    def build_social_envelope(
        self,
        *,
        from_bot: str,
        from_bot_name: str,
        to_bot: str,
        to_bot_name: str,
        text: str,
        phase: str = "opening",
        reply_budget: int = 3,
        cooldown_seconds: int = 0,
    ) -> RelayEnvelope:
        """Build a social-channel envelope with Phase 3 controls."""

        envelope = RelayEnvelope(
            from_bot=from_bot,
            from_bot_name=from_bot_name,
            to_bot=to_bot,
            to_bot_name=to_bot_name,
            channel="social",
            intent="say",
            payload={"text": text},
            phase=phase,
            reply_budget=reply_budget,
            cooldown_seconds=cooldown_seconds,
            allowed_responders=[to_bot],
            terminal=False,
            expect_reply=True,
            state=None,
        )
        return self.apply_expect_reply_overrides(envelope)

    def apply_expect_reply_overrides(self, envelope: RelayEnvelope) -> RelayEnvelope:
        """Apply the frozen Phase 3 expect_reply override priority."""

        if envelope.terminal is True:
            envelope.expect_reply = False
            return envelope
        if envelope.reply_budget <= 0:
            envelope.expect_reply = False
            return envelope
        if not envelope.allowed_responders:
            envelope.expect_reply = False
            return envelope
        if envelope.phase in self._SOCIAL_END_PHASES:
            envelope.expect_reply = False
            return envelope
        envelope.expect_reply = True
        return envelope

    def save_social_session_from_envelope(self, envelope: RelayEnvelope) -> store.RelaySession:
        """Persist minimal social-session state into the shared store."""

        session = store.RelaySession(
            conversation_id=envelope.conversation_id,
            peer_bot_id=envelope.to_bot,
            channel="social",
            intent=envelope.intent,
            state=None,
            terminal=envelope.terminal,
            expect_reply=envelope.expect_reply,
            reply_budget=envelope.reply_budget,
            allowed_responders=list(envelope.allowed_responders),
        )
        store.save_session(session)
        return session

    def maybe_create_memory_candidate(self, *, envelope: RelayEnvelope) -> None:
        """Project high-value relay messages into memory candidates."""

        text = envelope.text.strip()
        if envelope.channel not in {"social", "transaction"}:
            return
        if len(text) < 12:
            return
        score = min(1.0, len(text) / 100)
        if score < 0.2:
            return
        store.save_memory_candidate(
            store.RelayMemoryCandidate(
                candidate_id=uuid4().hex,
                conversation_id=envelope.conversation_id,
                peer_bot_id=envelope.to_bot,
                channel=envelope.channel,
                content=text,
                score=score,
            )
        )

    def validate_transaction_action(self, *, conversation_id: str, action: str, caller_bot: str, payload_complete: bool = True) -> tuple[bool, str, store.RelaySession | None]:
        """Run the six hard checks for a transaction tool."""

        session = store.get_session(conversation_id)
        if session is None:
            return False, "conversation_not_found", None
        state = session.state or "created"
        if action not in self._TRANSITIONS.get(state, {}):
            return False, "state_not_allowed", session
        if caller_bot not in session.allowed_responders:
            return False, "not_allowed_responder", session
        if session.terminal or state == "closed":
            return False, "conversation_closed", session
        if session.reply_budget <= 0:
            return False, "reply_budget_exhausted", session
        if not payload_complete:
            return False, "invalid_payload", session
        return True, "ok", session

    def apply_transaction_action(self, *, conversation_id: str, action: str, caller_bot: str) -> store.RelaySession:
        """Advance session after a validated tool action."""

        session = store.get_session(conversation_id)
        if session is None:
            raise ValueError("conversation_not_found")
        current_state = session.state or "created"
        next_state = self._TRANSITIONS[current_state][action]
        session.state = next_state
        session.reply_budget = max(0, session.reply_budget - 1)
        session.expect_reply = False if next_state in {"confirmed", "closed"} or action in {"confirm", "decline", "cancel", "ack", "close"} else session.expect_reply
        session.terminal = action in {"confirm", "decline", "cancel", "ack", "close"}
        store.save_session(session)
        record = store.TRANSACTION_LOG.get(conversation_id)
        if record is not None:
            record.current_state = next_state
            record.final_intent = action if session.terminal else record.final_intent
            store.save_transaction_record(record)
            if action == "confirm":
                store.save_todo(store.RelayTodoItem(todo_id=uuid4().hex, owner_bot=caller_bot, title=record.topic or "relay task"))
        return session

    def _find_session_for_outbound(self, *, context: dict[str, object], message_envelope: MessageEnvelope, to_bot: str) -> store.RelaySession | None:
        """Find an outbound session by explicit conversation id or peer bot id."""

        conversation_id = context.get("conversation_id")
        if isinstance(conversation_id, str) and conversation_id:
            return store.get_session(conversation_id)
        for session in store.SESSION_TABLE.values():
            if session.peer_bot_id == to_bot and session.channel == "transaction" and (session.state or "") not in {"closed"}:
                return session
        user_info = (message_envelope.get("message_info") or {}).get("user_info", {})
        user_id = user_info.get("user_id") if isinstance(user_info, dict) else None
        if isinstance(user_id, str):
            for session in store.SESSION_TABLE.values():
                if session.peer_bot_id == user_id and session.channel == "transaction" and (session.state or "") not in {"closed"}:
                    return session
        return None

    @staticmethod
    def _infer_intent_from_session(session: store.RelaySession | None) -> str | None:
        """Infer outbound intent from current transaction session state."""

        if session is None:
            return None
        state = session.state or ""
        if state == "confirmed":
            return "confirm"
        if state == "accepted":
            return "accept"
        if state == "reschedule_requested":
            return "reschedule"
        if state == "closed":
            return "close"
        return None


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
