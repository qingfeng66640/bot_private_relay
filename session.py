"""Relay session helpers and Phase 2 transaction state machine."""

from __future__ import annotations

import time
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
        },
        "accepted": {"confirm": "closed", "decline": "closed", "cancel": "closed"},
        "reschedule_requested": {"accept": "accepted", "decline": "closed", "close": "closed", "cancel": "closed"},
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
        channel = str(context.get("channel") or "transaction")
        if channel == "social":
            envelope = self.build_social_envelope(
                from_bot=from_bot,
                from_bot_name=from_bot_name,
                to_bot=to_bot,
                to_bot_name=to_bot_name,
                text=text,
                phase=str(context.get("phase") or "opening"),
                reply_budget=_context_int(context, "reply_budget", default_reply_budget),
                cooldown_seconds=_context_int(context, "cooldown_seconds", 0),
                max_turns=_context_int(context, "max_turns", 6),
            )
            envelope.ttl = default_ttl
            conversation_id = context.get("conversation_id")
            if isinstance(conversation_id, str) and conversation_id:
                envelope.conversation_id = conversation_id
            trace_id = context.get("trace_id")
            if isinstance(trace_id, str) and trace_id:
                envelope.trace_id = trace_id
            self.save_social_session_from_envelope(envelope)
            return envelope
        inferred_session = self._find_session_for_outbound(context=context, message_envelope=message_envelope, to_bot=to_bot)
        explicit_intent = context.get("intent")
        inferred_intent = self._infer_intent_from_session(inferred_session)
        intent = str(inferred_intent or explicit_intent or "notify")
        conversation_id = str(context.get("conversation_id") or (inferred_session.conversation_id if inferred_session else ""))
        expects_initial_reply = intent in {"request", "invite"}
        reply_budget = default_reply_budget if expects_initial_reply else 0
        allowed_responders = [to_bot] if expects_initial_reply else []
        terminal = intent == "notify"
        expect_reply = expects_initial_reply
        state = "pending_reply" if expects_initial_reply else "closed"
        if inferred_session is not None and inferred_intent:
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
            envelope.state = "pending_reply" if expects_initial_reply else envelope.state
            envelope.expect_reply = expects_initial_reply
            envelope.terminal = intent == "notify"
            envelope.reply_budget = default_reply_budget if expects_initial_reply else envelope.reply_budget
            envelope.allowed_responders = [to_bot] if expects_initial_reply else envelope.allowed_responders
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
            "allowed_responders": list(envelope.allowed_responders),
        }

    def sync_inbound_transaction_session(self, envelope: RelayEnvelope) -> store.RelaySession | None:
        """Persist inbound transaction state from a validated relay envelope."""

        if envelope.channel != "transaction":
            return None
        if envelope.intent not in self._transaction_intents():
            return None

        existing = store.get_session(envelope.conversation_id)
        state = envelope.state or self._state_for_inbound_intent(envelope.intent, existing)
        terminal = envelope.terminal or state == "closed" or envelope.intent in {"notify", "confirm", "decline", "cancel", "ack", "close"}
        expect_reply = False if terminal else envelope.expect_reply
        reply_budget = 0 if terminal else envelope.reply_budget
        session = store.RelaySession(
            conversation_id=envelope.conversation_id,
            peer_bot_id=envelope.from_bot,
            channel=envelope.channel,
            intent=envelope.intent,
            state=state,
            terminal=terminal,
            expect_reply=expect_reply,
            reply_budget=reply_budget,
            allowed_responders=list(envelope.allowed_responders),
            phase=envelope.phase,
        )
        store.save_session(session)
        store.save_transaction_record(
            store.RelayTransactionRecord(
                conversation_id=envelope.conversation_id,
                trace_id=envelope.trace_id,
                from_bot=envelope.from_bot,
                to_bot=envelope.to_bot,
                current_state=state or "",
                final_intent=envelope.intent if terminal else None,
                topic=envelope.text,
                summary=envelope.text,
            )
        )
        return session

    def sync_inbound_social_session(self, envelope: RelayEnvelope) -> store.RelaySession | None:
        """Persist inbound social state before the local bot replies."""

        if envelope.channel != "social":
            return None

        existing = store.get_session(envelope.conversation_id)
        phase = envelope.phase or (existing.phase if existing is not None else "active")
        terminal = envelope.terminal or phase in self._SOCIAL_END_PHASES
        reply_budget = 0 if terminal else envelope.reply_budget
        allowed_responders = [] if terminal or reply_budget <= 0 else list(envelope.allowed_responders)
        expect_reply = (
            False
            if terminal or reply_budget <= 0 or not allowed_responders
            else envelope.expect_reply
        )
        session = store.RelaySession(
            conversation_id=envelope.conversation_id,
            peer_bot_id=envelope.from_bot,
            channel="social",
            intent=envelope.intent,
            state=None,
            terminal=terminal,
            expect_reply=expect_reply,
            reply_budget=reply_budget,
            allowed_responders=allowed_responders,
            phase=phase,
            turn_count=existing.turn_count if existing is not None else 0,
            max_turns=existing.max_turns if existing is not None else 6,
            cooldown_seconds=envelope.cooldown_seconds,
            cooldown_until=existing.cooldown_until if existing is not None else 0.0,
        )
        store.save_session(session)
        return session

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
        max_turns: int = 6,
    ) -> RelayEnvelope:
        """Build a social-channel envelope with Phase 3 state machine controls.

        If a social session already exists for this peer, its current phase and
        turn count are used instead of the caller-supplied defaults.
        """

        existing = self._find_social_session(to_bot)
        if existing is not None and not existing.terminal:
            existing = self.advance_social_turn(
                session=existing, max_turns=max_turns, cooldown_seconds=cooldown_seconds
            )
            phase = existing.phase or phase
            reply_budget = existing.reply_budget
            cooldown_seconds = existing.cooldown_seconds
        elif existing is not None:
            phase = existing.phase or "closed"
            reply_budget = 0
        else:
            # First social message to this peer: advance from opening to active
            if phase == "opening":
                phase = "active"

        terminal = phase in ("ending", "closed") or reply_budget <= 0
        allowed_responders = [to_bot] if not terminal else []

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
            allowed_responders=allowed_responders,
            terminal=terminal,
            expect_reply=not terminal,
            state=None,
        )
        if existing is not None:
            envelope.conversation_id = existing.conversation_id
        return self.apply_expect_reply_overrides(envelope)

    def _find_social_session(self, peer_bot_id: str) -> store.RelaySession | None:
        """Return a non-terminal social session for a peer bot."""
        for session in store.SESSION_TABLE.values():
            if session.peer_bot_id == peer_bot_id and session.channel == "social":
                if (session.phase or "") not in ("closed",):
                    return session
        return None

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

        existing = store.get_session(envelope.conversation_id)
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
            phase=envelope.phase,
            turn_count=existing.turn_count if existing is not None else 0,
            max_turns=existing.max_turns if existing is not None else 6,
            cooldown_seconds=envelope.cooldown_seconds,
            cooldown_until=existing.cooldown_until if existing is not None else 0.0,
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
            return False, "invalid_payload", None
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
        terminal = next_state == "closed" or action in {"confirm", "decline", "cancel", "ack", "close"}
        session.state = next_state
        session.intent = action
        session.reply_budget = 0 if terminal else max(0, session.reply_budget - 1)
        session.expect_reply = False if terminal else session.expect_reply
        session.terminal = terminal
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
        if state == "accepted":
            return "accept"
        if state == "reschedule_requested":
            return "reschedule"
        if state == "closed":
            if session.intent in {"notify", "confirm", "decline", "cancel", "ack", "close"}:
                return session.intent
            return "close"
        return None

    @classmethod
    def _transaction_intents(cls) -> set[str]:
        """Return known transaction intents from the transition table."""

        intents = set(cls._TRANSITIONS["created"])
        for transitions in cls._TRANSITIONS.values():
            intents.update(transitions)
        return intents

    def _state_for_inbound_intent(self, intent: str, existing: store.RelaySession | None) -> str:
        """Infer inbound state when the envelope did not carry one."""

        current = existing.state if existing is not None and existing.state else "created"
        next_state = self._TRANSITIONS.get(current, {}).get(intent)
        if next_state is not None:
            return next_state
        return self._TRANSITIONS["created"].get(intent, existing.state if existing is not None else "closed")

    # ── Phase 3 social session state machine ──────────────────────────

    _SOCIAL_PHASE_ORDER = ("opening", "active", "cooling", "ending", "closed")

    @staticmethod
    def _next_social_phase(current: str) -> str:
        """Return the next social phase in the ordered chain."""
        try:
            idx = SessionManager._SOCIAL_PHASE_ORDER.index(current)
            if idx + 1 < len(SessionManager._SOCIAL_PHASE_ORDER):
                return SessionManager._SOCIAL_PHASE_ORDER[idx + 1]
        except ValueError:
            pass
        return "closed"

    def advance_social_turn(
        self, *, session: store.RelaySession, max_turns: int = 6, cooldown_seconds: int = 0
    ) -> store.RelaySession:
        """Increment turn count and advance social phase when thresholds met.

        Phase transitions:
        - opening → active after 1 turn
        - active → cooling when turn_count >= max_turns * 0.7
        - cooling → ending when turn_count >= max_turns
        - ending → closed when explicitly ended or budget exhausted
        """
        session.turn_count += 1
        session.max_turns = max_turns
        session.cooldown_seconds = cooldown_seconds

        phase = session.phase or "opening"
        turns = session.turn_count

        if phase == "opening" and turns >= 1:
            phase = "active"
        if phase == "active" and turns >= int(max_turns * 0.7):
            phase = "cooling"
        if phase == "cooling" and turns >= max_turns:
            phase = "ending"

        session.reply_budget = max(0, session.reply_budget - 1)
        if session.reply_budget <= 0:
            phase = "ending" if phase != "closed" else phase

        if phase in ("ending", "closed"):
            session.terminal = True
            session.expect_reply = False
            session.reply_budget = 0
            session.allowed_responders = []
        else:
            session.terminal = False
            session.expect_reply = bool(session.allowed_responders)

        if cooldown_seconds > 0 and phase == "cooling":
            session.cooldown_until = time.time() + cooldown_seconds

        session.phase = phase
        store.save_session(session)
        return session

    def is_social_in_cooldown(self, session: store.RelaySession) -> bool:
        """Return True if the session is in an active cooldown window."""
        if session.channel != "social":
            return False
        if session.cooldown_until > time.time():
            return True
        return False

    def force_social_ending(self, session: store.RelaySession) -> store.RelaySession:
        """Immediately escalate a social session to ending."""
        session.phase = "ending"
        session.terminal = True
        session.expect_reply = False
        session.reply_budget = 0
        store.save_session(session)
        return session


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


def _context_int(context: dict[str, object], key: str, default: int) -> int:
    """Return a non-negative integer from relay context."""

    value = context.get(key)
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default
