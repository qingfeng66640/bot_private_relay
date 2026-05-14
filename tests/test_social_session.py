"""Phase 3 social-session coverage for bot_private_relay."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from plugins.bot_private_relay import store
from plugins.bot_private_relay.chatter import BotRelayChatter
from plugins.bot_private_relay.config import BotPrivateRelayConfig
from plugins.bot_private_relay.envelope import RelayEnvelope
from plugins.bot_private_relay.memory_bridge import MemoryBridgeService
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from plugins.bot_private_relay.session import SessionManager


LOCAL_BOT_ID = "223123"
PEER_BOT_ID = "114514"


def _plugin() -> BotPrivateRelayPlugin:
    """Build a relay plugin instance for service tests."""

    config = BotPrivateRelayConfig()
    config.relay.bot_id = LOCAL_BOT_ID
    config.relay.bot_name = "Local Bot"
    return BotPrivateRelayPlugin(config)


def _memory_service_candidates() -> dict[str, store.RelayMemoryCandidate]:
    """Return candidates through the plugin-local memory bridge service."""

    return MemoryBridgeService(_plugin()).list_candidates()


def _social_envelope(**overrides: object) -> RelayEnvelope:
    """Build a social relay envelope for session and memory tests."""

    payload = {
        "conversation_id": "social-conv",
        "trace_id": "social-trace",
        "from_bot": LOCAL_BOT_ID,
        "from_bot_name": "Local Bot",
        "to_bot": PEER_BOT_ID,
        "to_bot_name": "Peer Bot",
        "channel": "social",
        "intent": "say",
        "phase": "active",
        "terminal": False,
        "expect_reply": True,
        "reply_budget": 3,
        "allowed_responders": [PEER_BOT_ID],
        "payload": {"text": "We should keep this social relay bounded."},
    }
    payload.update(overrides)
    return RelayEnvelope(**payload)


def _context_from_session(session: store.RelaySession) -> dict[str, object]:
    """Build the relay_context shape consumed by BotRelayChatter."""

    return {
        "channel": session.channel,
        "phase": session.phase,
        "terminal": session.terminal,
        "expect_reply": session.expect_reply,
        "reply_budget": session.reply_budget,
        "allowed_responders": list(session.allowed_responders),
    }


def _social_message_envelope(conversation_id: str) -> dict[str, object]:
    """Build a MessageEnvelope-like payload with explicit social context."""

    return {
        "message_info": {
            "platform": "bot_relay",
            "extra": {
                "relay_context": {
                    "channel": "social",
                    "conversation_id": conversation_id,
                    "phase": "opening",
                    "reply_budget": 3,
                    "max_turns": 6,
                }
            },
        },
        "message_segment": [
            {"type": "text", "data": "Start a fresh social relay conversation."}
        ],
    }


def test_repeated_social_turns_reach_cooling_and_ending_within_limits() -> None:
    """Repeated social envelopes should close by max_turns without overspending budget."""

    store.reset_state()
    manager = SessionManager()
    first = manager.build_social_envelope(
        from_bot=LOCAL_BOT_ID,
        from_bot_name="Local Bot",
        to_bot=PEER_BOT_ID,
        to_bot_name="Peer Bot",
        text="Open a social session with explicit limits.",
        reply_budget=6,
        max_turns=4,
    )
    manager.save_social_session_from_envelope(first)

    phases: list[str | None] = [first.phase]
    latest = first
    for turn_number in range(1, 5):
        latest = manager.build_social_envelope(
            from_bot=LOCAL_BOT_ID,
            from_bot_name="Local Bot",
            to_bot=PEER_BOT_ID,
            to_bot_name="Peer Bot",
            text=f"Turn {turn_number} continues without unlimited extension.",
            reply_budget=6,
            max_turns=4,
        )
        phases.append(latest.phase)
        stored = store.SESSION_TABLE[latest.conversation_id]
        assert stored.turn_count <= stored.max_turns
        assert stored.reply_budget >= 0

    assert "cooling" in phases
    assert latest.phase == "ending"
    assert latest.terminal is True
    assert latest.expect_reply is False
    assert latest.reply_budget == 0
    assert latest.allowed_responders == []


def test_social_budget_exhaustion_forces_terminal_controls() -> None:
    """A social turn that spends the last reply budget must stop auto-continuation."""

    store.reset_state()
    manager = SessionManager()
    session = store.RelaySession(
        conversation_id="social-budget-terminal",
        peer_bot_id=PEER_BOT_ID,
        channel="social",
        intent="say",
        phase="active",
        terminal=False,
        expect_reply=True,
        reply_budget=1,
        allowed_responders=[PEER_BOT_ID],
        max_turns=6,
    )
    store.save_session(session)

    result = manager.advance_social_turn(session=session, max_turns=6)

    assert result.phase == "ending"
    assert result.terminal is True
    assert result.expect_reply is False
    assert result.reply_budget == 0
    assert result.allowed_responders == []


@pytest.mark.parametrize("phase", ["ending", "closed"])
def test_build_social_envelope_does_not_revive_ending_or_closed_sessions(
    phase: str,
) -> None:
    """Building another social envelope must preserve terminal end phases."""

    store.reset_state()
    manager = SessionManager()
    store.save_session(
        store.RelaySession(
            conversation_id=f"social-{phase}",
            peer_bot_id=PEER_BOT_ID,
            channel="social",
            intent="say",
            phase=phase,
            terminal=True,
            expect_reply=False,
            reply_budget=0,
            allowed_responders=[],
            max_turns=4,
        )
    )

    envelope = manager.build_social_envelope(
        from_bot=LOCAL_BOT_ID,
        from_bot_name="Local Bot",
        to_bot=PEER_BOT_ID,
        to_bot_name="Peer Bot",
        text="A later send must not revive an ended session.",
        reply_budget=3,
        max_turns=4,
    )

    assert envelope.conversation_id == f"social-{phase}"
    assert envelope.phase == phase
    assert envelope.terminal is True
    assert envelope.expect_reply is False
    assert envelope.reply_budget == 0
    assert envelope.allowed_responders == []
    assert len(store.SESSION_TABLE) == 1


def test_build_social_envelope_prefers_active_session_over_old_closed_session() -> None:
    """Peer lookup should not let an old closed social session poison active turns."""

    store.reset_state()
    manager = SessionManager()
    store.save_session(
        store.RelaySession(
            conversation_id="social-old-closed",
            peer_bot_id=PEER_BOT_ID,
            channel="social",
            intent="say",
            phase="closed",
            terminal=True,
            expect_reply=False,
            reply_budget=0,
            allowed_responders=[],
            max_turns=4,
        )
    )
    store.save_session(
        store.RelaySession(
            conversation_id="social-new-active",
            peer_bot_id=PEER_BOT_ID,
            channel="social",
            intent="say",
            phase="active",
            terminal=False,
            expect_reply=True,
            reply_budget=3,
            allowed_responders=[PEER_BOT_ID],
            max_turns=6,
        )
    )

    envelope = manager.build_social_envelope(
        from_bot=LOCAL_BOT_ID,
        from_bot_name="Local Bot",
        to_bot=PEER_BOT_ID,
        to_bot_name="Peer Bot",
        text="Continue the active social session.",
        reply_budget=3,
        max_turns=6,
    )

    assert envelope.conversation_id == "social-new-active"
    assert envelope.phase not in {"ending", "closed"}
    assert envelope.terminal is False
    assert envelope.expect_reply is True
    assert envelope.reply_budget > 0
    assert envelope.allowed_responders == [PEER_BOT_ID]


def test_build_outbound_social_uses_fresh_explicit_conversation_id() -> None:
    """A fresh explicit social id should not inherit an old closed peer session."""

    store.reset_state()
    manager = SessionManager()
    store.save_session(
        store.RelaySession(
            conversation_id="social-old-closed",
            peer_bot_id=PEER_BOT_ID,
            channel="social",
            intent="say",
            phase="closed",
            terminal=True,
            expect_reply=False,
            reply_budget=0,
            allowed_responders=[],
        )
    )

    envelope = manager.build_outbound_envelope(
        message_envelope=_social_message_envelope("social-fresh-explicit"),
        from_bot=LOCAL_BOT_ID,
        from_bot_name="Local Bot",
        to_bot=PEER_BOT_ID,
        to_bot_name="Peer Bot",
    )

    assert envelope.conversation_id == "social-fresh-explicit"
    assert envelope.phase == "active"
    assert envelope.terminal is False
    assert envelope.expect_reply is True
    assert envelope.reply_budget == 3
    assert store.SESSION_TABLE["social-fresh-explicit"].terminal is False
    assert store.SESSION_TABLE["social-old-closed"].terminal is True


def test_build_outbound_social_does_not_revive_exact_closed_conversation_id() -> None:
    """An explicit ended social conversation remains ended."""

    store.reset_state()
    manager = SessionManager()
    store.save_session(
        store.RelaySession(
            conversation_id="social-exact-closed",
            peer_bot_id=PEER_BOT_ID,
            channel="social",
            intent="say",
            phase="closed",
            terminal=True,
            expect_reply=False,
            reply_budget=0,
            allowed_responders=[],
        )
    )

    envelope = manager.build_outbound_envelope(
        message_envelope=_social_message_envelope("social-exact-closed"),
        from_bot=LOCAL_BOT_ID,
        from_bot_name="Local Bot",
        to_bot=PEER_BOT_ID,
        to_bot_name="Peer Bot",
    )

    assert envelope.conversation_id == "social-exact-closed"
    assert envelope.phase == "closed"
    assert envelope.terminal is True
    assert envelope.expect_reply is False
    assert envelope.reply_budget == 0


@pytest.mark.parametrize(
    ("case_name", "overrides", "expected_terminal", "expected_budget", "expected_responders"),
    [
        (
            "terminal_true",
            {"terminal": True, "phase": "active", "reply_budget": 2},
            True,
            0,
            [],
        ),
        (
            "phase_ending",
            {"terminal": False, "phase": "ending", "reply_budget": 2},
            True,
            0,
            [],
        ),
        (
            "phase_closed",
            {"terminal": False, "phase": "closed", "reply_budget": 2},
            True,
            0,
            [],
        ),
        (
            "no_budget",
            {"terminal": False, "phase": "active", "reply_budget": 0},
            False,
            0,
            [],
        ),
        (
            "no_responders",
            {"terminal": False, "phase": "active", "reply_budget": 2},
            False,
            2,
            [],
        ),
    ],
)
def test_inbound_social_sync_suppresses_auto_continue(
    case_name: str,
    overrides: dict[str, object],
    expected_terminal: bool,
    expected_budget: int,
    expected_responders: list[str],
) -> None:
    """Inbound terminal/no-budget/no-responder social state must not auto-continue."""

    store.reset_state()
    allowed_responders = (
        [] if case_name == "no_responders" else [LOCAL_BOT_ID]
    )
    envelope = _social_envelope(
        conversation_id=f"social-inbound-{case_name}",
        from_bot=PEER_BOT_ID,
        from_bot_name="Peer Bot",
        to_bot=LOCAL_BOT_ID,
        to_bot_name="Local Bot",
        expect_reply=True,
        allowed_responders=allowed_responders,
        **overrides,
    )

    session = SessionManager().sync_inbound_social_session(envelope)

    assert session is not None
    assert session.conversation_id == f"social-inbound-{case_name}"
    assert session.peer_bot_id == PEER_BOT_ID
    assert session.channel == "social"
    assert session.terminal is expected_terminal
    assert session.expect_reply is False
    assert session.reply_budget == expected_budget
    assert session.allowed_responders == expected_responders
    assert BotRelayChatter._should_respond(
        _context_from_session(session),
        LOCAL_BOT_ID,
    ) is False


@pytest.mark.parametrize(
    ("relay_context", "expected"),
    [
        (
            {
                "expect_reply": True,
                "terminal": True,
                "reply_budget": 3,
                "allowed_responders": [LOCAL_BOT_ID],
            },
            False,
        ),
        (
            {
                "expect_reply": True,
                "terminal": False,
                "phase": "ending",
                "reply_budget": 3,
                "allowed_responders": [LOCAL_BOT_ID],
            },
            False,
        ),
        (
            {
                "expect_reply": True,
                "terminal": False,
                "phase": "closed",
                "reply_budget": 3,
                "allowed_responders": [LOCAL_BOT_ID],
            },
            False,
        ),
        (
            {
                "expect_reply": True,
                "terminal": False,
                "phase": "active",
                "reply_budget": 0,
                "allowed_responders": [LOCAL_BOT_ID],
            },
            False,
        ),
        (
            {
                "expect_reply": True,
                "terminal": False,
                "phase": "active",
                "reply_budget": -1,
                "allowed_responders": [LOCAL_BOT_ID],
            },
            False,
        ),
        (
            {
                "expect_reply": True,
                "terminal": False,
                "phase": "active",
                "reply_budget": 3,
            },
            False,
        ),
        (
            {
                "expect_reply": True,
                "terminal": False,
                "phase": "active",
                "reply_budget": 3,
                "allowed_responders": [],
            },
            False,
        ),
        (
            {
                "expect_reply": True,
                "terminal": False,
                "phase": "active",
                "reply_budget": 3,
                "allowed_responders": [PEER_BOT_ID],
            },
            False,
        ),
        (
            {
                "expect_reply": True,
                "terminal": False,
                "phase": "active",
                "reply_budget": 3,
                "allowed_responders": [LOCAL_BOT_ID],
            },
            True,
        ),
    ],
)
def test_bot_relay_chatter_should_respond_social_gates(
    relay_context: dict[str, object],
    expected: bool,
) -> None:
    """The auto-response gate must honor phase, budget, and responder identity."""

    assert BotRelayChatter._should_respond(relay_context, LOCAL_BOT_ID) is expected


def test_bot_relay_chatter_system_prompt_includes_complete_core_personality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Relay replies should keep protocol rules and include the full bot persona."""

    personality = SimpleNamespace(
        nickname="昔涟",
        alias_names=["Cyrene", "昔莲"],
        personality_core="温柔、坚定、记忆深处的同行者",
        personality_side="会用诗意但克制的方式表达关心",
        identity="虚拟生命体",
        background_story="她与记忆、因果和明日希望相关。",
        reply_style="温柔自然，少用夸张 emoji",
        safety_guidelines=["拒绝危险请求", "保持诚实"],
        negative_behaviors=["不要冒充真实人类", "不要绕过协议规则"],
    )
    monkeypatch.setattr(
        "plugins.bot_private_relay.chatter.get_core_config",
        lambda: SimpleNamespace(personality=personality),
    )
    chatter = BotRelayChatter(stream_id="relay-stream", plugin=_plugin())

    prompt = chatter._build_system_prompt(
        {
            "conversation_id": "social-persona",
            "channel": "social",
            "intent": "say",
            "phase": "active",
            "terminal": False,
            "expect_reply": True,
            "reply_budget": 3,
            "allowed_responders": [LOCAL_BOT_ID],
        }
    )

    assert "请严格遵守以下规则" in prompt
    assert "# 完整人设" in prompt
    assert "昔涟" in prompt
    assert "Cyrene、昔莲" in prompt
    assert "温柔、坚定、记忆深处的同行者" in prompt
    assert "会用诗意但克制的方式表达关心" in prompt
    assert "虚拟生命体" in prompt
    assert "她与记忆、因果和明日希望相关。" in prompt
    assert "温柔自然，少用夸张 emoji" in prompt
    assert "拒绝危险请求" in prompt
    assert "不要绕过协议规则" in prompt
    assert "协议字段与硬门禁优先" in prompt


def test_social_memory_candidate_projection_records_plugin_local_fields() -> None:
    """Valuable social content should project exactly one plugin-local candidate."""

    store.reset_state()
    manager = SessionManager()
    content = (
        "Peer bot consistently summarized the planning decisions and closed "
        "the social relay without drifting into extra turns."
    )
    envelope = _social_envelope(
        conversation_id="memory-social-positive",
        payload={"text": content},
    )

    manager.maybe_create_memory_candidate(envelope=envelope)

    candidates = _memory_service_candidates()
    assert len(candidates) == 1
    candidate = next(iter(candidates.values()))
    assert candidate.conversation_id == "memory-social-positive"
    assert candidate.peer_bot_id == PEER_BOT_ID
    assert candidate.channel == "social"
    assert candidate.content == content
    assert candidate.score > 0
    assert candidates == store.RELAY_MEMORY_CANDIDATES


@pytest.mark.parametrize(
    "text",
    [
        "short",
        "1234567890123456789",
    ],
)
def test_short_or_low_value_social_text_creates_no_memory_candidate(text: str) -> None:
    """Short or low-score social content should not become memory candidates."""

    store.reset_state()
    manager = SessionManager()

    manager.maybe_create_memory_candidate(envelope=_social_envelope(payload={"text": text}))

    assert _memory_service_candidates() == {}


def test_non_social_or_transaction_channel_creates_no_memory_candidate() -> None:
    """Memory projection stays limited to social and transaction relay channels."""

    store.reset_state()
    manager = SessionManager()

    manager.maybe_create_memory_candidate(
        envelope=_social_envelope(
            channel="system",
            intent="presence_update",
            payload={
                "text": (
                    "This text is long enough to score but belongs to the system channel."
                )
            },
        )
    )

    assert _memory_service_candidates() == {}
