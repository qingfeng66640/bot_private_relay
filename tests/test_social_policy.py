"""Social-channel policy contracts for bot_private_relay."""

from __future__ import annotations

from plugins.bot_private_relay.envelope import RelayEnvelope
from plugins.bot_private_relay.policy import PolicyEngine


def social_envelope(**overrides: object) -> RelayEnvelope:
    """Build a valid social.say envelope with override support."""

    payload = {
        "from_bot": "223123",
        "from_bot_name": "清风",
        "to_bot": "114514",
        "to_bot_name": "流光",
        "channel": "social",
        "intent": "say",
        "phase": "opening",
        "reply_budget": 2,
        "expect_reply": True,
        "terminal": False,
        "allowed_responders": ["114514"],
        "payload": {"text": "social smoke: 我们聊一下协作节奏。"},
    }
    payload.update(overrides)
    return RelayEnvelope(**payload)


def test_social_policy_preserves_phase_budget_and_reply_controls() -> None:
    """Social envelopes should keep caller-supplied phase/budget controls."""

    envelope = social_envelope()
    result = PolicyEngine().apply_outbound(envelope)

    assert result.channel == "social"
    assert result.intent == "say"
    assert result.phase == "opening"
    assert result.reply_budget == 2
    assert result.expect_reply is True
    assert result.terminal is False
    assert result.allowed_responders == ["114514"]
    result.validate()


def test_social_terminal_suppresses_expect_reply() -> None:
    """Universal terminal rule should suppress social auto-reply."""

    envelope = social_envelope(
        phase="ending",
        terminal=True,
        expect_reply=True,
        reply_budget=1,
    )
    result = PolicyEngine().apply_outbound(envelope)

    assert result.phase == "ending"
    assert result.terminal is True
    assert result.expect_reply is False
    assert result.reply_budget == 1


def test_social_without_allowed_responders_suppresses_expect_reply() -> None:
    """No allowed responders means no social auto-reply even if requested."""

    envelope = social_envelope(allowed_responders=[], expect_reply=True)
    result = PolicyEngine().apply_outbound(envelope)

    assert result.allowed_responders == []
    assert result.terminal is False
    assert result.expect_reply is False
