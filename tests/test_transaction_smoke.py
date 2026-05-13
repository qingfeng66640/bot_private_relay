"""Transaction smoke contracts for bot_private_relay."""

from __future__ import annotations

import json

from plugins.bot_private_relay.chatter import BotRelayChatter
from plugins.bot_private_relay.config import BotPrivateRelayConfig, PartnerSection
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from plugins.bot_private_relay.scripts import mqtt_smoke_test


def build_config() -> BotPrivateRelayConfig:
    """Build the standard two-bot smoke config."""

    config = BotPrivateRelayConfig()
    config.relay.bot_id = "223123"
    config.relay.bot_name = "清风"
    config.partners.bot_b = PartnerSection(bot_id="114514", bot_name="流光")
    config.presence.allowed_partner_bots = ["114514"]
    return config


def test_transaction_smoke_payload_sequence_preserves_conversation() -> None:
    """Wire smoke payloads should model request -> accept -> confirm -> closed."""

    conversation_id = "conv-smoke"
    trace_id = "trace-smoke"
    request = json.loads(
        mqtt_smoke_test._request_payload(  # noqa: SLF001 - smoke helper contract
            from_bot="223123",
            from_name="清风",
            to_bot="114514",
            to_name="流光",
            conversation_id=conversation_id,
            trace_id=trace_id,
        )
    )
    accept = json.loads(
        mqtt_smoke_test._accept_payload(  # noqa: SLF001 - smoke helper contract
            from_bot="114514",
            from_name="流光",
            to_bot="223123",
            to_name="清风",
            conversation_id=conversation_id,
            trace_id=trace_id,
        )
    )
    confirm = json.loads(
        mqtt_smoke_test._confirm_payload(  # noqa: SLF001 - smoke helper contract
            from_bot="114514",
            from_name="流光",
            to_bot="223123",
            to_name="清风",
            conversation_id=conversation_id,
            trace_id=trace_id,
        )
    )

    assert [request["intent"], accept["intent"], confirm["intent"]] == [
        "request",
        "accept",
        "confirm",
    ]
    assert {request["conversation_id"], accept["conversation_id"], confirm["conversation_id"]} == {conversation_id}
    assert request["state"] == "pending_reply"
    assert accept["state"] == "accepted"
    assert confirm["state"] == "closed"
    assert confirm["terminal"] is True
    assert confirm["expect_reply"] is False


def test_transaction_smoke_local_validation_covers_invalid_paths() -> None:
    """Smoke script should self-check wrong responder and direct confirm rejection."""

    assert mqtt_smoke_test._validate_local_lifecycle() == []  # noqa: SLF001 - smoke helper contract


def test_bot_relay_chatter_prompt_explains_accept_first_lifecycle() -> None:
    """The relay prompt should steer LLMs away from direct pending confirm."""

    chatter = BotRelayChatter(stream_id="s1", plugin=BotPrivateRelayPlugin(build_config()))
    prompt = chatter._build_system_prompt(  # noqa: SLF001 - prompt contract
        {
            "conversation_id": "conv-smoke",
            "peer_bot_id": "114514",
            "peer_bot_name": "流光",
            "channel": "transaction",
            "intent": "request",
            "state": "pending_reply",
            "terminal": False,
            "expect_reply": True,
            "reply_budget": 3,
            "allowed_responders": ["223123"],
        }
    )

    assert "pending_reply" in prompt
    assert "accept_transaction" in prompt
    assert "accepted" in prompt
    assert "confirm_transaction" in prompt
    assert "closed" in prompt
    assert "不要从 pending_reply 直接调用 confirm_transaction" in prompt
