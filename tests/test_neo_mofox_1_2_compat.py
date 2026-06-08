"""Neo-MoFox 1.2 compatibility contract tests."""

from __future__ import annotations

import asyncio
from typing import Any

from mofox_wire import MessageEnvelope

from plugins.bot_private_relay.components.actions.relay import (
    BotRelayPassAndWaitAction,
    BotRelaySendTextAction,
    BotRelayStopConversationAction,
)
from plugins.bot_private_relay.components.adapters.bot_relay import BotRelayAdapter
from plugins.bot_private_relay.components.config import BotPrivateRelayConfig, PartnerSection
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from plugins.bot_private_relay.runtime import store


class DummySink:
    """Minimal CoreSink stub for adapter contract tests."""

    def __init__(self) -> None:
        self.captured: list[MessageEnvelope] = []

    async def send(self, envelope: MessageEnvelope) -> None:
        """Record emitted envelopes."""

        self.captured.append(envelope)


def build_config() -> BotPrivateRelayConfig:
    """Build a minimal relay config with one allowed partner."""

    config = BotPrivateRelayConfig()
    config.relay.bot_id = "bot_alpha"
    config.relay.bot_name = "Bot Alpha"
    config.partners.bots = [PartnerSection(bot_id="bot_beta", bot_name="Bot Beta")]
    config.presence.allowed_partner_bots = ["bot_beta"]
    return config


def build_adapter() -> BotRelayAdapter:
    """Build a relay adapter with a dummy core sink."""

    return BotRelayAdapter(core_sink=DummySink(), plugin=BotPrivateRelayPlugin(build_config()))


def test_relay_actions_declare_text_associated_types() -> None:
    """Neo-MoFox 1.2 requires action components to declare valid associated types."""

    assert BotRelaySendTextAction.validate_associated_types() == ["text"]
    assert BotRelayPassAndWaitAction.validate_associated_types() == ["text"]
    assert BotRelayStopConversationAction.validate_associated_types() == ["text"]


def test_adapter_inbound_envelope_declares_accept_format() -> None:
    """Neo-MoFox 1.2 requires adapter envelopes to expose accepted formats."""

    store.reset_state()
    envelope = asyncio.run(
        build_adapter().from_platform_message(
            {
                "from_bot": "bot_beta",
                "from_bot_name": "Bot Beta",
                "to_bot": "bot_alpha",
                "to_bot_name": "Bot Alpha",
                "channel": "transaction",
                "intent": "request",
                "expect_reply": True,
                "reply_budget": 3,
                "terminal": False,
                "allowed_responders": ["bot_alpha"],
                "hop": 0,
                "ttl": 4,
                "message_id": "m-format-info",
                "conversation_id": "c-format-info",
                "trace_id": "t-format-info",
                "payload": {"text": "请帮我处理一下"},
            }
        )
    )

    assert envelope is not None
    assert isinstance(envelope, dict)
    message_info: Any = envelope.get("message_info") or {}
    extra: Any = message_info.get("extra") if isinstance(message_info, dict) else {}
    format_info: Any = extra.get("format_info") if isinstance(extra, dict) else {}
    assert format_info.get("accept_format") == ["text"]
