"""System-channel smoke contracts for bot_private_relay."""

from __future__ import annotations

import asyncio

from mofox_wire import MessageEnvelope

from plugins.bot_private_relay import store
from plugins.bot_private_relay.adapter import BotRelayAdapter
from plugins.bot_private_relay.config import BotPrivateRelayConfig, PartnerSection
from plugins.bot_private_relay.envelope import RelayEnvelope
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from plugins.bot_private_relay.system_handler import SystemChannelHandler


class DummySink:
    """Minimal CoreSink stub for adapter construction."""

    def __init__(self) -> None:
        self.captured: list[MessageEnvelope] = []

    async def send(self, envelope: MessageEnvelope) -> None:
        """Capture forwarded envelopes."""

        self.captured.append(envelope)


class StubPresenceManager:
    """PresenceManager test double recording presence updates."""

    def __init__(self) -> None:
        self.updated: list[RelayEnvelope] = []

    def update_from_envelope(self, envelope: RelayEnvelope) -> None:
        """Record a consumed presence update."""

        self.updated.append(envelope)


def build_config() -> BotPrivateRelayConfig:
    """Build the standard two-bot relay config for adapter tests."""

    config = BotPrivateRelayConfig()
    config.relay.bot_id = "223123"
    config.relay.bot_name = "清风"
    config.partners.bot_b = PartnerSection(bot_id="114514", bot_name="流光")
    config.presence.allowed_partner_bots = ["114514"]
    return config


def build_adapter() -> BotRelayAdapter:
    """Build a BotRelayAdapter with a dummy sink."""

    return BotRelayAdapter(
        core_sink=DummySink(),
        plugin=BotPrivateRelayPlugin(build_config()),
    )


def system_envelope(intent: str) -> RelayEnvelope:
    """Build a system envelope for the requested intent."""

    return RelayEnvelope(
        from_bot="114514",
        from_bot_name="流光",
        to_bot="223123",
        to_bot_name="清风",
        channel="system",
        intent=intent,
        terminal=True,
        expect_reply=False,
        payload={"status": "online"} if intent == "presence_update" else {},
    )


def test_system_presence_update_is_consumed_without_llm_path() -> None:
    """presence_update should update presence and stop before MessageEnvelope."""

    presence = StubPresenceManager()
    handler = SystemChannelHandler(presence)  # type: ignore[arg-type]
    envelope = system_envelope("presence_update")

    assert handler.handle(envelope) is True
    assert presence.updated == [envelope]


def test_system_control_intents_are_consumed_and_audited() -> None:
    """Control intents should be consumed and never enter the LLM path."""

    store.reset_state()
    presence = StubPresenceManager()
    handler = SystemChannelHandler(presence)  # type: ignore[arg-type]

    for intent in ("ack", "close", "cancel", "error", "heartbeat", "typing"):
        assert handler.handle(system_envelope(intent)) is True

    assert len(store.AUDIT_LOG) == 6
    assert [record["event"] for record in store.AUDIT_LOG] == ["system_event"] * 6
    assert presence.updated == []


def test_unknown_system_intent_is_consumed() -> None:
    """Unknown system intents should still be short-circuited away from LLM."""

    handler = SystemChannelHandler(StubPresenceManager())  # type: ignore[arg-type]

    assert handler.handle(system_envelope("weird")) is True



def test_non_system_envelope_falls_through() -> None:
    """Non-system envelopes should not be consumed by SystemChannelHandler."""

    handler = SystemChannelHandler(StubPresenceManager())  # type: ignore[arg-type]
    envelope = RelayEnvelope(
        from_bot="114514",
        to_bot="223123",
        channel="transaction",
        intent="request",
        expect_reply=True,
        payload={"text": "hello"},
    )

    assert handler.handle(envelope) is False


def test_adapter_system_envelopes_return_none_before_llm() -> None:
    """Adapter should consume system channel and not build MessageEnvelope."""

    adapter = build_adapter()
    raw = system_envelope("ack").to_dict()

    result = asyncio.run(adapter.from_platform_message(raw))

    assert result is None


def test_adapter_unknown_partner_transaction_still_rejected() -> None:
    """Non-system envelopes from unknown bots should be rejected."""

    adapter = build_adapter()
    raw = RelayEnvelope(
        from_bot="777777",
        from_bot_name="陌生 bot",
        to_bot="223123",
        to_bot_name="清风",
        channel="transaction",
        intent="notify",
        terminal=True,
        expect_reply=False,
        payload={"text": "unknown"},
    ).to_dict()

    result = asyncio.run(adapter.from_platform_message(raw))

    assert result is None
