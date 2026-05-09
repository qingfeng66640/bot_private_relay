"""Contract tests for bot_private_relay Phase 1."""

from __future__ import annotations

import json
from pathlib import Path

from plugins.bot_private_relay.command import RelayCommand
from plugins.bot_private_relay.config import BotPrivateRelayConfig, PartnerSection
from plugins.bot_private_relay.envelope import RelayEnvelope
from plugins.bot_private_relay.event_handler import LoopGuardEventHandler
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from plugins.bot_private_relay.policy import PolicyEngine
from plugins.bot_private_relay.presence import PresenceManager
from plugins.bot_private_relay.relay_actions import BotRelaySendTextAction
from plugins.bot_private_relay.session import SessionManager
from plugins.bot_private_relay.system_handler import SystemChannelHandler
from plugins.bot_private_relay import store


def build_config() -> BotPrivateRelayConfig:
    config = BotPrivateRelayConfig()
    config.relay.bot_id = "223123"
    config.relay.bot_name = "清风"
    config.partners.bot_b = PartnerSection(bot_id="114514", bot_name="流光")
    config.presence.allowed_partner_bots = ["114514"]
    return config


def test_manifest_and_plugin_identity() -> None:
    manifest = json.loads(Path(__file__).resolve().parents[1].joinpath("manifest.json").read_text(encoding="utf-8"))
    assert manifest["name"] == "bot_private_relay"
    assert manifest["python_dependencies"] == ["paho-mqtt>=2.0"]
    assert BotPrivateRelayPlugin.plugin_name == "bot_private_relay"
    plugin = BotPrivateRelayPlugin(build_config())
    components = plugin.get_components()
    assert components


def test_config_partner_lookup_uses_bot_id() -> None:
    config = build_config()
    partner = config.partner_by_id("114514")
    assert partner is not None
    assert partner.bot_name == "流光"
    assert config.first_allowed_partner() is partner


def test_relay_envelope_roundtrip_and_validation() -> None:
    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", payload={"text": "hi"})
    as_dict = envelope.to_dict()
    rebuilt = RelayEnvelope.from_dict(as_dict)
    rebuilt.validate()
    assert rebuilt.from_bot == "223123"
    assert rebuilt.text == "hi"


def test_store_dedup_and_reset() -> None:
    store.reset_state()
    assert store.remember_message("m1") is True
    assert store.remember_message("m1") is False
    store.reset_state()
    assert store.DEDUP_CACHE == {}


def test_policy_notify_is_one_way() -> None:
    envelope = RelayEnvelope(
        from_bot="223123",
        to_bot="114514",
        intent="notify",
        channel="transaction",
        expect_reply=True,
        reply_budget=9,
        terminal=False,
    )
    result = PolicyEngine().apply_outbound(envelope)
    assert result.terminal is True
    assert result.expect_reply is False
    assert result.reply_budget == 0


def test_session_manager_builds_request() -> None:
    manager = SessionManager()
    envelope = manager.build_outbound_envelope(
        message_envelope={
            "message_info": {
                "platform": "bot_relay",
                "extra": {"relay_context": {"intent": "request", "channel": "transaction"}},
            },
            "message_segment": [{"type": "text", "data": "请帮我处理一下"}],
        },
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
    )
    assert envelope.intent == "request"
    assert envelope.expect_reply is True
    assert envelope.allowed_responders == ["114514"]


def test_presence_and_system_handler_short_path() -> None:
    store.reset_state()
    config = build_config()
    presence = PresenceManager(config)
    handler = SystemChannelHandler(presence)
    consumed = handler.handle(
        RelayEnvelope(
            from_bot="223123",
            from_bot_name="清风",
            to_bot="*",
            to_bot_name="*",
            channel="system",
            intent="presence_update",
            payload={"status": "online"},
        )
    )
    assert consumed is True
    assert store.PRESENCE_TABLE["223123"].status == "online"


def test_relay_action_isolated_to_bot_relay_chatter() -> None:
    assert BotRelaySendTextAction.chatter_allow == ["bot_relay_chatter"]


def test_loop_guard_received_dedup_and_sent_boundary() -> None:
    store.reset_state()
    handler = LoopGuardEventHandler(plugin=BotPrivateRelayPlugin(build_config()))
    params = {
        "message": type("M", (), {"message_id": "m1", "extra": {"relay_envelope": {"message_id": "m1", "hop": 0, "ttl": 4}, "relay_context": {"reply_budget": 1}}})(),
        "envelope": None,
        "adapter_signature": "bot_private_relay:adapter:bot_relay",
    }
    decision, _ = handler._handle_received(params)
    assert str(decision) == "EventDecision.SUCCESS"
    decision2, _ = handler._handle_received(params)
    assert str(decision2) == "EventDecision.STOP"
    sent_params = {
        "message": type("M", (), {"platform": "bot_relay", "extra": {"relay_context": {}}})(),
        "envelope": None,
        "adapter_signature": "wrong:adapter:anything",
        "continue_send": True,
    }
    decision3, sent_after = handler._handle_sent(sent_params)
    assert str(decision3) == "EventDecision.STOP"
    assert sent_after["continue_send"] is False


def test_command_status() -> None:
    plugin = BotPrivateRelayPlugin(build_config())
    command = RelayCommand(plugin=plugin, stream_id="s1")
    success, text = __import__("asyncio").run(command.status())
    assert success is True
    assert "relay status:" in text
