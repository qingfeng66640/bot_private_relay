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
from plugins.bot_private_relay.relay_tools import (
    CancelTransactionTool,
    ConfirmTransactionTool,
    DeclineTransactionTool,
)
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


def test_transaction_tools_are_isolated_to_bot_relay_chatter() -> None:
    assert ConfirmTransactionTool.chatter_allow == ["bot_relay_chatter"]
    assert DeclineTransactionTool.chatter_allow == ["bot_relay_chatter"]
    assert CancelTransactionTool.chatter_allow == ["bot_relay_chatter"]


def test_confirm_tool_runs_six_hard_checks_and_updates_session() -> None:
    store.reset_state()
    session = store.RelaySession(
        conversation_id="conv-001",
        peer_bot_id="114514",
        channel="transaction",
        intent="request",
        state="pending_reply",
        terminal=False,
        expect_reply=True,
        reply_budget=3,
        allowed_responders=["114514"],
    )
    store.save_session(session)
    store.save_transaction_record(
        store.RelayTransactionRecord(
            conversation_id="conv-001",
            trace_id="trace-001",
            from_bot="223123",
            to_bot="114514",
            current_state="pending_reply",
            topic="整理会议纪要",
            summary="整理会议纪要",
        )
    )
    plugin = BotPrivateRelayPlugin(build_config())
    success, payload = __import__("asyncio").run(
        ConfirmTransactionTool(plugin).execute(
            conversation_id="conv-001",
            caller_bot="114514",
            reason="我可以整理，下午给你",
        )
    )
    assert success is True
    assert payload["status"] == "ok"
    assert store.SESSION_TABLE["conv-001"].state == "confirmed"
    assert store.SESSION_TABLE["conv-001"].terminal is True
    assert store.TRANSACTION_LOG["conv-001"].final_intent == "confirm"
    assert store.RELAY_TODOS


def test_outbound_envelope_can_infer_confirm_from_session_state() -> None:
    store.reset_state()
    store.save_session(
        store.RelaySession(
            conversation_id="conv-002",
            peer_bot_id="114514",
            channel="transaction",
            intent="request",
            state="confirmed",
            terminal=True,
            expect_reply=False,
            reply_budget=0,
            allowed_responders=["114514"],
        )
    )
    envelope = SessionManager().build_outbound_envelope(
        message_envelope={
            "message_info": {
                "platform": "bot_relay",
                "user_info": {"user_id": "114514", "user_nickname": "流光"},
                "extra": {},
            },
            "message_segment": [{"type": "text", "data": "没问题，我来整理"}],
        },
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
    )
    assert envelope.intent == "confirm"
    assert envelope.terminal is True
    assert envelope.expect_reply is False


def test_expect_reply_override_priority_for_social_session() -> None:
    manager = SessionManager()
    social = manager.build_social_envelope(
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
        text="我们晚点再继续聊这个话题。",
        phase="active",
        reply_budget=2,
        cooldown_seconds=5,
    )
    assert social.expect_reply is True
    social.phase = "ending"
    social = manager.apply_expect_reply_overrides(social)
    assert social.expect_reply is False
    social.phase = "active"
    social.reply_budget = 0
    social = manager.apply_expect_reply_overrides(social)
    assert social.expect_reply is False
    social.reply_budget = 2
    social.terminal = True
    social = manager.apply_expect_reply_overrides(social)
    assert social.expect_reply is False


def test_social_session_and_memory_candidate_projection() -> None:
    store.reset_state()
    manager = SessionManager()
    social = manager.build_social_envelope(
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
        text="这次合作里你对会议纪要的整理方式让我记住了。",
        phase="active",
        reply_budget=2,
        cooldown_seconds=10,
    )
    session = manager.save_social_session_from_envelope(social)
    assert session.channel == "social"
    assert session.expect_reply is True
    manager.maybe_create_memory_candidate(envelope=social)
    assert store.RELAY_MEMORY_CANDIDATES
