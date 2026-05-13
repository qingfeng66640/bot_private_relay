"""Contract tests for bot_private_relay Phase 1–4."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any
from mofox_wire import MessageEnvelope
from plugins.default_chatter.plugin import DefaultChatter

from plugins.bot_private_relay import command as relay_command_module
from plugins.bot_private_relay.adapter import BotRelayAdapter
from plugins.bot_private_relay.command import RelayCommand
from plugins.bot_private_relay.chatter import BotRelayChatter
from plugins.bot_private_relay.config import BotPrivateRelayConfig, PartnerSection
from plugins.bot_private_relay.envelope import RelayEnvelope
from plugins.bot_private_relay.event_handler import LoopGuardEventHandler
from plugins.bot_private_relay.memory_bridge import MemoryBridgeService
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from plugins.bot_private_relay.policy import PolicyEngine
from plugins.bot_private_relay.presence import PresenceManager
from plugins.bot_private_relay.relay_actions import BotRelaySendTextAction
from plugins.bot_private_relay.router import BotPrivateRelayRouter
from plugins.bot_private_relay.relay_tools import (
    AcceptTransactionTool,
    CancelTransactionTool,
    ConfirmTransactionTool,
    DeclineTransactionTool,
)
from plugins.bot_private_relay.session import SessionManager
from plugins.bot_private_relay.system_handler import SystemChannelHandler
from plugins.bot_private_relay import store
from src.core.components.base import BaseChatter, Success, Wait
from src.core.models.message import Message, MessageType


class DummySink:
    """Minimal CoreSink stub for adapter tests."""
    captured: list[MessageEnvelope]

    def __init__(self) -> None:
        self.captured = []

    async def send(self, envelope: MessageEnvelope) -> None:
        self.captured.append(envelope)


class RecordingMessageSender:
    """MessageSender test double recording outbound send calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, str | None]] = []

    async def send_message(self, message: Any, adapter_signature: str | None = None) -> bool:
        """Record the outgoing message and report success."""

        self.calls.append((message, adapter_signature))
        return True


def build_adapter() -> BotRelayAdapter:
    plugin = BotPrivateRelayPlugin(build_config())
    return BotRelayAdapter(core_sink=DummySink(), plugin=plugin)


def build_config() -> BotPrivateRelayConfig:
    config = BotPrivateRelayConfig()
    config.relay.bot_id = "223123"
    config.relay.bot_name = "清风"
    config.partners.bot_b = PartnerSection(bot_id="114514", bot_name="流光")
    config.presence.allowed_partner_bots = ["114514"]
    return config


# ── Phase 0 / manifest identity ──────────────────────────────────────

def test_manifest_and_plugin_identity() -> None:
    manifest = json.loads(Path(__file__).resolve().parents[1].joinpath("manifest.json").read_text(encoding="utf-8"))
    assert manifest["name"] == "bot_private_relay"
    assert manifest["python_dependencies"] == ["paho-mqtt>=2.0"]
    tools = {
        item["component_name"]
        for item in manifest["include"]
        if item["component_type"] == "tool"
    }
    assert "accept_transaction" in tools
    assert BotPrivateRelayPlugin.plugin_name == "bot_private_relay"
    plugin = BotPrivateRelayPlugin(build_config())
    components = plugin.get_components()
    assert components
    assert AcceptTransactionTool in components
    assert MemoryBridgeService in components


# ── Config ────────────────────────────────────────────────────────────

def test_config_partner_lookup_uses_bot_id() -> None:
    config = build_config()
    partner = config.partner_by_id("114514")
    assert partner is not None
    assert partner.bot_name == "流光"
    assert config.first_allowed_partner() is partner


def test_config_default_path_matches_framework_convention() -> None:
    """Framework reads config/plugins/{plugin_name}/config.toml by convention."""
    # _plugin_ is injected by PluginManager at load time; simulate it for the test.
    BotPrivateRelayConfig._plugin_ = "bot_private_relay"
    try:
        path = BotPrivateRelayConfig.get_default_path()
        assert path is not None
        parts = path.parts
        assert parts[-3:] == ("plugins", "bot_private_relay", "config.toml")
    finally:
        if hasattr(BotPrivateRelayConfig, "_plugin_"):
            delattr(BotPrivateRelayConfig, "_plugin_")


# ── Envelope ──────────────────────────────────────────────────────────

def test_relay_envelope_roundtrip_and_validation() -> None:
    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", payload={"text": "hi"})
    as_dict = envelope.to_dict()
    rebuilt = RelayEnvelope.from_dict(as_dict)
    rebuilt.validate()
    assert rebuilt.from_bot == "223123"
    assert rebuilt.text == "hi"


def test_relay_envelope_increment_hop() -> None:
    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", hop=0, ttl=4)
    incremented = envelope.increment_hop()
    assert incremented.hop == 1
    assert envelope.hop == 0  # original unchanged


def test_relay_envelope_hop_exceeds_ttl_validation() -> None:
    envelope = RelayEnvelope(from_bot="223123", to_bot="114514", hop=5, ttl=4)
    try:
        envelope.validate()
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "hop exceeds ttl" in str(exc)


# ── Store ─────────────────────────────────────────────────────────────

def test_store_dedup_and_reset() -> None:
    store.reset_state()
    assert store.remember_message("m1") is True
    assert store.remember_message("m1") is False
    store.reset_state()
    assert store.DEDUP_CACHE == {}


# ── Policy ────────────────────────────────────────────────────────────

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


# ── Session manager ───────────────────────────────────────────────────

def test_session_manager_builds_request() -> None:
    store.reset_state()
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


def test_session_manager_builds_social_say_with_reply_controls() -> None:
    store.reset_state()
    manager = SessionManager()
    envelope = manager.build_outbound_envelope(
        message_envelope={
            "message_info": {
                "platform": "bot_relay",
                "extra": {
                    "relay_context": {
                        "intent": "say",
                        "channel": "social",
                        "phase": "opening",
                        "reply_budget": 2,
                    }
                },
            },
            "message_segment": [{"type": "text", "data": "我们聊一下协作节奏。"}],
        },
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
    )
    assert envelope.channel == "social"
    assert envelope.intent == "say"
    assert envelope.phase == "active"
    assert envelope.expect_reply is True
    assert envelope.reply_budget == 2
    assert envelope.allowed_responders == ["114514"]
    assert store.SESSION_TABLE[envelope.conversation_id].phase == "active"


# ── Presence & system handler ─────────────────────────────────────────

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


# ── Adapter ───────────────────────────────────────────────────────────

def test_adapter_rejects_wrong_target_or_unknown_partner() -> None:
    adapter = build_adapter()
    wrong_target = __import__("asyncio").run(
        adapter.from_platform_message(
            {
                "from_bot": "223123",
                "from_bot_name": "清风",
                "to_bot": "999999",
                "to_bot_name": "别的 bot",
                "channel": "transaction",
                "intent": "notify",
                "message_id": "m-wrong-target",
                "conversation_id": "c1",
                "trace_id": "t1",
                "payload": {"text": "hello"},
            }
        )
    )
    assert wrong_target is None

    unknown_partner = __import__("asyncio").run(
        adapter.from_platform_message(
            {
                "from_bot": "777777",
                "from_bot_name": "陌生 bot",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "notify",
                "message_id": "m-unknown-partner",
                "conversation_id": "c2",
                "trace_id": "t2",
                "payload": {"text": "hello"},
            }
        )
    )
    assert unknown_partner is None


def test_adapter_accepts_allowed_partner_and_returns_message_envelope() -> None:
    store.reset_state()
    adapter = build_adapter()
    envelope = __import__("asyncio").run(
        adapter.from_platform_message(
            {
                "from_bot": "114514",
                "from_bot_name": "流光",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "request",
                "expect_reply": True,
                "reply_budget": 3,
                "terminal": False,
                "allowed_responders": ["223123"],
                "hop": 0,
                "ttl": 4,
                "message_id": "m-ok",
                "conversation_id": "c-ok",
                "trace_id": "t-ok",
                "payload": {"text": "请帮我处理一下"},
            }
        )
    )
    assert envelope is not None
    assert isinstance(envelope, dict)
    assert "message_info" in envelope
    message_info = envelope.get("message_info") or {}
    user_info = message_info.get("user_info") if isinstance(message_info, dict) else {}
    assert user_info.get("user_id") == "114514"
    extra = message_info.get("extra") if isinstance(message_info, dict) else {}
    relay_context = extra.get("relay_context") if isinstance(extra, dict) else {}
    assert relay_context.get("allowed_responders") == ["223123"]
    assert relay_context.get("peer_bot_id") == "114514"
    session = store.SESSION_TABLE["c-ok"]
    assert session.peer_bot_id == "114514"
    assert session.state == "pending_reply"
    assert session.allowed_responders == ["223123"]


def test_adapter_persists_inbound_invite_session() -> None:
    store.reset_state()
    adapter = build_adapter()
    envelope = __import__("asyncio").run(
        adapter.from_platform_message(
            {
                "from_bot": "114514",
                "from_bot_name": "流光",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "invite",
                "expect_reply": True,
                "reply_budget": 2,
                "terminal": False,
                "allowed_responders": ["223123"],
                "hop": 0,
                "ttl": 4,
                "message_id": "m-invite",
                "conversation_id": "c-invite",
                "trace_id": "t-invite",
                "payload": {"text": "一起处理这个任务吗？"},
            }
        )
    )
    assert envelope is not None
    session = store.SESSION_TABLE["c-invite"]
    assert session.intent == "invite"
    assert session.state == "pending_reply"
    assert session.peer_bot_id == "114514"
    assert session.allowed_responders == ["223123"]


def test_adapter_increments_hop_on_inbound() -> None:
    """Hop should be incremented during inbound processing for ttl protection."""
    adapter = build_adapter()
    envelope = __import__("asyncio").run(
        adapter.from_platform_message(
            {
                "from_bot": "114514",
                "from_bot_name": "流光",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "notify",
                "hop": 0,
                "ttl": 4,
                "message_id": "m-hop",
                "conversation_id": "c-hop",
                "trace_id": "t-hop",
                "payload": {"text": "hop test"},
            }
        )
    )
    assert envelope is not None
    extra = (envelope.get("message_info") or {}).get("extra", {})
    relay_envelope = extra.get("relay_envelope", {}) if isinstance(extra, dict) else {}
    assert relay_envelope.get("hop") == 1


def test_adapter_uses_standard_on_platform_message_pipeline() -> None:
    """Inherited AdapterBase.on_platform_message should forward accepted messages."""

    adapter = build_adapter()
    sink = adapter.core_sink
    assert isinstance(sink, DummySink)
    __import__("asyncio").run(
        adapter.on_platform_message(
            {
                "from_bot": "114514",
                "from_bot_name": "流光",
                "to_bot": "223123",
                "to_bot_name": "清风",
                "channel": "transaction",
                "intent": "request",
                "expect_reply": True,
                "reply_budget": 3,
                "terminal": False,
                "hop": 0,
                "ttl": 4,
                "message_id": "m-pipeline",
                "conversation_id": "c-pipeline",
                "trace_id": "t-pipeline",
                "payload": {"text": "请帮我确认一下"},
            }
        )
    )
    assert len(sink.captured) == 1

    __import__("asyncio").run(
        adapter.on_platform_message(
            {
                "from_bot": "114514",
                "from_bot_name": "流光",
                "to_bot": "999999",
                "to_bot_name": "别的 bot",
                "channel": "transaction",
                "intent": "request",
                "message_id": "m-filtered",
                "conversation_id": "c-filtered",
                "trace_id": "t-filtered",
                "payload": {"text": "wrong target"},
            }
        )
    )
    assert len(sink.captured) == 1


def test_adapter_health_check_uses_mqtt_client_state() -> None:
    """MQTT health must not use BaseAdapter's ws/http transport status."""

    class StubClient:
        def __init__(self, connected: bool) -> None:
            self.connected = connected

        def is_connected(self) -> bool:
            return self.connected

    adapter = build_adapter()
    assert __import__("asyncio").run(adapter.health_check()) is False
    adapter._mqtt_client = StubClient(True)
    assert __import__("asyncio").run(adapter.health_check()) is True
    adapter._mqtt_client = StubClient(False)
    adapter._reconnecting = True
    assert __import__("asyncio").run(adapter.health_check()) is True


def test_adapter_reconnect_does_not_stop_mqtt_loop() -> None:
    """Framework health reconnect should not tear down the paho client."""

    adapter = build_adapter()
    adapter._mqtt_client = object()
    __import__("asyncio").run(adapter.reconnect())
    assert adapter._mqtt_client is not None


def test_adapter_stops_existing_mqtt_client_before_reconnect() -> None:
    """Reconnect setup should not leak old paho network loops."""

    class StubClient:
        def __init__(self) -> None:
            self.loop_stopped = False
            self.disconnected = False

        def loop_stop(self) -> None:
            self.loop_stopped = True

        def disconnect(self) -> None:
            self.disconnected = True

    adapter = build_adapter()
    client = StubClient()
    adapter._mqtt_client = client
    adapter._stop_mqtt_client()
    assert client.loop_stopped is True
    assert client.disconnected is True
    assert adapter._mqtt_client is None


def test_adapter_partner_resolution_handles_malformed_context() -> None:
    """Malformed relay_context should fall back safely to the first partner."""

    adapter = build_adapter()
    partner = adapter._resolve_partner_from_message_envelope(
        {
            "message_info": {
                "platform": "bot_relay",
                "extra": {"relay_context": []},
            },
            "message_segment": [],
            "raw_message": {},
        }
    )
    assert partner.bot_id == "114514"

    partner = adapter._resolve_partner_from_message_envelope(
        {
            "message_info": {
                "platform": "bot_relay",
                "extra": {"relay_context": {"peer_bot_id": ""}},
            },
            "message_segment": [],
            "raw_message": {},
        }
    )
    assert partner.bot_id == "114514"


def test_adapter_no_custom_process_incoming_path() -> None:
    """Inbound dispatch should use AdapterBase.on_platform_message(raw)."""

    assert not hasattr(BotRelayAdapter, "_process_incoming")


# ── Actions ───────────────────────────────────────────────────────────

def test_relay_action_isolated_to_bot_relay_chatter() -> None:
    assert BotRelaySendTextAction.chatter_allow == ["bot_relay_chatter"]


def test_bot_relay_chatter_blocks_non_relay_usables() -> None:
    """Relay chatter must not expose unrelated global actions/tools."""

    class RelayUsable:
        @classmethod
        def get_signature(cls) -> str:
            return "bot_private_relay:action:send_text"

    class ForeignUsable:
        @classmethod
        def get_signature(cls) -> str:
            return "emoji_sticker:action:send_emoji"

    class UnsignedUsable:
        pass

    assert BotRelayChatter._is_relay_usable(RelayUsable) is True
    assert BotRelayChatter._is_relay_usable(ForeignUsable) is False
    assert BotRelayChatter._is_relay_usable(UnsignedUsable) is False


# ── Chatter ───────────────────────────────────────────────────────────

def test_bot_relay_chatter_uses_base_chatter_not_default_chatter() -> None:
    assert issubclass(BotRelayChatter, BaseChatter)
    assert DefaultChatter not in BotRelayChatter.__mro__


def test_bot_relay_context_summary_contains_relay_fields() -> None:
    extra = BotRelayChatter._format_relay_context(
        {
            "peer_bot_name": "流光",
            "peer_bot_id": "114514",
            "channel": "transaction",
            "intent": "request",
            "state": "pending_reply",
            "phase": None,
            "expect_reply": True,
            "reply_budget": 3,
            "terminal": False,
            "allowed_responders": ["223123"],
        }
    )
    assert "对端 bot：流光（id=114514）" in extra
    assert "channel：transaction" in extra
    assert "intent：request" in extra
    assert "reply_budget：3" in extra
    assert "allowed_responders：['223123']" in extra


def test_bot_relay_context_summary_warns_when_no_reply_expected() -> None:
    extra = BotRelayChatter._format_relay_context(
        {
            "peer_bot_name": "流光",
            "peer_bot_id": "114514",
            "channel": "transaction",
            "intent": "notify",
            "expect_reply": False,
            "reply_budget": 0,
            "terminal": True,
        }
    )
    assert "当前协议不期待你自动继续回复" in extra


def test_bot_relay_chatter_waits_when_context_does_not_expect_reply() -> None:
    assert BotRelayChatter._should_respond({"expect_reply": False}, "223123") is False
    assert BotRelayChatter._should_respond({"expect_reply": True, "terminal": True}, "223123") is False
    assert BotRelayChatter._should_respond({"expect_reply": True, "terminal": False}, "223123") is False
    assert BotRelayChatter._should_respond({"expect_reply": True, "terminal": False, "allowed_responders": []}, "223123") is False
    assert BotRelayChatter._should_respond({"expect_reply": True, "terminal": False, "allowed_responders": ["114514"]}, "223123") is False
    assert BotRelayChatter._should_respond({"expect_reply": True, "terminal": False, "allowed_responders": ["223123"]}, "223123") is True
    assert isinstance(Wait(), Wait)


def test_bot_relay_chatter_sends_plain_text_when_no_tool_call() -> None:
    """Plain LLM text must still leave through the relay send action."""

    class FakeRequest:
        def __init__(self) -> None:
            self.payloads = []
            self.stream_modes = []

        def add_payload(self, payload):
            self.payloads.append(payload)
            return self

        async def send(self, stream=True):
            self.stream_modes.append(stream)
            return FakeResponse()

    class FakeResponse:
        message = "收到，我会确认。"
        call_list = []

        def __await__(self):
            async def _collect():
                return self.message

            return _collect().__await__()

    plugin = BotPrivateRelayPlugin(build_config())
    chatter = BotRelayChatter(stream_id="s1", plugin=plugin)
    sent = {}

    fake_request = FakeRequest()

    def create_request(*args, **kwargs):
        return fake_request

    async def inject_usables(request):
        return object()

    async def exec_llm_usable(usable_cls, message, **kwargs):
        sent["usable_cls"] = usable_cls
        sent["message"] = message
        sent["kwargs"] = kwargs
        return True, {"status": "sent"}

    chatter.create_request = create_request  # type: ignore[method-assign]
    chatter.inject_usables = inject_usables  # type: ignore[method-assign]
    chatter.exec_llm_usable = exec_llm_usable  # type: ignore[method-assign]

    unread = type(
        "Msg",
        (),
        {
            "message_id": "m1",
            "extra": {"relay_context": {"expect_reply": True}},
            "processed_plain_text": "smoke request: 请回复确认",
            "content": "smoke request: 请回复确认",
            "sender_id": "114514",
            "sender_name": "流光",
            "sender_role": "bot",
            "sender_cardname": "",
            "time": 0,
        },
    )()
    stream = type(
        "Stream",
        (),
        {
            "context": type(
                "Context",
                (),
                {"history_messages": []},
            )()
        },
    )()
    result = __import__("asyncio").run(
        chatter._run_relay_turn(
            chat_stream=stream,
            unread_text="smoke request: 请回复确认",
            unread_messages=[unread],
            relay_context={
                "peer_bot_id": "114514",
                "peer_bot_name": "流光",
                "channel": "transaction",
                "intent": "request",
                "expect_reply": True,
                "reply_budget": 3,
                "terminal": False,
            },
        )
    )
    assert isinstance(result, Success)
    assert fake_request.stream_modes == [False]
    assert sent["kwargs"] == {"content": "收到，我会确认。"}
    assert sent["message"] is unread


def test_bot_relay_chatter_falls_back_to_stream_when_non_stream_fails() -> None:
    """Some providers may require streaming, but non-stream should be preferred."""

    class FakeRequest:
        def __init__(self) -> None:
            self.stream_modes = []

        async def send(self, stream=True):
            self.stream_modes.append(stream)
            if stream is False:
                raise RuntimeError("non-stream unsupported")
            return "stream-response"

    plugin = BotPrivateRelayPlugin(build_config())
    chatter = BotRelayChatter(stream_id="s1", plugin=plugin)
    request = FakeRequest()
    result = __import__("asyncio").run(chatter._send_relay_request(request))
    assert result == "stream-response"
    assert request.stream_modes == [False, True]


def test_bot_relay_chatter_runs_explicit_send_text_followup_after_transaction_tool_call() -> None:
    """Transaction tools may be followed by an explicit relay send_text call."""

    class TransactionCall:
        name = "tool-confirm_transaction"

    class SendTextCall:
        name = "action-send_text"

    class FollowResponse:
        message = "已完成事务工具调用。"
        call_list = [SendTextCall()]

        def __await__(self):
            async def _collect():
                return self.message

            return _collect().__await__()

    class InitialResponse:
        message = ""
        call_list = [TransactionCall()]

        def __init__(self) -> None:
            self.followup_stream_modes = []

        def __await__(self):
            async def _collect():
                return self.message

            return _collect().__await__()

        async def send(self, auto_append_response=True, stream=True):
            self.followup_stream_modes.append(stream)
            return FollowResponse()

    class FakeRequest:
        def __init__(self, initial: InitialResponse) -> None:
            self.initial = initial
            self.payloads = []
            self.stream_modes = []

        def add_payload(self, payload):
            self.payloads.append(payload)
            return self

        async def send(self, stream=True):
            self.stream_modes.append(stream)
            return self.initial

    plugin = BotPrivateRelayPlugin(build_config())
    chatter = BotRelayChatter(stream_id="s1", plugin=plugin)
    sent: list[dict[str, Any]] = []
    tool_calls: list[dict[str, Any]] = []
    initial = InitialResponse()
    fake_request = FakeRequest(initial)

    def create_request(*args, **kwargs):
        return fake_request

    async def inject_usables(request):
        return object()

    async def run_tool_call(calls, response, usable_map, trigger_msg):
        tool_calls.append({"calls": calls, "trigger_msg": trigger_msg})
        return [(True, True)]

    async def exec_llm_usable(usable_cls, message, **kwargs):
        sent.append({"message": message, "kwargs": kwargs})
        return True, {"status": "sent"}

    chatter.create_request = create_request  # type: ignore[method-assign]
    chatter.inject_usables = inject_usables  # type: ignore[method-assign]
    chatter.run_tool_call = run_tool_call  # type: ignore[method-assign]
    chatter.exec_llm_usable = exec_llm_usable  # type: ignore[method-assign]

    unread = type(
        "Msg",
        (),
        {
            "message_id": "m-tool",
            "extra": {"relay_context": {"expect_reply": True}},
            "processed_plain_text": "smoke request: 请回复确认",
            "content": "smoke request: 请回复确认",
            "sender_id": "114514",
            "sender_name": "流光",
            "sender_role": "bot",
            "sender_cardname": "",
            "time": 0,
        },
    )()
    stream = type(
        "Stream",
        (),
        {"context": type("Context", (), {"history_messages": []})()},
    )()

    result = __import__("asyncio").run(
        chatter._run_relay_turn(
            chat_stream=stream,
            unread_text="smoke request: 请回复确认",
            unread_messages=[unread],
            relay_context={
                "peer_bot_id": "114514",
                "peer_bot_name": "流光",
                "channel": "transaction",
                "intent": "request",
                "expect_reply": True,
                "reply_budget": 3,
                "terminal": False,
            },
        )
    )

    assert isinstance(result, Success)
    assert fake_request.stream_modes == [False]
    assert initial.followup_stream_modes == [False]
    assert len(tool_calls) == 2
    assert tool_calls[0]["calls"] == initial.call_list
    assert [getattr(call, "name", "") for call in tool_calls[1]["calls"]] == ["action-send_text"]
    assert tool_calls[1]["trigger_msg"] is unread
    assert sent == []


def test_bot_relay_chatter_suppresses_bare_followup_text_after_tool_call() -> None:
    """Bare follow-up text after tool calls is internal status, not relay content."""

    class FollowResponse:
        message = "已发送回复，简洁确认收到测试信息。"
        call_list = []

        def __await__(self):
            async def _collect():
                return self.message

            return _collect().__await__()

    class InitialResponse:
        def __init__(self) -> None:
            self.followup_stream_modes = []

        async def send(self, auto_append_response=True, stream=True):
            self.followup_stream_modes.append(stream)
            return FollowResponse()

    plugin = BotPrivateRelayPlugin(build_config())
    chatter = BotRelayChatter(stream_id="s1", plugin=plugin)
    sent: list[dict[str, Any]] = []
    response = InitialResponse()

    async def exec_llm_usable(usable_cls, message, **kwargs):
        sent.append({"message": message, "kwargs": kwargs})
        return True, {"status": "sent"}

    chatter.exec_llm_usable = exec_llm_usable  # type: ignore[method-assign]
    trigger = type(
        "Msg",
        (),
        {
            "message_id": "m-followup",
            "extra": {"relay_context": {"expect_reply": True}},
        },
    )()

    result = __import__("asyncio").run(
        chatter._run_followup_after_tools(
            response=response,
            tool_registry=object(),
            trigger_message=trigger,
        )
    )

    assert isinstance(result, Success)
    assert response.followup_stream_modes == [False]
    assert sent == []


# ── LoopGuard ─────────────────────────────────────────────────────────

def test_loop_guard_received_dedup_and_sent_boundary() -> None:
    """LoopGuard preserves params keys on STOP -- no injection."""
    store.reset_state()
    handler = LoopGuardEventHandler(plugin=BotPrivateRelayPlugin(build_config()))
    assert LoopGuardEventHandler.weight > 100

    params = {
        "message": type("M", (), {
            "message_id": "m1",
            "extra": {
                "relay_envelope": {"message_id": "m1", "hop": 0, "ttl": 4},
                "relay_context": {"reply_budget": 1},
            },
        })(),
        "envelope": None,
        "adapter_signature": "bot_private_relay:adapter:bot_relay",
    }
    initial_keys = set(params.keys())

    decision, out = handler._handle_received(params)
    assert str(decision) == "EventDecision.SUCCESS"
    assert set(out.keys()) == initial_keys  # param key invariance

    decision2, out2 = handler._handle_received(params)
    assert str(decision2) == "EventDecision.STOP"
    assert set(out2.keys()) == initial_keys  # param key invariance

    # Wrong adapter -- should STOP without injecting keys
    sent_params = {
        "message": type("M", (), {"platform": "bot_relay", "extra": {"relay_context": {}}})(),
        "envelope": None,
        "adapter_signature": "wrong:adapter:anything",
        "continue_send": True,
    }
    sent_keys = set(sent_params.keys())
    decision3, sent_out = handler._handle_sent(sent_params)
    assert str(decision3) == "EventDecision.STOP"
    assert set(sent_out.keys()) == sent_keys
    assert sent_out["continue_send"] is False

    # Missing relay_context with empty extra dict still isolates bot_relay
    # from later generic send handlers while allowing adapter delivery.
    bad_params = {
        "message": type("M", (), {"platform": "bot_relay", "extra": {}})(),
        "envelope": None,
        "adapter_signature": "bot_private_relay:adapter:bot_relay",
        "continue_send": True,
    }
    bad_keys = set(bad_params.keys())
    decision5, bad_out = handler._handle_sent(bad_params)
    assert str(decision5) == "EventDecision.STOP"
    assert set(bad_out.keys()) == bad_keys
    assert bad_out["continue_send"] is True

    malformed_params = {
        "message": type("M", (), {"platform": "bot_relay", "extra": {"relay_context": []}})(),
        "envelope": None,
        "adapter_signature": "bot_private_relay:adapter:bot_relay",
        "continue_send": True,
    }
    decision6, malformed_out = handler._handle_sent(malformed_params)
    assert str(decision6) == "EventDecision.STOP"
    assert malformed_out["continue_send"] is False

    # Correct adapter with relay_context stops later handlers but still sends.
    good_params = {
        "message": type("M", (), {"platform": "bot_relay", "extra": {"relay_context": {"intent": "notify"}}})(),
        "envelope": {"message_info": {}},
        "adapter_signature": "bot_private_relay:adapter:bot_relay",
        "continue_send": True,
    }
    good_keys = set(good_params.keys())
    decision4, good_out = handler._handle_sent(good_params)
    assert str(decision4) == "EventDecision.STOP"
    assert set(good_out.keys()) == good_keys
    assert good_out["continue_send"] is True
    envelope_extra = good_out["envelope"]["message_info"]["extra"]
    assert envelope_extra["relay_context"] == {"intent": "notify"}
    assert envelope_extra["bot_internal"] is True


# ── Command ───────────────────────────────────────────────────────────

def test_command_status() -> None:
    plugin = BotPrivateRelayPlugin(build_config())
    command = RelayCommand(plugin=plugin, stream_id="s1")
    success, text = __import__("asyncio").run(command.status())
    assert success is True
    assert "relay status:" in text


def test_command_status_replies_to_original_platform_message() -> None:
    plugin = BotPrivateRelayPlugin(build_config())
    sender = RecordingMessageSender()
    incoming_message = Message(
        message_id="msg-status",
        content="/relay status",
        processed_plain_text="/relay status",
        message_type=MessageType.TEXT,
        sender_id="user-001",
        sender_name="Alice",
        platform="qq",
        chat_type="group",
        stream_id="group-stream-001",
        group_id="group-123",
        group_name="Relay Group",
    )
    command = RelayCommand(
        plugin=plugin,
        stream_id="group-stream-001",
        message_id="msg-status",
        message=incoming_message,
    )
    original = relay_command_module.get_message_sender
    relay_command_module.get_message_sender = lambda: sender
    try:
        success, text = asyncio.run(command.execute("status"))
    finally:
        relay_command_module.get_message_sender = original

    assert success is True
    assert "relay status:" in text
    assert len(sender.calls) == 1
    reply_message, adapter_signature = sender.calls[0]
    assert adapter_signature is None
    assert reply_message.platform == "qq"
    assert reply_message.chat_type == "group"
    assert reply_message.stream_id == "group-stream-001"
    assert "relay status:" in reply_message.content
    assert "relay status:" in reply_message.processed_plain_text
    assert reply_message.extra["group_id"] == "group-123"
    assert reply_message.extra["group_name"] == "Relay Group"


def test_command_request_sends_transaction_to_default_partner() -> None:
    plugin = BotPrivateRelayPlugin(build_config())
    command = RelayCommand(plugin=plugin, stream_id="s1")
    sender = RecordingMessageSender()
    original = relay_command_module.get_message_sender
    relay_command_module.get_message_sender = lambda: sender
    try:
        success, text = asyncio.run(command.execute("request 请帮我整理会议纪要"))
    finally:
        relay_command_module.get_message_sender = original

    assert success is True
    assert "relay request sent" in text
    message, adapter_signature = sender.calls[0]
    assert adapter_signature == "bot_private_relay:adapter:bot_relay"
    assert message.platform == "bot_relay"
    assert message.chat_type == "private"
    assert message.stream_id == ""
    assert message.content == "请帮我整理会议纪要"
    relay_context = message.extra["relay_context"]
    assert relay_context["channel"] == "transaction"
    assert relay_context["intent"] == "request"
    assert relay_context["peer_bot_id"] == "114514"


def test_command_social_sends_to_explicit_partner() -> None:
    plugin = BotPrivateRelayPlugin(build_config())
    command = RelayCommand(plugin=plugin, stream_id="s1")
    sender = RecordingMessageSender()
    original = relay_command_module.get_message_sender
    relay_command_module.get_message_sender = lambda: sender
    try:
        success, text = asyncio.run(command.execute("social to 114514 我们聊一下协作节奏"))
    finally:
        relay_command_module.get_message_sender = original

    assert success is True
    assert "relay say sent" in text
    message, adapter_signature = sender.calls[0]
    assert adapter_signature == "bot_private_relay:adapter:bot_relay"
    assert message.content == "我们聊一下协作节奏"
    relay_context = message.extra["relay_context"]
    assert relay_context["channel"] == "social"
    assert relay_context["intent"] == "say"
    assert relay_context["peer_bot_id"] == "114514"
    assert relay_context["allowed_responders"] == ["114514"]


def test_command_rejects_unknown_explicit_partner() -> None:
    plugin = BotPrivateRelayPlugin(build_config())
    command = RelayCommand(plugin=plugin, stream_id="s1")
    sender = RecordingMessageSender()
    original = relay_command_module.get_message_sender
    relay_command_module.get_message_sender = lambda: sender
    try:
        success, text = asyncio.run(command.execute("request to unknown 请帮忙"))
    finally:
        relay_command_module.get_message_sender = original

    assert success is False
    assert "unknown partner" in text
    assert sender.calls == []


# ── Transaction tools ─────────────────────────────────────────────────

def test_transaction_tools_are_isolated_to_bot_relay_chatter() -> None:
    assert AcceptTransactionTool.chatter_allow == ["bot_relay_chatter"]
    assert ConfirmTransactionTool.chatter_allow == ["bot_relay_chatter"]
    assert DeclineTransactionTool.chatter_allow == ["bot_relay_chatter"]
    assert CancelTransactionTool.chatter_allow == ["bot_relay_chatter"]


def test_accept_tool_moves_pending_reply_to_accepted() -> None:
    """accept is the explicit first transaction step after request/invite."""

    store.reset_state()
    store.save_session(
        store.RelaySession(
            conversation_id="conv-accept",
            peer_bot_id="114514",
            channel="transaction",
            intent="request",
            state="pending_reply",
            terminal=False,
            expect_reply=True,
            reply_budget=3,
            allowed_responders=["114514"],
        )
    )
    plugin = BotPrivateRelayPlugin(build_config())
    success, payload = __import__("asyncio").run(
        AcceptTransactionTool(plugin).execute(
            conversation_id="conv-accept",
            caller_bot="114514",
            reason="接下任务",
        )
    )
    assert success is True
    assert payload["status"] == "ok"
    assert payload["intent"] == "accept"
    session = store.SESSION_TABLE["conv-accept"]
    assert session.intent == "accept"
    assert session.state == "accepted"
    assert session.terminal is False
    assert session.expect_reply is True
    assert session.reply_budget == 2


def test_confirm_tool_requires_accepted_state() -> None:
    """confirm only allowed from accepted state per plan §8 Phase 2."""
    store.reset_state()
    # accepted → confirm should succeed
    session = store.RelaySession(
        conversation_id="conv-001",
        peer_bot_id="114514",
        channel="transaction",
        intent="request",
        state="accepted",
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
            current_state="accepted",
            topic="整理会议纪要",
            summary="整理会议纪要",
        )
    )
    plugin = BotPrivateRelayPlugin(build_config())
    success, payload = __import__("asyncio").run(
        ConfirmTransactionTool(plugin).execute(
            conversation_id="conv-001",
            caller_bot="114514",
            reason="确认完成",
        )
    )
    assert success is True
    assert payload["status"] == "ok"
    assert store.SESSION_TABLE["conv-001"].state == "closed"
    assert store.SESSION_TABLE["conv-001"].terminal is True
    assert store.SESSION_TABLE["conv-001"].reply_budget == 0
    assert store.TRANSACTION_LOG["conv-001"].current_state == "closed"
    assert store.TRANSACTION_LOG["conv-001"].final_intent == "confirm"
    assert store.RELAY_TODOS


def test_confirm_from_pending_reply_is_rejected() -> None:
    """pending_reply → confirm is NOT a valid transition per plan."""
    store.reset_state()
    store.save_session(
        store.RelaySession(
            conversation_id="conv-002",
            peer_bot_id="114514",
            channel="transaction",
            intent="request",
            state="pending_reply",
            terminal=False,
            expect_reply=True,
            reply_budget=3,
            allowed_responders=["114514"],
        )
    )
    plugin = BotPrivateRelayPlugin(build_config())
    success, payload = __import__("asyncio").run(
        ConfirmTransactionTool(plugin).execute(
            conversation_id="conv-002",
            caller_bot="114514",
            reason="直接 confirm",
        )
    )
    assert success is False
    assert payload["status"] == "state_not_allowed"


def test_validate_transaction_action_error_codes_match_plan_enum() -> None:
    """The six error codes must match plan §8 Phase 2 enumeration exactly."""
    store.reset_state()
    allowed_codes = {
        "ok",
        "state_not_allowed",
        "not_allowed_responder",
        "reply_budget_exhausted",
        "conversation_closed",
        "invalid_payload",
    }
    manager = SessionManager()

    # missing conversation → invalid_payload
    ok, code, _ = manager.validate_transaction_action(
        conversation_id="nonexistent", action="confirm", caller_bot="114514"
    )
    assert ok is False
    assert code == "invalid_payload"
    assert code in allowed_codes

    # valid setup
    store.save_session(
        store.RelaySession(
            conversation_id="conv-check",
            peer_bot_id="114514",
            channel="transaction",
            intent="request",
            state="accepted",
            terminal=False,
            expect_reply=True,
            reply_budget=3,
            allowed_responders=["114514"],
        )
    )
    ok, code, _ = manager.validate_transaction_action(
        conversation_id="conv-check", action="confirm", caller_bot="114514"
    )
    assert ok is True
    assert code == "ok"
    assert code in allowed_codes


def test_outbound_envelope_can_infer_accept_from_session_state() -> None:
    store.reset_state()
    store.save_session(
        store.RelaySession(
            conversation_id="conv-002",
            peer_bot_id="114514",
            channel="transaction",
            intent="accept",
            state="accepted",
            terminal=False,
            expect_reply=True,
            reply_budget=2,
            allowed_responders=["114514"],
        )
    )
    envelope = SessionManager().build_outbound_envelope(
        message_envelope={
            "message_info": {
                "platform": "bot_relay",
                "user_info": {"user_id": "114514", "user_nickname": "流光"},
                "extra": {"relay_context": {"conversation_id": "conv-002", "peer_bot_id": "114514"}},
            },
            "message_segment": [{"type": "text", "data": "没问题，我来整理"}],
        },
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
    )
    assert envelope.intent == "accept"
    assert envelope.conversation_id == "conv-002"
    assert envelope.state == "accepted"
    assert envelope.terminal is False
    assert envelope.expect_reply is True


def test_outbound_envelope_can_infer_confirm_from_closed_session_intent() -> None:
    store.reset_state()
    store.save_session(
        store.RelaySession(
            conversation_id="conv-003",
            peer_bot_id="114514",
            channel="transaction",
            intent="confirm",
            state="closed",
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
                "extra": {"relay_context": {"conversation_id": "conv-003", "peer_bot_id": "114514"}},
            },
            "message_segment": [{"type": "text", "data": "确认完成"}],
        },
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
    )
    assert envelope.intent == "confirm"
    assert envelope.conversation_id == "conv-003"
    assert envelope.state == "closed"
    assert envelope.terminal is True
    assert envelope.expect_reply is False


def test_relay_send_text_context_preserves_conversation_without_inbound_intent() -> None:
    store.reset_state()
    store.save_session(
        store.RelaySession(
            conversation_id="conv-action",
            peer_bot_id="114514",
            channel="transaction",
            intent="accept",
            state="accepted",
            terminal=False,
            expect_reply=True,
            reply_budget=2,
            allowed_responders=["114514"],
        )
    )
    message = type(
        "Msg",
        (),
        {
            "extra": {
                "relay_context": {
                    "conversation_id": "conv-action",
                    "intent": "request",
                    "peer_bot_name": "流光",
                }
            }
        },
    )()
    context = type(
        "Context",
        (),
        {
            "unread_messages": [message],
            "history_messages": [],
            "current_message": None,
            "message_cache": [],
            "triggering_user_id": "",
        },
    )()
    stream = type(
        "Stream",
        (),
        {
            "context": context,
            "stream_id": "s1",
            "platform": "bot_relay",
            "chat_type": "private",
        },
    )()
    action = BotRelaySendTextAction(chat_stream=stream, plugin=BotPrivateRelayPlugin(build_config()))
    relay_context = action._relay_context_for_send(message)
    assert relay_context["conversation_id"] == "conv-action"
    assert "intent" not in relay_context
    assert relay_context["state"] == "accepted"


# ── Social session state machine (Phase 3) ────────────────────────────

def test_social_phase_advances_on_turns() -> None:
    """Phase advances opening → active → cooling → ending based on turn count."""
    store.reset_state()
    manager = SessionManager()
    session = store.RelaySession(
        conversation_id="social-001",
        peer_bot_id="114514",
        channel="social",
        intent="say",
        phase="opening",
        turn_count=0,
        max_turns=6,
        allowed_responders=["114514"],
        reply_budget=10,
    )
    store.save_session(session)

    adv = manager.advance_social_turn(session=session, max_turns=6, cooldown_seconds=5)
    assert adv.phase == "active"
    assert adv.turn_count == 1

    # Advance several turns to cooling
    for _ in range(4):
        adv = manager.advance_social_turn(session=adv, max_turns=6, cooldown_seconds=5)
    assert adv.phase == "cooling"
    assert adv.turn_count == 5

    # Advance to max_turns → ending
    adv = manager.advance_social_turn(session=adv, max_turns=6, cooldown_seconds=5)
    assert adv.phase == "ending"
    assert adv.terminal is True
    assert adv.expect_reply is False
    assert adv.turn_count == 6


def test_social_cooldown_active_while_timer_running() -> None:
    store.reset_state()
    manager = SessionManager()
    session = store.RelaySession(
        conversation_id="social-cool",
        peer_bot_id="114514",
        channel="social",
        intent="say",
        phase="cooling",
        turn_count=5,
        max_turns=6,
        cooldown_seconds=5,
        cooldown_until=time.time() + 30,
        allowed_responders=["114514"],
        reply_budget=2,
    )
    store.save_session(session)
    assert manager.is_social_in_cooldown(session) is True

    session.cooldown_until = time.time() - 1
    assert manager.is_social_in_cooldown(session) is False


def test_social_turn_consumes_reply_budget_and_closes_at_zero() -> None:
    """Social reply budget must only decrease and suppress auto-reply at zero."""
    store.reset_state()
    manager = SessionManager()
    session = store.RelaySession(
        conversation_id="social-budget",
        peer_bot_id="114514",
        channel="social",
        intent="say",
        phase="active",
        turn_count=0,
        max_turns=6,
        terminal=False,
        expect_reply=True,
        reply_budget=2,
        allowed_responders=["114514"],
    )
    store.save_session(session)

    first = manager.advance_social_turn(session=session, max_turns=6)
    assert first.reply_budget == 1
    assert first.expect_reply is True
    assert first.terminal is False
    assert first.allowed_responders == ["114514"]

    second = manager.advance_social_turn(session=first, max_turns=6)
    assert second.reply_budget == 0
    assert second.phase == "ending"
    assert second.expect_reply is False
    assert second.terminal is True
    assert second.allowed_responders == []


def test_inbound_social_session_is_synced_for_outbound_budget_control() -> None:
    """Inbound social envelopes must seed local session state before replies."""
    store.reset_state()
    manager = SessionManager()
    session = manager.sync_inbound_social_session(
        RelayEnvelope(
            conversation_id="social-inbound",
            trace_id="trace-inbound",
            from_bot="114514",
            from_bot_name="流光",
            to_bot="223123",
            to_bot_name="清风",
            channel="social",
            intent="say",
            phase="active",
            terminal=False,
            expect_reply=True,
            reply_budget=2,
            allowed_responders=["223123"],
            payload={"text": "我们聊一下协作节奏。"},
        )
    )

    assert session is not None
    assert session.conversation_id == "social-inbound"
    assert session.peer_bot_id == "114514"
    assert session.channel == "social"
    assert session.reply_budget == 2

    outbound = manager.build_outbound_envelope(
        message_envelope={
            "message_info": {
                "platform": "bot_relay",
                "extra": {
                    "relay_context": {
                        "conversation_id": "social-inbound",
                        "channel": "social",
                        "peer_bot_id": "114514",
                    }
                },
            },
            "message_segment": [{"type": "text", "data": "收到，我们保持节制。"}],
        },
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
    )

    assert outbound.conversation_id == "social-inbound"
    assert outbound.reply_budget == 1
    assert outbound.expect_reply is True


def test_force_social_ending_closes_session() -> None:
    store.reset_state()
    manager = SessionManager()
    session = store.RelaySession(
        conversation_id="social-end",
        peer_bot_id="114514",
        channel="social",
        intent="say",
        phase="active",
        terminal=False,
        expect_reply=True,
        reply_budget=3,
    )
    store.save_session(session)
    result = manager.force_social_ending(session)
    assert result.phase == "ending"
    assert result.terminal is True
    assert result.expect_reply is False
    assert result.reply_budget == 0


def test_expect_reply_override_priority_for_social_session() -> None:
    store.reset_state()
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


def test_social_envelope_integrates_session_phase() -> None:
    """Second call to build_social_envelope should advance the existing session."""
    store.reset_state()
    manager = SessionManager()

    # First call: no session exists, starts at opening, advances to active
    env1 = manager.build_social_envelope(
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
        text="Hello",
    )
    assert env1.phase == "active"
    assert env1.channel == "social"

    # Second call: existing session, should advance further
    env2 = manager.build_social_envelope(
        from_bot="223123",
        from_bot_name="清风",
        to_bot="114514",
        to_bot_name="流光",
        text="World",
    )
    assert env2.phase == "active"  # still active before cooling threshold
    assert env2.channel == "social"


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
    assert session.peer_bot_id == "114514"
    manager.maybe_create_memory_candidate(envelope=social)
    assert store.RELAY_MEMORY_CANDIDATES
    candidate = next(iter(store.RELAY_MEMORY_CANDIDATES.values()))
    assert candidate.peer_bot_id == "114514"


# ── MQTT reconnect & smoke-test script ───────────────────────────────

def test_adapter_reconnect_backoff_is_flapping_safe() -> None:
    """Reconnect backoff must be conservative to avoid broker Flapping protection."""
    assert BotRelayAdapter._RECONNECT_MIN_DELAY >= 10
    assert BotRelayAdapter._RECONNECT_MAX_DELAY >= 60


def test_adapter_keepalive_shorter_than_broker_idle_timeout() -> None:
    """Keepalive must be shorter than observed broker idle timeout (~30s)."""
    assert 0 < BotRelayAdapter._KEEPALIVE < 30


def test_adapter_disconnect_skips_when_reconnect_in_flight() -> None:
    """A second disconnect callback must not spawn another reconnect task."""
    adapter = build_adapter()
    adapter._reconnecting = True
    before = adapter._reconnect_task_info
    # paho v2 callback signature: client, userdata, disconnect_flags, reason_code, properties
    adapter._on_mqtt_disconnect(
        client=None, userdata=None, disconnect_flags=None, reason_code=1
    )
    assert adapter._reconnect_task_info is before  # unchanged


def test_mqtt_smoke_test_script_exists_and_is_importable_lazily() -> None:
    """Smoke test script must exist and define main()."""
    script = Path(__file__).resolve().parents[1] / "scripts" / "mqtt_smoke_test.py"
    assert script.exists()
    text = script.read_text(encoding="utf-8")
    assert "def main()" in text
    assert "8.163.34.70" in text  # default broker host documented


# ── Phase 4: command & router surface ─────────────────────────────────def test_phase4_command_and_router_surface() -> None:
    plugin = BotPrivateRelayPlugin(build_config())
    command = RelayCommand(plugin=plugin, stream_id="s1")
    ok_status, status_text = __import__("asyncio").run(command.status())
    ok_inspect, inspect_text = __import__("asyncio").run(command.inspect())
    ok_partners, partners_text = __import__("asyncio").run(command.partners())
    cwd = os.getcwd()
    tmp_dir = Path(__file__).resolve().parent / "tmp_runtime"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(tmp_dir)
    try:
        ok_export, export_text = __import__("asyncio").run(command.export())
    finally:
        os.chdir(cwd)
    assert ok_status is True and "memory_candidates=" in status_text
    assert ok_inspect is True and "transactions=" in inspect_text
    assert ok_partners is True and "114514" in partners_text
    assert ok_export is True and "relay_debug_snapshot.json" in export_text

    router = BotPrivateRelayRouter(plugin=plugin)
    health = __import__("asyncio").run(router.get_app().routes[4].endpoint())
    stats = __import__("asyncio").run(router.get_app().routes[5].endpoint())
    assert health["ok"] is True
    assert stats["debug_surface"] == "limited"
