"""Tests for bot_private_relay proactive initiation."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import pytest

from plugins.bot_private_relay import store
from plugins.bot_private_relay.config import BotPrivateRelayConfig, PartnerSection
from plugins.bot_private_relay.event_handler import LoopGuardEventHandler
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from plugins.bot_private_relay.proactive import (
    ProactiveDecision,
    build_proactive_snapshot,
    dispatch_proactive_message,
    generate_proactive_message,
    parse_decision,
    request_proactive_decision,
    run_proactive_tick,
    validate_decision,
)
from plugins.bot_private_relay.service import RelayProactiveService
from src.core.models.message import Message, MessageType
from src.kernel.event import EventDecision


class RecordingMessageSender:
    """Message sender test double."""

    def __init__(self, result: bool = True) -> None:
        self.result = result
        self.calls: list[tuple[Any, str | None]] = []

    async def send_message(self, message: Any, adapter_signature: str | None = None) -> bool:
        """Record outgoing relay messages."""

        self.calls.append((message, adapter_signature))
        return self.result


class FakeResponse:
    """LLM response test double."""

    def __init__(self, message: str) -> None:
        self.message = message
        self.payloads = []
        self.tool_calls = []

    def __await__(self):
        async def _collect() -> str:
            return self.message

        return _collect().__await__()


class FakeRequest:
    """LLM request test double."""

    def __init__(self, messages: list[str]) -> None:
        self.messages = messages
        self.payloads = []
        self.sent = 0

    def add_payload(self, payload: Any) -> None:
        self.payloads.append(payload)

    async def send(self, stream: bool = False) -> FakeResponse:
        self.sent += 1
        index = min(self.sent - 1, len(self.messages) - 1)
        return FakeResponse(self.messages[index])


def build_config() -> BotPrivateRelayConfig:
    """Build a valid relay config for proactive tests."""

    config = BotPrivateRelayConfig()
    config.relay.bot_id = "223123"
    config.relay.bot_name = "清风"
    config.partners.bot_b = PartnerSection(bot_id="114514", bot_name="流光")
    config.presence.allowed_partner_bots = ["114514"]
    config.proactive.enabled = True
    config.proactive.cooldown_seconds = 0
    config.proactive.decision_retry_interval_seconds = 0
    return config


def mark_online(bot_id: str = "114514") -> None:
    """Mark a partner online in relay presence state."""

    store.upsert_presence(
        store.PresenceRecord(
            bot_id=bot_id,
            bot_name="流光",
            status="online",
            is_known_partner=True,
        )
    )


def save_chat_hint(text: str = "跟流光约一下饭局，用 social") -> None:
    """Store an ordinary chat hint for proactive decision tests."""

    store.save_proactive_chat_hint(
        store.ProactiveChatHint(
            message_id=f"chat-{time.time()}",
            platform="qq",
            chat_type="group",
            stream_id="stream-1",
            sender_id="user-1",
            sender_name="用户",
            text=text,
        )
    )


def test_proactive_config_defaults_and_store_reset() -> None:
    config = BotPrivateRelayConfig()
    assert config.proactive.enabled is False
    assert config.proactive.transaction_enabled is False
    assert config.proactive.social_enabled is True
    assert config.proactive.allow_offline_social is False
    assert config.proactive.decision_model_task == "sub_actor"
    assert config.proactive.message_model_task == "actor"
    assert config.proactive.decision_retry_interval_seconds == 1.0
    assert config.proactive.chat_hint_snapshot_items == 20

    store.PROACTIVE_COOLDOWNS["114514"] = 1.0
    store.PROACTIVE_HOURLY_COUNTS[("send_social_message", "114514", "2026-05-28T10")] = 1
    store.save_proactive_chat_hint(
        store.ProactiveChatHint(
            message_id="chat-reset",
            platform="qq",
            chat_type="group",
            stream_id="stream-1",
            sender_id="user-1",
            sender_name="用户",
            text="联系流光",
        )
    )
    store.reset_state()
    assert store.PROACTIVE_COOLDOWNS == {}
    assert store.PROACTIVE_HOURLY_COUNTS == {}
    assert store.PROACTIVE_CHAT_HINTS == []


def test_snapshot_renders_proactive_state_and_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    store.reset_state()
    config = build_config()
    mark_online()
    save_chat_hint("跟流光约一下饭局，用 request")
    snapshot = build_proactive_snapshot(config)
    assert "# Relay Proactive State Snapshot" in snapshot
    assert "bot_id: 114514" in snapshot
    assert "## Recent Chat Flow Hints" in snapshot
    assert "跟流光约一下饭局" in snapshot

    monkeypatch.setattr("plugins.bot_private_relay.proactive_utils.count_text_tokens", lambda *_args, **_kwargs: 99999)
    from plugins.bot_private_relay.proactive_utils import fit_snapshot_to_budget

    fitted = fit_snapshot_to_budget([{"model_identifier": "fake", "max_context": 4096}], snapshot + "\n" + "x" * 5000)
    assert fitted


def test_snapshot_includes_more_recent_chat_hints() -> None:
    store.reset_state()
    config = build_config()
    mark_online()
    for index in range(25):
        store.save_proactive_chat_hint(
            store.ProactiveChatHint(
                message_id=f"chat-{index}",
                platform="qq",
                chat_type="group",
                stream_id="stream-1",
                sender_id="user-1",
                sender_name="用户",
                text=f"聊天上下文 {index}",
            )
        )

    snapshot = build_proactive_snapshot(config)

    assert "聊天上下文 5" in snapshot
    assert "聊天上下文 24" in snapshot
    assert "聊天上下文 4" not in snapshot


def test_snapshot_uses_configured_chat_hint_limit() -> None:
    store.reset_state()
    config = build_config()
    config.proactive.chat_hint_snapshot_items = 5
    mark_online()
    for index in range(8):
        store.save_proactive_chat_hint(
            store.ProactiveChatHint(
                message_id=f"chat-limit-{index}",
                platform="qq",
                chat_type="group",
                stream_id="stream-1",
                sender_id="user-1",
                sender_name="用户",
                text=f"可配置上下文 {index}",
            )
        )

    snapshot = build_proactive_snapshot(config)

    assert "可配置上下文 3" in snapshot
    assert "可配置上下文 7" in snapshot
    assert "可配置上下文 2" not in snapshot


def test_event_handler_records_ordinary_chat_as_proactive_hint() -> None:
    store.reset_state()
    handler = LoopGuardEventHandler(BotPrivateRelayPlugin(build_config()))
    message = Message(
        message_id="qq-1",
        content="跟流光约一下饭局，用 request",
        processed_plain_text="跟流光约一下饭局，用 request",
        message_type=MessageType.TEXT,
        platform="qq",
        chat_type="group",
        stream_id="stream-1",
        sender_id="user-1",
        sender_name="用户",
    )

    decision, _params = asyncio.run(handler.execute("on_message_received", {"message": message}))

    assert decision == EventDecision.PASS
    assert store.PROACTIVE_CHAT_HINTS[-1].text == "跟流光约一下饭局，用 request"


def test_decision_empty_response_retries_then_cancels(monkeypatch: pytest.MonkeyPatch) -> None:
    store.reset_state()
    config = build_config()
    fake_request = FakeRequest(["", " ", ""])
    monkeypatch.setattr("plugins.bot_private_relay.proactive.create_llm_request", lambda **_kwargs: fake_request)

    decision = asyncio.run(request_proactive_decision(config=config, model_set=[{}], snapshot="snapshot"))

    assert decision.action == "do_nothing"
    assert decision.reason == "decision_empty_after_retries"
    assert fake_request.sent == 3
    assert store.PROACTIVE_COOLDOWNS == {}
    assert store.PROACTIVE_HOURLY_COUNTS == {}


def test_parse_decision_fallbacks() -> None:
    store.reset_state()
    assert parse_decision("not json").action == "do_nothing"
    assert parse_decision(json.dumps({"action": "unknown"})).action == "do_nothing"
    decision = parse_decision(json.dumps({"action": "send_social_message", "target_bot_id": "114514", "context_hint": "问候", "reason": "test"}))
    assert decision.action == "send_social_message"
    assert decision.target_bot_id == "114514"


@pytest.mark.parametrize(
    ("decision", "mutate", "reason"),
    [
        (ProactiveDecision("send_social_message", "223123", "问候", "test"), lambda _config: None, "target_self"),
        (ProactiveDecision("send_social_message", "000", "问候", "test"), lambda _config: None, "target_unknown"),
        (ProactiveDecision("send_transaction_request", "114514", "事务", "test"), lambda config: setattr(config.proactive, "transaction_enabled", False), "transaction_disabled"),
        (ProactiveDecision("send_social_message", "114514", "问候", "test"), lambda _config: store.PROACTIVE_COOLDOWNS.__setitem__("114514", time.time() + 100), "cooldown_active"),
        (ProactiveDecision("send_social_message", "114514", "", "test"), lambda _config: None, "context_hint_empty"),
    ],
)
def test_validate_decision_hard_gates(
    decision: ProactiveDecision,
    mutate: Any,
    reason: str,
) -> None:
    store.reset_state()
    config = build_config()
    mark_online()
    mutate(config)

    ok, code = validate_decision(config, decision)

    assert ok is False
    assert code == reason


def test_validate_decision_offline_and_quota_gates() -> None:
    store.reset_state()
    config = build_config()
    config.proactive.transaction_enabled = True
    decision = ProactiveDecision("send_transaction_request", "114514", "确认一件事", "test")
    assert validate_decision(config, decision) == (False, "transaction_target_offline")

    mark_online()
    hour_key = time.strftime("%Y-%m-%dT%H", time.localtime(time.time()))
    store.PROACTIVE_HOURLY_COUNTS[("send_transaction_request", "114514", hour_key)] = config.proactive.max_per_hour
    assert validate_decision(config, decision) == (False, "hourly_quota_exhausted")


def test_validate_decision_open_transaction_blocks_new_transaction() -> None:
    store.reset_state()
    config = build_config()
    config.proactive.transaction_enabled = True
    mark_online()
    store.save_session(
        store.RelaySession(
            conversation_id="conv-open",
            peer_bot_id="114514",
            channel="transaction",
            intent="request",
            state="pending_reply",
            terminal=False,
        )
    )

    ok, code = validate_decision(config, ProactiveDecision("send_transaction_request", "114514", "确认", "test"))

    assert ok is False
    assert code == "open_transaction_exists"


def test_run_tick_without_chat_hint_skips_before_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    store.reset_state()
    config = build_config()
    mark_online()
    called = False

    def fake_model_set(_task: str) -> list[dict[str, object]]:
        nonlocal called
        called = True
        return [{}]

    monkeypatch.setattr("plugins.bot_private_relay.proactive.get_model_set_by_task", fake_model_set)

    ok = asyncio.run(run_proactive_tick(config))

    assert ok is False
    assert called is False
    assert store.AUDIT_LOG[-1]["reason_code"] == "no_recent_chat_hint"


def test_dispatch_social_uses_existing_relay_social_context(monkeypatch: pytest.MonkeyPatch) -> None:
    store.reset_state()
    config = build_config()
    sender = RecordingMessageSender()
    monkeypatch.setattr("plugins.bot_private_relay.proactive.get_message_sender", lambda: sender)

    sent = asyncio.run(
        dispatch_proactive_message(
            config=config,
            decision=ProactiveDecision("send_social_message", "114514", "问候", "test"),
            channel="social",
            text="现在方便聊两句吗？",
        )
    )

    assert sent is True
    message, adapter_signature = sender.calls[0]
    assert adapter_signature == "bot_private_relay:adapter:bot_relay"
    assert message.platform == "bot_relay"
    assert message.extra["relay_context"]["channel"] == "social"
    assert message.extra["relay_context"]["intent"] == "say"
    assert message.extra["relay_context"]["peer_bot_id"] == "114514"
    assert message.extra["relay_context"]["proactive"] is True
    assert store.DYNAMIC_SOCIAL_HOURLY_COUNTS == {}


def test_dispatch_transaction_uses_existing_request_context(monkeypatch: pytest.MonkeyPatch) -> None:
    store.reset_state()
    config = build_config()
    sender = RecordingMessageSender()
    monkeypatch.setattr("plugins.bot_private_relay.proactive.get_message_sender", lambda: sender)

    sent = asyncio.run(
        dispatch_proactive_message(
            config=config,
            decision=ProactiveDecision("send_transaction_request", "114514", "确认计划", "test"),
            channel="transaction",
            text="我想和你确认一件事。",
        )
    )

    assert sent is True
    message, _adapter_signature = sender.calls[0]
    context = message.extra["relay_context"]
    assert context["channel"] == "transaction"
    assert context["intent"] == "request"
    assert context["peer_bot_id"] == "114514"
    assert context["structured"]["source"] == "bot_private_relay_proactive"


def test_run_tick_uses_sub_actor_for_decision_and_actor_for_message(monkeypatch: pytest.MonkeyPatch) -> None:
    store.reset_state()
    config = build_config()
    mark_online()
    save_chat_hint()
    sender = RecordingMessageSender()
    requested_tasks = []
    decision = json.dumps({"action": "send_social_message", "target_bot_id": "114514", "context_hint": "问候", "reason": "test"})
    requests = [FakeRequest([decision]), FakeRequest(["现在方便聊两句吗？"])]

    def fake_model_set(task: str) -> list[dict[str, object]]:
        requested_tasks.append(task)
        return [{"task": task}]

    monkeypatch.setattr("plugins.bot_private_relay.proactive.get_model_set_by_task", fake_model_set)
    monkeypatch.setattr("plugins.bot_private_relay.proactive.create_llm_request", lambda **_kwargs: requests.pop(0))
    monkeypatch.setattr("plugins.bot_private_relay.proactive.get_message_sender", lambda: sender)

    ok = asyncio.run(run_proactive_tick(config))

    assert ok is True
    assert requested_tasks == ["sub_actor", "actor"]


def test_run_tick_success_consumes_only_proactive_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    store.reset_state()
    config = build_config()
    mark_online()
    save_chat_hint()
    sender = RecordingMessageSender()
    decision = json.dumps({"action": "send_social_message", "target_bot_id": "114514", "context_hint": "问候", "reason": "test"})
    requests = [FakeRequest([decision]), FakeRequest(["现在方便聊两句吗？"])]
    monkeypatch.setattr("plugins.bot_private_relay.proactive.get_model_set_by_task", lambda _task: [{}])
    monkeypatch.setattr("plugins.bot_private_relay.proactive.create_llm_request", lambda **_kwargs: requests.pop(0))
    monkeypatch.setattr("plugins.bot_private_relay.proactive.get_message_sender", lambda: sender)

    ok = asyncio.run(run_proactive_tick(config))

    assert ok is True
    assert len(sender.calls) == 1
    assert store.PROACTIVE_HOURLY_COUNTS
    assert store.DYNAMIC_SOCIAL_HOURLY_COUNTS == {}


def test_run_tick_send_failure_does_not_consume_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    store.reset_state()
    config = build_config()
    mark_online()
    save_chat_hint()
    sender = RecordingMessageSender(result=False)
    decision = json.dumps({"action": "send_social_message", "target_bot_id": "114514", "context_hint": "问候", "reason": "test"})
    requests = [FakeRequest([decision]), FakeRequest(["现在方便聊两句吗？"])]
    monkeypatch.setattr("plugins.bot_private_relay.proactive.get_model_set_by_task", lambda _task: [{}])
    monkeypatch.setattr("plugins.bot_private_relay.proactive.create_llm_request", lambda **_kwargs: requests.pop(0))
    monkeypatch.setattr("plugins.bot_private_relay.proactive.get_message_sender", lambda: sender)

    ok = asyncio.run(run_proactive_tick(config))

    assert ok is False
    assert store.PROACTIVE_HOURLY_COUNTS == {}
    assert store.PROACTIVE_COOLDOWNS == {}


def test_generate_message_fallback_when_model_unavailable() -> None:
    store.reset_state()
    config = build_config()
    text = asyncio.run(
        generate_proactive_message(
            config=config,
            model_set=None,
            decision=ProactiveDecision("send_transaction_request", "114514", "确认计划", "test"),
            channel="transaction",
        )
    )
    assert text == "我想和你确认一件事：确认计划"


class FakeTaskInfo:
    """Task manager return object."""

    task_id = "task-1"


class FakeTaskManager:
    """Task manager test double."""

    def __init__(self) -> None:
        self.created = []
        self.cancelled = []

    def create_task(self, coro: Any, **kwargs: Any) -> FakeTaskInfo:
        self.created.append((coro, kwargs))
        coro.close()
        return FakeTaskInfo()

    def cancel_task(self, task_id: str) -> None:
        self.cancelled.append(task_id)


def test_plugin_scheduler_disabled_does_not_register(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config()
    config.proactive.enabled = False
    plugin = BotPrivateRelayPlugin(config)
    manager = FakeTaskManager()
    monkeypatch.setattr("plugins.bot_private_relay.plugin.get_task_manager", lambda: manager)

    asyncio.run(plugin.on_plugin_loaded())

    assert manager.created == []


def test_plugin_scheduler_enabled_registers_and_unloads(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config()
    config.proactive.enabled = True
    plugin = BotPrivateRelayPlugin(config)
    manager = FakeTaskManager()
    removed = []

    class FakeScheduler:
        async def remove_schedule(self, schedule_id: str) -> bool:
            removed.append(schedule_id)
            return True

    monkeypatch.setattr("plugins.bot_private_relay.plugin.get_task_manager", lambda: manager)
    monkeypatch.setattr("src.kernel.scheduler.get_unified_scheduler", lambda: FakeScheduler())

    asyncio.run(plugin.on_plugin_loaded())
    plugin._proactive_schedule_id = "schedule-1"
    asyncio.run(plugin.on_plugin_unloaded())

    assert manager.created[0][1]["name"] == "bot_private_relay_register_proactive_schedule"
    assert removed == ["schedule-1"]
    assert manager.cancelled == ["task-1"]


def test_manifest_and_plugin_register_relay_proactive_service() -> None:
    manifest = json.loads(Path(__file__).resolve().parents[1].joinpath("manifest.json").read_text(encoding="utf-8"))
    services = {item["component_name"] for item in manifest["include"] if item["component_type"] == "service"}
    assert "relay_proactive" in services
    assert RelayProactiveService in BotPrivateRelayPlugin(build_config()).get_components()
