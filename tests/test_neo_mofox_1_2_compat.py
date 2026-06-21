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


class FakeTaskInfo:
    """Minimal task info stub for unload tests."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id


class FakeTaskManager:
    """Task manager test double."""

    def __init__(self) -> None:
        self.cancelled: list[str] = []

    def cancel_task(self, task_id: str) -> None:
        """Record cancelled task IDs."""

        self.cancelled.append(task_id)


class FakeAdapterManager:
    """Adapter manager test double."""

    def __init__(self, active: bool = True) -> None:
        self.active = active
        self.stopped: list[str] = []

    def is_adapter_active(self, signature: str) -> bool:
        """Report whether the relay adapter is active."""

        return self.active and signature == "bot_private_relay:adapter:bot_relay"

    async def stop_adapter(self, signature: str) -> bool:
        """Record stopped adapter signatures."""

        self.stopped.append(signature)
        self.active = False
        return True


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


def test_plugin_unload_stops_running_relay_adapter(monkeypatch: Any) -> None:
    """Plugin unload must stop the running adapter before component unregister."""

    plugin = BotPrivateRelayPlugin(build_config())
    task_manager = FakeTaskManager()
    adapter_manager = FakeAdapterManager(active=True)
    monkeypatch.setattr("plugins.bot_private_relay.plugin.get_task_manager", lambda: task_manager)
    monkeypatch.setattr("src.core.managers.adapter_manager.get_adapter_manager", lambda: adapter_manager)

    asyncio.run(plugin.on_plugin_unloaded())

    assert adapter_manager.stopped == ["bot_private_relay:adapter:bot_relay"]
    assert plugin._unloading is True


def test_plugin_unload_cancels_initial_proactive_tick(monkeypatch: Any) -> None:
    """Plugin unload must cancel tracked proactive registration and initial tick tasks."""

    plugin = BotPrivateRelayPlugin(build_config())
    task_manager = FakeTaskManager()
    monkeypatch.setattr("plugins.bot_private_relay.plugin.get_task_manager", lambda: task_manager)
    monkeypatch.setattr("src.core.managers.adapter_manager.get_adapter_manager", lambda: FakeAdapterManager(active=False))
    plugin._proactive_register_task_id = "register-task"
    plugin._proactive_initial_tick_task_id = "initial-task"

    asyncio.run(plugin.on_plugin_unloaded())

    assert task_manager.cancelled == ["register-task", "initial-task"]
    assert plugin._proactive_register_task_id is None
    assert plugin._proactive_initial_tick_task_id is None


def test_plugin_proactive_tick_skips_after_unload(monkeypatch: Any) -> None:
    """Scheduled proactive ticks must not run after plugin unload starts."""

    plugin = BotPrivateRelayPlugin(build_config())
    plugin._unloading = True

    class FailingService:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("proactive service should not run while unloading")

    monkeypatch.setattr("plugins.bot_private_relay.plugin.RelayProactiveService", FailingService)

    asyncio.run(plugin._proactive_tick_job())


def test_adapter_unload_cancels_tasks_before_offline_presence(monkeypatch: Any) -> None:
    """Heartbeat must be cancelled before offline presence is published."""

    async def scenario() -> list[str]:
        calls: list[str] = []
        adapter = build_adapter()
        adapter._heartbeat_task_info = FakeTaskInfo("heartbeat")
        adapter._reconnect_task_info = FakeTaskInfo("reconnect")
        adapter._mqtt_task_info = FakeTaskInfo("mqtt")

        class RecordingTaskManager:
            def cancel_task(self, task_id: str) -> None:
                calls.append(f"cancel:{task_id}")

        monkeypatch.setattr(
            "plugins.bot_private_relay.components.adapters.bot_relay.get_task_manager",
            lambda: RecordingTaskManager(),
        )

        async def publish_presence(status: str) -> None:
            calls.append(f"presence:{status}")

        adapter._publish_presence = publish_presence  # type: ignore[method-assign]
        adapter._stop_mqtt_client = lambda: calls.append("stop_mqtt")  # type: ignore[method-assign]

        await adapter.on_adapter_unloaded()
        return calls

    assert asyncio.run(scenario()) == [
        "cancel:heartbeat",
        "cancel:reconnect",
        "cancel:mqtt",
        "presence:offline",
        "stop_mqtt",
    ]


def test_adapter_stop_disconnects_before_stopping_network_loop() -> None:
    """MQTT client shutdown should send DISCONNECT before stopping the network loop."""

    class StubClient:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.on_disconnect = lambda *_args: None

        def disconnect(self) -> None:
            self.calls.append("disconnect")
            if callable(self.on_disconnect):
                self.on_disconnect(self, None, None, 0, None)

        def loop_stop(self) -> None:
            self.calls.append("loop_stop")

    adapter = build_adapter()
    client = StubClient()
    adapter._mqtt_client = client

    adapter._stop_mqtt_client()

    assert client.calls == ["disconnect", "loop_stop"]
    assert adapter._mqtt_client is None
