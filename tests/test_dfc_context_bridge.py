"""Relay DFC context bridge tests."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pytest

from plugins.bot_private_relay.components.config import BotPrivateRelayConfig
from plugins.bot_private_relay.components.events.relay import (
    DefaultChatterRelayContextBridgeEventHandler,
    LoopGuardEventHandler,
)
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from plugins.bot_private_relay.runtime import dfc_context_bridge, relay_index
from plugins.bot_private_relay.runtime.relay_index import RelayConversationIndex
from src.app.plugin_system.types import EventType
from src.core.models.message import Message
from src.kernel.event import EventDecision


@dataclass
class BridgeConfig:
    """Test bridge config."""

    enabled: bool = True
    index_file: str = ""
    max_conversations: int = 3
    messages_per_conversation: int = 2
    max_chars: int = 3000
    max_index_conversations: int = 100
    lookback_hours: float = 72.0
    include_channels: list[str] = field(default_factory=lambda: ["social", "transaction"])
    trigger_platforms: list[str] = field(default_factory=lambda: ["qq"])
    trigger_chat_types: list[str] = field(default_factory=lambda: ["private", "group"])


@dataclass
class BridgeContext:
    """Test stream context."""

    stream_id: str = "ordinary-stream"
    chat_type: str = "private"
    unread_messages: list[Message] = field(default_factory=list)
    history_messages: list[Message] = field(default_factory=list)

    def add_history_message(self, message: Message) -> None:
        """Add history message."""

        self.history_messages.append(message)


@pytest.mark.asyncio
async def test_relay_index_keeps_allowed_fields_and_limit(tmp_path):
    """Index writes only lightweight allowed fields and keeps newest records."""

    path = tmp_path / "relay_index.json"
    now = time.time()
    for index in range(3):
        await relay_index.upsert_record(
            RelayConversationIndex(
                conversation_id=f"c{index}",
                stream_id=f"s{index}",
                peer_bot_id=f"bot{index}",
                peer_bot_name=f"Bot {index}",
                channel="social",
                updated_at=now + index,
            ),
            index_file=str(path),
            max_index_conversations=2,
            lookback_hours=72,
        )

    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload["conversations"]
    assert len(rows) == 2
    assert rows[0]["conversation_id"] == "c2"
    assert set(rows[0]) == relay_index._ALLOWED_FIELDS


@pytest.mark.asyncio
async def test_relay_index_drops_expired_and_ignores_corrupt_file(tmp_path):
    """Expired records are cleaned and corrupt JSON does not raise."""

    path = tmp_path / "relay_index.json"
    path.write_text("not json", encoding="utf-8")
    assert await relay_index.load_index(str(path)) == []

    await relay_index.upsert_record(
        RelayConversationIndex(
            conversation_id="old",
            stream_id="s-old",
            peer_bot_id="bot-old",
            peer_bot_name="Old Bot",
            channel="social",
            updated_at=time.time() - 10_000,
        ),
        index_file=str(path),
        max_index_conversations=10,
        lookback_hours=0.001,
    )
    assert await relay_index.load_index(str(path)) == []


@pytest.mark.asyncio
async def test_select_conversations_prefers_different_bots(tmp_path):
    """Bridge selects recent conversations from distinct peer bots first."""

    path = tmp_path / "relay_index.json"
    now = time.time()
    rows = [
        RelayConversationIndex("c1", "s1", "bot-a", "A", "social", now + 3),
        RelayConversationIndex("c2", "s2", "bot-a", "A", "social", now + 2),
        RelayConversationIndex("c3", "s3", "bot-b", "B", "transaction", now + 1),
    ]
    path.write_text(
        json.dumps({"conversations": [asdict(row) for row in rows]}, ensure_ascii=False),
        encoding="utf-8",
    )

    config = BridgeConfig(index_file=str(path), max_conversations=2)
    selected = await dfc_context_bridge.select_conversations(config)
    assert [item.conversation_id for item in selected] == ["c1", "c3"]


def test_synthetic_message_dedup_and_truncate():
    """Existing synthetic messages are removed and max_chars truncates text."""

    context = BridgeContext(
        history_messages=[
            Message(message_id="bpr-dfc-context-bridge-old", content="old"),
            Message(message_id="normal", content="keep"),
        ]
    )
    dfc_context_bridge.remove_existing_synthetic_message(context)
    assert [message.message_id for message in context.history_messages] == ["normal"]

    text = dfc_context_bridge.truncate_text("abcdef", 5)
    assert "已按 dfc_context_bridge.max_chars 裁剪" in text


def test_should_inject_respects_enabled_platform_and_chat_type():
    """Injection is disabled unless config and trigger scope match."""

    config = BridgeConfig(enabled=False)
    assert not dfc_context_bridge.should_inject_for_context(config, platform="qq", chat_type="private")

    config.enabled = True
    assert dfc_context_bridge.should_inject_for_context(config, platform="qq", chat_type="private")
    assert not dfc_context_bridge.should_inject_for_context(config, platform="bot_relay", chat_type="private")
    assert not dfc_context_bridge.should_inject_for_context(config, platform="qq", chat_type="discuss")


@pytest.mark.asyncio
async def test_loop_guard_does_not_write_index_when_bridge_disabled(tmp_path):
    """Disabled DFC bridge must avoid relay index side effects."""

    index_path = tmp_path / "relay_index.json"
    config = BotPrivateRelayConfig()
    config.dfc_context_bridge.enabled = False
    config.dfc_context_bridge.index_file = str(index_path)
    handler = LoopGuardEventHandler(BotPrivateRelayPlugin(config))
    message = Message(
        message_id="m-disabled-index",
        content="hello",
        platform="bot_relay",
        chat_type="private",
        stream_id="relay-stream",
        sender_id="bot-beta",
        relay_envelope={
            "message_id": "m-disabled-index",
            "conversation_id": "conv-disabled-index",
            "from_bot": "bot_beta",
            "channel": "social",
            "hop": 0,
            "ttl": 4,
        },
        relay_context={
            "conversation_id": "conv-disabled-index",
            "peer_bot_id": "bot_beta",
            "channel": "social",
            "reply_budget": 1,
        },
    )

    await handler.execute(EventType.ON_MESSAGE_RECEIVED, {"message": message})

    assert not index_path.exists()


@pytest.mark.asyncio
async def test_loop_guard_writes_index_when_bridge_enabled(tmp_path):
    """Enabled DFC bridge records relay conversations for later injection."""

    index_path = tmp_path / "relay_index.json"
    config = BotPrivateRelayConfig()
    config.dfc_context_bridge.enabled = True
    config.dfc_context_bridge.index_file = str(index_path)
    handler = LoopGuardEventHandler(BotPrivateRelayPlugin(config))
    message = Message(
        message_id="m-enabled-index",
        content="hello",
        platform="bot_relay",
        chat_type="private",
        stream_id="relay-stream-enabled",
        sender_id="bot-beta",
        relay_envelope={
            "message_id": "m-enabled-index",
            "conversation_id": "conv-enabled-index",
            "from_bot": "bot_beta",
            "from_bot_name": "Beta Bot",
            "channel": "social",
            "hop": 0,
            "ttl": 4,
        },
        relay_context={
            "conversation_id": "conv-enabled-index",
            "peer_bot_id": "bot_beta",
            "peer_bot_name": "Beta Bot",
            "channel": "social",
            "reply_budget": 1,
        },
    )

    decision, returned = await handler.execute(EventType.ON_MESSAGE_RECEIVED, {"message": message})

    records = await relay_index.load_index(str(index_path))
    assert decision == EventDecision.SUCCESS
    assert returned["message"] is message
    assert len(records) == 1
    assert records[0].conversation_id == "conv-enabled-index"
    assert records[0].stream_id == "relay-stream-enabled"
    assert records[0].peer_bot_id == "bot_beta"
    assert records[0].peer_bot_name == "Beta Bot"
    assert records[0].channel == "social"


@pytest.mark.asyncio
async def test_default_chatter_event_handler_injects_relay_context(tmp_path, monkeypatch):
    """ON_CHATTER_STEP injects selected relay messages into ordinary QQ context."""

    async def fake_get_recent_messages(
        stream_id: str,
        *,
        hours: float,
        limit: int,
        limit_mode: str,
    ) -> list[dict[str, object]]:
        """Return deterministic relay messages for the bridge."""

        assert stream_id == "relay-stream-bridge"
        assert hours == 72.0
        assert limit == 2
        assert limit_mode == "latest"
        return [
            {
                "time": 1_700_000_000.0,
                "sender_name": "Beta Bot",
                "processed_plain_text": "已确认今晚 20:00 汇总事项。",
            }
        ]

    monkeypatch.setattr(dfc_context_bridge, "get_recent_messages", fake_get_recent_messages)

    index_path = tmp_path / "relay_index.json"
    index_path.write_text(
        json.dumps(
            {
                "conversations": [
                    asdict(
                        RelayConversationIndex(
                            conversation_id="conv-bridge",
                            stream_id="relay-stream-bridge",
                            peer_bot_id="bot_beta",
                            peer_bot_name="Beta Bot",
                            channel="social",
                            updated_at=time.time(),
                        )
                    )
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    config = BotPrivateRelayConfig()
    config.dfc_context_bridge.enabled = True
    config.dfc_context_bridge.index_file = str(index_path)
    config.dfc_context_bridge.messages_per_conversation = 2
    context = BridgeContext(
        stream_id="ordinary-qq-stream",
        chat_type="private",
        unread_messages=[
            Message(
                message_id="ordinary-incoming",
                content="最近有什么进展？",
                platform="qq",
                chat_type="private",
                stream_id="ordinary-qq-stream",
            )
        ],
    )
    handler = DefaultChatterRelayContextBridgeEventHandler(BotPrivateRelayPlugin(config))

    decision, returned = await handler.execute(
        EventType.ON_CHATTER_STEP,
        {"stream_id": "ordinary-qq-stream", "context": context, "continue": True},
    )

    assert decision == EventDecision.SUCCESS
    assert returned["context"] is context
    assert len(context.history_messages) == 1
    synthetic = context.history_messages[0]
    assert synthetic.message_id.startswith("bpr-dfc-context-bridge-ordinary-qq-stream-")
    assert synthetic.sender_role == "system"
    assert synthetic.platform == "qq"
    assert synthetic.chat_type == "private"
    assert "bot-to-bot 私有中继对话" in synthetic.content
    assert "与 Beta Bot(bot_beta) 的近期对话" in synthetic.content
    assert "已确认今晚 20:00 汇总事项" in synthetic.content


@pytest.mark.asyncio
async def test_default_chatter_event_handler_skips_bot_relay_stream(monkeypatch):
    """bot_relay chatter streams are never fed back into the DFC bridge."""

    async def fail_inject(*args: object, **kwargs: object) -> bool:
        """Fail if the handler tries to inject into relay streams."""

        raise AssertionError("bot_relay streams must not call inject_if_needed")

    monkeypatch.setattr(dfc_context_bridge, "inject_if_needed", fail_inject)

    config = BotPrivateRelayConfig()
    config.dfc_context_bridge.enabled = True
    context = BridgeContext(
        stream_id="relay-stream-skip",
        chat_type="private",
        unread_messages=[
            Message(
                message_id="relay-incoming",
                content="relay message",
                platform="bot_relay",
                chat_type="private",
                stream_id="relay-stream-skip",
            )
        ],
    )
    handler = DefaultChatterRelayContextBridgeEventHandler(BotPrivateRelayPlugin(config))

    decision, returned = await handler.execute(
        EventType.ON_CHATTER_STEP,
        {"stream_id": "relay-stream-skip", "context": context, "continue": True},
    )

    assert decision == EventDecision.PASS
    assert returned["context"] is context
    assert context.history_messages == []


@pytest.mark.asyncio
async def test_inject_if_needed_replaces_existing_synthetic_message(tmp_path, monkeypatch):
    """Repeated injections keep ordinary history and replace old bridge context."""

    async def fake_get_recent_messages(
        stream_id: str,
        *,
        hours: float,
        limit: int,
        limit_mode: str,
    ) -> list[dict[str, object]]:
        """Return one current relay message."""

        assert stream_id == "relay-stream-dedup"
        return [
            {
                "time": 1_700_000_100.0,
                "sender_name": "Beta Bot",
                "content": "新的跨 bot 协作上下文。",
            }
        ]

    monkeypatch.setattr(dfc_context_bridge, "get_recent_messages", fake_get_recent_messages)

    index_path = tmp_path / "relay_index.json"
    index_path.write_text(
        json.dumps(
            {
                "conversations": [
                    asdict(
                        RelayConversationIndex(
                            conversation_id="conv-dedup",
                            stream_id="relay-stream-dedup",
                            peer_bot_id="bot_beta",
                            peer_bot_name="Beta Bot",
                            channel="transaction",
                            updated_at=time.time(),
                        )
                    )
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    config = BridgeConfig(index_file=str(index_path), messages_per_conversation=1)
    context = BridgeContext(
        history_messages=[
            Message(message_id="normal-history", content="保留普通历史"),
            Message(message_id="bpr-dfc-context-bridge-old", content="旧 bridge 上下文"),
        ]
    )

    injected = await dfc_context_bridge.inject_if_needed(
        context,
        stream_id="ordinary-stream",
        platform="qq",
        chat_type="private",
        config=config,
    )

    assert injected is True
    assert [message.message_id for message in context.history_messages[:1]] == ["normal-history"]
    synthetic_messages = [
        message
        for message in context.history_messages
        if message.message_id.startswith("bpr-dfc-context-bridge-")
    ]
    assert len(synthetic_messages) == 1
    assert "新的跨 bot 协作上下文" in synthetic_messages[0].content


def test_plugin_and_manifest_register_dfc_bridge_handler():
    """Plugin components and manifest include the DFC bridge event handler."""

    component_names = {component.handler_name for component in BotPrivateRelayPlugin().get_components() if hasattr(component, "handler_name")}
    manifest = json.loads((Path(__file__).resolve().parents[1] / "manifest.json").read_text(encoding="utf-8"))
    manifest_handlers = {
        item["component_name"]
        for item in manifest["include"]
        if item.get("component_type") == "event_handler"
    }

    assert "default_chatter_relay_context_bridge" in component_names
    assert "default_chatter_relay_context_bridge" in manifest_handlers
