"""Tests for group reply suppression before default chatter runs."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from plugins.bot_private_relay import store
from plugins.bot_private_relay.config import BotPrivateRelayConfig, PartnerSection
from plugins.bot_private_relay.event_handler import GroupReplySuppressionEventHandler
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from src.core.components.types import EventType
from src.core.models.message import Message, MessageType
from src.kernel.event import EventDecision


def build_config() -> BotPrivateRelayConfig:
    """Build a config with group reply suppression enabled."""

    config = BotPrivateRelayConfig()
    config.group_reply_suppression.blocked_bot_ids = ["3807008939", "2899373955"]
    return config


def build_handler(config: BotPrivateRelayConfig | None = None) -> GroupReplySuppressionEventHandler:
    """Create the event handler under test."""

    return GroupReplySuppressionEventHandler(BotPrivateRelayPlugin(config or build_config()))


def build_message(
    *,
    message_id: str,
    sender_id: str,
    platform: str = "qq",
    chat_type: str = "group",
) -> Message:
    """Create a minimal incoming message for chatter-step tests."""

    return Message(
        message_id=message_id,
        content="hello",
        processed_plain_text="hello",
        message_type=MessageType.TEXT,
        sender_id=sender_id,
        sender_name=f"sender-{sender_id}",
        platform=platform,
        chat_type=chat_type,
        stream_id="qq-group-1",
    )


class FakeContext:
    """Minimal stream context for unread/history mutation tests."""

    def __init__(self, unread_messages: list[Message]) -> None:
        self.unread_messages = unread_messages
        self.history_messages: list[Message] = []
        self.triggering_user_id = unread_messages[-1].sender_id if unread_messages else None

    def add_history_message(self, message: Message) -> None:
        self.history_messages.append(message)


async def execute(handler: GroupReplySuppressionEventHandler, context: FakeContext) -> dict[str, Any]:
    """Run a chatter-step event and return mutated params."""

    params: dict[str, Any] = {
        "stream_id": "qq-group-1",
        "context": context,
        "tick": SimpleNamespace(tick_count=1),
        "chatter_gene": SimpleNamespace(),
        "continue": True,
    }
    decision, returned = await handler.execute(EventType.ON_CHATTER_STEP, params)
    assert decision in {EventDecision.SUCCESS, EventDecision.PASS}
    return returned


@pytest.fixture(autouse=True)
def reset_store() -> None:
    """Reset relay runtime state around every test."""

    store.reset_state()


@pytest.mark.asyncio
async def test_blocked_qq_group_bot_message_moves_to_history_and_stops_chatter() -> None:
    handler = build_handler()
    blocked = build_message(message_id="m1", sender_id="3807008939")
    context = FakeContext([blocked])

    params = await execute(handler, context)

    assert params["continue"] is False
    assert context.unread_messages == []
    assert context.history_messages == [blocked]
    assert context.triggering_user_id is None
    assert store.AUDIT_LOG[-1]["event"] == "group_reply_suppressed"
    assert store.AUDIT_LOG[-1]["sender_id"] == "3807008939"


@pytest.mark.asyncio
async def test_human_qq_group_message_continues_chatter() -> None:
    handler = build_handler()
    human = build_message(message_id="m1", sender_id="10001")
    context = FakeContext([human])

    params = await execute(handler, context)

    assert params["continue"] is True
    assert context.unread_messages == [human]
    assert context.history_messages == []
    assert context.triggering_user_id == "10001"
    assert store.AUDIT_LOG == []


@pytest.mark.asyncio
async def test_mixed_blocked_bot_and_human_messages_keep_human_unread() -> None:
    handler = build_handler()
    blocked = build_message(message_id="m1", sender_id="3807008939")
    human = build_message(message_id="m2", sender_id="10001")
    context = FakeContext([blocked, human])

    params = await execute(handler, context)

    assert params["continue"] is True
    assert context.unread_messages == [human]
    assert context.history_messages == [blocked]
    assert context.triggering_user_id == "10001"


@pytest.mark.asyncio
async def test_blocked_bot_private_message_is_not_suppressed() -> None:
    handler = build_handler()
    blocked_private = build_message(message_id="m1", sender_id="3807008939", chat_type="private")
    context = FakeContext([blocked_private])

    params = await execute(handler, context)

    assert params["continue"] is True
    assert context.unread_messages == [blocked_private]
    assert context.history_messages == []


@pytest.mark.asyncio
async def test_bot_relay_platform_message_is_not_suppressed() -> None:
    handler = build_handler()
    relay_message = build_message(message_id="m1", sender_id="3807008939", platform="bot_relay", chat_type="private")
    context = FakeContext([relay_message])

    params = await execute(handler, context)

    assert params["continue"] is True
    assert context.unread_messages == [relay_message]
    assert context.history_messages == []


@pytest.mark.asyncio
async def test_disabled_config_does_not_suppress() -> None:
    config = build_config()
    config.group_reply_suppression.enabled = False
    handler = build_handler(config)
    blocked = build_message(message_id="m1", sender_id="3807008939")
    context = FakeContext([blocked])

    params = await execute(handler, context)

    assert params["continue"] is True
    assert context.unread_messages == [blocked]
    assert context.history_messages == []


@pytest.mark.asyncio
async def test_empty_blocked_list_does_not_suppress() -> None:
    config = build_config()
    config.group_reply_suppression.blocked_bot_ids = []
    handler = build_handler(config)
    blocked = build_message(message_id="m1", sender_id="3807008939")
    context = FakeContext([blocked])

    params = await execute(handler, context)

    assert params["continue"] is True
    assert context.unread_messages == [blocked]
    assert context.history_messages == []


@pytest.mark.asyncio
async def test_existing_history_message_is_not_added_twice() -> None:
    handler = build_handler()
    blocked = build_message(message_id="m1", sender_id="3807008939")
    context = FakeContext([blocked])
    context.history_messages.append(blocked)

    await execute(handler, context)

    assert context.history_messages == [blocked]


def test_manifest_and_plugin_register_group_reply_suppression_handler() -> None:
    manifest = json.loads(Path(__file__).resolve().parents[1].joinpath("manifest.json").read_text(encoding="utf-8"))
    handlers = {item["component_name"] for item in manifest["include"] if item["component_type"] == "event_handler"}

    assert "group_reply_suppression" in handlers
    assert GroupReplySuppressionEventHandler in BotPrivateRelayPlugin(build_config()).get_components()


def test_group_reply_suppression_is_independent_from_relay_partners() -> None:
    config = BotPrivateRelayConfig()
    config.partners.bots = [PartnerSection(bot_id="3807008939", bot_name="风堇")]
    config.presence.allowed_partner_bots = ["3807008939"]

    assert config.group_reply_suppression.blocked_bot_ids == []
