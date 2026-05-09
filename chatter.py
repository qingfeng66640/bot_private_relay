"""Relay chatter implementation."""

from __future__ import annotations

from src.core.components.types import ChatType
from src.core.managers.stream_manager import get_stream_manager

from plugins.default_chatter.plugin import DefaultChatter


class BotRelayChatter(DefaultChatter):
    """DefaultChatter variant for bot relay private conversations.

    The only Phase 1 behavior change is a pre-check on relay_context to prevent
    automatic replies for one-way notify/terminal traffic.
    """

    chatter_name = "bot_relay_chatter"
    chatter_description = "Bot private relay chatter"
    associated_platforms = ["bot_relay"]
    chat_type = ChatType.PRIVATE

    async def sub_agent(self, unreads_text, unread_msgs, chat_stream):  # type: ignore[override]
        """Short-circuit private auto-reply when relay_context forbids it."""

        latest = unread_msgs[-1] if unread_msgs else None
        relay_context = latest.extra.get("relay_context", {}) if latest else {}
        if isinstance(relay_context, dict) and relay_context.get("expect_reply") is False:
            return {
                "reason": "relay_context.expect_reply=false，跳过自动回复",
                "should_respond": False,
            }
        return await super().sub_agent(unreads_text, unread_msgs, chat_stream)

    async def execute(self):  # type: ignore[override]
        """Run default chatter flow once the stream is allowed to respond."""

        stream_manager = get_stream_manager()
        chat_stream = await stream_manager.activate_stream(self.stream_id)
        if chat_stream is not None and chat_stream.context.unread_messages:
            latest = chat_stream.context.unread_messages[-1]
            relay_context = latest.extra.get("relay_context", {})
            if isinstance(relay_context, dict) and relay_context.get("expect_reply") is False:
                from src.core.components.base import Wait

                yield Wait()
                return
        async for result in super().execute():
            yield result
