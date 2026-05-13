"""Relay-only action wrappers around default_chatter actions."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from plugins.default_chatter.plugin import (
    PassAndWaitAction,
    SendTextAction,
    StopConversationAction,
)
from src.core.models.message import Message, MessageType
from src.kernel.logger import get_logger

from . import store

logger = get_logger("bot_private_relay_actions")


class BotRelaySendTextAction(SendTextAction):
    """Relay-only send text action."""

    chatter_allow = ["bot_relay_chatter"]

    async def _send_to_stream(
        self,
        content: Message | str,
        stream_id: str | None = None,
    ) -> bool:
        """Send relay text while preserving transaction context."""

        from src.core.managers.adapter_manager import get_adapter_manager
        from src.core.transport.message_send import get_message_sender

        try:
            if isinstance(content, Message):
                message = content
                relay_context = self._relay_context_for_send()
                if relay_context:
                    message.extra["relay_context"] = relay_context
            else:
                target_stream_id = stream_id or self.chat_stream.stream_id
                platform = self.chat_stream.platform
                chat_type = self.chat_stream.chat_type
                context = self.chat_stream.context
                bot_info = await get_adapter_manager().get_bot_info_by_platform(platform)
                content_str = str(content)
                last_msg = self._get_context_message_for_target()
                target_user_id = None
                target_user_name = None
                target_group_id = None
                target_group_name = None
                if chat_type == "group":
                    if last_msg:
                        target_group_id = last_msg.extra.get("group_id")
                        target_group_name = last_msg.extra.get("group_name")
                else:
                    target_user_id = context.triggering_user_id
                    if not target_user_id and last_msg:
                        target_user_id = last_msg.sender_id
                        target_user_name = last_msg.sender_name
                extra: dict[str, Any] = {}
                if target_user_id:
                    extra["target_user_id"] = target_user_id
                if target_user_name:
                    extra["target_user_name"] = target_user_name
                if target_group_id:
                    extra["target_group_id"] = target_group_id
                if target_group_name:
                    extra["target_group_name"] = target_group_name
                relay_context = self._relay_context_for_send(last_msg)
                if relay_context:
                    extra["relay_context"] = relay_context
                message = Message(
                    message_id=f"action_{self.action_name}_{uuid4().hex}",
                    content=content_str,
                    processed_plain_text=content_str,
                    message_type=MessageType.TEXT,
                    sender_id=bot_info.get("bot_id", "") if bot_info else "",
                    sender_name=bot_info.get("bot_name", "Bot") if bot_info else "Bot",
                    platform=platform,
                    chat_type=chat_type,
                    stream_id=target_stream_id,
                    **extra,
                )
            return await get_message_sender().send_message(message)
        except Exception as exc:
            logger.error(f"Relay send_text failed: {exc}", exc_info=True)
            return False

    def _relay_context_for_send(self, last_msg: Message | None = None) -> dict[str, object]:
        """Return outbound relay context without reusing inbound intent."""

        source = last_msg or self._get_context_message_for_target()
        relay_context = source.extra.get("relay_context", {}) if source is not None else {}
        if not isinstance(relay_context, dict):
            return {}
        conversation_id = relay_context.get("conversation_id")
        if isinstance(conversation_id, str) and conversation_id:
            session = store.get_session(conversation_id)
            if session is not None:
                return {
                    "conversation_id": session.conversation_id,
                    "channel": session.channel,
                    "peer_bot_id": session.peer_bot_id,
                    "peer_bot_name": relay_context.get("peer_bot_name", ""),
                    "state": session.state,
                    "phase": session.phase,
                    "terminal": session.terminal,
                    "expect_reply": session.expect_reply,
                    "reply_budget": session.reply_budget,
                    "allowed_responders": list(session.allowed_responders),
                }
        return {key: value for key, value in relay_context.items() if key != "intent"}


class BotRelayPassAndWaitAction(PassAndWaitAction):
    """Relay-only pass action."""

    chatter_allow = ["bot_relay_chatter"]


class BotRelayStopConversationAction(StopConversationAction):
    """Relay-only stop action."""

    chatter_allow = ["bot_relay_chatter"]
