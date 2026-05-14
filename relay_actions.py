"""Relay-only action components for bot_private_relay."""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from typing import Any
from uuid import uuid4

from src.core.components.base.action import BaseAction
from src.core.models.message import Message, MessageType
from src.kernel.logger import get_logger

from . import store

logger = get_logger("bot_private_relay_actions")


class BotRelaySendTextAction(BaseAction):
    """Relay-only send text action."""

    action_name = "send_text"
    action_description = "发送一段文本消息给对端 bot。content 只能包含要发送的正文；不要写行为理由、内心独白或格式说明。"
    chatter_allow = ["bot_relay_chatter"]

    async def execute(
        self,
        content: str,
        reply_to: str | None = None,
        at: str | None = None,
    ) -> AsyncGenerator[tuple[bool, str] | None, None]:
        """Send text to the current relay peer.

        Args:
            content: 要发送给对端 bot 的正文。
            reply_to: 兼容默认 send_text schema；bot_relay 私聊不使用引用回复。
            at: 兼容默认 send_text schema；bot_relay 私聊不使用 @。
        """

        _ = reply_to, at
        content = self._clean_content(content)
        if not content:
            yield True, "内容为空，跳过发送"
            return
        yield None
        success = await self._send_to_stream(content)
        yield success, f"已发送消息:{content}"

    @staticmethod
    def _clean_content(content: str) -> str:
        """Remove tool-call reasoning leakage and relay-irrelevant @ prefixes."""

        cleaned = re.split(r"[,，]?\s*reason[:：]", str(content or ""), flags=re.IGNORECASE)[0].strip()
        at_match = re.match(r"^\s*@([^\s]+)\s*", cleaned)
        if at_match:
            cleaned = cleaned[at_match.end():].lstrip()
        return cleaned

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


class BotRelayPassAndWaitAction(BaseAction):
    """Relay-only pass action."""

    action_name = "pass_and_wait"
    action_description = "当前 relay 对话轮次不再主动发送内容，等待对端 bot 的下一条消息；可传入 seconds 表示稍后恢复。"
    chatter_allow = ["bot_relay_chatter"]

    async def execute(self, seconds: float | None = None) -> tuple[bool, str]:
        """Wait for the next relay message or an optional timer.

        Args:
            seconds: 等待秒数；为空时等待对端新消息。
        """

        if seconds is None:
            return True, "已登记等待，将在本轮动作完成后等待新消息"
        return True, f"已登记等待，将在本轮动作完成后等待 {seconds} 秒再继续对话"


class BotRelayStopConversationAction(BaseAction):
    """Relay-only stop action."""

    action_name = "stop_conversation"
    action_description = "结束当前 relay 对话轮次，并在指定分钟数内避免主动继续。"
    chatter_allow = ["bot_relay_chatter"]

    async def execute(self, minutes: float) -> tuple[bool, str]:
        """Stop the current relay conversation turn.

        Args:
            minutes: 冷却时间，单位为分钟。
        """

        return True, f"对话已结束，将在 {minutes} 分钟后允许新对话"
