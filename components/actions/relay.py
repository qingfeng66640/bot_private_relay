"""bot_private_relay 的中继专用 Action 组件。"""

# =============================================================================
# 中继专用 Action 组件
# =============================================================================
# Action 是 LLM 可调用的"主动型"操作，在对话中由 Chatter 通过 Tool Calling
# 触发执行。与 Tool 的区别在于 Action 可以产生副作用（如发送消息）。
#
# 本模块包含三个 Action：
# 1. BotRelaySendTextAction        - 发送文本消息给对端 bot
# 2. BotRelayPassAndWaitAction     - 本轮不回复，等待对端消息
# 3. BotRelayStopConversationAction - 结束当前对话
#
# 这些 Action 仅对 bot_relay_chatter 可用（chatter_allow 限制）。
# =============================================================================

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from typing import Annotated, Any
from uuid import uuid4

from src.app.plugin_system.base import BaseAction
from src.core.models.message import Message, MessageType
from src.kernel.logger import get_logger

from ...runtime import store

logger = get_logger("bot_private_relay_actions")


# =============================================================================
# BotRelaySendTextAction - 发送文本消息
# =============================================================================
class BotRelaySendTextAction(BaseAction):
    """中继专用发送文本 Action。

    LLM 调用此 Action 向对端 bot 发送文本消息。
    通过 MessageSender → Adapter → MQTT 的路径发出。
    """

    action_name = "send_text"
    action_description = "发送一段文本消息给对端 bot。content 只能包含要发送的正文；不要写行为理由、内心独白或格式说明。"
    chatter_allow = ["bot_relay_chatter"]  # 仅对 relay chatter 可用
    associated_types = ["text"]

    async def execute(
        self,
        content: Annotated[str, "要发送给对端 bot 的正文，不包含行为理由、内心独白或格式说明"],
        reply_to: Annotated[str | None, "兼容默认 send_text schema；bot_relay 私聊不使用引用回复"] = None,
        at: Annotated[str | None, "兼容默认 send_text schema；bot_relay 私聊不使用 @"] = None,
    ) -> AsyncGenerator[tuple[bool, str] | None, None]:
        """向当前 relay 对端发送文本消息。

        执行流程（使用 AsyncGenerator 支持分步 yield）：
        1. 清洗内容（去除 LLM 推理泄漏的 reason: 前缀、@ 前缀）
        2. yield None → 框架知道 Action 还在执行
        3. 构建 Message 对象并调用 MessageSender 发送
        4. yield 最终结果

        Args:
            content: 要发送给对端 bot 的正文。
            reply_to: 兼容默认 send_text schema；bot_relay 私聊不使用引用回复。
            at: 兼容默认 send_text schema；bot_relay 私聊不使用 @。
        """

        _ = reply_to, at  # bot_relay 不使用这些参数

        # ── 清洗内容：去除 LLM 推理泄漏 ──
        content = self._clean_content(content)
        if not content:
            yield True, "内容为空，跳过发送"
            return

        # ── 分步 yield：None 表示 Action 仍在执行 ──
        yield None

        # ── 构建消息并发送 ──
        success = await self._send_to_stream(content)
        yield success, f"已发送消息:{content}"

    @staticmethod
    def _clean_content(content: str) -> str:
        """去除推理泄漏的 reason: 后缀和无关的 @ 前缀。

        清洗两个常见问题：
        1. LLM 有时会在 content 中附加 "reason:xxx" 的推理文本 → 截断
        2. LLM 有时会加 @someone 前缀 → 移除
        """

        # 截断 reason: 后缀（LLM 有时会在 content 里写推理）
        cleaned = re.split(r"[,，]?\s*reason[:：]", str(content or ""), flags=re.IGNORECASE)[0].strip()

        # 移除 @ 前缀（bot_relay 私聊不需要 @）
        at_match = re.match(r"^\s*@([^\s]+)\s*", cleaned)
        if at_match:
            cleaned = cleaned[at_match.end():].lstrip()

        return cleaned

    async def _send_to_stream(
        self,
        content: Message | str,
        stream_id: str | None = None,
    ) -> bool:
        """发送 relay 文本消息并保留事务上下文。

        构建 Message 对象并发送。关键点：
        - 保留 relay_context，确保事务/社交会话状态被正确传递
        - 通过 bot_info 填充 sender_id/sender_name
        """

        from src.core.managers.adapter_manager import get_adapter_manager
        from src.core.transport.message_send import get_message_sender

        try:
            if isinstance(content, Message):
                # 如果传入的已经是 Message 对象，补充 relay_context
                message = content
                relay_context = self._relay_context_for_send()
                if relay_context:
                    message.extra["relay_context"] = relay_context
            else:
                # 从字符串构建 Message 对象
                target_stream_id = stream_id or self.chat_stream.stream_id
                platform = self.chat_stream.platform
                chat_type = self.chat_stream.chat_type
                context = self.chat_stream.context
                bot_info = await get_adapter_manager().get_bot_info_by_platform(platform)
                content_str = str(content)
                last_msg = self._get_context_message_for_target()

                # ── 解析目标用户/群组信息 ──
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

                # ── 构建 extra 字典 ──
                extra: dict[str, Any] = {}
                if target_user_id:
                    extra["target_user_id"] = target_user_id
                if target_user_name:
                    extra["target_user_name"] = target_user_name
                if target_group_id:
                    extra["target_group_id"] = target_group_id
                if target_group_name:
                    extra["target_group_name"] = target_group_name

                # ── 附加 relay_context（保持会话状态） ──
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
            logger.error(f"Relay 发送文本失败: {exc}", exc_info=True)
            return False

    def _relay_context_for_send(self, last_msg: Message | None = None) -> dict[str, object]:
        """返回不使用入站 intent 的出站 relay_context。

        构建出站 relay_context。关键设计：
        - 不重用入站的 intent（避免协议语义错误）
        - 从会话存储中读取最新的状态（确保一致性）
        - 如果找不到会话，回退到原 relay_context（去掉 intent）
        """

        source = last_msg or self._get_context_message_for_target()
        relay_context = source.extra.get("relay_context", {}) if source is not None else {}
        if not isinstance(relay_context, dict):
            return {}

        conversation_id = relay_context.get("conversation_id")
        if isinstance(conversation_id, str) and conversation_id:
            # 从 store 中获取最新的会话状态
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

        # 回退：使用原 relay_context，但移除 intent（避免错误语义）
        return {key: value for key, value in relay_context.items() if key != "intent"}


# =============================================================================
# BotRelayPassAndWaitAction - 等待对端消息
# =============================================================================
class BotRelayPassAndWaitAction(BaseAction):
    """中继专用等待 Action。

    LLM 调用此 Action 表示本轮不主动发送内容，等待对端 bot 的下一条消息。
    用于 bot 判断当前不需要回复的场景。
    """

    action_name = "pass_and_wait"
    action_description = "当前 relay 对话轮次不再主动发送内容，等待对端 bot 的下一条消息；可传入 seconds 表示稍后恢复。"
    chatter_allow = ["bot_relay_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        seconds: Annotated[float | None, "等待秒数；为空时等待对端新消息"] = None,
    ) -> tuple[bool, str]:
        """等待下一条 relay 消息或可选定时器。

        Args:
            seconds: 等待秒数；为空时等待对端新消息（无限等待）。
        """

        if seconds is None:
            return True, "已登记等待，将在本轮动作完成后等待新消息"
        return True, f"已登记等待，将在本轮动作完成后等待 {seconds} 秒再继续对话"


# =============================================================================
# BotRelayStopConversationAction - 结束对话
# =============================================================================
class BotRelayStopConversationAction(BaseAction):
    """中继专用停止 Action。

    LLM 调用此 Action 表示结束当前对话，并在指定时间内避免主动继续。
    """

    action_name = "stop_conversation"
    action_description = "结束当前 relay 对话轮次，并在指定分钟数内避免主动继续。"
    chatter_allow = ["bot_relay_chatter"]
    associated_types = ["text"]

    async def execute(
        self,
        minutes: Annotated[float, "结束当前 relay 对话后的冷却时间，单位为分钟"],
    ) -> tuple[bool, str]:
        """结束当前 relay 对话轮次。

        Args:
            minutes: 冷却时间，单位为分钟。
        """

        return True, f"对话已结束，将在 {minutes} 分钟后允许新对话"
