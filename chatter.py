"""Relay-specific chatter implementation."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

from src.core.components.base import (
    BaseChatter,
    Failure,
    Stop,
    Success,
    Wait,
    WaitResumeEvent,
)
from src.core.components.base.chatter import ChatterResult
from src.core.components.types import ChatType
from src.core.managers.stream_manager import get_stream_manager
from src.core.models.message import Message
from src.core.models.stream import ChatStream
from src.core.config import get_core_config
from src.kernel.llm import LLMPayload, ROLE, Text
from src.kernel.logger import get_logger

from .config import BotPrivateRelayConfig

logger = get_logger("bot_private_relay_chatter")

_LOG_PREVIEW_LIMIT = 300


class BotRelayChatter(BaseChatter):
    """Chatter dedicated to bot-to-bot private relay conversations."""

    chatter_name = "bot_relay_chatter"
    chatter_description = "Bot private relay chatter"
    associated_platforms = ["bot_relay"]
    chat_type = ChatType.PRIVATE
    _RELAY_USABLE_SIGNATURES = {
        "bot_private_relay:action:send_text",
        "bot_private_relay:action:pass_and_wait",
        "bot_private_relay:action:stop_conversation",
        "bot_private_relay:tool:accept_transaction",
        "bot_private_relay:tool:confirm_transaction",
        "bot_private_relay:tool:decline_transaction",
        "bot_private_relay:tool:cancel_transaction",
        "bot_private_relay:tool:reschedule_transaction",
        "bot_private_relay:tool:ack_transaction",
        "bot_private_relay:tool:close_transaction",
    }

    _RELAY_SYSTEM_GUIDANCE = """
你当前处理的是 bot 与 bot 之间的私有中继对话，而不是面对普通用户的公开聊天。

请严格遵守以下规则：
1. 你面对的是另一个 bot，对话重点是协作、确认、收束，而不是闲聊式取悦。
2. bot_id 才是协议中的真实身份；bot_name 只是展示名称，不可把名称当作安全依据。
3. 当 relay_context.expect_reply=false 时，不要强行回复，不要没话找话。
4. transaction.notify 是单向通知；transaction.request 才表示对端期待你协作或回应。
5. 若当前消息属于事务上下文，请优先遵守事务状态、意图、预算和终态约束。
6. 调用 accept_transaction / confirm_transaction / decline_transaction / cancel_transaction / reschedule_transaction / ack_transaction / close_transaction 时，caller_bot 必须填写本机 bot_id。
7. 事务状态为 pending_reply 时，先调用 accept_transaction 表示接下事务；不要从 pending_reply 直接调用 confirm_transaction。
8. 事务状态为 accepted 时，只有在事务已完成并应关闭时才调用 confirm_transaction；confirm_transaction 会直接进入 closed 终态。
9. reschedule_transaction 只能在 pending_reply 时提出改期，进入 reschedule_requested；该状态可再 accept / decline / cancel / close。
10. ack_transaction 和 close_transaction 会关闭事务；只在无需继续协作或需要收束时使用。
11. 回复应简洁、明确、可执行，优先降低歧义，避免情绪化延展。
""".strip()

    @property
    def relay_config(self) -> BotPrivateRelayConfig:
        """Return the typed relay plugin config."""

        if not self.plugin or not isinstance(self.plugin.config, BotPrivateRelayConfig):
            raise RuntimeError("Bot relay chatter requires BotPrivateRelayConfig")
        return self.plugin.config

    async def get_llm_usables(self) -> list[type[Any]]:
        """Return only relay-owned LLM usables for the relay chatter."""

        usables = await super().get_llm_usables()
        return [usable for usable in usables if self._is_relay_usable(usable)]

    @classmethod
    def _is_relay_usable(cls, usable_cls: type[Any]) -> bool:
        """Return whether a usable belongs to the relay callable surface."""

        get_signature = getattr(usable_cls, "get_signature", None)
        signature = get_signature() if callable(get_signature) else ""
        return isinstance(signature, str) and signature in cls._RELAY_USABLE_SIGNATURES

    async def execute(
        self,
    ) -> AsyncGenerator[ChatterResult, WaitResumeEvent | None]:
        """Run one relay-focused LLM turn when the protocol expects a reply."""

        stream_manager = get_stream_manager()
        chat_stream = await stream_manager.activate_stream(self.stream_id)
        if chat_stream is None:
            logger.error(f"Cannot activate relay chat stream: {self.stream_id}")
            yield Failure("无法激活 bot_relay 聊天流")
            return

        unread_text, unread_messages = await self.fetch_unreads()
        if not unread_messages:
            yield Wait()
            return

        relay_context = self._latest_relay_context(chat_stream)
        if not self._should_respond(relay_context, self.relay_config.relay.bot_id):
            await self.flush_unreads(unread_messages)
            yield Wait()
            return

        if int(relay_context.get("reply_budget", 0) or 0) <= 0:
            await self.flush_unreads(unread_messages)
            yield Stop(time=5)
            return

        try:
            result = await self._run_relay_turn(
                chat_stream=chat_stream,
                unread_text=unread_text,
                unread_messages=unread_messages,
                relay_context=relay_context,
            )
        except Exception as exc:
            logger.error("Bot relay chatter execution failed", exc_info=True)
            await self.flush_unreads(unread_messages)
            yield Failure("bot_relay_chatter 执行失败", exc)
            return

        await self.flush_unreads(unread_messages)
        yield result

    async def _run_relay_turn(
        self,
        *,
        chat_stream: ChatStream,
        unread_text: str,
        unread_messages: list[Message],
        relay_context: dict[str, Any],
    ) -> ChatterResult:
        """Build and send the relay-only LLM request, then run returned tool calls."""

        request = self.create_request(
            task="actor",
            request_name=self.chatter_name,
            with_reminder="actor",
        )
        request.add_payload(
            LLMPayload(
                ROLE.SYSTEM,
                [Text(self._build_system_prompt(relay_context))],
            )
        )
        request.add_payload(
            LLMPayload(
                ROLE.USER,
                [Text(self._build_user_prompt(chat_stream, unread_text, relay_context))],
            )
        )
        tool_registry = await self.inject_usables(request)
        logger.info(
            "BotRelayChatter sending LLM request: "
            f"conversation_id={relay_context.get('conversation_id')}, "
            f"peer_bot_id={relay_context.get('peer_bot_id')}, "
            f"channel={relay_context.get('channel')}, "
            f"intent={relay_context.get('intent')}, "
            f"reply_budget={relay_context.get('reply_budget')}"
        )
        response = await self._send_relay_request(request)
        await response

        message_text = (response.message or "").strip()
        call_names = [getattr(call, "name", "") for call in response.call_list or []]
        logger.info(
            "BotRelayChatter LLM response: "
            f"text_len={len(message_text)}, "
            f"text_preview={self._preview_for_log(message_text)!r}, "
            f"tool_calls={call_names}"
        )

        if response.call_list:
            logger.info(f"BotRelayChatter executing tool calls: {call_names}")
            await self.run_tool_call(
                response.call_list,
                response,
                tool_registry,
                unread_messages[-1] if unread_messages else None,
            )
            if not self._should_request_followup_after_tools(call_names):
                logger.info("BotRelayChatter completed relay action turn without follow-up")
                return Success("bot_relay_chatter completed relay action turn")
            logger.info("BotRelayChatter completed relay tool turn; requesting follow-up reply")
            return await self._run_followup_after_tools(
                response=response,
                tool_registry=tool_registry,
                trigger_message=unread_messages[-1] if unread_messages else None,
            )

        if message_text:
            return await self._send_plain_text_response(message_text, unread_messages[-1])

        logger.info("BotRelayChatter LLM produced no text and no tool calls; nothing to send")
        return Success("bot_relay_chatter completed without tool calls")

    @classmethod
    def _should_request_followup_after_tools(cls, call_names: list[str]) -> bool:
        """Return whether tool results need a follow-up relay action."""

        return any(name.startswith("tool-") for name in call_names)

    async def _run_followup_after_tools(
        self,
        *,
        response: Any,
        tool_registry: Any,
        trigger_message: Message | None,
    ) -> ChatterResult:
        """Ask the LLM to turn tool results into an outbound relay reply."""

        follow_response = await self._send_relay_request(response)
        await follow_response
        follow_text = (follow_response.message or "").strip()
        follow_call_names = [
            getattr(call, "name", "") for call in follow_response.call_list or []
        ]
        logger.info(
            "BotRelayChatter follow-up response: "
            f"text_len={len(follow_text)}, "
            f"text_preview={self._preview_for_log(follow_text)!r}, "
            f"tool_calls={follow_call_names}"
        )

        if follow_response.call_list:
            logger.info(
                f"BotRelayChatter executing follow-up tool calls: {follow_call_names}"
            )
            await self.run_tool_call(
                follow_response.call_list,
                follow_response,
                tool_registry,
                trigger_message,
            )
            return Success("bot_relay_chatter completed follow-up tool turn")

        if follow_text:
            logger.info("BotRelayChatter suppressed bare follow-up text after tool calls")

        logger.info("BotRelayChatter follow-up produced no text and no tool calls")
        return Success("bot_relay_chatter completed relay tool turn without follow-up text")

    async def _send_plain_text_response(
        self,
        message_text: str,
        trigger_message: Message,
    ) -> ChatterResult:
        """Send plain LLM text through the relay send_text action."""

        from .relay_actions import BotRelaySendTextAction

        logger.info("BotRelayChatter sending plain-text fallback via send_text action")
        success, result = await self.exec_llm_usable(
            BotRelaySendTextAction,
            trigger_message,
            content=message_text,
        )
        if success:
            logger.info("BotRelayChatter plain-text fallback send_text succeeded")
            return Success(
                "bot_relay_chatter sent plain-text relay response",
                {"action_result": result},
            )
        logger.warning(f"BotRelayChatter plain-text fallback send_text failed: {result}")
        return Failure("bot_relay_chatter failed to send plain-text relay response")

    async def _send_relay_request(self, request: Any) -> Any:
        """Send relay LLM request using non-streaming mode first."""

        try:
            return await request.send(stream=False)
        except Exception as exc:
            logger.warning(
                f"Non-stream relay LLM request failed; retrying with stream mode: {exc}"
            )
            return await request.send(stream=True)

    @staticmethod
    def _preview_for_log(text: str) -> str:
        """Return a single-line bounded preview for LLM response logging."""

        single_line = " ".join(text.split())
        if len(single_line) <= _LOG_PREVIEW_LIMIT:
            return single_line
        return f"{single_line[:_LOG_PREVIEW_LIMIT]}..."

    def _build_system_prompt(self, relay_context: dict[str, Any]) -> str:
        """Build the relay-specific system prompt without DefaultChatter coupling."""

        local_bot_id = self.relay_config.relay.bot_id
        local_bot_name = self.relay_config.relay.bot_name
        return "\n".join(
            [
                self._RELAY_SYSTEM_GUIDANCE,
                "",
                "# 本机身份",
                f"- local_bot_id / caller_bot: {local_bot_id}",
                f"- local_bot_name: {local_bot_name}",
                "- 当工具参数需要 caller_bot 时，必须填写 local_bot_id，不要填写对端 bot_id。",
                "",
                "# 当前协议上下文",
                self._format_relay_context(relay_context),
                "",
                self._build_personality_prompt(),
            ]
        )

    @staticmethod
    def _build_personality_prompt() -> str:
        """Build the complete core personality block for relay expression style."""

        personality = get_core_config().personality
        return "\n".join(
            [
                "# 完整人设",
                "以下人设只用于影响允许回复后的语气、身份表达和语言风格；协议字段与硬门禁优先。",
                f"- nickname: {personality.nickname}",
                f"- alias_names: {'、'.join(personality.alias_names)}",
                f"- personality_core: {personality.personality_core}",
                f"- personality_side: {personality.personality_side}",
                f"- identity: {personality.identity}",
                f"- background_story: {personality.background_story}",
                f"- reply_style: {personality.reply_style}",
                "- safety_guidelines:",
                *[f"  - {item}" for item in personality.safety_guidelines],
                "- negative_behaviors:",
                *[f"  - {item}" for item in personality.negative_behaviors],
            ]
        )

    def _build_user_prompt(
        self,
        chat_stream: ChatStream,
        unread_text: str,
        relay_context: dict[str, Any],
    ) -> str:
        """Build the user prompt for one relay turn."""

        history_lines = [
            self.format_message_line(message)
            for message in chat_stream.context.history_messages[-8:]
        ]
        history = "\n".join(history_lines) or "（无历史消息）"
        unreads = unread_text or "（无新消息文本）"
        return "\n".join(
            [
                "请基于以下 bot_private_relay 私有对话上下文完成一次回应。",
                "若需要发送文本回复，必须调用 send_text action；若需要事务决策，优先调用对应事务 tool。",
                "",
                "# 历史消息",
                history,
                "",
                "# 新收到的消息",
                unreads,
                "",
                "# relay_context 摘要",
                self._format_relay_context(relay_context),
            ]
        )

    @classmethod
    def _should_respond(cls, relay_context: dict[str, Any], local_bot_id: str = "") -> bool:
        """Return whether the latest relay context expects an automatic response."""

        if not relay_context:
            return False
        if relay_context.get("terminal") is True:
            return False
        if relay_context.get("phase") in {"ending", "closed"}:
            return False
        reply_budget = relay_context.get("reply_budget")
        if isinstance(reply_budget, int) and reply_budget <= 0:
            return False
        if relay_context.get("expect_reply") is not True:
            return False
        allowed_responders = relay_context.get("allowed_responders")
        if not isinstance(allowed_responders, list):
            return False
        return local_bot_id in allowed_responders

    @classmethod
    def _format_relay_context(cls, relay_context: dict[str, Any]) -> str:
        """Build a human-readable relay context block for prompts and tests."""

        if not relay_context:
            return (
                "当前是 bot_private_relay 私有对话，但没有明确 relay_context；"
                "请保持谨慎，不要主动延展。"
            )

        peer_name = str(relay_context.get("peer_bot_name") or "未知对端")
        peer_id = str(relay_context.get("peer_bot_id") or "未知ID")
        channel = str(relay_context.get("channel") or "unknown")
        intent = str(relay_context.get("intent") or "unknown")
        state = str(relay_context.get("state") or "")
        phase = str(relay_context.get("phase") or "")
        expect_reply = bool(relay_context.get("expect_reply", False))
        reply_budget = int(relay_context.get("reply_budget", 0) or 0)
        terminal = bool(relay_context.get("terminal", False))
        allowed_responders = relay_context.get("allowed_responders", [])

        lines = [
            f"- 对端 bot：{peer_name}（id={peer_id}）",
            f"- channel：{channel}",
            f"- intent：{intent}",
            f"- expect_reply：{str(expect_reply).lower()}",
            f"- reply_budget：{reply_budget}",
            f"- terminal：{str(terminal).lower()}",
            f"- allowed_responders：{allowed_responders}",
        ]
        if state:
            lines.append(f"- state：{state}")
        if phase:
            lines.append(f"- phase：{phase}")
        if not expect_reply:
            lines.append("- 注意：当前协议不期待你自动继续回复，除非新的上文再次明确要求。")
        elif channel == "transaction":
            lines.append("- 注意：当前处于事务沟通，请优先给出清晰、低歧义、可执行的回应。")
        elif channel == "social":
            lines.append("- 注意：当前处于 bot 社交沟通，但仍应保持节制并尊重预算/终态。")
        return "\n".join(lines)

    @staticmethod
    def _latest_relay_context(chat_stream: ChatStream) -> dict[str, Any]:
        """Find the freshest relay_context available in the stream."""

        context = chat_stream.context
        candidates: list[Any] = []
        if context.unread_messages:
            candidates.extend(reversed(context.unread_messages))
        if context.current_message is not None:
            candidates.append(context.current_message)
        if context.history_messages:
            candidates.extend(reversed(context.history_messages))

        for message in candidates:
            extra = getattr(message, "extra", None)
            relay_context = extra.get("relay_context") if isinstance(extra, dict) else None
            if isinstance(relay_context, dict) and relay_context:
                return relay_context
        return {}
