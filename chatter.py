"""Relay-specific chatter implementation."""

# =============================================================================
# BotRelayChatter - 中继对话智能体
# =============================================================================
# 这是插件的"大脑"，负责处理 bot-to-bot 之间的 LLM 对话逻辑。
# 继承自 BaseChatter，在 Neo-MoFox 的 Chatter 框架中运行。
#
# 核心职责：
# 1. 判断是否应该回复（_should_respond）
# 2. 构建 relay 专用的 system prompt（包含协议约束）
# 3. 调用 LLM 生成回复
# 4. 执行 LLM 调用的 Tool Call（事务操作）
# 5. 发送文本回复（通过 send_text Action）
# 6. 处理事务回复的重试和回退逻辑
#
# 对 LLM 的协议约束（_RELAY_SYSTEM_GUIDANCE）：
# - bot_id 才是真实身份，bot_name 只是显示名称
# - 事务 channel 必须先调用事务 Tool，不能只用 send_text
# - 状态机约束：pending_reply → accept → confirm
# - 回复要简洁、明确、可执行
# - 人设/记忆只能影响语气，不能作为事实来源
# =============================================================================

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

_LOG_PREVIEW_LIMIT = 300  # 日志中 LLM 回复预览的最大字符数


class BotRelayChatter(BaseChatter):
    """Chatter dedicated to bot-to-bot private relay conversations."""

    chatter_name = "bot_relay_chatter"
    chatter_description = "Bot private relay chatter"
    associated_platforms = ["bot_relay"]  # 仅处理 bot_relay 平台的消息
    chat_type = ChatType.PRIVATE             # 私聊类型

    # ── 允许此 Chatter 使用的 LLM Usable 签名白名单 ──
    # 只有这些 Action/Tool 对 relay chatter 可见
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

    # 事务 Tool 的名称集合（用于排序和识别）
    _TRANSACTION_TOOL_NAMES = {
        "tool-accept_transaction",
        "tool-confirm_transaction",
        "tool-decline_transaction",
        "tool-cancel_transaction",
        "tool-reschedule_transaction",
        "tool-ack_transaction",
        "tool-close_transaction",
    }

    # =========================================================================
    # System Prompt - relay 协议约束
    # =========================================================================
    # 这是 LLM 的系统指导 prompt，定义了 bot-to-bot 通信的所有协议规则。
    # 这些规则是硬约束，优先级高于人设和语气。
    _RELAY_SYSTEM_GUIDANCE = """
你当前处理的是 bot 与 bot 之间的私有中继对话，而不是面对普通用户的公开聊天。

请严格遵守以下规则：
1. 你面对的是另一个 bot，对话重点是协作、确认、收束，而不是闲聊式取悦。
2. bot_id 才是协议中的真实身份；bot_name 只是展示名称，不可把名称当作安全依据。
3. 当 relay_context.expect_reply=false 时，不要强行回复，不要没话找话。
4. transaction.notify 是单向通知；transaction.request 才表示对端期待你协作或回应。
5. 若当前消息属于事务上下文，请优先遵守事务状态、意图、预算和终态约束。
6. 调用 accept_transaction / confirm_transaction / decline_transaction / cancel_transaction / reschedule_transaction / ack_transaction / close_transaction 时，caller_bot 必须填写本机 bot_id。
7. transaction channel 且事务未关闭时，如果要发送文本，必须同时调用一个事务 tool 表示协议动作；不要只用 send_text 表达接受、确认、拒绝、取消或改期。
8. 事务状态为 pending_reply 时，先调用 accept_transaction 表示接下事务；不要从 pending_reply 直接调用 confirm_transaction。
9. accept_transaction 只表示接下原提案，不会写入 todo；accept 后由对端最终 confirm。
10. 事务状态为 accepted 时，只有当前 bot 是 allowed_responders 时才调用 confirm_transaction；confirm_transaction 会直接进入 closed 终态，并触发 todo bridge。
11. reschedule_transaction 表示提出替代方案，进入 reschedule_requested；对端若接受当前改期方案，应直接调用 confirm_transaction。
12. ack_transaction 和 close_transaction 会关闭事务；只在无需继续协作或需要收束时使用。
13. 回复应简洁、明确、可执行，优先降低歧义，避免情绪化延展。
14. 人设、记忆和 reminder 只能影响语气，不能作为事实来源；不要主动引入当前 conversation 未出现的人物、地点、宠物/道具、旧约定或背景设定。
15. social 回复也必须围绕当前消息直接回应；若对端只是提醒或确认一件事，不要扩写邀约、见面地点、共同计划或角色设定。
""".strip()

    @property
    def relay_config(self) -> BotPrivateRelayConfig:
        """Return the typed relay plugin config."""

        if not self.plugin or not isinstance(self.plugin.config, BotPrivateRelayConfig):
            raise RuntimeError("Bot relay chatter requires BotPrivateRelayConfig")
        return self.plugin.config

    # =========================================================================
    # LLM Usable 过滤
    # =========================================================================

    async def get_llm_usables(self) -> list[type[Any]]:
        """Return only relay-owned LLM usables for the relay chatter.

        过滤全局的 LLM Usable 列表，只保留 relay 专用的 Action/Tool。
        这样 LLM 就不会看到或调用其他插件的功能。
        """

        usables = await super().get_llm_usables()
        return [usable for usable in usables if self._is_relay_usable(usable)]

    @classmethod
    def _is_relay_usable(cls, usable_cls: type[Any]) -> bool:
        """Return whether a usable belongs to the relay callable surface."""

        get_signature = getattr(usable_cls, "get_signature", None)
        signature = get_signature() if callable(get_signature) else ""
        return isinstance(signature, str) and signature in cls._RELAY_USABLE_SIGNATURES

    # =========================================================================
    # 主执行入口
    # =========================================================================

    async def execute(
        self,
    ) -> AsyncGenerator[ChatterResult, WaitResumeEvent | None]:
        """Run one relay-focused LLM turn when the protocol expects a reply.

        Chatter 的主执行循环。使用 AsyncGenerator 模式支持分步执行。

        执行流程：
        1. 激活 ChatStream
        2. 获取未读消息
        3. 检查是否应该回复（_should_respond）
        4. 检查 reply_budget
        5. 构建 LLM 请求 → 发送 → 处理回复 → 执行 Tool Call
        6. 标记消息已读
        """

        stream_manager = get_stream_manager()
        chat_stream = await stream_manager.activate_stream(self.stream_id)
        if chat_stream is None:
            logger.error(f"Cannot activate relay chat stream: {self.stream_id}")
            yield Failure("无法激活 bot_relay 聊天流")
            return

        # ── 获取未读消息 ──
        unread_text, unread_messages = await self.fetch_unreads()
        if not unread_messages:
            yield Wait()
            return

        # ── 检查是否应该回复 ──
        relay_context = self._latest_relay_context(chat_stream)
        if not self._should_respond(relay_context, self.relay_config.relay.bot_id):
            await self.flush_unreads(unread_messages)
            yield Wait()
            return

        # ── 检查 reply_budget ──
        if int(relay_context.get("reply_budget", 0) or 0) <= 0:
            await self.flush_unreads(unread_messages)
            yield Stop(time=5)
            return

        # ── 执行一轮 LLM 对话 ──
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

    # =========================================================================
    # LLM 对话轮次
    # =========================================================================

    async def _run_relay_turn(
        self,
        *,
        chat_stream: ChatStream,
        unread_text: str,
        unread_messages: list[Message],
        relay_context: dict[str, Any],
    ) -> ChatterResult:
        """Build and send the relay-only LLM request, then run returned tool calls.

        一轮完整的 LLM 对话流程：
        1. 构建 LLM 请求（system prompt + user prompt + tools）
        2. 发送请求
        3. 解析回复
        4. 如果有 Tool Call → 加固参数 → 执行 → 可能触发 follow-up
        5. 如果只有文本 → 发送文本回复
        6. 处理事务中的不完整回复（只有文本没有 Tool → 重试）
        """

        # ── 构建 LLM 请求 ──
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

        # ── 注入 Tool 注册表（让 LLM 知道有哪些 Tool 可用） ──
        tool_registry = await self.inject_usables(request)

        logger.info(
            "BotRelayChatter sending LLM request: "
            f"conversation_id={relay_context.get('conversation_id')}, "
            f"peer_bot_id={relay_context.get('peer_bot_id')}, "
            f"channel={relay_context.get('channel')}, "
            f"intent={relay_context.get('intent')}, "
            f"reply_budget={relay_context.get('reply_budget')}"
        )

        # ── 发送 LLM 请求（优先 non-streaming，失败回退 streaming） ──
        response = await self._send_relay_request(request)
        await response

        # ── 解析回复 ──
        message_text = (response.message or "").strip()
        call_names = [getattr(call, "name", "") for call in response.call_list or []]
        logger.info(
            "BotRelayChatter LLM response: "
            f"text_len={len(message_text)}, "
            f"text_preview={self._preview_for_log(message_text)!r}, "
            f"tool_calls={call_names}"
        )

        # ── 分支1：LLM 返回了 Tool Call ──
        if response.call_list:
            logger.info(f"BotRelayChatter executing tool calls: {call_names}")

            # 加固事务 Tool 参数（确保 conversation_id/caller_bot 正确）
            self._harden_transaction_tool_calls(response.call_list, relay_context)

            # 检查是否是不完整的事务回复（只有 send_text 没有事务 Tool）
            if self._is_incomplete_transaction_send_text(call_names, relay_context):
                return await self._retry_incomplete_transaction_action(
                    request=request,
                    tool_registry=tool_registry,
                    trigger_message=unread_messages[-1] if unread_messages else None,
                    relay_context=relay_context,
                )

            # 事务 Tool 优先执行（_sort_transaction_tool_calls）
            sorted_calls = self._sort_transaction_tool_calls(response.call_list, relay_context)
            tool_results = await self.run_tool_call(
                sorted_calls,
                response,
                tool_registry,
                unread_messages[-1] if unread_messages else None,
            )

            sorted_call_names = [getattr(call, "name", "") for call in sorted_calls]
            if not self._should_request_followup_after_tools(sorted_call_names):
                # 没有事务 Tool → 不需要 follow-up
                logger.info("BotRelayChatter completed relay action turn without follow-up")
                return Success("bot_relay_chatter completed relay action turn")

            # 有事务 Tool → 请求 LLM follow-up 将执行结果转为自然语言
            logger.info("BotRelayChatter completed relay tool turn; requesting follow-up reply")
            return await self._run_followup_after_tools(
                response=response,
                tool_registry=tool_registry,
                trigger_message=unread_messages[-1] if unread_messages else None,
                relay_context=relay_context,
                initial_call_names=sorted_call_names,
                initial_tool_results=tool_results,
            )

        # ── 分支2：LLM 只返回了文本（没有 Tool Call） ──
        if message_text:
            if self._is_open_transaction(relay_context):
                # 事务未关闭但只发了文本 → 警告并重试
                logger.warning(
                    "BotRelayChatter suppressed bare transaction text without tool call: "
                    f"conversation_id={relay_context.get('conversation_id')}, "
                    f"state={relay_context.get('state')}, intent={relay_context.get('intent')}"
                )
                return await self._retry_incomplete_transaction_action(
                    request=request,
                    tool_registry=tool_registry,
                    trigger_message=unread_messages[-1] if unread_messages else None,
                    relay_context=relay_context,
                )
            # 非事务 → 发送纯文本回复
            return await self._send_plain_text_response(message_text, unread_messages[-1])

        # ── 分支3：LLM 既无文本也无 Tool Call ──
        logger.info("BotRelayChatter LLM produced no text and no tool calls; nothing to send")
        return Success("bot_relay_chatter completed without tool calls")

    # =========================================================================
    # Follow-up 处理（Tool 执行后的自然语言跟进）
    # =========================================================================

    @classmethod
    def _should_request_followup_after_tools(cls, call_names: list[str]) -> bool:
        """Return whether tool results need a follow-up relay action.

        只有包含事务 Tool 时才需要 follow-up。
        因为事务操作后需要将结果通知对端 bot。
        """

        return any(name.startswith("tool-") for name in call_names)

    async def _run_followup_after_tools(
        self,
        *,
        response: Any,
        tool_registry: Any,
        trigger_message: Message | None,
        relay_context: dict[str, Any] | None = None,
        initial_call_names: list[str] | None = None,
        initial_tool_results: list[tuple[bool, bool]] | None = None,
    ) -> ChatterResult:
        """Ask the LLM to turn tool results into an outbound relay reply.

        Tool 执行后，请求 LLM 生成自然语言的跟进回复。
        如果 LLM 没有生成合适的文本，使用协议回退文本。
        """

        # ── 发送 follow-up 请求 ──
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

        # ── 有 Tool Call → 再次执行 ──
        if follow_response.call_list:
            logger.info(f"BotRelayChatter executing follow-up tool calls: {follow_call_names}")
            self._harden_transaction_tool_calls(follow_response.call_list, relay_context or {})
            sorted_calls = self._sort_transaction_tool_calls(follow_response.call_list, relay_context or {})
            sorted_call_names = [getattr(call, "name", "") for call in sorted_calls]
            follow_results = await self.run_tool_call(
                sorted_calls, follow_response, tool_registry, trigger_message,
            )

            # 检查是否需要协议回退文本
            fallback_text = self._protocol_fallback_text_after_transaction_tool(
                initial_call_names or [],
                initial_tool_results or [],
                sorted_call_names,
                relay_context or {},
            )
            if fallback_text and trigger_message is not None:
                logger.warning(
                    "BotRelayChatter sending protocol fallback after transaction tool follow-up: "
                    f"conversation_id={relay_context.get('conversation_id') if relay_context else ''}, "
                    f"initial_tool_calls={initial_call_names}, follow_tool_calls={sorted_call_names}, "
                    f"follow_results={follow_results}"
                )
                return await self._send_plain_text_response(fallback_text, trigger_message)
            return Success("bot_relay_chatter completed follow-up tool turn")

        # ── 只有文本 → 检查是否应该发送 ──
        if follow_text:
            if self._should_send_followup_text(relay_context or {}, follow_text) and trigger_message is not None:
                return await self._send_plain_text_response(follow_text, trigger_message)
            logger.info("BotRelayChatter suppressed bare follow-up text after tool calls")

        # ── 既无 Tool 也无合适文本 → 协议回退 ──
        fallback_text = self._protocol_fallback_text_after_transaction_tool(
            initial_call_names or [],
            initial_tool_results or [],
            [],
            relay_context or {},
        )
        if fallback_text and trigger_message is not None:
            logger.warning(
                "BotRelayChatter sending protocol fallback after empty transaction follow-up: "
                f"conversation_id={relay_context.get('conversation_id') if relay_context else ''}, "
                f"initial_tool_calls={initial_call_names}"
            )
            return await self._send_plain_text_response(fallback_text, trigger_message)

        logger.info("BotRelayChatter follow-up produced no text and no tool calls")
        return Success("bot_relay_chatter completed relay tool turn without follow-up text")

    @classmethod
    def _protocol_fallback_text_after_transaction_tool(
        cls,
        initial_call_names: list[str],
        initial_tool_results: list[tuple[bool, bool]],
        follow_call_names: list[str],
        relay_context: dict[str, Any],
    ) -> str:
        """Return fallback text when a transaction state change was not relayed.

        当事务 Tool 执行成功后，LLM 没有生成合适的回复文本时，
        使用预定义的协议回退文本。
        """

        if not cls._is_open_transaction(relay_context):
            return ""
        if "action-send_text" in follow_call_names:
            return ""

        transaction_call = cls._successful_transaction_call(
            initial_call_names, initial_tool_results,
        )
        fallback_by_tool = {
            "tool-accept_transaction": "已接下这个事务，等待你的最终确认。",
            "tool-confirm_transaction": "已确认当前事务。",
            "tool-decline_transaction": "已拒绝当前事务。",
            "tool-cancel_transaction": "已取消当前事务。",
            "tool-reschedule_transaction": "我想调整当前事务安排，请你确认新的方案。",
            "tool-ack_transaction": "已收到并关闭当前事务。",
            "tool-close_transaction": "已关闭当前事务。",
        }
        return fallback_by_tool.get(transaction_call, "")

    @classmethod
    def _successful_transaction_call(
        cls,
        call_names: list[str],
        tool_results: list[tuple[bool, bool]],
    ) -> str:
        """Return the first transaction tool call that executed successfully."""

        for index, name in enumerate(call_names):
            if name not in cls._TRANSACTION_TOOL_NAMES:
                continue
            if index >= len(tool_results):
                continue
            _wrote_result, ok = tool_results[index]
            if ok:
                return name
        return ""

    @classmethod
    def _should_send_followup_text(cls, relay_context: dict[str, Any], text: str) -> bool:
        """Return whether tool follow-up text is outbound relay content.

        判断 follow-up 文本是否应该作为外发内容：
        - 不在开放事务中 → 不发送
        - 文本为空 → 不发送
        - 文本包含内部标记词（如"事务状态""已转为"等）→ 不发送（这是 LLM 的自我描述）
        """

        if not cls._is_open_transaction(relay_context):
            return False
        normalized = " ".join(text.strip().split())
        if not normalized:
            return False
        internal_markers = (
            "事务状态", "已转为", "tool", "工具", "内部",
            "执行结果", "状态=", "state=", "status=",
        )
        return not any(marker in normalized for marker in internal_markers)

    # =========================================================================
    # 事务不完整回复的重试
    # =========================================================================

    async def _retry_incomplete_transaction_action(
        self,
        *,
        request: Any,
        tool_registry: Any,
        trigger_message: Message | None,
        relay_context: dict[str, Any],
    ) -> ChatterResult:
        """Retry once when an open transaction only produced plain text.

        当事务未关闭但 LLM 只返回了文本（没有事务 Tool）时，
        追加一条重试指令并重新请求 LLM。
        """

        logger.warning(
            "BotRelayChatter retrying incomplete transaction action: "
            f"conversation_id={relay_context.get('conversation_id')}, "
            f"state={relay_context.get('state')}, intent={relay_context.get('intent')}, "
            "call_names=['action-send_text']"
        )

        # ── 追加重试指令 ──
        request.add_payload(
            LLMPayload(
                ROLE.SYSTEM,
                [Text(self._build_incomplete_transaction_retry_prompt())],
            )
        )

        # ── 重新请求 LLM ──
        retry_response = await self._send_relay_request(request)
        await retry_response
        retry_call_names = [getattr(call, "name", "") for call in retry_response.call_list or []]
        logger.info(
            "BotRelayChatter retry response: "
            f"tool_calls={retry_call_names}, "
            f"text_len={len((retry_response.message or '').strip())}"
        )

        if retry_response.call_list:
            self._harden_transaction_tool_calls(retry_response.call_list, relay_context)
            if self._is_incomplete_transaction_send_text(retry_call_names, relay_context):
                logger.warning(
                    "BotRelayChatter dropping repeated incomplete transaction send_text: "
                    f"conversation_id={relay_context.get('conversation_id')}, "
                    f"state={relay_context.get('state')}, intent={relay_context.get('intent')}"
                )
                return Success("bot_relay_chatter dropped incomplete transaction text")

            sorted_calls = self._sort_transaction_tool_calls(retry_response.call_list, relay_context)
            await self.run_tool_call(sorted_calls, retry_response, tool_registry, trigger_message)
            return Success("bot_relay_chatter completed retried transaction tool turn")

        retry_text = (retry_response.message or "").strip()
        if retry_text:
            logger.warning(
                "BotRelayChatter dropping repeated bare transaction text: "
                f"conversation_id={relay_context.get('conversation_id')}, "
                f"state={relay_context.get('state')}, intent={relay_context.get('intent')}"
            )
        return Success("bot_relay_chatter completed retry without transaction tool")

    # =========================================================================
    # 事务状态检查
    # =========================================================================

    @classmethod
    def _is_incomplete_transaction_send_text(
        cls, call_names: list[str], relay_context: dict[str, Any]
    ) -> bool:
        """Return whether an open transaction only tried to send text.

        检查条件：事务未关闭 + LLM 只调用了 send_text（没有事务 Tool）
        """

        if not cls._is_open_transaction(relay_context):
            return False
        return bool(call_names) and set(call_names) == {"action-send_text"}

    @staticmethod
    def _is_open_transaction(relay_context: dict[str, Any]) -> bool:
        """Return whether the relay context is an unfinished transaction.

        判断事务是否仍在进行中（非终态）：
        - channel=transaction
        - terminal 不为 True
        - state 为 pending_reply / accepted / reschedule_requested 之一
        """

        return (
            relay_context.get("channel") == "transaction"
            and relay_context.get("terminal") is not True
            and relay_context.get("state") in {"pending_reply", "accepted", "reschedule_requested"}
        )

    @classmethod
    def _sort_transaction_tool_calls(
        cls, calls: list[Any], relay_context: dict[str, Any]
    ) -> list[Any]:
        """Run transaction tools before send_text in the same relay turn.

        事务 Tool 优先于 send_text 执行。
        因为事务操作必须在发送文本之前完成，确保协议一致性。
        """

        if relay_context.get("channel") != "transaction":
            return calls
        return sorted(
            calls,
            key=lambda call: 0 if getattr(call, "name", "") in cls._TRANSACTION_TOOL_NAMES else 1,
        )

    # =========================================================================
    # 事务 Tool 参数加固
    # =========================================================================

    def _harden_transaction_tool_calls(
        self, calls: list[Any], relay_context: dict[str, Any]
    ) -> None:
        """Force protocol-critical transaction args from current relay context.

        用 relay_context 中的值覆盖 LLM 生成的 Tool 参数。
        防止 LLM 幻觉导致的 conversation_id 或 caller_bot 错误。

        关键：conversation_id 和 caller_bot 是安全关键参数，
        必须由系统强制设置，不能信任 LLM 的输出。
        """

        conversation_id = str(relay_context.get("conversation_id") or "").strip()
        caller_bot = self.relay_config.relay.bot_id
        if not conversation_id:
            return

        for call in calls:
            name = getattr(call, "name", "")
            if name not in self._TRANSACTION_TOOL_NAMES:
                continue

            args = getattr(call, "args", None)
            if not isinstance(args, dict):
                logger.warning(
                    "BotRelayChatter replacing non-dict transaction tool args: "
                    f"tool={name}, conversation_id={conversation_id}, caller_bot={caller_bot}"
                )
                object.__setattr__(call, "args", {})
                args = call.args

            original_conversation_id = str(args.get("conversation_id") or "")
            original_caller_bot = str(args.get("caller_bot") or "")

            # 强制覆盖
            args["conversation_id"] = conversation_id
            args["caller_bot"] = caller_bot

            if original_conversation_id != conversation_id or original_caller_bot != caller_bot:
                logger.warning(
                    "BotRelayChatter corrected transaction tool args from relay_context: "
                    f"tool={name}, original_conversation_id={original_conversation_id}, "
                    f"conversation_id={conversation_id}, original_caller_bot={original_caller_bot}, "
                    f"caller_bot={caller_bot}"
                )

    # =========================================================================
    # 文本回复发送
    # =========================================================================

    async def _send_plain_text_response(
        self, message_text: str, trigger_message: Message,
    ) -> ChatterResult:
        """Send plain LLM text through the relay send_text action."""

        from .relay_actions import BotRelaySendTextAction

        logger.info("BotRelayChatter sending plain-text fallback via send_text action")
        success, result = await self.exec_llm_usable(
            BotRelaySendTextAction, trigger_message, content=message_text,
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
        """Send relay LLM request using non-streaming mode first.

        优先使用 non-streaming 模式（更快），失败时回退到 streaming。
        """

        try:
            return await request.send(stream=False)
        except Exception as exc:
            logger.warning(f"Non-stream relay LLM request failed; retrying with stream mode: {exc}")
            return await request.send(stream=True)

    # =========================================================================
    # Prompt 构建
    # =========================================================================

    @staticmethod
    def _preview_for_log(text: str) -> str:
        """Return a single-line bounded preview for LLM response logging."""

        single_line = " ".join(text.split())
        if len(single_line) <= _LOG_PREVIEW_LIMIT:
            return single_line
        return f"{single_line[:_LOG_PREVIEW_LIMIT]}..."

    def _build_system_prompt(self, relay_context: dict[str, Any]) -> str:
        """Build the relay-specific system prompt without DefaultChatter coupling.

        构建 system prompt，包含四部分：
        1. 协议约束（_RELAY_SYSTEM_GUIDANCE）
        2. 本机身份（bot_id / bot_name）
        3. 当前协议上下文（relay_context 格式化）
        4. 人设信息（仅影响语气）
        """

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
        """Build the complete core personality block for relay expression style.

        构建人设 prompt，只用于影响回复的语气和表达风格。
        添加了严格的"事实边界"声明。
        """

        personality = get_core_config().personality
        return "\n".join(
            [
                "# 完整人设",
                "以下人设只用于影响允许回复后的语气、身份表达和语言风格；协议字段与硬门禁优先。",
                "不得把人设背景、长期记忆或 reminder 中的人物、地点、道具、旧约定当作当前事实主动加入回复。",
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
        """Build the user prompt for one relay turn.

        构建 user prompt，包含：
        1. 历史消息（同一 conversation_id 的最近 8 条）
        2. 新收到的消息
        3. relay_context 摘要
        """

        history_lines = [
            self.format_message_line(message)
            for message in self._conversation_history_messages(
                chat_stream.context.history_messages,
                relay_context,
            )[-8:]
        ]
        history = "\n".join(history_lines) or "（无历史消息）"
        unreads = unread_text or "（无新消息文本）"
        return "\n".join(
            [
                "请基于以下 bot_private_relay 私有对话上下文完成一次回应。",
                "若需要发送文本回复，必须调用 send_text action；若当前是未关闭 transaction，发送文本时必须同时调用对应事务 tool。",
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

    @staticmethod
    def _conversation_history_messages(
        messages: list[Message],
        relay_context: dict[str, Any],
    ) -> list[Message]:
        """Return history from the same relay conversation block.

        从历史消息中筛选出同一 conversation_id 的消息。
        这确保不同事务/社交对话的上下文不会互相干扰。
        """

        conversation_id = str(relay_context.get("conversation_id") or "").strip()
        if not conversation_id:
            return messages

        scoped_messages: list[Message] = []
        for message in messages:
            extra = getattr(message, "extra", None)
            message_context = extra.get("relay_context") if isinstance(extra, dict) else None
            message_conversation_id = ""
            if isinstance(message_context, dict):
                message_conversation_id = str(message_context.get("conversation_id") or "").strip()
            if message_conversation_id == conversation_id:
                scoped_messages.append(message)
        return scoped_messages

    @staticmethod
    def _build_incomplete_transaction_retry_prompt() -> str:
        """Build the retry-only instruction for incomplete transaction actions.

        当事务未关闭但 LLM 只返回了文本时，追加此指令。
        """

        return (
            "上一轮只调用了 send_text，但当前 transaction 未关闭。"
            "必须选择 accept_transaction / confirm_transaction / decline_transaction / "
            "reschedule_transaction / cancel_transaction / close_transaction / "
            "ack_transaction / pass_and_wait 之一。"
            "如果要发文本，请同时调用对应事务 tool 和 send_text。"
        )

    # =========================================================================
    # 回复决策
    # =========================================================================

    @classmethod
    def _should_respond(cls, relay_context: dict[str, Any], local_bot_id: str = "") -> bool:
        """Return whether the latest relay context expects an automatic response.

        决定是否应该自动回复的检查条件：
        1. relay_context 非空
        2. 非终态（terminal != True）
        3. 非 ending/closed 阶段
        4. reply_budget > 0
        5. expect_reply = True
        6. allowed_responders 是列表
        7. 本地 bot 在 allowed_responders 中
        """

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
        """Build a human-readable relay context block for prompts and tests.

        将 relay_context 格式化为 LLM 可读的文本块。
        """

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
        """Find the freshest relay_context available in the stream.

        从聊天流中查找最新的 relay_context。
        查找顺序：未读消息（倒序）→ 当前消息 → 历史消息（倒序）
        """

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
