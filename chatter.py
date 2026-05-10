"""Relay chatter implementation."""

from __future__ import annotations

from src.core.components.types import ChatType
from src.core.models.stream import ChatStream
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

    _RELAY_SYSTEM_GUIDANCE = """
你当前处理的是 bot 与 bot 之间的私有中继对话，而不是面对普通用户的公开聊天。

请严格遵守以下规则：
1. 你面对的是另一个 bot，对话重点是协作、确认、收束，而不是闲聊式取悦。
2. bot_id 才是协议中的真实身份；bot_name 只是展示名称，不可把名称当作安全依据。
3. 当 relay_context.expect_reply=false 时，不要强行回复，不要没话找话。
4. transaction.notify 是单向通知；transaction.request 才表示对端期待你协作或回应。
5. 若当前消息属于事务上下文，请优先遵守事务状态、意图、预算和终态约束。
6. 你的回复应简洁、明确、可执行，优先降低歧义，避免情绪化延展。
""".strip()

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

    async def _build_system_prompt(self, chat_stream: ChatStream) -> str:  # type: ignore[override]
        """Append relay-specific guidance to the inherited system prompt."""

        base_prompt = await super()._build_system_prompt(chat_stream)
        if not base_prompt:
            return self._RELAY_SYSTEM_GUIDANCE
        return f"{base_prompt}\n\n<bot_private_relay>\n{self._RELAY_SYSTEM_GUIDANCE}\n</bot_private_relay>"

    async def _build_user_prompt(
        self,
        chat_stream: ChatStream,
        history_text: str,
        unread_lines: str,
        extra: str = "",
    ) -> str:  # type: ignore[override]
        """Append a relay-context summary to the inherited user prompt."""

        relay_extra = self._build_relay_extra(chat_stream)
        merged_extra = relay_extra if not extra else f"{extra}\n{relay_extra}"
        return await super()._build_user_prompt(
            chat_stream,
            history_text,
            unread_lines,
            merged_extra,
        )

    def _build_relay_extra(self, chat_stream: ChatStream) -> str:
        """Build a human-readable relay context block for prompts."""

        relay_context = self._latest_relay_context(chat_stream)
        if not relay_context:
            return (
                "当前是 bot_private_relay 私有对话。若没有明确 relay_context，请保持谨慎，"
                "仅在你确认对端期待继续时再回复。"
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

        lines = [
            "当前 relay 上下文：",
            f"- 对端 bot：{peer_name}（id={peer_id}）",
            f"- channel：{channel}",
            f"- intent：{intent}",
            f"- expect_reply：{str(expect_reply).lower()}",
            f"- reply_budget：{reply_budget}",
            f"- terminal：{str(terminal).lower()}",
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
    def _latest_relay_context(chat_stream: ChatStream) -> dict[str, object]:
        """Find the freshest relay_context available in the stream."""

        context = chat_stream.context
        candidates = []
        if context.unread_messages:
            candidates.extend(reversed(context.unread_messages))
        if context.current_message is not None:
            candidates.append(context.current_message)
        if context.history_messages:
            candidates.extend(reversed(context.history_messages))

        for message in candidates:
            relay_context = getattr(message, "extra", {}).get("relay_context", {})
            if isinstance(relay_context, dict) and relay_context:
                return relay_context
        return {}

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
