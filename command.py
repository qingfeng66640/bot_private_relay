"""Relay plugin commands."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from src.app.plugin_system.base import BaseCommand, cmd_route
from src.app.plugin_system.types import PermissionLevel
from src.core.models.message import Message, MessageType
from src.core.models.stream import ChatStream
from src.core.transport.message_send import get_message_sender

from .config import BotPrivateRelayConfig, PartnerSection
from .service import RelayStateService


class RelayCommand(BaseCommand):
    """Command entrypoint for ``/relay`` management."""

    command_name = "relay"
    command_description = "Inspect and manually test bot private relay runtime"
    permission_level = PermissionLevel.OWNER

    async def execute(self, message_text: str) -> tuple[bool, str]:
        """Execute relay commands, preserving free-form send text."""

        stripped = message_text.strip()
        if stripped.startswith(self.command_prefix):
            result = False, "命令文本格式错误：RelayCommand.execute 只接受去掉前缀后的子路由文本"
            await self._send_result_to_invoker(result[1])
            return result
        parts = stripped.split(maxsplit=1)
        if parts and parts[0] == self.command_name:
            result = False, "命令文本格式错误：RelayCommand.execute 只接受去掉 command_name 后的子路由文本"
            await self._send_result_to_invoker(result[1])
            return result
        if parts and parts[0] in {"request", "social"}:
            result = await self._execute_send_command(parts[0], parts[1] if len(parts) > 1 else "")
        else:
            result = await super().execute(message_text)
        await self._send_result_to_invoker(result[1])
        return result

    @cmd_route("status")
    async def status(self) -> tuple[bool, str]:
        """显示当前 relay 在线状态与会话数量。"""

        service = RelayStateService(self.plugin)
        presence_count = len(service.presence_snapshot())
        session_count = len(service.session_snapshot())
        memory_count = len(service.memory_candidate_snapshot())
        return True, f"relay status: presence={presence_count}, sessions={session_count}, memory_candidates={memory_count}"

    @cmd_route("inspect")
    async def inspect(self) -> tuple[bool, str]:
        """查看 transaction / audit 摘要。"""

        service = RelayStateService(self.plugin)
        transactions = len(service.transaction_log_snapshot())
        audits = len(service.audit_snapshot())
        return True, f"relay inspect: transactions={transactions}, audits={audits}"

    @cmd_route("close", "all")
    async def close_all(self) -> tuple[bool, str]:
        """关闭所有当前会话（插件内运行态）。"""

        count = 0
        for session in self._closeable_sessions().values():
            session.state = "closed"
            session.terminal = True
            session.expect_reply = False
            count += 1
        return True, f"relay close: closed={count}"

    @cmd_route("partners")
    async def partners(self) -> tuple[bool, str]:
        """列出当前插件配置中的伙伴 bot。"""

        partners: list[str] = []
        config = getattr(self.plugin, "config", None)
        if config is not None and hasattr(config, "partners"):
            for value in vars(config.partners).values():
                bot_id = getattr(value, "bot_id", "")
                bot_name = getattr(value, "bot_name", "")
                if bot_id:
                    partners.append(f"{bot_name or 'unknown'}({bot_id})")
        return True, "relay partners: " + ", ".join(partners)

    @cmd_route("export")
    async def export(self) -> tuple[bool, str]:
        """导出插件内调试快照到本地 data 目录。"""

        service = RelayStateService(self.plugin)
        target = service.export_debug_snapshot(Path("data"))
        return True, f"relay export: {target.name}"

    @cmd_route("request")
    async def request(self, text: str = "") -> tuple[bool, str]:
        """向默认伙伴 bot 发起 transaction.request。"""

        return await self._send_relay_message(
            channel="transaction",
            intent="request",
            text=text,
            target_bot_id=None,
        )

    @cmd_route("request", "to")
    async def request_to(self, bot_id: str, text: str = "") -> tuple[bool, str]:
        """向指定伙伴 bot 发起 transaction.request。"""

        return await self._send_relay_message(
            channel="transaction",
            intent="request",
            text=text,
            target_bot_id=bot_id,
        )

    @cmd_route("social")
    async def social(self, text: str = "") -> tuple[bool, str]:
        """向默认伙伴 bot 发送 social.say。"""

        return await self._send_relay_message(
            channel="social",
            intent="say",
            text=text,
            target_bot_id=None,
        )

    @cmd_route("social", "to")
    async def social_to(self, bot_id: str, text: str = "") -> tuple[bool, str]:
        """向指定伙伴 bot 发送 social.say。"""

        return await self._send_relay_message(
            channel="social",
            intent="say",
            text=text,
            target_bot_id=bot_id,
        )

    def _closeable_sessions(self):
        service = RelayStateService(self.plugin)
        return service.session_snapshot()

    async def _execute_send_command(self, verb: str, rest: str) -> tuple[bool, str]:
        """Parse raw send command text and dispatch it."""

        target_bot_id, text = self._parse_send_args(rest)
        if not text:
            return False, f"用法: /relay {verb} [to <bot_id>] <text>"
        if verb == "request":
            return await self._send_relay_message(
                channel="transaction",
                intent="request",
                text=text,
                target_bot_id=target_bot_id,
            )
        return await self._send_relay_message(
            channel="social",
            intent="say",
            text=text,
            target_bot_id=target_bot_id,
        )

    async def _send_relay_message(
        self,
        *,
        channel: str,
        intent: str,
        text: str,
        target_bot_id: str | None,
    ) -> tuple[bool, str]:
        """Send a manual relay message through the normal MessageSender path."""

        text = self._strip_wrapping_quotes(text.strip())
        if not text:
            return False, "relay send failed: message text is empty"
        config = self._relay_config()
        if config is None:
            return False, "relay send failed: config unavailable"
        partner, error = self._resolve_partner(config, target_bot_id)
        if partner is None:
            return False, error or "relay send failed: partner unavailable"

        relay_context: dict[str, object] = {
            "channel": channel,
            "intent": intent,
            "peer_bot_id": partner.bot_id,
            "peer_bot_name": partner.bot_name,
            "manual_command": True,
        }
        if channel == "social":
            relay_context.update(
                {
                    "conversation_id": uuid4().hex,
                    "phase": "opening",
                    "terminal": False,
                    "expect_reply": True,
                    "reply_budget": config.relay.default_reply_budget,
                    "allowed_responders": [partner.bot_id],
                }
            )

        target_stream_id = ChatStream.generate_stream_id(
            "bot_relay",
            user_id=partner.bot_id,
        )
        message = Message(
            message_id=f"relay-command-{uuid4().hex}",
            content=text,
            processed_plain_text=text,
            message_type=MessageType.TEXT,
            platform="bot_relay",
            chat_type="private",
            stream_id=target_stream_id,
            target_user_id=partner.bot_id,
            target_user_name=partner.bot_name,
            relay_context=relay_context,
        )
        sent = await get_message_sender().send_message(
            message,
            "bot_private_relay:adapter:bot_relay",
        )
        if not sent:
            return False, f"relay {intent} send failed: {partner.bot_name or partner.bot_id}({partner.bot_id})"
        return True, f"relay {intent} sent to {partner.bot_name or 'unknown'}({partner.bot_id}): {text}"

    async def _send_result_to_invoker(self, result_text: str) -> None:
        """Send command result text back to the original invoking platform."""

        if self._message is None or not result_text:
            return

        original_message = self._message
        response = Message(
            message_id=f"relay-command-result-{uuid4().hex}",
            reply_to=self.message_id or original_message.message_id,
            content=result_text,
            processed_plain_text=result_text,
            message_type=MessageType.TEXT,
            platform=original_message.platform,
            chat_type=original_message.chat_type,
            stream_id=self.stream_id,
            target_user_id=original_message.sender_id,
            target_user_name=original_message.sender_name,
        )
        if original_message.chat_type == "group":
            response.extra["group_id"] = original_message.extra.get("group_id")
            response.extra["group_name"] = original_message.extra.get("group_name")
        await get_message_sender().send_message(response)

    def _relay_config(self) -> BotPrivateRelayConfig | None:
        """Return typed plugin config if available."""

        config = getattr(self.plugin, "config", None)
        return config if isinstance(config, BotPrivateRelayConfig) else None

    @staticmethod
    def _resolve_partner(
        config: BotPrivateRelayConfig,
        target_bot_id: str | None,
    ) -> tuple[PartnerSection | None, str | None]:
        """Resolve explicit or default relay partner."""

        if target_bot_id:
            partner = config.partner_by_id(target_bot_id)
            if partner is None:
                return None, f"relay send failed: unknown partner bot_id={target_bot_id}"
            return partner, None
        partner = config.first_allowed_partner()
        if partner is None or not partner.bot_id:
            return None, "relay send failed: no allowed relay partner configured"
        return partner, None

    @staticmethod
    def _parse_send_args(rest: str) -> tuple[str | None, str]:
        """Parse optional ``to <bot_id>`` and preserve the remaining text."""

        stripped = rest.strip()
        if not stripped:
            return None, ""
        if stripped.startswith("to "):
            parts = stripped.split(maxsplit=2)
            if len(parts) < 3:
                return parts[1] if len(parts) > 1 else None, ""
            return parts[1], parts[2].strip()
        return None, stripped

    @staticmethod
    def _strip_wrapping_quotes(text: str) -> str:
        """Remove one symmetric quote pair from manually supplied text."""

        if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
            return text[1:-1]
        return text
