"""Relay plugin commands."""

# =============================================================================
# RelayCommand - /relay 管理命令
# =============================================================================
# 提供 /relay 前缀的命令行界面，用于检查和管理 relay 运行时状态。
#
# 子命令列表：
#   /relay status          - 显示在线状态与会话数量
#   /relay inspect         - 查看事务/审计摘要
#   /relay close all       - 关闭所有活跃会话
#   /relay partners        - 列出伙伴 bot 配置
#   /relay export          - 导出调试快照到 data 目录
#   /relay request <text>  - 向默认伙伴 bot 发起事务请求
#   /relay request to <id> <text> - 向指定伙伴发起事务请求
#   /relay social <text>   - 向默认伙伴 bot 发送社交消息
#   /relay social to <id> <text>  - 向指定伙伴发送社交消息
#
# 所有命令需要 OWNER 权限级别。
# =============================================================================

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from src.app.plugin_system.base import BaseCommand, cmd_route
from src.app.plugin_system.types import PermissionLevel
from src.core.models.message import Message, MessageType
from src.core.models.stream import ChatStream
from src.core.transport.message_send import get_message_sender

from ..config import BotPrivateRelayConfig, PartnerSection
from ..services.relay import RelayStateService
from ..tools.dynamic_social import DynamicSocialLimiter


class RelayCommand(BaseCommand):
    """Command entrypoint for ``/relay`` management."""

    command_name = "relay"
    command_description = "Inspect and manually test bot private relay runtime"
    permission_level = PermissionLevel.OWNER  # 仅 Bot 拥有者可用

    async def execute(self, message_text: str) -> tuple[bool, str]:
        """Execute relay commands, preserving free-form send text.

        命令分发逻辑：
        1. 如果文本以 command_prefix 或 command_name 开头 → 格式错误（此方法只接受剥离后的子路由）
        2. 如果第一个词是 "request" 或 "social" → 分发到 _execute_send_command
        3. 否则 → 调用父类 execute（由 cmd_route 装饰器路由）
        """

        stripped = message_text.strip()

        # 防御性检查：不应该包含前缀
        if stripped.startswith(self.command_prefix):
            result = False, "命令文本格式错误：RelayCommand.execute 只接受去掉前缀后的子路由文本"
            await self._send_result_to_invoker(result[1])
            return result

        parts = stripped.split(maxsplit=1)
        if parts and parts[0] == self.command_name:
            result = False, "命令文本格式错误：RelayCommand.execute 只接受去掉 command_name 后的子路由文本"
            await self._send_result_to_invoker(result[1])
            return result

        # ── 分发：request/social 走快捷路径，其他走 cmd_route 路由 ──
        if parts and parts[0] in {"request", "social"}:
            result = await self._execute_send_command(parts[0], parts[1] if len(parts) > 1 else "")
        else:
            result = await super().execute(message_text)

        await self._send_result_to_invoker(result[1])
        return result

    # =========================================================================
    # 查询/管理子命令
    # =========================================================================

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
        """关闭所有当前会话（插件内运行态）。

        遍历所有会话，将状态设为 closed、terminal=True、expect_reply=False。
        注意：这只是运行时状态修改，不会发送网络消息。
        """

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
        if isinstance(config, BotPrivateRelayConfig):
            for partner in config.iter_partners():
                partners.append(f"{partner.bot_name or 'unknown'}({partner.bot_id})")
        return True, "relay partners: " + ", ".join(partners)

    @cmd_route("export")
    async def export(self) -> tuple[bool, str]:
        """导出插件内调试快照到本地 data 目录。

        生成包含 presence、sessions、transactions、audit 等完整运行状态
        的 JSON 文件。
        """

        service = RelayStateService(self.plugin)
        target = service.export_debug_snapshot(Path("data"))
        return True, f"relay export: {target.name}"

    # =========================================================================
    # 消息发送子命令
    # =========================================================================

    @cmd_route("request")
    async def request(self, text: str = "") -> tuple[bool, str]:
        """向默认伙伴 bot 发起 transaction.request。

        不需要指定目标 bot_id，自动使用 first_allowed_partner() 找到的第一个伙伴。
        """

        return await self._send_relay_message(
            channel="transaction",
            intent="request",
            text=text,
            target_bot_id=None,
        )

    @cmd_route("request", "to")
    async def request_to(self, bot_id: str, text: str = "") -> tuple[bool, str]:
        """向指定伙伴 bot 发起 transaction.request。

        Args:
            bot_id: 接收方的 bot_id。
            text: 事务请求的信息。
        """

        return await self._send_relay_message(
            channel="transaction",
            intent="request",
            text=text,
            target_bot_id=bot_id,
        )

    @cmd_route("social")
    async def social(self, text: str = "") -> tuple[bool, str]:
        """向默认伙伴 bot 发送 social.say。

        社交消息会经过 DynamicSocialLimiter 配额检查。
        """

        return await self._send_relay_message(
            channel="social",
            intent="say",
            text=text,
            target_bot_id=None,
        )

    @cmd_route("social", "to")
    async def social_to(self, bot_id: str, text: str = "") -> tuple[bool, str]:
        """向指定伙伴 bot 发送 social.say。

        Args:
            bot_id: 接收方的 bot_id。
            text: 要发送的社交消息。
        """

        return await self._send_relay_message(
            channel="social",
            intent="say",
            text=text,
            target_bot_id=bot_id,
        )

    # =========================================================================
    # 内部辅助方法
    # =========================================================================

    def _closeable_sessions(self):
        """获取所有可关闭的会话。"""
        service = RelayStateService(self.plugin)
        return service.session_snapshot()

    async def _execute_send_command(self, verb: str, rest: str) -> tuple[bool, str]:
        """Parse raw send command text and dispatch it.

        解析用户输入的命令文本，提取可选的 target_bot_id 和消息正文。

        支持格式：
        - /relay request to <bot_id> <text>
        - /relay request <text>
        - /relay social to <bot_id> <text>
        - /relay social <text>
        """

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
        """Send a manual relay message through the normal MessageSender path.

        通过标准的 MessageSender → Adapter 路径发送 relay 消息。
        这是手动命令触发的消息，不是 LLM 自动生成的。

        发送流程：
        1. 验证消息文本不为空
        2. 获取配置
        3. 解析目标伙伴
        4. 如果是 social channel → 检查配额
        5. 构建 relay_context
        6. 创建 Message 对象
        7. 通过 MessageSender 发送
        """

        text = self._strip_wrapping_quotes(text.strip())
        if not text:
            return False, "relay send failed: message text is empty"

        config = self._relay_config()
        if config is None:
            return False, "relay send failed: config unavailable"

        # ── 解析目标伙伴 ──
        partner, error = self._resolve_partner(config, target_bot_id)
        if partner is None:
            return False, error or "relay send failed: partner unavailable"

        # ── Social channel 需要配额检查 ──
        if channel == "social":
            ok, code = DynamicSocialLimiter(config).allow(target_bot_id=partner.bot_id, source="user_command")
            if not ok:
                return False, f"relay social denied: {code}"

        # ── 构建 relay_context ──
        relay_context: dict[str, object] = {
            "channel": channel,
            "intent": intent,
            "peer_bot_id": partner.bot_id,
            "peer_bot_name": partner.bot_name,
            "manual_command": True,  # 标记为手动命令触发
        }

        # Social channel 需要额外的会话控制字段
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

        # ── 生成 stream_id 和 Message ──
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

        # ── 发送 ──
        sent = await get_message_sender().send_message(
            message,
            "bot_private_relay:adapter:bot_relay",
        )
        if not sent:
            return False, f"relay {intent} send failed: {partner.bot_name or partner.bot_id}({partner.bot_id})"
        return True, f"relay {intent} sent to {partner.bot_name or 'unknown'}({partner.bot_id}): {text}"

    async def _send_result_to_invoker(self, result_text: str) -> None:
        """Send command result text back to the original invoking platform.

        将命令执行结果发送回原始调用平台（如 QQ）。
        支持私聊和群聊两种场景。
        """

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

        # 如果是群聊，保留群组信息
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
        """Resolve explicit or default relay partner.

        解析目标伙伴：
        - 如果指定了 target_bot_id → 按 ID 查找
        - 如果未指定 → 使用 first_allowed_partner()（白名单中第一个）
        """

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
        """Parse optional ``to <bot_id>`` and preserve the remaining text.

        解析格式：
        - "to <bot_id> <text>" → (bot_id, text)
        - "<text>" → (None, text)
        """

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
        """Remove one symmetric quote pair from manually supplied text.

        去除用户手动添加的一对对称引号（单引号或双引号）。
        只去除一层。
        """

        if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
            return text[1:-1]
        return text
