"""Relay plugin commands."""

from __future__ import annotations

from pathlib import Path

from src.app.plugin_system.base import BaseCommand, cmd_route
from src.app.plugin_system.types import PermissionLevel

from .service import RelayStateService


class RelayCommand(BaseCommand):
    """Command entrypoint for ``/relay`` management."""

    command_name = "relay"
    command_description = "Inspect bot private relay runtime status"
    permission_level = PermissionLevel.OWNER

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

    def _closeable_sessions(self):
        service = RelayStateService(self.plugin)
        return service.session_snapshot()
