"""Relay plugin commands."""

from __future__ import annotations

from src.app.plugin_system.base import BaseCommand, cmd_route
from src.app.plugin_system.types import PermissionLevel

from .service import RelayStateService


class RelayCommand(BaseCommand):
    """Command entrypoint for ``/relay`` management."""

    command_name = "relay"
    command_description = "Inspect bot private relay runtime status"
    permission_level = PermissionLevel.USER

    @cmd_route("status")
    async def status(self) -> tuple[bool, str]:
        """显示当前 relay 在线状态与会话数量。"""

        service = RelayStateService(self.plugin)
        presence_count = len(service.presence_snapshot())
        session_count = len(service.session_snapshot())
        return True, f"relay status: presence={presence_count}, sessions={session_count}"
