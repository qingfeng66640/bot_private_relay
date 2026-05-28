"""bot_private_relay plugin entrypoint."""

from __future__ import annotations

import asyncio

from src.app.plugin_system.base import BasePlugin, register_plugin
from src.kernel.concurrency import get_task_manager
from src.kernel.logger import get_logger

from .adapter import BotRelayAdapter
from .chatter import BotRelayChatter
from .command import RelayCommand
from .config import BotPrivateRelayConfig
from .dynamic_social import RelaySocialContactTool, register_relay_config
from .event_handler import GroupReplySuppressionEventHandler, LoopGuardEventHandler
from .memory_bridge import MemoryBridgeService
from .relay_actions import (
    BotRelayPassAndWaitAction,
    BotRelaySendTextAction,
    BotRelayStopConversationAction,
)
from .router import BotPrivateRelayRouter
from .relay_tools import (
    AcceptTransactionTool,
    AckTransactionTool,
    CancelTransactionTool,
    CloseTransactionTool,
    ConfirmTransactionTool,
    DeclineTransactionTool,
    RescheduleTransactionTool,
)
from .service import RelayProactiveService, RelayStateService

logger = get_logger("bot_private_relay_plugin")


@register_plugin
class BotPrivateRelayPlugin(BasePlugin):
    """Bot private relay plugin.

    Runtime plugin identity follows the bound repository name.
    Transport platform remains ``bot_relay``.
    """

    plugin_name = "bot_private_relay"
    plugin_description = "Bot-to-bot private relay plugin over MQTT"
    plugin_version = "0.1.0"
    configs = [BotPrivateRelayConfig]
    dependent_components: list[str] = []

    def __init__(self, config: BotPrivateRelayConfig | None = None) -> None:
        super().__init__(config)
        self._proactive_schedule_id: str | None = None
        self._proactive_register_task_id: str | None = None

    def get_components(self) -> list[type]:
        """Return registered plugin components for Phase 1."""

        return [
            BotRelayAdapter,
            BotRelayChatter,
            BotRelaySendTextAction,
            BotRelayPassAndWaitAction,
            BotRelayStopConversationAction,
            AcceptTransactionTool,
            ConfirmTransactionTool,
            DeclineTransactionTool,
            CancelTransactionTool,
            RescheduleTransactionTool,
            AckTransactionTool,
            CloseTransactionTool,
            RelaySocialContactTool,
            LoopGuardEventHandler,
            GroupReplySuppressionEventHandler,
            RelayCommand,
            RelayStateService,
            RelayProactiveService,
            MemoryBridgeService,
            BotPrivateRelayRouter,
        ]

    async def on_plugin_loaded(self) -> None:
        """Expose relay tools and register proactive scheduler when enabled."""

        if isinstance(self.config, BotPrivateRelayConfig):
            register_relay_config(self.config)
            if self.config.proactive.enabled:
                task = get_task_manager().create_task(
                    self._register_proactive_schedule_when_ready(),
                    name="bot_private_relay_register_proactive_schedule",
                    daemon=True,
                )
                self._proactive_register_task_id = task.task_id
        try:
            from plugins.todo_plugin.registry import register_bot_tool
        except Exception:
            return
        register_bot_tool(RelaySocialContactTool)

    async def on_plugin_unloaded(self) -> None:
        """Remove proactive scheduler state owned by this plugin instance."""

        if self._proactive_schedule_id:
            try:
                from src.kernel.scheduler import get_unified_scheduler

                await get_unified_scheduler().remove_schedule(self._proactive_schedule_id)
            except Exception:
                pass
            self._proactive_schedule_id = None

        if self._proactive_register_task_id:
            try:
                get_task_manager().cancel_task(self._proactive_register_task_id)
            except Exception:
                pass
            self._proactive_register_task_id = None

    async def _register_proactive_schedule_when_ready(self) -> None:
        """Register proactive periodic tick after scheduler becomes ready."""

        from src.kernel.scheduler import TriggerType, get_unified_scheduler

        if not isinstance(self.config, BotPrivateRelayConfig) or not self.config.proactive.enabled:
            return
        interval = max(1, int(self.config.proactive.check_interval_seconds))
        scheduler = get_unified_scheduler()
        for _attempt in range(600):
            try:
                self._proactive_schedule_id = await scheduler.create_schedule(
                    callback=self._proactive_tick_job,
                    trigger_type=TriggerType.TIME,
                    trigger_config={"interval_seconds": interval},
                    is_recurring=True,
                    task_name="bot_private_relay_proactive",
                    force_overwrite=True,
                )
                logger.info(f"Bot private relay proactive schedule registered: {self._proactive_schedule_id}")
                get_task_manager().create_task(
                    self._proactive_tick_job(),
                    name="bot_private_relay_proactive_initial_tick",
                    daemon=True,
                )
                return
            except RuntimeError:
                await asyncio.sleep(0.5)
            except Exception as exc:
                logger.warning(f"Bot private relay proactive schedule registration failed: {exc}")
                await asyncio.sleep(2.0)
        logger.warning("Bot private relay proactive schedule registration timed out")

    async def _proactive_tick_job(self) -> None:
        """Scheduler callback for one proactive relay tick."""

        await RelayProactiveService(self).tick()
