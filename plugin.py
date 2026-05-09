"""bot_private_relay plugin entrypoint."""

from __future__ import annotations

from src.app.plugin_system.base import BasePlugin, register_plugin

from .adapter import BotRelayAdapter
from .chatter import BotRelayChatter
from .command import RelayCommand
from .config import BotPrivateRelayConfig
from .event_handler import LoopGuardEventHandler
from .relay_actions import (
    BotRelayPassAndWaitAction,
    BotRelaySendTextAction,
    BotRelayStopConversationAction,
)
from .service import RelayStateService


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

    def get_components(self) -> list[type]:
        """Return registered plugin components for Phase 1."""

        return [
            BotRelayAdapter,
            BotRelayChatter,
            BotRelaySendTextAction,
            BotRelayPassAndWaitAction,
            BotRelayStopConversationAction,
            LoopGuardEventHandler,
            RelayCommand,
            RelayStateService,
        ]
