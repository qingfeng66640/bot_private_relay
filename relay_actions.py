"""Relay-only action wrappers around default_chatter actions."""

from __future__ import annotations

from plugins.default_chatter.plugin import (
    PassAndWaitAction,
    SendTextAction,
    StopConversationAction,
)


class BotRelaySendTextAction(SendTextAction):
    """Relay-only send text action."""

    chatter_allow = ["bot_relay_chatter"]


class BotRelayPassAndWaitAction(PassAndWaitAction):
    """Relay-only pass action."""

    chatter_allow = ["bot_relay_chatter"]


class BotRelayStopConversationAction(StopConversationAction):
    """Relay-only stop action."""

    chatter_allow = ["bot_relay_chatter"]
