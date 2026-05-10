"""MQTT adapter for the bot private relay plugin."""

from __future__ import annotations

import json
from typing import Any

from mofox_wire import MessageEnvelope

from src.app.plugin_system.base import BaseAdapter
from src.kernel.concurrency import get_task_manager
from src.kernel.logger import get_logger

from .config import BotPrivateRelayConfig, PartnerSection
from .envelope import RelayEnvelope
from .policy import PolicyEngine
from .presence import PresenceManager
from .session import SessionManager
from .system_handler import SystemChannelHandler

logger = get_logger("bot_private_relay_adapter")


class BotRelayAdapter(BaseAdapter):
    """Adapter exposing the ``bot_relay`` transport platform."""

    adapter_name = "bot_relay"
    adapter_version = "0.1.0"
    adapter_description = "Bot private relay adapter"
    platform = "bot_relay"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._mqtt_client: Any | None = None
        self._mqtt_task_info: Any | None = None
        self._session_manager = SessionManager()
        self._policy_engine = PolicyEngine()

    @property
    def relay_config(self) -> BotPrivateRelayConfig:
        """Return typed plugin config."""

        if not self.plugin or not isinstance(self.plugin.config, BotPrivateRelayConfig):
            raise RuntimeError("Bot private relay adapter requires BotPrivateRelayConfig")
        return self.plugin.config

    async def on_adapter_loaded(self) -> None:
        """Start MQTT background connection task via task_manager."""

        if not self.relay_config.relay.enabled:
            logger.info("Bot private relay adapter disabled by config")
            return
        tm = get_task_manager()
        self._mqtt_task_info = tm.create_task(
            self._mqtt_connect_loop(),
            name="bot_private_relay_mqtt",
            daemon=True,
        )

    async def on_adapter_unloaded(self) -> None:
        """Stop MQTT background task."""

        if self._mqtt_task_info:
            get_task_manager().cancel_task(self._mqtt_task_info.task_id)
            self._mqtt_task_info = None

    async def _mqtt_connect_loop(self) -> None:
        """Best-effort MQTT setup loop for Phase 1.

        Tests patch ``_mqtt_client`` directly, so this path remains intentionally
        conservative and does not require a running broker to import the plugin.
        """

        try:
            import paho.mqtt.client as mqtt
        except Exception as error:  # pragma: no cover - dependency may be absent in unit tests
            logger.warning(f"paho-mqtt unavailable in current environment: {error}")
            return

        if self._mqtt_client is None:
            self._mqtt_client = mqtt.Client()
        logger.info("Bot private relay MQTT loop initialized")

    async def get_bot_info(self) -> dict[str, Any]:  # type: ignore[override]
        """Return local bot identity for prompt display and sender fill."""

        return {
            "bot_id": self.relay_config.relay.bot_id,
            "bot_name": self.relay_config.relay.bot_name,
            "platform": self.platform,
        }

    async def _send_platform_message(self, envelope: MessageEnvelope) -> None:  # type: ignore[override]
        """Translate MessageEnvelope into RelayEnvelope and publish via MQTT."""

        partner = self._resolve_partner_from_message_envelope(envelope)
        relay_envelope = self._session_manager.build_outbound_envelope(
            message_envelope=envelope,
            from_bot=self.relay_config.relay.bot_id,
            from_bot_name=self.relay_config.relay.bot_name,
            to_bot=partner.bot_id,
            to_bot_name=partner.bot_name,
            default_ttl=self.relay_config.relay.default_ttl,
            default_reply_budget=self.relay_config.relay.default_reply_budget,
        )
        relay_envelope = self._policy_engine.apply_outbound(relay_envelope)
        relay_envelope.validate()
        await self.publish_relay_envelope(relay_envelope)

    async def publish_relay_envelope(self, envelope: RelayEnvelope) -> None:
        """Publish a validated relay envelope through the current MQTT client."""

        if self._mqtt_client is None:
            logger.info("MQTT client not connected; skipping live publish in current environment")
            return
        payload = json.dumps(envelope.to_dict(), ensure_ascii=False)
        topic = self._topic_for_envelope(envelope)
        publish = getattr(self._mqtt_client, "publish", None)
        if callable(publish):
            publish(topic, payload, qos=1, retain=False)

    async def from_platform_message(self, raw: Any) -> MessageEnvelope | None:  # type: ignore[override]
        """Convert raw relay payload into MessageEnvelope or consume system events."""

        raw_dict: dict[str, Any]
        if isinstance(raw, str):
            raw_dict = json.loads(raw)
        elif isinstance(raw, bytes):
            raw_dict = json.loads(raw.decode("utf-8"))
        elif isinstance(raw, dict):
            raw_dict = raw
        else:
            return None
        relay_envelope = RelayEnvelope.from_dict(raw_dict)
        relay_envelope.validate()
        presence_manager = PresenceManager(self.relay_config)
        if relay_envelope.to_bot not in {self.relay_config.relay.bot_id, "*"}:
            logger.warning(
                f"Ignoring relay envelope for different target bot: {relay_envelope.to_bot}"
            )
            return None
        if relay_envelope.channel != "system" and not presence_manager.is_allowed(relay_envelope.from_bot):
            logger.warning(
                f"Ignoring relay envelope from unknown partner bot: {relay_envelope.from_bot}"
            )
            return None
        system_handler = SystemChannelHandler(presence_manager)
        if system_handler.handle(relay_envelope):
            return None
        return {
            "direction": "incoming",
            "message_info": {
                "platform": self.platform,
                "message_id": relay_envelope.message_id,
                "message_type": "message",
                "user_info": {
                    "platform": self.platform,
                    "user_id": relay_envelope.from_bot,
                    "user_nickname": relay_envelope.from_bot_name,
                },
                "extra": {
                    "bot_internal": True,
                    "relay_context": self._session_manager.relay_context_from_envelope(relay_envelope),
                    "relay_envelope": relay_envelope.to_dict(),
                },
            },
            "message_segment": [
                {
                    "type": "text",
                    "data": relay_envelope.text,
                }
            ],
            "raw_message": raw_dict,
        }

    def _resolve_partner_from_message_envelope(self, envelope: MessageEnvelope) -> PartnerSection:
        """Resolve the routing partner from envelope metadata.

        Routing and permission checks are based only on partner ``bot_id``.
        """

        message_info = envelope.get("message_info") or {}
        extra = message_info.get("extra") if isinstance(message_info, dict) else {}
        relay_context = extra.get("relay_context") if isinstance(extra, dict) else {}
        peer_bot_id = relay_context.get("peer_bot_id") if isinstance(relay_context, dict) else None
        if isinstance(peer_bot_id, str):
            partner = self.relay_config.partner_by_id(peer_bot_id)
            if partner is not None:
                return partner
        partner = self.relay_config.first_allowed_partner()
        if partner is None or not partner.bot_id:
            raise ValueError("No allowed relay partner configured")
        return partner

    def _topic_for_envelope(self, envelope: RelayEnvelope) -> str:
        """Return MQTT topic for a relay envelope."""

        if envelope.channel == "system" and envelope.intent == "presence_update":
            return f"bot/presence/{envelope.from_bot}"
        return f"bot/{envelope.to_bot}/inbox"
