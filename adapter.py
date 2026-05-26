"""MQTT adapter for the bot private relay plugin."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import urlparse

from mofox_wire import MessageEnvelope

from src.app.plugin_system.base import BaseAdapter
from src.kernel.concurrency import get_task_manager
from src.kernel.logger import get_logger

from . import store
from .config import BotPrivateRelayConfig, PartnerSection
from .envelope import RelayEnvelope
from .policy import PolicyEngine
from .presence import PresenceManager
from .session import SessionManager
from .system_handler import SystemChannelHandler
from .todo_bridge import TodoBridge

logger = get_logger("bot_private_relay_adapter")


class BotRelayAdapter(BaseAdapter):
    """Adapter exposing the ``bot_relay`` transport platform."""

    adapter_name = "bot_relay"
    adapter_version = "0.1.0"
    adapter_description = "Bot private relay adapter"
    platform = "bot_relay"

    _HEARTBEAT_INTERVAL = 30  # seconds between presence publishes
    _RECONNECT_MIN_DELAY = 10  # seconds, initial reconnect backoff (Flapping-safe)
    _RECONNECT_MAX_DELAY = 120  # seconds, max reconnect backoff
    _KEEPALIVE = 20  # seconds, MQTT PINGREQ interval (< broker idle timeout)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._mqtt_client: Any | None = None
        self._mqtt_task_info: Any | None = None
        self._heartbeat_task_info: Any | None = None
        self._reconnect_task_info: Any | None = None
        self._reconnecting: bool = False
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._session_manager = SessionManager()
        self._policy_engine = PolicyEngine()
        self._reconnect_delay = self._RECONNECT_MIN_DELAY

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
        # Capture the running loop so paho-mqtt network-thread callbacks can
        # schedule async work safely via asyncio.run_coroutine_threadsafe.
        self._event_loop = asyncio.get_running_loop()
        tm = get_task_manager()
        self._mqtt_task_info = tm.create_task(
            self._mqtt_connect_loop(),
            name="bot_private_relay_mqtt",
            daemon=True,
        )

    async def on_adapter_unloaded(self) -> None:
        """Publish offline presence and stop MQTT background tasks."""

        await self._publish_presence("offline")
        for task_info in (self._mqtt_task_info, self._heartbeat_task_info):
            if task_info:
                get_task_manager().cancel_task(task_info.task_id)
        self._mqtt_task_info = None
        self._heartbeat_task_info = None
        if self._mqtt_client and hasattr(self._mqtt_client, "loop_stop"):
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
        self._mqtt_client = None

    def _cancel_heartbeat_task(self) -> None:
        """Cancel the current heartbeat task if one is registered."""

        if self._heartbeat_task_info:
            get_task_manager().cancel_task(self._heartbeat_task_info.task_id)
            self._heartbeat_task_info = None

    def _stop_mqtt_client(self) -> None:
        """Stop the existing paho client without publishing presence."""

        if self._mqtt_client is None:
            return
        loop_stop = getattr(self._mqtt_client, "loop_stop", None)
        if callable(loop_stop):
            loop_stop()
        disconnect = getattr(self._mqtt_client, "disconnect", None)
        if callable(disconnect):
            disconnect()
        self._mqtt_client = None

    async def health_check(self) -> bool:
        """Report MQTT client health instead of BaseAdapter transport health."""

        if self._mqtt_client is None:
            return False
        is_connected = getattr(self._mqtt_client, "is_connected", None)
        if callable(is_connected):
            return bool(is_connected()) or self._reconnecting
        return True

    async def reconnect(self) -> None:
        """Let the MQTT disconnect callback own reconnect scheduling."""

        logger.debug("MQTT reconnect is managed by paho disconnect callbacks")

    # ── MQTT connection lifecycle ────────────────────────────────────

    def _parse_broker_url(self) -> tuple[str, int]:
        """Parse host and port from relay_url."""
        url = self.relay_config.relay.relay_url
        parsed = urlparse(url)
        host = parsed.hostname or "localhost"
        port = parsed.port or 1883
        return host, port

    async def _mqtt_connect_loop(self) -> None:
        """Full MQTT connect / subscribe / presence / heartbeat / reconnect loop.

        All MQTT background activity runs through task_manager as required by
        plan constraint #7.
        """

        try:
            import paho.mqtt.client as mqtt
        except Exception as error:  # pragma: no cover
            logger.warning(f"paho-mqtt unavailable in current environment: {error}")
            return

        config = self.relay_config.relay
        broker_host, broker_port = self._parse_broker_url()
        self._cancel_heartbeat_task()
        self._stop_mqtt_client()

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"bot_relay_{config.bot_id}",
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )
        client.on_connect = self._on_mqtt_connect
        client.on_message = self._on_mqtt_message_callback
        client.on_disconnect = self._on_mqtt_disconnect

        presence_mgr = PresenceManager(self.relay_config)
        will_envelope = presence_mgr.build_presence_envelope(status="offline")
        will_payload = json.dumps(will_envelope.to_dict(), ensure_ascii=False)
        client.will_set(
            f"bot/presence/{config.bot_id}",
            will_payload,
            qos=1,
            retain=True,
        )

        logger.info(
            f"Bot private relay MQTT connecting to {broker_host}:{broker_port}"
        )
        try:
            client.connect(broker_host, broker_port, keepalive=self._KEEPALIVE)
        except Exception as exc:
            logger.warning(f"MQTT connect failed: {exc}; will retry")
            self._reconnect_delay = min(
                self._reconnect_delay * 2, self._RECONNECT_MAX_DELAY
            )
            await asyncio.sleep(self._reconnect_delay)
            self._mqtt_task_info = get_task_manager().create_task(
                self._mqtt_connect_loop(),
                name="bot_private_relay_mqtt",
                daemon=True,
            )
            return

        client.loop_start()
        self._mqtt_client = client
        self._reconnect_delay = self._RECONNECT_MIN_DELAY

        # Start heartbeat after connection stabilises
        tm = get_task_manager()
        self._heartbeat_task_info = tm.create_task(
            self._heartbeat_loop(client, config.bot_id),
            name="bot_private_relay_heartbeat",
            daemon=True,
        )

    def _on_mqtt_connect(
        self,
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        """Callback: connection established with broker (paho v2 API).

        paho v2 passes a ``ReasonCode`` object rather than an int; we use
        ``is_failure`` when available and fall back to equality with 0.
        """

        is_failure = getattr(reason_code, "is_failure", None)
        if callable(is_failure):
            ok = not is_failure()
        else:
            ok = reason_code == 0

        if ok:
            logger.info("Bot private relay MQTT connected")
            config = self.relay_config.relay
            client.subscribe(f"bot/{config.bot_id}/inbox", qos=1)
            logger.info(f"Subscribed to bot/{config.bot_id}/inbox")
            self._publish_presence_sync(client, config.bot_id, "online")
        else:
            logger.warning(f"MQTT connect returned reason_code: {reason_code}")

    def _on_mqtt_disconnect(
        self,
        client: Any,
        userdata: Any,
        disconnect_flags: Any = None,
        reason_code: Any = None,
        properties: Any = None,
    ) -> None:
        """Callback: connection lost with broker (paho v2 API).

        Guarded against duplicate reconnect tasks to avoid triggering broker
        Flapping protection. Only one reconnect task may be in flight at a time.
        """

        logger.info(f"MQTT disconnected (reason_code={reason_code})")
        if self._reconnecting:
            logger.debug("Reconnect already in flight; skipping duplicate schedule")
            return
        if self._event_loop is None or self._event_loop.is_closed():
            logger.warning("Disconnect callback fired without an event loop; cannot reconnect")
            return
        self._reconnecting = True
        self._reconnect_delay = min(
            self._reconnect_delay * 2, self._RECONNECT_MAX_DELAY
        )
        # paho network thread → schedule reconnect on the captured loop.
        asyncio.run_coroutine_threadsafe(
            self._mqtt_reconnect_delayed(),
            self._event_loop,
        )

    async def _mqtt_reconnect_delayed(self) -> None:
        """Sleep then retry connection. Clears the reconnect flag on completion."""
        try:
            await asyncio.sleep(self._reconnect_delay)
            self._mqtt_task_info = get_task_manager().create_task(
                self._mqtt_connect_loop(),
                name="bot_private_relay_mqtt",
                daemon=True,
            )
        finally:
            self._reconnecting = False
            self._reconnect_task_info = None

    def _on_mqtt_message_callback(self, client: Any, userdata: Any, msg: Any) -> None:
        """Callback: message received from broker.

        This is invoked on paho-mqtt's network thread, which is not an asyncio
        thread.  We cannot call ``task_manager.create_task`` directly here;
        instead schedule the async coroutine onto the captured event loop.
        """
        try:
            raw = msg.payload.decode("utf-8")
        except Exception as exc:
            logger.warning(f"MQTT message decode failed on topic {msg.topic}: {exc}")
            return
        logger.info(
            f"MQTT inbound on {msg.topic} ({len(raw)} bytes); dispatching to event loop"
        )
        if self._event_loop is None or self._event_loop.is_closed():
            logger.warning("MQTT message received but event loop unavailable; dropping")
            return
        asyncio.run_coroutine_threadsafe(
            self.on_platform_message(raw),
            self._event_loop,
        )

    # ── Presence helpers ─────────────────────────────────────────────

    async def _publish_presence(self, status: str) -> None:
        """Publish retained presence message (async-friendly)."""
        if self._mqtt_client is None:
            return
        presence_mgr = PresenceManager(self.relay_config)
        envelope = presence_mgr.build_presence_envelope(status=status)
        topic = self._topic_for_envelope(envelope)
        payload = json.dumps(envelope.to_dict(), ensure_ascii=False)
        publish = getattr(self._mqtt_client, "publish", None)
        if callable(publish):
            publish(topic, payload, qos=1, retain=True)

    @staticmethod
    def _publish_presence_sync(client: Any, bot_id: str, status: str) -> None:
        """Publish presence synchronously from MQTT callback thread."""
        payload = json.dumps(
            {
                "from_bot": bot_id,
                "to_bot": "*",
                "channel": "system",
                "intent": "presence_update",
                "terminal": True,
                "expect_reply": False,
                "payload": {"status": status},
            },
            ensure_ascii=False,
        )
        publish = getattr(client, "publish", None)
        if callable(publish):
            publish(f"bot/presence/{bot_id}", payload, qos=1, retain=True)

    async def _heartbeat_loop(self, client: Any, bot_id: str) -> None:
        """Publish presence periodically to signal online status."""

        while True:
            try:
                self._publish_presence_sync(client, bot_id, "online")
                await asyncio.sleep(self._HEARTBEAT_INTERVAL)
            except Exception:
                await asyncio.sleep(5)

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
        relay_envelope = relay_envelope.increment_hop()
        relay_envelope.validate()
        presence_manager = PresenceManager(self.relay_config)
        if relay_envelope.to_bot not in {self.relay_config.relay.bot_id, "*"}:
            logger.warning(
                f"Ignoring relay envelope for different target bot: {relay_envelope.to_bot}"
            )
            return None
        if relay_envelope.channel != "system" and not presence_manager.is_allowed(relay_envelope.from_bot):
            logger.warning(
                "Rejecting relay envelope from unknown partner bot: "
                f"from_bot={relay_envelope.from_bot}, conversation_id={relay_envelope.conversation_id}"
            )
            store.audit(
                "sender_not_allowed",
                from_bot=relay_envelope.from_bot,
                to_bot=relay_envelope.to_bot,
                channel=relay_envelope.channel,
                intent=relay_envelope.intent,
                conversation_id=relay_envelope.conversation_id,
            )
            await self._publish_sender_not_allowed_error(relay_envelope)
            return None
        system_handler = SystemChannelHandler(presence_manager)
        if system_handler.handle(relay_envelope):
            return None
        transaction_session = self._session_manager.sync_inbound_transaction_session(relay_envelope)
        transaction_session = await self._auto_confirm_inbound_accept(relay_envelope, transaction_session)
        if transaction_session is not None:
            self._apply_session_state_to_envelope(relay_envelope, transaction_session)
        inbound_todo_result = await self._session_manager.publish_inbound_final_todo_decision(
            envelope=relay_envelope,
            local_bot_id=self.relay_config.relay.bot_id,
            config=self.relay_config,
        )
        if inbound_todo_result is not None:
            ok, status, result = inbound_todo_result
            logger.info(
                "Inbound relay final decision todo projection handled: "
                f"conversation_id={relay_envelope.conversation_id}, "
                f"owner_bot={self.relay_config.relay.bot_id}, "
                f"peer_bot_id={relay_envelope.from_bot}, "
                f"ok={ok}, status={status}, todo_uid={result.get('todo_uid', '')}"
            )
        self._session_manager.sync_inbound_social_session(relay_envelope)
        return MessageEnvelope(
            direction="incoming",
            message_info={
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
            message_segment=[
                {
                    "type": "text",
                    "data": relay_envelope.text,
                }
            ],
            raw_message=raw_dict,
        )

    async def _publish_sender_not_allowed_error(self, inbound: RelayEnvelope) -> None:
        """Send an explicit protocol error for rejected non-system envelopes."""

        error_envelope = RelayEnvelope(
            conversation_id=inbound.conversation_id,
            trace_id=inbound.trace_id,
            parent_message_id=inbound.message_id,
            from_bot=self.relay_config.relay.bot_id,
            from_bot_name=self.relay_config.relay.bot_name,
            to_bot=inbound.from_bot,
            to_bot_name=inbound.from_bot_name,
            channel="system",
            intent="error",
            expect_reply=False,
            reply_budget=0,
            ttl=self.relay_config.relay.default_ttl,
            terminal=True,
            allowed_responders=[],
            no_relay=True,
            payload={
                "code": "sender_not_allowed",
                "text": "Sender bot is not allowed to contact this relay endpoint.",
                "rejected_channel": inbound.channel,
                "rejected_intent": inbound.intent,
            },
        )
        try:
            error_envelope.validate()
            await self.publish_relay_envelope(error_envelope)
        except Exception as exc:
            logger.error(
                "Failed to publish sender-not-allowed relay error: "
                f"from_bot={inbound.from_bot}, conversation_id={inbound.conversation_id}, error={exc}",
                exc_info=True,
            )

    async def _auto_confirm_inbound_accept(
        self,
        envelope: RelayEnvelope,
        session: store.RelaySession | None,
    ) -> store.RelaySession | None:
        """Confirm an inbound accept only after local projection succeeds."""

        local_bot_id = self.relay_config.relay.bot_id
        if envelope.channel != "transaction" or envelope.intent != "accept":
            return session
        if session is None or session.state != "accepted" or session.terminal:
            return session
        if local_bot_id not in session.allowed_responders:
            return session

        ok, code, checked_session = self._session_manager.validate_transaction_action(
            conversation_id=envelope.conversation_id,
            action="confirm",
            caller_bot=local_bot_id,
            payload_complete=bool(envelope.conversation_id),
        )
        if not ok or checked_session is None:
            logger.warning(
                "Inbound accept auto-confirm rejected by validation: "
                f"conversation_id={envelope.conversation_id}, status={code}"
            )
            return session

        record = store.TRANSACTION_LOG.get(envelope.conversation_id)
        if record is None:
            logger.warning(
                "Inbound accept auto-confirm skipped: transaction record missing, "
                f"conversation_id={envelope.conversation_id}"
            )
            return session

        confirm_envelope = self._build_auto_confirm_envelope(envelope)
        try:
            confirm_envelope.validate()
        except Exception as exc:
            logger.error(
                "Inbound accept auto-confirm envelope invalid; not publishing confirm: "
                f"conversation_id={envelope.conversation_id}, error={exc}",
                exc_info=True,
            )
            return session

        bridge_ok, bridge_status, bridge_result = await TodoBridge(self.relay_config).publish_final_decision(
            record=record,
            final_intent="confirm",
            owner_bot=local_bot_id,
            peer_bot_id=envelope.from_bot,
        )
        if not bridge_ok:
            logger.warning(
                "Inbound accept auto-confirm rejected by todo bridge; not publishing confirm: "
                f"conversation_id={envelope.conversation_id}, status={bridge_status}, "
                f"todo_uid={bridge_result.get('todo_uid', '')}"
            )
            return session

        try:
            confirmed_session = self._session_manager.apply_transaction_action(
                conversation_id=envelope.conversation_id,
                action="confirm",
                caller_bot=local_bot_id,
            )
        except Exception as exc:
            logger.error(
                "Inbound accept auto-confirm failed while applying local state; not publishing confirm: "
                f"conversation_id={envelope.conversation_id}, error={exc}",
                exc_info=True,
            )
            return session

        try:
            await self.publish_relay_envelope(confirm_envelope)
        except Exception as exc:
            logger.error(
                "Inbound accept auto-confirm publish failed after local confirm: "
                f"conversation_id={envelope.conversation_id}, error={exc}",
                exc_info=True,
            )
        else:
            logger.info(
                "Inbound accept auto-confirm published: "
                f"conversation_id={envelope.conversation_id}, peer_bot_id={envelope.from_bot}, "
                f"todo_bridge_status={bridge_status}"
            )
        return confirmed_session

    def _build_auto_confirm_envelope(self, inbound: RelayEnvelope) -> RelayEnvelope:
        """Build the outbound confirm envelope for an accepted transaction."""

        return RelayEnvelope(
            conversation_id=inbound.conversation_id,
            trace_id=inbound.trace_id,
            parent_message_id=inbound.message_id,
            from_bot=self.relay_config.relay.bot_id,
            from_bot_name=self.relay_config.relay.bot_name,
            to_bot=inbound.from_bot,
            to_bot_name=inbound.from_bot_name,
            channel="transaction",
            intent="confirm",
            expect_reply=False,
            reply_budget=0,
            ttl=self.relay_config.relay.default_ttl,
            terminal=True,
            allowed_responders=[],
            state="closed",
            payload={"text": "已确认当前事务。"},
        )

    @staticmethod
    def _apply_session_state_to_envelope(envelope: RelayEnvelope, session: store.RelaySession) -> None:
        """Reflect locally applied session state in downstream relay_context."""

        envelope.state = session.state
        envelope.terminal = session.terminal
        envelope.expect_reply = session.expect_reply
        envelope.reply_budget = session.reply_budget
        envelope.allowed_responders = list(session.allowed_responders)

    def _resolve_partner_from_message_envelope(self, envelope: MessageEnvelope) -> PartnerSection:
        """Resolve the routing partner from envelope metadata.

        Routing and permission checks are based only on partner ``bot_id``.
        """

        message_info = envelope.get("message_info") if isinstance(envelope, dict) else None
        extra = message_info.get("extra") if isinstance(message_info, dict) else None
        relay_context = extra.get("relay_context") if isinstance(extra, dict) else None
        peer_bot_id = relay_context.get("peer_bot_id") if isinstance(relay_context, dict) else None
        if isinstance(peer_bot_id, str) and peer_bot_id:
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
