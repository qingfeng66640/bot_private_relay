"""MQTT system-channel smoke test for bot_private_relay.

This script validates wire-level delivery of system envelopes against a real
broker. It does not start the bot runtime; the guarantee that system envelopes
do not enter LLM is covered by ``tests/test_system_handler.py``.

Usage::

    python plugins/bot_private_relay/scripts/mqtt_smoke_system.py

Default broker: ``mqtt://8.163.34.70:1883`` (anonymous test).
Override with ``RELAY_MQTT_HOST`` / ``RELAY_MQTT_PORT`` env vars if needed.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from typing import Any

DEFAULT_HOST = os.environ.get("RELAY_MQTT_HOST", "8.163.34.70")
DEFAULT_PORT = int(os.environ.get("RELAY_MQTT_PORT", "1883"))

BOT_A_ID = "223123"
BOT_A_NAME = "清风"
BOT_B_ID = "114514"
BOT_B_NAME = "流光"

SYSTEM_INTENTS = ("ack", "close", "cancel", "error", "heartbeat", "typing", "weird")


def _presence_payload(bot_id: str, status: str) -> str:
    """Build a system.presence_update payload."""

    return json.dumps(
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


def _system_payload(intent: str) -> str:
    """Build one system-channel envelope for A → B."""

    return json.dumps(
        {
            "message_id": uuid.uuid4().hex,
            "conversation_id": uuid.uuid4().hex,
            "trace_id": uuid.uuid4().hex,
            "from_bot": BOT_A_ID,
            "from_bot_name": BOT_A_NAME,
            "to_bot": BOT_B_ID,
            "to_bot_name": BOT_B_NAME,
            "channel": "system",
            "intent": intent,
            "terminal": True,
            "expect_reply": False,
            "hop": 0,
            "ttl": 4,
            "payload": {"text": f"system smoke: {intent}"},
        },
        ensure_ascii=False,
    )


def _make_client(role: str, bot_id: str, received: list[tuple[str, str]]) -> Any:
    """Create a paho-mqtt client with v2 callbacks."""

    import paho.mqtt.client as mqtt

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"bot_relay_{bot_id}_system_smoke_{role}",
        clean_session=True,
        protocol=mqtt.MQTTv311,
    )

    def on_connect(
        c: Any,
        _ud: Any,
        _flags: Any,
        reason_code: Any,
        _properties: Any = None,
    ) -> None:
        is_failure = getattr(reason_code, "is_failure", None)
        ok = not is_failure() if callable(is_failure) else reason_code == 0
        if ok:
            c.subscribe(f"bot/{bot_id}/inbox", qos=1)
            print(f"[{role}] connected; subscribed to bot/{bot_id}/inbox")
        else:
            print(f"[{role}] connect failed reason={reason_code}")

    def on_message(_c: Any, _ud: Any, msg: Any) -> None:
        text = msg.payload.decode("utf-8", errors="replace")
        print(f"[{role}] received on {msg.topic}: {text[:200]}")
        received.append((msg.topic, text))

    client.on_connect = on_connect
    client.on_message = on_message
    client.will_set(
        f"bot/presence/{bot_id}",
        _presence_payload(bot_id, "offline"),
        qos=1,
        retain=True,
    )
    return client


def main() -> int:
    """Run the system-channel smoke sequence."""

    try:
        import paho.mqtt.client  # noqa: F401
    except Exception as exc:
        print(f"paho-mqtt not installed: {exc}")
        return 2

    print(f"Connecting to MQTT broker {DEFAULT_HOST}:{DEFAULT_PORT} (anonymous test)")
    received_a: list[tuple[str, str]] = []
    received_b: list[tuple[str, str]] = []
    client_a = _make_client("a", BOT_A_ID, received_a)
    client_b = _make_client("b", BOT_B_ID, received_b)

    client_a.connect(DEFAULT_HOST, DEFAULT_PORT, keepalive=20)
    client_b.connect(DEFAULT_HOST, DEFAULT_PORT, keepalive=20)
    client_a.loop_start()
    client_b.loop_start()

    try:
        time.sleep(2)
        client_a.publish(
            f"bot/presence/{BOT_A_ID}",
            _presence_payload(BOT_A_ID, "online"),
            qos=1,
            retain=True,
        )
        client_b.publish(
            f"bot/presence/{BOT_B_ID}",
            _presence_payload(BOT_B_ID, "online"),
            qos=1,
            retain=True,
        )
        time.sleep(2)

        for intent in SYSTEM_INTENTS:
            client_a.publish(
                f"bot/{BOT_B_ID}/inbox",
                _system_payload(intent),
                qos=1,
                retain=False,
            )
            time.sleep(1)

        client_a.publish(
            f"bot/presence/{BOT_A_ID}",
            _presence_payload(BOT_A_ID, "offline"),
            qos=1,
            retain=True,
        )
        client_b.publish(
            f"bot/presence/{BOT_B_ID}",
            _presence_payload(BOT_B_ID, "offline"),
            qos=1,
            retain=True,
        )
        time.sleep(2)
    finally:
        client_a.loop_stop()
        client_b.loop_stop()
        client_a.disconnect()
        client_b.disconnect()

    parsed = [json.loads(payload) for _, payload in received_b]
    system_messages = [item for item in parsed if item.get("channel") == "system"]

    print("\n=== System smoke summary ===")
    print(f"Bot B system inbox messages received: {len(system_messages)}")
    for item in system_messages:
        print(f"  - {item.get('intent')}: message_id={item.get('message_id')}")

    if len(system_messages) == len(SYSTEM_INTENTS):
        print("OK: Bot B received all system smoke envelopes")
        return 0
    print("FAIL: Bot B did not receive all expected system envelopes")
    return 1


if __name__ == "__main__":
    sys.exit(main())
