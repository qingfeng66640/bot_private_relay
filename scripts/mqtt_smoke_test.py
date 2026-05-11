"""MQTT smoke test for bot_private_relay against a real broker.

This script is **not** wired into pytest and must be invoked manually.  It
performs the minimal real-broker connectivity check described in the plan:

1. Bot A connects, subscribes, publishes retained presence.
2. Bot B connects, subscribes, publishes retained presence.
3. Bot A publishes a ``transaction.notify`` envelope to ``bot/{B}/inbox``.
4. Bot A publishes a ``transaction.request`` envelope to ``bot/{B}/inbox``.
5. Both bots publish ``offline`` presence and disconnect cleanly.

Flapping safety:
- Single connect attempt per bot.
- No retries within the script itself.
- Sleep windows are conservative (>= 2s) to avoid back-to-back churn.
- Distinct ``client_id`` per role to prevent broker-side takeover loops.

Usage::

    python plugins/bot_private_relay/scripts/mqtt_smoke_test.py

Sibling scripts cover system and social channels:
``mqtt_smoke_system.py`` and ``mqtt_smoke_social.py``.

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


def _notify_payload(from_bot: str, from_name: str, to_bot: str, to_name: str) -> str:
    """Build a transaction.notify payload (one-way, terminal)."""
    return json.dumps(
        {
            "message_id": uuid.uuid4().hex,
            "conversation_id": uuid.uuid4().hex,
            "trace_id": uuid.uuid4().hex,
            "from_bot": from_bot,
            "from_bot_name": from_name,
            "to_bot": to_bot,
            "to_bot_name": to_name,
            "channel": "transaction",
            "intent": "notify",
            "terminal": True,
            "expect_reply": False,
            "hop": 0,
            "ttl": 4,
            "payload": {"text": "smoke notify: 不应触发自动回复"},
        },
        ensure_ascii=False,
    )


def _request_payload(from_bot: str, from_name: str, to_bot: str, to_name: str) -> str:
    """Build a transaction.request payload (expect_reply=True)."""
    return json.dumps(
        {
            "message_id": uuid.uuid4().hex,
            "conversation_id": uuid.uuid4().hex,
            "trace_id": uuid.uuid4().hex,
            "from_bot": from_bot,
            "from_bot_name": from_name,
            "to_bot": to_bot,
            "to_bot_name": to_name,
            "channel": "transaction",
            "intent": "request",
            "terminal": False,
            "expect_reply": True,
            "reply_budget": 3,
            "allowed_responders": [to_bot],
            "state": "pending_reply",
            "hop": 0,
            "ttl": 4,
            "payload": {"text": "smoke request: 请回复确认"},
        },
        ensure_ascii=False,
    )


def _make_client(role: str, bot_id: str, received: list[tuple[str, str]]) -> Any:
    """Create a paho-mqtt client with subscribe/will/callback wired (v2 API)."""
    import paho.mqtt.client as mqtt

    client_id = f"bot_relay_{bot_id}_smoke_{role}"
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
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
        # paho v2: reason_code is a ReasonCode object, not int.
        # Use .is_failure for success check; .value for numeric display.
        is_failure = getattr(reason_code, "is_failure", None)
        if callable(is_failure):
            ok = not is_failure()
        else:
            ok = reason_code == 0
        if ok:
            c.subscribe(f"bot/{bot_id}/inbox", qos=1)
            print(f"[{role}] connected; subscribed to bot/{bot_id}/inbox")
        else:
            print(f"[{role}] connect failed reason={reason_code}")

    def on_message(_c: Any, _ud: Any, msg: Any) -> None:
        try:
            text = msg.payload.decode("utf-8")
        except Exception:
            text = "<binary>"
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
    """Run the smoke test sequence against the configured broker."""
    try:
        import paho.mqtt.client  # noqa: F401
    except Exception as exc:
        print(f"paho-mqtt not installed: {exc}")
        return 2

    host, port = DEFAULT_HOST, DEFAULT_PORT
    print(f"Connecting to MQTT broker {host}:{port} (anonymous test)")

    received_a: list[tuple[str, str]] = []
    received_b: list[tuple[str, str]] = []

    client_a = _make_client("a", BOT_A_ID, received_a)
    client_b = _make_client("b", BOT_B_ID, received_b)

    # Single, polite connect per side. No retries inside this script.
    # keepalive=20s < broker idle timeout (~30s) so paho sends PINGREQ in time.
    client_a.connect(host, port, keepalive=20)
    client_b.connect(host, port, keepalive=20)
    client_a.loop_start()
    client_b.loop_start()

    try:
        time.sleep(2)  # let on_connect / subscribe complete

        # 1. presence
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

        # 2. notify  A → B
        client_a.publish(
            f"bot/{BOT_B_ID}/inbox",
            _notify_payload(BOT_A_ID, BOT_A_NAME, BOT_B_ID, BOT_B_NAME),
            qos=1,
            retain=False,
        )
        time.sleep(2)

        # 3. request  A → B
        client_a.publish(
            f"bot/{BOT_B_ID}/inbox",
            _request_payload(BOT_A_ID, BOT_A_NAME, BOT_B_ID, BOT_B_NAME),
            qos=1,
            retain=False,
        )
        time.sleep(3)

        # 4. clean offline presence
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

    print("\n=== Smoke test summary ===")
    print(f"Bot B inbox messages received: {len(received_b)}")
    for topic, payload in received_b:
        print(f"  - {topic}: {payload[:120]}")

    if len(received_b) >= 2:
        print("OK: Bot B received at least notify + request")
        return 0
    print("FAIL: Bot B did not receive expected messages")
    return 1


if __name__ == "__main__":
    sys.exit(main())
