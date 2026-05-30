"""MQTT social-channel smoke test for bot_private_relay.

This script validates wire-level delivery and field preservation for social
envelopes. It does not start the bot runtime; policy behavior is covered by
``tests/test_social_policy.py``.

Usage::

    python plugins/bot_private_relay/scripts/mqtt_smoke_social.py

Default broker: ``mqtts://mqtt.epieikeia216.cn:8883`` (anonymous test).
Override with ``RELAY_MQTT_HOST`` / ``RELAY_MQTT_PORT`` / ``RELAY_MQTT_TLS``
env vars if needed.
"""

from __future__ import annotations

import json
import os
import ssl
import sys
import time
import uuid
from typing import Any

DEFAULT_HOST = os.environ.get("RELAY_MQTT_HOST", "mqtt.epieikeia216.cn")
DEFAULT_PORT = int(os.environ.get("RELAY_MQTT_PORT", "8883"))
DEFAULT_TLS = os.environ.get("RELAY_MQTT_TLS", "1").lower() not in {"0", "false", "no", "off"}
DEFAULT_CA_FILE = os.environ.get("RELAY_MQTT_CA_FILE", "")

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


def _social_payload(
    *,
    text: str,
    phase: str,
    reply_budget: int,
    expect_reply: bool,
    terminal: bool,
    allowed_responders: list[str],
) -> str:
    """Build one social.say envelope for A → B."""

    return json.dumps(
        {
            "message_id": uuid.uuid4().hex,
            "conversation_id": uuid.uuid4().hex,
            "trace_id": uuid.uuid4().hex,
            "from_bot": BOT_A_ID,
            "from_bot_name": BOT_A_NAME,
            "to_bot": BOT_B_ID,
            "to_bot_name": BOT_B_NAME,
            "channel": "social",
            "intent": "say",
            "phase": phase,
            "terminal": terminal,
            "expect_reply": expect_reply,
            "reply_budget": reply_budget,
            "allowed_responders": allowed_responders,
            "hop": 0,
            "ttl": 4,
            "payload": {"text": text},
        },
        ensure_ascii=False,
    )


def _make_client(
    role: str,
    bot_id: str,
    received: list[tuple[str, str]],
    *,
    use_tls: bool,
    ca_file: str,
) -> Any:
    """Create a paho-mqtt client with v2 callbacks."""

    import paho.mqtt.client as mqtt

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"bot_relay_{bot_id}_social_smoke_{role}",
        clean_session=True,
        protocol=mqtt.MQTTv311,
    )
    if use_tls:
        client.tls_set_context(ssl.create_default_context(cafile=ca_file or None))

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
    """Run the social-channel smoke sequence."""

    try:
        import paho.mqtt.client  # noqa: F401
    except Exception as exc:
        print(f"paho-mqtt not installed: {exc}")
        return 2

    scheme = "mqtts" if DEFAULT_TLS else "mqtt"
    print(f"Connecting to MQTT broker {scheme}://{DEFAULT_HOST}:{DEFAULT_PORT} (anonymous test)")
    received_a: list[tuple[str, str]] = []
    received_b: list[tuple[str, str]] = []
    client_a = _make_client("a", BOT_A_ID, received_a, use_tls=DEFAULT_TLS, ca_file=DEFAULT_CA_FILE)
    client_b = _make_client("b", BOT_B_ID, received_b, use_tls=DEFAULT_TLS, ca_file=DEFAULT_CA_FILE)

    client_a.connect(DEFAULT_HOST, DEFAULT_PORT, keepalive=20)
    client_b.connect(DEFAULT_HOST, DEFAULT_PORT, keepalive=20)
    client_a.loop_start()
    client_b.loop_start()

    cases = [
        _social_payload(
            text="social smoke: 我们聊一下协作节奏。",
            phase="opening",
            reply_budget=2,
            expect_reply=True,
            terminal=False,
            allowed_responders=[BOT_B_ID],
        ),
        _social_payload(
            text="social smoke: 这个话题先收束。",
            phase="ending",
            reply_budget=0,
            expect_reply=False,
            terminal=True,
            allowed_responders=[],
        ),
        _social_payload(
            text="social smoke: 没有允许响应者，不应自动回复。",
            phase="active",
            reply_budget=2,
            expect_reply=True,
            terminal=False,
            allowed_responders=[],
        ),
    ]

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

        for payload in cases:
            client_a.publish(f"bot/{BOT_B_ID}/inbox", payload, qos=1, retain=False)
            time.sleep(2)

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
    social_messages = [item for item in parsed if item.get("channel") == "social"]

    print("\n=== Social smoke summary ===")
    print(f"Bot B social inbox messages received: {len(social_messages)}")
    for item in social_messages:
        print(
            "  - "
            f"phase={item.get('phase')}, "
            f"reply_budget={item.get('reply_budget')}, "
            f"expect_reply={item.get('expect_reply')}, "
            f"terminal={item.get('terminal')}"
        )

    if len(social_messages) == len(cases):
        print("OK: Bot B received all social smoke envelopes")
        return 0
    print("FAIL: Bot B did not receive all expected social envelopes")
    return 1


if __name__ == "__main__":
    sys.exit(main())
