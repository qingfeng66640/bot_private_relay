"""MQTT transaction-channel smoke test for bot_private_relay.

This script validates the P0 transaction sequence against a real broker:

1. Bot A publishes ``transaction.request`` to Bot B.
2. Bot B receives the request envelope.
3. Bot B publishes ``transaction.accept`` back to Bot A.
4. Bot B publishes ``transaction.confirm`` back to Bot A.
5. Bot A receives ``accept`` and terminal ``confirm`` with the same
   ``conversation_id``.

The script also runs local state-machine checks for wrong responder rejection
and direct ``pending_reply -> confirm`` rejection before touching MQTT.

Flapping safety:
- Single connect attempt per bot.
- No reconnect loop inside the script.
- Distinct ``client_id`` per role/run.
- Default keepalive stays below the broker idle timeout.

Usage::

    python plugins/bot_private_relay/scripts/mqtt_smoke_test.py
    python plugins/bot_private_relay/scripts/mqtt_smoke_test.py --timeout 20

Default broker: ``mqtts://mqtt.epieikeia216.cn:8883`` (anonymous test).
Override with CLI flags or ``RELAY_MQTT_HOST`` / ``RELAY_MQTT_PORT`` /
``RELAY_MQTT_TLS``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import ssl
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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


def _transaction_payload(
    *,
    intent: str,
    from_bot: str,
    from_name: str,
    to_bot: str,
    to_name: str,
    conversation_id: str,
    trace_id: str,
    state: str,
    terminal: bool,
    expect_reply: bool,
    reply_budget: int,
    allowed_responders: list[str],
    text: str,
) -> str:
    """Build a transaction envelope payload."""

    return json.dumps(
        {
            "message_id": uuid.uuid4().hex,
            "conversation_id": conversation_id,
            "trace_id": trace_id,
            "from_bot": from_bot,
            "from_bot_name": from_name,
            "to_bot": to_bot,
            "to_bot_name": to_name,
            "channel": "transaction",
            "intent": intent,
            "terminal": terminal,
            "expect_reply": expect_reply,
            "reply_budget": reply_budget,
            "allowed_responders": allowed_responders,
            "state": state,
            "hop": 0,
            "ttl": 4,
            "payload": {"text": text},
        },
        ensure_ascii=False,
    )


def _notify_payload(from_bot: str, from_name: str, to_bot: str, to_name: str) -> str:
    """Build a transaction.notify payload (one-way, terminal)."""

    return _transaction_payload(
        intent="notify",
        from_bot=from_bot,
        from_name=from_name,
        to_bot=to_bot,
        to_name=to_name,
        conversation_id=uuid.uuid4().hex,
        trace_id=uuid.uuid4().hex,
        state="closed",
        terminal=True,
        expect_reply=False,
        reply_budget=0,
        allowed_responders=[],
        text="smoke notify: 不应触发自动回复",
    )


def _request_payload(
    from_bot: str,
    from_name: str,
    to_bot: str,
    to_name: str,
    *,
    conversation_id: str | None = None,
    trace_id: str | None = None,
) -> str:
    """Build a transaction.request payload."""

    return _transaction_payload(
        intent="request",
        from_bot=from_bot,
        from_name=from_name,
        to_bot=to_bot,
        to_name=to_name,
        conversation_id=conversation_id or uuid.uuid4().hex,
        trace_id=trace_id or uuid.uuid4().hex,
        state="pending_reply",
        terminal=False,
        expect_reply=True,
        reply_budget=3,
        allowed_responders=[to_bot],
        text="smoke request: 请先 accept，再 confirm 关闭事务",
    )


def _accept_payload(
    from_bot: str,
    from_name: str,
    to_bot: str,
    to_name: str,
    *,
    conversation_id: str,
    trace_id: str,
) -> str:
    """Build a transaction.accept payload."""

    return _transaction_payload(
        intent="accept",
        from_bot=from_bot,
        from_name=from_name,
        to_bot=to_bot,
        to_name=to_name,
        conversation_id=conversation_id,
        trace_id=trace_id,
        state="accepted",
        terminal=False,
        expect_reply=True,
        reply_budget=2,
        allowed_responders=[from_bot],
        text="smoke accept: 我接下这个事务",
    )


def _confirm_payload(
    from_bot: str,
    from_name: str,
    to_bot: str,
    to_name: str,
    *,
    conversation_id: str,
    trace_id: str,
) -> str:
    """Build a transaction.confirm payload that closes the transaction."""

    return _transaction_payload(
        intent="confirm",
        from_bot=from_bot,
        from_name=from_name,
        to_bot=to_bot,
        to_name=to_name,
        conversation_id=conversation_id,
        trace_id=trace_id,
        state="closed",
        terminal=True,
        expect_reply=False,
        reply_budget=0,
        allowed_responders=[from_bot],
        text="smoke confirm: 事务已完成并关闭",
    )


def _make_client(
    role: str,
    bot_id: str,
    received: list[tuple[str, str]],
    *,
    run_id: str,
    qos: int,
    use_tls: bool,
    ca_file: str,
    tls_insecure: bool,
) -> Any:
    """Create a paho-mqtt client with subscribe/will/callback wired."""

    import paho.mqtt.client as mqtt

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"bot_relay_{bot_id}_txn_smoke_{role}_{run_id}",
        clean_session=True,
        protocol=mqtt.MQTTv311,
    )
    if use_tls:
        context = ssl.create_default_context(cafile=ca_file or None)
        if tls_insecure:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        client.tls_set_context(context)

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
            c.subscribe(f"bot/{bot_id}/inbox", qos=qos)
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
        qos=qos,
        retain=True,
    )
    return client


def _parsed_transactions(received: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """Return parsed transaction envelopes from received MQTT payloads."""

    parsed: list[dict[str, Any]] = []
    for _topic, payload in received:
        try:
            item = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if item.get("channel") == "transaction":
            parsed.append(item)
    return parsed


def _wait_until(predicate: Callable[[], bool], *, timeout: float) -> bool:
    """Poll a predicate until timeout without retrying MQTT connections."""

    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return predicate()


def _validate_local_lifecycle() -> list[str]:
    """Validate local transaction invariants before the live broker check."""

    from plugins.bot_private_relay import store
    from plugins.bot_private_relay.config import BotPrivateRelayConfig, PartnerSection
    from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
    from plugins.bot_private_relay.relay_tools import (
        AcceptTransactionTool,
        CancelTransactionTool,
        ConfirmTransactionTool,
    )

    errors: list[str] = []
    store.reset_state()
    store.save_session(
        store.RelaySession(
            conversation_id="smoke-local",
            peer_bot_id=BOT_B_ID,
            channel="transaction",
            intent="request",
            state="pending_reply",
            terminal=False,
            expect_reply=True,
            reply_budget=3,
            allowed_responders=[BOT_B_ID],
        )
    )
    config = BotPrivateRelayConfig()
    config.relay.bot_id = BOT_A_ID
    config.relay.bot_name = BOT_A_NAME
    config.partners.bot_b = PartnerSection(bot_id=BOT_B_ID, bot_name=BOT_B_NAME)
    config.todo_bridge.enabled = False
    plugin = BotPrivateRelayPlugin(config)

    wrong_ok, wrong_payload = asyncio.run(
        AcceptTransactionTool(plugin).execute(
            conversation_id="smoke-local",
            caller_bot=BOT_A_ID,
            reason="wrong responder",
        )
    )
    if wrong_ok or wrong_payload.get("status") != "not_allowed_responder":
        errors.append("wrong responder was not rejected")

    direct_ok, direct_payload = asyncio.run(
        ConfirmTransactionTool(plugin).execute(
            conversation_id="smoke-local",
            caller_bot=BOT_B_ID,
            reason="direct confirm",
        )
    )
    if direct_ok or direct_payload.get("status") != "state_not_allowed":
        errors.append("pending_reply -> confirm was not rejected")

    accept_ok, accept_payload = asyncio.run(
        AcceptTransactionTool(plugin).execute(
            conversation_id="smoke-local",
            caller_bot=BOT_B_ID,
            reason="accept",
        )
    )
    if not accept_ok or accept_payload.get("state") != "accepted":
        errors.append("pending_reply -> accept did not succeed")

    confirm_ok, confirm_payload = asyncio.run(
        ConfirmTransactionTool(plugin).execute(
            conversation_id="smoke-local",
            caller_bot=BOT_B_ID,
            reason="confirm",
        )
    )
    if not confirm_ok or confirm_payload.get("state") != "closed":
        errors.append("accepted -> confirm -> closed did not succeed")

    closed_ok, _closed_payload = asyncio.run(
        CancelTransactionTool(plugin).execute(
            conversation_id="smoke-local",
            caller_bot=BOT_B_ID,
            reason="closed mutation",
        )
    )
    if closed_ok:
        errors.append("closed transaction accepted a later mutation")
    return errors


def _parse_args() -> argparse.Namespace:
    """Parse non-interactive smoke-test arguments."""

    parser = argparse.ArgumentParser(description="Run bot_private_relay transaction MQTT smoke test")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--tls", dest="tls", action="store_true", default=DEFAULT_TLS)
    parser.add_argument("--no-tls", dest="tls", action="store_false")
    parser.add_argument("--ca-file", default=DEFAULT_CA_FILE)
    parser.add_argument("--tls-insecure", action="store_true")
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--qos", type=int, default=1, choices=(0, 1, 2))
    return parser.parse_args()


def main() -> int:
    """Run the transaction smoke sequence against the configured broker."""

    try:
        import paho.mqtt.client  # noqa: F401
    except Exception as exc:
        print(f"paho-mqtt not installed: {exc}")
        return 2

    local_errors = _validate_local_lifecycle()
    if local_errors:
        print("Local lifecycle checks failed:")
        for error in local_errors:
            print(f"  - {error}")
        return 1

    args = _parse_args()
    run_id = uuid.uuid4().hex[:8]
    conversation_id = f"txn-smoke-{run_id}"
    trace_id = f"trace-{run_id}"
    scheme = "mqtts" if args.tls else "mqtt"
    print(f"Connecting to MQTT broker {scheme}://{args.host}:{args.port} (anonymous transaction smoke)")
    print(f"run_id={run_id}, conversation_id={conversation_id}")

    received_a: list[tuple[str, str]] = []
    received_b: list[tuple[str, str]] = []
    client_a = _make_client(
        "a",
        BOT_A_ID,
        received_a,
        run_id=run_id,
        qos=args.qos,
        use_tls=args.tls,
        ca_file=args.ca_file,
        tls_insecure=args.tls_insecure,
    )
    client_b = _make_client(
        "b",
        BOT_B_ID,
        received_b,
        run_id=run_id,
        qos=args.qos,
        use_tls=args.tls,
        ca_file=args.ca_file,
        tls_insecure=args.tls_insecure,
    )

    client_a.connect(args.host, args.port, keepalive=20)
    client_b.connect(args.host, args.port, keepalive=20)
    client_a.loop_start()
    client_b.loop_start()

    try:
        time.sleep(2)
        client_a.publish(f"bot/presence/{BOT_A_ID}", _presence_payload(BOT_A_ID, "online"), qos=args.qos, retain=True)
        client_b.publish(f"bot/presence/{BOT_B_ID}", _presence_payload(BOT_B_ID, "online"), qos=args.qos, retain=True)
        time.sleep(1)

        client_a.publish(
            f"bot/{BOT_B_ID}/inbox",
            _request_payload(
                BOT_A_ID,
                BOT_A_NAME,
                BOT_B_ID,
                BOT_B_NAME,
                conversation_id=conversation_id,
                trace_id=trace_id,
            ),
            qos=args.qos,
            retain=False,
        )
        got_request = _wait_until(
            lambda: any(
                item.get("conversation_id") == conversation_id and item.get("intent") == "request"
                for item in _parsed_transactions(received_b)
            ),
            timeout=args.timeout,
        )

        client_b.publish(
            f"bot/{BOT_A_ID}/inbox",
            _accept_payload(
                BOT_B_ID,
                BOT_B_NAME,
                BOT_A_ID,
                BOT_A_NAME,
                conversation_id=conversation_id,
                trace_id=trace_id,
            ),
            qos=args.qos,
            retain=False,
        )
        client_b.publish(
            f"bot/{BOT_A_ID}/inbox",
            _confirm_payload(
                BOT_B_ID,
                BOT_B_NAME,
                BOT_A_ID,
                BOT_A_NAME,
                conversation_id=conversation_id,
                trace_id=trace_id,
            ),
            qos=args.qos,
            retain=False,
        )
        got_response_sequence = _wait_until(
            lambda: [
                item.get("intent")
                for item in _parsed_transactions(received_a)
                if item.get("conversation_id") == conversation_id
            ]
            == ["accept", "confirm"],
            timeout=args.timeout,
        )

        client_a.publish(f"bot/presence/{BOT_A_ID}", _presence_payload(BOT_A_ID, "offline"), qos=args.qos, retain=True)
        client_b.publish(f"bot/presence/{BOT_B_ID}", _presence_payload(BOT_B_ID, "offline"), qos=args.qos, retain=True)
        time.sleep(1)
    finally:
        client_a.loop_stop()
        client_b.loop_stop()
        client_a.disconnect()
        client_b.disconnect()

    b_transactions = [item for item in _parsed_transactions(received_b) if item.get("conversation_id") == conversation_id]
    a_transactions = [item for item in _parsed_transactions(received_a) if item.get("conversation_id") == conversation_id]

    print("\n=== Transaction smoke summary ===")
    print(f"Bot B transaction messages: {[item.get('intent') for item in b_transactions]}")
    print(f"Bot A transaction messages: {[item.get('intent') for item in a_transactions]}")
    if a_transactions:
        final = a_transactions[-1]
        print(
            "Final transaction envelope: "
            f"intent={final.get('intent')}, state={final.get('state')}, "
            f"terminal={final.get('terminal')}, expect_reply={final.get('expect_reply')}"
        )

    if not got_request:
        print("FAIL: Bot B did not receive transaction.request")
        return 1
    if not got_response_sequence:
        print("FAIL: Bot A did not receive accept -> confirm in order")
        return 1
    final = a_transactions[-1]
    if final.get("intent") != "confirm" or final.get("state") != "closed" or final.get("terminal") is not True:
        print("FAIL: final confirm envelope did not close the transaction")
        return 1
    print("OK: transaction request -> accept -> confirm -> closed smoke passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
