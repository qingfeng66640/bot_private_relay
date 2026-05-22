"""Phase 2 transaction matrix tests for bot_private_relay."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from plugins.bot_private_relay import store
from plugins.bot_private_relay.chatter import BotRelayChatter
from plugins.bot_private_relay.envelope import RelayEnvelope
from plugins.bot_private_relay.plugin import BotPrivateRelayPlugin
from plugins.bot_private_relay.relay_tools import (
    AcceptTransactionTool,
    AckTransactionTool,
    CancelTransactionTool,
    CloseTransactionTool,
    ConfirmTransactionTool,
    DeclineTransactionTool,
    RescheduleTransactionTool,
)
from plugins.bot_private_relay.session import SessionManager


TRANSACTION_TOOLS = {
    "accept_transaction": AcceptTransactionTool,
    "confirm_transaction": ConfirmTransactionTool,
    "decline_transaction": DeclineTransactionTool,
    "cancel_transaction": CancelTransactionTool,
    "reschedule_transaction": RescheduleTransactionTool,
    "ack_transaction": AckTransactionTool,
    "close_transaction": CloseTransactionTool,
}


def _build_config() -> Any:
    """Build the plugin config used by transaction tool tests."""

    from plugins.bot_private_relay.config import BotPrivateRelayConfig, PartnerSection

    config = BotPrivateRelayConfig()
    config.relay.bot_id = "223123"
    config.relay.bot_name = "清风"
    config.partners.bot_b = PartnerSection(bot_id="114514", bot_name="流光")
    config.presence.allowed_partner_bots = ["114514"]
    config.todo_bridge.enabled = False
    return config


def _plugin() -> BotPrivateRelayPlugin:
    """Return a configured bot_private_relay plugin instance."""

    return BotPrivateRelayPlugin(_build_config())


def _save_session(
    *,
    conversation_id: str = "conv-phase2",
    state: str = "pending_reply",
    intent: str = "request",
    terminal: bool = False,
    reply_budget: int = 3,
    allowed_responders: list[str] | None = None,
) -> store.RelaySession:
    """Save a transaction session with valid defaults for tool execution."""

    session = store.RelaySession(
        conversation_id=conversation_id,
        peer_bot_id="114514",
        channel="transaction",
        intent=intent,
        state=state,
        terminal=terminal,
        expect_reply=not terminal,
        reply_budget=reply_budget,
        allowed_responders=allowed_responders if allowed_responders is not None else ["114514"],
    )
    store.save_session(session)
    store.save_transaction_record(
        store.RelayTransactionRecord(
            conversation_id=conversation_id,
            trace_id=f"trace-{conversation_id}",
            from_bot="223123",
            to_bot="114514",
            current_state=state,
            topic="Phase 2 matrix task",
            summary="Phase 2 matrix task",
        )
    )
    return session


def _execute_tool(tool_cls: type[Any], conversation_id: str = "conv-phase2") -> tuple[bool, dict[str, Any]]:
    """Execute a transaction tool as the allowed responder."""

    return __import__("asyncio").run(
        tool_cls(_plugin()).execute(
            conversation_id=conversation_id,
            caller_bot="114514",
            reason="matrix test",
        )
    )


def test_phase2_transaction_tools_are_registered_and_relay_isolated() -> None:
    """All transaction tools must be plugin-local and only exposed to relay chatter."""

    manifest = json.loads(Path(__file__).resolve().parents[1].joinpath("manifest.json").read_text(encoding="utf-8"))
    manifest_tools = {
        item["component_name"]
        for item in manifest["include"]
        if item["component_type"] == "tool"
    }
    components = set(BotPrivateRelayPlugin(_build_config()).get_components())

    assert set(TRANSACTION_TOOLS) <= manifest_tools
    assert set(TRANSACTION_TOOLS.values()) <= components
    for tool_name, tool_cls in TRANSACTION_TOOLS.items():
        signature = f"bot_private_relay:tool:{tool_name}"
        assert tool_cls.tool_name == tool_name
        assert tool_cls.chatter_allow == ["bot_relay_chatter"]
        assert tool_cls.associated_platforms == ["bot_relay"]
        assert signature in BotRelayChatter._RELAY_USABLE_SIGNATURES

        class RuntimeUsable:
            @classmethod
            def get_signature(cls) -> str:
                return signature

        assert BotRelayChatter._is_relay_usable(RuntimeUsable) is True


@pytest.mark.parametrize(
    ("start_state", "tool_cls", "expected_intent", "expected_state", "expected_terminal", "expected_budget"),
    [
        ("pending_reply", DeclineTransactionTool, "decline", "closed", True, 0),
        ("pending_reply", CancelTransactionTool, "cancel", "closed", True, 0),
        ("pending_reply", RescheduleTransactionTool, "reschedule", "reschedule_requested", False, 2),
        ("pending_reply", AckTransactionTool, "ack", "closed", True, 0),
        ("pending_reply", CloseTransactionTool, "close", "closed", True, 0),
        ("accepted", DeclineTransactionTool, "decline", "closed", True, 0),
        ("accepted", CancelTransactionTool, "cancel", "closed", True, 0),
        ("reschedule_requested", AcceptTransactionTool, "accept", "accepted", False, 2),
        ("reschedule_requested", DeclineTransactionTool, "decline", "closed", True, 0),
        ("reschedule_requested", CancelTransactionTool, "cancel", "closed", True, 0),
        ("reschedule_requested", CloseTransactionTool, "close", "closed", True, 0),
    ],
)
def test_phase2_transaction_tools_cover_valid_state_matrix(
    start_state: str,
    tool_cls: type[Any],
    expected_intent: str,
    expected_state: str,
    expected_terminal: bool,
    expected_budget: int,
) -> None:
    """The public tools should cover every Phase 2 non-P0 valid transition."""

    store.reset_state()
    _save_session(state=start_state)

    success, payload = _execute_tool(tool_cls)

    assert success is True
    assert payload["status"] == "ok"
    assert payload["intent"] == expected_intent
    assert payload["state"] == expected_state
    session = store.SESSION_TABLE["conv-phase2"]
    assert session.intent == expected_intent
    assert session.state == expected_state
    assert session.terminal is expected_terminal
    assert session.reply_budget == expected_budget


def test_p0_request_invite_accept_confirm_lifecycle_stays_compatible() -> None:
    """P0 transactions still require accept before confirm and then close."""

    for initial_intent in ("request", "invite"):
        store.reset_state()
        _save_session(conversation_id=f"conv-{initial_intent}", intent=initial_intent, state="pending_reply")

        accepted, accept_payload = _execute_tool(AcceptTransactionTool, f"conv-{initial_intent}")
        confirmed, confirm_payload = _execute_tool(ConfirmTransactionTool, f"conv-{initial_intent}")

        assert accepted is True
        assert accept_payload["state"] == "accepted"
        assert confirmed is True
        assert confirm_payload["state"] == "closed"
        assert store.SESSION_TABLE[f"conv-{initial_intent}"].terminal is True

    store.reset_state()
    _save_session(conversation_id="conv-direct-confirm", state="pending_reply")
    success, payload = _execute_tool(ConfirmTransactionTool, "conv-direct-confirm")
    assert success is False
    assert payload["status"] == "state_not_allowed"


@pytest.mark.parametrize(
    ("conversation_id", "state", "terminal", "reply_budget", "allowed_responders", "tool_cls", "caller_bot", "payload_complete", "expected_status"),
    [
        ("conv-accepted-accept", "accepted", False, 3, ["114514"], AcceptTransactionTool, "114514", True, "state_not_allowed"),
        ("conv-closed-mutation", "closed", True, 3, ["114514"], CancelTransactionTool, "114514", True, "conversation_closed"),
        ("conv-wrong-responder", "pending_reply", False, 3, ["114514"], AcceptTransactionTool, "223123", True, "not_allowed_responder"),
        ("conv-budget-zero", "pending_reply", False, 0, ["114514"], AcceptTransactionTool, "114514", True, "reply_budget_exhausted"),
        ("conv-terminal", "pending_reply", True, 3, ["114514"], AcceptTransactionTool, "114514", True, "conversation_closed"),
        ("conv-incomplete-payload", "pending_reply", False, 3, ["114514"], AcceptTransactionTool, "114514", False, "invalid_payload"),
    ],
)
def test_phase2_transaction_validation_gates_return_expected_error_codes(
    conversation_id: str,
    state: str,
    terminal: bool,
    reply_budget: int,
    allowed_responders: list[str],
    tool_cls: type[Any],
    caller_bot: str,
    payload_complete: bool,
    expected_status: str,
) -> None:
    """Invalid matrix paths should exercise the six validation gates."""

    store.reset_state()
    _save_session(
        conversation_id=conversation_id,
        state=state,
        terminal=terminal,
        reply_budget=reply_budget,
        allowed_responders=allowed_responders,
    )
    manager = SessionManager()

    ok, code, _session = manager.validate_transaction_action(
        conversation_id=conversation_id,
        action=tool_cls.action_intent,
        caller_bot=caller_bot,
        payload_complete=payload_complete,
    )

    assert ok is False
    assert code == expected_status
    assert code in {
        "ok",
        "state_not_allowed",
        "not_allowed_responder",
        "reply_budget_exhausted",
        "conversation_closed",
        "invalid_payload",
    }


def test_phase2_transaction_validation_rejects_empty_tool_payload() -> None:
    """An empty conversation_id is an incomplete tool payload."""

    store.reset_state()
    _save_session(conversation_id="conv-empty")

    success, payload = __import__("asyncio").run(
        AcceptTransactionTool(_plugin()).execute(
            conversation_id="",
            caller_bot="114514",
            reason="missing conversation id",
        )
    )

    assert success is False
    assert payload["status"] == "invalid_payload"


@pytest.mark.parametrize(
    ("existing_state", "intent", "expected_state", "expected_terminal", "expected_budget"),
    [
        ("pending_reply", "accept", "accepted", False, 2),
        ("pending_reply", "reschedule", "reschedule_requested", False, 2),
        ("accepted", "confirm", "closed", True, 0),
        ("pending_reply", "decline", "closed", True, 0),
        ("accepted", "cancel", "closed", True, 0),
        ("pending_reply", "ack", "closed", True, 0),
        ("reschedule_requested", "close", "closed", True, 0),
    ],
)
def test_inbound_transaction_sync_infers_phase2_intents(
    existing_state: str,
    intent: str,
    expected_state: str,
    expected_terminal: bool,
    expected_budget: int,
) -> None:
    """Inbound transaction envelopes without state should still sync Phase 2 intents."""

    store.reset_state()
    _save_session(conversation_id=f"conv-inbound-{intent}", state=existing_state)

    session = SessionManager().sync_inbound_transaction_session(
        RelayEnvelope(
            conversation_id=f"conv-inbound-{intent}",
            trace_id=f"trace-inbound-{intent}",
            from_bot="114514",
            from_bot_name="流光",
            to_bot="223123",
            to_bot_name="清风",
            channel="transaction",
            intent=intent,
            state=None,
            terminal=False,
            expect_reply=True,
            reply_budget=2,
            allowed_responders=["223123"],
            payload={"text": f"inbound {intent}"},
        )
    )

    assert session is not None
    assert session.intent == intent
    assert session.state == expected_state
    assert session.terminal is expected_terminal
    assert session.reply_budget == expected_budget
    assert store.TRANSACTION_LOG[f"conv-inbound-{intent}"].current_state == expected_state


def test_inbound_transaction_sync_ignores_conflicting_envelope_state() -> None:
    """Inbound state sync must derive state from the local transition table."""

    store.reset_state()
    _save_session(conversation_id="conv-inbound-conflict", state="pending_reply")

    session = SessionManager().sync_inbound_transaction_session(
        RelayEnvelope(
            conversation_id="conv-inbound-conflict",
            trace_id="trace-inbound-conflict",
            from_bot="114514",
            from_bot_name="流光",
            to_bot="223123",
            to_bot_name="清风",
            channel="transaction",
            intent="accept",
            state="closed",
            terminal=True,
            expect_reply=True,
            reply_budget=2,
            allowed_responders=["223123"],
            payload={"text": "inbound accept with conflicting state"},
        )
    )

    assert session is not None
    assert session.intent == "accept"
    assert session.state == "accepted"
    assert session.terminal is False
    assert session.reply_budget == 2
    assert store.TRANSACTION_LOG["conv-inbound-conflict"].current_state == "accepted"


def test_inbound_transaction_sync_rejects_invalid_direct_confirm() -> None:
    """Inbound sync must not close a pending transaction through direct confirm."""

    store.reset_state()
    _save_session(conversation_id="conv-inbound-direct-confirm", state="pending_reply")

    session = SessionManager().sync_inbound_transaction_session(
        RelayEnvelope(
            conversation_id="conv-inbound-direct-confirm",
            trace_id="trace-inbound-direct-confirm",
            from_bot="114514",
            from_bot_name="流光",
            to_bot="223123",
            to_bot_name="清风",
            channel="transaction",
            intent="confirm",
            state="closed",
            terminal=True,
            expect_reply=False,
            reply_budget=0,
            allowed_responders=["223123"],
            payload={"text": "invalid direct confirm"},
        )
    )

    assert session is not None
    assert session.intent == "request"
    assert session.state == "pending_reply"
    assert session.terminal is False
    assert session.expect_reply is True
    assert session.reply_budget == 3
    assert store.TRANSACTION_LOG["conv-inbound-direct-confirm"].current_state == "pending_reply"
    assert store.RELAY_TODOS == {}


def test_inbound_transaction_sync_rejects_invalid_intent_without_local_session() -> None:
    """Inbound terminal intents should not create sessions without a local transition."""

    store.reset_state()

    session = SessionManager().sync_inbound_transaction_session(
        RelayEnvelope(
            conversation_id="conv-inbound-missing-confirm",
            trace_id="trace-inbound-missing-confirm",
            from_bot="114514",
            from_bot_name="流光",
            to_bot="223123",
            to_bot_name="清风",
            channel="transaction",
            intent="confirm",
            state="closed",
            terminal=True,
            expect_reply=False,
            reply_budget=0,
            allowed_responders=["223123"],
            payload={"text": "missing local transaction"},
        )
    )

    assert session is None
    assert "conv-inbound-missing-confirm" not in store.SESSION_TABLE
    assert "conv-inbound-missing-confirm" not in store.TRANSACTION_LOG
    assert store.RELAY_TODOS == {}
