"""Relay transaction tools with hard validation gates."""

from __future__ import annotations

from typing import Any

from src.app.plugin_system.base import BaseTool

from .session import SessionManager


class _BaseRelayTransactionTool(BaseTool):
    """Shared transaction tool behavior."""

    chatter_allow = ["bot_relay_chatter"]
    associated_platforms = ["bot_relay"]

    action_intent = ""

    def _manager(self) -> SessionManager:
        return SessionManager()

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """Validate and apply a transaction action.

        Args:
            conversation_id: Target transaction conversation id.
            caller_bot: Caller bot id for responder validation.
            reason: Optional natural-language reason.
        """

        manager = self._manager()
        ok, code, _session = manager.validate_transaction_action(
            conversation_id=conversation_id,
            action=self.action_intent,
            caller_bot=caller_bot,
            payload_complete=bool(conversation_id),
        )
        if not ok:
            return False, {"status": code, "intent": self.action_intent, "reason": reason}
        session = manager.apply_transaction_action(
            conversation_id=conversation_id,
            action=self.action_intent,
            caller_bot=caller_bot,
        )
        return True, {
            "status": "ok",
            "intent": self.action_intent,
            "conversation_id": conversation_id,
            "state": session.state,
            "reason": reason,
        }


class ConfirmTransactionTool(_BaseRelayTransactionTool):
    """Confirm a pending transaction request."""

    tool_name = "confirm_transaction"
    tool_description = "对事务请求执行确认，并执行六项硬校验。"
    action_intent = "confirm"


class DeclineTransactionTool(_BaseRelayTransactionTool):
    """Decline a pending transaction request."""

    tool_name = "decline_transaction"
    tool_description = "对事务请求执行拒绝，并执行六项硬校验。"
    action_intent = "decline"


class CancelTransactionTool(_BaseRelayTransactionTool):
    """Cancel a pending transaction request."""

    tool_name = "cancel_transaction"
    tool_description = "对事务请求执行取消，并执行六项硬校验。"
    action_intent = "cancel"
