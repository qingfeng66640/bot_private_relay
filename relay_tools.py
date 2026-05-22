"""Relay transaction tools with hard validation gates."""

from __future__ import annotations

from typing import Any

from src.app.plugin_system.base import BaseTool
from src.kernel.logger import get_logger

from . import store
from .config import BotPrivateRelayConfig
from .session import SessionManager
from .todo_bridge import TodoBridge


logger = get_logger("bot_private_relay_tools")


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
            logger.warning(
                "Relay transaction action rejected: "
                f"conversation_id={conversation_id}, "
                f"intent={self.action_intent}, "
                f"caller_bot={caller_bot}, "
                f"status={code}"
            )
            return False, {"status": code, "intent": self.action_intent, "reason": reason}
        if self.action_intent == "confirm":
            config = getattr(self.plugin, "config", None)
            if not isinstance(config, BotPrivateRelayConfig):
                logger.warning(
                    "Relay transaction confirm rejected: relay config unavailable, "
                    f"conversation_id={conversation_id}, caller_bot={caller_bot}"
                )
                return False, {
                    "status": "relay_config_unavailable",
                    "intent": self.action_intent,
                    "conversation_id": conversation_id,
                    "reason": reason,
                }
            if not config.todo_bridge.enabled:
                bridge_status = "todo_bridge_disabled"
                bridge_result: dict[str, Any] = {}
            else:
                record = store.TRANSACTION_LOG.get(conversation_id)
                if record is None:
                    logger.warning(
                        "Relay transaction confirm rejected: transaction record missing, "
                        f"conversation_id={conversation_id}, caller_bot={caller_bot}"
                    )
                    return False, {
                        "status": "transaction_record_missing",
                        "intent": self.action_intent,
                        "conversation_id": conversation_id,
                        "reason": reason,
                    }
                ok, bridge_status, bridge_result = await TodoBridge(config).publish_final_decision(
                    record=record,
                    final_intent=self.action_intent,
                    owner_bot=caller_bot,
                    peer_bot_id=_session.peer_bot_id,
                )
                if not ok:
                    logger.warning(
                        "Relay transaction confirm rejected by todo bridge: "
                        f"conversation_id={conversation_id}, "
                        f"caller_bot={caller_bot}, "
                        f"todo_bridge_status={bridge_status}, "
                        f"todo_uid={bridge_result.get('todo_uid', '')}"
                    )
                    return False, {
                        "status": bridge_status,
                        "intent": self.action_intent,
                        "conversation_id": conversation_id,
                        "state": _session.state if _session is not None else None,
                        "reason": reason,
                        "todo_bridge_status": bridge_status,
                        "todo_bridge": bridge_result,
                    }

        session = manager.apply_transaction_action(
            conversation_id=conversation_id,
            action=self.action_intent,
            caller_bot=caller_bot,
        )
        payload: dict[str, Any] = {
            "status": "ok",
            "intent": self.action_intent,
            "conversation_id": conversation_id,
            "state": session.state,
            "reason": reason,
        }
        if self.action_intent == "confirm":
            payload["todo_bridge_status"] = bridge_status
            payload["todo_bridge"] = bridge_result
        logger.info(
            "Relay transaction action applied: "
            f"conversation_id={conversation_id}, "
            f"intent={self.action_intent}, "
            f"caller_bot={caller_bot}, "
            f"state={session.state}, "
            f"terminal={session.terminal}, "
            f"reply_budget={session.reply_budget}"
        )
        if self.action_intent == "confirm":
            logger.info(
                "Relay transaction confirm todo bridge result: "
                f"conversation_id={conversation_id}, "
                f"todo_bridge_status={bridge_status}, "
                f"todo_uid={bridge_result.get('todo_uid', '')}"
            )
        return True, payload


class AcceptTransactionTool(_BaseRelayTransactionTool):
    """Accept a pending transaction request."""

    tool_name = "accept_transaction"
    tool_description = "接受事务请求并进入 accepted 状态，同时执行六项硬校验。"
    action_intent = "accept"


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


class RescheduleTransactionTool(_BaseRelayTransactionTool):
    """Request a transaction reschedule."""

    tool_name = "reschedule_transaction"
    tool_description = "对事务请求提出改期，并执行六项硬校验。"
    action_intent = "reschedule"


class AckTransactionTool(_BaseRelayTransactionTool):
    """Acknowledge and close a pending transaction request."""

    tool_name = "ack_transaction"
    tool_description = "对事务请求执行收到确认并关闭事务，同时执行六项硬校验。"
    action_intent = "ack"


class CloseTransactionTool(_BaseRelayTransactionTool):
    """Close a pending or reschedule transaction request."""

    tool_name = "close_transaction"
    tool_description = "对事务请求执行关闭，并执行六项硬校验。"
    action_intent = "close"
