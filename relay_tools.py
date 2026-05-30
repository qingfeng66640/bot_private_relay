"""Relay transaction tools with hard validation gates."""

# =============================================================================
# 事务协议 Tool 组件
# =============================================================================
# Tool 是 LLM 可调用的"查询型"函数，用于执行事务协议操作。
# 与 Action 不同，Tool 不直接产生消息副作用，而是修改会话状态。
# 消息副作用由 Action（send_text）负责。
#
# 所有事务 Tool 都直接继承 BaseTool，并调用共享执行函数提供统一的：
# 1. 六项硬校验（validate_transaction_action）
# 2. 状态推进（apply_transaction_action）
# 3. Confirm 时的 Todo Bridge 桥接
#
# 七个事务 Tool：
# - accept_transaction      接受事务请求 → 进入 accepted 状态
# - confirm_transaction     确认事务 → 进入 closed 终态，触发 todo bridge
# - decline_transaction     拒绝事务 → 进入 closed 终态
# - cancel_transaction      取消事务 → 进入 closed 终态
# - reschedule_transaction  提出改期 → 进入 reschedule_requested 状态
# - ack_transaction         确认收到并关闭 → 进入 closed 终态
# - close_transaction       关闭事务 → 进入 closed 终态
#
# 事务状态机（详见 session.py）：
#   created → pending_reply → accepted → (confirm) → closed
#                            → declined/cancelled → closed
#                            → reschedule_requested → (confirm) → closed
# =============================================================================

from __future__ import annotations

from typing import Any

from src.app.plugin_system.base import BaseTool
from src.kernel.logger import get_logger

from . import store
from .config import BotPrivateRelayConfig
from .session import SessionManager
from .todo_bridge import TodoBridge


logger = get_logger("bot_private_relay_tools")


# =============================================================================
# 事务 Tool 共享执行逻辑
# =============================================================================
RELAY_CHATTER_ALLOW = ["bot_relay_chatter"]
RELAY_ASSOCIATED_PLATFORMS = ["bot_relay"]


async def _execute_transaction_action(
    *,
    plugin: object,
    action_intent: str,
    conversation_id: str,
    caller_bot: str,
    reason: str,
) -> tuple[bool, dict[str, Any]]:
    """Validate and apply a transaction action.

    执行流程：
    1. 六项硬校验（validate_transaction_action）
    2. confirm 特殊处理：通过 TodoBridge 发布 Todo 决策
    3. 推进会话状态（apply_transaction_action）
    """

    manager = SessionManager()
    ok, code, session_for_validation = manager.validate_transaction_action(
        conversation_id=conversation_id,
        action=action_intent,
        caller_bot=caller_bot,
        payload_complete=bool(conversation_id),
    )
    if not ok:
        logger.warning(
            "Relay transaction action rejected: "
            f"conversation_id={conversation_id}, "
            f"intent={action_intent}, "
            f"caller_bot={caller_bot}, "
            f"status={code}"
        )
        return False, {"status": code, "intent": action_intent, "reason": reason}

    if action_intent == "confirm":
        config = getattr(plugin, "config", None)
        if not isinstance(config, BotPrivateRelayConfig):
            logger.warning(
                "Relay transaction confirm rejected: relay config unavailable, "
                f"conversation_id={conversation_id}, caller_bot={caller_bot}"
            )
            return False, {
                "status": "relay_config_unavailable",
                "intent": action_intent,
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
                    "intent": action_intent,
                    "conversation_id": conversation_id,
                    "reason": reason,
                }

            ok, bridge_status, bridge_result = await TodoBridge(config).publish_final_decision(
                record=record,
                final_intent=action_intent,
                owner_bot=caller_bot,
                peer_bot_id=session_for_validation.peer_bot_id,
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
                    "intent": action_intent,
                    "conversation_id": conversation_id,
                    "state": session_for_validation.state,
                    "reason": reason,
                    "todo_bridge_status": bridge_status,
                    "todo_bridge": bridge_result,
                }

    session = manager.apply_transaction_action(
        conversation_id=conversation_id,
        action=action_intent,
        caller_bot=caller_bot,
    )

    payload: dict[str, Any] = {
        "status": "ok",
        "intent": action_intent,
        "conversation_id": conversation_id,
        "state": session.state,
        "reason": reason,
    }
    if action_intent == "confirm":
        payload["todo_bridge_status"] = bridge_status
        payload["todo_bridge"] = bridge_result

    logger.info(
        "Relay transaction action applied: "
        f"conversation_id={conversation_id}, "
        f"intent={action_intent}, "
        f"caller_bot={caller_bot}, "
        f"state={session.state}, "
        f"terminal={session.terminal}, "
        f"reply_budget={session.reply_budget}"
    )
    if action_intent == "confirm":
        logger.info(
            "Relay transaction confirm todo bridge result: "
            f"conversation_id={conversation_id}, "
            f"todo_bridge_status={bridge_status}, "
            f"todo_uid={bridge_result.get('todo_uid', '')}"
        )
    return True, payload


# =============================================================================
# 七个事务 Tool 的具体实现
# 每个 Tool 直接继承 BaseTool，执行逻辑委托给 _execute_transaction_action。
# =============================================================================

class AcceptTransactionTool(BaseTool):
    """Accept a pending transaction request.

    接受事务请求并进入 accepted 状态。
    可用状态：pending_reply → accepted
    """

    tool_name = "accept_transaction"
    tool_description = "接受事务请求并进入 accepted 状态，同时执行六项硬校验。"
    action_intent = "accept"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """Accept a transaction after hard validation."""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class ConfirmTransactionTool(BaseTool):
    """Confirm a pending transaction request.

    确认事务并进入 closed 终态。这是唯一触发 Todo Bridge 的操作。
    可用状态：accepted / reschedule_requested → closed
    """

    tool_name = "confirm_transaction"
    tool_description = "对事务请求执行确认，并执行六项硬校验。"
    action_intent = "confirm"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """Confirm a transaction and publish todo projection when enabled."""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class DeclineTransactionTool(BaseTool):
    """Decline a pending transaction request.

    拒绝事务并进入 closed 终态。
    """

    tool_name = "decline_transaction"
    tool_description = "对事务请求执行拒绝，并执行六项硬校验。"
    action_intent = "decline"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """Decline a transaction after hard validation."""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class CancelTransactionTool(BaseTool):
    """Cancel a pending transaction request.

    取消事务并进入 closed 终态。
    """

    tool_name = "cancel_transaction"
    tool_description = "对事务请求执行取消，并执行六项硬校验。"
    action_intent = "cancel"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """Cancel a transaction after hard validation."""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class RescheduleTransactionTool(BaseTool):
    """Request a transaction reschedule.

    提出改期方案，进入 reschedule_requested 状态。
    对端收到后可以 confirm（接受改期）或提出新的 reschedule。
    """

    tool_name = "reschedule_transaction"
    tool_description = "对事务请求提出改期，并执行六项硬校验。"
    action_intent = "reschedule"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """Request transaction reschedule after hard validation."""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class AckTransactionTool(BaseTool):
    """Acknowledge and close a pending transaction request.

    确认收到并关闭事务。不同于 confirm，不会触发 Todo Bridge。
    """

    tool_name = "ack_transaction"
    tool_description = "对事务请求执行收到确认并关闭事务，同时执行六项硬校验。"
    action_intent = "ack"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """Acknowledge and close a transaction after hard validation."""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class CloseTransactionTool(BaseTool):
    """Close a pending or reschedule transaction request.

    关闭事务并进入 closed 终态。
    """

    tool_name = "close_transaction"
    tool_description = "对事务请求执行关闭，并执行六项硬校验。"
    action_intent = "close"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """Close a transaction after hard validation."""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )
