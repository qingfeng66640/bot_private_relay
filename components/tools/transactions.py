"""带硬校验门控的中继事务工具。"""

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

from ...runtime import store
from ...runtime.session import SessionManager
from ...runtime.todo_bridge import TodoBridge
from ..config import BotPrivateRelayConfig


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
    """校验并应用事务操作。

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
            "中继事务操作被拒绝: "
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
                "中继事务确认被拒绝: 中继配置不可用, "
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
                    "中继事务确认被拒绝: 事务记录缺失, "
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
                    "中继事务确认被 Todo Bridge 拒绝: "
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
        "中继事务操作已应用: "
        f"conversation_id={conversation_id}, "
        f"intent={action_intent}, "
        f"caller_bot={caller_bot}, "
        f"state={session.state}, "
        f"terminal={session.terminal}, "
        f"reply_budget={session.reply_budget}"
    )
    if action_intent == "confirm":
        logger.info(
            "中继事务确认 Todo Bridge 结果: "
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
    """接受待处理的事务请求。

    接受事务请求并进入 accepted 状态。
    可用状态：pending_reply → accepted
    """

    tool_name = "accept_transaction"
    tool_description = "接受事务请求并进入 accepted 状态，同时执行六项硬校验。"
    action_intent = "accept"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """通过硬校验后接受事务。"""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class ConfirmTransactionTool(BaseTool):
    """确认待处理的事务请求。

    确认事务并进入 closed 终态。这是唯一触发 Todo Bridge 的操作。
    可用状态：accepted / reschedule_requested → closed
    """

    tool_name = "confirm_transaction"
    tool_description = "对事务请求执行确认，并执行六项硬校验。"
    action_intent = "confirm"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """确认事务并在启用时发布 Todo 投影。"""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class DeclineTransactionTool(BaseTool):
    """拒绝待处理的事务请求。

    拒绝事务并进入 closed 终态。
    """

    tool_name = "decline_transaction"
    tool_description = "对事务请求执行拒绝，并执行六项硬校验。"
    action_intent = "decline"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """通过硬校验后拒绝事务。"""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class CancelTransactionTool(BaseTool):
    """取消待处理的事务请求。

    取消事务并进入 closed 终态。
    """

    tool_name = "cancel_transaction"
    tool_description = "对事务请求执行取消，并执行六项硬校验。"
    action_intent = "cancel"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """通过硬校验后取消事务。"""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class RescheduleTransactionTool(BaseTool):
    """请求事务改期。

    提出改期方案，进入 reschedule_requested 状态。
    对端收到后可以 confirm（接受改期）或提出新的 reschedule。
    """

    tool_name = "reschedule_transaction"
    tool_description = "对事务请求提出改期，并执行六项硬校验。"
    action_intent = "reschedule"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """通过硬校验后请求事务改期。"""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class AckTransactionTool(BaseTool):
    """确认收到并关闭待处理的事务请求。

    确认收到并关闭事务。不同于 confirm，不会触发 Todo Bridge。
    """

    tool_name = "ack_transaction"
    tool_description = "对事务请求执行收到确认并关闭事务，同时执行六项硬校验。"
    action_intent = "ack"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """通过硬校验后确认收到并关闭事务。"""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )


class CloseTransactionTool(BaseTool):
    """关闭待处理或改期的事务请求。

    关闭事务并进入 closed 终态。
    """

    tool_name = "close_transaction"
    tool_description = "对事务请求执行关闭，并执行六项硬校验。"
    action_intent = "close"
    chatter_allow = RELAY_CHATTER_ALLOW
    associated_platforms = RELAY_ASSOCIATED_PLATFORMS

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """通过硬校验后关闭事务。"""
        return await _execute_transaction_action(
            plugin=self.plugin,
            action_intent=self.action_intent,
            conversation_id=conversation_id,
            caller_bot=caller_bot,
            reason=reason,
        )
