"""Relay transaction tools with hard validation gates."""

# =============================================================================
# 事务协议 Tool 组件
# =============================================================================
# Tool 是 LLM 可调用的"查询型"函数，用于执行事务协议操作。
# 与 Action 不同，Tool 不直接产生消息副作用，而是修改会话状态。
# 消息副作用由 Action（send_text）负责。
#
# 所有事务 Tool 都继承自 _BaseRelayTransactionTool，该基类提供了统一的：
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
# _BaseRelayTransactionTool - 事务 Tool 基类
# =============================================================================
class _BaseRelayTransactionTool(BaseTool):
    """Shared transaction tool behavior.

    所有事务 Tool 的公共逻辑：
    1. 六项硬校验（validate_transaction_action）
    2. 状态推进（apply_transaction_action）
    3. confirm 特殊处理：先通过 TodoBridge 发布 Todo 决策
    """

    chatter_allow = ["bot_relay_chatter"]             # 仅对 relay chatter 可用
    associated_platforms = ["bot_relay"]               # 仅在 bot_relay 平台可用

    action_intent = ""  # 子类必须覆盖：accept/confirm/decline/cancel/reschedule/ack/close

    def _manager(self) -> SessionManager:
        """创建 SessionManager 实例（无状态）。"""
        return SessionManager()

    async def execute(self, conversation_id: str, caller_bot: str, reason: str = "") -> tuple[bool, dict[str, Any]]:
        """Validate and apply a transaction action.

        执行流程：
        1. 六项硬校验（validate_transaction_action）：
           - 会话是否存在
           - 会话是否已关闭
           - 当前状态是否允许此操作
           - 调用方是否在 allowed_responders 中
           - 回复预算是否耗尽
           - payload 是否完整
        2. 如果是 confirm 操作：
           - 检查 transaction record 是否存在
           - 通过 TodoBridge 发布 Todo 决策事件
           - 如果 todo bridge 失败且配置要求阻止，拒绝确认
        3. 推进会话状态（apply_transaction_action）

        Args:
            conversation_id: 目标事务的 conversation_id。
            caller_bot: 调用方 bot_id（用于权限校验）。
            reason: 可选的自由文本原因说明。
        """

        manager = self._manager()

        # ── 第一步：六项硬校验 ──
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

        # ── 第二步：confirm 特殊处理 —— Todo Bridge 桥接 ──
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
                # Todo 桥接被禁用 → 跳过，但不阻止 confirm
                bridge_status = "todo_bridge_disabled"
                bridge_result: dict[str, Any] = {}
            else:
                # 检查事务记录是否存在
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

                # 通过事件总线发布 Todo 决策
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

        # ── 第三步：推进会话状态 ──
        session = manager.apply_transaction_action(
            conversation_id=conversation_id,
            action=self.action_intent,
            caller_bot=caller_bot,
        )

        # ── 构建返回 payload ──
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


# =============================================================================
# 七个事务 Tool 的具体实现
# 每个 Tool 只需设置 tool_name、tool_description、action_intent
# 所有逻辑继承自 _BaseRelayTransactionTool
# =============================================================================

class AcceptTransactionTool(_BaseRelayTransactionTool):
    """Accept a pending transaction request.

    接受事务请求并进入 accepted 状态。
    可用状态：pending_reply → accepted
    """

    tool_name = "accept_transaction"
    tool_description = "接受事务请求并进入 accepted 状态，同时执行六项硬校验。"
    action_intent = "accept"


class ConfirmTransactionTool(_BaseRelayTransactionTool):
    """Confirm a pending transaction request.

    确认事务并进入 closed 终态。这是唯一触发 Todo Bridge 的操作。
    可用状态：accepted / reschedule_requested → closed
    """

    tool_name = "confirm_transaction"
    tool_description = "对事务请求执行确认，并执行六项硬校验。"
    action_intent = "confirm"


class DeclineTransactionTool(_BaseRelayTransactionTool):
    """Decline a pending transaction request.

    拒绝事务并进入 closed 终态。
    """

    tool_name = "decline_transaction"
    tool_description = "对事务请求执行拒绝，并执行六项硬校验。"
    action_intent = "decline"


class CancelTransactionTool(_BaseRelayTransactionTool):
    """Cancel a pending transaction request.

    取消事务并进入 closed 终态。
    """

    tool_name = "cancel_transaction"
    tool_description = "对事务请求执行取消，并执行六项硬校验。"
    action_intent = "cancel"


class RescheduleTransactionTool(_BaseRelayTransactionTool):
    """Request a transaction reschedule.

    提出改期方案，进入 reschedule_requested 状态。
    对端收到后可以 confirm（接受改期）或提出新的 reschedule。
    """

    tool_name = "reschedule_transaction"
    tool_description = "对事务请求提出改期，并执行六项硬校验。"
    action_intent = "reschedule"


class AckTransactionTool(_BaseRelayTransactionTool):
    """Acknowledge and close a pending transaction request.

    确认收到并关闭事务。不同于 confirm，不会触发 Todo Bridge。
    """

    tool_name = "ack_transaction"
    tool_description = "对事务请求执行收到确认并关闭事务，同时执行六项硬校验。"
    action_intent = "ack"


class CloseTransactionTool(_BaseRelayTransactionTool):
    """Close a pending or reschedule transaction request.

    关闭事务并进入 closed 终态。
    """

    tool_name = "close_transaction"
    tool_description = "对事务请求执行关闭，并执行六项硬校验。"
    action_intent = "close"
