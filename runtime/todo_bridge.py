"""将最终中继事务桥接到外部 todo 插件。"""

# =============================================================================
# TodoBridge - 事务到 Todo 的桥接
# =============================================================================
# 当 relay 事务被 confirm（确认）后，通过事件总线将事务信息发布给
# todo_plugin，自动创建待办事项。
#
# 核心流程：
# 1. 检查 todo_bridge 是否启用
# 2. 检查 intent 是否为 confirm（只有 confirm 触发 Todo 创建）
# 3. 解析 peer_bot_id（确定事务的另一方）
# 4. 构建 Todo 标题（owner 视角）
# 5. 通过事件总线发布 bot_relay.todo_decided 事件
# 6. 根据配置决定失败时是否阻止 confirm
#
# 重试机制：如果 todo_plugin 未响应或响应失败，最多重试 max_retries 次。
# =============================================================================

from __future__ import annotations

import asyncio
from typing import Any

from src.kernel.event import get_event_bus
from src.kernel.logger import get_logger

from . import store
from ..components.config import BotPrivateRelayConfig


logger = get_logger("bot_private_relay_todo_bridge")


class TodoBridge:
    """在事务最终决策后发布幂等的 todo 事件。

    注意：Todo 事件的发布是幂等的，同一个 conversation_id 的 confirm 事件
    可以被多次发布，todo_plugin 侧需要自行处理去重。
    """

    def __init__(self, config: BotPrivateRelayConfig) -> None:
        self.config = config

    async def publish_final_decision(
        self,
        *,
        record: store.RelayTransactionRecord,
        final_intent: str,
        owner_bot: str,
        peer_bot_id: str,
    ) -> tuple[bool, str, dict[str, Any]]:
        """发布 Todo 桥接事件（带重试）。

        发布 Todo 决策事件的完整流程。

        Args:
            record: 事务记录。
            final_intent: 最终意图（应为 "confirm"）。
            owner_bot: Todo 拥有者 bot_id。
            peer_bot_id: 事务对端 bot_id。

        Returns:
            ``(ok, status_code, result)`` 其中 result 包含 todo_uid 等字段。
        """

        bridge = self.config.todo_bridge

        # ── 1. 检查是否启用 ──
        if not bridge.enabled:
            logger.info(
                "Relay todo bridge 已跳过：未启用, "
                f"conversation_id={record.conversation_id}, final_intent={final_intent}"
            )
            return True, "todo_bridge_disabled", {}

        # ── 2. 检查 intent（只有 confirm 才触发） ──
        if final_intent != "confirm":
            logger.debug(
                "Relay todo bridge 已跳过：非最终意图, "
                f"conversation_id={record.conversation_id}, final_intent={final_intent}"
            )
            return True, "todo_bridge_skipped", {}

        # ── 3. 解析 peer_bot_id ──
        normalized_peer_bot_id = self._peer_for_owner(
            record=record,
            owner_bot=owner_bot,
            explicit_peer_bot_id=peer_bot_id,
        )

        # ── 4. 构建 Todo 标题 ──
        plan_title = self._owner_view_title(
            record=record,
            owner_bot=owner_bot,
            peer_bot_id=normalized_peer_bot_id,
        )

        # ── 5. 构建事件 payload ──
        relay_todo_key = f"bot_relay.todo:{owner_bot}"
        payload = {
            "source": "bot_private_relay",
            "conversation_id": record.conversation_id,
            "trace_id": record.trace_id,
            "decision": final_intent,
            "from_bot": record.from_bot,
            "to_bot": record.to_bot,
            "owner_bot": owner_bot,
            "participants": [record.from_bot, record.to_bot],  # 事务参与者列表
            "title": plan_title,
            "summary": record.summary or record.topic or "relay task",
            "due_at": record.due_at,
            "due_at_text": record.due_at_text,
            "relay_todo_key": relay_todo_key,
            "peer_bot_id": normalized_peer_bot_id,
            "source_message_id": "",
        }

        # ── 6. 发布事件（带重试） ──
        result: dict[str, Any] = {"ok": None, "todo_uid": "", "status": "", "error": ""}
        attempts = max(0, int(bridge.max_retries)) + 1

        logger.info(
            "正在发布 relay todo 决策: "
            f"event_name={bridge.event_name}, "
            f"conversation_id={record.conversation_id}, "
            f"owner_bot={owner_bot}, "
            f"peer_bot_id={normalized_peer_bot_id}, "
            f"relay_todo_key={relay_todo_key}, "
            f"title={plan_title}"
        )

        for attempt in range(attempts):
            params: dict[str, Any] = {"payload": dict(payload), "result": dict(result)}
            try:
                # 通过事件总线发布事件
                _decision, out = await get_event_bus().publish(bridge.event_name, params)
                event_result = out.get("result") if isinstance(out, dict) else None

                if isinstance(event_result, dict) and event_result.get("ok") is True:
                    # ── Todo 发布成功 ──
                    logger.info(
                        "Relay todo bridge 已接受决策: "
                        f"conversation_id={record.conversation_id}, "
                        f"owner_bot={owner_bot}, peer_bot_id={normalized_peer_bot_id}, "
                        f"attempt={attempt + 1}/{attempts}, "
                        f"status={event_result.get('status')}, "
                        f"todo_uid={event_result.get('todo_uid', '')}"
                    )
                    return True, str(event_result.get("status") or "ok"), event_result

                # ── 响应格式不对或无监听者 ──
                result = event_result if isinstance(event_result, dict) else result
                if isinstance(event_result, dict) and not event_result.get("status"):
                    result = {
                        "ok": False,
                        "todo_uid": "",
                        "status": "todo_bridge_unavailable",
                        "error": "no todo bridge listener",
                    }
                if event_result is None:
                    result = {"ok": False, "todo_uid": "", "status": "todo_bridge_unavailable", "error": "no todo bridge listener"}

            except Exception as exc:
                result = {"ok": False, "todo_uid": "", "status": "todo_bridge_failed", "error": str(exc)}
                logger.warning(
                    "Relay todo bridge 发布失败: "
                    f"conversation_id={record.conversation_id}, "
                    f"attempt={attempt + 1}/{attempts}, "
                    f"error={exc}"
                )

            # ── 重试前等待 ──
            if attempt + 1 < attempts:
                retry_backoff = max(0.0, float(bridge.retry_backoff_seconds))
                logger.warning(
                    "Relay todo bridge 发布尝试失败；正在重试: "
                    f"conversation_id={record.conversation_id}, "
                    f"owner_bot={owner_bot}, peer_bot_id={normalized_peer_bot_id}, "
                    f"attempt={attempt + 1}/{attempts}, "
                    f"status={result.get('status', '')}, error={result.get('error', '')}, "
                    f"retry_after_seconds={retry_backoff}"
                )
                await asyncio.sleep(retry_backoff)

        # ── 所有重试耗尽 ──
        if str(result.get("status") or "") == "todo_bridge_unavailable":
            result["status"] = "todo_bridge_retry_exhausted"

        status = str(result.get("status") or "todo_bridge_retry_exhausted")
        logger.warning(
            "Relay todo bridge 重试已耗尽: "
            f"conversation_id={record.conversation_id}, "
            f"owner_bot={owner_bot}, peer_bot_id={normalized_peer_bot_id}, "
            f"attempts={attempts}, "
            f"status={status}, "
            f"error={result.get('error', '')}"
        )

        # ── 根据配置决定是否阻止 confirm ──
        if bridge.fail_transaction_on_unavailable:
            return False, status, result
        return True, status, result

    # =========================================================================
    # 辅助方法
    # =========================================================================

    @staticmethod
    def _peer_for_owner(
        *,
        record: store.RelayTransactionRecord,
        owner_bot: str,
        explicit_peer_bot_id: str,
    ) -> str:
        """返回事务中不等于 owner_bot 的参与者。

        从事务记录中找出"对端"的 bot_id。
        逻辑：在 record.from_bot 和 record.to_bot 中找不等于 owner_bot 的那个。
        """

        if explicit_peer_bot_id and explicit_peer_bot_id != owner_bot:
            return explicit_peer_bot_id
        for candidate in (record.from_bot, record.to_bot):
            if candidate and candidate != owner_bot:
                return candidate
        return explicit_peer_bot_id

    @staticmethod
    def _owner_view_title(
        *,
        record: store.RelayTransactionRecord,
        owner_bot: str,
        peer_bot_id: str,
    ) -> str:
        """构建不包含自身引用的保守型 owner 视角标题。

        构建 Todo 标题。格式：
        "与 {peer_bot_id} 确认的计划：{summary/topic}"

        避免在标题中包含 owner_bot 自身的 ID。
        """

        base = (record.summary or record.topic or "relay task").strip()
        if not base:
            base = "relay task"
        cleaned = base.rstrip("?？")  # 去除末尾问号
        if peer_bot_id and peer_bot_id not in cleaned:
            return f"与 {peer_bot_id} 确认的计划：{cleaned}"
        if owner_bot and f"与 {owner_bot}" in cleaned and peer_bot_id:
            return cleaned.replace(f"与 {owner_bot}", f"与 {peer_bot_id}")
        return cleaned
