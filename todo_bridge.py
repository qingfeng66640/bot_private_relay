"""Bridge final relay transactions to the external todo plugin."""

from __future__ import annotations

import asyncio
from typing import Any

from src.kernel.event import get_event_bus
from src.kernel.logger import get_logger

from . import store
from .config import BotPrivateRelayConfig


logger = get_logger("bot_private_relay_todo_bridge")


class TodoBridge:
    """Publish idempotent todo events after final transaction decisions."""

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
        """Publish the todo bridge event with retry.

        Returns:
            ``(ok, status_code, result)`` from the todo bridge.
        """

        bridge = self.config.todo_bridge
        if not bridge.enabled:
            logger.info(
                "Relay todo bridge skipped: disabled, "
                f"conversation_id={record.conversation_id}, final_intent={final_intent}"
            )
            return True, "todo_bridge_disabled", {}
        if final_intent != "confirm":
            logger.debug(
                "Relay todo bridge skipped: non-final intent, "
                f"conversation_id={record.conversation_id}, final_intent={final_intent}"
            )
            return True, "todo_bridge_skipped", {}

        normalized_peer_bot_id = self._peer_for_owner(
            record=record,
            owner_bot=owner_bot,
            explicit_peer_bot_id=peer_bot_id,
        )
        plan_title = self._owner_view_title(
            record=record,
            owner_bot=owner_bot,
            peer_bot_id=normalized_peer_bot_id,
        )

        payload = {
            "source": "bot_private_relay",
            "conversation_id": record.conversation_id,
            "trace_id": record.trace_id,
            "decision": final_intent,
            "from_bot": record.from_bot,
            "to_bot": record.to_bot,
            "owner_bot": owner_bot,
            "participants": [record.from_bot, record.to_bot],
            "title": plan_title,
            "summary": record.summary or record.topic or "relay task",
            "due_at": None,
            "source_stream_id": f"bot_relay:{owner_bot}",
            "peer_bot_id": normalized_peer_bot_id,
            "source_message_id": "",
        }
        result: dict[str, Any] = {"ok": None, "todo_uid": "", "status": "", "error": ""}
        attempts = max(0, int(bridge.max_retries)) + 1
        logger.info(
            "Publishing relay todo decision: "
            f"event_name={bridge.event_name}, "
            f"conversation_id={record.conversation_id}, "
            f"owner_bot={owner_bot}, "
            f"peer_bot_id={normalized_peer_bot_id}, "
            f"title={plan_title}"
        )
        for attempt in range(attempts):
            params: dict[str, Any] = {"payload": dict(payload), "result": dict(result)}
            try:
                _decision, out = await get_event_bus().publish(bridge.event_name, params)
                event_result = out.get("result") if isinstance(out, dict) else None
                if isinstance(event_result, dict) and event_result.get("ok") is True:
                    logger.info(
                        "Relay todo bridge accepted decision: "
                        f"conversation_id={record.conversation_id}, "
                        f"owner_bot={owner_bot}, peer_bot_id={normalized_peer_bot_id}, "
                        f"attempt={attempt + 1}/{attempts}, "
                        f"status={event_result.get('status')}, "
                        f"todo_uid={event_result.get('todo_uid', '')}"
                    )
                    return True, str(event_result.get("status") or "ok"), event_result
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
                    "Relay todo bridge publish failed: "
                    f"conversation_id={record.conversation_id}, "
                    f"attempt={attempt + 1}/{attempts}, "
                    f"error={exc}"
                )
            if attempt + 1 < attempts:
                retry_backoff = max(0.0, float(bridge.retry_backoff_seconds))
                logger.warning(
                    "Relay todo bridge publish attempt failed; retrying: "
                    f"conversation_id={record.conversation_id}, "
                    f"owner_bot={owner_bot}, peer_bot_id={normalized_peer_bot_id}, "
                    f"attempt={attempt + 1}/{attempts}, "
                    f"status={result.get('status', '')}, error={result.get('error', '')}, "
                    f"retry_after_seconds={retry_backoff}"
                )
                await asyncio.sleep(retry_backoff)
        if str(result.get("status") or "") == "todo_bridge_unavailable":
            result["status"] = "todo_bridge_retry_exhausted"
        status = str(result.get("status") or "todo_bridge_retry_exhausted")
        logger.warning(
            "Relay todo bridge exhausted: "
            f"conversation_id={record.conversation_id}, "
            f"owner_bot={owner_bot}, peer_bot_id={normalized_peer_bot_id}, "
            f"attempts={attempts}, "
            f"status={status}, "
            f"error={result.get('error', '')}"
        )
        if bridge.fail_transaction_on_unavailable:
            return False, status, result
        return True, status, result

    @staticmethod
    def _peer_for_owner(
        *,
        record: store.RelayTransactionRecord,
        owner_bot: str,
        explicit_peer_bot_id: str,
    ) -> str:
        """Return the transaction participant that is not owner_bot."""

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
        """Build a conservative owner-view title without self-reference."""

        base = (record.summary or record.topic or "relay task").strip()
        if not base:
            base = "relay task"
        cleaned = base.rstrip("?？")
        if peer_bot_id and peer_bot_id not in cleaned:
            return f"与 {peer_bot_id} 确认的计划：{cleaned}"
        if owner_bot and f"与 {owner_bot}" in cleaned and peer_bot_id:
            return cleaned.replace(f"与 {owner_bot}", f"与 {peer_bot_id}")
        return cleaned
