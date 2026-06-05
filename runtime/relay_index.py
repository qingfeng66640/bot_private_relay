"""Relay 对话轻量索引。"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.app.plugin_system.types import Message
from src.kernel.logger import get_logger

logger = get_logger("bot_private_relay_index")
_INDEX_LOCK = asyncio.Lock()
_ALLOWED_FIELDS = {"conversation_id", "stream_id", "peer_bot_id", "peer_bot_name", "channel", "updated_at"}


@dataclass(slots=True)
class RelayConversationIndex:
    """一条 relay conversation 到框架 stream 的轻量映射。"""

    conversation_id: str
    stream_id: str
    peer_bot_id: str
    peer_bot_name: str
    channel: str
    updated_at: float


def _index_path(index_file: str) -> Path:
    """解析索引文件路径。"""

    return Path(index_file).expanduser()


def _coerce_record(raw: dict[str, Any]) -> RelayConversationIndex | None:
    """将 JSON 记录转换为索引对象，丢弃非允许字段。"""

    conversation_id = str(raw.get("conversation_id") or "").strip()
    stream_id = str(raw.get("stream_id") or "").strip()
    peer_bot_id = str(raw.get("peer_bot_id") or "").strip()
    if not conversation_id or not stream_id or not peer_bot_id:
        return None
    return RelayConversationIndex(
        conversation_id=conversation_id,
        stream_id=stream_id,
        peer_bot_id=peer_bot_id,
        peer_bot_name=str(raw.get("peer_bot_name") or peer_bot_id).strip(),
        channel=str(raw.get("channel") or "").strip(),
        updated_at=float(raw.get("updated_at") or 0.0),
    )


def _cleanup(
    records: dict[str, RelayConversationIndex],
    *,
    max_index_conversations: int,
    lookback_hours: float,
    now: float | None = None,
) -> dict[str, RelayConversationIndex]:
    """按时间窗口和数量上限清理索引。"""

    current = time.time() if now is None else now
    cutoff = current - max(0.0, float(lookback_hours)) * 3600
    kept = [record for record in records.values() if float(record.updated_at or 0.0) >= cutoff]
    kept.sort(key=lambda item: item.updated_at, reverse=True)
    if max_index_conversations > 0:
        kept = kept[:max_index_conversations]
    return {record.conversation_id: record for record in kept}


async def load_index(index_file: str) -> list[RelayConversationIndex]:
    """读取 relay conversation 索引。"""

    path = _index_path(index_file)
    async with _INDEX_LOCK:
        return await asyncio.to_thread(_load_index_unlocked, path)


def _load_index_unlocked(path: Path) -> list[RelayConversationIndex]:
    """在已持锁状态下读取索引。"""

    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(f"relay conversation index read failed, ignore stale file: {exc}")
        return []

    raw_records = payload.get("conversations", []) if isinstance(payload, dict) else payload
    if not isinstance(raw_records, list):
        return []

    records: list[RelayConversationIndex] = []
    for raw in raw_records:
        if isinstance(raw, dict):
            record = _coerce_record({key: raw.get(key) for key in _ALLOWED_FIELDS})
            if record is not None:
                records.append(record)
    return records


async def upsert_from_message(
    message: Message,
    *,
    index_file: str,
    max_index_conversations: int,
    lookback_hours: float,
    envelope: dict[str, Any] | None = None,
) -> None:
    """从 relay 消息中提取 conversation 元数据并更新轻量索引。"""

    record = _record_from_message(message, envelope=envelope)
    if record is None:
        return
    await upsert_record(
        record,
        index_file=index_file,
        max_index_conversations=max_index_conversations,
        lookback_hours=lookback_hours,
    )


async def upsert_record(
    record: RelayConversationIndex,
    *,
    index_file: str,
    max_index_conversations: int,
    lookback_hours: float,
) -> None:
    """更新一条 relay conversation 索引。"""

    path = _index_path(index_file)
    async with _INDEX_LOCK:
        loaded = await asyncio.to_thread(_load_index_unlocked, path)
        records = {item.conversation_id: item for item in loaded}
        records[record.conversation_id] = record
        records = _cleanup(
            records,
            max_index_conversations=max_index_conversations,
            lookback_hours=lookback_hours,
        )
        await asyncio.to_thread(_write_index_unlocked, path, list(records.values()))


def _record_from_message(message: Message, *, envelope: dict[str, Any] | None = None) -> RelayConversationIndex | None:
    """从消息 extra 或 envelope 中构造索引记录。"""

    relay_context = message.extra.get("relay_context", {}) if hasattr(message, "extra") else {}
    if not isinstance(relay_context, dict):
        relay_context = {}
    relay_envelope = message.extra.get("relay_envelope", {}) if hasattr(message, "extra") else {}
    if not isinstance(relay_envelope, dict):
        relay_envelope = {}
    if isinstance(envelope, dict):
        relay_envelope = {**relay_envelope, **envelope}

    conversation_id = str(
        relay_context.get("conversation_id")
        or relay_envelope.get("conversation_id")
        or ""
    ).strip()
    stream_id = str(message.stream_id or relay_context.get("stream_id") or "").strip()
    peer_bot_id = str(
        relay_context.get("peer_bot_id")
        or relay_envelope.get("from_bot")
        or relay_envelope.get("to_bot")
        or ""
    ).strip()
    if not conversation_id or not stream_id or not peer_bot_id:
        return None

    peer_bot_name = str(
        relay_context.get("peer_bot_name")
        or relay_envelope.get("from_bot_name")
        or relay_envelope.get("to_bot_name")
        or peer_bot_id
    ).strip()
    channel = str(relay_context.get("channel") or relay_envelope.get("channel") or "").strip()
    return RelayConversationIndex(
        conversation_id=conversation_id,
        stream_id=stream_id,
        peer_bot_id=peer_bot_id,
        peer_bot_name=peer_bot_name,
        channel=channel,
        updated_at=time.time(),
    )


def _write_index_unlocked(path: Path, records: list[RelayConversationIndex]) -> None:
    """原子写入索引文件。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "conversations": [
            {key: value for key, value in asdict(record).items() if key in _ALLOWED_FIELDS}
            for record in sorted(records, key=lambda item: item.updated_at, reverse=True)
        ]
    }
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)
