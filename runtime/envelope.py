"""Relay envelope model and validation helpers."""

# =============================================================================
# RelayEnvelope - 协议信封
# =============================================================================
# RelayEnvelope 是中继通信的核心数据结构，定义了 bot 之间消息交换的完整格式。
#
# 关键字段说明：
# ──────────────────────────────────────────────────────────────────────────
# 身份与路由：
#   from_bot / to_bot          - 发送方/接收方的安全身份 ID（不可伪造）
#   from_bot_name / to_bot_name - 显示名称（仅用于日志和 prompt）
#
# 消息追踪：
#   message_id                 - 单条消息唯一 ID（用于去重）
#   conversation_id            - 对话唯一 ID（关联同一对话中的所有消息）
#   trace_id                   - 链路追踪 ID（关联整个消息链）
#   parent_message_id          - 父消息 ID（关联回复关系）
#
# 协议控制：
#   channel                    - 通信通道类型：transaction（事务）/ social（社交）/ system（系统）
#   intent                     - 意图：notify/request/invite/accept/confirm/decline/cancel 等
#   expect_reply               - 是否期待对方回复
#   reply_budget               - 剩余回复配额（每次回复减1，耗尽后禁止自动回复）
#   terminal                   - 是否为对话终点（True 表示不需要继续）
#   allowed_responders         - 允许回复的 bot_id 列表
#
# 安全控制：
#   hop                        - 当前跳数
#   ttl                        - 最大跳数（超过此值消息被丢弃，防止无限循环）
#   no_relay                   - 是否禁止再次中继转发
#
# 会话状态：
#   state                      - 事务状态：pending_reply / accepted / closed 等
#   phase                      - 社交阶段：opening / active / cooling / ending / closed
# =============================================================================

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import uuid4

# 预定义的通信通道类型
RelayChannel = Literal["system", "transaction", "social"]


@dataclass(slots=True)
class RelayEnvelope:
    """Protocol envelope exchanged between bots.

    ``from_bot`` and ``to_bot`` are the only routing/security identities.
    ``from_bot_name`` and ``to_bot_name`` are display-only.
    """

    # ── 协议版本 ──────────────────────────────────────────────────────
    protocol_version: str = "1.0"

    # ── 消息标识（自动生成 UUID） ──────────────────────────────────────
    message_id: str = field(default_factory=lambda: uuid4().hex)
    conversation_id: str = field(default_factory=lambda: uuid4().hex)
    trace_id: str = field(default_factory=lambda: uuid4().hex)
    parent_message_id: str | None = None

    # ── 身份与路由 ────────────────────────────────────────────────────
    from_bot: str = ""          # 发送方安全 ID
    from_bot_name: str = ""     # 发送方显示名称
    to_bot: str = ""            # 接收方安全 ID
    to_bot_name: str = ""       # 接收方显示名称
    sender_instance_id: str = ""  # 发送方实例 ID（预留）
    target_scope: str = "direct"  # 目标范围：direct（直连）/ broadcast（广播）

    # ── 通道与意图 ────────────────────────────────────────────────────
    channel: RelayChannel = "transaction"
    intent: str = "notify"      # 意图标识

    # ── 回复控制 ──────────────────────────────────────────────────────
    expect_reply: bool = False
    reply_budget: int = 0       # 剩余回复配额
    allowed_responders: list[str] = field(default_factory=list)

    # ── 中继控制 ──────────────────────────────────────────────────────
    hop: int = 0                # 当前跳数
    ttl: int = 4                # 最大跳数
    no_relay: bool = False      # 是否禁止再次中继

    # ── 终态控制 ──────────────────────────────────────────────────────
    terminal: bool = True

    # ── 会话状态 ──────────────────────────────────────────────────────
    state: str | None = None    # 事务状态
    phase: str | None = None    # 社交阶段

    # ── 其他 ──────────────────────────────────────────────────────────
    cooldown_seconds: int = 0   # 冷却秒数
    reply_contract: dict[str, Any] = field(default_factory=dict)  # 回复合约（预留）
    payload: dict[str, Any] = field(default_factory=dict)         # 消息正文数据
    created_at: float = field(default_factory=time.time)          # 创建时间戳

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RelayEnvelope":
        """Build an envelope from a dictionary.

        从字典构建信封对象，只保留 dataclass 中定义的已知字段，
        忽略未知字段（兼容未来协议扩展）。
        """

        known = {field_name for field_name in cls.__dataclass_fields__}
        return cls(**{key: value for key, value in data.items() if key in known})

    def to_dict(self) -> dict[str, Any]:
        """Serialize the envelope to a JSON-friendly dictionary.

        序列化为 JSON 友好的字典格式，用于 MQTT 消息体传输。
        注意：列表和字典字段使用副本，防止外部修改影响内部状态。
        """

        return {
            "protocol_version": self.protocol_version,
            "message_id": self.message_id,
            "conversation_id": self.conversation_id,
            "trace_id": self.trace_id,
            "parent_message_id": self.parent_message_id,
            "from_bot": self.from_bot,
            "from_bot_name": self.from_bot_name,
            "to_bot": self.to_bot,
            "to_bot_name": self.to_bot_name,
            "sender_instance_id": self.sender_instance_id,
            "target_scope": self.target_scope,
            "channel": self.channel,
            "intent": self.intent,
            "expect_reply": self.expect_reply,
            "reply_budget": self.reply_budget,
            "hop": self.hop,
            "ttl": self.ttl,
            "no_relay": self.no_relay,
            "terminal": self.terminal,
            "allowed_responders": list(self.allowed_responders),
            "cooldown_seconds": self.cooldown_seconds,
            "reply_contract": dict(self.reply_contract),
            "state": self.state,
            "phase": self.phase,
            "payload": dict(self.payload),
            "created_at": self.created_at,
        }

    @property
    def text(self) -> str:
        """Return text payload content.

        便捷属性：从 payload 中提取文本内容。
        """

        value = self.payload.get("text", "")
        return value if isinstance(value, str) else str(value)

    def validate(self) -> None:
        """Validate security-critical envelope fields.

        验证安全关键的字段，防止非法信封进入系统：
        - message_id 不能为空（去重需要）
        - from_bot 和 to_bot 不能为空（身份校验需要）
        - hop 不能超过 ttl（防止无限循环）
        - reply_budget 不能为负数

        Raises:
            ValueError: If required identities or control fields are invalid.
        """

        if not self.message_id:
            raise ValueError("message_id is required")
        if not self.from_bot:
            raise ValueError("from_bot is required")
        if not self.to_bot:
            raise ValueError("to_bot is required")
        if self.hop > self.ttl:
            raise ValueError("hop exceeds ttl")
        if self.reply_budget < 0:
            raise ValueError("reply_budget must not be negative")

    def increment_hop(self) -> "RelayEnvelope":
        """Return a copy-like envelope with hop incremented.

        每次消息被接收/处理时，跳数 +1。
        返回新对象（copy-on-write），不修改原信封。
        """

        data = self.to_dict()
        data["hop"] = self.hop + 1
        return RelayEnvelope.from_dict(data)
