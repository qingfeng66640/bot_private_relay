"""bot private relay 插件的模块级运行时状态。

在 Neo-MoFox 中，Service 实例不是单例的，因此所有可变运行时状态都集中在此处管理。
"""

# =============================================================================
# 模块级运行时状态存储
# =============================================================================
# 在 Neo-MoFox 架构中，Service 实例不是单例的，可能被多次创建。
# 因此所有可变运行时状态必须集中存储在此模块的全局变量中。
#
# 数据结构一览：
# ──────────────────────────────────────────────────────────────────────────
# 数据类（dataclass）：
#   PresenceRecord          - 伙伴 bot 在线状态记录
#   RelaySession            - relay 会话状态（事务/社交共用）
#   RelayTransactionRecord  - 事务日志记录
#   RelayTodoItem           - 投影的待办事项
#   RelayScheduleItem       - 投影的日程安排
#   RelayMemoryCandidate    - 记忆候选
#   ProactiveChatHint       - proactive 决策的聊天线索
#
# 全局字典/列表（运行时状态）：
#   DEDUP_CACHE             - 消息去重缓存 {message_id: received_at}
#   PRESENCE_TABLE          - 在线状态表 {bot_id: PresenceRecord}
#   SESSION_TABLE           - 会话表 {conversation_id: RelaySession}
#   AUDIT_LOG               - 审计日志列表
#   TRANSACTION_LOG         - 事务日志 {conversation_id: RelayTransactionRecord}
#   RELAY_TODOS             - 待办投影 {todo_id: RelayTodoItem}
#   RELAY_SCHEDULES         - 日程投影 {schedule_id: RelayScheduleItem}
#   RELAY_MEMORY_CANDIDATES - 记忆候选 {candidate_id: RelayMemoryCandidate}
#   PROACTIVE_CHAT_HINTS    - proactive 聊天线索列表
#   DYNAMIC_SOCIAL_DAILY_COUNTS    - 动态社交每日计数器 {(bot_id, day): count}
#   DYNAMIC_SOCIAL_HOURLY_COUNTS   - 动态社交每小时计数器 {(bot_id, hour): count}
#   DYNAMIC_SOCIAL_COOLDOWNS       - 动态社交冷却 {bot_id: cooldown_until}
#   PROACTIVE_COOLDOWNS            - proactive 冷却 {bot_id: cooldown_until}
#   PROACTIVE_HOURLY_COUNTS        - proactive 每小时计数器 {(action, bot_id, hour): count}
#
# 所有状态是运行时内存数据，插件重启后清空。未来可以接入持久化层。
# =============================================================================

from __future__ import annotations

import time
from dataclasses import dataclass, field


# =============================================================================
# 数据类定义
# =============================================================================

@dataclass(slots=True)
class PresenceRecord:
    """伙伴 bot 的在线状态。

    记录一个伙伴 bot 的在线状态信息。
    """

    bot_id: str
    bot_name: str = ""
    status: str = "offline"              # online / offline / unknown
    last_seen: float = field(default_factory=time.time)
    is_known_partner: bool = False       # 是否在配置的已知伙伴列表中


@dataclass(slots=True)
class RelaySession:
    """Phase 1 的最简 relay 会话状态。

    中继会话状态，同时用于 transaction（事务）和 social（社交）两种 channel。
    通过 channel 字段区分。
    """

    conversation_id: str                  # 会话唯一 ID
    peer_bot_id: str                      # 对端 bot ID
    channel: str                          # 通道类型：transaction / social
    intent: str                           # 当前意图
    state: str | None = None              # 事务状态：pending_reply / accepted / closed 等
    terminal: bool = False                # 是否为会话终点
    expect_reply: bool = False            # 是否期待回复
    reply_budget: int = 0                 # 剩余回复配额
    allowed_responders: list[str] = field(default_factory=list)  # 允许回复的 bot ID 列表
    phase: str | None = None              # 社交阶段：opening / active / cooling / ending / closed
    turn_count: int = 0                   # 社交轮次计数
    max_turns: int = 6                    # 社交最大轮次
    cooldown_seconds: int = 0             # 社交冷却秒数
    cooldown_until: float = 0.0           # 社交冷却截止时间
    updated_at: float = field(default_factory=time.time)  # 最后更新时间


@dataclass(slots=True)
class RelayTransactionRecord:
    """Phase 2 的事务记录。

    事务日志记录，追踪一个事务从创建到终结的完整生命周期。
    """

    conversation_id: str
    trace_id: str
    from_bot: str                         # 发起方
    to_bot: str                           # 接收方
    current_state: str                    # 当前状态
    final_intent: str | None = None       # 最终意图（终态时设置）
    topic: str = ""                       # 事务主题
    summary: str = ""                     # 事务摘要


@dataclass(slots=True)
class RelayTodoItem:
    """Phase 2 的轻量级待办事项投影。

    从事务确认中投影出的待办事项。供 todo_plugin 消费。
    """

    todo_id: str
    owner_bot: str                        # 待办拥有者
    title: str
    status: str = "open"


@dataclass(slots=True)
class RelayScheduleItem:
    """Phase 2 的轻量级日程投影。

    从事务中投影出的日程安排。
    """

    schedule_id: str
    organizer_bot: str                    # 组织者
    participant_bots: list[str] = field(default_factory=list)  # 参与者列表
    title: str = ""
    status: str = "proposed"


@dataclass(slots=True)
class RelayMemoryCandidate:
    """Phase 3 的记忆候选投影。

    从 relay 对话中投影出的记忆候选。高价值消息被标记为候选，
    供长期记忆系统消费。
    """

    candidate_id: str
    conversation_id: str
    peer_bot_id: str
    channel: str
    content: str
    score: float = 0.0                    # 记忆价值评分（0~1）


@dataclass(slots=True)
class ProactiveChatHint:
    """供 proactive 决策使用的近期非 relay 聊天消息。

    普通聊天消息的快照，供 proactive 决策 LLM 分析是否有合适的
    时机主动联系伙伴 bot。
    """

    message_id: str
    platform: str
    chat_type: str
    stream_id: str
    sender_id: str
    sender_name: str
    text: str
    received_at: float = field(default_factory=time.time)


# =============================================================================
# 全局运行时状态（所有可变状态集中于此）
# =============================================================================

DEDUP_CACHE: dict[str, float] = {}                          # 消息去重缓存
PRESENCE_TABLE: dict[str, PresenceRecord] = {}               # 在线状态表
SESSION_TABLE: dict[str, RelaySession] = {}                  # 会话表
AUDIT_LOG: list[dict[str, object]] = []                      # 审计日志
TRANSACTION_LOG: dict[str, RelayTransactionRecord] = {}      # 事务日志
RELAY_TODOS: dict[str, RelayTodoItem] = {}                   # 待办投影
RELAY_SCHEDULES: dict[str, RelayScheduleItem] = {}           # 日程投影
RELAY_MEMORY_CANDIDATES: dict[str, RelayMemoryCandidate] = {}  # 记忆候选
PROACTIVE_CHAT_HINTS: list[ProactiveChatHint] = []            # proactive 聊天线索
DYNAMIC_SOCIAL_DAILY_COUNTS: dict[tuple[str, str], int] = {}  # 动态社交每日计数
DYNAMIC_SOCIAL_HOURLY_COUNTS: dict[tuple[str, str], int] = {} # 动态社交每小时计数
DYNAMIC_SOCIAL_COOLDOWNS: dict[str, float] = {}               # 动态社交冷却
PROACTIVE_COOLDOWNS: dict[str, float] = {}                    # proactive 冷却
PROACTIVE_HOURLY_COUNTS: dict[tuple[str, str, str], int] = {} # proactive 每小时计数


# =============================================================================
# 状态操作函数
# =============================================================================

def reset_state() -> None:
    """清空所有模块级状态，供插件本地测试使用。

    清空所有运行时状态。主要用于测试环境。
    """

    DEDUP_CACHE.clear()
    PRESENCE_TABLE.clear()
    SESSION_TABLE.clear()
    AUDIT_LOG.clear()
    TRANSACTION_LOG.clear()
    RELAY_TODOS.clear()
    RELAY_SCHEDULES.clear()
    RELAY_MEMORY_CANDIDATES.clear()
    PROACTIVE_CHAT_HINTS.clear()
    DYNAMIC_SOCIAL_DAILY_COUNTS.clear()
    DYNAMIC_SOCIAL_HOURLY_COUNTS.clear()
    DYNAMIC_SOCIAL_COOLDOWNS.clear()
    PROACTIVE_COOLDOWNS.clear()
    PROACTIVE_HOURLY_COUNTS.clear()


def remember_message(message_id: str, ttl_seconds: int = 3600) -> bool:
    """如果消息 ID 近期未出现过，则记录它。

    消息去重逻辑：记录已处理的消息 ID，如果短期内再次收到同一消息，
    返回 False 表示重复。

    使用 TTL（默认 3600 秒 = 1 小时）自动过期旧的去重记录，
    防止缓存无限增长。

    Args:
        message_id: relay 消息 ID。
        ttl_seconds: 去重记录的过期窗口。

    Returns:
        如果这是一条新消息返回 ``True``，否则返回 ``False``。
    """

    now = time.time()

    # ── 清理过期的去重记录 ──
    expired = [key for key, seen_at in DEDUP_CACHE.items() if now - seen_at > ttl_seconds]
    for key in expired:
        DEDUP_CACHE.pop(key, None)

    # ── 检查是否已存在 ──
    if message_id in DEDUP_CACHE:
        return False

    DEDUP_CACHE[message_id] = now
    return True


def upsert_presence(record: PresenceRecord) -> None:
    """存储在线状态。

    更新或插入伙伴 bot 的在线状态记录。
    """

    PRESENCE_TABLE[record.bot_id] = record


def save_session(session: RelaySession) -> None:
    """存储 relay 会话状态。

    保存 relay 会话状态，自动更新 updated_at 时间戳。
    """

    session.updated_at = time.time()
    SESSION_TABLE[session.conversation_id] = session


def get_session(conversation_id: str) -> RelaySession | None:
    """根据会话 ID 返回 relay 会话状态。"""

    return SESSION_TABLE.get(conversation_id)


def audit(event: str, **data: object) -> None:
    """追加一条轻量级审计日志条目。

    追加一条审计日志。审计日志用于调试和追踪插件行为。
    每条日志自动附加 event、time 和调用方提供的自定义字段。
    """

    AUDIT_LOG.append({"event": event, "time": time.time(), **data})


def save_transaction_record(record: RelayTransactionRecord) -> None:
    """持久化事务日志条目。"""

    TRANSACTION_LOG[record.conversation_id] = record


def save_todo(todo: RelayTodoItem) -> None:
    """持久化投影的待办事项。"""

    RELAY_TODOS[todo.todo_id] = todo


def save_schedule(item: RelayScheduleItem) -> None:
    """持久化投影的日程条目。"""

    RELAY_SCHEDULES[item.schedule_id] = item


def save_memory_candidate(candidate: RelayMemoryCandidate) -> None:
    """持久化投影的记忆候选。"""

    RELAY_MEMORY_CANDIDATES[candidate.candidate_id] = candidate


def save_proactive_chat_hint(hint: ProactiveChatHint, *, max_items: int = 60, ttl_seconds: int = 3600) -> None:
    """将近期普通聊天线索保存供 proactive 决策使用。

    保存一条普通聊天消息作为 proactive 决策的上下文线索。
    自动处理：
    - TTL 过期清理（默认 3600 秒 = 1 小时）
    - 同 message_id 去重
    - 最多保留 max_items 条（默认 60 条）

    Args:
        hint: 聊天线索记录。
        max_items: 最大保留条数。
        ttl_seconds: 线索过期时间。
    """

    now = time.time()

    # ── 清理过期线索和同 ID 重复 ──
    PROACTIVE_CHAT_HINTS[:] = [
        item for item in PROACTIVE_CHAT_HINTS
        if now - item.received_at <= ttl_seconds and item.message_id != hint.message_id
    ]

    # ── 追加新线索 ──
    PROACTIVE_CHAT_HINTS.append(hint)

    # ── 限制最大条数 ──
    if len(PROACTIVE_CHAT_HINTS) > max_items:
        del PROACTIVE_CHAT_HINTS[:-max_items]
