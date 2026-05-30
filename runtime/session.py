"""Relay session helpers and Phase 2 transaction state machine."""

# =============================================================================
# SessionManager - 会话管理器
# =============================================================================
# 这是插件的核心业务逻辑层，管理两种通信通道的完整状态机：
#
# 1. Transaction（事务）状态机
#    严格的有限状态机，控制 bot 之间的事务协作流程。
#
#    状态转换图：
#    ┌─────────┐    request/invite    ┌──────────────┐
#    │ created │ ─────────────────→  │ pending_reply │
#    └────┬────┘                     └──────┬───────┘
#         │ notify                          │
#         ↓                                 ├── accept → ┌──────────┐
#       closed                              │            │ accepted │
#                                           │            └────┬─────┘
#                                           │                 ├── confirm → closed
#                                           │                 ├── decline → closed
#                                           │                 ├── cancel  → closed
#                                           │                 └── reschedule → reschedule_requested
#                                           │
#                                           ├── decline → closed
#                                           ├── ack     → closed
#                                           ├── close   → closed
#                                           └── cancel  → closed
#
#    从 reschedule_requested：
#    ┌──────────────────────┐
#    │ reschedule_requested │
#    └──────────┬───────────┘
#               ├── confirm    → closed
#               ├── decline    → closed
#               ├── close      → closed
#               ├── cancel     → closed
#               └── reschedule → reschedule_requested (再次改期)
#
# 2. Social（社交）阶段状态机
#    渐进的社交对话阶段管理，比事务更宽松。
#
#    阶段流转：
#    opening → active → cooling → ending → closed
#    - opening: 初始社交消息
#    - active:  活跃对话中
#    - cooling: 达到 max_turns * 0.7 后进入冷却
#    - ending:  达到 max_turns 或 reply_budget 耗尽
#    - closed:  对话结束
#
# 六项硬校验（validate_transaction_action）：
#   1. 会话是否存在
#   2. 会话是否已关闭（terminal 或 state=closed）
#   3. 当前状态是否允许此操作
#   4. 调用方是否在 allowed_responders 中
#   5. 回复预算是否耗尽
#   6. payload 是否完整
# =============================================================================

from __future__ import annotations

import time
from uuid import uuid4

from mofox_wire import MessageEnvelope

from .envelope import RelayEnvelope
from . import store


class SessionManager:
    """Provide transaction semantics without owning runtime state.

    SessionManager 自身是无状态的，所有数据存储在 store 模块的全局变量中。
    这允许多个 Service 实例共享同一份会话数据。
    """

    # =========================================================================
    # 事务状态转换表
    # =========================================================================
    # key=当前状态, value={intent: 下一状态}
    # closed 状态不接受任何操作（空字典）
    _TRANSITIONS = {
        "created": {
            "notify": "closed",
            "request": "pending_reply",
            "invite": "pending_reply",
        },
        "pending_reply": {
            "accept": "accepted",
            "decline": "closed",
            "reschedule": "reschedule_requested",
            "ack": "closed",
            "close": "closed",
            "cancel": "closed",
        },
        "accepted": {
            "confirm": "closed",
            "decline": "closed",
            "cancel": "closed",
            "reschedule": "reschedule_requested",
        },
        "reschedule_requested": {
            "confirm": "closed",
            "decline": "closed",
            "close": "closed",
            "cancel": "closed",
            "reschedule": "reschedule_requested",  # 可以再次改期
        },
        "closed": {},  # 终态，不接受任何操作
    }

    # 社交的终态阶段
    _SOCIAL_END_PHASES = {"ending", "closed"}

    # =========================================================================
    # 出站信封构建
    # =========================================================================

    def build_outbound_envelope(
        self,
        *,
        message_envelope: MessageEnvelope,
        from_bot: str,
        from_bot_name: str,
        to_bot: str,
        to_bot_name: str,
        default_ttl: int = 4,
        default_reply_budget: int = 3,
    ) -> RelayEnvelope:
        """Build the Phase 1 outbound relay envelope.

        从框架的 MessageEnvelope 构建 relay 协议的 RelayEnvelope。
        根据 relay_context 中的 channel 字段分两路处理：
        - social channel → 使用 build_social_envelope()
        - transaction channel（默认）→ 使用事务逻辑
        """

        text = _extract_text(message_envelope)
        extra = _extract_extra(message_envelope)
        relay_context = extra.get("relay_context") if isinstance(extra, dict) else None
        context = relay_context if isinstance(relay_context, dict) else {}
        channel = str(context.get("channel") or "transaction")

        # ── Social channel 路径 ──
        if channel == "social":
            conversation_id = context.get("conversation_id")
            explicit_conversation_id = conversation_id if isinstance(conversation_id, str) and conversation_id else None
            envelope = self.build_social_envelope(
                from_bot=from_bot,
                from_bot_name=from_bot_name,
                to_bot=to_bot,
                to_bot_name=to_bot_name,
                text=text,
                conversation_id=explicit_conversation_id,
                phase=str(context.get("phase") or "opening"),
                reply_budget=_context_int(context, "reply_budget", default_reply_budget),
                cooldown_seconds=_context_int(context, "cooldown_seconds", 0),
                max_turns=_context_int(context, "max_turns", 6),
            )
            envelope.ttl = default_ttl
            trace_id = context.get("trace_id")
            if isinstance(trace_id, str) and trace_id:
                envelope.trace_id = trace_id
            self.save_social_session_from_envelope(envelope)
            return envelope

        # ── Transaction channel 路径 ──
        # 从上下文中推断意图和会话状态
        inferred_session = self._find_session_for_outbound(
            context=context, message_envelope=message_envelope, to_bot=to_bot
        )
        explicit_intent = context.get("intent")
        inferred_intent = self._infer_intent_from_session(inferred_session)
        intent = str(inferred_intent or explicit_intent or "notify")
        conversation_id = str(
            context.get("conversation_id") or (inferred_session.conversation_id if inferred_session else "")
        )

        # 决定初始会话参数
        expects_initial_reply = intent in {"request", "invite"}
        reply_budget = default_reply_budget if expects_initial_reply else 0
        allowed_responders = [to_bot] if expects_initial_reply else []
        terminal = intent == "notify"
        expect_reply = expects_initial_reply
        state = "pending_reply" if expects_initial_reply else "closed"

        # 如果存在已有会话，使用其状态
        if inferred_session is not None and inferred_intent:
            reply_budget = inferred_session.reply_budget
            allowed_responders = list(inferred_session.allowed_responders)
            state = inferred_session.state or state
            terminal = inferred_session.terminal
            expect_reply = inferred_session.expect_reply

        # 构建信封
        envelope = RelayEnvelope(
            conversation_id=conversation_id or RelayEnvelope().conversation_id,
            from_bot=from_bot,
            from_bot_name=from_bot_name,
            to_bot=to_bot,
            to_bot_name=to_bot_name,
            channel=channel if channel in {"system", "transaction", "social"} else "transaction",
            intent=intent,
            ttl=default_ttl,
            payload={"text": text, "structured": context.get("structured", {})},
            allowed_responders=allowed_responders,
            reply_budget=reply_budget,
            terminal=terminal,
            expect_reply=expect_reply,
            state=state,
        )

        # 如果没有已有会话，设置初始状态
        if inferred_session is None:
            envelope.state = "pending_reply" if expects_initial_reply else envelope.state
            envelope.expect_reply = expects_initial_reply
            envelope.terminal = intent == "notify"
            envelope.reply_budget = default_reply_budget if expects_initial_reply else envelope.reply_budget
            envelope.allowed_responders = [to_bot] if expects_initial_reply else envelope.allowed_responders

        # ── 保存会话状态 ──
        store.save_session(
            store.RelaySession(
                conversation_id=envelope.conversation_id,
                peer_bot_id=to_bot,
                channel=envelope.channel,
                intent=envelope.intent,
                state=envelope.state,
                terminal=envelope.terminal,
                expect_reply=envelope.expect_reply,
                reply_budget=envelope.reply_budget,
                allowed_responders=list(envelope.allowed_responders),
            )
        )

        # ── 保存事务记录 ──
        existing_record = store.TRANSACTION_LOG.get(envelope.conversation_id)
        if existing_record is None or envelope.intent in {"request", "invite", "notify"}:
            # 新建或重置事务记录
            store.save_transaction_record(
                store.RelayTransactionRecord(
                    conversation_id=envelope.conversation_id,
                    trace_id=envelope.trace_id,
                    from_bot=from_bot,
                    to_bot=to_bot,
                    current_state=envelope.state or "",
                    final_intent=envelope.intent if envelope.terminal else None,
                    topic=text,
                    summary=text,
                )
            )
        else:
            # 更新已有记录
            existing_record.current_state = envelope.state or existing_record.current_state
            existing_record.final_intent = envelope.intent if envelope.terminal else existing_record.final_intent
            store.save_transaction_record(existing_record)

        return envelope

    # =========================================================================
    # relay_context 构建
    # =========================================================================

    def relay_context_from_envelope(self, envelope: RelayEnvelope) -> dict[str, object]:
        """Build Message.extra relay_context from an envelope.

        从 RelayEnvelope 构建 relay_context，注入到 Message.extra 中，
        供 Chatter 和其他组件使用。
        """

        return {
            "conversation_id": envelope.conversation_id,
            "trace_id": envelope.trace_id,
            "channel": envelope.channel,
            "intent": envelope.intent,
            "peer_bot_id": envelope.from_bot,
            "peer_bot_name": envelope.from_bot_name,
            "state": envelope.state,
            "phase": envelope.phase,
            "terminal": envelope.terminal,
            "expect_reply": envelope.expect_reply,
            "reply_budget": envelope.reply_budget,
            "allowed_responders": list(envelope.allowed_responders),
        }

    # =========================================================================
    # 入站事务会话同步
    # =========================================================================

    def sync_inbound_transaction_session(self, envelope: RelayEnvelope) -> store.RelaySession | None:
        """Persist inbound transaction state from a validated relay envelope.

        处理入站事务消息，推进本地会话状态机。
        根据入站 intent 和当前会话状态，决定下一个状态。

        返回更新后的会话对象（或 None，如果不是事务消息）。
        """

        if envelope.channel != "transaction":
            return None
        if envelope.intent not in self._transaction_intents():
            return None

        # ── 获取当前会话状态 ──
        existing = store.get_session(envelope.conversation_id)
        current_state = existing.state if existing is not None and existing.state else "created"

        # ── 查找状态转换 ──
        next_state = self._TRANSITIONS.get(current_state, {}).get(envelope.intent)
        if next_state is None:
            return existing  # 不允许的转换 → 保持原状态

        # ── 计算新的会话参数 ──
        state = next_state
        terminal = state == "closed"
        previous_budget = existing.reply_budget if existing is not None else envelope.reply_budget
        reply_budget = 0 if terminal else max(0, int(previous_budget) - 1)

        # 推导允许的回复者
        allowed_responders = self._derive_inbound_allowed_responders(
            state=state,
            terminal=terminal,
            local_bot_id=envelope.to_bot,
        )
        expect_reply = False if terminal else bool(allowed_responders and reply_budget > 0)

        # ── 更新会话 ──
        session = store.RelaySession(
            conversation_id=envelope.conversation_id,
            peer_bot_id=envelope.from_bot,
            channel=envelope.channel,
            intent=envelope.intent,
            state=state,
            terminal=terminal,
            expect_reply=expect_reply,
            reply_budget=reply_budget,
            allowed_responders=allowed_responders,
            phase=envelope.phase,
        )
        store.save_session(session)

        # ── 更新事务日志 ──
        existing_record = store.TRANSACTION_LOG.get(envelope.conversation_id)
        store.save_transaction_record(
            store.RelayTransactionRecord(
                conversation_id=envelope.conversation_id,
                trace_id=envelope.trace_id,
                from_bot=envelope.from_bot,
                to_bot=envelope.to_bot,
                current_state=state or "",
                final_intent=envelope.intent if terminal else None,
                topic=existing_record.topic if existing_record is not None else envelope.text,
                summary=existing_record.summary if existing_record is not None else envelope.text,
            )
        )
        return session

    @staticmethod
    def _derive_inbound_allowed_responders(
        *,
        state: str,
        terminal: bool,
        local_bot_id: str,
    ) -> list[str]:
        """Derive trusted inbound responders from local state only.

        从本地状态推导允许回复的 bot 列表，不信任入站信封中的 allowed_responders。
        安全原则：只信任本地状态。

        规则：
        - 终态：无人可回复
        - pending_reply/accepted/reschedule_requested：只有本地 bot 可回复
        - 其他状态：无人可回复
        """

        if terminal:
            return []
        if state in {"pending_reply", "accepted", "reschedule_requested"} and local_bot_id:
            return [local_bot_id]
        return []

    # =========================================================================
    # 入站 confirm → Todo 投影
    # =========================================================================

    async def publish_inbound_final_todo_decision(
        self,
        *,
        envelope: RelayEnvelope,
        local_bot_id: str,
        config: object,
    ) -> tuple[bool, str, dict[str, object]] | None:
        """Publish a local todo projection after receiving a final confirm.

        当收到对端的 confirm 消息时（事务最终确认），
        在本地也创建 Todo 投影。
        这是因为对端确认的事务，本端也需要记录为待办。

        Returns:
            (ok, status, result) 或 None（不需要创建 Todo）。
        """

        if envelope.channel != "transaction" or envelope.intent != "confirm":
            return None

        record = store.TRANSACTION_LOG.get(envelope.conversation_id)
        if record is None:
            return None

        session = store.get_session(envelope.conversation_id)
        if session is None or not session.terminal:
            return None

        from .todo_bridge import TodoBridge
        from ..components.config import BotPrivateRelayConfig

        if not isinstance(config, BotPrivateRelayConfig):
            return None

        result = await TodoBridge(config).publish_final_decision(
            record=record,
            final_intent="confirm",
            owner_bot=local_bot_id,
            peer_bot_id=envelope.from_bot,
        )
        return result

    # =========================================================================
    # 入站社交会话同步
    # =========================================================================

    def sync_inbound_social_session(self, envelope: RelayEnvelope) -> store.RelaySession | None:
        """Persist inbound social state before the local bot replies.

        处理入站社交消息，更新本地社交会话状态。
        """

        if envelope.channel != "social":
            return None

        existing = store.get_session(envelope.conversation_id)
        phase = envelope.phase or (existing.phase if existing is not None else "active")
        terminal = envelope.terminal or phase in self._SOCIAL_END_PHASES
        reply_budget = 0 if terminal else envelope.reply_budget
        allowed_responders = [] if terminal or reply_budget <= 0 else list(envelope.allowed_responders)
        expect_reply = (
            False
            if terminal or reply_budget <= 0 or not allowed_responders
            else envelope.expect_reply
        )

        session = store.RelaySession(
            conversation_id=envelope.conversation_id,
            peer_bot_id=envelope.from_bot,
            channel="social",
            intent=envelope.intent,
            state=None,
            terminal=terminal,
            expect_reply=expect_reply,
            reply_budget=reply_budget,
            allowed_responders=allowed_responders,
            phase=phase,
            turn_count=existing.turn_count if existing is not None else 0,
            max_turns=existing.max_turns if existing is not None else 6,
            cooldown_seconds=envelope.cooldown_seconds,
            cooldown_until=existing.cooldown_until if existing is not None else 0.0,
        )
        store.save_session(session)
        return session

    # =========================================================================
    # 社交信封构建
    # =========================================================================

    def build_social_envelope(
        self,
        *,
        from_bot: str,
        from_bot_name: str,
        to_bot: str,
        to_bot_name: str,
        text: str,
        conversation_id: str | None = None,
        phase: str = "opening",
        reply_budget: int = 3,
        cooldown_seconds: int = 0,
        max_turns: int = 6,
    ) -> RelayEnvelope:
        """Build a social-channel envelope with Phase 3 state machine controls.

        构建社交通道的信封。如果与目标 bot 已存在社交会话，
        会推进会话阶段和轮次。

        社交阶段流转：
        opening → active (第一轮后)
        active → cooling (达到 max_turns * 70%)
        cooling → ending (达到 max_turns 或 budget 耗尽)
        ending/closed → 对话结束
        """

        # ── 查找已有社交会话 ──
        existing = self._find_social_session(to_bot, conversation_id=conversation_id)
        if existing is not None and not existing.terminal:
            # 活会话：推进轮次和阶段
            existing = self.advance_social_turn(
                session=existing, max_turns=max_turns, cooldown_seconds=cooldown_seconds
            )
            phase = existing.phase or phase
            reply_budget = existing.reply_budget
            cooldown_seconds = existing.cooldown_seconds
        elif existing is not None:
            # 已结束的会话
            phase = existing.phase or "closed"
            reply_budget = 0
        else:
            # 首次社交消息 → 从 opening 推进到 active
            if phase == "opening":
                phase = "active"

        terminal = phase in ("ending", "closed") or reply_budget <= 0
        allowed_responders = [to_bot] if not terminal else []

        envelope = RelayEnvelope(
            from_bot=from_bot,
            from_bot_name=from_bot_name,
            to_bot=to_bot,
            to_bot_name=to_bot_name,
            channel="social",
            intent="say",
            payload={"text": text},
            phase=phase,
            reply_budget=reply_budget,
            cooldown_seconds=cooldown_seconds,
            allowed_responders=allowed_responders,
            terminal=terminal,
            expect_reply=not terminal,
            state=None,
        )

        # 继承已有会话的 conversation_id
        if existing is not None:
            envelope.conversation_id = existing.conversation_id
        elif conversation_id is not None:
            envelope.conversation_id = conversation_id

        return self.apply_expect_reply_overrides(envelope)

    def _find_social_session(
        self,
        peer_bot_id: str,
        conversation_id: str | None = None,
    ) -> store.RelaySession | None:
        """Return the stored social session for a peer bot.

        查找与指定 bot 的社交会话。
        优先按 conversation_id 精确查找，否则找最新活跃会话。
        """

        if conversation_id is not None:
            session = store.get_session(conversation_id)
            if session is not None and session.peer_bot_id == peer_bot_id and session.channel == "social":
                return session
            return None

        # 按 peer_bot_id 查找
        candidates = [
            session
            for session in store.SESSION_TABLE.values()
            if session.peer_bot_id == peer_bot_id and session.channel == "social"
        ]
        # 优先返回活跃会话
        active = [
            session
            for session in candidates
            if not session.terminal and session.phase not in self._SOCIAL_END_PHASES
        ]
        pool = active or candidates
        return max(pool, key=lambda session: session.updated_at) if pool else None

    def apply_expect_reply_overrides(self, envelope: RelayEnvelope) -> RelayEnvelope:
        """Apply the frozen Phase 3 expect_reply override priority.

        确定是否期待回复的优先级规则：
        1. terminal=True → expect_reply=False（最高优先级）
        2. reply_budget <= 0 → expect_reply=False
        3. allowed_responders 为空 → expect_reply=False
        4. phase 为 ending/closed → expect_reply=False
        5. 否则 → expect_reply=True
        """

        if envelope.terminal is True:
            envelope.expect_reply = False
            return envelope
        if envelope.reply_budget <= 0:
            envelope.expect_reply = False
            return envelope
        if not envelope.allowed_responders:
            envelope.expect_reply = False
            return envelope
        if envelope.phase in self._SOCIAL_END_PHASES:
            envelope.expect_reply = False
            return envelope
        envelope.expect_reply = True
        return envelope

    def save_social_session_from_envelope(self, envelope: RelayEnvelope) -> store.RelaySession:
        """Persist minimal social-session state into the shared store."""

        existing = store.get_session(envelope.conversation_id)
        session = store.RelaySession(
            conversation_id=envelope.conversation_id,
            peer_bot_id=envelope.to_bot,
            channel="social",
            intent=envelope.intent,
            state=None,
            terminal=envelope.terminal,
            expect_reply=envelope.expect_reply,
            reply_budget=envelope.reply_budget,
            allowed_responders=list(envelope.allowed_responders),
            phase=envelope.phase,
            turn_count=existing.turn_count if existing is not None else 0,
            max_turns=existing.max_turns if existing is not None else 6,
            cooldown_seconds=envelope.cooldown_seconds,
            cooldown_until=existing.cooldown_until if existing is not None else 0.0,
        )
        store.save_session(session)
        return session

    # =========================================================================
    # 记忆候选投影
    # =========================================================================

    def maybe_create_memory_candidate(self, *, envelope: RelayEnvelope) -> None:
        """Project high-value relay messages into memory candidates.

        筛选规则：
        - 只处理 social 和 transaction channel
        - 消息长度 >= 12 字符
        - 根据消息长度计算 score（最长 100 字符得 1.0 分）
        - score >= 0.2 才保存
        """

        text = envelope.text.strip()
        if envelope.channel not in {"social", "transaction"}:
            return
        if len(text) < 12:
            return
        score = min(1.0, len(text) / 100)
        if score < 0.2:
            return
        store.save_memory_candidate(
            store.RelayMemoryCandidate(
                candidate_id=uuid4().hex,
                conversation_id=envelope.conversation_id,
                peer_bot_id=envelope.to_bot,
                channel=envelope.channel,
                content=text,
                score=score,
            )
        )

    # =========================================================================
    # 事务 Tool 校验与执行
    # =========================================================================

    def validate_transaction_action(
        self, *, conversation_id: str, action: str, caller_bot: str, payload_complete: bool = True
    ) -> tuple[bool, str, store.RelaySession | None]:
        """Run the six hard checks for a transaction tool.

        六项硬校验（用于事务 Tool 执行前验证）：
        1. 会话是否存在 → invalid_payload
        2. 会话是否已关闭 → conversation_closed
        3. 当前状态是否允许此操作 → state_not_allowed
        4. 调用方是否在 allowed_responders → not_allowed_responder
        5. 回复预算是否耗尽 → reply_budget_exhausted
        6. payload 是否完整 → invalid_payload

        Returns:
            (是否通过, 状态码, 会话对象)
        """

        session = store.get_session(conversation_id)
        if session is None:
            return False, "invalid_payload", None

        state = session.state or "created"
        if session.terminal or state == "closed":
            return False, "conversation_closed", session
        if action not in self._TRANSITIONS.get(state, {}):
            return False, "state_not_allowed", session
        if caller_bot not in session.allowed_responders:
            return False, "not_allowed_responder", session
        if session.reply_budget <= 0:
            return False, "reply_budget_exhausted", session
        if not payload_complete:
            return False, "invalid_payload", session

        return True, "ok", session

    def apply_transaction_action(
        self,
        *,
        conversation_id: str,
        action: str,
        caller_bot: str,
    ) -> store.RelaySession:
        """Advance session after a validated tool action.

        在通过六项硬校验后，推进会话状态。
        - 更新 state
        - 减少 reply_budget
        - 设置 terminal/expect_reply/allowed_responders
        - 更新事务日志
        """

        session = store.get_session(conversation_id)
        if session is None:
            raise ValueError("conversation_not_found")

        current_state = session.state or "created"
        next_state = self._TRANSITIONS[current_state][action]
        terminal = next_state == "closed" or action in {"confirm", "decline", "cancel", "ack", "close"}

        session.state = next_state
        session.intent = action
        session.reply_budget = 0 if terminal else max(0, session.reply_budget - 1)
        session.terminal = terminal

        if terminal:
            session.expect_reply = False
            session.allowed_responders = []
        elif action in {"accept", "reschedule"}:
            # accept/reschedule 后将回复权交给对端
            session.expect_reply = True
            session.allowed_responders = [session.peer_bot_id]

        store.save_session(session)

        # ── 更新事务日志 ──
        record = store.TRANSACTION_LOG.get(conversation_id)
        if record is not None:
            record.current_state = next_state
            record.final_intent = action if session.terminal else record.final_intent
            store.save_transaction_record(record)

        return session

    # =========================================================================
    # 出站会话查找
    # =========================================================================

    def _find_session_for_outbound(
        self, *, context: dict[str, object], message_envelope: MessageEnvelope, to_bot: str
    ) -> store.RelaySession | None:
        """Find an outbound session by explicit conversation id or peer bot id.

        查找顺序：
        1. context 中有明确 conversation_id → 直接查找
        2. 按 peer_bot_id + channel=transaction + 非 closed 状态查找
        3. 按 MessageEnvelope 中的 user_id 查找
        """

        conversation_id = context.get("conversation_id")
        if isinstance(conversation_id, str) and conversation_id:
            return store.get_session(conversation_id)

        for session in store.SESSION_TABLE.values():
            if session.peer_bot_id == to_bot and session.channel == "transaction" and (session.state or "") not in {"closed"}:
                return session

        user_info = (message_envelope.get("message_info") or {}).get("user_info", {})
        user_id = user_info.get("user_id") if isinstance(user_info, dict) else None
        if isinstance(user_id, str):
            for session in store.SESSION_TABLE.values():
                if session.peer_bot_id == user_id and session.channel == "transaction" and (session.state or "") not in {"closed"}:
                    return session

        return None

    @staticmethod
    def _infer_intent_from_session(session: store.RelaySession | None) -> str | None:
        """Infer outbound intent from current transaction session state.

        从事务会话的当前状态推断出站意图：
        - accepted → "accept"（转发已接受状态）
        - reschedule_requested → "reschedule"（转发改期状态）
        - closed → 使用会话记录的 intent
        """

        if session is None:
            return None
        state = session.state or ""
        if state == "accepted":
            return "accept"
        if state == "reschedule_requested":
            return "reschedule"
        if state == "closed":
            if session.intent in {"notify", "confirm", "decline", "cancel", "ack", "close"}:
                return session.intent
            return "close"
        return None

    @classmethod
    def _transaction_intents(cls) -> set[str]:
        """Return known transaction intents from the transition table."""

        intents = set(cls._TRANSITIONS["created"])
        for transitions in cls._TRANSITIONS.values():
            intents.update(transitions)
        return intents

    def _state_for_inbound_intent(self, intent: str, existing: store.RelaySession | None) -> str:
        """Infer inbound state when the envelope did not carry one."""

        current = existing.state if existing is not None and existing.state else "created"
        next_state = self._TRANSITIONS.get(current, {}).get(intent)
        if next_state is not None:
            return next_state
        return self._TRANSITIONS["created"].get(intent, existing.state if existing is not None else "closed")

    # =========================================================================
    # 社交阶段状态机
    # =========================================================================

    _SOCIAL_PHASE_ORDER = ("opening", "active", "cooling", "ending", "closed")

    @staticmethod
    def _next_social_phase(current: str) -> str:
        """Return the next social phase in the ordered chain."""

        try:
            idx = SessionManager._SOCIAL_PHASE_ORDER.index(current)
            if idx + 1 < len(SessionManager._SOCIAL_PHASE_ORDER):
                return SessionManager._SOCIAL_PHASE_ORDER[idx + 1]
        except ValueError:
            pass
        return "closed"

    def advance_social_turn(
        self, *, session: store.RelaySession, max_turns: int = 6, cooldown_seconds: int = 0
    ) -> store.RelaySession:
        """Increment turn count and advance social phase when thresholds met.

        推进社交会话的轮次和阶段。

        阶段阈值：
        - opening → active：第一轮后
        - active → cooling：turn_count >= max_turns * 0.7
        - cooling → ending：turn_count >= max_turns 或 reply_budget 耗尽
        - ending/closed：终态

        每轮（每次出入站）turn_count +1，reply_budget -1。
        """

        session.turn_count += 1
        session.max_turns = max_turns
        session.cooldown_seconds = cooldown_seconds

        phase = session.phase or "opening"
        turns = session.turn_count

        # ── 阶段推进 ──
        if phase == "opening" and turns >= 1:
            phase = "active"
        if phase == "active" and turns >= int(max_turns * 0.7):
            phase = "cooling"
        if phase == "cooling" and turns >= max_turns:
            phase = "ending"

        # ── 预算递减 ──
        session.reply_budget = max(0, session.reply_budget - 1)
        if session.reply_budget <= 0:
            phase = "ending" if phase != "closed" else phase

        # ── 终态设置 ──
        if phase in ("ending", "closed"):
            session.terminal = True
            session.expect_reply = False
            session.reply_budget = 0
            session.allowed_responders = []
        else:
            session.terminal = False
            session.expect_reply = bool(session.allowed_responders)

        # ── 冷却窗口 ──
        if cooldown_seconds > 0 and phase == "cooling":
            session.cooldown_until = time.time() + cooldown_seconds

        session.phase = phase
        store.save_session(session)
        return session

    def is_social_in_cooldown(self, session: store.RelaySession) -> bool:
        """Return True if the session is in an active cooldown window."""

        if session.channel != "social":
            return False
        if session.cooldown_until > time.time():
            return True
        return False

    def force_social_ending(self, session: store.RelaySession) -> store.RelaySession:
        """Immediately escalate a social session to ending."""

        session.phase = "ending"
        session.terminal = True
        session.expect_reply = False
        session.reply_budget = 0
        store.save_session(session)
        return session


# =============================================================================
# 内部辅助函数
# =============================================================================

def _extract_text(message_envelope: MessageEnvelope) -> str:
    """从 MessageEnvelope 中提取文本内容。"""
    segments = message_envelope.get("message_segment") or []
    if isinstance(segments, dict):
        segments = [segments]
    text_parts: list[str] = []
    for segment in segments:
        if isinstance(segment, dict) and segment.get("type") == "text":
            text_parts.append(str(segment.get("data", "")))
    return "".join(text_parts)


def _extract_extra(message_envelope: MessageEnvelope) -> dict[str, object]:
    """从 MessageEnvelope 中提取 extra 字典。"""
    message_info = message_envelope.get("message_info") or {}
    extra = message_info.get("extra") if isinstance(message_info, dict) else None
    return extra if isinstance(extra, dict) else {}


def _context_int(context: dict[str, object], key: str, default: int) -> int:
    """Return a non-negative integer from relay context.

    从 relay_context 中安全提取整数，处理 None/非整数/负数等边界情况。
    """

    value = context.get(key)
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default
