"""Policy rules for Phase 1 relay control fields."""

# =============================================================================
# PolicyEngine - 策略引擎
# =============================================================================
# 对出站消息信封应用确定性的控制字段规则。
# 这些策略确保消息的协议控制字段（terminal, expect_reply, reply_budget,
# allowed_responders）始终符合协议约定，不受 LLM 或调用方错误影响。
#
# 核心规则：
# 1. transaction.notify → 单向通知，terminal=True，不期待回复
# 2. transaction.request → 事务请求，terminal=False，期待回复，默认 budget=3
# 3. terminal=True → expect_reply 强制为 False
# 4. 无 allowed_responders 且非 notify → expect_reply=False
# =============================================================================

from __future__ import annotations

from .envelope import RelayEnvelope


class PolicyEngine:
    """Apply deterministic Phase 1 control-field rules."""

    def apply_outbound(self, envelope: RelayEnvelope) -> RelayEnvelope:
        """Apply terminal and reply-budget policy to an outbound envelope.

        对出站信封应用策略，确保协议控制字段一致性。
        这是确定性的、硬编码的规则，不依赖 LLM 判断。

        规则详解：
        - notify 意图：单向通知，不需要对方回复 → terminal=True, expect_reply=False, reply_budget=0
        - request 意图：事务请求，需要对方回复 → terminal=False, expect_reply=True, reply_budget=3（默认）
        - 如果 terminal=True，强制 expect_reply=False
        - 如果没有 allowed_responders 且意图不是 notify，强制 expect_reply=False
        """

        if envelope.channel == "transaction" and envelope.intent == "notify":
            # 单向通知：不期待回复，消息即终态
            envelope.terminal = True
            envelope.expect_reply = False
            envelope.reply_budget = 0
            envelope.allowed_responders = []
            envelope.state = envelope.state or "closed"

        elif envelope.channel == "transaction" and envelope.intent == "request":
            # 事务请求：期待回复，进入 pending_reply 状态
            envelope.terminal = False
            envelope.expect_reply = True
            envelope.state = envelope.state or "pending_reply"
            if envelope.reply_budget <= 0:
                envelope.reply_budget = 3  # 默认给 3 轮回复预算

        # 终态消息不期待回复
        if envelope.terminal:
            envelope.expect_reply = False

        # 没有允许的回复者时，不期待回复
        if not envelope.allowed_responders and envelope.intent != "notify":
            envelope.expect_reply = False

        return envelope

    def should_auto_reply(self, relay_context: dict[str, object] | None) -> bool:
        """Return whether a relay message may trigger bot auto-reply.

        判断一条中继消息是否应该触发 bot 自动回复。
        两个条件：
        1. relay_context 存在
        2. 不是终态（terminal != True）
        3. expect_reply 为 True
        """

        if not relay_context:
            return False
        if relay_context.get("terminal") is True:
            return False
        return bool(relay_context.get("expect_reply", False))
