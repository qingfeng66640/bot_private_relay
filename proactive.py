"""Autonomous proactive relay initiation."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

try:
    from json_repair import loads as repair_json_loads
except Exception:  # pragma: no cover
    repair_json_loads = json.loads

from src.app.plugin_system.api.llm_api import create_llm_request, get_model_set_by_task
from src.core.config import get_core_config
from src.core.models.message import Message, MessageType
from src.core.models.stream import ChatStream
from src.core.transport.message_send import get_message_sender
from src.kernel.llm import LLMPayload, ROLE, Text
from src.kernel.logger import get_logger

from . import store
from .config import BotPrivateRelayConfig, PartnerSection
from .proactive_utils import cap_field, compact_audit_value, fit_snapshot_to_budget

logger = get_logger("bot_private_relay_proactive")

_DECISION_ACTIONS = {"do_nothing", "send_social_message", "send_transaction_request"}
_DECISION_ATTEMPTS = 3
_PROACTIVE_LOCK = asyncio.Lock()


@dataclass(slots=True)
class ProactiveDecision:
    """Normalized proactive decision."""

    action: str
    target_bot_id: str = ""
    context_hint: str = ""
    reason: str = ""


async def run_proactive_tick(config: BotPrivateRelayConfig) -> bool:
    """Run one proactive decision and dispatch cycle."""

    if _PROACTIVE_LOCK.locked():
        store.audit("proactive_tick_skipped", reason_code="tick_already_running")
        return False
    async with _PROACTIVE_LOCK:
        return await _run_proactive_tick_locked(config)


async def _run_proactive_tick_locked(config: BotPrivateRelayConfig) -> bool:
    if not config.proactive.enabled:
        store.audit("proactive_tick_skipped", reason_code="proactive_disabled")
        logger.info("Proactive tick skipped: proactive_disabled")
        return False

    if not _has_recent_chat_hint():
        store.audit("proactive_tick_skipped", reason_code="no_recent_chat_hint")
        logger.info("Proactive tick skipped before LLM decision: no_recent_chat_hint")
        return False

    decision_task_name, decision_model_set = _resolve_model_set(config.proactive.decision_model_task)
    if decision_model_set is None:
        store.audit("proactive_tick_skipped", reason_code="decision_model_unavailable")
        logger.warning(f"Proactive tick skipped: decision_model_unavailable task={config.proactive.decision_model_task}")
        return False
    logger.info(f"Proactive decision model task resolved: {decision_task_name}")

    snapshot = fit_snapshot_to_budget(decision_model_set, build_proactive_snapshot(config))
    decision = await request_proactive_decision(config=config, model_set=decision_model_set, snapshot=snapshot)
    if decision.action == "do_nothing":
        reason_code = decision.reason or "decision_do_nothing"
        store.audit("proactive_tick_skipped", reason_code=reason_code)
        logger.info(f"Proactive tick skipped: {reason_code}")
        return False

    ok, reason_code = validate_decision(config, decision)
    if not ok:
        store.audit(
            "proactive_decision_rejected",
            action=decision.action,
            target_bot_id=decision.target_bot_id,
            reason_code=reason_code,
        )
        logger.info(
            "Proactive decision rejected: "
            f"action={decision.action}, target_bot_id={decision.target_bot_id}, reason_code={reason_code}"
        )
        return False

    message_task_name, message_model_set = _resolve_model_set(config.proactive.message_model_task)
    if message_model_set is None:
        logger.warning(f"Proactive message model unavailable; using fallback text task={config.proactive.message_model_task}")
    else:
        logger.info(f"Proactive message model task resolved: {message_task_name}")
    channel = "social" if decision.action == "send_social_message" else "transaction"
    text = await generate_proactive_message(
        config=config,
        model_set=message_model_set,
        decision=decision,
        channel=channel,
    )
    sent = await dispatch_proactive_message(config=config, decision=decision, channel=channel, text=text)
    if not sent:
        store.audit(
            "proactive_send_failed",
            action=decision.action,
            target_bot_id=decision.target_bot_id,
            reason_code="send_message_false",
        )
        logger.warning(
            "Proactive send failed: "
            f"action={decision.action}, target_bot_id={decision.target_bot_id}, reason_code=send_message_false"
        )
        return False

    mark_proactive_success(config, decision)
    store.audit(
        "proactive_send_succeeded",
        action=decision.action,
        target_bot_id=decision.target_bot_id,
        reason_code=decision.reason,
    )
    logger.info(
        "Proactive send succeeded: "
        f"action={decision.action}, target_bot_id={decision.target_bot_id}, reason={decision.reason}"
    )
    return True


def build_proactive_snapshot(config: BotPrivateRelayConfig) -> str:
    """Build bounded text snapshot for proactive decision."""

    now = time.time()
    lines = [
        "# Relay Proactive State Snapshot",
        "",
        "## Local Bot",
        f"bot_id: {config.relay.bot_id}",
        f"bot_name: {config.relay.bot_name}",
        f"now: {time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(now))}",
        "",
        "## Proactive Config",
        f"enabled: {config.proactive.enabled}",
        f"social_enabled: {config.proactive.social_enabled}",
        f"transaction_enabled: {config.proactive.transaction_enabled}",
        f"allow_offline_social: {config.proactive.allow_offline_social}",
        f"max_per_hour: {config.proactive.max_per_hour}",
        f"cooldown_seconds: {config.proactive.cooldown_seconds}",
        "",
        "## Partner Bots",
    ]
    partners = _configured_partners(config)
    for partner in sorted(partners, key=lambda item: _partner_sort_key(config, item, now))[:20]:
        presence = store.PRESENCE_TABLE.get(partner.bot_id)
        status = presence.status if presence is not None else "unknown"
        last_seen = "unknown" if presence is None else str(max(0, int(now - presence.last_seen)))
        open_transactions = _open_transaction_ids(partner.bot_id)
        lines.extend([
            f"- bot_id: {cap_field(partner.bot_id)}",
            f"  bot_name: {cap_field(partner.bot_name)}",
            f"  allowed: {partner.bot_id in config.presence.allowed_partner_bots}",
            f"  presence: {status}",
            f"  last_seen_seconds_ago: {last_seen}",
            f"  proactive_cooldown_active: {_cooldown_active(partner.bot_id, now)}",
            f"  social_count_this_hour: {_hourly_count('send_social_message', partner.bot_id, now)}",
            f"  transaction_count_this_hour: {_hourly_count('send_transaction_request', partner.bot_id, now)}",
            f"  has_open_transaction: {bool(open_transactions)}",
            f"  open_transaction_ids: {', '.join(open_transactions) if open_transactions else ''}",
        ])

    lines.extend(["", "## Active Sessions"])
    sessions = sorted(store.SESSION_TABLE.values(), key=lambda item: (item.terminal, -item.updated_at))[:20]
    for session in sessions:
        lines.extend([
            f"- conversation_id: {cap_field(session.conversation_id)}",
            f"  peer_bot_id: {cap_field(session.peer_bot_id)}",
            f"  channel: {session.channel}",
            f"  intent: {session.intent}",
            f"  state: {session.state or ''}",
            f"  terminal: {session.terminal}",
            f"  expect_reply: {session.expect_reply}",
            f"  reply_budget: {session.reply_budget}",
        ])

    lines.extend(["", "## Recent Chat Flow Hints"])
    chat_hint_limit = max(1, int(config.proactive.chat_hint_snapshot_items))
    for hint in store.PROACTIVE_CHAT_HINTS[-chat_hint_limit:]:
        lines.extend([
            f"- platform: {cap_field(hint.platform)}",
            f"  chat_type: {cap_field(hint.chat_type)}",
            f"  stream_id: {cap_field(hint.stream_id)}",
            f"  sender_id: {cap_field(hint.sender_id)}",
            f"  sender_name: {cap_field(hint.sender_name)}",
            f"  text: {cap_field(hint.text)}",
        ])

    lines.extend(["", "## Recent Audit Summary"])
    for entry in store.AUDIT_LOG[-10:]:
        event = compact_audit_value(entry.get("event"))
        data = ", ".join(
            f"{key}={compact_audit_value(value)}"
            for key, value in entry.items()
            if key not in {"event", "time"}
        )
        lines.append(f"- {event}: {data}")
    lines.extend(["", "## Instruction", "根据以上状态，选择一个 action。"])
    return "\n".join(lines)


async def request_proactive_decision(
    *,
    config: BotPrivateRelayConfig,
    model_set: object,
    snapshot: str,
) -> ProactiveDecision:
    """Ask the decision model and normalize its JSON answer."""

    prompt = _decision_prompt(config, snapshot)
    for attempt in range(1, _DECISION_ATTEMPTS + 1):
        store.audit("proactive_decision_requested", attempt=attempt)
        request = create_llm_request(
            model_set=model_set,
            request_name="bot_relay_proactive_decision",
        )
        request.add_payload(LLMPayload(ROLE.USER, Text(prompt)))
        response = await request.send(stream=False)
        message = str(getattr(response, "message", "") or "").strip()
        if not message:
            store.audit("proactive_decision_empty_response", attempt=attempt)
            logger.info(f"Proactive decision empty response: attempt={attempt}")
            if attempt < _DECISION_ATTEMPTS and config.proactive.decision_retry_interval_seconds > 0:
                await asyncio.sleep(config.proactive.decision_retry_interval_seconds)
            continue
        return parse_decision(message)

    store.audit("proactive_decision_cancelled", reason_code="decision_empty_after_retries")
    return ProactiveDecision("do_nothing", reason="decision_empty_after_retries")


def parse_decision(raw: str) -> ProactiveDecision:
    """Parse and normalize decision JSON."""

    try:
        data = repair_json_loads(raw)
    except Exception:
        store.audit("proactive_decision_parse_failed", reason_code="json_parse_failed")
        return ProactiveDecision("do_nothing", reason="decision_parse_failed")
    if not isinstance(data, dict):
        return ProactiveDecision("do_nothing", reason="decision_parse_failed")
    action = str(data.get("action") or "").strip()
    if action not in _DECISION_ACTIONS:
        return ProactiveDecision("do_nothing", reason="decision_parse_failed")
    return ProactiveDecision(
        action=action,
        target_bot_id=str(data.get("target_bot_id") or "").strip(),
        context_hint=str(data.get("context_hint") or "").strip(),
        reason=str(data.get("reason") or "").strip(),
    )


def validate_decision(config: BotPrivateRelayConfig, decision: ProactiveDecision) -> tuple[bool, str]:
    """Validate decision against deterministic hard gates."""

    if not config.proactive.enabled:
        return False, "proactive_disabled"
    if decision.action == "send_social_message" and not config.proactive.social_enabled:
        return False, "social_disabled"
    if decision.action == "send_transaction_request" and not config.proactive.transaction_enabled:
        return False, "transaction_disabled"
    if not decision.target_bot_id:
        return False, "target_empty"
    if decision.target_bot_id == config.relay.bot_id:
        return False, "target_self"
    partner = config.partner_by_id(decision.target_bot_id)
    if partner is None:
        return False, "target_unknown"
    if config.presence.require_known_partner and decision.target_bot_id not in config.presence.allowed_partner_bots:
        return False, "target_not_allowed"
    now = time.time()
    if _cooldown_active(decision.target_bot_id, now):
        return False, "cooldown_active"
    if _hourly_count(decision.action, decision.target_bot_id, now) >= config.proactive.max_per_hour:
        return False, "hourly_quota_exhausted"
    status = _presence_status(decision.target_bot_id)
    if decision.action == "send_transaction_request" and status != "online":
        return False, "transaction_target_offline"
    if (
        decision.action == "send_social_message"
        and status != "online"
        and not config.proactive.allow_offline_social
    ):
        return False, "social_target_offline"
    if decision.action == "send_transaction_request" and _has_open_transaction(decision.target_bot_id):
        return False, "open_transaction_exists"
    if not decision.context_hint:
        return False, "context_hint_empty"
    return True, "ok"


async def generate_proactive_message(
    *,
    config: BotPrivateRelayConfig,
    model_set: object | None,
    decision: ProactiveDecision,
    channel: str,
) -> str:
    """Generate outbound relay text or return a safe fallback."""

    fallback = _message_fallback(channel, decision.context_hint)
    if model_set is None:
        return fallback
    partner = config.partner_by_id(decision.target_bot_id)
    prompt_system, prompt_user = _message_prompts(config, decision, channel, partner)
    try:
        request = create_llm_request(
            model_set=model_set,
            request_name="bot_relay_proactive_message",
        )
        request.add_payload(LLMPayload(ROLE.SYSTEM, Text(prompt_system)))
        request.add_payload(LLMPayload(ROLE.USER, Text(prompt_user)))
        response = await request.send(stream=False)
        message = str(getattr(response, "message", "") or "").strip()
    except Exception as exc:
        logger.warning(f"Proactive message generation failed: {exc}")
        message = ""
    text = message or fallback
    store.audit(
        "proactive_message_generated",
        action=decision.action,
        target_bot_id=decision.target_bot_id,
        reason_code=decision.reason,
        preview=cap_field(text, 80),
    )
    return text


async def dispatch_proactive_message(
    *,
    config: BotPrivateRelayConfig,
    decision: ProactiveDecision,
    channel: str,
    text: str,
) -> bool:
    """Send proactive social or transaction through the normal relay adapter path."""

    partner = config.partner_by_id(decision.target_bot_id)
    if partner is None:
        return False
    stream_id = ChatStream.generate_stream_id("bot_relay", user_id=partner.bot_id)
    trace_id = uuid4().hex
    relay_context: dict[str, Any]
    if channel == "social":
        relay_context = {
            "channel": "social",
            "intent": "say",
            "peer_bot_id": partner.bot_id,
            "peer_bot_name": partner.bot_name,
            "conversation_id": uuid4().hex,
            "trace_id": trace_id,
            "phase": "opening",
            "terminal": False,
            "expect_reply": True,
            "reply_budget": config.relay.default_reply_budget,
            "allowed_responders": [partner.bot_id],
            "proactive": True,
            "proactive_reason": decision.reason,
        }
    else:
        relay_context = {
            "channel": "transaction",
            "intent": "request",
            "peer_bot_id": partner.bot_id,
            "peer_bot_name": partner.bot_name,
            "trace_id": trace_id,
            "proactive": True,
            "proactive_reason": decision.reason,
            "structured": {
                "context_hint": decision.context_hint,
                "source": "bot_private_relay_proactive",
            },
        }
    message = Message(
        message_id=f"relay-proactive-{uuid4().hex}",
        content=text,
        processed_plain_text=text,
        message_type=MessageType.TEXT,
        platform="bot_relay",
        chat_type="private",
        stream_id=stream_id,
        target_user_id=partner.bot_id,
        target_user_name=partner.bot_name,
        relay_context=relay_context,
    )
    return bool(await get_message_sender().send_message(message, "bot_private_relay:adapter:bot_relay"))


def mark_proactive_success(config: BotPrivateRelayConfig, decision: ProactiveDecision) -> None:
    """Consume proactive quota after a successful send."""

    now = time.time()
    hour_key = _hour_key(now)
    key = (decision.action, decision.target_bot_id, hour_key)
    store.PROACTIVE_HOURLY_COUNTS[key] = store.PROACTIVE_HOURLY_COUNTS.get(key, 0) + 1
    if config.proactive.cooldown_seconds > 0:
        store.PROACTIVE_COOLDOWNS[decision.target_bot_id] = now + config.proactive.cooldown_seconds


def _resolve_model_set(*task_names: str) -> tuple[str, object | None]:
    for task_name in task_names:
        try:
            model_set = get_model_set_by_task(task_name)
        except Exception:
            continue
        if model_set:
            return task_name, model_set
    return "", None


def _configured_partners(config: BotPrivateRelayConfig) -> list[PartnerSection]:
    partners: list[PartnerSection] = []
    for value in vars(config.partners).values():
        if isinstance(value, PartnerSection) and value.bot_id:
            partners.append(value)
    return partners


def _partner_sort_key(config: BotPrivateRelayConfig, partner: PartnerSection, now: float) -> tuple[int, int, float]:
    presence = store.PRESENCE_TABLE.get(partner.bot_id)
    allowed = partner.bot_id in config.presence.allowed_partner_bots
    online = presence is not None and presence.status == "online"
    last_seen = presence.last_seen if presence is not None else 0.0
    return (0 if allowed else 1, 0 if online else 1, -(last_seen or now))


def _presence_status(bot_id: str) -> str:
    presence = store.PRESENCE_TABLE.get(bot_id)
    return presence.status if presence is not None else "unknown"


def _open_transaction_ids(peer_bot_id: str) -> list[str]:
    return [
        session.conversation_id
        for session in store.SESSION_TABLE.values()
        if session.peer_bot_id == peer_bot_id
        and session.channel == "transaction"
        and not session.terminal
        and (session.state or "") != "closed"
    ]


def _has_open_transaction(peer_bot_id: str) -> bool:
    return bool(_open_transaction_ids(peer_bot_id))


def _cooldown_active(target_bot_id: str, now: float) -> bool:
    return store.PROACTIVE_COOLDOWNS.get(target_bot_id, 0.0) > now


def _hourly_count(action: str, target_bot_id: str, now: float) -> int:
    return store.PROACTIVE_HOURLY_COUNTS.get((action, target_bot_id, _hour_key(now)), 0)


def _hour_key(now: float) -> str:
    return time.strftime("%Y-%m-%dT%H", time.localtime(now))


def _has_recent_chat_hint(ttl_seconds: int = 1800) -> bool:
    """Return whether a recent ordinary chat hint exists."""

    now = time.time()
    store.PROACTIVE_CHAT_HINTS[:] = [
        hint for hint in store.PROACTIVE_CHAT_HINTS if now - hint.received_at <= ttl_seconds
    ]
    return bool(store.PROACTIVE_CHAT_HINTS)


def _decision_prompt(config: BotPrivateRelayConfig, snapshot: str) -> str:
    partner_lines = _decision_partner_lines(config)
    return "\n".join([
        "你是 bot_private_relay 的主动通信决策子代理。",
        "",
        f"你正在替 {config.relay.bot_name} 做判断：",
        f"- bot_id: {config.relay.bot_id}",
        "- 本轮最多只能选择一个 action",
        "- 你只做决策，不生成最终外发消息正文",
        "",
        "# 可联系 bot 列表",
        "只能从下面列表选择 target_bot_id；不要猜测、编造或使用 bot_name 作为 target_bot_id。",
        partner_lines,
        "",
        "# 可用 action",
        "1. do_nothing",
        "2. send_social_message",
        "3. send_transaction_request",
        "",
        "# 决策规则",
        "如果没有用户意图、上下文契机或可自然延续的话题，选择 do_nothing。Recent Chat Flow Hints 是当前普通聊天流触发来源；如果其中出现用户要求联系某个伙伴、约定事项、转达请求或明确使用 request/social 的意图，应优先据此决策。",
        "social 可以用于低压力问候、转达、约饭/聊天邀约、自然延续当前聊天流中的轻量话题；不要求必须已经发生过与目标 bot 的直接互动，也不要求用户完整说出目标 bot_id。只要能从 bot_name 映射到可联系 bot，且 gate 允许，就可以选择 send_social_message。",
        "transaction 必须有清晰事务目标、需要对方接受/拒绝/确认的事项；context_hint 是消息生成方向，不是最终正文，且不得编造 snapshot 没有的事实。",
        "禁止向自身、不在 allowed partners 中、离线或 cooldown/quota 不满足的目标发起通信。",
        "",
        "# 输出要求",
        "只输出 JSON，不要 Markdown，不要解释，不要代码块。",
        '{"action":"do_nothing|send_social_message|send_transaction_request","target_bot_id":"string","context_hint":"string","reason":"string"}',
        "",
        snapshot,
    ])


def _decision_partner_lines(config: BotPrivateRelayConfig) -> str:
    """Render explicit target bot ids for the decision prompt."""

    lines: list[str] = []
    for partner in _configured_partners(config):
        allowed = partner.bot_id in config.presence.allowed_partner_bots
        lines.append(
            f"- target_bot_id: {partner.bot_id} | bot_name: {partner.bot_name or partner.bot_id} | allowed: {allowed}"
        )
    return "\n".join(lines) or "（无可联系 bot）"


def _message_prompts(
    config: BotPrivateRelayConfig,
    decision: ProactiveDecision,
    channel: str,
    partner: PartnerSection | None,
) -> tuple[str, str]:
    personality = _core_personality_or_none()
    target_name = partner.bot_name if partner is not None else decision.target_bot_id
    nickname = config.relay.bot_name or _personality_field(personality, "nickname")
    system = "\n".join([
        "你是 bot_private_relay 的主动外发消息生成器。",
        f"本 bot 昵称: {nickname}",
        f"本 bot id: {config.relay.bot_id}",
        f"目标 bot 名称: {target_name}",
        f"目标 bot id: {decision.target_bot_id}",
        f"channel: {channel}",
        "",
        "核心人格:",
        _personality_field(personality, "personality_core"),
        "表达风格:",
        _personality_field(personality, "reply_style"),
        "人格侧面:",
        _personality_field(personality, "personality_side"),
        "身份:",
        _personality_field(personality, "identity"),
        "背景故事:",
        _personality_field(personality, "background_story"),
        "",
        "# 事实边界",
        "你只能基于 USER 中给出的 context_hint 和 relay snapshot facts 生成消息。",
        "人格、背景故事、表达风格只能影响语气，不能新增事实。",
        "禁止主动引入新地点、新人物、旧约定、未出现的任务、承诺、牺牲、守护、宿命等剧情化表达、未出现的称呼或对方未说过的意图。",
        "",
        "# channel 规则",
        "social: 输出一条自然、低压力、简短的社交消息，不要求对方做明确承诺。",
        "transaction: 输出一条明确、可接受或拒绝的事务提案。",
        "",
        "# 输出要求",
        "只输出最终消息正文。不要 JSON。不要 Markdown。不要解释。长度 1 到 2 句话。",
    ])
    user = "\n".join([
        "# Context Hint",
        decision.context_hint,
        "",
        "# Relay Snapshot Facts",
        f"target_presence: {_presence_status(decision.target_bot_id)}",
        f"open_transaction: {_has_open_transaction(decision.target_bot_id)}",
        "recent_relevant_sessions:",
        _relevant_sessions_text(decision.target_bot_id),
        "",
        "# Task",
        f"请生成一条适合通过 {channel} channel 发送给目标 bot 的消息。",
    ])
    return system, user


def _relevant_sessions_text(peer_bot_id: str) -> str:
    lines: list[str] = []
    sessions = [session for session in store.SESSION_TABLE.values() if session.peer_bot_id == peer_bot_id]
    for session in sorted(sessions, key=lambda item: -item.updated_at)[:5]:
        lines.append(
            f"- {session.conversation_id}: channel={session.channel}, intent={session.intent}, "
            f"state={session.state or ''}, phase={session.phase or ''}, terminal={session.terminal}"
        )
    return "\n".join(lines) or "（无）"


def _core_personality_or_none() -> object | None:
    """Return core personality when core config is initialized."""

    try:
        return get_core_config().personality
    except RuntimeError:
        return None


def _personality_field(personality: object | None, field_name: str) -> str:
    """Return a personality string field, or empty text when unavailable."""

    return str(getattr(personality, field_name, "") or "")


def _message_fallback(channel: str, context_hint: str) -> str:
    if channel == "transaction":
        return f"我想和你确认一件事：{context_hint}"
    return "现在方便聊两句吗？"
