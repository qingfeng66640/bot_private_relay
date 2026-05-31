"""主动中继快照裁剪工具函数。"""

# =============================================================================
# Proactive 快照裁剪工具
# =============================================================================
# proactive 决策需要将系统状态快照发送给 LLM，但 LLM 的上下文窗口有限。
# 此模块提供了快照裁剪和 token 预算管理的工具函数。
#
# 核心功能：
# 1. fit_snapshot_to_budget() - 将快照裁剪到模型上下文的 1/4
# 2. cap_field()            - 截断过长字段
# 3. compact_audit_value()  - 压缩审计日志值
# =============================================================================

from __future__ import annotations

from typing import Any

from src.kernel.llm.token_counter import count_text_tokens


def model_identifier_from_model_set(model_set: object) -> str:
    """从 ModelSet 类对象中返回第一个模型标识符。

    从 ModelSet 中提取第一个模型的标识符，用于 token 计数。
    """

    if not isinstance(model_set, list) or not model_set:
        return ""
    first_model = model_set[0]
    if not isinstance(first_model, dict):
        return ""
    model_identifier = first_model.get("model_identifier")
    return model_identifier if isinstance(model_identifier, str) else ""


def token_budget_from_model_set(model_set: object) -> int:
    """从第一个模型配置中推导出主动快照的 token 预算。

    计算 token 预算：min(max(1024, max_context // 4), 8000)。
    即使用模型最大上下文的 1/4，但不能低于 1024，不能超过 8000。
    """

    if not isinstance(model_set, list) or not model_set:
        return 6000
    first_model = model_set[0]
    if not isinstance(first_model, dict):
        return 6000
    max_context = first_model.get("max_context")
    if isinstance(max_context, int) and max_context > 0:
        return min(max(1024, max_context // 4), 8000)
    return 6000


def fit_snapshot_to_budget(model_set: object, snapshot: str) -> str:
    """将结构化快照裁剪到主动决策模型的预算范围内。

    如果快照的 token 数在预算内，直接返回。
    否则按行从末尾开始保留（保留最新的信息），直到超出预算。
    如果按行裁剪后仍然过大，使用二分查找在字符级别裁剪。
    """

    model_identifier = model_identifier_from_model_set(model_set)
    if not model_identifier:
        return snapshot

    token_budget = token_budget_from_model_set(model_set)
    try:
        if count_text_tokens(snapshot, model_identifier=model_identifier) <= token_budget:
            return snapshot
    except Exception:
        return snapshot

    return _trim_text_suffix_by_budget(snapshot, model_identifier, token_budget)


def cap_field(value: object, max_chars: int = 500) -> str:
    """返回用于快照渲染的有界单字段字符串。

    截断过长的字符串字段，防止单个字段占用过多 token。
    """

    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}..."


def _safe_count_tokens(text: str, model_identifier: str) -> int:
    """对文本进行 token 计数，如果分词器失败则返回 0。"""

    try:
        return count_text_tokens(text, model_identifier=model_identifier)
    except Exception:
        return 0


def _trim_text_suffix_by_budget(
    text: str,
    model_identifier: str,
    token_budget: int,
) -> str:
    """保留最新的快照行，同时确保不超出 token 预算。

    从末尾开始保留行（优先保留最新信息），直到超出 token 预算。
    如果单行就超出预算，回退到字符级别裁剪。
    """

    if token_budget <= 0 or not text:
        return ""

    lines = text.splitlines()
    kept_reversed: list[str] = []
    used_tokens = 0

    for line in reversed(lines):
        line_tokens = _safe_count_tokens(line, model_identifier)
        if kept_reversed and used_tokens + line_tokens > token_budget:
            break
        kept_reversed.append(line)
        used_tokens += line_tokens

    candidate = "\n".join(reversed(kept_reversed)).strip()
    if candidate and _safe_count_tokens(candidate, model_identifier) <= token_budget:
        return candidate

    # 按行裁剪后仍超标 → 回退到字符级别裁剪
    return _trim_text_suffix_by_chars(text, model_identifier, token_budget)


def _trim_text_suffix_by_chars(
    text: str,
    model_identifier: str,
    token_budget: int,
) -> str:
    """当按行裁剪仍然过大时，对字符后缀进行二分查找。

    使用二分查找找到满足 token 预算的最大字符后缀。
    """

    left = 0
    right = len(text)
    best = text[-512:]  # 默认保留最后 512 字符

    while left <= right:
        middle = (left + right) // 2
        suffix = text[middle:]
        token_count = _safe_count_tokens(suffix, model_identifier)

        if token_count == 0 or token_count > token_budget:
            left = middle + 1
            continue

        best = suffix
        right = middle - 1

    return best.strip()


def compact_audit_value(value: Any) -> str:
    """格式化审计值用于快照行，避免泄漏大型 payload。

    格式化审计日志值：简单类型直接截断，复杂类型用 repr 截断。
    防止大 payload 泄漏到快照中。
    """

    if isinstance(value, (str, int, float, bool)) or value is None:
        return cap_field(value, 120)
    return cap_field(repr(value), 120)
