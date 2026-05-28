"""Helpers for proactive relay snapshot trimming."""

from __future__ import annotations

from typing import Any

from src.kernel.llm.token_counter import count_text_tokens


def model_identifier_from_model_set(model_set: object) -> str:
    """Return the first model identifier from a ModelSet-like object."""

    if not isinstance(model_set, list) or not model_set:
        return ""
    first_model = model_set[0]
    if not isinstance(first_model, dict):
        return ""
    model_identifier = first_model.get("model_identifier")
    return model_identifier if isinstance(model_identifier, str) else ""


def token_budget_from_model_set(model_set: object) -> int:
    """Return proactive snapshot budget derived from first model config."""

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
    """Trim a structured snapshot to the proactive decision model budget."""

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
    """Return a bounded single-field string for snapshot rendering."""

    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}..."


def _safe_count_tokens(text: str, model_identifier: str) -> int:
    """Count text tokens and return 0 if the provider tokenizer fails."""

    try:
        return count_text_tokens(text, model_identifier=model_identifier)
    except Exception:
        return 0


def _trim_text_suffix_by_budget(
    text: str,
    model_identifier: str,
    token_budget: int,
) -> str:
    """Keep the newest snapshot lines while staying under token budget."""

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

    return _trim_text_suffix_by_chars(text, model_identifier, token_budget)


def _trim_text_suffix_by_chars(
    text: str,
    model_identifier: str,
    token_budget: int,
) -> str:
    """Binary-search a char suffix when line-based trimming is still too large."""

    left = 0
    right = len(text)
    best = text[-512:]
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
    """Format an audit value for snapshot lines without leaking large payloads."""

    if isinstance(value, (str, int, float, bool)) or value is None:
        return cap_field(value, 120)
    return cap_field(repr(value), 120)
