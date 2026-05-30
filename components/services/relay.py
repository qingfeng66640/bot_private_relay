"""Stateless service wrappers for relay state."""

# =============================================================================
# 服务组件模块
# =============================================================================
# 提供两个 Service 组件：
#
# 1. RelayStateService  - 状态查询服务
#    对外暴露插件运行时的各种状态快照（在线状态、会话、事务日志、审计日志等），
#    并支持将调试数据导出为 JSON 文件。
#    被 /relay status 和 /relay inspect 命令调用。
#
# 2. RelayProactiveService - 主动通信调度服务
#    封装 proactive 决策周期的执行入口，被定时器和插件初始化时调用。
# =============================================================================

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from src.core.components.base import BaseService

from ...runtime import store
from ...runtime.proactive import run_proactive_tick
from ..config import BotPrivateRelayConfig


class RelayStateService(BaseService):
    """Expose relay state without owning any instance-local runtime state.

    此服务自身不持有状态，所有数据从模块级 store 中读取。
    """

    service_name = "relay_state"
    service_description = "Bot private relay state access"
    version = "0.1.0"

    def presence_snapshot(self) -> dict[str, store.PresenceRecord]:
        """Return current presence table.

        返回当前所有已知伙伴 bot 的在线状态快照。
        key 为 bot_id，value 为 PresenceRecord。
        """

        return dict(store.PRESENCE_TABLE)

    def session_snapshot(self) -> dict[str, store.RelaySession]:
        """Return current session table.

        返回当前所有活跃的 relay 会话快照。
        key 为 conversation_id，value 为 RelaySession。
        """

        return dict(store.SESSION_TABLE)

    def memory_candidate_snapshot(self) -> dict[str, store.RelayMemoryCandidate]:
        """Return projected memory candidates.

        返回 relay 对话中产生的记忆候选。
        """

        return dict(store.RELAY_MEMORY_CANDIDATES)

    def audit_snapshot(self) -> list[dict[str, object]]:
        """Return audit log snapshot.

        返回审计日志的快照列表。
        每条日志包含 event、time 及调用方传入的自定义字段。
        """

        return list(store.AUDIT_LOG)

    def transaction_log_snapshot(self) -> dict[str, store.RelayTransactionRecord]:
        """Return transaction log snapshot.

        返回事务日志快照，记录了每个事务的状态变化历史。
        """

        return dict(store.TRANSACTION_LOG)

    def export_debug_snapshot(self, output_dir: str | Path) -> Path:
        """Persist a plugin-local debug snapshot.

        将当前插件的完整运行状态导出为 JSON 文件，用于离线调试。
        导出内容包括：presence、sessions、transactions、memory_candidates、audit。

        This is a plugin-local optional persistence helper only. It does not
        replace future framework-approved persistence paths.
        """

        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        target = path / "relay_debug_snapshot.json"
        payload = {
            "presence": {
                key: asdict(value) for key, value in store.PRESENCE_TABLE.items()
            },
            "sessions": {
                key: asdict(value) for key, value in store.SESSION_TABLE.items()
            },
            "transactions": {
                key: asdict(value) for key, value in store.TRANSACTION_LOG.items()
            },
            "memory_candidates": {
                key: asdict(value) for key, value in store.RELAY_MEMORY_CANDIDATES.items()
            },
            "audit": list(store.AUDIT_LOG),
        }
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return target


class RelayProactiveService(BaseService):
    """Run proactive relay ticks without owning runtime state.

    封装 proactive 决策的执行入口。
    不持有状态，每次 tick 从配置中读取参数并执行完整的决策→生成→发送流程。
    """

    service_name = "relay_proactive"
    service_description = "Bot private relay proactive initiation"
    version = "0.1.0"

    async def tick(self) -> bool:
        """Run one proactive decision cycle.

        执行一次完整的主动通信决策周期：
        1. 构建系统状态快照
        2. 调用决策 LLM 判断是否需要行动
        3. 如果决策为 do_nothing，记录原因并返回
        4. 如果决策为 send_social_message 或 send_transaction_request：
           - 验证决策的合法性（硬门禁）
           - 调用消息生成 LLM 生成外发内容
           - 通过 MQTT 发送消息

        Returns:
            True 如果成功发送了一条主动消息，否则 False。
        """

        config = getattr(self.plugin, "config", None)
        if not isinstance(config, BotPrivateRelayConfig):
            return False
        return await run_proactive_tick(config)
