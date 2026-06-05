"""bot_private_relay 插件入口点。"""

# =============================================================================
# bot_private_relay 插件入口模块
# =============================================================================
# 此模块是插件的入口点，负责：
# 1. 定义 BotPrivateRelayPlugin 类并注册到框架
# 2. 声明所有插件组件（Adapter、Chatter、Action、Tool、Command 等）
# 3. 管理插件生命周期钩子（加载/卸载）
# 4. 注册 proactive（主动通信）调度器
# 5. 将 RelaySocialContactTool 注册到外部 todo_plugin
# =============================================================================

from __future__ import annotations

import asyncio

from src.app.plugin_system.base import BasePlugin, register_plugin
from src.kernel.concurrency import get_task_manager
from src.kernel.logger import get_logger

# ── 导入所有插件组件 ──────────────────────────────────────────────────────
from .components.actions.relay import (
    BotRelayPassAndWaitAction,                # 等待对端消息的 Action
    BotRelaySendTextAction,                   # 发送文本的 Action
    BotRelayStopConversationAction,           # 停止对话的 Action
)
from .components.adapters.bot_relay import BotRelayAdapter          # MQTT 通信适配器
from .components.chatters.bot_relay import BotRelayChatter          # 中继对话智能体
from .components.commands.relay import RelayCommand             # /relay 命令行管理
from .components.config import BotPrivateRelayConfig     # 插件配置定义
from .components.events.relay import (  # 事件处理器
    DefaultChatterRelayContextBridgeEventHandler,
    GroupReplySuppressionEventHandler,
    LoopGuardEventHandler,
)
from .components.routers.bot_private_relay import BotPrivateRelayRouter     # HTTP 管理路由
from .components.services.memory_bridge import MemoryBridgeService  # 记忆桥接服务
from .components.services.relay import RelayProactiveService, RelayStateService  # 服务组件
from .components.tools.dynamic_social import RelaySocialContactTool, register_relay_config  # 动态社交联系
from .components.tools.transactions import (
    AcceptTransactionTool,                    # 接受事务 Tool
    AckTransactionTool,                       # 确认收到并关闭 Tool
    CancelTransactionTool,                    # 取消事务 Tool
    CloseTransactionTool,                     # 关闭事务 Tool
    ConfirmTransactionTool,                   # 确认事务 Tool
    DeclineTransactionTool,                   # 拒绝事务 Tool
    RescheduleTransactionTool,                # 改期事务 Tool
)

logger = get_logger("bot_private_relay_plugin")


@register_plugin
class BotPrivateRelayPlugin(BasePlugin):
    """Bot 私有中继插件。

    运行时插件标识遵循绑定的仓库名称。
    传输平台保持为 ``bot_relay``。
    """

    # ── 插件元数据 ──────────────────────────────────────────────────────
    plugin_name = "bot_private_relay"
    plugin_description = "基于 MQTT 的 bot 私有中继插件，支持跨 bot 私聊、社交联系、事务协商与在线状态"
    plugin_version = "0.1.5"
    configs = [BotPrivateRelayConfig]          # 使用的配置类
    dependent_components: list[str] = []       # 无外部插件依赖

    def __init__(self, config: BotPrivateRelayConfig | None = None) -> None:
        super().__init__(config)
        # proactive 调度器的 schedule ID，用于卸载时取消调度
        self._proactive_schedule_id: str | None = None
        # proactive 注册任务的 task_id，用于卸载时取消等待
        self._proactive_register_task_id: str | None = None

    def get_components(self) -> list[type]:
        """返回为阶段1注册的插件组件。"""

        # 返回插件注册的所有组件类型，框架会根据类型自动实例化和管理
        return [
            # ── 核心通信层 ──
            BotRelayAdapter,                   # MQTT 适配器：建立连接、收发消息、心跳维护

            # ── LLM 对话层 ──
            BotRelayChatter,                   # 中继 Chatter：处理 bot 间对话逻辑

            # ── Action 组件（LLM 可调用的动作） ──
            BotRelaySendTextAction,            # 发送文本消息给对端 bot
            BotRelayPassAndWaitAction,         # 本轮不回复，等待对端消息
            BotRelayStopConversationAction,    # 结束当前对话，进入冷却

            # ── Tool 组件（事务协议工具，LLM 可调用） ──
            AcceptTransactionTool,             # 接受事务请求
            ConfirmTransactionTool,            # 确认事务（终态操作）
            DeclineTransactionTool,            # 拒绝事务
            CancelTransactionTool,             # 取消事务
            RescheduleTransactionTool,         # 提出改期方案
            AckTransactionTool,                # 确认收到并关闭事务
            CloseTransactionTool,              # 关闭事务
            RelaySocialContactTool,            # 通过 social channel 主动联系其他 bot

            # ── 事件处理器 ──
            LoopGuardEventHandler,             # 防循环/消息去重/预算守卫
            DefaultChatterRelayContextBridgeEventHandler,  # 将精选 relay 上下文注入普通 Chatter
            GroupReplySuppressionEventHandler, # 群聊中静默特定 bot 消息

            # ── 命令 ──
            RelayCommand,                      # /relay 管理命令

            # ── 服务 ──
            RelayStateService,                 # 状态查询和调试导出
            RelayProactiveService,             # 主动通信调度服务
            MemoryBridgeService,               # 记忆候选桥接
            BotPrivateRelayRouter,             # HTTP 管理端点
        ]

    # =========================================================================
    # 生命周期钩子
    # =========================================================================

    async def on_plugin_loaded(self) -> None:
        """对外暴露中继工具，并在启用时注册 proactive 调度器。"""

        # ── 注册配置到全局，供其他插件（如 todo_plugin）的 Tool 使用 ──
        if isinstance(self.config, BotPrivateRelayConfig):
            register_relay_config(self.config)

            # ── 如果启用了 proactive（主动通信），创建调度器注册任务 ──
            if self.config.proactive.enabled:
                # 使用 task_manager 创建 daemon 任务，而不是裸 asyncio.create_task
                task = get_task_manager().create_task(
                    self._register_proactive_schedule_when_ready(),
                    name="bot_private_relay_register_proactive_schedule",
                    daemon=True,
                )
                self._proactive_register_task_id = task.task_id

        # ── 尝试将 RelaySocialContactTool 注册到外部 todo_plugin ──
        # 如果 todo_plugin 未安装，则静默跳过
        try:
            from plugins.todo_plugin.registry import register_bot_tool
        except Exception:
            return
        register_bot_tool(RelaySocialContactTool)

    async def on_plugin_unloaded(self) -> None:
        """移除此插件实例持有的 proactive 调度器状态。"""

        # ── 取消 proactive 调度器 ──
        if self._proactive_schedule_id:
            try:
                from src.kernel.scheduler import get_unified_scheduler

                await get_unified_scheduler().remove_schedule(self._proactive_schedule_id)
            except Exception:
                pass
            self._proactive_schedule_id = None

        # ── 取消注册任务 ──
        if self._proactive_register_task_id:
            try:
                get_task_manager().cancel_task(self._proactive_register_task_id)
            except Exception:
                pass
            self._proactive_register_task_id = None

    # =========================================================================
    # Proactive 调度器注册
    # =========================================================================

    async def _register_proactive_schedule_when_ready(self) -> None:
        """在调度器就绪后注册定期 proactive tick。

        功能：等待调度器就绪后注册定期 tick。因为调度器可能在插件加载时
        尚未完全初始化，所以这里使用轮询等待（最多 600 次 × 0.5s = 5 分钟）。
        """

        from src.kernel.scheduler import TriggerType, get_unified_scheduler

        # 二次确认配置有效
        if not isinstance(self.config, BotPrivateRelayConfig) or not self.config.proactive.enabled:
            return

        # 取配置的检查间隔，最小 1 秒
        interval = max(1, int(self.config.proactive.check_interval_seconds))
        scheduler = get_unified_scheduler()

        # 轮询等待调度器就绪（最多 600 次尝试）
        for _attempt in range(600):
            try:
                # 创建周期性调度：每隔 interval 秒触发一次 proactive_tick_job
                self._proactive_schedule_id = await scheduler.create_schedule(
                    callback=self._proactive_tick_job,
                    trigger_type=TriggerType.TIME,
                    trigger_config={"interval_seconds": interval},
                    is_recurring=True,
                    task_name="bot_private_relay_proactive",
                    force_overwrite=True,
                )
                logger.info(f"Bot 私有中继 proactive 调度已注册: {self._proactive_schedule_id}")

                # 立即触发首次 proactive tick（不等第一个周期）
                get_task_manager().create_task(
                    self._proactive_tick_job(),
                    name="bot_private_relay_proactive_initial_tick",
                    daemon=True,
                )
                return
            except RuntimeError:
                # 调度器尚未就绪，等待 0.5 秒后重试
                await asyncio.sleep(0.5)
            except Exception as exc:
                # 其他异常，等待 2 秒后重试
                logger.warning(f"Bot 私有中继 proactive 调度注册失败: {exc}")
                await asyncio.sleep(2.0)

        logger.warning("Bot 私有中继 proactive 调度注册超时")

    async def _proactive_tick_job(self) -> None:
        """单次 proactive 中继 tick 的调度器回调。

        每次定时器触发时，执行一次主动通信决策循环：
        1. 收集最近的聊天线索和系统状态快照
        2. 调用决策 LLM 判断是否需要主动联系伙伴 bot
        3. 如果需要，调用消息生成 LLM 生成外发内容
        4. 通过 MQTT 发送消息
        """

        await RelayProactiveService(self).tick()
