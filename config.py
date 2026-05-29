"""Bot private relay plugin configuration.

Production deployments use Neo-MoFox's standard plugin config loading flow:
``BaseConfig.get_default_path()`` resolves to
``config/plugins/bot_private_relay/config.toml`` and is read automatically by
``config_manager`` during plugin load.  Any hand-written config files kept
inside the plugin directory are dev/test only.
"""

# =============================================================================
# bot_private_relay 插件配置模块
# =============================================================================
# 定义了插件的所有配置项，分为多个配置段（Section）：
#
# [relay]          - 中继基本配置（bot 身份、MQTT 连接、TLS、TTL 等）
# [partners]       - 伙伴 bot 映射（bot_id → bot_name）
# [presence]       - 在线状态与白名单
# [todo_bridge]    - 事务确认后桥接到 todo_plugin
# [dynamic_social] - 动态社交联系配额
# [proactive]      - bot 自主发起通信设置
# [group_reply_suppression] - 群聊中静默特定 bot
# [social_quotas]  - 每目标社交配额覆盖
#
# 生产部署时，配置文件位于：
#   config/plugins/bot_private_relay/config.toml
# =============================================================================

from __future__ import annotations

from typing import ClassVar

from src.core.components.base.config import BaseConfig, Field, SectionBase, config_section


# =============================================================================
# 伙伴 Bot 配置
# =============================================================================
class PartnerSection(SectionBase):
    """Configured relay partner.

    ``bot_id`` is the only value used for routing and permission checks.
    ``bot_name`` is display-only for prompts, logs, and history rendering.
    """

    # bot_id 是路由和安全校验的唯一标识，不可伪造
    bot_id: str = Field(default="", description="伙伴 bot 的路由 ID")
    # bot_name 仅用于显示（prompt、日志、历史渲染），不作为安全依据
    bot_name: str = Field(default="", description="伙伴 bot 的显示名称")


# =============================================================================
# 社交配额配置（每目标覆盖）
# =============================================================================
class SocialQuotaSection(SectionBase):
    """Per-target proactive social quota."""

    max_per_day: int = Field(default=5, description="每天最多主动社交联系次数")
    max_per_hour: int = Field(default=2, description="每小时最多主动社交联系次数")
    cooldown_seconds: int = Field(default=300, description="主动社交联系冷却秒数")


# =============================================================================
# 主配置类 BotPrivateRelayConfig
# =============================================================================
class BotPrivateRelayConfig(BaseConfig):
    """Configuration for the bot private relay plugin.

    Production setup: edit ``config/plugins/bot_private_relay/config.toml``
    (created automatically by the framework on first load).  A minimal
    production TOML::

        [relay]
        bot_id = "223123"
        bot_name = "清风"
        relay_url = "mqtts://relay.example.com:8883"
        auth_token = "shared-token"
        tls_ca_file = ""
        tls_insecure = false

        [partners.bot_b]
        bot_id = "114514"
        bot_name = "流光"

        [presence]
        allowed_partner_bots = ["114514"]
    """

    config_name: ClassVar[str] = "config"
    config_description: ClassVar[str] = "Bot 私有中继配置"

    # =========================================================================
    # [relay] 配置段 - 中继身份与 MQTT 连接
    # =========================================================================
    @config_section("relay", title="Relay", tag="plugin")
    class RelaySection(SectionBase):
        """Relay identity and broker options."""

        enabled: bool = Field(default=True, description="启用 bot 私有中继")
        bot_id: str = Field(default="", description="本 bot 的安全身份 ID，用于路由和权限校验")
        bot_name: str = Field(default="", description="本 bot 的显示名称")
        relay_url: str = Field(default="mqtt://localhost:1883", description="MQTT 中继地址，支持 mqtt:// 和 mqtts://")
        auth_token: str = Field(default="", description="可选认证 token，用于传输层安全校验")
        tls_enabled: bool = Field(default=False, description="强制启用 MQTT TLS；relay_url 使用 mqtts:// 时会自动启用")
        tls_ca_file: str = Field(default="", description="TLS CA 证书路径；为空时使用系统默认 CA")
        tls_cert_file: str = Field(default="", description="可选客户端证书路径，用于双向 TLS（mTLS）")
        tls_key_file: str = Field(default="", description="可选客户端私钥路径，用于双向 TLS（mTLS）")
        tls_insecure: bool = Field(default=False, description="仅调试使用：跳过 TLS 证书与主机名校验，生产环境不要启用")
        default_ttl: int = Field(default=4, description="默认中继跳数 TTL，超过此跳数的消息将被丢弃")
        default_reply_budget: int = Field(default=3, description="默认请求回复预算，每次回复减 1，耗尽后禁止自动回复")
        show_system_message_logs: bool = Field(default=True, description="是否在日志中展示系统消息（presence）的入站记录")

    # =========================================================================
    # [partners] 配置段 - 伙伴 bot 映射
    # =========================================================================
    @config_section("partners", title="Partners", tag="plugin")
    class PartnersSection(SectionBase):
        """Partner mapping for local tests and simple deployments."""

        bot_b: PartnerSection = Field(default_factory=PartnerSection)

    # =========================================================================
    # [presence] 配置段 - 在线状态与白名单
    # =========================================================================
    @config_section("presence", title="Presence", tag="plugin")
    class PresenceSection(SectionBase):
        """Presence and allowlist settings."""

        allowed_partner_bots: list[str] = Field(default_factory=list, description="允许通信的伙伴 bot_id 列表（白名单）")
        require_known_partner: bool = Field(default=True, description="是否要求对端必须在已知伙伴列表中，关闭后允许任意 bot 通信")

    # =========================================================================
    # [todo_bridge] 配置段 - 事务到 Todo 的桥接
    # =========================================================================
    @config_section("todo_bridge", title="Todo Bridge", tag="plugin")
    class TodoBridgeSection(SectionBase):
        """Bridge confirmed relay transactions into todo_plugin.

        当 relay 事务被 confirm（确认）后，通过事件总线将事务信息发布给
        todo_plugin，自动创建待办事项。
        """

        enabled: bool = Field(default=True, description="启用事务确认后的 todo_plugin 桥接")
        event_name: str = Field(default="bot_relay.todo_decided", description="relay todo 决策事件名")
        max_retries: int = Field(default=2, description="首次发布失败后的重试次数")
        retry_backoff_seconds: float = Field(default=0.1, description="桥接发布重试间隔秒数")
        fail_transaction_on_unavailable: bool = Field(default=True, description="todo 桥接不可用时是否阻止事务确认")

    # =========================================================================
    # [dynamic_social] 配置段 - 动态社交配额
    # =========================================================================
    @config_section("dynamic_social", title="Dynamic Social", tag="plugin")
    class DynamicSocialSection(SectionBase):
        """Runtime quotas for proactive social contact.

        控制 bot 主动向其他 bot 发起社交联系的频率限制。
        """

        enabled: bool = Field(default=True, description="启用动态社交联系配额")
        default_allow_all_bots: bool = Field(default=True, description="允许联系未配置为伙伴的 bot")
        impulse_enabled: bool = Field(default=True, description="允许'突发奇想'触发主动社交")
        event_triggers_enabled: bool = Field(default=True, description="允许事件触发主动社交")
        user_command_triggers_enabled: bool = Field(default=True, description="允许用户指令（/relay social）触发主动社交")
        default_max_per_day: int = Field(default=5, description="默认每目标每日主动社交上限")
        default_max_per_hour: int = Field(default=2, description="默认每目标每小时主动社交上限")
        default_cooldown_seconds: int = Field(default=300, description="默认每目标冷却秒数（5分钟）")

    # =========================================================================
    # [proactive] 配置段 - 自主发起通信
    # =========================================================================
    @config_section("proactive", title="Proactive", tag="plugin")
    class ProactiveSection(SectionBase):
        """Bot-owned autonomous relay initiation settings.

        控制 bot 是否以及如何在没有用户触发的情况下，自主向伙伴 bot 发起通信。
        包括 LLM 决策模型、消息生成模型、频率限制等。
        """

        enabled: bool = Field(default=False, description="启用 bot 自主发起通信（默认关闭）")
        check_interval_seconds: int = Field(default=300, description="主动决策检查间隔秒数（默认5分钟）")
        max_per_hour: int = Field(default=3, description="每目标每小时 proactive 发送上限")
        cooldown_seconds: int = Field(default=300, description="每目标 proactive 冷却秒数")
        transaction_enabled: bool = Field(default=False, description="允许自主发起事务请求（默认关闭，只允许 social）")
        social_enabled: bool = Field(default=True, description="允许自主发起社交消息")
        allow_offline_social: bool = Field(default=False, description="允许向离线目标发送社交消息")
        decision_model_task: str = Field(default="sub_actor", description="主动通信决策固定使用的模型任务名")
        message_model_task: str = Field(default="actor", description="主动消息生成固定使用的模型任务名")
        decision_retry_interval_seconds: float = Field(default=1.0, description="主动决策空回复重试间隔秒数")
        chat_hint_snapshot_items: int = Field(default=20, description="主动决策注入的最近聊天上下文条数")

    # =========================================================================
    # [group_reply_suppression] 配置段 - 群聊静默
    # =========================================================================
    @config_section("group_reply_suppression", title="Group Reply Suppression", tag="plugin")
    class GroupReplySuppressionSection(SectionBase):
        """Suppress local chatter replies to configured bots in group chats.

        在某些群聊中，我们只希望接收特定 bot 的消息但不回复它们。
        此配置段指定需要静默处理的 bot ID 列表。
        """

        enabled: bool = Field(default=True, description="启用群聊 bot 静默拦截")
        platforms: list[str] = Field(default_factory=lambda: ["qq"], description="启用静默拦截的平台列表")
        chat_types: list[str] = Field(default_factory=lambda: ["group"], description="启用静默拦截的聊天类型列表")
        blocked_bot_ids: list[str] = Field(default_factory=list, description="群聊中只接收不回复的 bot QQ 号列表")

    # =========================================================================
    # [social_quotas] 配置段 - 每目标社交配额覆盖
    # =========================================================================
    @config_section("social_quotas", title="Social Quotas", tag="plugin")
    class SocialQuotasSection(SectionBase):
        """Named per-target social quota overrides.

        为特定伙伴 bot 设置独立的社交频率限制。
        key 名称与 partners 中的 key 对应。
        """

        bot_b: SocialQuotaSection = Field(default_factory=SocialQuotaSection)

    # ── 配置段实例 ──────────────────────────────────────────────────────
    relay: RelaySection = Field(default_factory=RelaySection)
    partners: PartnersSection = Field(default_factory=PartnersSection)
    presence: PresenceSection = Field(default_factory=PresenceSection)
    todo_bridge: TodoBridgeSection = Field(default_factory=TodoBridgeSection)
    dynamic_social: DynamicSocialSection = Field(default_factory=DynamicSocialSection)
    proactive: ProactiveSection = Field(default_factory=ProactiveSection)
    group_reply_suppression: GroupReplySuppressionSection = Field(default_factory=GroupReplySuppressionSection)
    social_quotas: SocialQuotasSection = Field(default_factory=SocialQuotasSection)

    # =========================================================================
    # 配置查询辅助方法
    # =========================================================================

    def partner_by_id(self, bot_id: str) -> PartnerSection | None:
        """Return configured partner by ``bot_id``.

        遍历 partners 配置段中所有已配置的伙伴，按 bot_id 匹配。

        Args:
            bot_id: Security identity of the partner bot.

        Returns:
            Matching partner configuration, or ``None``.
        """

        for value in vars(self.partners).values():
            if isinstance(value, PartnerSection) and value.bot_id == bot_id:
                return value
        return None

    def first_allowed_partner(self) -> PartnerSection | None:
        """Return the first partner allowed by id.

        返回白名单中第一个在 partners 中配置的伙伴。
        用于在未指定目标时，自动选择默认通信对象。

        Returns:
            Partner configuration for the first allowlisted bot, or ``None``.
        """

        for bot_id in self.presence.allowed_partner_bots:
            partner = self.partner_by_id(bot_id)
            if partner is not None:
                return partner
        return None

    def social_quota_by_id(self, bot_id: str) -> SocialQuotaSection | None:
        """Return a quota override that matches a partner bot id.

        根据 bot_id 查找在 social_quotas 中配置的对应配额覆盖。
        匹配逻辑：先通过 bot_id 找到 partners 中的 key，再查找 social_quotas 中同名的 key。
        """

        for key, partner in vars(self.partners).items():
            if not isinstance(partner, PartnerSection) or partner.bot_id != bot_id:
                continue
            quota = getattr(self.social_quotas, key, None)
            return quota if isinstance(quota, SocialQuotaSection) else None
        return None
