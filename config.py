"""Bot private relay plugin configuration.

Production deployments use Neo-MoFox's standard plugin config loading flow:
``BaseConfig.get_default_path()`` resolves to
``config/plugins/bot_private_relay/config.toml`` and is read automatically by
``config_manager`` during plugin load.  Any hand-written config files kept
inside the plugin directory are dev/test only.
"""

from __future__ import annotations

from typing import ClassVar

from src.core.components.base.config import BaseConfig, Field, SectionBase, config_section


class PartnerSection(SectionBase):
    """Configured relay partner.

    ``bot_id`` is the only value used for routing and permission checks.
    ``bot_name`` is display-only for prompts, logs, and history rendering.
    """

    bot_id: str = Field(default="", description="伙伴 bot 的路由 ID")
    bot_name: str = Field(default="", description="伙伴 bot 的显示名称")


class SocialQuotaSection(SectionBase):
    """Per-target proactive social quota."""

    max_per_day: int = Field(default=5, description="每天最多主动社交联系次数")
    max_per_hour: int = Field(default=2, description="每小时最多主动社交联系次数")
    cooldown_seconds: int = Field(default=300, description="主动社交联系冷却秒数")


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

    @config_section("relay", title="Relay", tag="plugin")
    class RelaySection(SectionBase):
        """Relay identity and broker options."""

        enabled: bool = Field(default=True, description="启用 bot 私有中继")
        bot_id: str = Field(default="", description="本 bot 的安全身份 ID")
        bot_name: str = Field(default="", description="本 bot 的显示名称")
        relay_url: str = Field(default="mqtt://localhost:1883", description="MQTT 中继地址")
        auth_token: str = Field(default="", description="可选认证 token")
        tls_enabled: bool = Field(default=False, description="强制启用 MQTT TLS；relay_url 使用 mqtts:// 时会自动启用")
        tls_ca_file: str = Field(default="", description="TLS CA 证书路径；为空时使用系统默认 CA")
        tls_cert_file: str = Field(default="", description="可选客户端证书路径，用于双向 TLS")
        tls_key_file: str = Field(default="", description="可选客户端私钥路径，用于双向 TLS")
        tls_insecure: bool = Field(default=False, description="仅调试使用：跳过 TLS 证书与主机名校验，生产环境不要启用")
        default_ttl: int = Field(default=4, description="默认中继跳数 TTL")
        default_reply_budget: int = Field(default=3, description="默认请求回复预算")
        show_system_message_logs: bool = Field(default=True, description="是否在日志中展示系统消息入站")

    @config_section("partners", title="Partners", tag="plugin")
    class PartnersSection(SectionBase):
        """Partner mapping for local tests and simple deployments."""

        bot_b: PartnerSection = Field(default_factory=PartnerSection)

    @config_section("presence", title="Presence", tag="plugin")
    class PresenceSection(SectionBase):
        """Presence and allowlist settings."""

        allowed_partner_bots: list[str] = Field(default_factory=list, description="允许通信的伙伴 bot_id 列表")
        require_known_partner: bool = Field(default=True, description="是否要求对端必须在已知伙伴列表中")

    @config_section("todo_bridge", title="Todo Bridge", tag="plugin")
    class TodoBridgeSection(SectionBase):
        """Bridge confirmed relay transactions into todo_plugin."""

        enabled: bool = Field(default=True, description="启用事务确认后的 todo_plugin 桥接")
        event_name: str = Field(default="bot_relay.todo_decided", description="relay todo 决策事件名")
        max_retries: int = Field(default=2, description="首次发布失败后的重试次数")
        retry_backoff_seconds: float = Field(default=0.1, description="桥接发布重试间隔秒数")
        fail_transaction_on_unavailable: bool = Field(default=True, description="todo 桥接不可用时是否阻止事务确认")

    @config_section("dynamic_social", title="Dynamic Social", tag="plugin")
    class DynamicSocialSection(SectionBase):
        """Runtime quotas for proactive social contact."""

        enabled: bool = Field(default=True, description="启用动态社交联系配额")
        default_allow_all_bots: bool = Field(default=True, description="允许联系未配置为伙伴的 bot")
        impulse_enabled: bool = Field(default=True, description="允许突发奇想触发主动社交")
        event_triggers_enabled: bool = Field(default=True, description="允许事件触发主动社交")
        user_command_triggers_enabled: bool = Field(default=True, description="允许用户指令触发主动社交")
        default_max_per_day: int = Field(default=5, description="默认每目标每日主动社交上限")
        default_max_per_hour: int = Field(default=2, description="默认每目标每小时主动社交上限")
        default_cooldown_seconds: int = Field(default=300, description="默认每目标冷却秒数")

    @config_section("proactive", title="Proactive", tag="plugin")
    class ProactiveSection(SectionBase):
        """Bot-owned autonomous relay initiation settings."""

        enabled: bool = Field(default=False, description="启用 bot 自主发起通信")
        check_interval_seconds: int = Field(default=300, description="主动决策检查间隔秒数")
        max_per_hour: int = Field(default=3, description="每目标每小时 proactive 发送上限")
        cooldown_seconds: int = Field(default=300, description="每目标 proactive 冷却秒数")
        transaction_enabled: bool = Field(default=False, description="允许自主发起事务请求")
        social_enabled: bool = Field(default=True, description="允许自主发起社交消息")
        allow_offline_social: bool = Field(default=False, description="允许向离线目标发送社交消息")
        decision_model_task: str = Field(default="sub_actor", description="主动通信决策固定使用的模型任务名")
        message_model_task: str = Field(default="actor", description="主动消息生成固定使用的模型任务名")
        decision_retry_interval_seconds: float = Field(default=1.0, description="主动决策空回复重试间隔秒数")
        chat_hint_snapshot_items: int = Field(default=20, description="主动决策注入的最近聊天上下文条数")

    @config_section("group_reply_suppression", title="Group Reply Suppression", tag="plugin")
    class GroupReplySuppressionSection(SectionBase):
        """Suppress local chatter replies to configured bots in group chats."""

        enabled: bool = Field(default=True, description="启用群聊 bot 静默拦截")
        platforms: list[str] = Field(default_factory=lambda: ["qq"], description="启用静默拦截的平台列表")
        chat_types: list[str] = Field(default_factory=lambda: ["group"], description="启用静默拦截的聊天类型列表")
        blocked_bot_ids: list[str] = Field(default_factory=list, description="群聊中只接收不回复的 bot QQ 号列表")

    @config_section("social_quotas", title="Social Quotas", tag="plugin")
    class SocialQuotasSection(SectionBase):
        """Named per-target social quota overrides."""

        bot_b: SocialQuotaSection = Field(default_factory=SocialQuotaSection)

    relay: RelaySection = Field(default_factory=RelaySection)
    partners: PartnersSection = Field(default_factory=PartnersSection)
    presence: PresenceSection = Field(default_factory=PresenceSection)
    todo_bridge: TodoBridgeSection = Field(default_factory=TodoBridgeSection)
    dynamic_social: DynamicSocialSection = Field(default_factory=DynamicSocialSection)
    proactive: ProactiveSection = Field(default_factory=ProactiveSection)
    group_reply_suppression: GroupReplySuppressionSection = Field(default_factory=GroupReplySuppressionSection)
    social_quotas: SocialQuotasSection = Field(default_factory=SocialQuotasSection)

    def partner_by_id(self, bot_id: str) -> PartnerSection | None:
        """Return configured partner by ``bot_id``.

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

        Returns:
            Partner configuration for the first allowlisted bot, or ``None``.
        """

        for bot_id in self.presence.allowed_partner_bots:
            partner = self.partner_by_id(bot_id)
            if partner is not None:
                return partner
        return None

    def social_quota_by_id(self, bot_id: str) -> SocialQuotaSection | None:
        """Return a quota override that matches a partner bot id."""

        for key, partner in vars(self.partners).items():
            if not isinstance(partner, PartnerSection) or partner.bot_id != bot_id:
                continue
            quota = getattr(self.social_quotas, key, None)
            return quota if isinstance(quota, SocialQuotaSection) else None
        return None
