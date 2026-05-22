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

    bot_id: str = Field(default="", description="Partner bot id used for routing")
    bot_name: str = Field(default="", description="Partner display name")


class SocialQuotaSection(SectionBase):
    """Per-target proactive social quota."""

    max_per_day: int = Field(default=5, description="Maximum proactive social contacts per day")
    max_per_hour: int = Field(default=2, description="Maximum proactive social contacts per hour")
    cooldown_seconds: int = Field(default=300, description="Cooldown between proactive social contacts")


class BotPrivateRelayConfig(BaseConfig):
    """Configuration for the bot private relay plugin.

    Production setup: edit ``config/plugins/bot_private_relay/config.toml``
    (created automatically by the framework on first load).  A minimal
    production TOML::

        [relay]
        bot_id = "223123"
        bot_name = "清风"
        relay_url = "mqtt://8.163.34.70:1883"

        [partners.bot_b]
        bot_id = "114514"
        bot_name = "流光"

        [presence]
        allowed_partner_bots = ["114514"]
    """

    config_name: ClassVar[str] = "config"
    config_description: ClassVar[str] = "Bot private relay configuration"

    @config_section("relay", title="Relay", tag="plugin")
    class RelaySection(SectionBase):
        """Relay identity and broker options."""

        enabled: bool = Field(default=True, description="Enable bot private relay")
        bot_id: str = Field(default="", description="This bot id; security-critical")
        bot_name: str = Field(default="", description="This bot display name")
        relay_url: str = Field(default="mqtt://localhost:1883", description="MQTT relay URL")
        auth_token: str = Field(default="", description="Optional auth token")
        default_ttl: int = Field(default=4, description="Default relay hop TTL")
        default_reply_budget: int = Field(default=3, description="Default request reply budget")

    @config_section("partners", title="Partners", tag="plugin")
    class PartnersSection(SectionBase):
        """Partner mapping for local tests and simple deployments."""

        bot_b: PartnerSection = Field(default_factory=PartnerSection)

    @config_section("presence", title="Presence", tag="plugin")
    class PresenceSection(SectionBase):
        """Presence and allowlist settings."""

        allowed_partner_bots: list[str] = Field(default_factory=list)
        require_known_partner: bool = Field(default=True)

    @config_section("todo_bridge", title="Todo Bridge", tag="plugin")
    class TodoBridgeSection(SectionBase):
        """Bridge confirmed relay transactions into todo_plugin."""

        enabled: bool = Field(default=True, description="Publish confirmed relay transactions to todo_plugin")
        event_name: str = Field(default="bot_relay.todo_decided", description="EventBus topic for relay todo decisions")
        max_retries: int = Field(default=2, description="Retry count after the first bridge publish attempt")
        retry_backoff_seconds: float = Field(default=0.1, description="Delay between bridge publish retries")
        fail_transaction_on_unavailable: bool = Field(default=True, description="Fail confirm if todo bridge is unavailable")

    @config_section("dynamic_social", title="Dynamic Social", tag="plugin")
    class DynamicSocialSection(SectionBase):
        """Runtime quotas for proactive social contact."""

        enabled: bool = Field(default=True, description="Enable proactive social contact quotas")
        default_allow_all_bots: bool = Field(default=True, description="Allow contacting bots not listed as partners")
        impulse_enabled: bool = Field(default=True, description="Allow impulse-triggered proactive social contact")
        event_triggers_enabled: bool = Field(default=True, description="Allow event-triggered proactive social contact")
        user_command_triggers_enabled: bool = Field(default=True, description="Allow owner command-triggered social contact")
        default_max_per_day: int = Field(default=5, description="Default daily proactive social quota per target bot")
        default_max_per_hour: int = Field(default=2, description="Default hourly proactive social quota per target bot")
        default_cooldown_seconds: int = Field(default=300, description="Default cooldown per target bot")

    @config_section("social_quotas", title="Social Quotas", tag="plugin")
    class SocialQuotasSection(SectionBase):
        """Named per-target social quota overrides."""

        bot_b: SocialQuotaSection = Field(default_factory=SocialQuotaSection)

    relay: RelaySection = Field(default_factory=RelaySection)
    partners: PartnersSection = Field(default_factory=PartnersSection)
    presence: PresenceSection = Field(default_factory=PresenceSection)
    todo_bridge: TodoBridgeSection = Field(default_factory=TodoBridgeSection)
    dynamic_social: DynamicSocialSection = Field(default_factory=DynamicSocialSection)
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
