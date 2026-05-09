"""Bot private relay plugin configuration.

Production deployments must use Neo-MoFox's standard configuration creation and
loading flow. Any hand-written config files used by tests are dev/test only.
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


class BotPrivateRelayConfig(BaseConfig):
    """Configuration for the bot private relay plugin."""

    config_name: ClassVar[str] = "config"
    config_description: ClassVar[str] = "Bot private relay configuration"

    @config_section("relay", title="Relay", tag="plugin", order=0)
    class RelaySection(SectionBase):
        """Relay identity and broker options."""

        enabled: bool = Field(default=True, description="Enable bot private relay")
        bot_id: str = Field(default="", description="This bot id; security-critical")
        bot_name: str = Field(default="", description="This bot display name")
        relay_url: str = Field(default="mqtt://localhost:1883", description="MQTT relay URL")
        auth_token: str = Field(default="", description="Optional auth token")
        default_ttl: int = Field(default=4, description="Default relay hop TTL")
        default_reply_budget: int = Field(default=3, description="Default request reply budget")

    @config_section("partners", title="Partners", tag="plugin", order=1)
    class PartnersSection(SectionBase):
        """Partner mapping for local tests and simple deployments."""

        bot_b: PartnerSection = Field(default_factory=PartnerSection)

    @config_section("presence", title="Presence", tag="plugin", order=2)
    class PresenceSection(SectionBase):
        """Presence and allowlist settings."""

        allowed_partner_bots: list[str] = Field(default_factory=list)
        require_known_partner: bool = Field(default=True)

    relay: RelaySection = Field(default_factory=RelaySection)
    partners: PartnersSection = Field(default_factory=PartnersSection)
    presence: PresenceSection = Field(default_factory=PresenceSection)

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
