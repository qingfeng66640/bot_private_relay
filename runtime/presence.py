"""在线状态追踪和白名单校验。"""

# =============================================================================
# PresenceManager - 在线状态管理
# =============================================================================
# 管理伙伴 bot 的在线状态追踪和白名单校验。
#
# 核心功能：
# 1. is_allowed()   - 白名单校验：检查 bot_id 是否在允许通信的列表中
# 2. update_from_envelope() - 从系统信封中更新伙伴的在线状态
# 3. build_presence_envelope() - 构建本机的在线状态信封（用于心跳发布）
#
# 状态数据存储在 store.PRESENCE_TABLE 中（模块级全局状态）。
# =============================================================================

from __future__ import annotations

import time

from . import store
from .envelope import RelayEnvelope
from ..components.config import BotPrivateRelayConfig


class PresenceManager:
    """使用模块级状态管理伙伴在线状态。"""

    def __init__(self, config: BotPrivateRelayConfig) -> None:
        self.config = config

    def is_allowed(self, bot_id: str) -> bool:
        """检查 bot_id 是否允许通信。

        白名单校验逻辑：
        - 如果配置不要求已知伙伴（require_known_partner=False），任何 bot 都允许
        - 否则，检查 bot_id 是否在 allowed_partner_bots 列表中
        """

        if not self.config.presence.require_known_partner:
            return True
        return bot_id in self.config.presence.allowed_partner_bots

    def update_from_envelope(self, envelope: RelayEnvelope) -> None:
        """从系统信封中更新在线状态。

        当收到 presence_update 系统消息时，更新对应伙伴的在线状态记录。
        记录内容包括：bot_id、bot_name、状态（online/offline）、最后在线时间。
        """

        status = str(envelope.payload.get("status") or "online")
        store.upsert_presence(
            store.PresenceRecord(
                bot_id=envelope.from_bot,
                bot_name=envelope.from_bot_name,
                status=status,
                last_seen=time.time(),
                is_known_partner=self.is_allowed(envelope.from_bot),
            )
        )

    def build_presence_envelope(self, *, status: str) -> RelayEnvelope:
        """构建本机在线状态信封。

        构建本机在线状态的信封，用于：
        1. MQTT 连接时的上线通知（status="online"）
        2. MQTT 断开时的遗嘱消息（status="offline"，通过 MQTT will_set 发布）
        3. 周期性心跳保持（status="online"，每 30 秒发布一次）

        使用 to_bot="*" 表示广播给所有订阅者。
        """

        return RelayEnvelope(
            from_bot=self.config.relay.bot_id,
            from_bot_name=self.config.relay.bot_name,
            to_bot="*",
            to_bot_name="*",
            channel="system",
            intent="presence_update",
            terminal=True,
            expect_reply=False,
            payload={"status": status},
        )
