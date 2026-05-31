"""bot 私有中继插件的 MQTT 适配器。"""

# =============================================================================
# BotRelayAdapter - MQTT 通信适配器
# =============================================================================
# 这是插件的网络通信核心，负责 bot_private_relay 与 MQTT Broker 之间的
# 所有连接管理和消息收发。
#
# 核心职责：
# 1. MQTT 连接管理（连接/重连/断开）
# 2. TLS 安全配置（支持 mTLS）
# 3. 消息收发（入站解析 → MessageEnvelope / 出站 MessageEnvelope → MQTT）
# 4. 心跳维护（每 30 秒发布 presence 保持在线）
# 5. 身份认证（auth_token HMAC 比对）
# 6. 入站消息安全校验（白名单/消息去重/TTL）
# 7. 自动确认逻辑（inbound accept → 自动 confirm）
# 8. 会话状态同步（调用 SessionManager 更新状态机）
#
# MQTT Topic 结构：
# - bot/{bot_id}/inbox         → 消息收件箱
# - bot/presence/{bot_id}      → 在线状态（retained）
#
# 重连策略：
# - 初始延迟：10 秒
# - 最大延迟：120 秒
# - 指数退避：每次失败后延迟翻倍
# - 防重入：_reconnecting 标志防止重复重连
# =============================================================================

from __future__ import annotations

import asyncio
import hmac
import json
import ssl
from typing import Any
from urllib.parse import urlparse

from mofox_wire import MessageEnvelope

from src.app.plugin_system.base import BaseAdapter
from src.kernel.concurrency import get_task_manager
from src.kernel.logger import get_logger

from ...runtime import store
from ...runtime.envelope import RelayEnvelope
from ...runtime.policy import PolicyEngine
from ...runtime.presence import PresenceManager
from ...runtime.session import SessionManager
from ...runtime.system_handler import SystemChannelHandler
from ...runtime.todo_bridge import TodoBridge
from ..config import BotPrivateRelayConfig, PartnerSection

logger = get_logger("bot_private_relay_adapter")

# 认证 token 在 JSON payload 中的字段名
_AUTH_TOKEN_FIELD = "auth_token"


class BotRelayAdapter(BaseAdapter):
    """暴露 ``bot_relay`` 传输平台的适配器。

    向 Neo-MoFox 框架暴露 bot_relay 传输平台。
    """

    adapter_name = "bot_relay"
    adapter_version = "0.1.0"
    adapter_description = "Bot 私有中继 MQTT 适配器"
    platform = "bot_relay"

    # ── 常量 ──────────────────────────────────────────────────────────
    _HEARTBEAT_INTERVAL = 30      # 心跳间隔（秒）
    _RECONNECT_MIN_DELAY = 10     # 重连最小延迟（秒）
    _RECONNECT_MAX_DELAY = 120    # 重连最大延迟（秒）
    _KEEPALIVE = 20               # MQTT keepalive（秒），需小于 broker 空闲超时

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._mqtt_client: Any | None = None           # paho MQTT 客户端实例
        self._mqtt_task_info: Any | None = None          # MQTT 连接任务 info
        self._heartbeat_task_info: Any | None = None     # 心跳任务 info
        self._reconnect_task_info: Any | None = None     # 重连任务 info
        self._reconnecting: bool = False                 # 是否正在重连（防重入）
        self._event_loop: asyncio.AbstractEventLoop | None = None  # 捕获的事件循环
        self._session_manager = SessionManager()         # 会话管理器
        self._policy_engine = PolicyEngine()             # 策略引擎
        self._reconnect_delay = self._RECONNECT_MIN_DELAY  # 当前重连延迟

    # =========================================================================
    # 配置访问
    # =========================================================================

    @property
    def relay_config(self) -> BotPrivateRelayConfig:
        """返回类型化的插件配置。"""

        if not self.plugin or not isinstance(self.plugin.config, BotPrivateRelayConfig):
            raise RuntimeError("Bot private relay adapter requires BotPrivateRelayConfig")
        return self.plugin.config

    # =========================================================================
    # 生命周期：加载 / 卸载
    # =========================================================================

    async def on_adapter_loaded(self) -> None:
        """启动 MQTT 后台连接任务（通过 task_manager）。

        适配器加载时：启动 MQTT 连接循环（后台 daemon 任务）。
        如果配置禁用了 relay，则跳过。
        """

        if not self.relay_config.relay.enabled:
            logger.info("Bot private relay adapter disabled by config")
            return

        # ── 捕获事件循环 ──
        # paho-mqtt 的回调在非 asyncio 线程中执行，需要用 run_coroutine_threadsafe
        # 把异步操作调度回事件循环。
        self._event_loop = asyncio.get_running_loop()

        tm = get_task_manager()
        self._mqtt_task_info = tm.create_task(
            self._mqtt_connect_loop(),
            name="bot_private_relay_mqtt",
            daemon=True,
        )

    async def on_adapter_unloaded(self) -> None:
        """发布离线 presence 并停止 MQTT 后台任务。

        适配器卸载时：发布 offline 状态、取消所有后台任务、断开 MQTT。
        """

        # ── 发布离线状态 ──
        await self._publish_presence("offline")

        # ── 取消心跳和 MQTT 任务 ──
        for task_info in (self._mqtt_task_info, self._heartbeat_task_info):
            if task_info:
                get_task_manager().cancel_task(task_info.task_id)
        self._mqtt_task_info = None
        self._heartbeat_task_info = None

        # ── 停止 MQTT 客户端 ──
        if self._mqtt_client and hasattr(self._mqtt_client, "loop_stop"):
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
        self._mqtt_client = None

    # =========================================================================
    # 健康检查 / 重连接口
    # =========================================================================

    async def health_check(self) -> bool:
        """报告 MQTT 客户端健康状态，替代 BaseAdapter 的传输层健康检查。"""

        if self._mqtt_client is None:
            return False
        is_connected = getattr(self._mqtt_client, "is_connected", None)
        if callable(is_connected):
            return bool(is_connected()) or self._reconnecting
        return True

    async def reconnect(self) -> None:
        """让 MQTT disconnect 回调自行管理重连调度。

        重连由 paho 的 disconnect 回调自动管理，不需要外部触发。
        """

        logger.debug("MQTT reconnect is managed by paho disconnect callbacks")

    # =========================================================================
    # MQTT 连接生命周期
    # =========================================================================

    def _parse_broker_url(self) -> tuple[str, int, bool]:
        """从 relay_url 和配置中解析主机、端口和 TLS 模式。

        从配置的 relay_url 解析 MQTT Broker 的地址、端口和 TLS 模式。
        - mqtt:// → 1883 端口，无 TLS
        - mqtts:// → 8883 端口，启用 TLS
        """

        url = self.relay_config.relay.relay_url
        parsed = urlparse(url)
        host = parsed.hostname or "localhost"
        use_tls = parsed.scheme == "mqtts" or self.relay_config.relay.tls_enabled
        default_port = 8883 if use_tls else 1883
        port = parsed.port or default_port
        return host, port, use_tls

    def _build_tls_context(self) -> ssl.SSLContext:
        """从 relay TLS 配置构建 MQTT TLS 上下文。

        构建 SSL Context，支持：
        - CA 证书验证（可自定义 CA 文件）
        - 客户端证书（mTLS 双向认证）
        - insecure 模式（仅调试用，跳过证书验证）
        """

        config = self.relay_config.relay
        ca_file = config.tls_ca_file.strip() or None
        context = ssl.create_default_context(cafile=ca_file)

        # ── 客户端证书（mTLS） ──
        cert_file = config.tls_cert_file.strip()
        key_file = config.tls_key_file.strip() or None
        if cert_file:
            context.load_cert_chain(certfile=cert_file, keyfile=key_file)

        # ── insecure 模式（仅调试） ──
        if config.tls_insecure:
            logger.warning("MQTT TLS certificate verification is disabled by relay.tls_insecure")
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        return context

    def _configure_mqtt_tls(self, client: Any) -> None:
        """在 connect() 之前为 paho MQTT 客户端应用 TLS 设置。"""

        tls_set_context = getattr(client, "tls_set_context", None)
        if not callable(tls_set_context):
            raise RuntimeError("MQTT client does not support tls_set_context")
        tls_set_context(self._build_tls_context())

    async def _mqtt_connect_loop(self) -> None:
        """完整的 MQTT 连接 / 订阅 / presence / 心跳 / 重连循环。

        完整的 MQTT 连接流程：
        1. 导入 paho-mqtt（如果不可用则退出）
        2. 取消旧任务和客户端
        3. 创建新客户端（指定 client_id 和 mqtt v3.1.1 协议）
        4. 注册回调：on_connect / on_message / on_disconnect
        5. 设置遗嘱消息（will_set）：离线时自动发布 offline 状态
        6. 配置 TLS（如果需要）
        7. 连接 broker
        8. 启动事件循环（loop_start）
        9. 启动心跳任务
        """

        try:
            import paho.mqtt.client as mqtt
        except Exception as error:  # pragma: no cover
            logger.warning(f"当前环境中 paho-mqtt 不可用: {error}")
            return

        config = self.relay_config.relay
        broker_host, broker_port, use_tls = self._parse_broker_url()

        # ── 清理旧状态 ──
        self._cancel_heartbeat_task()
        self._stop_mqtt_client()

        # ── 创建 MQTT 客户端 ──
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,  # paho v2 API
            client_id=f"bot_relay_{config.bot_id}",                 # 唯一客户端 ID
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )
        client.on_connect = self._on_mqtt_connect
        client.on_message = self._on_mqtt_message_callback
        client.on_disconnect = self._on_mqtt_disconnect

        # ── 设置遗嘱消息（Will Message） ──
        # 当客户端异常断开时，broker 自动发布此消息
        # 这样伙伴 bot 能感知到我们的离线状态
        presence_mgr = PresenceManager(self.relay_config)
        will_envelope = presence_mgr.build_presence_envelope(status="offline")
        will_payload = json.dumps(self._payload_dict_for_envelope(will_envelope), ensure_ascii=False)
        client.will_set(
            f"bot/presence/{config.bot_id}",
            will_payload,
            qos=1,
            retain=True,  # retained 消息，确保新订阅者能看到最后的状态
        )

        # ── TLS 配置 ──
        if use_tls:
            self._configure_mqtt_tls(client)

        # ── 连接 Broker ──
        scheme = "mqtts" if use_tls else "mqtt"
        logger.info(
            f"Bot 私有中继 MQTT 正在连接 {scheme}://{broker_host}:{broker_port}"
        )
        try:
            client.connect(broker_host, broker_port, keepalive=self._KEEPALIVE)
        except Exception as exc:
            # 连接失败 → 指数退避重试
            logger.warning(f"MQTT 连接失败: {exc}; 将重试")
            self._reconnect_delay = min(
                self._reconnect_delay * 2, self._RECONNECT_MAX_DELAY
            )
            await asyncio.sleep(self._reconnect_delay)
            self._mqtt_task_info = get_task_manager().create_task(
                self._mqtt_connect_loop(),
                name="bot_private_relay_mqtt",
                daemon=True,
            )
            return

        # ── 连接成功 → 启动事件循环和心跳 ──
        client.loop_start()
        self._mqtt_client = client
        self._reconnect_delay = self._RECONNECT_MIN_DELAY  # 重置重连延迟

        tm = get_task_manager()
        self._heartbeat_task_info = tm.create_task(
            self._heartbeat_loop(client, config.bot_id),
            name="bot_private_relay_heartbeat",
            daemon=True,
        )

    # =========================================================================
    # MQTT 回调处理
    # =========================================================================

    def _on_mqtt_connect(
        self,
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        """回调：与 broker 建立连接（paho v2 API）。

        连接成功回调：
        1. 订阅自己的 inbox topic
        2. 订阅所有伙伴 bot 的 presence topic
        3. 发布自己的 online presence
        """

        # ── 检查连接结果 ──
        is_failure = getattr(reason_code, "is_failure", None)
        if callable(is_failure):
            ok = not is_failure()
        else:
            ok = reason_code == 0

        if ok:
            logger.info("Bot 私有中继 MQTT 已连接")
            config = self.relay_config.relay

            # 订阅自己的收件箱
            client.subscribe(f"bot/{config.bot_id}/inbox", qos=1)
            logger.info(f"已订阅 bot/{config.bot_id}/inbox")

            # 订阅所有伙伴的在线状态
            for partner_bot_id in self.relay_config.presence.allowed_partner_bots:
                client.subscribe(f"bot/presence/{partner_bot_id}", qos=1)
                logger.info(f"已订阅 bot/presence/{partner_bot_id}")

            # 发布自己的在线状态
            self._publish_presence_sync(client, config.bot_id, "online")
        else:
            logger.warning(f"MQTT 连接返回 reason_code: {reason_code}")

    def _on_mqtt_disconnect(
        self,
        client: Any,
        userdata: Any,
        disconnect_flags: Any = None,
        reason_code: Any = None,
        properties: Any = None,
    ) -> None:
        """回调：与 broker 失去连接（paho v2 API）。

        断开连接回调：
        - 防重入：_reconnecting 标志
        - 指数退避：延迟翻倍
        - run_coroutine_threadsafe：从 paho 线程调度到 asyncio 事件循环
        """

        logger.info(f"MQTT 已断开 (reason_code={reason_code})")

        # ── 防重入 ──
        if self._reconnecting:
            logger.debug("重连已在进行中; 跳过重复调度")
            return

        # ── 事件循环检查 ──
        if self._event_loop is None or self._event_loop.is_closed():
            logger.warning("断开连接回调触发时无事件循环; 无法重连")
            return

        self._reconnecting = True
        self._reconnect_delay = min(
            self._reconnect_delay * 2, self._RECONNECT_MAX_DELAY
        )

        # ── 调度重连到事件循环 ──
        asyncio.run_coroutine_threadsafe(
            self._mqtt_reconnect_delayed(),
            self._event_loop,
        )

    async def _mqtt_reconnect_delayed(self) -> None:
        """等待延迟后重试连接。完成后清除重连标志。"""

        try:
            await asyncio.sleep(self._reconnect_delay)
            self._mqtt_task_info = get_task_manager().create_task(
                self._mqtt_connect_loop(),
                name="bot_private_relay_mqtt",
                daemon=True,
            )
        finally:
            self._reconnecting = False
            self._reconnect_task_info = None

    def _on_mqtt_message_callback(self, client: Any, userdata: Any, msg: Any) -> None:
        """回调：从 broker 收到消息。

        paho 的回调在线程池中执行，不能直接调用 asyncio 方法。
        因此使用 run_coroutine_threadsafe 将消息处理调度到事件循环。
        """

        try:
            raw = msg.payload.decode("utf-8")
        except Exception as exc:
            logger.warning(f"MQTT 消息在 topic {msg.topic} 上解码失败: {exc}")
            return

        # ── 日志（presence 消息按配置决定是否显示） ──
        log_message = f"MQTT 入站消息 on {msg.topic} ({len(raw)} bytes); 正在分发到事件循环"
        if str(msg.topic).startswith("bot/presence/"):
            if self.relay_config.relay.show_system_message_logs:
                logger.info(log_message)
        else:
            logger.info(log_message)

        # ── 事件循环检查 ──
        if self._event_loop is None or self._event_loop.is_closed():
            logger.warning("收到 MQTT 消息但事件循环不可用; 丢弃")
            return

        # ── 调度到事件循环 ──
        asyncio.run_coroutine_threadsafe(
            self.on_platform_message(raw),
            self._event_loop,
        )

    # =========================================================================
    # Presence 辅助方法
    # =========================================================================

    async def _publish_presence(self, status: str) -> None:
        """发布 retained presence 消息（异步友好版本）。"""

        if self._mqtt_client is None:
            return
        presence_mgr = PresenceManager(self.relay_config)
        envelope = presence_mgr.build_presence_envelope(status=status)
        topic = self._topic_for_envelope(envelope)
        payload = json.dumps(self._payload_dict_for_envelope(envelope), ensure_ascii=False)
        publish = getattr(self._mqtt_client, "publish", None)
        if callable(publish):
            publish(topic, payload, qos=1, retain=True)

    def _publish_presence_sync(self, client: Any, bot_id: str, status: str) -> None:
        """从 MQTT 回调线程同步发布 presence。

        MQTT 回调线程是同步的，需要同步版本的 presence 发布。
        """

        envelope = RelayEnvelope(
            from_bot=bot_id,
            to_bot="*",
            channel="system",
            intent="presence_update",
            terminal=True,
            expect_reply=False,
            payload={"status": status},
        )
        payload = json.dumps(self._payload_dict_for_envelope(envelope), ensure_ascii=False)
        publish = getattr(client, "publish", None)
        if callable(publish):
            publish(f"bot/presence/{bot_id}", payload, qos=1, retain=True)

    async def _heartbeat_loop(self, client: Any, bot_id: str) -> None:
        """定期发布 presence 以保持在线状态。

        每 30 秒发布一次 online presence，保持在线状态。
        """

        while True:
            try:
                self._publish_presence_sync(client, bot_id, "online")
                await asyncio.sleep(self._HEARTBEAT_INTERVAL)
            except Exception:
                await asyncio.sleep(5)  # 出错时短暂等待

    # =========================================================================
    # Bot 信息
    # =========================================================================

    async def get_bot_info(self) -> dict[str, Any]:  # type: ignore[override]
        """返回本地 bot 身份信息，用于提示词显示和发送者填充。"""

        return {
            "bot_id": self.relay_config.relay.bot_id,
            "bot_name": self.relay_config.relay.bot_name,
            "platform": self.platform,
        }

    # =========================================================================
    # 出站消息处理
    # =========================================================================

    async def _send_platform_message(self, envelope: MessageEnvelope) -> None:  # type: ignore[override]
        """将 MessageEnvelope 转换为 RelayEnvelope 并通过 MQTT 发布。

        将框架的 MessageEnvelope 转换为 RelayEnvelope，然后通过 MQTT 发布。
        流程：
        1. 解析目标伙伴 bot
        2. SessionManager 构建出站 RelayEnvelope
        3. PolicyEngine 应用策略
        4. 验证信封
        5. MQTT 发布
        """

        partner = self._resolve_partner_from_message_envelope(envelope)
        relay_envelope = self._session_manager.build_outbound_envelope(
            message_envelope=envelope,
            from_bot=self.relay_config.relay.bot_id,
            from_bot_name=self.relay_config.relay.bot_name,
            to_bot=partner.bot_id,
            to_bot_name=partner.bot_name,
            default_ttl=self.relay_config.relay.default_ttl,
            default_reply_budget=self.relay_config.relay.default_reply_budget,
        )
        relay_envelope = self._policy_engine.apply_outbound(relay_envelope)
        relay_envelope.validate()
        await self.publish_relay_envelope(relay_envelope)

    async def publish_relay_envelope(self, envelope: RelayEnvelope) -> None:
        """通过当前 MQTT 客户端发布已验证的 relay 信封。"""

        if self._mqtt_client is None:
            logger.info("MQTT 客户端未连接; 跳过在当前环境中的实时发布")
            return
        payload = json.dumps(self._payload_dict_for_envelope(envelope), ensure_ascii=False)
        topic = self._topic_for_envelope(envelope)
        publish = getattr(self._mqtt_client, "publish", None)
        if callable(publish):
            publish(topic, payload, qos=1, retain=False)

    # =========================================================================
    # 入站消息处理（核心逻辑）
    # =========================================================================

    async def from_platform_message(self, raw: Any) -> MessageEnvelope | None:  # type: ignore[override]
        """将原始 relay 数据转换为 MessageEnvelope 或消费系统事件。

        入站消息处理的主入口。完整的处理流水线：
        1. 解析原始数据（支持 str/bytes/dict）
        2. 认证验证（auth_token HMAC 比对）
        3. 构建 RelayEnvelope 并 hop+1
        4. 目标验证（to_bot 是否匹配）
        5. 发送方白名单检查（is_allowed）
        6. 系统消息短路处理（SystemChannelHandler）
        7. 孤儿事务消息检查（无本地会话的事务后续消息）
        8. 入站事务会话同步
        9. 入站 accept 自动确认（auto_confirm_inbound_accept）
        10. Todo 投影发布
        11. 入站社交会话同步
        12. 构建 MessageEnvelope 返回给框架
        """

        # ── 1. 解析原始数据 ──
        raw_dict: dict[str, Any]
        if isinstance(raw, str):
            raw_dict = json.loads(raw)
        elif isinstance(raw, bytes):
            raw_dict = json.loads(raw.decode("utf-8"))
        elif isinstance(raw, dict):
            raw_dict = raw
        else:
            return None

        # ── 2. 认证验证 ──
        if not self._verify_inbound_auth(raw_dict):
            return None

        # ── 3. 去除认证字段（防止泄漏到 downstream） ──
        raw_dict = self._without_auth_fields(raw_dict)

        # ── 4. 构建和验证 RelayEnvelope ──
        relay_envelope = RelayEnvelope.from_dict(raw_dict)
        relay_envelope = relay_envelope.increment_hop()  # 跳数 +1
        relay_envelope.validate()

        # ── 5. 目标验证（不是发给我们的消息 → 忽略） ──
        presence_manager = PresenceManager(self.relay_config)
        if relay_envelope.to_bot not in {self.relay_config.relay.bot_id, "*"}:
            logger.warning(
                f"忽略发往其他目标 bot 的 relay 信封: {relay_envelope.to_bot}"
            )
            return None

        # ── 6. 发送方白名单检查 ──
        # system channel 不检查白名单（presence 更新等不受限制）
        if relay_envelope.channel != "system" and not presence_manager.is_allowed(relay_envelope.from_bot):
            logger.warning(
                "拒绝来自未知伙伴 bot 的 relay 信封: "
                f"from_bot={relay_envelope.from_bot}, conversation_id={relay_envelope.conversation_id}"
            )
            store.audit(
                "sender_not_allowed",
                from_bot=relay_envelope.from_bot,
                to_bot=relay_envelope.to_bot,
                channel=relay_envelope.channel,
                intent=relay_envelope.intent,
                conversation_id=relay_envelope.conversation_id,
            )
            # 发送错误回复给发送方
            await self._publish_sender_not_allowed_error(relay_envelope)
            return None

        # ── 7. 系统消息短路处理 ──
        system_handler = SystemChannelHandler(presence_manager)
        if system_handler.handle(relay_envelope):
            return None  # 系统消息被消费，不需要进入 LLM 流程

        # ── 8. 孤儿事务消息检查 ──
        # 如果没有本地会话记录，拒绝事务的后续消息（accept/confirm 等）
        if self._is_orphan_transaction_continuation(relay_envelope):
            logger.warning(
                "丢弃孤儿 relay 事务后续消息: "
                f"from_bot={relay_envelope.from_bot}, "
                f"conversation_id={relay_envelope.conversation_id}, intent={relay_envelope.intent}"
            )
            store.audit(
                "orphan_transaction_continuation",
                from_bot=relay_envelope.from_bot,
                to_bot=relay_envelope.to_bot,
                intent=relay_envelope.intent,
                conversation_id=relay_envelope.conversation_id,
            )
            return None

        # ── 9. 入站事务会话同步 ──
        transaction_session = self._session_manager.sync_inbound_transaction_session(relay_envelope)

        # ── 10. 入站 accept 自动确认 ──
        # 当收到对端的 accept 时，如果本地状态允许且是 allowed_responder，
        # 自动发送 confirm 完成事务。
        transaction_session = await self._auto_confirm_inbound_accept(relay_envelope, transaction_session)

        # ── 11. 同步信封中的会话状态 ──
        if transaction_session is not None:
            self._apply_session_state_to_envelope(relay_envelope, transaction_session)

        # ── 12. Todo 投影（入站 confirm） ──
        inbound_todo_result = await self._session_manager.publish_inbound_final_todo_decision(
            envelope=relay_envelope,
            local_bot_id=self.relay_config.relay.bot_id,
            config=self.relay_config,
        )
        if inbound_todo_result is not None:
            ok, status, result = inbound_todo_result
            logger.info(
                "入站 relay 最终决策 todo 投影已处理: "
                f"conversation_id={relay_envelope.conversation_id}, "
                f"owner_bot={self.relay_config.relay.bot_id}, "
                f"peer_bot_id={relay_envelope.from_bot}, "
                f"ok={ok}, status={status}, todo_uid={result.get('todo_uid', '')}"
            )

        # ── 13. 入站社交会话同步 ──
        self._session_manager.sync_inbound_social_session(relay_envelope)

        # ── 14. 构建 MessageEnvelope 返回给框架 ──
        return MessageEnvelope(
            direction="incoming",
            message_info={
                "platform": self.platform,
                "message_id": relay_envelope.message_id,
                "message_type": "message",
                "user_info": {
                    "platform": self.platform,
                    "user_id": relay_envelope.from_bot,
                    "user_nickname": relay_envelope.from_bot_name,
                },
                "extra": {
                    "bot_internal": True,
                    "relay_context": self._session_manager.relay_context_from_envelope(relay_envelope),
                    "relay_envelope": relay_envelope.to_dict(),
                },
            },
            message_segment=[
                {
                    "type": "text",
                    "data": relay_envelope.text,
                }
            ],
            raw_message=raw_dict,
        )

    @staticmethod
    def _is_orphan_transaction_continuation(envelope: RelayEnvelope) -> bool:
        """拒绝没有本地会话的事务后续消息。

        判断是否为孤儿事务消息：事务 channel 且 intent 不是 notify/request/invite，
        但本地没有对应的会话记录。
        """

        if envelope.channel != "transaction" or envelope.intent in {"notify", "request", "invite"}:
            return False
        return store.get_session(envelope.conversation_id) is None

    async def _publish_sender_not_allowed_error(self, inbound: RelayEnvelope) -> None:
        """为被拒绝的非系统消息发送显式协议错误。

        当入站消息的发送方不在白名单中时，向发送方回复一个错误信封。
        """

        error_envelope = RelayEnvelope(
            conversation_id=inbound.conversation_id,
            trace_id=inbound.trace_id,
            parent_message_id=inbound.message_id,
            from_bot=self.relay_config.relay.bot_id,
            from_bot_name=self.relay_config.relay.bot_name,
            to_bot=inbound.from_bot,
            to_bot_name=inbound.from_bot_name,
            channel="system",
            intent="error",
            expect_reply=False,
            reply_budget=0,
            ttl=self.relay_config.relay.default_ttl,
            terminal=True,
            allowed_responders=[],
            no_relay=True,
            payload={
                "code": "sender_not_allowed",
                "text": "发送方 bot 未被允许联系此 relay 端点。",
                "rejected_channel": inbound.channel,
                "rejected_intent": inbound.intent,
            },
        )
        try:
            error_envelope.validate()
            await self.publish_relay_envelope(error_envelope)
        except Exception as exc:
            logger.error(
                "发送 sender-not-allowed relay 错误失败: "
                f"from_bot={inbound.from_bot}, conversation_id={inbound.conversation_id}, error={exc}",
                exc_info=True,
            )

    # =========================================================================
    # 入站 accept 自动确认
    # =========================================================================

    async def _auto_confirm_inbound_accept(
        self,
        envelope: RelayEnvelope,
        session: store.RelaySession | None,
    ) -> store.RelaySession | None:
        """仅在本地投影成功后确认入站 accept。

        当收到对端的 accept 消息时，如果满足以下条件，自动发送 confirm：
        1. channel=transaction, intent=accept
        2. 本地会话存在，state=accepted，非 terminal
        3. 本地 bot 在 allowed_responders 中
        4. 六项硬校验通过
        5. Todo Bridge 发布成功
        6. 本地状态更新成功

        这是协议设计的关键优化：减少一轮往返。
        正常流程：A request → B accept → A confirm → closed
        自动确认后：A request → B accept → (auto confirm) → closed
        """

        local_bot_id = self.relay_config.relay.bot_id
        if envelope.channel != "transaction" or envelope.intent != "accept":
            return session
        if session is None or session.state != "accepted" or session.terminal:
            return session
        if local_bot_id not in session.allowed_responders:
            return session

        # ── 六项硬校验 ──
        ok, code, checked_session = self._session_manager.validate_transaction_action(
            conversation_id=envelope.conversation_id,
            action="confirm",
            caller_bot=local_bot_id,
            payload_complete=bool(envelope.conversation_id),
        )
        if not ok or checked_session is None:
            logger.warning(
                "入站 accept 自动确认被校验拒绝: "
                f"conversation_id={envelope.conversation_id}, status={code}"
            )
            return session

        # ── 检查事务记录 ──
        record = store.TRANSACTION_LOG.get(envelope.conversation_id)
        if record is None:
            logger.warning(
                "入站 accept 自动确认已跳过: 事务记录缺失, "
                f"conversation_id={envelope.conversation_id}"
            )
            return session

        # ── 构建 confirm 信封 ──
        confirm_envelope = self._build_auto_confirm_envelope(envelope)
        try:
            confirm_envelope.validate()
        except Exception as exc:
            logger.error(
                "入站 accept 自动确认信封无效; 不发布 confirm: "
                f"conversation_id={envelope.conversation_id}, error={exc}",
                exc_info=True,
            )
            return session

        # ── Todo Bridge 桥接 ──
        bridge_ok, bridge_status, bridge_result = await TodoBridge(self.relay_config).publish_final_decision(
            record=record,
            final_intent="confirm",
            owner_bot=local_bot_id,
            peer_bot_id=envelope.from_bot,
        )
        if not bridge_ok:
            logger.warning(
                "入站 accept 自动确认被 todo bridge 拒绝; 不发布 confirm: "
                f"conversation_id={envelope.conversation_id}, status={bridge_status}, "
                f"todo_uid={bridge_result.get('todo_uid', '')}"
            )
            return session

        # ── 推进本地状态 ──
        try:
            confirmed_session = self._session_manager.apply_transaction_action(
                conversation_id=envelope.conversation_id,
                action="confirm",
                caller_bot=local_bot_id,
            )
        except Exception as exc:
            logger.error(
                "入站 accept 自动确认在应用本地状态时失败; 不发布 confirm: "
                f"conversation_id={envelope.conversation_id}, error={exc}",
                exc_info=True,
            )
            return session

        # ── 发布 confirm ──
        try:
            await self.publish_relay_envelope(confirm_envelope)
        except Exception as exc:
            logger.error(
                "入站 accept 自动确认在本地确认后发布失败: "
                f"conversation_id={envelope.conversation_id}, error={exc}",
                exc_info=True,
            )
        else:
            logger.info(
                "入站 accept 自动确认已发布: "
                f"conversation_id={envelope.conversation_id}, peer_bot_id={envelope.from_bot}, "
                f"todo_bridge_status={bridge_status}"
            )
        return confirmed_session

    def _build_auto_confirm_envelope(self, inbound: RelayEnvelope) -> RelayEnvelope:
        """为已接受的事务构建出站 confirm 信封。"""

        return RelayEnvelope(
            conversation_id=inbound.conversation_id,
            trace_id=inbound.trace_id,
            parent_message_id=inbound.message_id,
            from_bot=self.relay_config.relay.bot_id,
            from_bot_name=self.relay_config.relay.bot_name,
            to_bot=inbound.from_bot,
            to_bot_name=inbound.from_bot_name,
            channel="transaction",
            intent="confirm",
            expect_reply=False,
            reply_budget=0,
            ttl=self.relay_config.relay.default_ttl,
            terminal=True,
            allowed_responders=[],
            state="closed",
            payload={"text": "已确认当前事务。"},
        )

    # =========================================================================
    # 认证与安全
    # =========================================================================

    def _payload_dict_for_envelope(self, envelope: RelayEnvelope) -> dict[str, Any]:
        """Return outbound payload data with optional transport auth.

        序列化信封并附加认证 token。
        """

        data = envelope.to_dict()
        token = self.relay_config.relay.auth_token.strip()
        if token:
            data[_AUTH_TOKEN_FIELD] = token
        return data

    def _verify_inbound_auth(self, raw_dict: dict[str, Any]) -> bool:
        """Verify optional transport auth before processing an envelope.

        使用 hmac.compare_digest 进行常量时间的安全比对。
        防止时序攻击（timing attack）。
        """

        expected = self.relay_config.relay.auth_token.strip()
        if not expected:
            return True  # 未配置 token → 不校验

        supplied = raw_dict.get(_AUTH_TOKEN_FIELD)
        supplied_token = supplied if isinstance(supplied, str) else ""
        if hmac.compare_digest(supplied_token, expected):
            return True

        reason_code = "missing_token" if not supplied_token else "invalid_token"
        logger.warning(
            "Rejecting relay envelope with invalid auth token: "
            f"from_bot={raw_dict.get('from_bot', '')}, "
            f"to_bot={raw_dict.get('to_bot', '')}, "
            f"channel={raw_dict.get('channel', '')}, "
            f"intent={raw_dict.get('intent', '')}, "
            f"conversation_id={raw_dict.get('conversation_id', '')}, "
            f"reason={reason_code}"
        )
        store.audit(
            "auth_token_invalid",
            from_bot=raw_dict.get("from_bot", ""),
            to_bot=raw_dict.get("to_bot", ""),
            channel=raw_dict.get("channel", ""),
            intent=raw_dict.get("intent", ""),
            conversation_id=raw_dict.get("conversation_id", ""),
            reason_code=reason_code,
        )
        return False

    @staticmethod
    def _without_auth_fields(raw_dict: dict[str, Any]) -> dict[str, Any]:
        """Remove transport auth fields before storing message metadata.

        去除认证字段，防止泄漏到 downstream 组件。
        """

        sanitized = dict(raw_dict)
        sanitized.pop(_AUTH_TOKEN_FIELD, None)
        return sanitized

    @staticmethod
    def _apply_session_state_to_envelope(envelope: RelayEnvelope, session: store.RelaySession) -> None:
        """Reflect locally applied session state in downstream relay_context.

        将本地会话状态同步到信封中，确保 downstream 的 relay_context 准确。
        """

        envelope.state = session.state
        envelope.terminal = session.terminal
        envelope.expect_reply = session.expect_reply
        envelope.reply_budget = session.reply_budget
        envelope.allowed_responders = list(session.allowed_responders)

    # =========================================================================
    # 路由与 Topic
    # =========================================================================

    def _resolve_partner_from_message_envelope(self, envelope: MessageEnvelope) -> PartnerSection:
        """Resolve the routing partner from envelope metadata.

        从 MessageEnvelope 的 relay_context 中提取 peer_bot_id，
        再查找对应的伙伴配置。如果找不到，使用默认的第一个允许伙伴。
        """

        message_info = envelope.get("message_info") if isinstance(envelope, dict) else None
        extra = message_info.get("extra") if isinstance(message_info, dict) else None
        relay_context = extra.get("relay_context") if isinstance(extra, dict) else None
        peer_bot_id = relay_context.get("peer_bot_id") if isinstance(relay_context, dict) else None

        if isinstance(peer_bot_id, str) and peer_bot_id:
            partner = self.relay_config.partner_by_id(peer_bot_id)
            if partner is not None:
                return partner

        partner = self.relay_config.first_allowed_partner()
        if partner is None or not partner.bot_id:
            raise ValueError("No allowed relay partner configured")
        return partner

    def _topic_for_envelope(self, envelope: RelayEnvelope) -> str:
        """Return MQTT topic for a relay envelope.

        Topic 规则：
        - presence_update → bot/presence/{from_bot}
        - 其他消息 → bot/{to_bot}/inbox
        """

        if envelope.channel == "system" and envelope.intent == "presence_update":
            return f"bot/presence/{envelope.from_bot}"
        return f"bot/{envelope.to_bot}/inbox"

    # =========================================================================
    # 清理辅助方法
    # =========================================================================

    def _cancel_heartbeat_task(self) -> None:
        """Cancel the current heartbeat task if one is registered."""

        if self._heartbeat_task_info:
            get_task_manager().cancel_task(self._heartbeat_task_info.task_id)
            self._heartbeat_task_info = None

    def _stop_mqtt_client(self) -> None:
        """Stop the existing paho client without publishing presence."""

        if self._mqtt_client is None:
            return
        loop_stop = getattr(self._mqtt_client, "loop_stop", None)
        if callable(loop_stop):
            loop_stop()
        disconnect = getattr(self._mqtt_client, "disconnect", None)
        if callable(disconnect):
            disconnect()
        self._mqtt_client = None
