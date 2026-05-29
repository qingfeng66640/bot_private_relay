"""Bot private relay plugin package.

The runtime plugin identity is ``bot_private_relay`` while the transport
platform exposed to Neo-MoFox is ``bot_relay``.

Keep package import side-effect free so plugin registration only happens when
``plugin.py`` is imported intentionally by the framework or tests.
"""

# =============================================================================
# bot_private_relay 插件包
# =============================================================================
# 功能：实现 Bot 与 Bot 之间的私有中继通信系统
# 运行时插件标识：bot_private_relay
# 对外暴露的传输平台：bot_relay
# 通信协议：基于 MQTT，使用 RelayEnvelope 作为协议信封
#
# 注意：此 __init__.py 不产生导入副作用，插件注册仅在框架或测试
# 主动导入 plugin.py 时发生。
# =============================================================================

__all__: list[str] = []
