# bot_private_relay

`bot_private_relay` 是 Neo-MoFox 的 bot 私有中继插件。它基于 MQTT 提供 `bot_relay` 传输、专用 chatter、事务工具、动态社交联系、主动联系调度，并可将已确认的 relay 事务投影到 `todo_plugin`。

## 发布包结构

- `plugin.py` - 插件入口和组件注册。
- `manifest.json` - 插件市场元数据和组件列表。
- `components/` - 按类型分组的 Neo-MoFox 框架组件。
- `runtime/` - 协议、会话、策略、在线状态和运行期状态辅助代码。
- `examples/` - 脱敏后的示例配置。

开发测试、MQTT smoke 脚本、缓存和本地运行快照不会包含在市场发布包中。
