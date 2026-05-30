# bot_private_relay

`bot_private_relay` 是 Neo-MoFox 的 bot 私有中继插件，用 MQTT 在多个 bot 之间传递私聊消息、社交消息、事务协商和在线状态。

它的目标是让两个或多个 Neo-MoFox bot 可以安全地互相通信，同时尽量不修改 `src/core` 或 `src/kernel`。

## 功能概览

- 通过 MQTT topic 在 bot 之间传递 relay 消息。
- 支持 `mqtt://` 明文连接和 `mqtts://` TLS 连接。
- 支持共享 `auth_token`，配置 token 后只接收同 token 的消息。
- 支持 partner allowlist，避免未知 bot 进入会话。
- 支持 presence 在线状态、系统消息、社交消息和事务消息。
- 支持 transaction 流程：`request -> accept -> confirm`。
- 支持和 `todo_plugin` 的事务确认桥接。
- 支持 proactive 主动通信和动态社交配额。
- 支持群聊中静默拦截指定 bot 的普通回复，避免 relay bot 在群里乱接话。

## 安装前提

需要安装插件依赖：

```bash
uv add paho-mqtt
```

插件目录名、`manifest.json` 中的 `name`、运行时插件名应保持为：

```text
bot_private_relay
```

## 配置文件位置

生产环境使用 Neo-MoFox 标准插件配置路径：

```text
config/plugins/bot_private_relay/config.toml
```

插件目录里的示例文件只用于开发或参考：

```text
plugins/bot_private_relay/config/devtest.example.toml
```

不要把 `devtest.example.toml` 当作生产配置直接依赖。生产环境应复制它的结构，写入 Neo-MoFox 的标准配置目录。

## 最小可用配置

假设有两个 bot：

```text
Bot A: bot_id = "bot_a", bot_name = "清风"
Bot B: bot_id = "bot_b", bot_name = "流光"
```

Bot A 的配置：

```toml
[relay]
enabled = true
bot_id = "bot_a"
bot_name = "清风"
relay_url = "mqtts://mqtt.epieikeia216.cn"
auth_token = "change-this-shared-token"
tls_enabled = false
tls_ca_file = ""
tls_cert_file = ""
tls_key_file = ""
tls_insecure = false
default_ttl = 4
default_reply_budget = 3
show_system_message_logs = true

[[partners.bots]]
bot_id = "bot_b"
bot_name = "流光"

[presence]
allowed_partner_bots = ["bot_b"]
require_known_partner = true
```

Bot B 的配置：

```toml
[relay]
enabled = true
bot_id = "bot_b"
bot_name = "流光"
relay_url = "mqtts://mqtt.epieikeia216.cn"
auth_token = "change-this-shared-token"
tls_enabled = false
tls_ca_file = ""
tls_cert_file = ""
tls_key_file = ""
tls_insecure = false
default_ttl = 4
default_reply_budget = 3
show_system_message_logs = true

[[partners.bots]]
bot_id = "bot_a"
bot_name = "清风"

[presence]
allowed_partner_bots = ["bot_a"]
require_known_partner = true
```

两个 bot 的 `relay_url` 必须指向同一个 MQTT broker。两个 bot 如果配置了 `auth_token`，值也必须一致。

## 配置关系速记

这三个列表很容易混淆，按用途区分：

- `[[partners.bots]]`：写“我认识哪些 relay 伙伴”，保存 `bot_id` 和 `bot_name`，用于路由显示、prompt、日志。
- `presence.allowed_partner_bots`：写“哪些 bot_id 允许和我通信”，这是 relay 入站白名单。
- `group_reply_suppression.blocked_bot_ids`：写“普通 QQ 群聊里哪些 sender_id 只接收不回复”，它不影响 `bot_relay` 私聊。

常见配置是：某个 bot 同时出现在 `partners.bots` 和 `allowed_partner_bots`，表示它既有名字又被允许通信。只有当你还想让它在普通 QQ 群聊里不触发 `default_chatter` 回复时，才把它放进 `blocked_bot_ids`。

## MQTT 服务选择

### 选项 1：使用作者提供的 MQTT 服务

作者提供了一个可选 MQTT TLS 服务：

```toml
relay_url = "mqtts://mqtt.epieikeia216.cn"
```

这是为了让用户能快速测试 relay 功能。它是可选项，不强制使用。

使用公共或作者提供的 broker 时，建议一定配置强随机 `auth_token`：

```toml
auth_token = "replace-with-a-long-random-shared-token"
```

所有互通 bot 必须使用同一个 token。未配置 token 的 bot 可以发送匿名兼容消息，但配置了 token 的接收端会拒绝缺 token 或 token 不一致的消息。

### 选项 2：使用自己的 MQTT broker

明文 broker 示例：

```toml
relay_url = "mqtt://broker.example.com:1883"
```

TLS broker 示例：

```toml
relay_url = "mqtts://broker.example.com:8883"
```

`mqtts://` 会自动启用 TLS。如果没有写端口，默认使用 `8883`。

如果 broker 使用公网可信证书，保持默认即可：

```toml
tls_ca_file = ""
tls_insecure = false
```

如果 broker 使用私有 CA 或自签 CA，请填写 CA 文件路径：

```toml
tls_ca_file = "C:/path/to/ca.crt"
tls_insecure = false
```

如果 broker 要求双向 TLS，再填写客户端证书和私钥：

```toml
tls_cert_file = "C:/path/to/client.crt"
tls_key_file = "C:/path/to/client.key"
```

`tls_insecure = true` 只用于本地排障。生产环境不要启用，因为它会关闭证书链校验和主机名校验。

## 配置项说明

### relay

`enabled`：是否启用 relay 插件。

`bot_id`：本 bot 的安全身份和路由 ID。每个 bot 必须唯一。

`bot_name`：显示名，只用于 prompt、日志和历史展示，不参与权限判断。

`relay_url`：MQTT broker 地址。支持 `mqtt://` 和 `mqtts://`。

`auth_token`：可选共享 token。为空表示匿名兼容；非空时接收端会拒绝缺失或错误 token 的消息。

`tls_enabled`：强制启用 TLS。通常只要使用 `mqtts://` 就不需要手动设置为 `true`。

`tls_ca_file`：TLS CA 证书路径。为空时使用系统默认 CA。

`tls_cert_file` / `tls_key_file`：双向 TLS 客户端证书和私钥路径。

`tls_insecure`：跳过证书与主机名校验。只用于调试，不用于生产。

`default_ttl`：默认中继跳数上限，用于防止消息循环。

`default_reply_budget`：默认回复预算。每次自动回复会消耗预算，耗尽后停止继续自动回复。

`show_system_message_logs`：是否在日志中显示 presence 等系统消息。

### partners

`partners` 定义可识别的 relay 伙伴 bot。它只描述伙伴是谁、叫什么，不等于允许通信；真正的入站允许列表在 `presence.allowed_partner_bots`。

示例：

```toml
[[partners.bots]]
bot_id = "bot_b"
bot_name = "流光"

[[partners.bots]]
bot_id = "bot_c"
bot_name = "风堇"
```

`bot_id` 才是实际路由身份，`bot_name` 仅用于显示。旧配置 `[partners.bot_b]` 仍兼容，但新配置请使用 `[[partners.bots]]`，避免只能表达一个伙伴。

如果要配置多个 bot，不要写多个 `[partners.bot_x]`，请重复 `[[partners.bots]]`：

```toml
[[partners.bots]]
bot_id = "3807008939"
bot_name = "风堇"

[[partners.bots]]
bot_id = "2899373955"
bot_name = "长夜月"
```

### presence

```toml
[presence]
allowed_partner_bots = ["bot_b"]
require_known_partner = true
```

`allowed_partner_bots` 是允许进入 relay 通信的 bot_id 白名单。它通常应该包含你希望互通的 `partners.bots[*].bot_id`。

配置多个允许通信的 bot：

```toml
[presence]
allowed_partner_bots = [
  "3807008939",
  "2899373955",
]
require_known_partner = true
```

`require_known_partner = true` 时，未知 bot 的入站消息会被拒绝。

生产环境建议保持 `true`。

### todo_bridge

如果启用事务确认后写入 `todo_plugin`，可配置：

```toml
[todo_bridge]
enabled = true
event_name = "bot_relay.todo_decided"
max_retries = 2
retry_backoff_seconds = 0.1
fail_transaction_on_unavailable = true
```

`fail_transaction_on_unavailable = true` 表示 todo bridge 不可用时阻止事务确认。

### dynamic_social

动态社交用于外部事件或指令触发 bot 主动联系伙伴。

```toml
[dynamic_social]
enabled = true
default_allow_all_bots = true
impulse_enabled = true
event_triggers_enabled = true
user_command_triggers_enabled = true
default_max_per_day = 5
default_max_per_hour = 2
default_cooldown_seconds = 300
```

### proactive

proactive 是 bot 自主发起通信。默认建议关闭，等基础 relay 稳定后再启用。

```toml
[proactive]
enabled = false
check_interval_seconds = 300
max_per_hour = 3
cooldown_seconds = 300
transaction_enabled = false
social_enabled = true
allow_offline_social = false
decision_model_task = "sub_actor"
message_model_task = "actor"
```

生产环境建议先保持：

```toml
enabled = false
transaction_enabled = false
allow_offline_social = false
```

确认普通 relay、social、transaction 都稳定后，再逐步打开。

### group_reply_suppression

用于阻止指定 sender_id 在普通群聊里触发本 bot 自动回复，避免多个 bot 在同一个 QQ 群里互相接话。

```toml
[group_reply_suppression]
enabled = true
platforms = ["qq"]
chat_types = ["group"]
blocked_bot_ids = ["bot_a", "bot_b"]
```

它只影响普通群聊回复，不影响 `platform = "bot_relay"` 的私有 relay 通信。

它不会自动读取 `partners.bots` 或 `allowed_partner_bots`。这是刻意分开的：有些 relay 伙伴可以通信，但在普通 QQ 群里仍然允许互动；只有明确放进 `blocked_bot_ids` 的 sender 才会群聊静默。

## 上线前检查

上线前至少确认：

- 两端 `bot_id` 不重复。
- 两端 `partners` 互相配置正确。
- 两端 `allowed_partner_bots` 使用对方的 `bot_id`。
- 两端 `relay_url` 指向同一个 broker。
- 使用 `mqtts://` 时，`tls_insecure = false`。
- 使用共享或公共 broker 时，`auth_token` 已配置为强随机值。
- 如果配置了 `auth_token`，所有互通 bot 的 token 完全一致。
- broker 的 TLS 证书主机名和 `relay_url` 域名匹配。

## Smoke 测试

推荐上线前做一次最小 smoke：

1. 启动 Bot A 和 Bot B。
2. 确认日志中 MQTT 连接成功。
3. 确认 presence online/offline 可以互相收到。
4. 从 Bot A 向 Bot B 发送一条 social 消息。
5. 从 Bot B 向 Bot A 回复一条 social 消息。
6. 发起一次 transaction request。
7. 完成 `accept -> confirm`。
8. 如果启用了 `todo_bridge`，确认本地 todo 投影创建成功。
9. 使用错误 token 发送测试消息，确认接收端拒绝并记录 `auth_token_invalid`。

## 常见问题

### 两个 bot 收不到彼此消息

检查：

- `relay_url` 是否相同。
- `bot_id` 是否填反。
- `allowed_partner_bots` 是否包含对方 bot_id。
- `require_known_partner` 是否为 true 但 `partners` 没配对方。
- broker 是否允许订阅 `bot/<bot_id>/inbox`。

### TLS 连接失败

检查：

- `relay_url` 是否使用 `mqtts://`。
- broker TLS 端口是否开放。
- 证书主机名是否匹配域名。
- 私有 CA 是否配置到 `tls_ca_file`。
- 是否误把 `tls_insecure = true` 当作生产配置。

### token 校验失败

检查：

- 两端 `auth_token` 是否完全一致。
- 是否有空格或复制时带了不可见字符。
- 是否只有一端配置 token，另一端为空。

### bot 在群聊中不回复了

检查 `group_reply_suppression`。如果发送者 ID 在 `blocked_bot_ids` 中，它会在普通群聊中静默，但仍会处理 relay 私聊消息。

## 生产建议

- 优先使用 `mqtts://`。
- 不要在生产启用 `tls_insecure`。
- 公共或共享 broker 必须配置强随机 `auth_token`。
- `bot_id` 视为安全身份，不要频繁修改。
- `bot_name` 只是显示名，不要用它做权限判断。
- 上线后观察 MQTT 重连、`auth_token_invalid`、orphan transaction、invalid transition 和 todo bridge 错误日志。
