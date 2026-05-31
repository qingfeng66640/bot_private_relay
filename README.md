# bot_private_relay

`bot_private_relay` 是 Neo-MoFox 的 bot 私有中继插件。它通过 MQTT 在多个 bot 之间传递私聊消息、社交消息、事务协商消息和在线状态。

## 功能

- 通过 MQTT topic 在 bot 之间传递 relay 消息。
- 支持 `mqtt://` 明文连接和 `mqtts://` TLS 连接。
- 支持共享 `auth_token`，配置后只接收携带相同 token 的消息。
- 支持伙伴白名单，限制允许进入 relay 会话的 bot。
- 支持 presence 在线状态、system 消息、social 消息和 transaction 消息。
- 支持事务流程，例如 `request -> accept -> confirm`。
- 支持将事务确认结果桥接到 `todo_plugin`。
- 支持主动社交通信和按目标 bot 的发送配额。
- 支持在普通群聊中静默指定 sender，避免多个 bot 在同一群聊中互相触发回复。

## 依赖

插件需要安装 MQTT 客户端依赖：

```bash
uv add paho-mqtt
```

插件目录名、`manifest.json` 中的 `name`、运行时插件名应保持一致：

```text
bot_private_relay
```

## 配置位置

Neo-MoFox 会从标准插件配置路径加载配置：

```text
config/plugins/bot_private_relay/config.toml
```

插件目录中的配置文件仅作为结构参考，不应作为运行环境的实际配置来源。

## 最小配置

以下示例展示两个 bot 互相通信所需的核心配置。示例中的 bot ID、显示名、broker 地址和 token 都应替换为实际部署值。

Bot Alpha：

```toml
[relay]
enabled = true
bot_id = "bot_alpha"
bot_name = "Bot Alpha"
relay_url = "mqtts://broker.example.com:8883"
auth_token = "replace-with-a-long-random-shared-token"
tls_enabled = false
tls_ca_file = ""
tls_cert_file = ""
tls_key_file = ""
tls_insecure = false
default_ttl = 4
default_reply_budget = 3
show_system_message_logs = true

[[partners.bots]]
bot_id = "bot_beta"
bot_name = "Bot Beta"

[presence]
allowed_partner_bots = ["bot_beta"]
require_known_partner = true
```

Bot Beta：

```toml
[relay]
enabled = true
bot_id = "bot_beta"
bot_name = "Bot Beta"
relay_url = "mqtts://broker.example.com:8883"
auth_token = "replace-with-a-long-random-shared-token"
tls_enabled = false
tls_ca_file = ""
tls_cert_file = ""
tls_key_file = ""
tls_insecure = false
default_ttl = 4
default_reply_budget = 3
show_system_message_logs = true

[[partners.bots]]
bot_id = "bot_alpha"
bot_name = "Bot Alpha"

[presence]
allowed_partner_bots = ["bot_alpha"]
require_known_partner = true
```

互通的 bot 必须使用同一个 `relay_url`。如果配置了 `auth_token`，互通双方也必须使用同一个 token。

## 核心配置

### relay

`enabled`：是否启用 relay 插件。

`bot_id`：本 bot 的路由身份。每个 bot 必须唯一。

`bot_name`：显示名，只用于 prompt、日志和历史展示，不参与权限判断。

`relay_url`：MQTT broker 地址。支持 `mqtt://` 和 `mqtts://`。

`auth_token`：可选共享 token。为空表示匿名兼容；非空时接收端会拒绝缺失或错误 token 的消息。

`tls_enabled`：强制启用 TLS。通常使用 `mqtts://` 即可自动启用 TLS。

`tls_ca_file`：TLS CA 证书路径。为空时使用系统默认 CA。

`tls_cert_file` / `tls_key_file`：双向 TLS 客户端证书和私钥路径。

`tls_insecure`：跳过证书与主机名校验。仅用于受控诊断场景。

`default_ttl`：默认中继跳数上限，用于防止消息循环。

`default_reply_budget`：默认回复预算。每次自动回复会消耗预算，耗尽后停止继续自动回复。

`show_system_message_logs`：是否在日志中显示 presence 等系统消息。

### partners

`partners` 定义可识别的 relay 伙伴 bot。它保存 `bot_id` 和 `bot_name`，用于路由显示、prompt 和日志。

```toml
[[partners.bots]]
bot_id = "bot_beta"
bot_name = "Bot Beta"

[[partners.bots]]
bot_id = "bot_gamma"
bot_name = "Bot Gamma"
```

`bot_id` 是实际路由身份，`bot_name` 仅用于显示。多个伙伴请重复使用 `[[partners.bots]]`。

### presence

`presence.allowed_partner_bots` 是允许进入 relay 通信的 bot ID 白名单。它通常应包含需要互通的 `partners.bots[*].bot_id`。

```toml
[presence]
allowed_partner_bots = ["bot_beta", "bot_gamma"]
require_known_partner = true
```

`require_known_partner = true` 时，未知 bot 的入站消息会被拒绝。

### todo_bridge

`todo_bridge` 用于在事务确认后向 `todo_plugin` 发布决策事件。

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

`dynamic_social` 用于外部事件或指令触发 bot 主动联系伙伴，并提供发送配额控制。

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

`proactive` 用于 bot 自主发起通信。默认关闭。

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

### group_reply_suppression

`group_reply_suppression` 用于阻止指定 sender 在普通群聊里触发本 bot 自动回复。

```toml
[group_reply_suppression]
enabled = true
platforms = ["qq"]
chat_types = ["group"]
blocked_bot_ids = ["bot_alpha", "bot_beta"]
```

它只影响普通群聊回复，不影响 `platform = "bot_relay"` 的私有 relay 通信。

## MQTT Broker

明文 broker 示例：

```toml
relay_url = "mqtt://broker.example.com:1883"
```

TLS broker 示例：

```toml
relay_url = "mqtts://broker.example.com:8883"
```

作者提供的可选 MQTT TLS broke 服务：

```toml
relay_url = "mqtts://mqtt.epieikeia216.cn"
```

使用公共或共享 broker 时，应配置强随机 `auth_token`。

`mqtts://` 会自动启用 TLS。如果未写端口，默认使用 `8883`。

如果 broker 使用公网可信证书，保持默认即可：

```toml
tls_ca_file = ""
tls_insecure = false
```

如果 broker 使用私有 CA，请填写 CA 文件路径：

```toml
tls_ca_file = "C:/path/to/ca.crt"
tls_insecure = false
```

如果 broker 要求双向 TLS，请填写客户端证书和私钥：

```toml
tls_cert_file = "C:/path/to/client.crt"
tls_key_file = "C:/path/to/client.key"
```

## 安全说明

- 优先使用 `mqtts://`。
- 公共或共享 broker 应配置强随机 `auth_token`。
- `tls_insecure = true` 会关闭证书链校验和主机名校验，仅适合受控诊断场景。
- `bot_id` 是协议身份，不应频繁修改。
- `bot_name` 是显示名，不应用作权限判断。

## 常见问题

### 两个 bot 收不到彼此消息

检查：

- `relay_url` 是否相同。
- `bot_id` 是否填反。
- `allowed_partner_bots` 是否包含对方 bot ID。
- `require_known_partner` 是否为 `true` 但 `partners` 没有配置对方。
- broker 是否允许订阅 `bot/<bot_id>/inbox`。

### TLS 连接失败

检查：

- `relay_url` 是否使用 `mqtts://`。
- broker TLS 端口是否开放。
- 证书主机名是否匹配域名。
- 私有 CA 是否配置到 `tls_ca_file`。
- `tls_insecure` 是否符合当前运行环境的安全要求。

### token 校验失败

检查：

- 两端 `auth_token` 是否完全一致。
- token 是否包含复制时带入的空格或不可见字符。
- 是否只有一端配置 token，另一端为空。

### bot 在群聊中不回复

检查 `group_reply_suppression`。如果发送者 ID 在 `blocked_bot_ids` 中，它会在普通群聊中静默，但仍会处理 relay 私聊消息。
