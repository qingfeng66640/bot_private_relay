# bot_private_relay 上线整合方向

> 本文档只描述**不修改 MoFox 主项目代码前提下**的上线整合方向。

## 1. 配置创建策略

- **当前测试环境**：允许在插件目录内使用 `config/devtest.example.toml` 作为临时配置样例。
- **正式上线**：必须切换为 Neo-MoFox 架构提供的标准配置创建/加载方法。
- **落地原则**：
  1. 不把 `devtest.example.toml` 当成生产配置方案。
  2. 不在 `src/core/` / `src/kernel/` 中补临时兼容逻辑。
  3. 如果上线前发现插件配置无法由现有架构正确创建，只记录为整合阻塞项，不私自改框架。

## 2. 目录与加载

- 当前 git 仓库目录名是 `bot_private_relay`。
- 运行时插件身份当前也使用 `bot_private_relay`。
- 传输平台标识继续使用 `bot_relay`。
- 上线时只要 `插件目录名 / manifest.name / plugin_name` 三者保持一致即可。

## 3. Broker 与网络

- MQTT broker 连接参数来自插件配置，不从主项目其他配置旁路读取。
- 上线前需完成：
  - broker 地址
  - 凭证/ACL
  - retain / will 策略确认
  - topic 命名冻结

## 4. 安全上线前检查

上线前至少要确认：

- `from_bot` / `to_bot` 强校验开启
- allowlist 已配置且真实可用
- `notify` 不自动回复
- debug export / inspect / stats 权限符合预期
- `bot_name` 未进入任何路由或权限判断

## 5. 推荐上线顺序

1. 单机本地 broker 联调
2. 两个 bot 的灰度联调
3. 只开 `notify` / `request` 事务流
4. 观察 loop guard / audit / export 输出
5. 再开放社交通道与 memory candidate

## 6. 仍未纳入本轮的整合项

以下项目故意未在本轮实现：

- 外部 todo/schedule 插件桥接
- 主项目级持久化接线
- 修改 MoFox 核心配置创建流程
- 改动 `src/core/` 或 `src/kernel/`

这些内容如果未来要做，应另开整合阶段，不与当前插件内实现混做。
