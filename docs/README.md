# bot_private_relay

Bot-to-bot private relay plugin for Neo-MoFox.

The plugin exposes an MQTT-backed `bot_relay` transport, relay-only chatter,
transaction tools, dynamic social contact, proactive contact scheduling, and
optional todo projection for confirmed relay transactions.

## Published Package Layout

- `plugin.py` - plugin entry point and component registration.
- `manifest.json` - market metadata and component list.
- `components/` - Neo-MoFox framework components grouped by type.
- `runtime/` - protocol, session, policy, presence, and state helpers.
- `examples/` - sanitized example configuration.

Development-only tests, MQTT smoke scripts, caches, and local runtime snapshots
are not included in the market package.
