"""Bot private relay plugin package.

The runtime plugin identity is ``bot_private_relay`` while the transport
platform exposed to Neo-MoFox is ``bot_relay``.

Keep package import side-effect free so plugin registration only happens when
``plugin.py`` is imported intentionally by the framework or tests.
"""

__all__: list[str] = []
