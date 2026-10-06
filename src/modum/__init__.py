"""modum: MCP server around discord.py for single-agent guild management.

Package root for the Modum MCP server (multi-task Discord agent support)
with shared conversation context and provider adapters. Minimal root:
importing this module performs no work: no SDK imports, no environment
reads, no network, no Discord access. Import submodules
(``modum.server``, ``modum.conversation``, ``modum.jev``) explicitly.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
