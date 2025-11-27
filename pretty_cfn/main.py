"""Unified entry point that dispatches between CLI and MCP server."""

from __future__ import annotations

import sys

from .cli import dispatch_cli


def main() -> None:
    """Run the Click CLI by default; launch the MCP server when requested."""

    args = sys.argv[1:]

    # Run the MCP server when explicitly requested as a subcommand.
    if args and args[0] == "mcp":
        from .server import mcp

        mcp.run(transport="stdio", show_banner=False)
        return

    dispatch_cli(args)


if __name__ == "__main__":
    main()
