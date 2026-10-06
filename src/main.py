"""Entry point: load settings and run the MCP server over stdio."""

from config import Settings
from server import create_server


def main() -> None:
    settings = Settings()
    server = create_server(settings)
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
