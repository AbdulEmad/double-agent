import asyncio
from pathlib import Path
from typing import Annotated

import typer

from double_agent import __version__, server

app = typer.Typer(no_args_is_help=True)


def _print_version(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option("--version", callback=_print_version, is_eager=True, help="Show the version."),
    ] = False,
) -> None:
    """An open source runtime for always-on agents."""


@app.command()
def serve(
    agents: Annotated[
        str, typer.Argument(help="Agents to run, as module:attribute naming a list of Agent.")
    ],
    db: Annotated[Path, typer.Option(help="SQLite database file.")] = Path("double-agent.sqlite"),
    host: Annotated[str, typer.Option(help="Address to listen on.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to listen on.")] = 8000,
) -> None:
    """Run the webhook API and one worker per agent."""
    asyncio.run(server.serve(server.load_agents(agents), db, host, port))


if __name__ == "__main__":
    app()
