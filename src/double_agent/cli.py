import asyncio
import json
from pathlib import Path
from typing import Annotated, Any

import click
import httpx
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


approvals = typer.Typer(no_args_is_help=True, help="List and answer approval requests.")
app.add_typer(approvals, name="approvals")

Url = Annotated[str, typer.Option(envvar="DOUBLE_AGENT_URL", help="Address of the running server.")]
DEFAULT_URL = "http://127.0.0.1:8000"


def _request(method: str, url: str, path: str, body: dict[str, Any] | None = None) -> Any:
    try:
        response = httpx.request(method, url.rstrip("/") + path, json=body, timeout=10)
    except httpx.TransportError as exc:
        typer.echo(f"Could not reach the server at {url}: {exc}", err=True)
        raise typer.Exit(1) from None
    if response.is_error:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        typer.echo(f"Error {response.status_code}: {detail}", err=True)
        raise typer.Exit(1)
    return response.json()


@approvals.command("list")
def list_approvals(url: Url = DEFAULT_URL) -> None:
    """Show approval requests waiting for an answer."""
    pending = _request("GET", url, "/approvals")
    if not pending:
        typer.echo("No approvals waiting.")
    for a in pending:
        typer.echo(f"{a['id']}  {a['agent_id']}  {a['tool_name']}  {json.dumps(a['input'])}")


@approvals.command()
def approve(approval_id: str, url: Url = DEFAULT_URL) -> None:
    """Allow a tool call. Its run resumes and the tool runs."""
    _request("POST", url, f"/approvals/{approval_id}", {"approve": True})
    typer.echo(f"Approved {approval_id}.")


@approvals.command()
def deny(
    approval_id: str,
    reason: Annotated[str | None, typer.Option(help="Told to the agent.")] = None,
    url: Url = DEFAULT_URL,
) -> None:
    """Refuse a tool call. Its run resumes and the agent is told it was denied."""
    _request("POST", url, f"/approvals/{approval_id}", {"approve": False, "reason": reason})
    typer.echo(f"Denied {approval_id}.")


tasks = typer.Typer(no_args_is_help=True, help="See the tasks agents are tracking.")
app.add_typer(tasks, name="tasks")


@tasks.command("list")
def list_tasks(
    agent: Annotated[str | None, typer.Option(help="Only this agent's tasks.")] = None,
    status: Annotated[str, typer.Option(help="open, done or cancelled.")] = "open",
    url: Url = DEFAULT_URL,
) -> None:
    """List tasks."""
    query = f"/tasks?status={status}" + (f"&agent_id={agent}" if agent else "")
    found = _request("GET", url, query)
    if not found:
        typer.echo(f"No {status} tasks.")
    for t in found:
        typer.echo(f"{t['agent_id']}  {t['id']}  {t['title']}  ({t['status']})")


@tasks.command()
def show(agent_id: str, task_id: str, url: Url = DEFAULT_URL) -> None:
    """Show a task the way the agent sees it."""
    typer.echo(_request("GET", url, f"/tasks/{agent_id}/{task_id}")["markdown"])


def _file(path: str) -> str:
    return "/" + path.lstrip("/")


memory = typer.Typer(
    no_args_is_help=True,
    help="See and edit an agent's memory. /instructions.md holds your standing instructions.",
)
app.add_typer(memory, name="memory")


@memory.command("list")
def list_memory(agent_id: str, url: Url = DEFAULT_URL) -> None:
    """List an agent's memory files."""
    found = _request("GET", url, f"/agents/{agent_id}/memory")
    if not found:
        typer.echo("No memory yet.")
    for f in found:
        typer.echo(f"{f['path']}  {f['size']} chars  updated {f['updated_at']}")


@memory.command("show")
def show_memory(agent_id: str, path: str, url: Url = DEFAULT_URL) -> None:
    """Print a memory file, or /instructions.md."""
    typer.echo(_request("GET", url, f"/agents/{agent_id}/memory{_file(path)}")["content"])


@memory.command("edit")
def edit_memory(agent_id: str, path: str, url: Url = DEFAULT_URL) -> None:
    """Open a memory file, or /instructions.md, in your editor and save it as a new version."""
    current = ""
    try:
        response = httpx.get(f"{url.rstrip('/')}/agents/{agent_id}/memory{_file(path)}", timeout=10)
    except httpx.TransportError as exc:
        typer.echo(f"Could not reach the server at {url}: {exc}", err=True)
        raise typer.Exit(1) from None
    if response.status_code == 200:
        current = response.json()["content"]
    edited = click.edit(current, extension=".md")
    if edited is None or edited == current:
        typer.echo("No changes.")
        return
    saved = _request("PUT", url, f"/agents/{agent_id}/memory{_file(path)}", {"content": edited})
    typer.echo(saved["result"])


@memory.command("history")
def memory_history(agent_id: str, path: str, url: Url = DEFAULT_URL) -> None:
    """Show every write to a file, with the run and source that made it."""
    for v in _request("GET", url, f"/agents/{agent_id}/memory-history{_file(path)}"):
        action = "deleted" if v["content"] is None else f"{len(v['content'])} chars"
        typer.echo(f"{v['at']}  {v['source']}  run {v['run_id'] or '-'}  {action}")


if __name__ == "__main__":
    app()
