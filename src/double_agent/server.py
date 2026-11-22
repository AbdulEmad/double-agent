import asyncio
import importlib
import os
import sys
from pathlib import Path
from typing import cast

import uvicorn

from double_agent.agent import Agent
from double_agent.api import create_app
from double_agent.sqlite_storage import SqliteStorage
from double_agent.worker import Worker


def load_agents(target: str) -> list[Agent]:
    """Import a list of agents from a `module:attribute` path, relative to the current directory."""
    module_name, _, attr = target.partition(":")
    if not module_name or not attr:
        raise ValueError(f"Expected module:attribute, got {target!r}")
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    value: object = getattr(importlib.import_module(module_name), attr)
    items = cast(list[object], value) if isinstance(value, list) else None
    if items is None or not all(isinstance(a, Agent) for a in items):
        raise ValueError(f"{target} must be a list of Agent")
    return cast(list[Agent], items)


async def serve(agents: list[Agent], db_path: Path, host: str, port: int) -> None:
    """Run the HTTP API and one worker per agent until the server is stopped."""
    ids = [a.id for a in agents]
    if len(set(ids)) != len(ids):
        raise ValueError(f"Agent ids must be unique: {ids}")
    storage = await SqliteStorage.open(db_path)
    workers = {a.id: Worker(a, storage) for a in agents}
    server = uvicorn.Server(uvicorn.Config(create_app(storage, workers), host=host, port=port))
    tasks = [asyncio.create_task(w.run_forever()) for w in workers.values()]
    try:
        await server.serve()
    finally:
        # A run cut off here is left unfinished and resumes on the next start.
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await storage.close()
