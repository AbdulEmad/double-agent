import asyncio
import importlib
import os
import sys
from pathlib import Path
from typing import cast

import uvicorn

from double_agent.agent import Agent
from double_agent.api import create_app
from double_agent.dispatcher import Dispatcher
from double_agent.memory import memory_tool
from double_agent.message_tool import send_message_tool
from double_agent.scheduler import Scheduler, sync_schedules
from double_agent.sqlite_storage import SqliteStorage
from double_agent.tasks import task_tools
from double_agent.timer_tools import timer_tools
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
    for agent in agents:
        if agent.approval_channel and agent.approval_channel not in agent.channels:
            raise ValueError(
                f"Agent {agent.id}: approval_channel {agent.approval_channel!r} is not one of "
                "its channels"
            )
    storage = await SqliteStorage.open(db_path)
    workers: dict[str, Worker] = {}

    def wake_worker(agent_id: str) -> None:
        if worker := workers.get(agent_id):
            worker.wake()

    scheduler = Scheduler(storage, on_event=wake_worker)
    dispatcher = Dispatcher(storage, {a.id: a.channels for a in agents})
    for agent in agents:
        builtins = [
            *timer_tools(storage, on_change=scheduler.wake),
            *task_tools(storage),
            memory_tool(storage, agent.trusted_sources),
        ]
        if agent.channels:
            builtins.append(send_message_tool(list(agent.channels), storage, dispatcher.wake))
        workers[agent.id] = Worker(agent, storage, builtins, on_queue=dispatcher.wake)
    await sync_schedules(storage, agents)
    server = uvicorn.Server(uvicorn.Config(create_app(storage, workers), host=host, port=port))
    tasks = [asyncio.create_task(w.run_forever()) for w in workers.values()]
    tasks.append(asyncio.create_task(scheduler.run_forever()))
    tasks.append(asyncio.create_task(dispatcher.run_forever()))
    try:
        await server.serve()
    finally:
        # A run cut off here is left unfinished and resumes on the next start.
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await storage.close()
