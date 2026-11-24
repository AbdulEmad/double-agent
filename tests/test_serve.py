import os
import socket
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable
from contextlib import closing
from pathlib import Path

import httpx
import pytest

from double_agent.server import load_agents

TESTS = Path(__file__).parent
WEBHOOK = {"agent_id": "a1", "type": "push", "idempotency_key": "delivery-1"}


def _free_port() -> int:
    with closing(socket.socket()) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for(check: Callable[[], bool], timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (httpx.TransportError, sqlite3.OperationalError):
            pass
        time.sleep(0.1)
    raise AssertionError("timed out")


def test_serve_runs_one_agent_run_per_webhook(tmp_path: Path) -> None:
    port, db = _free_port(), tmp_path / "db.sqlite"
    url = f"http://127.0.0.1:{port}/events"
    cmd = [sys.executable, "-m", "double_agent.cli", "serve", "serve_agents:agents"]
    proc = subprocess.Popen([*cmd, "--db", str(db), "--port", str(port)], cwd=TESTS)
    try:
        _wait_for(lambda: httpx.post(url, json=WEBHOOK).status_code == 202)
        assert httpx.post(url, json=WEBHOOK).json() == {"duplicate": True}

        def finished_runs() -> list[tuple[str]]:
            with closing(sqlite3.connect(db)) as conn:
                return conn.execute("SELECT status FROM runs").fetchall()

        _wait_for(lambda: finished_runs() == [("finished",)])
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_approval_through_the_cli_resumes_the_run(tmp_path: Path) -> None:
    port, db, log = _free_port(), tmp_path / "db.sqlite", tmp_path / "deploys.log"
    url = f"http://127.0.0.1:{port}"
    env = {**os.environ, "DEPLOY_LOG": str(log), "DOUBLE_AGENT_URL": url}
    cmd = [sys.executable, "-m", "double_agent.cli"]
    serve = [*cmd, "serve", "serve_agents:agents", "--db", str(db), "--port", str(port)]
    proc = subprocess.Popen(serve, cwd=TESTS, env=env)

    def cli(*args: str) -> str:
        done = subprocess.run([*cmd, *args], env=env, capture_output=True, text=True, timeout=20)
        assert done.returncode == 0, done.stderr
        return done.stdout

    try:
        webhook = {"agent_id": "deployer", "type": "push", "idempotency_key": "d1"}
        _wait_for(lambda: httpx.post(f"{url}/events", json=webhook).status_code == 202)
        _wait_for(lambda: "deploy" in cli("approvals", "list"))
        assert not log.exists()  # Waiting for approval, so nothing deployed yet.

        approval_id = cli("approvals", "list").split()[0]
        assert cli("approvals", "approve", approval_id).strip() == f"Approved {approval_id}."
        _wait_for(lambda: log.exists() and log.read_text() == "prod\n")
        assert cli("approvals", "list").strip() == "No approvals waiting."
        assert cli("tasks", "list").strip() == "No open tasks."
        assert cli("memory", "list", "deployer").strip() == "No memory yet."
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_load_agents_rejects_bad_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(TESTS)
    assert [a.id for a in load_agents("serve_agents:agents")] == ["a1", "deployer"]
    with pytest.raises(ValueError):
        load_agents("serve_agents")
    with pytest.raises(ValueError):
        load_agents("serve_agents:DONE")
