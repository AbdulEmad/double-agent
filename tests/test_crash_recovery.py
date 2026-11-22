import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

SCRIPT = Path(__file__).with_name("crash_agent.py")


def test_sigkill_mid_run_resumes_without_repeating_completed_actions(tmp_path: Path) -> None:
    agent = subprocess.Popen([sys.executable, SCRIPT, tmp_path])
    deadline = time.monotonic() + 20
    while not (tmp_path / "hang.marker").exists():
        assert agent.poll() is None, "agent exited before reaching the hang point"
        assert time.monotonic() < deadline, "agent never reached the hang point"
        time.sleep(0.05)

    agent.kill()  # SIGKILL: no cleanup, no chance to save anything.
    agent.wait()
    # Killed during the second tool call: one action done, two model calls made.
    assert (tmp_path / "actions.log").read_text() == "1\n"
    assert (tmp_path / "model_calls.log").read_text() == "0\n1\n"

    subprocess.run([sys.executable, SCRIPT, tmp_path], check=True, timeout=20)

    # Action 1 and both earlier model calls were not repeated.
    assert (tmp_path / "actions.log").read_text() == "1\n2\n"
    assert (tmp_path / "model_calls.log").read_text() == "0\n1\n2\n"
    with closing(sqlite3.connect(tmp_path / "db.sqlite")) as db:
        assert db.execute("SELECT status FROM runs").fetchall() == [("finished",)]
        assert db.execute("SELECT status FROM events").fetchall() == [("done",)]
