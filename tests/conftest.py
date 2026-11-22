from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from double_agent.sqlite_storage import SqliteStorage
from double_agent.storage import InMemoryStorage, Storage


@pytest.fixture(params=["memory", "sqlite"])
async def storage(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Storage]:
    """Each test using this runs against both storage implementations."""
    if request.param == "memory":
        yield InMemoryStorage()
        return
    sqlite = await SqliteStorage.open(tmp_path / "db.sqlite")
    yield sqlite
    await sqlite.close()
