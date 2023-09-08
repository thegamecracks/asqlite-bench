import asyncio
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, AsyncContextManager, Literal, Sequence

from .pools import Pool
from .queries import QuerySpec

log = logging.getLogger(__name__)


def _delete_database(database: str) -> None:
    # Obtain exclusive access to the database, remove any temporary files,
    # then delete the database
    log.info("Deleting SQLite database %s", database)
    conn = sqlite3.connect(database)
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    conn.execute("PRAGMA journal_mode = DELETE")
    conn.close()

    path = Path(database)
    path.unlink()

    # (despite the above procedure, the shared memory file might still exist)
    path.with_suffix(f"{path.suffix}-shm").unlink(missing_ok=True)


def create_pool(
    module: Literal["asqlite"],
    *,
    path: str,
    size: int,
) -> AsyncContextManager[Pool]:
    if module == "asqlite":
        import asqlite

        return asqlite.create_pool(path, size=size)
    else:
        raise RuntimeError(f"Unknown module {module!r}")


async def run_query(pool: Pool, query: str, args: Sequence[Any]) -> None:
    async with pool.acquire() as conn:
        c = await conn.execute(query, *args)
        await c.fetchall()


async def run_queries(pool: Pool, queries: QuerySpec) -> None:
    if queries.setup:
        async with pool.acquire() as conn:
            await conn.executescript(queries.setup)

    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(run_query(pool, queries.query, args))
            for args in queries.iter_args()
        ]
        log.info("%s query tasks created", len(tasks))


async def runner(
    queries: QuerySpec,
    *,
    cleanup: bool = True,
    n_connections: int,
):
    database_path = "asqlite_bench.db"
    pool_connector = create_pool(
        "asqlite",
        path=database_path,
        size=n_connections,
    )

    try:
        async with pool_connector as pool:
            log.info("Running queries...")

            start = time.perf_counter()
            await run_queries(pool, queries)
            elapsed = time.perf_counter() - start

            print(f"Finished in {elapsed:.3f}s")
    finally:
        if cleanup:
            _delete_database(database_path)
