from __future__ import annotations

import asyncio
import weakref
from abc import abstractmethod
from contextlib import AsyncExitStack, asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Protocol,
    Self,
)

if TYPE_CHECKING:
    import aiosqlite
    import asqlite


class Connection(Protocol):
    @abstractmethod
    async def execute(self, sql: str, *params) -> Cursor:
        raise NotImplementedError

    @abstractmethod
    async def executescript(self, sql: str) -> Cursor:
        raise NotImplementedError


class Cursor(Protocol):
    @abstractmethod
    async def fetchall(self) -> list[Any]:
        raise NotImplementedError


class Pool(Protocol):
    @abstractmethod
    def acquire(self) -> AsyncContextManager[Connection]:
        raise NotImplementedError


class NullPool(Pool):
    def __init__(self, *, conn_args=(), conn_kwargs={}) -> None:
        self.conn_args = conn_args
        self.conn_kwargs = conn_kwargs

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[asqlite.Connection, None]:
        import asqlite

        async with asqlite.connect(*self.conn_args, **self.conn_kwargs) as conn:
            yield conn


class AIOSQLitePool(Pool):
    def __init__(self, database: str, *, size: int):
        self.database = database
        self.size = size
        self._connections: list[aiosqlite.Connection] = []
        self._connection_queue: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue()
        self._connection_stack = AsyncExitStack()

    async def __aenter__(self) -> Self:
        stack = await self._connection_stack.__aenter__()
        for _ in range(self.size):
            conn = self._connect()
            conn = await stack.enter_async_context(conn)
            self._add_connection(conn)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
        await self._connection_stack.__aexit__(exc_type, exc_val, exc_tb)

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[Connection, None]:
        conn = await self._connection_queue.get()
        try:
            yield conn
        finally:
            self._release(conn)

    async def close(self) -> None:
        try:
            for _ in range(self.size):
                await self._connection_queue.get()
        finally:
            self._connections.clear()

    def _add_connection(self, connection: aiosqlite.Connection) -> None:
        self._connections.append(connection)
        self._connection_queue.put_nowait(weakref.proxy(connection))

    def _connect(self) -> AsyncContextManager[aiosqlite.Connection]:
        import aiosqlite

        return aiosqlite.connect(self.database)

    def _release(self, connection: aiosqlite.Connection) -> None:
        self._connection_queue.put_nowait(connection)
