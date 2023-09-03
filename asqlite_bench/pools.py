from abc import abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncContextManager, AsyncGenerator, Protocol, Self

import asqlite


class Pool(Protocol):
    @abstractmethod
    def acquire(self) -> AsyncContextManager[asqlite.Connection]:
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
        async with asqlite.connect(*self.conn_args, **self.conn_kwargs) as conn:
            yield conn
