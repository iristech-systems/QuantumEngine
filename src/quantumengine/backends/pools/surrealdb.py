import asyncio
from typing import Any, Set, Optional

from ...connection.pool import ConnectionPoolBase, PoolConfig


class SurrealDBConnectionPool(ConnectionPoolBase):
    """SurrealDB-specific connection pool using asyncio queues."""

    def __init__(self, connection_config: dict, pool_config: PoolConfig):
        super().__init__(pool_config)
        self.connection_config = connection_config
        self._pool: asyncio.Queue = asyncio.Queue(maxsize=self.pool_config.max_size)
        self._active_connections: Set[Any] = set()
        self._created_count: int = 0
        self._health_check_task: Optional[asyncio.Task] = None

    async def get_connection(self) -> 'Surreal':
        """Get connection from pool or create new one."""
        try:
            # Try to get existing connection from pool
            conn = self._pool.get_nowait()
            if await self._is_connection_healthy(conn):
                self._active_connections.add(conn)
                return conn
            else:
                # Connection unhealthy, close it and don't reuse
                await self._close_connection(conn)
                self._created_count -= 1
        except asyncio.QueueEmpty:
            pass

        # If pool is empty, create new connection if under max limit
        if self._created_count < self.pool_config.max_size:
            conn = await self._create_connection()
            self._active_connections.add(conn)
            self._created_count += 1
            return conn

        # Wait for a connection to become available if pool is full
        try:
            conn = await asyncio.wait_for(
                self._pool.get(),
                timeout=self.pool_config.connection_timeout
            )
            if await self._is_connection_healthy(conn):
                self._active_connections.add(conn)
                return conn
            else:
                # Connection from pool was unhealthy, try creating a new one if possible
                await self._close_connection(conn)
                self._created_count -= 1
                if self._created_count < self.pool_config.max_size:
                    conn = await self._create_connection()
                    self._active_connections.add(conn)
                    self._created_count += 1
                    return conn
                else:
                    # Re-raise timeout if we can't create a new one
                    raise asyncio.TimeoutError("Could not get a healthy connection in time.")

        except asyncio.TimeoutError:
            raise ConnectionError("Timeout waiting for available connection.")

    async def return_connection(self, conn: 'Surreal') -> None:
        """Return connection to the pool."""
        if conn in self._active_connections:
            self._active_connections.remove(conn)

            if await self._is_connection_healthy(conn):
                try:
                    self._pool.put_nowait(conn)
                except asyncio.QueueFull:
                    # Pool is full, close this connection
                    await self._close_connection(conn)
                    self._created_count -= 1
            else:
                # Connection is unhealthy, close it
                await self._close_connection(conn)
                self._created_count -= 1

    async def close_all(self) -> None:
        """Close all connections in the pool."""
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            await self._close_connection(conn)
        for conn in self._active_connections:
            await self._close_connection(conn)
        self._active_connections.clear()
        self._created_count = 0

    def get_stats(self) -> dict:
        """Get pool statistics."""
        return {
            "total_connections": self._created_count,
            "active_connections": len(self._active_connections),
            "idle_connections": self._pool.qsize(),
            "max_size": self.pool_config.max_size,
        }

    async def _create_connection(self) -> 'Surreal':
        """Create a new SurrealDB connection."""
        from surrealdb import Surreal

        conn = Surreal()
        await conn.connect(self.connection_config['url'])

        if 'username' in self.connection_config and self.connection_config['username']:
            await conn.signin({
                'user': self.connection_config['username'],
                'pass': self.connection_config['password']
            })

        if 'namespace' in self.connection_config and self.connection_config['namespace']:
            await conn.use(
                self.connection_config['namespace'],
                self.connection_config['database']
            )

        return conn

    async def _close_connection(self, conn: 'Surreal') -> None:
        """Safely close a single SurrealDB connection."""
        try:
            await conn.close()
        except Exception:
            # Ignore errors on close
            pass

    async def _is_connection_healthy(self, conn: 'Surreal') -> bool:
        """Check if a connection is still healthy."""
        try:
            # SurrealDB's Python driver doesn't have a simple ping.
            # A lightweight query is the most reliable way.
            return await conn.status() == 'OK'
        except Exception:
            return False
