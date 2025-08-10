import asyncio
from typing import Any, Set

from ...connection.pool import ConnectionPoolBase, PoolConfig


class ClickHouseConnectionPool(ConnectionPoolBase):
    """ClickHouse connection pool using a semaphore and a queue."""

    def __init__(self, connection_config: dict, pool_config: PoolConfig):
        super().__init__(pool_config)
        self.connection_config = connection_config
        self._semaphore = asyncio.Semaphore(pool_config.max_size)
        self._pool: asyncio.Queue = asyncio.Queue()
        self._active_connections: Set[Any] = set()
        self._created_count: int = 0

    async def get_connection(self) -> Any:
        """Get a ClickHouse connection from the pool."""
        await asyncio.wait_for(
            self._semaphore.acquire(),
            timeout=self.pool_config.connection_timeout
        )

        try:
            # Try to reuse a connection from the pool
            conn = self._pool.get_nowait()
            if self._is_connection_healthy(conn):
                self._active_connections.add(conn)
                return conn
            else:
                # Connection is not healthy, discard it
                self._close_connection(conn)
                self._created_count -= 1
        except asyncio.QueueEmpty:
            pass # Pool is empty, will create a new connection.

        # Create a new connection if we are here
        try:
            conn = self._create_connection()
            self._active_connections.add(conn)
            self._created_count += 1
            return conn
        except Exception:
            # If creation fails, release the semaphore and re-raise
            self._semaphore.release()
            raise

    async def return_connection(self, conn: Any) -> None:
        """Return a ClickHouse connection to the pool."""
        if conn in self._active_connections:
            self._active_connections.remove(conn)

            if self._is_connection_healthy(conn):
                try:
                    self._pool.put_nowait(conn)
                except asyncio.QueueFull:
                    # Pool is full, just close the connection
                    self._close_connection(conn)
                    self._created_count -= 1
            else:
                # Connection is not healthy, discard it
                self._close_connection(conn)
                self._created_count -= 1

        # Release the semaphore regardless of connection health
        self._semaphore.release()

    async def close_all(self) -> None:
        """Close all connections in the pool."""
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            self._close_connection(conn)
        for conn in self._active_connections:
            self._close_connection(conn)
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

    def _create_connection(self) -> Any:
        """Create a new ClickHouse connection."""
        import clickhouse_connect

        return clickhouse_connect.get_client(
            host=self.connection_config.get('host', 'localhost'),
            port=self.connection_config.get('port', 8123),
            username=self.connection_config.get('username', 'default'),
            password=self.connection_config.get('password', ''),
            database=self.connection_config.get('database', 'default'),
            secure=self.connection_config.get('secure', False)
        )

    def _close_connection(self, conn: Any) -> None:
        """Safely close a single ClickHouse connection."""
        try:
            conn.close()
        except Exception:
            # Ignore errors on close
            pass

    def _is_connection_healthy(self, conn: Any) -> bool:
        """Check if a connection is still healthy."""
        try:
            # clickhouse-connect client has a ping method
            return conn.ping()
        except Exception:
            return False
