from typing import Any

from ...connection.pool import ConnectionPoolBase, PoolConfig


class RedisConnectionPool(ConnectionPoolBase):
    """Redis connection pool using redis-py's built-in connection pool."""

    def __init__(self, connection_config: dict, pool_config: PoolConfig):
        import redis.asyncio as redis
        super().__init__(pool_config)
        self.connection_config = connection_config

        # Create a redis-py connection pool instance
        self._pool = redis.ConnectionPool(
            host=connection_config.get('host', 'localhost'),
            port=connection_config.get('port', 6379),
            db=connection_config.get('db', 0),
            password=connection_config.get('password'),
            max_connections=pool_config.max_size,
            retry_on_timeout=True,
            # Note: redis-py's pool doesn't map all our config options directly.
            # We rely on its implementation for idle timeouts and health checks.
        )

    async def get_connection(self) -> Any:
        """Get a Redis client instance from the pool.

        Each client instance created from the pool uses a connection from the pool.
        """
        import redis.asyncio as redis
        return redis.Redis(connection_pool=self._pool)

    async def return_connection(self, conn: Any) -> None:
        """Return a Redis connection to the pool.

        With redis-py, connections are automatically returned to the pool when the
        client instance goes out of scope or its connection is closed. This
        method is a no-op for compatibility with the interface.
        """
        # redis-py handles connection returning automatically.
        pass

    async def close_all(self) -> None:
        """Close all connections in the pool."""
        # redis-py's pool has a disconnect method to close all connections.
        await self._pool.disconnect()

    def get_stats(self) -> dict:
        """Get pool statistics from the underlying redis-py pool."""
        # Note: redis-py's pool doesn't expose detailed stats in the same way.
        # We'll provide what we can.
        return {
            "total_connections": self._pool.max_connections,
            "active_connections": len(self._pool._in_use_connections),
            "idle_connections": len(self._pool._available_connections),
            "max_size": self._pool.max_connections,
        }
