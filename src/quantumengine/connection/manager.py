from typing import Dict
from .pool import ConnectionPoolBase

class ConnectionPoolManager:
    """Manages connection pools across all backends."""

    def __init__(self):
        self._pools: Dict[str, ConnectionPoolBase] = {}

    async def get_pool(self, connection_name: str) -> ConnectionPoolBase:
        """
        Retrieves a connection pool by its name.

        Args:
            connection_name: The name of the connection pool.

        Returns:
            The connection pool instance.

        Raises:
            KeyError: If the pool is not found.
        """
        if connection_name not in self._pools:
            raise KeyError(f"Connection pool '{connection_name}' not found.")
        return self._pools[connection_name]

    def register_pool(self, connection_name: str, pool: ConnectionPoolBase):
        """
        Registers a new connection pool.

        Args:
            connection_name: The name to register the pool under.
            pool: The connection pool instance to register.
        """
        self._pools[connection_name] = pool

    async def close_all_pools(self) -> None:
        """Closes all registered connection pools."""
        for pool in self._pools.values():
            await pool.close_all()
        self._pools.clear()
