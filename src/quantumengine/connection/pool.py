from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class PoolConfig:
    min_size: int = 1
    max_size: int = 10
    max_idle_time: int = 300  # seconds
    connection_timeout: int = 30
    retry_attempts: int = 3
    health_check_interval: int = 60

class ConnectionPoolBase(ABC):
    """Abstract base class for connection pools."""

    def __init__(self, pool_config: PoolConfig):
        self.pool_config = pool_config

    @abstractmethod
    async def get_connection(self) -> Any:
        """Get a connection from the pool."""
        ...

    @abstractmethod
    async def return_connection(self, conn: Any) -> None:
        """Return a connection to the pool."""
        ...

    @abstractmethod
    async def close_all(self) -> None:
        """Close all connections in the pool."""
        ...

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the pool."""
        ...
