from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class PoolStats:
    pool_name: str
    backend_type: str
    active_connections: int
    idle_connections: int
    total_connections: int
    max_connections: int
    connection_requests: int = 0
    failed_requests: int = 0
    average_wait_time: float = 0.0
    health_check_failures: int = 0

class PoolMonitor:
    """Monitor and collect pool statistics."""

    def __init__(self, pool_manager: 'ConnectionPoolManager'):
        """Initializes the monitor with a pool manager."""
        self.pool_manager = pool_manager
        self._stats: Dict[str, PoolStats] = {}

    async def collect_stats(self) -> Dict[str, PoolStats]:
        """Collect statistics from all registered pools."""
        # This is a simplified implementation. A real one would need to aggregate
        # data from the pools themselves, which would require the pools to
        # track more detailed stats.
        for name, pool in self.pool_manager._pools.items():
            stats = pool.get_stats()
            self._stats[name] = PoolStats(
                pool_name=name,
                backend_type=pool.__class__.__name__,
                active_connections=stats.get('active_connections', 0),
                idle_connections=stats.get('idle_connections', 0),
                total_connections=stats.get('total_connections', 0),
                max_connections=stats.get('max_size', 0),
            )
        return self._stats

    async def export_metrics(self, format: str = 'prometheus') -> str:
        """Export metrics in a specified format."""
        if format != 'prometheus':
            raise ValueError("Unsupported format. Only 'prometheus' is supported.")

        await self.collect_stats()
        lines = []
        for name, stats in self._stats.items():
            labels = f'pool_name="{name}", backend_type="{stats.backend_type}"'
            lines.append(f'quantumengine_pool_active_connections{{{labels}}} {stats.active_connections}')
            lines.append(f'quantumengine_pool_idle_connections{{{labels}}} {stats.idle_connections}')
            lines.append(f'quantumengine_pool_total_connections{{{labels}}} {stats.total_connections}')
            lines.append(f'quantumengine_pool_max_connections{{{labels}}} {stats.max_connections}')

        return "\n".join(lines)
