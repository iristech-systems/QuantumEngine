# This file makes 'connection' a package.
from .pool import ConnectionPoolBase, PoolConfig
from .manager import ConnectionPoolManager
from .stats import PoolStats, PoolMonitor
from .health import HealthChecker

# Import ConnectionRegistry from connection_api for backward compatibility
from ..connection_api import ConnectionRegistry
