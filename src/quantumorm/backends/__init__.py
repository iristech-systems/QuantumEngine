"""Backend system for SurrealEngine.

This module provides a registry for different database backends,
allowing SurrealEngine to work with multiple databases like SurrealDB
and ClickHouse.
"""

from typing import Dict, Type, Any


class BackendRegistry:
    """Registry for database backends."""
    
    _backends: Dict[str, Type['BaseBackend']] = {}
    
    @classmethod
    def register(cls, name: str, backend_class: Type['BaseBackend']) -> None:
        """Register a backend.
        
        Args:
            name: The name to register the backend under
            backend_class: The backend class to register
        """
        cls._backends[name] = backend_class
    
    @classmethod
    def get_backend(cls, name: str) -> Type['BaseBackend']:
        """Get a backend class by name.
        
        Args:
            name: The name of the backend
            
        Returns:
            The backend class
            
        Raises:
            ValueError: If the backend is not registered
        """
        if name not in cls._backends:
            raise ValueError(f"Backend '{name}' not registered. Available backends: {list(cls._backends.keys())}")
        return cls._backends[name]
    
    @classmethod
    def list_backends(cls) -> list[str]:
        """List all registered backends."""
        return list(cls._backends.keys())


# Import backends after registry is defined to avoid circular imports
from .base import BaseBackend

# Auto-register backends when they're imported
# These imports will happen after the package is fully loaded
def _register_backends():
    try:
        from .surrealdb import SurrealDBBackend
        BackendRegistry.register('surrealdb', SurrealDBBackend)
    except ImportError:
        pass
    
    try:
        from .clickhouse import ClickHouseBackend
        BackendRegistry.register('clickhouse', ClickHouseBackend)
    except ImportError:
        pass

# Register backends on import
_register_backends()


__all__ = ['BackendRegistry', 'BaseBackend']