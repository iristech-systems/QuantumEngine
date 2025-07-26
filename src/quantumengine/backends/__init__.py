"""Backend system for QuantumEngine.

This module provides a modular registry for different database backends,
allowing QuantumEngine to work with multiple databases like SurrealDB
and ClickHouse through optional dependencies.
"""

from typing import Dict, Type, Any, Optional


class BackendRegistry:
    """Registry for database backends with optional dependency support."""
    
    _backends: Dict[str, Type['BaseBackend']] = {}
    _backend_errors: Dict[str, str] = {}
    
    @classmethod
    def register(cls, name: str, backend_class: Type['BaseBackend']) -> None:
        """Register a backend.
        
        Args:
            name: The name to register the backend under
            backend_class: The backend class to register
        """
        cls._backends[name] = backend_class
        # Clear any previous error for this backend
        cls._backend_errors.pop(name, None)
    
    @classmethod
    def register_error(cls, name: str, error_message: str) -> None:
        """Register that a backend failed to load with a helpful error message.
        
        Args:
            name: The name of the backend
            error_message: Error message to show when backend is requested
        """
        cls._backend_errors[name] = error_message
    
    @classmethod
    def get_backend(cls, name: str) -> Type['BaseBackend']:
        """Get a backend class by name.
        
        Args:
            name: The name of the backend
            
        Returns:
            The backend class
            
        Raises:
            ImportError: If the backend requires optional dependencies
            ValueError: If the backend doesn't exist
        """
        # Check if backend is registered successfully
        if name in cls._backends:
            return cls._backends[name]
        
        # Check if backend failed to load due to missing dependencies
        if name in cls._backend_errors:
            raise ImportError(cls._backend_errors[name])
        
        # Backend doesn't exist at all
        available = list(cls._backends.keys())
        failed = list(cls._backend_errors.keys())
        
        error_msg = f"Unknown backend '{name}'."
        if available:
            error_msg += f" Available backends: {available}."
        if failed:
            error_msg += f" Backends with missing dependencies: {failed}."
        
        raise ValueError(error_msg)
    
    @classmethod
    def list_backends(cls) -> list[str]:
        """List all successfully registered backends."""
        return list(cls._backends.keys())
    
    @classmethod
    def list_failed_backends(cls) -> Dict[str, str]:
        """List backends that failed to load due to missing dependencies."""
        return cls._backend_errors.copy()
    
    @classmethod
    def is_backend_available(cls, name: str) -> bool:
        """Check if a backend is available for use."""
        return name in cls._backends


# Import base backend class
from .base import BaseBackend

# Backend installation instructions
_BACKEND_INSTALL_INSTRUCTIONS = {
    'clickhouse': "pip install quantumengine[clickhouse]",
    'surrealdb': "pip install quantumengine[surrealdb]",
    'redis': "pip install quantumengine[redis]",
}

def _register_backends():
    """Auto-register backends and track missing dependencies."""
    
    # Try to register ClickHouse backend
    try:
        import clickhouse_connect  # Check for required dependency
        from .clickhouse import ClickHouseBackend
        BackendRegistry.register('clickhouse', ClickHouseBackend)
    except ImportError as e:
        install_cmd = _BACKEND_INSTALL_INSTRUCTIONS.get('clickhouse', 'pip install clickhouse-connect')
        error_msg = (
            f"ClickHouse backend requires the 'clickhouse-connect' package. "
            f"Install it with: {install_cmd}"
        )
        BackendRegistry.register_error('clickhouse', error_msg)
    
    # Try to register SurrealDB backend
    try:
        import surrealdb  # Check for required dependency
        from .surrealdb import SurrealDBBackend
        BackendRegistry.register('surrealdb', SurrealDBBackend)
    except ImportError as e:
        install_cmd = _BACKEND_INSTALL_INSTRUCTIONS.get('surrealdb', 'pip install surrealdb')
        error_msg = (
            f"SurrealDB backend requires the 'surrealdb' package. "
            f"Install it with: {install_cmd}"
        )
        BackendRegistry.register_error('surrealdb', error_msg)
    
    # Try to register Redis backend
    try:
        import redis  # Check for required dependency
        from .redis import RedisBackend
        BackendRegistry.register('redis', RedisBackend)
    except ImportError as e:
        install_cmd = _BACKEND_INSTALL_INSTRUCTIONS.get('redis', 'pip install redis')
        error_msg = (
            f"Redis backend requires the 'redis' package. "
            f"Install it with: {install_cmd}"
        )
        BackendRegistry.register_error('redis', error_msg)

# Register backends on import
_register_backends()


__all__ = ['BackendRegistry', 'BaseBackend']