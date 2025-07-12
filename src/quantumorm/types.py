"""
Type definitions for QuantumORM.

This module contains common type definitions used throughout the codebase
to improve type safety and provide better IDE support.
"""

from typing import Union, TypeVar, Type, Any, Dict, List, Optional, Protocol, TypedDict
from surrealdb import RecordID

# Common ID types used throughout QuantumORM
IdType = Union[str, int, RecordID]
"""Type for document IDs - can be string, integer, or SurrealDB RecordID"""

DatabaseValue = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
"""Type for values that can be stored in the database"""

FieldValue = Any
"""Type for field values - can be any Python value that a field can handle"""

# Document type variable for generic constraints
DocumentType = TypeVar('DocumentType', bound='Document')
"""Type variable for Document classes"""

QuerySetType = TypeVar('QuerySetType', bound='BaseQuerySet')
"""Type variable for QuerySet classes"""

# Backend-related types
class DocumentDict(TypedDict, total=False):
    """TypedDict for document data from database."""
    id: IdType
    
BackendDict = Dict[str, DatabaseValue]
"""Type for document data at the backend level"""

# Connection types
ConnectionType = Any  # This would be more specific in real implementation
"""Type for database connections"""

# Query condition types
QueryCondition = tuple[str, str, Any]
"""Type for query conditions: (field, operator, value)"""

QueryConditions = List[QueryCondition]
"""Type for list of query conditions"""

# Pagination types
class PaginationInfo(TypedDict):
    """Type for pagination information."""
    page: int
    per_page: int
    total: int
    pages: int
    has_prev: bool
    has_next: bool

# Field validation types
class ValidationError(Exception):
    """Exception raised when field validation fails."""
    pass

# Protocol for field validation
class ValidatableField(Protocol):
    """Protocol for fields that can validate values."""
    
    def validate(self, value: Any) -> Any:
        """Validate a value and return the validated result."""
        ...

# Protocol for database serialization
class SerializableField(Protocol):
    """Protocol for fields that can serialize to/from database format."""
    
    def to_db(self, value: Any, backend: Optional[str] = None) -> DatabaseValue:
        """Convert value to database format."""
        ...
    
    def from_db(self, value: DatabaseValue, backend: Optional[str] = None) -> Any:
        """Convert value from database format."""
        ...

# Forward reference types (to avoid circular imports)
if False:  # TYPE_CHECKING would be used in real implementation
    from .document import Document
    from .base_query import BaseQuerySet
    from .fields.base import Field