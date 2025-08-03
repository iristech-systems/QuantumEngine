import datetime
import re
import uuid
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Pattern, Type, TypeVar, Union, cast, Generic
from surrealdb.data.types.datetime import IsoDateTimeWrapper
from surrealdb import RecordID
from ..exceptions import ValidationError
from ..signals import (
    pre_validate, post_validate, pre_to_db, post_to_db,
    pre_from_db, post_from_db, SIGNAL_SUPPORT
)
import json
from ..query_expressions_pythonic import QueryableFieldProxy

# Type variable for field types
T = TypeVar('T')

class Field(Generic[T]):
    """Base class for all field types.

    This class provides the foundation for all field types in the document model.
    It includes methods for validation and conversion between Python and database
    representations.

    Attributes:
        required: Whether the field is required
        default: Default value for the field
        name: Name of the field (set during document class creation)
        db_field: Name of the field in the database
        owner_document: The document class that owns this field
        define_schema: Whether to define this field in the schema (even for SCHEMALESS tables)
    """

    def __init__(self, required: bool = False, default: Any = None, db_field: Optional[str] = None,
                 define_schema: bool = False, indexed: bool = False, unique: bool = False, 
                 search: bool = False, analyzer: Optional[str] = None, index_with: Optional[List[str]] = None,
                 materialized: Optional[str] = None, indexes: Optional[List[Dict[str, Any]]] = None,
                 help_text: Optional[str] = None) -> None:
        """Initialize a new Field.

        Args:
            required: Whether the field is required
            default: Default value for the field
            db_field: Name of the field in the database (defaults to the field name)
            define_schema: Whether to define this field in the schema (even for SCHEMALESS tables)
            indexed: Whether the field should be indexed
            unique: Whether the index should enforce uniqueness
            search: Whether the index is a search index
            analyzer: Analyzer to use for search indexes
            index_with: List of other field names to include in the index
            materialized: ClickHouse materialized column expression
            help_text: Human-readable description of what this field represents
            indexes: List of index specifications for advanced indexing
        """
        self.required = required
        self.default = default
        self.name: Optional[str] = None  # Will be set during document class creation
        self.db_field = db_field
        self.owner_document: Optional[Type] = None
        self.define_schema = define_schema
        self.indexed = indexed
        self.unique = unique
        self.search = search
        self.analyzer = analyzer
        self.index_with = index_with
        self.materialized = materialized
        self.indexes = indexes or []
        self.help_text = help_text
        self.py_type = Any

    def validate(self, value: Any) -> T:
        """Validate the field value.

        This method checks if the value is valid for this field type.
        Subclasses should override this method to provide type-specific validation.

        Args:
            value: The value to validate

        Returns:
            The validated value

        Raises:
            ValueError: If the value is invalid
        """
        # Trigger pre_validate signal
        if SIGNAL_SUPPORT:
            pre_validate.send(self.__class__, field=self, value=value)

        if value is None and self.required:
            raise ValueError(f"Field '{self.name}' is required")

        result = cast(T, value)

        # Trigger post_validate signal
        if SIGNAL_SUPPORT:
            post_validate.send(self.__class__, field=self, value=result)

        return result

    def to_db(self, value: Any, backend: Optional[str] = None) -> Any:
        """Convert Python value to database representation.

        This method converts a Python value to a representation that can be
        stored in the database. Subclasses should override this method to
        provide type-specific conversion.

        Args:
            value: The Python value to convert
            backend: The backend type ('surrealdb', 'clickhouse', etc.)

        Returns:
            The database representation of the value
        """
        # Trigger pre_to_db signal
        if SIGNAL_SUPPORT:
            pre_to_db.send(self.__class__, field=self, value=value)

        # Backend-specific conversion logic can be added by subclasses
        result = self._to_db_backend_specific(value, backend or 'surrealdb')

        # Trigger post_to_db signal
        if SIGNAL_SUPPORT:
            post_to_db.send(self.__class__, field=self, value=result)

        return result
    
    def _to_db_backend_specific(self, value: Any, backend: str) -> Any:
        """Backend-specific conversion logic.
        
        Subclasses can override this method to provide backend-specific
        conversion logic while maintaining the base to_db behavior.
        
        Args:
            value: The Python value to convert
            backend: The backend type
            
        Returns:
            The database representation of the value
        """
        return value

    def from_db(self, value: Any, backend: Optional[str] = None) -> T:
        """Convert database value to Python representation.

        This method converts a value from the database to a Python value.
        Subclasses should override this method to provide type-specific conversion.

        Args:
            value: The database value to convert
            backend: The backend type ('surrealdb', 'clickhouse', etc.)

        Returns:
            The Python representation of the value
        """
        # Trigger pre_from_db signal
        if SIGNAL_SUPPORT:
            pre_from_db.send(self.__class__, field=self, value=value)

        # Backend-specific conversion logic can be added by subclasses
        result = self._from_db_backend_specific(value, backend or 'surrealdb')

        # Trigger post_from_db signal
        if SIGNAL_SUPPORT:
            post_from_db.send(self.__class__, field=self, value=result)

        return result
    
    def _from_db_backend_specific(self, value: Any, backend: str) -> T:
        """Backend-specific conversion logic.
        
        Subclasses can override this method to provide backend-specific
        conversion logic while maintaining the base from_db behavior.
        
        Args:
            value: The database value to convert
            backend: The backend type
            
        Returns:
            The Python representation of the value
        """
        return cast(T, value)
    
    def __get__(self, instance: Any, owner: Type) -> Union[Any, QueryableFieldProxy]:
        """Field descriptor implementation for Pythonic query syntax.
        
        When accessed on the class (e.g., User.age), returns a QueryableFieldProxy
        that supports query operators. When accessed on an instance, returns the
        field value.
        
        Args:
            instance: The instance the field is accessed from (None for class access)
            owner: The class that owns this field
            
        Returns:
            QueryableFieldProxy for class access, field value for instance access
        """
        if instance is None:
            # Class-level access: return queryable proxy for query building
            return QueryableFieldProxy(self, self.name or '')
        else:
            # Instance-level access: return the actual value
            return instance._data.get(self.name, self.default)
    
    def __set__(self, instance: Any, value: Any) -> None:
        """Set the field value on an instance.
        
        Args:
            instance: The instance to set the value on
            value: The value to set
        """
        if hasattr(instance, '__setattr__'):
            # Use the document's __setattr__ to ensure proper tracking
            instance.__setattr__(self.name, value)
        else:
            # Fallback for direct field assignment
            instance._data[self.name] = value
            if self.name not in instance._changed_fields:
                instance._changed_fields.append(self.name)