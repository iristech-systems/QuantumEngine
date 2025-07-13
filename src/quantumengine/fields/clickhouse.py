"""
ClickHouse-specific field types for QuantumORM.

This module contains field types that are optimized for or specific to ClickHouse backend.
"""

from typing import Any, Optional, List, Dict
from .scalar import StringField
from .base import Field


class LowCardinalityField(StringField):
    """ClickHouse LowCardinality field for enum-like string values.
    
    LowCardinality is a ClickHouse optimization for string columns with a small
    number of distinct values (typically < 10,000). It uses dictionary encoding
    to reduce memory usage and improve query performance.
    
    This field automatically maps to LowCardinality(String) in ClickHouse and
    falls back to regular String type in other backends.
    
    Example:
        >>> class MarketplaceData(Document):
        ...     seller_name = LowCardinalityField(required=True)
        ...     marketplace = LowCardinalityField(choices=['Amazon', 'eBay', 'Walmart'])
        ...     
        ...     class Meta:
        ...         backend = 'clickhouse'
    """
    
    def __init__(self, base_type: str = 'String', **kwargs: Any) -> None:
        """Initialize a new LowCardinalityField.
        
        Args:
            base_type: The base ClickHouse type to wrap with LowCardinality (default: 'String')
            **kwargs: Additional arguments passed to StringField
        """
        self.base_type = base_type
        super().__init__(**kwargs)
        self.py_type = str
    
    def _to_db_backend_specific(self, value: Any, backend: str) -> Any:
        """Backend-specific conversion for LowCardinality fields.
        
        Args:
            value: The Python value to convert
            backend: The backend type
            
        Returns:
            Backend-appropriate representation
        """
        if value is not None:
            if backend == 'clickhouse':
                # ClickHouse handles LowCardinality optimization automatically
                return str(value)
            elif backend == 'surrealdb':
                # SurrealDB stores as regular string
                return str(value)
            else:
                # Default to string for other backends
                return str(value)
        return value
    
    def _from_db_backend_specific(self, value: Any, backend: str) -> Optional[str]:
        """Backend-specific conversion from database.
        
        Args:
            value: The database value to convert
            backend: The backend type
            
        Returns:
            Python string representation
        """
        if value is not None:
            return str(value)
        return value
    
    def get_clickhouse_type(self) -> str:
        """Get the ClickHouse-specific field type.
        
        Returns:
            The ClickHouse field type definition
        """
        return f"LowCardinality({self.base_type})"
    
    def get_surrealdb_type(self) -> str:
        """Get the SurrealDB fallback field type.
        
        Returns:
            The SurrealDB field type definition
        """
        return "string"


class FixedStringField(StringField):
    """ClickHouse FixedString field for fixed-length strings.
    
    FixedString is a ClickHouse type for strings of exactly N bytes.
    It's more memory-efficient than String for fixed-length data like
    country codes, currency codes, etc.
    
    Example:
        >>> class MarketplaceData(Document):
        ...     currency_code = FixedStringField(length=3)  # USD, EUR, etc.
        ...     country_code = FixedStringField(length=2)   # US, CA, etc.
    """
    
    def __init__(self, length: int, **kwargs: Any) -> None:
        """Initialize a new FixedStringField.
        
        Args:
            length: The exact length in bytes for the string
            **kwargs: Additional arguments passed to StringField
        """
        if length <= 0:
            raise ValueError("FixedString length must be positive")
        
        self.length = length
        # Set max_length for validation
        kwargs['max_length'] = length
        super().__init__(**kwargs)
    
    def validate(self, value: Any) -> str:
        """Validate the fixed string value.
        
        Ensures the string is exactly the specified length.
        
        Args:
            value: The value to validate
            
        Returns:
            The validated string value
            
        Raises:
            ValueError: If the string length doesn't match exactly
        """
        value = super().validate(value)
        if value is not None:
            if len(value) != self.length:
                raise ValueError(
                    f"FixedString field '{self.name}' requires exactly {self.length} "
                    f"characters, got {len(value)}"
                )
        return value
    
    def get_clickhouse_type(self) -> str:
        """Get the ClickHouse-specific field type.
        
        Returns:
            The ClickHouse field type definition
        """
        return f"FixedString({self.length})"
    
    def get_surrealdb_type(self) -> str:
        """Get the SurrealDB fallback field type.
        
        Returns:
            The SurrealDB field type definition
        """
        return "string"


class EnumField(Field[str]):
    """ClickHouse Enum field for predefined string values.
    
    Enum fields in ClickHouse are stored as integers internally but presented
    as strings. They're more efficient than LowCardinality for small sets of
    values that rarely change.
    
    Example:
        >>> class MarketplaceData(Document):
        ...     status = EnumField(values={
        ...         'active': 1,
        ...         'inactive': 2,
        ...         'discontinued': 3
        ...     })
    """
    
    def __init__(self, values: Dict[str, int], **kwargs: Any) -> None:
        """Initialize a new EnumField.
        
        Args:
            values: Dictionary mapping string values to integer codes
            **kwargs: Additional arguments passed to Field
        """
        if not values:
            raise ValueError("Enum field must have at least one value")
        
        self.values = values
        self.reverse_values = {v: k for k, v in values.items()}
        super().__init__(**kwargs)
        self.py_type = str
    
    def validate(self, value: Any) -> str:
        """Validate the enum value.
        
        Args:
            value: The value to validate
            
        Returns:
            The validated enum value
            
        Raises:
            ValueError: If the value is not in the enum
        """
        value = super().validate(value)
        if value is not None:
            if value not in self.values:
                valid_values = ", ".join(f"'{v}'" for v in self.values.keys())
                raise ValueError(
                    f"Value '{value}' for field '{self.name}' must be one of: {valid_values}"
                )
        return value
    
    def _to_db_backend_specific(self, value: Any, backend: str) -> Any:
        """Backend-specific conversion for Enum fields.
        
        Args:
            value: The Python value to convert
            backend: The backend type
            
        Returns:
            Backend-appropriate representation
        """
        if value is not None:
            if backend == 'clickhouse':
                # ClickHouse Enum uses string values directly
                return str(value)
            elif backend == 'surrealdb':
                # SurrealDB stores as regular string
                return str(value)
            else:
                # Default to string for other backends
                return str(value)
        return value
    
    def get_clickhouse_type(self) -> str:
        """Get the ClickHouse-specific field type.
        
        Returns:
            The ClickHouse field type definition
        """
        enum_values = ", ".join(f"'{k}' = {v}" for k, v in self.values.items())
        return f"Enum8({enum_values})"
    
    def get_surrealdb_type(self) -> str:
        """Get the SurrealDB fallback field type.
        
        Returns:
            The SurrealDB field type definition
        """
        return "string"


class CompressionMixin:
    """Mixin class to add compression codec support to fields.
    
    This mixin can be used with string fields to add ClickHouse
    compression codec support.
    """
    
    def __init__(self, codec: Optional[str] = None, **kwargs: Any) -> None:
        """Initialize compression settings.
        
        Args:
            codec: ClickHouse compression codec (e.g., 'ZSTD(3)', 'LZ4', 'NONE')
            **kwargs: Additional arguments passed to parent class
        """
        self.codec = codec
        super().__init__(**kwargs)
    
    def get_compression_suffix(self) -> str:
        """Get the compression codec suffix for ClickHouse.
        
        Returns:
            The codec suffix to append to field type
        """
        if self.codec:
            return f" CODEC({self.codec})"
        return ""


class CompressedStringField(CompressionMixin, StringField):
    """String field with ClickHouse compression codec support.
    
    Useful for large text fields like URLs, descriptions, etc.
    
    Example:
        >>> class MarketplaceData(Document):
        ...     ad_page_url = CompressedStringField(codec="ZSTD(3)")
        ...     product_description = CompressedStringField(codec="LZ4")
    """
    
    def get_clickhouse_type(self) -> str:
        """Get the ClickHouse-specific field type with compression.
        
        Returns:
            The ClickHouse field type definition with codec
        """
        base_type = "String"
        codec_suffix = self.get_compression_suffix()
        if codec_suffix:
            return f"{base_type}{codec_suffix}"
        return base_type


class CompressedLowCardinalityField(CompressionMixin, LowCardinalityField):
    """LowCardinality field with ClickHouse compression codec support.
    
    Example:
        >>> class MarketplaceData(Document):
        ...     category = CompressedLowCardinalityField(codec="LZ4")
    """
    
    def get_clickhouse_type(self) -> str:
        """Get the ClickHouse-specific field type with compression.
        
        Returns:
            The ClickHouse field type definition with codec
        """
        base_type = f"LowCardinality({self.base_type})"
        codec_suffix = self.get_compression_suffix()
        if codec_suffix:
            return f"{base_type}{codec_suffix}"
        return base_type


class ArrayField(Field):
    """ClickHouse Array field with support for nested types and optimizations.
    
    ClickHouse arrays can contain any type including LowCardinality, Nullable,
    and other complex types. This field provides full ClickHouse array support
    with automatic type detection and optimization.
    
    Example:
        >>> class MarketplaceData(Document):
        ...     # Array of low cardinality strings (efficient for repeated values)
        ...     tags = ArrayField(LowCardinalityField())
        ...     
        ...     # Array of integers
        ...     ratings = ArrayField(IntField())
        ...     
        ...     # Array of strings with compression
        ...     urls = ArrayField(StringField(), codec="LZ4")
    """
    
    def __init__(self, element_field: Field, codec: Optional[str] = None, **kwargs: Any) -> None:
        """Initialize a new ArrayField.
        
        Args:
            element_field: The field type for array elements
            codec: Optional ClickHouse compression codec for the array
            **kwargs: Additional arguments passed to Field
        """
        self.element_field = element_field
        self.codec = codec
        super().__init__(**kwargs)
        self.py_type = list
    
    def validate(self, value: Any) -> Optional[List[Any]]:
        """Validate the array value and all elements.
        
        Args:
            value: The value to validate
            
        Returns:
            The validated array value
            
        Raises:
            TypeError: If the value is not a list
            ValueError: If an element fails validation
        """
        value = super().validate(value)
        if value is not None:
            if not isinstance(value, list):
                raise TypeError(f"Expected list for field '{self.name}', got {type(value)}")
            
            # Validate each element using the element field
            validated_elements = []
            for i, element in enumerate(value):
                try:
                    validated_element = self.element_field.validate(element)
                    validated_elements.append(validated_element)
                except (TypeError, ValueError) as e:
                    raise ValueError(f"Error validating element {i} in array field '{self.name}': {str(e)}")
            
            return validated_elements
        return value
    
    def _to_db_backend_specific(self, value: Any, backend: str) -> Any:
        """Backend-specific conversion for Array fields.
        
        Args:
            value: The Python value to convert
            backend: The backend type
            
        Returns:
            Backend-appropriate representation
        """
        if value is not None:
            if backend == 'clickhouse':
                # ClickHouse arrays are stored as native arrays
                # Convert each element using the element field
                converted_elements = []
                for element in value:
                    if hasattr(self.element_field, '_to_db_backend_specific'):
                        converted_element = self.element_field._to_db_backend_specific(element, backend)
                    else:
                        converted_element = self.element_field.to_db(element)
                    converted_elements.append(converted_element)
                return converted_elements
            elif backend == 'surrealdb':
                # SurrealDB stores arrays as native arrays
                converted_elements = []
                for element in value:
                    if hasattr(self.element_field, '_to_db_backend_specific'):
                        converted_element = self.element_field._to_db_backend_specific(element, backend)
                    else:
                        converted_element = self.element_field.to_db(element)
                    converted_elements.append(converted_element)
                return converted_elements
            else:
                # For other backends, convert to JSON string
                import json
                return json.dumps(value)
        return value
    
    def _from_db_backend_specific(self, value: Any, backend: str) -> Optional[List[Any]]:
        """Backend-specific conversion from database.
        
        Args:
            value: The database value to convert
            backend: The backend type
            
        Returns:
            Python list representation
        """
        if value is not None:
            if backend in ['clickhouse', 'surrealdb']:
                # Both ClickHouse and SurrealDB return arrays as lists
                converted_elements = []
                for element in value:
                    if hasattr(self.element_field, '_from_db_backend_specific'):
                        converted_element = self.element_field._from_db_backend_specific(element, backend)
                    else:
                        converted_element = self.element_field.from_db(element)
                    converted_elements.append(converted_element)
                return converted_elements
            else:
                # For other backends, parse from JSON string
                import json
                return json.loads(value)
        return value
    
    def get_clickhouse_type(self) -> str:
        """Get the ClickHouse-specific field type.
        
        Returns:
            The ClickHouse field type definition
        """
        # Get the element type from the element field
        if hasattr(self.element_field, 'get_clickhouse_type'):
            element_type = self.element_field.get_clickhouse_type()
        else:
            # Fallback to basic type mapping
            element_type = self._get_basic_clickhouse_type(self.element_field)
        
        array_type = f"Array({element_type})"
        
        # Add compression codec if specified
        if self.codec:
            array_type += f" CODEC({self.codec})"
        
        return array_type
    
    def get_surrealdb_type(self) -> str:
        """Get the SurrealDB fallback field type.
        
        Returns:
            The SurrealDB field type definition
        """
        return "array"
    
    def _get_basic_clickhouse_type(self, field: Field) -> str:
        """Get basic ClickHouse type for a field.
        
        Args:
            field: The field to get type for
            
        Returns:
            ClickHouse type string
        """
        from .scalar import StringField, IntField, FloatField, BooleanField
        from .datetime import DateTimeField
        from .specialized import DecimalField, UUIDField
        
        if isinstance(field, StringField):
            return "String"
        elif isinstance(field, IntField):
            return "Int64"  # Safe default for integers
        elif isinstance(field, FloatField):
            return "Float64"
        elif isinstance(field, BooleanField):
            return "UInt8"  # ClickHouse uses UInt8 for booleans
        elif isinstance(field, DateTimeField):
            return "DateTime"
        elif isinstance(field, DecimalField):
            return f"Decimal({field.max_digits}, {field.decimal_places})"
        elif isinstance(field, UUIDField):
            return "UUID"
        else:
            return "String"  # Default fallback