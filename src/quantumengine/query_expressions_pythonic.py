"""
Pythonic query expression system for QuantumEngine.

This module provides a more Pythonic query syntax using operator overloading,
allowing expressions like User.age > 18 instead of age__gt=18.
"""

from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING
import json

if TYPE_CHECKING:
    from .fields.base import Field


class FieldExpression:
    """Represents a query expression on a field using Pythonic operators."""
    
    def __init__(self, field_name: str, operator: str, value: Any):
        """Initialize a field expression.
        
        Args:
            field_name: Name of the field
            operator: Operator symbol (>, <, >=, etc.)
            value: Value to compare against
        """
        self.field_name = field_name
        self.operator = operator
        self.value = value
    
    def __repr__(self):
        return f"<FieldExpression: {self.field_name} {self.operator} {self.value}>"
    
    def __and__(self, other: Union['FieldExpression', 'CompoundExpression']) -> 'CompoundExpression':
        """Combine with another expression using AND."""
        return CompoundExpression(self, 'AND', other)
    
    def __or__(self, other: Union['FieldExpression', 'CompoundExpression']) -> 'CompoundExpression':
        """Combine with another expression using OR."""
        return CompoundExpression(self, 'OR', other)
    
    def __invert__(self) -> 'CompoundExpression':
        """Negate this expression using NOT."""
        return CompoundExpression(self, 'NOT', None)
    
    def to_django_kwargs(self) -> Dict[str, Any]:
        """Convert to Django-style filter kwargs.
        
        Returns:
            Dictionary with Django-style field__operator syntax
        """
        # Map Pythonic operators to Django suffixes
        op_map = {
            '>': '__gt',
            '<': '__lt',
            '>=': '__gte',
            '<=': '__lte',
            '==': '',  # No suffix for equality
            '!=': '__ne',
            'in': '__in',
            'not in': '__nin',
            'contains': '__contains',
            'startswith': '__startswith',
            'endswith': '__endswith',
            'regex': '__regex',
        }
        
        suffix = op_map.get(self.operator, '')
        key = f"{self.field_name}{suffix}"
        return {key: self.value}
    
    def to_query_condition(self) -> tuple:
        """Convert to internal query condition format.
        
        Returns:
            Tuple of (field_name, operator, value) for internal processing
        """
        # Map Pythonic operators to internal operators
        op_map = {
            '>': '>',
            '<': '<',
            '>=': '>=',
            '<=': '<=',
            '==': '=',
            '!=': '!=',
            'in': 'INSIDE',
            'not in': 'NOT INSIDE',
            'contains': 'CONTAINS',
            'startswith': 'STARTSWITH',
            'endswith': 'ENDSWITH',
            'regex': 'REGEX',
        }
        
        internal_op = op_map.get(self.operator, '=')
        return (self.field_name, internal_op, self.value)


class CompoundExpression:
    """Represents a compound query expression with AND/OR/NOT logic."""
    
    def __init__(self, left: Union[FieldExpression, 'CompoundExpression'], 
                 operator: str, 
                 right: Optional[Union[FieldExpression, 'CompoundExpression']]):
        """Initialize a compound expression.
        
        Args:
            left: Left side expression
            operator: Logical operator (AND, OR, NOT)
            right: Right side expression (None for NOT)
        """
        self.left = left
        self.operator = operator
        self.right = right
    
    def __repr__(self):
        if self.operator == 'NOT':
            return f"<CompoundExpression: NOT {self.left}>"
        return f"<CompoundExpression: {self.left} {self.operator} {self.right}>"
    
    def __and__(self, other: Union[FieldExpression, 'CompoundExpression']) -> 'CompoundExpression':
        """Combine with another expression using AND."""
        return CompoundExpression(self, 'AND', other)
    
    def __or__(self, other: Union[FieldExpression, 'CompoundExpression']) -> 'CompoundExpression':
        """Combine with another expression using OR."""
        return CompoundExpression(self, 'OR', other)
    
    def __invert__(self) -> 'CompoundExpression':
        """Negate this expression using NOT."""
        return CompoundExpression(self, 'NOT', None)
    
    def to_q_object(self):
        """Convert to a Q object for compatibility with existing system.
        
        Returns:
            Q object representing this compound expression
        """
        from .query_expressions import Q
        
        if self.operator == 'NOT':
            left_q = self._expr_to_q(self.left)
            return ~left_q
        
        left_q = self._expr_to_q(self.left)
        right_q = self._expr_to_q(self.right)
        
        if self.operator == 'AND':
            return left_q & right_q
        elif self.operator == 'OR':
            return left_q | right_q
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
    
    def _expr_to_q(self, expr: Union[FieldExpression, 'CompoundExpression']):
        """Convert an expression to a Q object.
        
        Args:
            expr: Expression to convert
            
        Returns:
            Q object
        """
        from .query_expressions import Q
        
        if isinstance(expr, FieldExpression):
            return Q(**expr.to_django_kwargs())
        elif isinstance(expr, CompoundExpression):
            return expr.to_q_object()
        else:
            raise ValueError(f"Unknown expression type: {type(expr)}")


class QueryableFieldProxy:
    """Proxy object for fields that enables Pythonic query syntax."""
    
    def __init__(self, field: 'Field', field_name: str):
        """Initialize the queryable field proxy.
        
        Args:
            field: The actual field instance
            field_name: Name of the field
        """
        self._field = field
        self._field_name = field_name
    
    def __repr__(self):
        return f"<QueryableField: {self._field_name}>"
    
    # Comparison operators
    def __gt__(self, other: Any) -> FieldExpression:
        """Greater than comparison."""
        return FieldExpression(self._field_name, '>', other)
    
    def __lt__(self, other: Any) -> FieldExpression:
        """Less than comparison."""
        return FieldExpression(self._field_name, '<', other)
    
    def __ge__(self, other: Any) -> FieldExpression:
        """Greater than or equal comparison."""
        return FieldExpression(self._field_name, '>=', other)
    
    def __le__(self, other: Any) -> FieldExpression:
        """Less than or equal comparison."""
        return FieldExpression(self._field_name, '<=', other)
    
    def __eq__(self, other: Any) -> FieldExpression:
        """Equality comparison."""
        return FieldExpression(self._field_name, '==', other)
    
    def __ne__(self, other: Any) -> FieldExpression:
        """Not equal comparison."""
        return FieldExpression(self._field_name, '!=', other)
    
    # String operations
    def contains(self, value: str) -> FieldExpression:
        """Check if field contains a value."""
        return FieldExpression(self._field_name, 'contains', value)
    
    def startswith(self, value: str) -> FieldExpression:
        """Check if field starts with a value."""
        return FieldExpression(self._field_name, 'startswith', value)
    
    def endswith(self, value: str) -> FieldExpression:
        """Check if field ends with a value."""
        return FieldExpression(self._field_name, 'endswith', value)
    
    def matches(self, pattern: str) -> FieldExpression:
        """Check if field matches a regex pattern."""
        return FieldExpression(self._field_name, 'regex', pattern)
    
    # List operations
    def in_(self, values: List[Any]) -> FieldExpression:
        """Check if field value is in a list of values."""
        return FieldExpression(self._field_name, 'in', values)
    
    def not_in(self, values: List[Any]) -> FieldExpression:
        """Check if field value is not in a list of values."""
        return FieldExpression(self._field_name, 'not in', values)
    
    # Nested field access for DictField
    def __getattr__(self, name: str) -> 'NestedFieldProxy':
        """Access nested fields in DictField."""
        return NestedFieldProxy(f"{self._field_name}.{name}")
    
    def __getitem__(self, key: str) -> 'NestedFieldProxy':
        """Access nested fields using dictionary syntax."""
        return NestedFieldProxy(f"{self._field_name}.{key}")


class NestedFieldProxy:
    """Proxy for nested field access in DictFields."""
    
    def __init__(self, field_path: str):
        """Initialize nested field proxy.
        
        Args:
            field_path: Dot-separated path to the nested field
        """
        self._field_path = field_path
    
    def __repr__(self):
        return f"<NestedField: {self._field_path}>"
    
    # All the same operators as QueryableFieldProxy
    def __gt__(self, other: Any) -> FieldExpression:
        return FieldExpression(self._field_path, '>', other)
    
    def __lt__(self, other: Any) -> FieldExpression:
        return FieldExpression(self._field_path, '<', other)
    
    def __ge__(self, other: Any) -> FieldExpression:
        return FieldExpression(self._field_path, '>=', other)
    
    def __le__(self, other: Any) -> FieldExpression:
        return FieldExpression(self._field_path, '<=', other)
    
    def __eq__(self, other: Any) -> FieldExpression:
        return FieldExpression(self._field_path, '==', other)
    
    def __ne__(self, other: Any) -> FieldExpression:
        return FieldExpression(self._field_path, '!=', other)
    
    def contains(self, value: str) -> FieldExpression:
        return FieldExpression(self._field_path, 'contains', value)
    
    def startswith(self, value: str) -> FieldExpression:
        return FieldExpression(self._field_path, 'startswith', value)
    
    def endswith(self, value: str) -> FieldExpression:
        return FieldExpression(self._field_path, 'endswith', value)
    
    def matches(self, pattern: str) -> FieldExpression:
        return FieldExpression(self._field_path, 'regex', pattern)
    
    def in_(self, values: List[Any]) -> FieldExpression:
        return FieldExpression(self._field_path, 'in', values)
    
    def not_in(self, values: List[Any]) -> FieldExpression:
        return FieldExpression(self._field_path, 'not in', values)
    
    # Further nesting
    def __getattr__(self, name: str) -> 'NestedFieldProxy':
        return NestedFieldProxy(f"{self._field_path}.{name}")
    
    def __getitem__(self, key: str) -> 'NestedFieldProxy':
        return NestedFieldProxy(f"{self._field_path}.{key}")


def is_pythonic_expression(obj: Any) -> bool:
    """Check if an object is a Pythonic query expression.
    
    Args:
        obj: Object to check
        
    Returns:
        True if the object is a FieldExpression or CompoundExpression
    """
    return isinstance(obj, (FieldExpression, CompoundExpression))