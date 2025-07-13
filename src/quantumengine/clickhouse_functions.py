"""
ClickHouse-specific query functions for QuantumORM.

This module provides ClickHouse-specific query functions that can be used
in filter conditions, aggregations, and other query operations.
"""

from typing import Any, List, Optional, Union
from datetime import datetime, date


class ClickHouseFunction:
    """Base class for ClickHouse functions."""
    
    def __init__(self, *args, **kwargs):
        """Initialize function with arguments."""
        self.args = args
        self.kwargs = kwargs
    
    def to_sql(self) -> str:
        """Convert function to ClickHouse SQL string."""
        raise NotImplementedError("Subclasses must implement to_sql()")
    
    def __str__(self) -> str:
        """String representation is the SQL."""
        return self.to_sql()


# Array Functions
class has(ClickHouseFunction):
    """Check if array contains value.
    
    Example:
        >>> Product.objects.filter(tags=has('electronics'))
        # WHERE has(tags, 'electronics')
    """
    
    def __init__(self, array_field: str, value: Any):
        self.array_field = array_field
        self.value = value
    
    def to_sql(self) -> str:
        if isinstance(self.value, str):
            return f"has({self.array_field}, '{self.value}')"
        else:
            return f"has({self.array_field}, {self.value})"


class hasAll(ClickHouseFunction):
    """Check if array contains all values.
    
    Example:
        >>> Product.objects.filter(tags=hasAll(['electronics', 'phones']))
        # WHERE hasAll(tags, ['electronics', 'phones'])
    """
    
    def __init__(self, array_field: str, values: List[Any]):
        self.array_field = array_field
        self.values = values
    
    def to_sql(self) -> str:
        formatted_values = []
        for value in self.values:
            if isinstance(value, str):
                formatted_values.append(f"'{value}'")
            else:
                formatted_values.append(str(value))
        
        values_str = f"[{', '.join(formatted_values)}]"
        return f"hasAll({self.array_field}, {values_str})"


class hasAny(ClickHouseFunction):
    """Check if array contains any of the values.
    
    Example:
        >>> Product.objects.filter(tags=hasAny(['electronics', 'computers']))
        # WHERE hasAny(tags, ['electronics', 'computers'])
    """
    
    def __init__(self, array_field: str, values: List[Any]):
        self.array_field = array_field
        self.values = values
    
    def to_sql(self) -> str:
        formatted_values = []
        for value in self.values:
            if isinstance(value, str):
                formatted_values.append(f"'{value}'")
            else:
                formatted_values.append(str(value))
        
        values_str = f"[{', '.join(formatted_values)}]"
        return f"hasAny({self.array_field}, {values_str})"


class arrayElement(ClickHouseFunction):
    """Get array element by index (1-based).
    
    Example:
        >>> Product.objects.filter(first_tag=arrayElement('tags', 1))
        # WHERE arrayElement(tags, 1) = 'first_tag'
    """
    
    def __init__(self, array_field: str, index: int):
        self.array_field = array_field
        self.index = index
    
    def to_sql(self) -> str:
        return f"arrayElement({self.array_field}, {self.index})"


class length(ClickHouseFunction):
    """Get array or string length.
    
    Example:
        >>> Product.objects.filter(num_tags=length('tags'))
        # WHERE length(tags) > 5
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"length({self.field})"


# Date and Time Functions
class toDate(ClickHouseFunction):
    """Convert datetime to date.
    
    Example:
        >>> Sales.objects.filter(sale_date=toDate('created_at'))
        # WHERE toDate(created_at) = '2023-01-01'
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"toDate({self.field})"


class toDateTime(ClickHouseFunction):
    """Convert to datetime.
    
    Example:
        >>> Sales.objects.filter(exact_time=toDateTime('date_string'))
        # WHERE toDateTime(date_string) > '2023-01-01 12:00:00'
    """
    
    def __init__(self, field: str, timezone: Optional[str] = None):
        self.field = field
        self.timezone = timezone
    
    def to_sql(self) -> str:
        if self.timezone:
            return f"toDateTime({self.field}, '{self.timezone}')"
        else:
            return f"toDateTime({self.field})"


class toYYYYMM(ClickHouseFunction):
    """Convert date to YYYYMM format.
    
    Example:
        >>> Sales.objects.filter(month=toYYYYMM('created_at'))
        # WHERE toYYYYMM(created_at) = 202301
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"toYYYYMM({self.field})"


class toYYYYMMDD(ClickHouseFunction):
    """Convert date to YYYYMMDD format.
    
    Example:
        >>> Sales.objects.filter(day=toYYYYMMDD('created_at'))
        # WHERE toYYYYMMDD(created_at) = 20230101
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"toYYYYMMDD({self.field})"


class formatDateTime(ClickHouseFunction):
    """Format datetime with custom format.
    
    Example:
        >>> Sales.objects.filter(formatted_date=formatDateTime('created_at', '%Y-%m-%d'))
        # WHERE formatDateTime(created_at, '%Y-%m-%d') = '2023-01-01'
    """
    
    def __init__(self, field: str, format_string: str):
        self.field = field
        self.format_string = format_string
    
    def to_sql(self) -> str:
        return f"formatDateTime({self.field}, '{self.format_string}')"


class now(ClickHouseFunction):
    """Get current timestamp.
    
    Example:
        >>> Sales.objects.filter(created_at__lt=now())
        # WHERE created_at < now()
    """
    
    def to_sql(self) -> str:
        return "now()"


class yesterday(ClickHouseFunction):
    """Get yesterday's date.
    
    Example:
        >>> Sales.objects.filter(sale_date=yesterday())
        # WHERE sale_date = yesterday()
    """
    
    def to_sql(self) -> str:
        return "yesterday()"


class today(ClickHouseFunction):
    """Get today's date.
    
    Example:
        >>> Sales.objects.filter(sale_date=today())
        # WHERE sale_date = today()
    """
    
    def to_sql(self) -> str:
        return "today()"


# String Functions
class lower(ClickHouseFunction):
    """Convert string to lowercase.
    
    Example:
        >>> User.objects.filter(email=lower('email'))
        # WHERE lower(email) = 'user@example.com'
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"lower({self.field})"


class upper(ClickHouseFunction):
    """Convert string to uppercase.
    
    Example:
        >>> Product.objects.filter(sku=upper('sku'))
        # WHERE upper(sku) = 'PROD-123'
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"upper({self.field})"


class substring(ClickHouseFunction):
    """Extract substring.
    
    Example:
        >>> Product.objects.filter(prefix=substring('sku', 1, 4))
        # WHERE substring(sku, 1, 4) = 'PROD'
    """
    
    def __init__(self, field: str, start: int, length: Optional[int] = None):
        self.field = field
        self.start = start
        self.length = length
    
    def to_sql(self) -> str:
        if self.length is not None:
            return f"substring({self.field}, {self.start}, {self.length})"
        else:
            return f"substring({self.field}, {self.start})"


class position(ClickHouseFunction):
    """Find position of substring.
    
    Example:
        >>> Product.objects.filter(hyphen_pos=position('sku', '-'))
        # WHERE position(sku, '-') > 0
    """
    
    def __init__(self, field: str, needle: str):
        self.field = field
        self.needle = needle
    
    def to_sql(self) -> str:
        return f"position({self.field}, '{self.needle}')"


# Math Functions
class round_(ClickHouseFunction):
    """Round number to specified decimal places.
    
    Example:
        >>> Product.objects.filter(rounded_price=round_('price', 2))
        # WHERE round(price, 2) = 99.99
    """
    
    def __init__(self, field: str, precision: int = 0):
        self.field = field
        self.precision = precision
    
    def to_sql(self) -> str:
        if self.precision == 0:
            return f"round({self.field})"
        else:
            return f"round({self.field}, {self.precision})"


class abs_(ClickHouseFunction):
    """Get absolute value.
    
    Example:
        >>> Product.objects.filter(price_diff=abs_('price - cost'))
        # WHERE abs(price - cost) > 10
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"abs({self.field})"


class floor_(ClickHouseFunction):
    """Floor function.
    
    Example:
        >>> Product.objects.filter(whole_price=floor_('price'))
        # WHERE floor(price) = 99
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"floor({self.field})"


class ceil_(ClickHouseFunction):
    """Ceiling function.
    
    Example:
        >>> Product.objects.filter(ceil_price=ceil_('price'))
        # WHERE ceil(price) = 100
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"ceil({self.field})"


# Conditional Functions
class if_(ClickHouseFunction):
    """Conditional expression.
    
    Example:
        >>> Product.objects.filter(status=if_('price > 100', 'expensive', 'cheap'))
        # WHERE if(price > 100, 'expensive', 'cheap') = 'expensive'
    """
    
    def __init__(self, condition: str, then_value: Any, else_value: Any):
        self.condition = condition
        self.then_value = then_value
        self.else_value = else_value
    
    def to_sql(self) -> str:
        def format_value(value):
            if isinstance(value, str):
                return f"'{value}'"
            else:
                return str(value)
        
        return f"if({self.condition}, {format_value(self.then_value)}, {format_value(self.else_value)})"


class multiIf(ClickHouseFunction):
    """Multi-condition IF statement.
    
    Example:
        >>> Product.objects.filter(
        ...     category=multiIf([
        ...         ('price < 50', 'budget'),
        ...         ('price < 200', 'mid-range'),
        ...         ('true', 'premium')
        ...     ])
        ... )
        # WHERE multiIf(price < 50, 'budget', price < 200, 'mid-range', 'premium')
    """
    
    def __init__(self, conditions: List[tuple]):
        self.conditions = conditions
    
    def to_sql(self) -> str:
        parts = []
        for condition, value in self.conditions:
            parts.append(condition)
            if isinstance(value, str):
                parts.append(f"'{value}'")
            else:
                parts.append(str(value))
        
        return f"multiIf({', '.join(parts)})"


# Type Conversion Functions
class toString(ClickHouseFunction):
    """Convert to string.
    
    Example:
        >>> Product.objects.filter(price_str=toString('price'))
        # WHERE toString(price) = '99.99'
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"toString({self.field})"


class toInt32(ClickHouseFunction):
    """Convert to 32-bit integer.
    
    Example:
        >>> Product.objects.filter(price_int=toInt32('price'))
        # WHERE toInt32(price) = 99
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"toInt32({self.field})"


class toFloat64(ClickHouseFunction):
    """Convert to 64-bit float.
    
    Example:
        >>> Product.objects.filter(price_float=toFloat64('price_string'))
        # WHERE toFloat64(price_string) > 99.99
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"toFloat64({self.field})"


# Aggregate functions (for use in materialized views and group by)
class uniq(ClickHouseFunction):
    """Count unique values (approximate).
    
    Example:
        >>> MaterializedField(aggregate=uniq('user_id'))
        # SELECT uniq(user_id) FROM ...
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"uniq({self.field})"


class uniqExact(ClickHouseFunction):
    """Count unique values (exact).
    
    Example:
        >>> MaterializedField(aggregate=uniqExact('user_id'))
        # SELECT uniqExact(user_id) FROM ...
    """
    
    def __init__(self, field: str):
        self.field = field
    
    def to_sql(self) -> str:
        return f"uniqExact({self.field})"


# Export all functions
__all__ = [
    # Array functions
    'has', 'hasAll', 'hasAny', 'arrayElement', 'length',
    # Date/time functions
    'toDate', 'toDateTime', 'toYYYYMM', 'toYYYYMMDD', 'formatDateTime',
    'now', 'yesterday', 'today',
    # String functions
    'lower', 'upper', 'substring', 'position',
    # Math functions
    'round_', 'abs_', 'floor_', 'ceil_',
    # Conditional functions
    'if_', 'multiIf',
    # Type conversion functions
    'toString', 'toInt32', 'toFloat64',
    # Aggregate functions
    'uniq', 'uniqExact'
]