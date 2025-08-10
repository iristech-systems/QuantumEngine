"""Materialized documents for QuantumORM.

MaterializedDocument provides a Document-like interface for creating and querying
materialized views across different backends (SurrealDB, ClickHouse).

Example:
    class DailySalesSummary(MaterializedDocument):
        class Meta:
            source = SalesDocument
            backend = 'clickhouse'
            
        # Dimensions (grouping fields)
        date = DateField(source='date_collected', transform=ToDate)
        seller_name = LowCardinalityField(source='seller_name')
        
        # Metrics (aggregation fields)
        total_sales = DecimalField(aggregate=Sum('offer_price'))
        transaction_count = IntField(aggregate=Count())
        avg_price = DecimalField(aggregate=Avg('offer_price'))
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Type, Union, ClassVar, Callable
from dataclasses import dataclass
import inspect

from .document import Document, DocumentMetaclass
from .fields import Field
from .query import QuerySet
from .connection_api import ConnectionRegistry


class AggregateFunction:
    """Base class for aggregate functions."""
    
    def __init__(self, field: Optional[str] = None):
        self.field = field
    
    def __str__(self) -> str:
        """Return the string representation for the backend."""
        raise NotImplementedError


class Count(AggregateFunction):
    """Count aggregation."""
    
    def __str__(self) -> str:
        if self.field:
            return f"COUNT({self.field})"
        return "COUNT(*)"


class Sum(AggregateFunction):
    """Sum aggregation."""
    
    def __str__(self) -> str:
        if not self.field:
            raise ValueError("Sum requires a field")
        return f"SUM({self.field})"


class Avg(AggregateFunction):
    """Average aggregation."""
    
    def __str__(self) -> str:
        if not self.field:
            raise ValueError("Avg requires a field")
        return f"AVG({self.field})"


class Min(AggregateFunction):
    """Minimum aggregation."""
    
    def __str__(self) -> str:
        if not self.field:
            raise ValueError("Min requires a field")
        return f"MIN({self.field})"


class Max(AggregateFunction):
    """Maximum aggregation."""
    
    def __str__(self) -> str:
        if not self.field:
            raise ValueError("Max requires a field")
        return f"MAX({self.field})"


class CountDistinct(AggregateFunction):
    """Count distinct aggregation."""
    
    def __str__(self) -> str:
        if not self.field:
            raise ValueError("CountDistinct requires a field")
        return f"COUNT(DISTINCT {self.field})"


class Variance(AggregateFunction):
    """Variance aggregation."""
    
    def __str__(self) -> str:
        if not self.field:
            raise ValueError("Variance requires a field")
        return f"VAR({self.field})"


class StdDev(AggregateFunction):
    """Standard deviation aggregation."""
    
    def __str__(self) -> str:
        if not self.field:
            raise ValueError("StdDev requires a field")
        return f"STDDEV({self.field})"


class FieldTransform:
    """Base class for field transformations."""
    
    def __init__(self, field: str):
        self.field = field
    
    def __str__(self) -> str:
        """Return the string representation for the backend."""
        raise NotImplementedError


class ToDate(FieldTransform):
    """Convert to date (ClickHouse-specific)."""
    
    def __str__(self) -> str:
        return f"toDate({self.field})"


class ToYearMonth(FieldTransform):
    """Convert to YYYYMM format (ClickHouse-specific)."""
    
    def __str__(self) -> str:
        return f"toYYYYMM({self.field})"


class MaterializedField(Field):
    """Field for materialized documents with aggregation support."""
    
    def __init__(
        self,
        source: Optional[str] = None,
        aggregate: Optional[AggregateFunction] = None,
        transform: Optional[Union[FieldTransform, Callable]] = None,
        **kwargs
    ):
        """Initialize a materialized field.
        
        Args:
            source: Source field name from the base document
            aggregate: Aggregation function to apply
            transform: Transformation to apply to the source field
            **kwargs: Additional field arguments
        """
        super().__init__(**kwargs)
        self.source = source
        self.aggregate = aggregate
        self.transform = transform
        
        # Set Python type based on aggregation or assume Any
        if aggregate:
            # Infer type from aggregation
            if isinstance(aggregate, (Count, CountDistinct)):
                self.py_type = int
            elif isinstance(aggregate, (Sum, Avg, Min, Max, Variance, StdDev)):
                self.py_type = float  # Default to float for numeric aggregations
            else:
                self.py_type = object
        else:
            self.py_type = str  # Default for dimension fields
        
        # If this is an aggregation field, it shouldn't be required
        if self.aggregate:
            self.required = False


class MaterializedDocumentMetaclass(DocumentMetaclass):
    """Metaclass for MaterializedDocument classes."""
    
    def __new__(mcs, name: str, bases: tuple, attrs: Dict[str, Any]) -> Type:
        """Create a new MaterializedDocument class."""
        
        # Skip processing for the base MaterializedDocument class
        if name == 'MaterializedDocument' and attrs.get('__module__') == __name__:
            return super().__new__(mcs, name, bases, attrs)
        
        # Process Meta class
        meta = attrs.get('Meta', type('Meta', (), {}))
        source_model = getattr(meta, 'source', None)
        
        if not source_model and name != 'MaterializedDocument':
            raise ValueError(f"MaterializedDocument {name} must specify source model in Meta")
        
        # Extract filters if defined
        filters_class = attrs.get('Filters', None)
        if filters_class:
            filters = {}
            for attr_name, attr_value in inspect.getmembers(filters_class):
                if not attr_name.startswith('_'):
                    filters[attr_name] = attr_value
            attrs['_filters'] = filters
        else:
            # Check for where clause in Meta
            where_clause = getattr(meta, 'where', None)
            if where_clause:
                # Parse simple where clause like "processing_status = 'pending'"
                attrs['_where_clause'] = where_clause
            attrs['_filters'] = {}
        
        # Extract having conditions if defined
        having_class = attrs.get('Having', None)
        if having_class:
            having = {}
            for attr_name, attr_value in inspect.getmembers(having_class):
                if not attr_name.startswith('_'):
                    having[attr_name] = attr_value
            attrs['_having'] = having
        else:
            attrs['_having'] = {}
        
        # Separate dimension and metric fields
        dimension_fields = {}
        metric_fields = {}
        
        for attr_name, attr_value in list(attrs.items()):
            if isinstance(attr_value, Field):
                # Convert regular fields to MaterializedFields if needed
                if not isinstance(attr_value, MaterializedField):
                    # This is a dimension field
                    source = attr_value.db_field or attr_name
                    new_field = MaterializedField(
                        source=source,
                        **{k: v for k, v in attr_value.__dict__.items() 
                           if k not in ['name', 'owner_document']}
                    )
                    attrs[attr_name] = new_field
                    dimension_fields[attr_name] = new_field
                else:
                    # Check if it's a metric or dimension
                    if attr_value.aggregate:
                        metric_fields[attr_name] = attr_value
                    else:
                        dimension_fields[attr_name] = attr_value
        
        attrs['_dimension_fields'] = dimension_fields
        attrs['_metric_fields'] = metric_fields
        
        # Create the class using DocumentMetaclass
        new_class = super().__new__(mcs, name, bases, attrs)
        
        return new_class


class MaterializedDocument(Document, metaclass=MaterializedDocumentMetaclass):
    """Base class for materialized documents (views).
    
    MaterializedDocument provides a Document-like interface for creating
    and querying materialized views across different backends.
    """
    
    _dimension_fields: ClassVar[Dict[str, MaterializedField]] = {}
    _metric_fields: ClassVar[Dict[str, MaterializedField]] = {}
    _filters: ClassVar[Dict[str, Any]] = {}
    _having: ClassVar[Dict[str, Any]] = {}
    _where_clause: ClassVar[Optional[str]] = None
    
    @classmethod
    async def create_view(cls) -> None:
        """Create the materialized view in the database."""
        # Get the backend from the source document
        meta_class = getattr(cls, 'Meta', None)
        source_model = getattr(meta_class, 'source', None) if meta_class else None
        
        if source_model:
            backend = source_model._get_backend()
        else:
            backend = cls._get_backend()
        
        if backend.supports_materialized_views():
            await backend.create_materialized_view(cls)
        else:
            raise NotImplementedError(
                f"Backend {backend.__class__.__name__} doesn't support materialized views"
            )
    
    @classmethod
    async def drop_view(cls) -> None:
        """Drop the materialized view from the database."""
        backend = cls._get_backend()
        
        if backend.supports_materialized_views():
            await backend.drop_materialized_view(cls)
        else:
            raise NotImplementedError(
                f"Backend {backend.__class__.__name__} doesn't support materialized views"
            )
    
    @classmethod
    async def refresh_view(cls) -> None:
        """Refresh the materialized view (backend-specific behavior)."""
        backend = cls._get_backend()
        
        if backend.supports_materialized_views():
            await backend.refresh_materialized_view(cls)
        else:
            # Backends that don't support materialized views can't refresh them
            raise NotImplementedError(
                f"Backend {backend.__class__.__name__} doesn't support materialized views"
            )
    
    @classmethod
    def _build_source_query(cls) -> str:
        """Build the source query for the materialized view.
        
        This method constructs the SELECT query that defines the view's data.
        """
        # Get source model from Meta class
        meta_class = getattr(cls, 'Meta', None)
        source_model = getattr(meta_class, 'source', None) if meta_class else None
        
        if not source_model:
            raise ValueError("No source model defined")
        
        # Build SELECT clause
        select_parts = []
        group_by_parts = []
        
        # Add dimension fields
        for field_name, field in cls._dimension_fields.items():
            source_field = field.source or field_name
            
            if field.transform:
                if isinstance(field.transform, FieldTransform):
                    select_expr = str(field.transform).replace(
                        field.transform.field, source_field
                    )
                else:
                    # Callable transform
                    select_expr = field.transform(source_field)
            else:
                select_expr = source_field
            
            select_parts.append(f"{select_expr} AS {field_name}")
            group_by_parts.append(field_name)
        
        # Add metric fields
        for field_name, field in cls._metric_fields.items():
            if field.aggregate:
                if field.source:
                    agg_expr = str(field.aggregate).replace(
                        field.aggregate.field or '*', field.source
                    )
                else:
                    agg_expr = str(field.aggregate)
                
                select_parts.append(f"{agg_expr} AS {field_name}")
        
        # Build FROM clause
        table_name = source_model._meta.get('table_name', source_model.__name__.lower())
        
        # Build WHERE clause from filters
        where_parts = []
        for filter_name, filter_value in cls._filters.items():
            # Convert Django-style filters to SQL
            if '__' in filter_name:
                field, op = filter_name.rsplit('__', 1)
                formatted_value = cls._format_filter_value(filter_value)
                if op == 'gte':
                    where_parts.append(f"{field} >= {formatted_value}")
                elif op == 'lte':
                    where_parts.append(f"{field} <= {formatted_value}")
                elif op == 'gt':
                    where_parts.append(f"{field} > {formatted_value}")
                elif op == 'lt':
                    where_parts.append(f"{field} < {formatted_value}")
                else:
                    where_parts.append(f"{field} = {formatted_value}")
            else:
                formatted_value = cls._format_filter_value(filter_value)
                where_parts.append(f"{filter_name} = {formatted_value}")
        
        # Build query
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        # Add WHERE clause from either filters or direct where clause
        if hasattr(cls, '_where_clause') and cls._where_clause:
            query += f" WHERE {cls._where_clause}"
        elif where_parts:
            query += f" WHERE {' AND '.join(where_parts)}"
        
        if group_by_parts:
            query += f" GROUP BY {', '.join(group_by_parts)}"
        
        # Add HAVING clause
        having_parts = []
        for having_name, having_value in cls._having.items():
            if '__' in having_name:
                field, op = having_name.rsplit('__', 1)
                if op == 'gte':
                    having_parts.append(f"{field} >= {having_value}")
                # ... other operators
            else:
                having_parts.append(f"{having_name} = {having_value}")
        
        if having_parts:
            query += f" HAVING {' AND '.join(having_parts)}"
        
        return query
    
    @classmethod
    def _format_filter_value(cls, value: Any) -> str:
        """Format a filter value for SQL."""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif isinstance(value, (int, float)):
            return str(value)
        elif hasattr(value, 'strftime'):  # datetime objects
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            return repr(value)
    
    async def save(self, **kwargs):
        """MaterializedDocuments are read-only."""
        raise NotImplementedError("MaterializedDocuments are read-only")
    
    async def delete(self):
        """MaterializedDocuments are read-only."""
        raise NotImplementedError("MaterializedDocuments are read-only")