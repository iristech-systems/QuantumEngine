"""
QuantumEngine: Multi-backend Object-Document Mapper with both sync and async support
"""

from .connection_api import (
    SurrealEngineAsyncConnection,
    SurrealEngineSyncConnection,
    ConnectionRegistry,
    create_connection,
    BaseSurrealEngineConnection
)
from .connection import PoolConfig

# For backward compatibility
SurrealEngineConnection = SurrealEngineAsyncConnection
from .schemaless import SurrealEngine
from .document import Document, RelationDocument
from .exceptions import (
    DoesNotExist,
    MultipleObjectsReturned,
    ValidationError,
)
from .fields import (
    BooleanField,
    DateTimeField,
    DictField,
    Field,
    FloatField,
    GeometryField,
    IntField,
    ListField,
    NumberField,
    ReferenceField,
    RelationField,
    StringField,
    FutureField,
    DecimalField,
    DurationField,
    OptionField,
    LiteralField,
    RangeField,
    SetField,
    TimeSeriesField,
    EmailField,
    URLField,
    IPAddressField,
    SlugField,
    ChoiceField
)
from .materialized_view import (
    MaterializedView, 
    Aggregation, 
    Count, 
    Mean, 
    Sum, 
    Min, 
    Max, 
    ArrayCollect,
    Median,
    StdDev,
    Variance,
    Percentile,
    Distinct,
    GroupConcat
)
from .materialized_document import (
    MaterializedDocument,
    MaterializedField,
    Count as MaterializedCount,
    Sum as MaterializedSum,
    Avg as MaterializedAvg,
    Min as MaterializedMin,
    Max as MaterializedMax,
    CountDistinct,
    Variance as MaterializedVariance,
    StdDev as MaterializedStdDev,
    ToDate,
    ToYearMonth
)
from .query import QuerySet, RelationQuerySet
from .query_expressions import Q, QueryExpression
from .aggregation import AggregationPipeline
from .schema import (
    get_document_classes,
    create_tables_from_module,
    create_tables_from_module_sync,
    generate_schema_statements,
    generate_schema_statements_from_module,
    generate_drop_statements,
    generate_drop_statements_from_module,
    generate_migration_statements,
    drop_tables_from_module,
    drop_tables_from_module_sync
)
from .datagrid_api import (
    DataGridQueryBuilder,
    get_grid_data,
    get_grid_data_sync,
    parse_datatables_params,
    format_datatables_response
)

__version__ = "0.2.1"
__all__ = [
    "SurrealEngine",
    "SurrealEngineAsyncConnection",
    "SurrealEngineSyncConnection",
    "SurrealEngineConnection",  # For backward compatibility
    "BaseSurrealEngineConnection",
    "create_connection",
    "ConnectionRegistry",
    "Document",
    "RelationDocument",
    "DoesNotExist",
    "MultipleObjectsReturned",
    "ValidationError",
    "Field",
    "StringField",
    "NumberField",
    "IntField",
    "FloatField",
    "BooleanField",
    "DateTimeField",
    "ListField",
    "DictField",
    "ReferenceField",
    "RelationField",
    "GeometryField",
    "QuerySet",
    "RelationQuerySet",
    "Q",
    "QueryExpression",
    "DurationField",
    "OptionField",
    "LiteralField",
    "RangeField",
    "SetField",
    "TimeSeriesField",
    "EmailField",
    "URLField",
    "IPAddressField",
    "SlugField",
    "ChoiceField",
    "MaterializedView",
    "MaterializedDocument",
    "MaterializedField",
    # Aggregation classes
    "AggregationPipeline",
    "Aggregation",
    "Count",
    "Mean",
    "Sum",
    "Min",
    "Max",
    "ArrayCollect",
    "Median",
    "StdDev",
    "Variance",
    "Percentile",
    "Distinct",
    "GroupConcat",
    # Schema generation functions
    "get_document_classes",
    "create_tables_from_module",
    "create_tables_from_module_sync",
    "generate_schema_statements",
    "generate_schema_statements_from_module",
    "generate_drop_statements",
    "generate_drop_statements_from_module",
    "generate_migration_statements",
    "drop_tables_from_module",
    "drop_tables_from_module_sync",
    # DataGrid helpers
    "DataGridQueryBuilder",
    "get_grid_data",
    "get_grid_data_sync",
    "parse_datatables_params",
    "format_datatables_response"
]
