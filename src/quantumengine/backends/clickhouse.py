"""ClickHouse backend implementation for SurrealEngine."""

import asyncio
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Type
from functools import partial

from ..connection import ConnectionPoolBase, PoolConfig
from .base import BaseBackend
from ..backends.pools.clickhouse import ClickHouseConnectionPool


class ClickHouseBackend(BaseBackend):
    """ClickHouse backend implementation using clickhouse-connect."""

    def __init__(self, connection_config: dict, pool_config: Optional[PoolConfig] = None):
        """Initialize the ClickHouse backend."""
        super().__init__(connection_config, pool_config)

    def _create_pool(self) -> ConnectionPoolBase:
        """Create the ClickHouse-specific connection pool."""
        return ClickHouseConnectionPool(self.connection_config, self.pool_config)
    
    async def _create_table_op(self, conn: Any, document_class: Type, **kwargs) -> None:
        """Operation to create a table for the document class."""
        table_name = document_class._meta.get('table_name')
        meta = document_class._meta
        engine = kwargs.get('engine', meta.get('engine'))
        if not engine:
            raise ValueError(f"ClickHouse backend requires 'engine' to be specified in {document_class.__name__}.Meta")

        engine_params = kwargs.get('engine_params', meta.get('engine_params', []))
        order_by = kwargs.get('order_by', meta.get('order_by'))
        partition_by = kwargs.get('partition_by', meta.get('partition_by'))
        primary_key = kwargs.get('primary_key', meta.get('primary_key'))
        settings = kwargs.get('settings', meta.get('settings', {}))
        ttl = kwargs.get('ttl', meta.get('ttl'))

        if not order_by:
            order_by = self._determine_smart_order_by(document_class)

        from ..fields.id import RecordIDField
        fields_dict = dict(document_class._fields)
        if 'id' not in fields_dict:
            fields_dict['id'] = RecordIDField()

        columns, materialized_columns = [], []
        for field_name, field in fields_dict.items():
            field_type = self.get_field_type(field)
            if hasattr(field, 'materialized') and field.materialized:
                columns.append(f"`{field_name}` {field_type} MATERIALIZED ({field.materialized})")
                materialized_columns.append(field_name)
            elif (field.required or (field_name == 'id' and isinstance(field, RecordIDField))) and field_name not in materialized_columns:
                columns.append(f"`{field_name}` {field_type}")
            elif field_name not in materialized_columns:
                columns.append(f"`{field_name}` Nullable({field_type})")

        query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n" + ",\n".join(f"    {col}" for col in columns)
        if engine_params:
            params_str = ", ".join(f"`{p}`" if isinstance(p, str) else str(p) for p in engine_params)
            query += f"\n) ENGINE = {engine}({params_str})"
        else:
            query += f"\n) ENGINE = {engine}()"
        if partition_by:
            query += f"\nPARTITION BY {partition_by}"
        if primary_key:
            pk_str = ", ".join(f"`{pk}`" for pk in primary_key) if isinstance(primary_key, list) else f"`{primary_key}`"
            query += f"\nPRIMARY KEY ({pk_str})"
        order_by_str = ", ".join(f"`{col}`" for col in order_by) if isinstance(order_by, list) else f"`{order_by}`"
        query += f"\nORDER BY ({order_by_str})"
        if ttl:
            query += f"\nTTL {ttl}"
        if settings:
            settings_str = ", ".join(f"{k}={v}" for k, v in settings.items())
            query += f"\nSETTINGS {settings_str}"

        await self._execute(conn, query)
        await self._create_indexes(conn, document_class, table_name)

    async def create_table(self, document_class: Type, **kwargs) -> None:
        """Create a table for the document class with advanced ClickHouse features."""
        await self.execute_with_pool(self._create_table_op, document_class, **kwargs)
    
    async def _create_indexes(self, conn: Any, document_class: Type, table_name: str) -> None:
        """Create indexes for the table based on field specifications.
        
        Args:
            conn: The database connection
            document_class: The document class
            table_name: The table name
        """
        for field_name, field in document_class._fields.items():
            if hasattr(field, 'indexes') and field.indexes:
                for index_spec in field.indexes:
                    await self._create_single_index(conn, table_name, field_name, index_spec)
    
    async def _create_single_index(self, conn: Any, table_name: str, field_name: str, index_spec: Dict[str, Any]) -> None:
        """Create a single index based on specification.
        
        Args:
            table_name: The table name
            field_name: The field name
            index_spec: Index specification dictionary
        """
        index_type = index_spec.get('type', 'bloom_filter')
        granularity = index_spec.get('granularity', 3)
        
        # Generate index name
        index_name = f"idx_{table_name}_{field_name}_{index_type}"
        
        if index_type == 'bloom_filter':
            false_positive_rate = index_spec.get('false_positive_rate', 0.01)
            query = (f"ALTER TABLE {table_name} "
                    f"ADD INDEX {index_name} {field_name} "
                    f"TYPE bloom_filter({false_positive_rate}) GRANULARITY {granularity}")
        
        elif index_type == 'set':
            max_values = index_spec.get('max_values', 100)
            query = (f"ALTER TABLE {table_name} "
                    f"ADD INDEX {index_name} {field_name} "
                    f"TYPE set({max_values}) GRANULARITY {granularity}")
        
        elif index_type == 'minmax':
            query = (f"ALTER TABLE {table_name} "
                    f"ADD INDEX {index_name} {field_name} "
                    f"TYPE minmax GRANULARITY {granularity}")
        
        else:
            # Custom index type - use as-is
            query = (f"ALTER TABLE {table_name} "
                    f"ADD INDEX {index_name} {field_name} "
                    f"TYPE {index_type} GRANULARITY {granularity}")
        
        try:
            await self._execute(conn, query)
        except Exception as e:
            # Log index creation failure but don't fail table creation
            print(f"Warning: Failed to create index {index_name}: {e}")
    
    def _determine_smart_order_by(self, document_class: Type) -> List[str]:
        """Intelligently determine ORDER BY clause for ClickHouse tables.
        
        ClickHouse requires an ORDER BY clause, but unlike traditional databases,
        it doesn't need an artificial 'id' field. This method analyzes the document
        fields to choose the most appropriate ORDER BY strategy.
        
        Priority order:
        1. Time-based fields (common in analytics workloads)
        2. Required categorical fields (user_id, product_id, etc.)
        3. Any required non-nullable fields
        4. Auto-generate a simple ordering field if needed
        
        Args:
            document_class: The document class to analyze
            
        Returns:
            List of field names for ORDER BY clause
        """
        fields = document_class._fields
        time_fields = []
        categorical_fields = []
        required_fields = []
        
        # Import field types for analysis
        from ..fields import DateTimeField, StringField
        from ..fields.clickhouse import LowCardinalityField
        
        for field_name, field in fields.items():
            # Skip materialized columns from ORDER BY consideration
            if hasattr(field, 'materialized') and field.materialized:
                continue
                
            # Analyze field patterns
            field_name_lower = field_name.lower()
            
            # Priority 1: Time-based fields
            if isinstance(field, DateTimeField):
                priority = 0
                # Give higher priority to common timestamp field names
                if any(keyword in field_name_lower for keyword in 
                       ['created', 'updated', 'collected', 'timestamp', 'time', 'date']):
                    priority = -1
                time_fields.append((priority, field_name, field))
            
            # Priority 2: Categorical identifier fields
            elif (isinstance(field, (StringField, LowCardinalityField)) and 
                  field.required and
                  any(keyword in field_name_lower for keyword in 
                      ['id', 'key', 'name', 'code', 'type', 'category', 'brand', 'seller'])):
                # Lower cardinality fields get higher priority
                priority = 0 if isinstance(field, LowCardinalityField) else 1
                categorical_fields.append((priority, field_name, field))
            
            # Priority 3: Any required fields that could work for ordering
            elif field.required:
                required_fields.append((field_name, field))
        
        # Build ORDER BY strategy
        order_by = []
        
        # Add best time field (most common pattern in ClickHouse)
        if time_fields:
            time_fields.sort(key=lambda x: x[0])  # Sort by priority
            best_time_field = time_fields[0][1]
            order_by.append(best_time_field)
            
            # Add best categorical field to improve sorting
            if categorical_fields and len(order_by) < 3:
                categorical_fields.sort(key=lambda x: x[0])
                best_categorical = categorical_fields[0][1]
                order_by.append(best_categorical)
        
        # If no time fields, start with categorical fields
        elif categorical_fields:
            categorical_fields.sort(key=lambda x: x[0])
            # Add up to 2 categorical fields for compound sorting
            for i, (_, field_name, _) in enumerate(categorical_fields[:2]):
                order_by.append(field_name)
        
        # If no good categorical fields, use any required field
        elif required_fields:
            order_by.append(required_fields[0][0])
        
        # Last resort: auto-generate a simple ordering field
        if not order_by:
            print("Warning: No suitable fields found for ORDER BY. "
                  "Consider adding a timestamp or identifier field, "
                  "or specify order_by explicitly in Meta class.")
            # Create a synthetic ordering using tuple() of available fields
            available_fields = [name for name, field in fields.items() 
                              if not (hasattr(field, 'materialized') and field.materialized)]
            if available_fields:
                order_by = available_fields[:3]  # Use first few fields
            else:
                # Absolute last resort - this shouldn't happen in practice
                order_by = ['tuple()']
        
        return order_by
    
    async def _insert_op(self, conn: Any, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Operation to insert a single document."""
        try:
            describe_result = await self._query(conn, f"DESCRIBE {table_name}")
            column_names = [row[0] for row in describe_result] if describe_result else []
            if 'id' in column_names and ('id' not in data or not data['id']):
                data['id'] = str(uuid.uuid4())
        except Exception:
            if 'id' not in data or not data['id']:
                data['id'] = str(uuid.uuid4())
        
        columns = list(data.keys())
        values = [data[col] for col in columns]
        await self._execute_insert(conn, table_name, [values], columns)
        return data

    async def insert(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single document."""
        return await self.execute_with_pool(self._insert_op, table_name, data)

    async def _insert_many_op(self, conn: Any, table_name: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Operation to insert multiple documents."""
        if not data:
            return []
        
        try:
            describe_result = await self._query(conn, f"DESCRIBE {table_name}")
            column_names = [row[0] for row in describe_result] if describe_result else []
            table_has_id = 'id' in column_names
        except Exception:
            table_has_id = True
        
        for doc in data:
            if table_has_id and ('id' not in doc or not doc['id']):
                doc['id'] = str(uuid.uuid4())
        
        columns = list(data[0].keys())
        values = [[doc.get(col) for col in columns] for doc in data]
        await self._execute_insert(conn, table_name, values, columns)
        return data

    async def insert_many(self, table_name: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Insert multiple documents efficiently."""
        return await self.execute_with_pool(self._insert_many_op, table_name, data)
    
    async def _select_op(self, conn: Any, table_name: str, conditions: List[str], fields: Optional[List[str]], limit: Optional[int], offset: Optional[int], order_by: Optional[List[tuple[str, str]]]) -> List[Dict[str, Any]]:
        """Operation to select documents from a table."""
        select_clause = ", ".join(f"`{f}`" for f in fields) if fields else "*"
        query = f"SELECT {select_clause} FROM {table_name}"
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        if order_by:
            query += f" ORDER BY {', '.join([f'`{f}` {d.upper()}' for f, d in order_by])}"
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        result = await self._query(conn, query)
        if not result:
            return []
        
        columns_result = await self._query(conn, f"DESCRIBE {table_name}")
        column_names = [row[0] for row in columns_result] if columns_result else (fields or [])
        
        return [dict(zip(column_names, row)) for row in result]

    async def select(self, table_name: str, conditions: List[str], fields: Optional[List[str]] = None, limit: Optional[int] = None, offset: Optional[int] = None, order_by: Optional[List[tuple[str, str]]] = None) -> List[Dict[str, Any]]:
        """Select documents from a table."""
        return await self.execute_with_pool(self._select_op, table_name, conditions, fields, limit, offset, order_by)

    async def _count_op(self, conn: Any, table_name: str, conditions: List[str]) -> int:
        """Operation to count documents."""
        query = f"SELECT count(*) FROM {table_name}"
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        result = await self._query(conn, query)
        return result[0][0] if result and result[0] else 0

    async def count(self, table_name: str, conditions: List[str]) -> int:
        """Count documents matching conditions."""
        return await self.execute_with_pool(self._count_op, table_name, conditions)
    
    async def _update_op(self, conn: Any, table_name: str, conditions: List[str], data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Operation to update documents."""
        # First, get the documents that will be updated
        docs_to_update = await self._select_op(conn, table_name, conditions, fields=None, limit=None, offset=None, order_by=None)
        
        if not docs_to_update:
            return []
        
        # Build UPDATE query
        set_clauses = []
        for key, value in data.items():
            set_clauses.append(f"`{key}` = {self.format_value(value)}")
        
        query = f"ALTER TABLE {table_name} UPDATE {', '.join(set_clauses)}"
        
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        
        await self._execute(conn, query)
        
        # Return the documents with updates applied
        # Note: In real ClickHouse, the update is asynchronous
        for doc in docs_to_update:
            doc.update(data)
        
        return docs_to_update

    async def update(self, table_name: str, conditions: List[str], data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Update documents matching conditions."""
        return await self.execute_with_pool(self._update_op, table_name, conditions, data)

    async def _delete_op(self, conn: Any, table_name: str, conditions: List[str]) -> int:
        """Operation to delete documents."""
        # Count documents before deletion
        count = await self._count_op(conn, table_name, conditions)
        
        if count == 0:
            return 0
        
        query = f"ALTER TABLE {table_name} DELETE"
        
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        
        await self._execute(conn, query)
        
        return count

    async def delete(self, table_name: str, conditions: List[str]) -> int:
        """Delete documents matching conditions."""
        return await self.execute_with_pool(self._delete_op, table_name, conditions)
    
    async def _drop_table_op(self, conn: Any, table_name: str, if_exists: bool = True) -> None:
        """Operation to drop a table."""
        if if_exists:
            query = f"DROP TABLE IF EXISTS {table_name}"
        else:
            query = f"DROP TABLE {table_name}"
        await self._execute(conn, query)

    async def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """Drop a table using ClickHouse's DROP TABLE statement."""
        await self.execute_with_pool(self._drop_table_op, table_name, if_exists)

    async def _execute_raw_op(self, conn: Any, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Operation to execute a raw query."""
        if params:
            for key, value in params.items():
                query = query.replace(f":{key}", self.format_value(value))
        return await self._query(conn, query)

    async def execute_raw(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a raw query."""
        return await self.execute_with_pool(self._execute_raw_op, query, params)
    
    def build_condition(self, field: str, operator: str, value: Any) -> str:
        """Build a condition string for ClickHouse SQL.
        
        Args:
            field: The field name
            operator: The operator
            value: The value to compare against
            
        Returns:
            A condition string in ClickHouse SQL
        """
        field = f"`{field}`"
        
        if operator == '=':
            return f"{field} = {self.format_value(value)}"
        elif operator == '!=':
            return f"{field} != {self.format_value(value)}"
        elif operator in ['>', '<', '>=', '<=']:
            return f"{field} {operator} {self.format_value(value)}"
        elif operator == 'in':
            if isinstance(value, list):
                formatted_values = [self.format_value(v) for v in value]
                return f"{field} IN ({', '.join(formatted_values)})"
            return f"{field} IN {self.format_value(value)}"
        elif operator == 'not in':
            if isinstance(value, list):
                formatted_values = [self.format_value(v) for v in value]
                return f"{field} NOT IN ({', '.join(formatted_values)})"
            return f"{field} NOT IN {self.format_value(value)}"
        elif operator == 'like':
            return f"{field} LIKE {self.format_value(value)}"
        elif operator == 'ilike':
            return f"{field} ILIKE {self.format_value(value)}"
        elif operator == 'contains':
            # For string contains (LIKE with wildcards) or array contains
            # Check if this is likely a string field by the value type
            if isinstance(value, str):
                # String contains - use LIKE with wildcards
                escaped_value = value.replace('%', '\\%').replace('_', '\\_')
                return f"{field} LIKE {self.format_value(f'%{escaped_value}%')}"
            else:
                # Array contains - use has()
                return f"has({field}, {self.format_value(value)})"
        elif operator == 'is null':
            return f"{field} IS NULL"
        elif operator == 'is not null':
            return f"{field} IS NOT NULL"
        else:
            return f"{field} {operator} {self.format_value(value)}"
    
    def get_field_type(self, field: Any) -> str:
        """Get the ClickHouse field type for a QuantumORM field.
        
        Args:
            field: A QuantumORM field instance
            
        Returns:
            The corresponding ClickHouse field type
        """
        # Import here to avoid circular imports
        from ..fields import (
            StringField, IntField, FloatField, BooleanField,
            DateTimeField, UUIDField, DictField, DecimalField
        )
        from ..fields.id import RecordIDField
        from ..fields.clickhouse import (
            LowCardinalityField, FixedStringField, EnumField,
            CompressedStringField, CompressedLowCardinalityField
        )
        
        # Check for ClickHouse-specific fields first
        if hasattr(field, 'get_clickhouse_type'):
            return field.get_clickhouse_type()
        
        # Handle standard fields
        if isinstance(field, RecordIDField):
            return "String"  # Store record IDs as strings in ClickHouse
        elif isinstance(field, StringField):
            if hasattr(field, 'max_length') and field.max_length:
                return f"FixedString({field.max_length})"
            return "String"
        elif isinstance(field, IntField):
            return "Int64"
        elif isinstance(field, FloatField):
            return "Float64"
        elif isinstance(field, BooleanField):
            return "UInt8"  # ClickHouse uses UInt8 for booleans
        elif isinstance(field, DateTimeField):
            return "DateTime64(3)"  # Millisecond precision
        elif isinstance(field, UUIDField):
            return "UUID"
        elif isinstance(field, DecimalField):
            return "Decimal(38, 18)"  # High precision decimal
        elif isinstance(field, DictField):
            return "String"  # Store JSON as string
        else:
            return "String"  # Default to string
    
    def format_value(self, value: Any, field_type: Optional[str] = None) -> str:
        """Format a value for ClickHouse SQL.
        
        Args:
            value: The value to format
            field_type: Optional field type hint
            
        Returns:
            The formatted value as a string
        """
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "\\'")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, datetime):
            # Format datetime for ClickHouse
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        elif isinstance(value, list):
            # Format array
            formatted_items = [self.format_value(item) for item in value]
            return f"[{', '.join(formatted_items)}]"
        elif isinstance(value, dict):
            # Store dict as JSON string
            return self.format_value(json.dumps(value))
        elif isinstance(value, uuid.UUID):
            return f"'{str(value)}'"
        else:
            # Default: convert to string
            return self.format_value(str(value))
    
    # Transaction support (limited in ClickHouse)
    
    async def begin_transaction(self) -> Any:
        """Begin a transaction.
        
        Note: ClickHouse has limited transaction support.
        """
        # ClickHouse doesn't support traditional transactions
        # Return None to indicate no transaction
        return None
    
    async def commit_transaction(self, transaction: Any) -> None:
        """Commit a transaction.
        
        Note: No-op for ClickHouse.
        """
        pass
    
    async def rollback_transaction(self, transaction: Any) -> None:
        """Rollback a transaction.
        
        Note: No-op for ClickHouse.
        """
        pass
    
    def supports_transactions(self) -> bool:
        """ClickHouse has limited transaction support."""
        return False
    
    def supports_references(self) -> bool:
        """ClickHouse doesn't support references between tables."""
        return False
    
    def supports_graph_relations(self) -> bool:
        """ClickHouse doesn't support graph relations."""
        return False
    
    def supports_direct_record_access(self) -> bool:
        """ClickHouse doesn't support direct record access syntax."""
        return False
    
    def supports_explain(self) -> bool:
        """ClickHouse supports EXPLAIN queries."""
        return True
    
    def supports_indexes(self) -> bool:
        """ClickHouse supports indexes."""
        return True
    
    def supports_full_text_search(self) -> bool:
        """ClickHouse has limited full-text search support."""
        return False
    
    def supports_bulk_operations(self) -> bool:
        """ClickHouse excels at bulk operations."""
        return True
    
    def supports_materialized_views(self) -> bool:
        """ClickHouse supports materialized views."""
        return True
    
    def get_optimized_methods(self) -> Dict[str, str]:
        """Get ClickHouse-specific optimization methods."""
        return {
            'bulk_insert': 'INSERT INTO table VALUES (...)',
            'analytical_functions': 'groupArray(), uniq(), quantile()',
            'array_functions': 'has(), arrayFilter(), arrayMap()',
            'columnar_storage': 'Optimized for analytical workloads',
        }
    
    # Materialized view support
    
    async def create_materialized_view(self, materialized_document_class: Type) -> None:
        """Create a ClickHouse materialized view.
        
        Args:
            materialized_document_class: The MaterializedDocument class
        """
        view_name = materialized_document_class._meta.get('view_name') or \
                   materialized_document_class._meta.get('table_name') or \
                   materialized_document_class.__name__.lower()
        
        meta = materialized_document_class._meta
        
        # Build the source query
        source_query = materialized_document_class._build_source_query()
        
        # Get ClickHouse-specific configuration
        engine = meta.get('engine', 'AggregatingMergeTree')
        engine_params = meta.get('engine_params', [])
        order_by = meta.get('order_by', [])
        partition_by = meta.get('partition_by')
        
        # If no ORDER BY specified, use dimension fields
        if not order_by:
            order_by = list(materialized_document_class._dimension_fields.keys())
        
        # Build CREATE MATERIALIZED VIEW query
        if engine_params:
            params_str = ", ".join(f"`{p}`" if isinstance(p, str) else str(p) for p in engine_params)
            engine_clause = f"ENGINE = {engine}({params_str})"
        else:
            engine_clause = f"ENGINE = {engine}()"
        
        # Build ORDER BY clause
        if isinstance(order_by, list):
            order_by_str = ", ".join(f"`{col}`" for col in order_by)
        else:
            order_by_str = f"`{order_by}`" if not order_by.startswith('`') else order_by
        order_by_clause = f"ORDER BY ({order_by_str})"
        
        # Add partition clause if specified
        partition_clause = f"PARTITION BY {partition_by}" if partition_by else ""
        
        # Build the complete query
        query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        {engine_clause}
        {partition_clause}
        {order_by_clause}
        AS {source_query}
        """.strip()
        
        # Debug: Print the generated query
        print("Generated ClickHouse Materialized View SQL:")
        print(query)
        print("=" * 60)
        
        await self._execute(query)
    
    async def drop_materialized_view(self, materialized_document_class: Type) -> None:
        """Drop a ClickHouse materialized view.
        
        Args:
            materialized_document_class: The MaterializedDocument class
        """
        view_name = materialized_document_class._meta.get('view_name') or \
                   materialized_document_class._meta.get('table_name') or \
                   materialized_document_class.__name__.lower()
        
        query = f"DROP VIEW IF EXISTS {view_name}"
        await self._execute(query)
    
    async def refresh_materialized_view(self, materialized_document_class: Type) -> None:
        """Refresh a ClickHouse materialized view.
        
        Note: ClickHouse materialized views update automatically as data arrives.
        This is a no-op for ClickHouse.
        
        Args:
            materialized_document_class: The MaterializedDocument class
        """
        # ClickHouse materialized views refresh automatically
        pass
    
    # Helper methods for async execution
    
    async def _execute(self, conn: Any, query: str) -> None:
        """Execute a query without returning results."""
        # Run sync operation in thread pool
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, conn.command, query)
    
    async def _query(self, conn: Any, query: str) -> List[Any]:
        """Execute a query and return results."""
        # Run sync operation in thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, conn.query, query)
        return result.result_rows if result else []
    
    async def _execute_insert(self, conn: Any, table_name: str, data: List[List[Any]], column_names: List[str]) -> None:
        """Execute an INSERT with multiple rows."""
        # Run sync operation in thread pool
        loop = asyncio.get_event_loop()
        
        # Use partial to bind keyword arguments
        insert_func = partial(
            conn.insert,
            table_name,
            data,
            column_names=column_names
        )
        await loop.run_in_executor(None, insert_func)