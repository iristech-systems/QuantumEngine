"""ClickHouse backend implementation for SurrealEngine."""

import asyncio
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union

import clickhouse_connect

from .base import BaseBackend


class ClickHouseBackend(BaseBackend):
    """ClickHouse backend implementation using clickhouse-connect."""
    
    def __init__(self, connection: Any) -> None:
        """Initialize the ClickHouse backend.
        
        Args:
            connection: ClickHouse client connection
        """
        super().__init__(connection)
        # If connection is async, we'll need to handle it differently
        self.client = connection
    
    async def create_table(self, document_class: Type, **kwargs) -> None:
        """Create a table for the document class.
        
        Args:
            document_class: The document class to create a table for
            **kwargs: Backend-specific options:
                - engine: ClickHouse table engine (default: MergeTree)
                - order_by: Order by columns (default: id)
                - partition_by: Partition by expression
                - primary_key: Primary key columns
                - settings: Additional table settings
        """
        table_name = document_class._meta.get('table_name')
        
        # Build column definitions
        columns = []
        for field_name, field in document_class._fields.items():
            field_type = self.get_field_type(field)
            if field.required:
                columns.append(f"`{field_name}` {field_type}")
            else:
                columns.append(f"`{field_name}` Nullable({field_type})")
        
        # Table engine settings
        engine = kwargs.get('engine', 'MergeTree')
        order_by = kwargs.get('order_by', 'id')
        partition_by = kwargs.get('partition_by')
        primary_key = kwargs.get('primary_key')
        settings = kwargs.get('settings', {})
        
        # Build CREATE TABLE query
        query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        query += ",\n".join(f"    {col}" for col in columns)
        query += f"\n) ENGINE = {engine}()"
        
        if partition_by:
            query += f" PARTITION BY {partition_by}"
        
        if primary_key:
            query += f" PRIMARY KEY {primary_key}"
        
        query += f" ORDER BY {order_by}"
        
        # Add settings
        if settings:
            settings_str = ", ".join(f"{k}={v}" for k, v in settings.items())
            query += f" SETTINGS {settings_str}"
        
        # Execute in async context
        await self._execute(query)
    
    async def insert(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single document.
        
        Args:
            table_name: The table name
            data: The document data to insert
            
        Returns:
            The inserted document with generated id if not provided
        """
        # Generate ID if not provided
        if 'id' not in data or not data['id']:
            data['id'] = str(uuid.uuid4())
        
        columns = list(data.keys())
        values = [data[col] for col in columns]
        
        await self._execute_insert(table_name, [values], columns)
        
        return data
    
    async def insert_many(self, table_name: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Insert multiple documents efficiently.
        
        Args:
            table_name: The table name
            data: List of documents to insert
            
        Returns:
            List of inserted documents
        """
        if not data:
            return []
        
        # Ensure all documents have IDs
        for doc in data:
            if 'id' not in doc or not doc['id']:
                doc['id'] = str(uuid.uuid4())
        
        # Get columns from first document
        columns = list(data[0].keys())
        
        # Prepare values
        values = []
        for doc in data:
            row = [doc.get(col) for col in columns]
            values.append(row)
        
        await self._execute_insert(table_name, values, columns)
        
        return data
    
    async def select(self, table_name: str, conditions: List[str], 
                    fields: Optional[List[str]] = None,
                    limit: Optional[int] = None, 
                    offset: Optional[int] = None,
                    order_by: Optional[List[tuple[str, str]]] = None) -> List[Dict[str, Any]]:
        """Select documents from a table.
        
        Args:
            table_name: The table name
            conditions: List of condition strings
            fields: List of fields to return (None for all)
            limit: Maximum number of results
            offset: Number of results to skip
            order_by: List of (field, direction) tuples
            
        Returns:
            List of matching documents
        """
        # Build SELECT clause
        if fields:
            select_clause = ", ".join(f"`{field}`" for field in fields)
        else:
            select_clause = "*"
        
        query = f"SELECT {select_clause} FROM {table_name}"
        
        # Add WHERE clause
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        
        # Add ORDER BY clause
        if order_by:
            order_parts = []
            for field, direction in order_by:
                order_parts.append(f"`{field}` {direction.upper()}")
            query += f" ORDER BY {', '.join(order_parts)}"
        
        # Add LIMIT and OFFSET
        if limit:
            query += f" LIMIT {limit}"
        
        if offset:
            query += f" OFFSET {offset}"
        
        result = await self._query(query)
        
        if not result:
            return []
        
        # Get column names for converting to dicts
        columns_query = f"DESCRIBE {table_name}"
        columns_result = await self._query(columns_query)
        column_names = [row[0] for row in columns_result] if columns_result else None
        
        # Convert to list of dicts
        if column_names:
            return [dict(zip(column_names, row)) for row in result]
        else:
            # Fallback: use generic column names
            if result and len(result) > 0:
                column_count = len(result[0])
                column_names = [f"col_{i}" for i in range(column_count)]
                return [dict(zip(column_names, row)) for row in result]
            return []
    
    async def count(self, table_name: str, conditions: List[str]) -> int:
        """Count documents matching conditions.
        
        Args:
            table_name: The table name
            conditions: List of condition strings
            
        Returns:
            Number of matching documents
        """
        query = f"SELECT count(*) FROM {table_name}"
        
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        
        result = await self._query(query)
        
        if result and result[0]:
            return result[0][0]
        return 0
    
    async def update(self, table_name: str, conditions: List[str], 
                    data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Update documents matching conditions.
        
        Note: ClickHouse uses ALTER TABLE UPDATE which is asynchronous
        and doesn't immediately return updated rows.
        
        Args:
            table_name: The table name
            conditions: List of condition strings
            data: The fields to update
            
        Returns:
            List of documents that will be updated
        """
        # First, get the documents that will be updated
        docs_to_update = await self.select(table_name, conditions)
        
        if not docs_to_update:
            return []
        
        # Build UPDATE query
        set_clauses = []
        for key, value in data.items():
            set_clauses.append(f"`{key}` = {self.format_value(value)}")
        
        query = f"ALTER TABLE {table_name} UPDATE {', '.join(set_clauses)}"
        
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        
        await self._execute(query)
        
        # Return the documents with updates applied
        # Note: In real ClickHouse, the update is asynchronous
        for doc in docs_to_update:
            doc.update(data)
        
        return docs_to_update
    
    async def delete(self, table_name: str, conditions: List[str]) -> int:
        """Delete documents matching conditions.
        
        Note: ClickHouse uses ALTER TABLE DELETE which is asynchronous.
        
        Args:
            table_name: The table name
            conditions: List of condition strings
            
        Returns:
            Number of documents that will be deleted
        """
        # Count documents before deletion
        count = await self.count(table_name, conditions)
        
        if count == 0:
            return 0
        
        query = f"ALTER TABLE {table_name} DELETE"
        
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        
        await self._execute(query)
        
        return count
    
    async def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """Drop a table using ClickHouse's DROP TABLE statement.
        
        Args:
            table_name: The table name to drop
            if_exists: Whether to use IF EXISTS clause to avoid errors if table doesn't exist
        """
        if if_exists:
            query = f"DROP TABLE IF EXISTS {table_name}"
        else:
            query = f"DROP TABLE {table_name}"
        
        await self._execute(query)
    
    async def execute_raw(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a raw query.
        
        Args:
            query: The raw query string
            params: Optional query parameters
            
        Returns:
            Query result
        """
        if params:
            # Simple parameter substitution for ClickHouse
            for key, value in params.items():
                query = query.replace(f":{key}", self.format_value(value))
        
        return await self._query(query)
    
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
        """Get the ClickHouse field type for a SurrealEngine field.
        
        Args:
            field: A SurrealEngine field instance
            
        Returns:
            The corresponding ClickHouse field type
        """
        # Import here to avoid circular imports
        from ..fields import (
            StringField, IntField, FloatField, BooleanField,
            DateTimeField, UUIDField, DictField
        )
        
        if isinstance(field, StringField):
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
    
    def get_optimized_methods(self) -> Dict[str, str]:
        """Get ClickHouse-specific optimization methods."""
        return {
            'bulk_insert': 'INSERT INTO table VALUES (...)',
            'analytical_functions': 'groupArray(), uniq(), quantile()',
            'array_functions': 'has(), arrayFilter(), arrayMap()',
            'columnar_storage': 'Optimized for analytical workloads',
        }
    
    # Helper methods for async execution
    
    async def _execute(self, query: str) -> None:
        """Execute a query without returning results."""
        # Run sync operation in thread pool
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.client.command, query)
    
    async def _query(self, query: str) -> List[Any]:
        """Execute a query and return results."""
        # Run sync operation in thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self.client.query, query)
        return result.result_rows if result else []
    
    async def _execute_insert(self, table_name: str, data: List[List[Any]], column_names: List[str]) -> None:
        """Execute an INSERT with multiple rows."""
        # Run sync operation in thread pool
        loop = asyncio.get_event_loop()
        
        # Use partial to bind keyword arguments
        from functools import partial
        insert_func = partial(
            self.client.insert,
            table_name,
            data,
            column_names=column_names
        )
        await loop.run_in_executor(None, insert_func)