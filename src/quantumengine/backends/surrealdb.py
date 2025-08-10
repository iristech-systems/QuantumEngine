"""SurrealDB backend implementation for SurrealEngine."""

import uuid
from typing import Any, Dict, List, Optional, Type

from surrealdb import RecordID

from .base import BaseBackend
from ..connection import ConnectionPoolBase, PoolConfig
from .pools.surrealdb import SurrealDBConnectionPool


class SurrealDBBackend(BaseBackend):
    """SurrealDB backend implementation.
    
    This backend implements the BaseBackend interface for SurrealDB,
    providing all the core database operations using SurrealQL.
    """
    
    def _initialize_client(self, connection: Any) -> Any:
        """Initialize the SurrealDB client from the connection.
        
        Args:
            connection: The connection object from ConnectionRegistry
            
        Returns:
            The SurrealDB client object
        """
        # For SurrealDB, the connection has a .client attribute
        return connection.client
    
    async def _get_client(self) -> Any:
        """Get the client, ensuring connection is established first."""
        # Ensure connection is established before accessing client
        if hasattr(self.connection, '_ensure_connected'):
            await self.connection._ensure_connected()
        elif hasattr(self.connection, 'connect') and not self.connection.client:
            if hasattr(self.connection.connect, '__await__'):  # async connect
                await self.connection.connect()
            else:  # sync connect
                self.connection.connect()
        
        return self.connection.client
    
    def _get_client_sync(self) -> Any:
        """Get the client synchronously, ensuring connection is established first."""
        # Ensure connection is established before accessing client
        if hasattr(self.connection, 'connect') and not self.connection.client:
            # For sync connections, call connect directly
            self.connection.connect()
        
        return self.connection.client
    
    def __init__(self, connection_config, pool_config: Optional[PoolConfig] = None) -> None:
        """Initialize the SurrealDB backend.
        
        Args:
            connection_config: Either a dict of configuration or legacy connection object
            pool_config: Configuration for connection pooling
        """
        # Handle backward compatibility - old signature: __init__(connection)
        if hasattr(connection_config, 'client') and not isinstance(connection_config, dict):
            # Legacy mode: connection_config is actually a connection object
            connection = connection_config
            self.connection = connection  # Set the connection attribute for legacy methods
            connection_config = {'connection': connection}
            self.is_async = hasattr(connection, 'client') and hasattr(connection.client, 'create')
            # Don't call super().__init__ for legacy mode - set attributes directly  
            self.connection_config = connection_config
            self.pool_config = pool_config or PoolConfig()
            self._pool = None
        else:
            # New mode: connection_config is a dict
            super().__init__(connection_config, pool_config)
            # Legacy connection support - will be deprecated
            if 'connection' in connection_config:
                connection = connection_config['connection']
                self.connection = connection  # Set for legacy method compatibility
                self.is_async = hasattr(connection, 'client') and hasattr(connection.client, 'create')
            else:
                self.is_async = True  # Default to async mode for new connections
                self.connection = None  # Will be set up through pool
    
    async def create_table(self, document_class: Type, **kwargs) -> None:
        """Create a table/collection for the document class.
        
        Args:
            document_class: The document class to create a table for
            **kwargs: Backend-specific options:
                - schemafull: Whether to create a schemafull table (default: True)
        """
        # Check if we should use sync methods
        if hasattr(self, 'is_async') and not self.is_async:
            return self.create_table_sync(document_class, **kwargs)
        
        table_name = document_class._meta.get('collection')
        schemafull = kwargs.get('schemafull', True)
        
        # Create table definition
        schema_type = "SCHEMAFULL" if schemafull else "SCHEMALESS"
        query = f"DEFINE TABLE {table_name} {schema_type}"
        
        await self._execute(query)
        
        # Define fields if schemafull
        if schemafull:
            for field_name, field in document_class._fields.items():
                if field_name == document_class._meta.get('id_field', 'id'):
                    continue  # Skip ID field
                
                field_type = self.get_field_type(field)
                field_query = f"DEFINE FIELD {field.db_field} ON {table_name} TYPE {field_type}"
                
                if field.required:
                    field_query += " ASSERT $value != NONE"
                
                await self._execute(field_query)
    
    def create_table_sync(self, document_class: Type, **kwargs) -> None:
        """Create a table/collection for the document class synchronously.
        
        Args:
            document_class: The document class to create a table for
            **kwargs: Backend-specific options:
                - schemafull: Whether to create a schemafull table (default: True)
        """
        table_name = document_class._meta.get('collection')
        schemafull = kwargs.get('schemafull', True)
        
        # Create table definition
        schema_type = "SCHEMAFULL" if schemafull else "SCHEMALESS"
        query = f"DEFINE TABLE {table_name} {schema_type}"
        
        self._execute_sync(query)
        
        # Define fields if schemafull
        if schemafull:
            for field_name, field in document_class._fields.items():
                if field_name == document_class._meta.get('id_field', 'id'):
                    continue  # Skip ID field
                
                field_type = self.get_field_type(field)
                field_query = f"DEFINE FIELD {field.db_field} ON {table_name} TYPE {field_type}"
                
                if field.required:
                    field_query += " ASSERT $value != NONE"
                
                self._execute_sync(field_query)
        
        # Create indexes
        indexes = document_class._meta.get('indexes', [])
        for index in indexes:
            if isinstance(index, str):
                # Simple field index
                index_query = f"DEFINE INDEX idx_{index} ON {table_name} COLUMNS {index}"
            elif isinstance(index, dict):
                # Complex index
                index_name = index.get('name', f"idx_{'_'.join(index['fields'])}")
                fields = ', '.join(index['fields'])
                index_query = f"DEFINE INDEX {index_name} ON {table_name} COLUMNS {fields}"
                
                if index.get('unique'):
                    index_query += " UNIQUE"
            else:
                continue
            
            self._execute_sync(index_query)
    
    async def insert(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single document.
        
        Args:
            table_name: The table name
            data: The document data to insert
            
        Returns:
            The inserted document with any generated fields
        """
        # Format data for SurrealDB
        formatted_data = self._format_document_data(data)
        
        if 'id' in formatted_data and formatted_data['id']:
            # Use CREATE with specific ID for new records, UPDATE for existing ones
            record_id = formatted_data.pop('id')
            if not isinstance(record_id, RecordID):
                if ':' in str(record_id):
                    # Split table and id parts
                    parts = str(record_id).split(':', 1)
                    # Check if the id part is numeric
                    try:
                        # If it's numeric, convert to int for proper RecordID format
                        record_id = RecordID(parts[0], int(parts[1]))
                    except ValueError:
                        # If not numeric, keep as string
                        record_id = RecordID(parts[0], parts[1])
                else:
                    # Check if the id is numeric
                    try:
                        record_id = RecordID(table_name, int(record_id))
                    except ValueError:
                        record_id = RecordID(table_name, record_id)
            
            # Try CREATE first (for new records), fallback to UPDATE if it exists
            try:
                client = await self._get_client()
                result = await client.create(record_id, formatted_data)
            except Exception as e:
                if 'already exists' in str(e):
                    # Record exists, use UPDATE instead
                    client = await self._get_client()
                    result = await client.update(record_id, formatted_data)
                else:
                    raise e
        else:
            # Use CREATE without ID (auto-generate)
            client = await self._get_client()
            result = await client.create(table_name, formatted_data)


        if result:
            # Result can be either a dict (single record) or a list (multiple records)
            if isinstance(result, list):
                return self._format_result_data(result[0]) if result else data
            else:
                return self._format_result_data(result)
        else:
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
        
        results = []
        
        # Group by documents with and without IDs
        docs_with_id = []
        docs_without_id = []
        
        for doc in data:
            formatted_doc = self._format_document_data(doc)
            if 'id' in formatted_doc and formatted_doc['id']:
                docs_with_id.append(formatted_doc)
            else:
                docs_without_id.append(formatted_doc)
        
        # Insert documents without IDs (bulk create)
        if docs_without_id:
            client = await self._get_client()
            batch_results = await client.insert(table_name, docs_without_id)
            if batch_results:
                results.extend([self._format_result_data(r) for r in batch_results])
        
        # Insert documents with IDs (individual creates)
        for doc in docs_with_id:
            record_id = doc.pop('id')
            if not isinstance(record_id, RecordID):
                if ':' in str(record_id):
                    record_id = RecordID(record_id)
                else:
                    record_id = RecordID(table_name, record_id)
            
            client = await self._get_client()
            result = await client.create(record_id, doc)
            if result and len(result) > 0:
                results.append(self._format_result_data(result[0]))
        
        return results
    
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
            offset: Number of results to skip (START in SurrealDB)
            order_by: List of (field, direction) tuples
            
        Returns:
            List of matching documents
        """
        # Build SELECT clause
        if fields:
            select_clause = ", ".join(fields)
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
                order_parts.append(f"{field} {direction.upper()}")
            query += f" ORDER BY {', '.join(order_parts)}"
        
        # Add LIMIT clause
        if limit:
            query += f" LIMIT {limit}"
        
        # Add START clause (SurrealDB's equivalent to OFFSET)
        if offset:
            query += f" START {offset}"
        
        result = await self._query(query)
        
        if result:
            # The SurrealDB Python client returns SELECT results as a plain list of dicts
            if isinstance(result, list):
                # If it's a list of dicts (documents), format each one
                if result and isinstance(result[0], dict):
                    return [self._format_result_data(doc) for doc in result]
                # Empty list case
                return []
        return []
    
    async def select_by_ids(self, table_name: str, ids: List[Any]) -> List[Dict[str, Any]]:
        """Select documents by their IDs using direct record access.
        
        Args:
            table_name: The table name
            ids: List of IDs to select
            
        Returns:
            List of matching documents
        """
        if not ids:
            return []
        
        # Format IDs for direct access
        record_ids = []
        for id_val in ids:
            if isinstance(id_val, RecordID):
                record_ids.append(str(id_val))
            elif isinstance(id_val, str) and ':' in id_val:
                record_ids.append(id_val)
            else:
                # Convert to proper RecordID format
                record_ids.append(f"{table_name}:{id_val}")
        
        # Use direct record access syntax
        query = f"SELECT * FROM {', '.join(record_ids)}"
        result = await self._query(query)
        
        if result:
            # The SurrealDB Python client returns SELECT results as a plain list of dicts
            if isinstance(result, list):
                # If it's a list of dicts (documents), format each one
                if result and isinstance(result[0], dict):
                    return [self._format_result_data(doc) for doc in result]
                # Empty list case
                return []
        return []
    
    async def count(self, table_name: str, conditions: List[str]) -> int:
        """Count documents matching conditions.
        
        Args:
            table_name: The table name
            conditions: List of condition strings
            
        Returns:
            Number of matching documents
        """
        query = f"SELECT count() FROM {table_name}"
        
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        
        result = await self._query(query)
        
        if result and isinstance(result, list):
            # SurrealDB returns a list of dicts with count for each record
            # Sum all the counts
            total_count = sum(item.get('count', 0) for item in result if isinstance(item, dict))
            return total_count
        return 0
    
    async def update(self, table_name: str, conditions: List[str], 
                    data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Update documents matching conditions.
        
        Args:
            table_name: The table name
            conditions: List of condition strings
            data: The fields to update
            
        Returns:
            List of updated documents
        """
        # Format update data
        formatted_data = self._format_document_data(data)
        
        # Build SET clause
        set_parts = []
        for key, value in formatted_data.items():
            set_parts.append(f"{key} = {self.format_value(value)}")
        
        if not set_parts:
            return []
        
        # Check if we have a simple id condition that we can use for direct record update
        record_id = self._extract_record_id_from_conditions(conditions)
        
        if record_id:
            # Use direct record identifier syntax: UPDATE table:record_id SET ...
            query = f"UPDATE {table_name}:{record_id} SET {', '.join(set_parts)}"
        else:
            # Fall back to WHERE clause syntax
            query = f"UPDATE {table_name} SET {', '.join(set_parts)}"
            if conditions:
                query += f" WHERE {' AND '.join(conditions)}"
        
        result = await self._query(query)
        
        if result and len(result) > 0:
            return [self._format_result_data(doc) for doc in result[0]]
        return []
    
    def _extract_record_id_from_conditions(self, conditions: List[str]) -> Optional[str]:
        """Extract record ID from conditions if there's a simple id = value condition.
        
        Args:
            conditions: List of condition strings
            
        Returns:
            Record ID if found, None otherwise
        """
        if not conditions or len(conditions) != 1:
            return None
        
        condition = conditions[0].strip()
        
        # Look for patterns like "id = 'value'" or "id = value"
        if condition.startswith('id = '):
            value_part = condition[5:].strip()
            # Remove quotes if present
            if value_part.startswith("'") and value_part.endswith("'"):
                record_id = value_part[1:-1]
            elif value_part.startswith('"') and value_part.endswith('"'):
                record_id = value_part[1:-1]
            else:
                record_id = value_part
            
            # If record_id already contains table:id format, extract just the ID part
            if ':' in record_id:
                parts = record_id.split(':', 1)
                return parts[1]  # Return just the ID part
            else:
                return record_id
        
        return None
    
    async def delete(self, table_name: str, conditions: List[str]) -> int:
        """Delete documents matching conditions.
        
        Args:
            table_name: The table name
            conditions: List of condition strings
            
        Returns:
            Number of deleted documents
        """
        query = f"DELETE FROM {table_name}"
        
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        
        result = await self._query(query)
        
        if result and len(result) > 0:
            return len(result[0])
        return 0
    
    async def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """Drop a table using SurrealDB's REMOVE TABLE statement.
        
        Args:
            table_name: The table name to drop
            if_exists: Whether to use IF EXISTS clause to avoid errors if table doesn't exist
        """
        if if_exists:
            query = f"REMOVE TABLE IF EXISTS {table_name}"
        else:
            query = f"REMOVE TABLE {table_name}"
        
        await self._execute(query)
    
    async def execute_raw(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a raw SurrealQL query.
        
        Args:
            query: The raw SurrealQL query string
            params: Optional query parameters (not used - SurrealDB handles this)
            
        Returns:
            Query result
        """
        return await self._query(query)
    
    def build_condition(self, field: str, operator: str, value: Any) -> str:
        """Build a condition string for SurrealQL.
        
        Args:
            field: The field name
            operator: The operator (=, !=, >, <, >=, <=, ~, !~, etc.)
            value: The value to compare against
            
        Returns:
            A condition string in SurrealQL
        """
        # Special handling for 'id' field - convert string to RecordID if needed
        if field == 'id' and isinstance(value, str) and ':' in value:
            # Convert string ID like "users:abc123" to RecordID
            parts = value.split(':', 1)
            value = RecordID(parts[0], parts[1])
        
        formatted_value = self.format_value(value)
        
        if operator == '=':
            return f"{field} = {formatted_value}"
        elif operator == '!=':
            return f"{field} != {formatted_value}"
        elif operator in ['>', '<', '>=', '<=']:
            return f"{field} {operator} {formatted_value}"
        elif operator == 'in':
            return f"{field} INSIDE {formatted_value}"
        elif operator == 'not in':
            return f"{field} NOT INSIDE {formatted_value}"
        elif operator == 'contains':
            return f"{field} CONTAINS {formatted_value}"
        elif operator == 'containsnot':
            return f"{field} CONTAINSNOT {formatted_value}"
        elif operator == 'containsall':
            return f"{field} CONTAINSALL {formatted_value}"
        elif operator == 'containsany':
            return f"{field} CONTAINSANY {formatted_value}"
        elif operator == 'containsnone':
            return f"{field} CONTAINSNONE {formatted_value}"
        elif operator == 'CONTAINS':
            # String contains operation
            if isinstance(value, str):
                return f"string::contains({field}, {formatted_value})"
            else:
                return f"{field} CONTAINS {formatted_value}"
        elif operator == 'STARTSWITH':
            return f"string::starts_with({field}, {formatted_value})"
        elif operator == 'ENDSWITH':
            return f"string::ends_with({field}, {formatted_value})"
        elif operator == 'REGEX':
            return f"{field} ~ {formatted_value}"
        elif operator == '~':
            return f"{field} ~ {formatted_value}"
        elif operator == '!~':
            return f"{field} !~ {formatted_value}"
        elif operator == 'is null':
            return f"{field} IS NULL"
        elif operator == 'is not null':
            return f"{field} IS NOT NULL"
        else:
            return f"{field} {operator} {formatted_value}"
    
    def get_field_type(self, field: Any) -> str:
        """Get the SurrealDB field type for a SurrealEngine field.
        
        Args:
            field: A SurrealEngine field instance
            
        Returns:
            The corresponding SurrealDB field type
        """
        # Import here to avoid circular imports
        from ..fields import (
            StringField, IntField, FloatField, BooleanField,
            DateTimeField, UUIDField, 
            DictField, DecimalField
        )
        
        if isinstance(field, StringField):
            return "string"
        elif isinstance(field, IntField):
            return "int"
        elif isinstance(field, FloatField):
            return "float"
        elif isinstance(field, BooleanField):
            return "bool"
        elif isinstance(field, DateTimeField):
            return "datetime"
        elif isinstance(field, UUIDField):
            return "uuid"
        elif isinstance(field, DictField):
            return "object"
        elif isinstance(field, DecimalField):
            return "decimal"
        else:
            return "any"
    
    def format_value(self, value: Any, field_type: Optional[str] = None) -> str:
        """Format a value for SurrealQL.
        
        Args:
            value: The value to format
            field_type: Optional field type hint
            
        Returns:
            The formatted value as a string for SurrealQL
        """
        if value is None:
            return "NONE"
        elif isinstance(value, str):
            # Escape quotes and wrap in quotes
            escaped = value.replace('"', '\\"')
            return f'"{escaped}"'
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, RecordID):
            return str(value)
        elif isinstance(value, list):
            # Format array
            formatted_items = [self.format_value(item) for item in value]
            return f"[{', '.join(formatted_items)}]"
        elif isinstance(value, dict):
            # Format object
            formatted_pairs = []
            for k, v in value.items():
                formatted_pairs.append(f"{k}: {self.format_value(v)}")
            return f"{{{', '.join(formatted_pairs)}}}"
        elif isinstance(value, uuid.UUID):
            return f'"{str(value)}"'
        else:
            # Default: convert to string
            return f'"{str(value)}"'
    
    # Transaction support
    
    async def begin_transaction(self) -> Any:
        """Begin a transaction.
        
        Returns:
            Transaction object (SurrealDB client for now)
        """
        # SurrealDB doesn't have explicit transaction syntax like BEGIN
        # Transactions are implicit within query batches
        return await self._get_client()
    
    async def commit_transaction(self, transaction: Any) -> None:
        """Commit a transaction.
        
        Args:
            transaction: The transaction object
        """
        # SurrealDB transactions are auto-committed
        # This is a no-op for compatibility
        pass
    
    async def rollback_transaction(self, transaction: Any) -> None:
        """Rollback a transaction.
        
        Args:
            transaction: The transaction object
        """
        # SurrealDB doesn't support explicit rollback in the same way
        # This would require application-level rollback logic
        pass
    
    def supports_transactions(self) -> bool:
        """SurrealDB supports transactions within query batches."""
        return True
    
    def supports_references(self) -> bool:
        """SurrealDB supports references between records."""
        return True
    
    def supports_graph_relations(self) -> bool:
        """SurrealDB has native graph relation support."""
        return True
    
    def supports_direct_record_access(self) -> bool:
        """SurrealDB supports direct record access syntax."""
        return True
    
    def supports_explain(self) -> bool:
        """SurrealDB supports EXPLAIN queries."""
        return True
    
    def supports_indexes(self) -> bool:
        """SurrealDB supports indexes."""
        return True
    
    def supports_full_text_search(self) -> bool:
        """SurrealDB supports full-text search."""
        return True
    
    def supports_bulk_operations(self) -> bool:
        """SurrealDB supports bulk operations."""
        return True
    
    def supports_materialized_views(self) -> bool:
        """SurrealDB supports materialized views."""
        return True
    
    def get_optimized_methods(self) -> Dict[str, str]:
        """Get SurrealDB-specific optimization methods."""
        return {
            'direct_record_access': 'SELECT * FROM user:1, user:2, user:3',
            'range_access': 'SELECT * FROM user:1..=100',
            'graph_traversal': 'SELECT * FROM user:1->likes->post',
            'string_functions': 'string::contains(), string::starts_with()',
        }
    
    # Graph/Relation implementations
    
    async def create_relation(self, from_table: str, from_id: str, 
                             relation_name: str, to_table: str, to_id: str,
                             attributes: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Create a relation using SurrealDB's RELATE statement.
        
        Args:
            from_table: Source table name
            from_id: Source document ID
            relation_name: Name of the relation
            to_table: Target table name  
            to_id: Target document ID
            attributes: Optional attributes for the relation
            
        Returns:
            The created relation record
        """
        from_record = RecordID(from_table, from_id)
        to_record = RecordID(to_table, to_id)
        
        # Construct RELATE query
        query = f"RELATE {from_record}->{relation_name}->{to_record}"
        
        # Add attributes if provided
        if attributes:
            import json
            attrs_str = ", ".join([f"{k}: {json.dumps(v)}" for k, v in attributes.items()])
            query += f" CONTENT {{ {attrs_str} }}"
        
        result = await self._query(query)
        
        if result and isinstance(result, list) and len(result) > 0:
            # SurrealDB returns the relation as a single dict in a list
            return self._format_result_data(result[0])
        return None
    
    async def delete_relation(self, from_table: str, from_id: str,
                             relation_name: str, to_table: Optional[str] = None, 
                             to_id: Optional[str] = None) -> int:
        """Delete relations using SurrealDB's DELETE statement.
        
        Args:
            from_table: Source table name
            from_id: Source document ID
            relation_name: Name of the relation
            to_table: Target table name (optional)
            to_id: Target document ID (optional)
            
        Returns:
            Number of relations deleted
        """
        from_record = RecordID(from_table, from_id)
        
        if to_table and to_id:
            # Delete specific relation
            to_record = RecordID(to_table, to_id)
            query = f"DELETE {from_record}->{relation_name}->{to_record}"
        else:
            # Delete all relations of this type from the source document
            query = f"DELETE {from_record}->{relation_name}"
        
        result = await self._query(query)
        
        if result and len(result) > 0:
            return len(result[0])
        return 0
    
    async def query_relations(self, from_table: str, from_id: str,
                             relation_name: str, direction: str = 'out') -> List[Dict[str, Any]]:
        """Query relations using SurrealDB's graph traversal.
        
        Args:
            from_table: Source table name
            from_id: Source document ID
            relation_name: Name of the relation
            direction: Direction of relations ('out', 'in', 'both')
            
        Returns:
            List of related documents
        """
        from_record = RecordID(from_table, from_id)
        
        if direction == 'out':
            query = f"SELECT * FROM {from_record}->{relation_name}"
        elif direction == 'in':
            query = f"SELECT * FROM {from_record}<-{relation_name}"
        elif direction == 'both':
            query = f"SELECT * FROM {from_record}<->{relation_name}"
        else:
            raise ValueError(f"Invalid direction: {direction}. Must be 'out', 'in', or 'both'")
        
        result = await self._query(query)
        
        if result and len(result) > 0:
            return [self._format_result_data(doc) for doc in result[0]]
        return []
    
    # Helper methods
    
    async def _execute(self, query: str) -> None:
        """Execute a query without returning results."""
        client = await self._get_client()
        await client.query(query)
    
    def _execute_sync(self, query: str) -> None:
        """Execute a query without returning results synchronously."""
        client = self._get_client_sync()
        client.query(query)
    
    async def _query(self, query: str) -> Any:
        """Execute a query and return results."""
        client = await self._get_client()
        return await client.query(query)
    
    def _format_document_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Format document data for SurrealDB storage."""
        from decimal import Decimal
        
        formatted = {}
        for key, value in data.items():
            # Handle special field types
            if hasattr(value, 'to_db'):
                formatted[key] = value.to_db()
            elif isinstance(value, Decimal):
                # Convert Decimal to float for SurrealDB
                formatted[key] = float(value)
            else:
                formatted[key] = value
        return formatted
    
    # Materialized view support
    
    async def create_materialized_view(self, materialized_document_class: Type) -> None:
        """Create a SurrealDB materialized view using DEFINE TABLE ... AS SELECT.
        
        Args:
            materialized_document_class: The MaterializedDocument class
        """
        view_name = materialized_document_class._meta.get('view_name') or \
                   materialized_document_class._meta.get('table_name') or \
                   materialized_document_class.__name__.lower()
        
        # Build the source query
        source_query = materialized_document_class._build_source_query()
        
        # Convert ClickHouse-specific functions to SurrealDB equivalents
        source_query = self._convert_query_to_surrealdb(source_query)
        
        # SurrealDB materialized view syntax
        query = f"DEFINE TABLE {view_name} AS {source_query}"
        
        # Debug: Print the generated query
        print("Generated SurrealDB Materialized View SQL:")
        print(query)
        print("=" * 60)
        
        await self._execute(query)
    
    async def drop_materialized_view(self, materialized_document_class: Type) -> None:
        """Drop a SurrealDB materialized view.
        
        Args:
            materialized_document_class: The MaterializedDocument class
        """
        view_name = materialized_document_class._meta.get('view_name') or \
                   materialized_document_class._meta.get('table_name') or \
                   materialized_document_class.__name__.lower()
        
        query = f"REMOVE TABLE {view_name}"
        await self._execute(query)
    
    async def refresh_materialized_view(self, materialized_document_class: Type) -> None:
        """Refresh a SurrealDB materialized view.
        
        Note: SurrealDB materialized views update automatically when data changes.
        This is a no-op for SurrealDB.
        
        Args:
            materialized_document_class: The MaterializedDocument class
        """
        # SurrealDB materialized views refresh automatically
        pass
    
    # Transaction support
    
    async def begin_transaction(self) -> Any:
        """Begin a transaction in SurrealDB.
        
        Returns:
            Transaction object (None for SurrealDB as it uses implicit transactions)
        """
        # SurrealDB doesn't have explicit transaction objects like PostgreSQL
        # All operations are implicitly transactional
        return None
    
    async def commit_transaction(self, transaction: Any) -> None:
        """Commit a transaction in SurrealDB.
        
        Args:
            transaction: The transaction object (unused for SurrealDB)
        """
        # SurrealDB auto-commits, no explicit commit needed
        pass
    
    async def rollback_transaction(self, transaction: Any) -> None:
        """Rollback a transaction in SurrealDB.
        
        Args:
            transaction: The transaction object (unused for SurrealDB)
        """
        # SurrealDB doesn't support explicit rollback
        # This would require implementing transaction scoping
        pass
    
    def _convert_query_to_surrealdb(self, query: str) -> str:
        """Convert ClickHouse-specific query syntax to SurrealDB.
        
        Args:
            query: The ClickHouse-style query
            
        Returns:
            SurrealDB-compatible query
        """
        # Handle COUNT DISTINCT - SurrealDB doesn't have direct COUNT DISTINCT
        # For materialized views, we'll use a simplified approach
        import re
        count_distinct_pattern = r'COUNT\(DISTINCT\s+([^)]+)\)'
        def replace_count_distinct(match):
            field = match.group(1).strip()
            # For SurrealDB, use a different approach for COUNT DISTINCT
            # We'll group by the field and count the groups
            return f'1'  # Simplified for now - each record contributes 1
        
        converted_query = re.sub(count_distinct_pattern, replace_count_distinct, query, flags=re.IGNORECASE)
        
        # Convert other ClickHouse functions to SurrealDB equivalents
        conversions = {
            'toDate(': 'time::day(',
            'toYYYYMM(': 'time::format(',
            'COUNT(*)': 'count()',
            'SUM(': 'math::sum(',
            'AVG(': 'math::mean(',
            'MIN(': 'math::min(',
            'MAX(': 'math::max(',
        }
        
        # Also handle COUNT(*) in SELECT clauses outside of aggregations
        converted_query = converted_query.replace('SELECT COUNT(*)', 'SELECT count()')
        
        for clickhouse_func, surrealdb_func in conversions.items():
            converted_query = converted_query.replace(clickhouse_func, surrealdb_func)
        
        # Handle special cases for time format
        if 'time::format(' in converted_query:
            # Convert toYYYYMM to proper SurrealDB time format
            converted_query = converted_query.replace(
                'time::format(', 'time::format('
            ).replace(') AS year_month', ', "%Y%m") AS year_month')
        
        return converted_query
    
    def _format_result_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Format result data from SurrealDB."""
        if not isinstance(data, dict):
            return data
        
        formatted = {}
        for key, value in data.items():
            # Handle RecordID conversion
            if isinstance(value, RecordID):
                formatted[key] = str(value)
            else:
                formatted[key] = value
        
        return formatted
    
    # Materialized view support
    
    async def create_materialized_view(self, materialized_document_class: Type) -> None:
        """Create a SurrealDB materialized view using DEFINE TABLE ... AS SELECT.
        
        Args:
            materialized_document_class: The MaterializedDocument class
        """
        view_name = materialized_document_class._meta.get('view_name') or \
                   materialized_document_class._meta.get('table_name') or \
                   materialized_document_class.__name__.lower()
        
        # Build the source query
        source_query = materialized_document_class._build_source_query()
        
        # Convert ClickHouse-specific functions to SurrealDB equivalents
        source_query = self._convert_query_to_surrealdb(source_query)
        
        # SurrealDB materialized view syntax
        query = f"DEFINE TABLE {view_name} AS {source_query}"
        
        # Debug: Print the generated query
        print("Generated SurrealDB Materialized View SQL:")
        print(query)
        print("=" * 60)
        
        await self._execute(query)
    
    async def drop_materialized_view(self, materialized_document_class: Type) -> None:
        """Drop a SurrealDB materialized view.
        
        Args:
            materialized_document_class: The MaterializedDocument class
        """
        view_name = materialized_document_class._meta.get('view_name') or \
                   materialized_document_class._meta.get('table_name') or \
                   materialized_document_class.__name__.lower()
        
        query = f"REMOVE TABLE {view_name}"
        await self._execute(query)
    
    async def refresh_materialized_view(self, materialized_document_class: Type) -> None:
        """Refresh a SurrealDB materialized view.
        
        Note: SurrealDB materialized views update automatically when data changes.
        This is a no-op for SurrealDB.
        
        Args:
            materialized_document_class: The MaterializedDocument class
        """
        # SurrealDB materialized views refresh automatically
        pass
    
    # Transaction support
    
    async def begin_transaction(self) -> Any:
        """Begin a transaction in SurrealDB.
        
        Returns:
            Transaction object (None for SurrealDB as it uses implicit transactions)
        """
        # SurrealDB doesn't have explicit transaction objects like PostgreSQL
        # All operations are implicitly transactional
        return None
    
    async def commit_transaction(self, transaction: Any) -> None:
        """Commit a transaction in SurrealDB.
        
        Args:
            transaction: The transaction object (unused for SurrealDB)
        """
        # SurrealDB auto-commits, no explicit commit needed
        pass
    
    async def rollback_transaction(self, transaction: Any) -> None:
        """Rollback a transaction in SurrealDB.
        
        Args:
            transaction: The transaction object (unused for SurrealDB)
        """
        # SurrealDB doesn't support explicit rollback
        # This would require implementing transaction scoping
        pass
    
    
    def _create_pool(self) -> ConnectionPoolBase:
        """Create a SurrealDB-specific connection pool.
        
        Returns:
            SurrealDBConnectionPool instance
        """
        return SurrealDBConnectionPool(
            connection_config=self.connection_config,
            pool_config=self.pool_config
        )