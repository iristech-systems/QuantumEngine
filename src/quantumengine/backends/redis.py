"""Redis/Dragonfly backend implementation for QuantumEngine."""

import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional, Type, Union
from datetime import datetime
from decimal import Decimal

import redis
from redis import asyncio as aioredis

from .base import BaseBackend


class RedisBackend(BaseBackend):
    """Redis/Dragonfly backend implementation.
    
    This backend implements the BaseBackend interface for Redis and Dragonfly,
    providing document storage using Redis data structures.
    
    Documents are stored as Redis hashes with JSON-serialized values for complex types.
    Secondary indexes are implemented using Redis sorted sets.
    """
    
    def __init__(self, connection: Any) -> None:
        """Initialize the Redis backend.
        
        Args:
            connection: Redis connection (sync or async)
        """
        super().__init__(connection)
        # For now, assume sync Redis client - async support can be added later
        self.is_async = False
        
        # Key prefixes for organization
        self.doc_prefix = "doc:"
        self.index_prefix = "idx:"
        self.meta_prefix = "meta:"
        self.seq_prefix = "seq:"
    
    def _initialize_client(self, connection: Any) -> Any:
        """Initialize the Redis client from the connection.
        
        Args:
            connection: The connection object from ConnectionRegistry
            
        Returns:
            The Redis client object
        """
        # For Redis, the connection IS the client
        return connection
    
    def _get_doc_key(self, table_name: str, doc_id: str) -> str:
        """Get the Redis key for a document."""
        return f"{self.doc_prefix}{table_name}:{doc_id}"
    
    def _get_index_key(self, table_name: str, field: str) -> str:
        """Get the Redis key for an index."""
        return f"{self.index_prefix}{table_name}:{field}"
    
    def _get_table_key(self, table_name: str) -> str:
        """Get the Redis key for table metadata."""
        return f"{self.meta_prefix}table:{table_name}"
    
    def _get_sequence_key(self, table_name: str) -> str:
        """Get the Redis key for ID sequence."""
        return f"{self.seq_prefix}{table_name}"
    
    async def _execute(self, method: str, *args, **kwargs) -> Any:
        """Execute a Redis command."""
        redis_method = getattr(self.client, method)
        if self.is_async:
            return await redis_method(*args, **kwargs)
        return redis_method(*args, **kwargs)
    
    async def _pipeline(self):
        """Create a pipeline for batch operations."""
        if self.is_async:
            return self.client.pipeline()
        return self.client.pipeline()
    
    async def _execute_pipeline(self, pipe):
        """Execute a pipeline."""
        if self.is_async:
            return await pipe.execute()
        return pipe.execute()
    
    def _serialize_value(self, value: Any) -> str:
        """Serialize a value for storage in Redis."""
        if value is None:
            return "null"
        elif isinstance(value, (str, int, float, bool)):
            return json.dumps(value)
        elif isinstance(value, datetime):
            return json.dumps(value.isoformat())
        elif isinstance(value, Decimal):
            return json.dumps(str(value))
        elif isinstance(value, (list, dict)):
            return json.dumps(value)
        else:
            return json.dumps(str(value))
    
    def _deserialize_value(self, value: str) -> Any:
        """Deserialize a value from Redis storage."""
        if value is None:
            return None
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    
    def _serialize_document(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Serialize a document for Redis hash storage."""
        return {k: self._serialize_value(v) for k, v in data.items()}
    
    def _deserialize_document(self, data: Dict[bytes, bytes]) -> Dict[str, Any]:
        """Deserialize a document from Redis hash storage."""
        if not data:
            return {}
        
        result = {}
        for k, v in data.items():
            key = k.decode() if isinstance(k, bytes) else k
            value = v.decode() if isinstance(v, bytes) else v
            result[key] = self._deserialize_value(value)
        return result
    
    async def create_table(self, document_class: Type, **kwargs) -> None:
        """Create a table/collection for the document class.
        
        For Redis, this primarily sets up metadata and indexes.
        
        Args:
            document_class: The document class to create a table for
            **kwargs: Backend-specific options
        """
        table_name = document_class._meta.get('collection')
        
        # Store table metadata
        table_meta = {
            'created_at': datetime.utcnow().isoformat(),
            'fields': json.dumps({
                name: self.get_field_type(field)
                for name, field in document_class._fields.items()
            }),
            'indexes': json.dumps(document_class._meta.get('indexes', []))
        }
        
        await self._execute('hset', self._get_table_key(table_name), mapping=table_meta)
        
        # Initialize ID sequence
        await self._execute('set', self._get_sequence_key(table_name), 0)
    
    async def insert(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single document.
        
        Args:
            table_name: The table name
            data: The document data to insert
            
        Returns:
            The inserted document with any generated fields
        """
        # Generate ID if not provided
        if 'id' not in data or not data['id']:
            # Get next ID from sequence
            doc_id = await self._execute('incr', self._get_sequence_key(table_name))
            data['id'] = str(doc_id)
        else:
            doc_id = data['id']
        
        # Create document key
        doc_key = self._get_doc_key(table_name, str(doc_id))
        
        # Serialize and store document
        serialized = self._serialize_document(data)
        await self._execute('hset', doc_key, mapping=serialized)
        
        # Update indexes
        pipe = await self._pipeline()
        for field, value in data.items():
            if value is not None:
                index_key = self._get_index_key(table_name, field)
                # For numeric values, use them as scores in sorted sets
                if isinstance(value, (int, float)):
                    pipe.zadd(index_key, {doc_key: float(value)})
                else:
                    # For non-numeric, use lexicographical sorting
                    pipe.zadd(index_key, {doc_key: 0})
        
        await self._execute_pipeline(pipe)
        
        return data
    
    async def insert_many(self, table_name: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Insert multiple documents.
        
        Args:
            table_name: The table name
            data: List of documents to insert
            
        Returns:
            List of inserted documents
        """
        results = []
        pipe = await self._pipeline()
        
        for doc in data:
            # Generate ID if needed
            if 'id' not in doc or not doc['id']:
                doc_id = await self._execute('incr', self._get_sequence_key(table_name))
                doc['id'] = str(doc_id)
            else:
                doc_id = doc['id']
            
            # Add to pipeline
            doc_key = self._get_doc_key(table_name, str(doc_id))
            serialized = self._serialize_document(doc)
            pipe.hset(doc_key, mapping=serialized)
            
            # Update indexes in pipeline
            for field, value in doc.items():
                if value is not None:
                    index_key = self._get_index_key(table_name, field)
                    if isinstance(value, (int, float)):
                        pipe.zadd(index_key, {doc_key: float(value)})
                    else:
                        pipe.zadd(index_key, {doc_key: 0})
            
            results.append(doc)
        
        await self._execute_pipeline(pipe)
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
            offset: Number of results to skip
            order_by: List of (field, direction) tuples
            
        Returns:
            List of matching documents
        """
        # For now, implement a simple scan approach
        # In production, this would use indexes more intelligently
        
        # Get all document keys for the table
        pattern = f"{self.doc_prefix}{table_name}:*"
        cursor = 0
        keys = []
        
        # Scan for all keys matching the pattern
        while True:
            if self.is_async:
                cursor, batch = await self.client.scan(cursor, match=pattern, count=1000)
            else:
                cursor, batch = self.client.scan(cursor, match=pattern, count=1000)
            
            keys.extend(batch)
            if cursor == 0:
                break
        
        # Fetch all documents
        results = []
        if keys:
            pipe = await self._pipeline()
            for key in keys:
                pipe.hgetall(key)
            
            docs = await self._execute_pipeline(pipe)
            
            for i, doc_data in enumerate(docs):
                if doc_data:
                    doc = self._deserialize_document(doc_data)
                    
                    # Apply conditions (basic implementation)
                    if self._match_conditions(doc, conditions):
                        results.append(doc)
        
        # Apply ordering
        if order_by:
            for field, direction in reversed(order_by):
                reverse = direction.upper() == 'DESC'
                results.sort(key=lambda x: x.get(field, ''), reverse=reverse)
        
        # Apply offset and limit
        if offset:
            results = results[offset:]
        if limit:
            results = results[:limit]
        
        # Apply field projection
        if fields:
            results = [
                {k: v for k, v in doc.items() if k in fields}
                for doc in results
            ]
        
        return results
    
    def _match_conditions(self, doc: Dict[str, Any], conditions: List[str]) -> bool:
        """Check if a document matches the given conditions.
        
        This is a simplified implementation. In production, you'd want
        to parse conditions properly and use indexes.
        """
        if not conditions:
            return True
        
        # For now, just check for basic equality in conditions
        for condition in conditions:
            # Simple parsing - expects conditions like "field = value"
            if '=' in condition and '!=' not in condition:
                parts = condition.split('=', 1)
                if len(parts) == 2:
                    field = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    
                    # Get the actual field value from the document
                    doc_value = doc.get(field)
                    
                    # Try to convert condition value to appropriate type
                    try:
                        if value == 'null':
                            value = None
                        elif value == 'true':
                            value = True
                        elif value == 'false':
                            value = False
                        elif value.isdigit():
                            value = int(value)
                        elif '.' in value:
                            try:
                                value = float(value)
                            except ValueError:
                                pass
                    except (ValueError, AttributeError):
                        pass
                    
                    # Smart comparison: try both string and type-converted values
                    # This handles cases where ID fields are strings but conditions use integers
                    if doc_value != value:
                        # Try string comparison if one is string and other is numeric
                        if (isinstance(doc_value, str) and isinstance(value, (int, float))):
                            if doc_value != str(value):
                                return False
                        elif (isinstance(value, str) and isinstance(doc_value, (int, float))):
                            if str(doc_value) != value:
                                return False
                        else:
                            return False
        
        return True
    
    async def count(self, table_name: str, conditions: List[str]) -> int:
        """Count documents matching conditions.
        
        Args:
            table_name: The table name
            conditions: List of condition strings
            
        Returns:
            Number of matching documents
        """
        # Use select without limit to count
        docs = await self.select(table_name, conditions)
        return len(docs)
    
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
        # Find matching documents
        docs = await self.select(table_name, conditions)
        
        if not docs:
            return []
        
        # Update each document
        pipe = await self._pipeline()
        updated = []
        
        for doc in docs:
            doc_id = doc['id']
            doc_key = self._get_doc_key(table_name, str(doc_id))
            
            # Merge updates
            doc.update(data)
            
            # Update in Redis
            serialized = self._serialize_document(doc)
            pipe.hset(doc_key, mapping=serialized)
            
            # Update indexes for changed fields
            for field, value in data.items():
                if value is not None:
                    index_key = self._get_index_key(table_name, field)
                    if isinstance(value, (int, float)):
                        pipe.zadd(index_key, {doc_key: float(value)})
                    else:
                        pipe.zadd(index_key, {doc_key: 0})
            
            updated.append(doc)
        
        await self._execute_pipeline(pipe)
        return updated
    
    async def delete(self, table_name: str, conditions: List[str]) -> int:
        """Delete documents matching conditions.
        
        Args:
            table_name: The table name
            conditions: List of condition strings
            
        Returns:
            Number of deleted documents
        """
        # Find matching documents
        docs = await self.select(table_name, conditions)
        
        if not docs:
            return 0
        
        # Delete each document
        pipe = await self._pipeline()
        
        for doc in docs:
            doc_id = doc['id']
            doc_key = self._get_doc_key(table_name, str(doc_id))
            
            # Delete document
            pipe.delete(doc_key)
            
            # Remove from indexes
            for field in doc.keys():
                index_key = self._get_index_key(table_name, field)
                pipe.zrem(index_key, doc_key)
        
        await self._execute_pipeline(pipe)
        return len(docs)
    
    async def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """Drop a table/collection.
        
        Args:
            table_name: The table name to drop
            if_exists: Whether to ignore if table doesn't exist
        """
        # Delete all documents
        pattern = f"{self.doc_prefix}{table_name}:*"
        await self._delete_keys_by_pattern(pattern)
        
        # Delete all indexes
        pattern = f"{self.index_prefix}{table_name}:*"
        await self._delete_keys_by_pattern(pattern)
        
        # Delete table metadata
        await self._execute('delete', self._get_table_key(table_name))
        
        # Delete sequence
        await self._execute('delete', self._get_sequence_key(table_name))
    
    async def _delete_keys_by_pattern(self, pattern: str) -> None:
        """Delete all keys matching a pattern."""
        cursor = 0
        while True:
            if self.is_async:
                cursor, keys = await self.client.scan(cursor, match=pattern, count=1000)
            else:
                cursor, keys = self.client.scan(cursor, match=pattern, count=1000)
            
            if keys:
                await self._execute('delete', *keys)
            
            if cursor == 0:
                break
    
    async def execute_raw(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a raw Redis command.
        
        Args:
            query: The Redis command
            params: Optional parameters
            
        Returns:
            Command result
        """
        # Parse the command
        parts = query.split()
        if not parts:
            return None
        
        command = parts[0].lower()
        args = parts[1:]
        
        # Substitute parameters if provided
        if params:
            args = [
                params.get(arg[1:], arg) if arg.startswith('$') else arg
                for arg in args
            ]
        
        return await self._execute(command, *args)
    
    def build_condition(self, field: str, operator: str, value: Any) -> str:
        """Build a condition string for Redis queries.
        
        Args:
            field: The field name
            operator: The operator
            value: The value to compare against
            
        Returns:
            A condition string
        """
        # Format value for condition
        if value is None:
            formatted_value = 'null'
        elif isinstance(value, str):
            formatted_value = f"'{value}'"
        else:
            formatted_value = str(value)
        
        return f"{field} {operator} {formatted_value}"
    
    def get_field_type(self, field: Any) -> str:
        """Get the Redis field type for a QuantumEngine field.
        
        Args:
            field: A QuantumEngine field instance
            
        Returns:
            The corresponding Redis field type
        """
        # Redis doesn't have strict types like SQL databases
        # We use this for metadata and indexing hints
        field_type = type(field).__name__
        
        type_map = {
            'StringField': 'string',
            'IntField': 'integer',
            'FloatField': 'float',
            'DecimalField': 'decimal',
            'BooleanField': 'boolean',
            'DateTimeField': 'datetime',
            'ListField': 'list',
            'DictField': 'hash',
            'ReferenceField': 'reference',
        }
        
        return type_map.get(field_type, 'string')
    
    def format_value(self, value: Any, field_type: Optional[str] = None) -> Any:
        """Format a value for Redis storage.
        
        Args:
            value: The value to format
            field_type: Optional field type hint
            
        Returns:
            The formatted value
        """
        return self._serialize_value(value)
    
    async def begin_transaction(self) -> Any:
        """Begin a transaction.
        
        Returns:
            Transaction object (pipeline)
        """
        pipe = await self._pipeline()
        if self.is_async:
            pipe.multi()
        else:
            pipe.multi()
        return pipe
    
    async def commit_transaction(self, transaction: Any) -> None:
        """Commit a transaction.
        
        Args:
            transaction: The transaction object (pipeline)
        """
        await self._execute_pipeline(transaction)
    
    async def rollback_transaction(self, transaction: Any) -> None:
        """Rollback a transaction.
        
        Args:
            transaction: The transaction object (pipeline)
        """
        # Redis doesn't support rollback, but we can discard the pipeline
        if hasattr(transaction, 'reset'):
            transaction.reset()
    
    # Backend capabilities
    
    def supports_transactions(self) -> bool:
        """Redis supports MULTI/EXEC transactions."""
        return True
    
    def supports_indexes(self) -> bool:
        """Redis supports secondary indexes via sorted sets."""
        return True
    
    def supports_bulk_operations(self) -> bool:
        """Redis supports bulk operations via pipelines."""
        return True
    
    def supports_schemas(self) -> bool:
        """Redis is schemaless."""
        return False
    
    def supports_materialized_views(self) -> bool:
        """Redis doesn't support materialized views."""
        return False