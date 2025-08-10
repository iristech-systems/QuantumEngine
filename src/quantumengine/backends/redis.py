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
from ..connection import ConnectionPoolBase, PoolConfig
from ..backends.pools.redis import RedisConnectionPool


class RedisBackend(BaseBackend):
    """Redis/Dragonfly backend implementation.
    
    This backend implements the BaseBackend interface for Redis and Dragonfly,
    providing document storage using Redis data structures.
    
    Documents are stored as Redis hashes with JSON-serialized values for complex types.
    Secondary indexes are implemented using Redis sorted sets.
    """
    
    def __init__(self, connection_config: dict, pool_config: Optional[PoolConfig] = None):
        """Initialize the Redis backend."""
        super().__init__(connection_config, pool_config)
        self.is_async = True  # The pooling implementation is asynchronous
        
        # Key prefixes for organization
        self.doc_prefix = "doc:"
        self.index_prefix = "idx:"
        self.meta_prefix = "meta:"
        self.seq_prefix = "seq:"

    def _create_pool(self) -> ConnectionPoolBase:
        """Create the Redis-specific connection pool."""
        return RedisConnectionPool(self.connection_config, self.pool_config)
    
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
    
    async def _execute(self, conn: Any, method: str, *args, **kwargs) -> Any:
        """Execute a Redis command on a given connection."""
        redis_method = getattr(conn, method)
        return await redis_method(*args, **kwargs)

    def _pipeline(self, conn: Any):
        """Create a pipeline for batch operations from a given connection."""
        return conn.pipeline()

    async def _execute_pipeline(self, pipe: Any) -> Any:
        """Execute a pipeline."""
        return await pipe.execute()
    
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
    
    async def _create_table_op(self, conn: Any, document_class: Type, **kwargs) -> None:
        """Operation to create a table/collection for the document class."""
        table_name = document_class._meta.get('collection')
        
        table_meta = {
            'created_at': datetime.utcnow().isoformat(),
            'fields': json.dumps({name: self.get_field_type(field) for name, field in document_class._fields.items()}),
            'indexes': json.dumps(document_class._meta.get('indexes', []))
        }
        
        await self._execute(conn, 'hset', self._get_table_key(table_name), mapping=table_meta)
        await self._execute(conn, 'set', self._get_sequence_key(table_name), 0)

    async def create_table(self, document_class: Type, **kwargs) -> None:
        """Create a table/collection for the document class."""
        await self.execute_with_pool(self._create_table_op, document_class, **kwargs)
    
    async def _insert_op(self, conn: Any, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Operation to insert a single document."""
        if 'id' not in data or not data['id']:
            doc_id = await self._execute(conn, 'incr', self._get_sequence_key(table_name))
            data['id'] = str(doc_id)
        else:
            doc_id = data['id']
        
        doc_key = self._get_doc_key(table_name, str(doc_id))
        serialized = self._serialize_document(data)
        
        pipe = self._pipeline(conn)
        pipe.hset(doc_key, mapping=serialized)
        for field, value in data.items():
            if value is not None:
                index_key = self._get_index_key(table_name, field)
                score = float(value) if isinstance(value, (int, float)) else 0
                pipe.zadd(index_key, {doc_key: score})
        
        await self._execute_pipeline(pipe)
        return data

    async def insert(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single document."""
        return await self.execute_with_pool(self._insert_op, table_name, data)

    async def _insert_many_op(self, conn: Any, table_name: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Operation to insert multiple documents."""
        results = []
        
        # Generate IDs for docs that need them. This requires separate calls.
        for doc in data:
            if 'id' not in doc or not doc['id']:
                doc_id = await self._execute(conn, 'incr', self._get_sequence_key(table_name))
                doc['id'] = str(doc_id)

        # Pipeline the rest of the operations
        pipe = self._pipeline(conn)
        for doc in data:
            doc_key = self._get_doc_key(table_name, str(doc['id']))
            serialized = self._serialize_document(doc)
            pipe.hset(doc_key, mapping=serialized)
            
            for field, value in doc.items():
                if value is not None:
                    index_key = self._get_index_key(table_name, field)
                    score = float(value) if isinstance(value, (int, float)) else 0
                    pipe.zadd(index_key, {doc_key: score})
            results.append(doc)

        await self._execute_pipeline(pipe)
        return results

    async def insert_many(self, table_name: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Insert multiple documents."""
        return await self.execute_with_pool(self._insert_many_op, table_name, data)
    
    async def _select_op(self, conn: Any, table_name: str, conditions: List[str], fields: Optional[List[str]], limit: Optional[int], offset: Optional[int], order_by: Optional[List[tuple[str, str]]]) -> List[Dict[str, Any]]:
        """Operation to select documents from a table."""
        pattern = f"{self.doc_prefix}{table_name}:*"
        cursor = 0
        keys = []
        while True:
            cursor, batch = await conn.scan(cursor, match=pattern, count=1000)
            keys.extend(batch)
            if cursor == 0:
                break
        
        results = []
        if keys:
            pipe = self._pipeline(conn)
            for key in keys:
                pipe.hgetall(key)
            docs = await self._execute_pipeline(pipe)
            for doc_data in docs:
                if doc_data:
                    doc = self._deserialize_document(doc_data)
                    if self._match_conditions(doc, conditions):
                        results.append(doc)
        
        if order_by:
            for field, direction in reversed(order_by):
                results.sort(key=lambda x: x.get(field, ''), reverse=(direction.upper() == 'DESC'))
        
        if offset:
            results = results[offset:]
        if limit:
            results = results[:limit]
        
        if fields:
            results = [{k: v for k, v in doc.items() if k in fields} for doc in results]
        
        return results

    async def select(self, table_name: str, conditions: List[str], fields: Optional[List[str]] = None, limit: Optional[int] = None, offset: Optional[int] = None, order_by: Optional[List[tuple[str, str]]] = None) -> List[Dict[str, Any]]:
        """Select documents from a table."""
        return await self.execute_with_pool(self._select_op, table_name, conditions, fields, limit, offset, order_by)
    
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
    
    async def _count_op(self, conn: Any, table_name: str, conditions: List[str]) -> int:
        """Operation to count documents."""
        # This is inefficient, but matches the original logic. A real implementation
        # would use indexes or other Redis features.
        docs = await self._select_op(conn, table_name, conditions, fields=None, limit=None, offset=None, order_by=None)
        return len(docs)

    async def count(self, table_name: str, conditions: List[str]) -> int:
        """Count documents matching conditions."""
        return await self.execute_with_pool(self._count_op, table_name, conditions)
    
    async def _update_op(self, conn: Any, table_name: str, conditions: List[str], data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Operation to update documents."""
        docs = await self._select_op(conn, table_name, conditions, fields=None, limit=None, offset=None, order_by=None)
        if not docs:
            return []
        
        pipe = self._pipeline(conn)
        updated = []
        for doc in docs:
            doc_id = doc['id']
            doc_key = self._get_doc_key(table_name, str(doc_id))
            doc.update(data)
            serialized = self._serialize_document(doc)
            pipe.hset(doc_key, mapping=serialized)
            for field, value in data.items():
                if value is not None:
                    index_key = self._get_index_key(table_name, field)
                    score = float(value) if isinstance(value, (int, float)) else 0
                    pipe.zadd(index_key, {doc_key: score})
            updated.append(doc)
        
        await self._execute_pipeline(pipe)
        return updated

    async def update(self, table_name: str, conditions: List[str], data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Update documents matching conditions."""
        return await self.execute_with_pool(self._update_op, table_name, conditions, data)

    async def _delete_op(self, conn: Any, table_name: str, conditions: List[str]) -> int:
        """Operation to delete documents."""
        docs = await self._select_op(conn, table_name, conditions, fields=None, limit=None, offset=None, order_by=None)
        if not docs:
            return 0
        
        pipe = self._pipeline(conn)
        for doc in docs:
            doc_key = self._get_doc_key(table_name, str(doc['id']))
            pipe.delete(doc_key)
            for field in doc.keys():
                index_key = self._get_index_key(table_name, field)
                pipe.zrem(index_key, doc_key)
        
        await self._execute_pipeline(pipe)
        return len(docs)

    async def delete(self, table_name: str, conditions: List[str]) -> int:
        """Delete documents matching conditions."""
        return await self.execute_with_pool(self._delete_op, table_name, conditions)
    
    async def _drop_table_op(self, conn: Any, table_name: str, if_exists: bool = True) -> None:
        """Operation to drop a table."""
        # Note: if_exists is not easily handled here without an extra EXISTS call.
        patterns = [
            f"{self.doc_prefix}{table_name}:*",
            f"{self.index_prefix}{table_name}:*"
        ]
        for pattern in patterns:
            await self._delete_keys_by_pattern(conn, pattern)
        
        table_key = self._get_table_key(table_name)
        seq_key = self._get_sequence_key(table_name)
        await self._execute(conn, 'delete', table_key, seq_key)

    async def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """Drop a table/collection."""
        await self.execute_with_pool(self._drop_table_op, table_name, if_exists=if_exists)

    async def _delete_keys_by_pattern(self, conn: Any, pattern: str) -> None:
        """Delete all keys matching a pattern using a provided connection."""
        cursor = 0
        while True:
            cursor, keys = await conn.scan(cursor, match=pattern, count=1000)
            if keys:
                await self._execute(conn, 'delete', *keys)
            if cursor == 0:
                break

    async def _execute_raw_op(self, conn: Any, query: str, params: Optional[Dict[str, Any]]) -> Any:
        """Operation to execute a raw Redis command."""
        parts = query.split()
        if not parts:
            return None
        command = parts[0].lower()
        args = parts[1:]
        if params:
            args = [params.get(arg[1:], arg) if arg.startswith('$') else arg for arg in args]
        return await self._execute(conn, command, *args)

    async def execute_raw(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a raw Redis command."""
        return await self.execute_with_pool(self._execute_raw_op, query, params=params)
    
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