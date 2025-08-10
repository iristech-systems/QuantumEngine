"""Base backend interface for QuantumEngine."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Type, Union

from ..connection import ConnectionPoolBase, PoolConfig


class BaseBackend(ABC):
    """Abstract base class for database backends.
    
    All database backends must implement this interface to work with QuantumEngine.
    """
    
    def __init__(self, connection_config: dict, pool_config: Optional[PoolConfig] = None):
        """Initialize the backend with connection and pool configurations."""
        self.connection_config = connection_config
        self.pool_config = pool_config or PoolConfig()
        self._pool: Optional[ConnectionPoolBase] = None
    
    def get_pool(self) -> ConnectionPoolBase:
        """Get or create the connection pool."""
        if self._pool is None:
            self._pool = self._create_pool()
        return self._pool

    @abstractmethod
    def _create_pool(self) -> ConnectionPoolBase:
        """Create a backend-specific connection pool."""
        pass

    async def execute_with_pool(self, operation: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute an operation using a pooled connection."""
        pool = self.get_pool()
        conn = await pool.get_connection()
        try:
            # The operation is expected to be a coroutine function that accepts the connection as its first argument
            return await operation(conn, *args, **kwargs)
        finally:
            await pool.return_connection(conn)
    
    @abstractmethod
    async def create_table(self, document_class: Type, **kwargs) -> None:
        """Create a table/collection for the document class.
        
        Args:
            document_class: The document class to create a table for
            **kwargs: Backend-specific options (e.g., schemafull for SurrealDB)
        """
        pass
    
    @abstractmethod
    async def insert(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single document.
        
        Args:
            table_name: The table/collection name
            data: The document data to insert
            
        Returns:
            The inserted document with any generated fields (e.g., id)
        """
        pass
    
    @abstractmethod
    async def insert_many(self, table_name: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Insert multiple documents.
        
        Args:
            table_name: The table/collection name
            data: List of documents to insert
            
        Returns:
            List of inserted documents
        """
        pass
    
    @abstractmethod
    async def select(self, table_name: str, conditions: List[str], 
                    fields: Optional[List[str]] = None,
                    limit: Optional[int] = None, 
                    offset: Optional[int] = None,
                    order_by: Optional[List[tuple[str, str]]] = None) -> List[Dict[str, Any]]:
        """Select documents from a table.
        
        Args:
            table_name: The table/collection name
            conditions: List of condition strings (already formatted by build_condition)
            fields: List of fields to return (None for all fields)
            limit: Maximum number of results
            offset: Number of results to skip
            order_by: List of (field, direction) tuples for ordering
            
        Returns:
            List of matching documents
        """
        pass
    
    @abstractmethod
    async def count(self, table_name: str, conditions: List[str]) -> int:
        """Count documents matching conditions.
        
        Args:
            table_name: The table/collection name
            conditions: List of condition strings
            
        Returns:
            Number of matching documents
        """
        pass
    
    @abstractmethod
    async def update(self, table_name: str, conditions: List[str], 
                    data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Update documents matching conditions.
        
        Args:
            table_name: The table/collection name
            conditions: List of condition strings
            data: The fields to update
            
        Returns:
            List of updated documents
        """
        pass
    
    @abstractmethod
    async def delete(self, table_name: str, conditions: List[str]) -> int:
        """Delete documents matching conditions.
        
        Args:
            table_name: The table/collection name
            conditions: List of condition strings
            
        Returns:
            Number of deleted documents
        """
        pass
    
    @abstractmethod
    async def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """Drop a table/collection.
        
        Args:
            table_name: The table/collection name to drop
            if_exists: Whether to use IF EXISTS clause to avoid errors if table doesn't exist
        """
        pass
    
    @abstractmethod
    async def execute_raw(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a raw query.
        
        Args:
            query: The raw query string
            params: Optional query parameters
            
        Returns:
            Query result (backend-specific)
        """
        pass
    
    @abstractmethod
    def build_condition(self, field: str, operator: str, value: Any) -> str:
        """Build a condition string for the backend's query language.
        
        Args:
            field: The field name
            operator: The operator (=, !=, >, <, >=, <=, in, contains, etc.)
            value: The value to compare against
            
        Returns:
            A condition string in the backend's query language
        """
        pass
    
    @abstractmethod
    def get_field_type(self, field: Any) -> str:
        """Get the database field type for a SurrealEngine field.
        
        Args:
            field: A SurrealEngine field instance
            
        Returns:
            The corresponding database field type
        """
        pass
    
    @abstractmethod
    def format_value(self, value: Any, field_type: Optional[str] = None) -> Any:
        """Format a value for the backend's query language.
        
        Args:
            value: The value to format
            field_type: Optional field type hint
            
        Returns:
            The formatted value
        """
        pass
    
    @abstractmethod
    async def begin_transaction(self) -> Any:
        """Begin a transaction.
        
        Returns:
            Transaction object (backend-specific)
        """
        pass
    
    @abstractmethod
    async def commit_transaction(self, transaction: Any) -> None:
        """Commit a transaction.
        
        Args:
            transaction: The transaction object
        """
        pass
    
    @abstractmethod
    async def rollback_transaction(self, transaction: Any) -> None:
        """Rollback a transaction.
        
        Args:
            transaction: The transaction object
        """
        pass
    
    # Backend capability detection methods
    
    def supports_transactions(self) -> bool:
        """Check if the backend supports transactions.
        
        Returns:
            True if transactions are supported, False otherwise
        """
        return False
    
    def supports_references(self) -> bool:
        """Check if the backend supports references/foreign keys.
        
        Returns:
            True if references are supported, False otherwise
        """
        return False
    
    def supports_graph_relations(self) -> bool:
        """Check if the backend supports graph-style relations.
        
        Returns:
            True if graph relations are supported, False otherwise
        """
        return False
    
    def supports_direct_record_access(self) -> bool:
        """Check if the backend supports direct record access syntax.
        
        Returns:
            True if direct record access is supported, False otherwise
        """
        return False
    
    def supports_explain(self) -> bool:
        """Check if the backend supports EXPLAIN queries.
        
        Returns:
            True if EXPLAIN is supported, False otherwise
        """
        return False
    
    def supports_indexes(self) -> bool:
        """Check if the backend supports indexes.
        
        Returns:
            True if indexes are supported, False otherwise
        """
        return False
    
    def supports_full_text_search(self) -> bool:
        """Check if the backend supports full-text search.
        
        Returns:
            True if full-text search is supported, False otherwise
        """
        return False
    
    def supports_bulk_operations(self) -> bool:
        """Check if the backend supports bulk insert/update/delete operations.
        
        Returns:
            True if bulk operations are supported, False otherwise
        """
        return False
    
    def supports_materialized_views(self) -> bool:
        """Check if the backend supports materialized views.
        
        Returns:
            True if materialized views are supported, False otherwise
        """
        return False
    
    def get_optimized_methods(self) -> Dict[str, str]:
        """Get backend-specific optimization methods.
        
        Returns:
            Dictionary mapping operation names to backend-specific implementations
        """
        return {}
    
    # Helper methods that backends can override if needed
    
    def supports_schemas(self) -> bool:
        """Check if this backend supports strict schemas.
        
        Returns:
            True if schemas are supported
        """
        return True
    
    def get_capabilities(self) -> Dict[str, bool]:
        """Get backend capabilities.
        
        Returns:
            Dictionary of capability flags
        """
        return {
            'transactions': self.supports_transactions(),
            'schemas': self.supports_schemas(),
            'references': self.supports_references(),
            'graph_relations': self.supports_graph_relations(),
            'direct_record_access': self.supports_direct_record_access(),
            'explain': self.supports_explain(),
            'indexes': self.supports_indexes(),
            'full_text_search': self.supports_full_text_search(),
            'bulk_operations': self.supports_bulk_operations(),
            'materialized_views': self.supports_materialized_views(),
        }
    
    # Graph/Relation methods (optional - not all backends support these)
    
    async def create_relation(self, from_table: str, from_id: str, 
                             relation_name: str, to_table: str, to_id: str,
                             attributes: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Create a relation between two documents.
        
        Args:
            from_table: Source table name
            from_id: Source document ID
            relation_name: Name of the relation
            to_table: Target table name  
            to_id: Target document ID
            attributes: Optional attributes for the relation
            
        Returns:
            The created relation record or None
            
        Raises:
            NotImplementedError: If backend doesn't support graph relations
        """
        raise NotImplementedError("Graph relations not supported by this backend")
    
    async def delete_relation(self, from_table: str, from_id: str,
                             relation_name: str, to_table: Optional[str] = None, 
                             to_id: Optional[str] = None) -> int:
        """Delete relations.
        
        Args:
            from_table: Source table name
            from_id: Source document ID
            relation_name: Name of the relation
            to_table: Target table name (optional)
            to_id: Target document ID (optional)
            
        Returns:
            Number of relations deleted
            
        Raises:
            NotImplementedError: If backend doesn't support graph relations
        """
        raise NotImplementedError("Graph relations not supported by this backend")
    
    async def query_relations(self, from_table: str, from_id: str,
                             relation_name: str, direction: str = 'out') -> List[Dict[str, Any]]:
        """Query relations from a document.
        
        Args:
            from_table: Source table name
            from_id: Source document ID
            relation_name: Name of the relation
            direction: Direction of relations ('out', 'in', 'both')
            
        Returns:
            List of related documents
            
        Raises:
            NotImplementedError: If backend doesn't support graph relations
        """
        raise NotImplementedError("Graph relations not supported by this backend")