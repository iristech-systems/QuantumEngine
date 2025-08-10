# AI Agent Onboarding Guide for QuantumEngine

## Quick Start for AI Agents

This guide helps AI agents quickly understand and contribute to the QuantumEngine codebase. QuantumEngine is a multi-backend Object-Document Mapping (ODM) library that provides a unified interface for SurrealDB, ClickHouse, and Redis.

## 🎯 Core Concepts You Must Understand

### 1. Multi-Backend Architecture
Unlike traditional ORMs that work with one database, QuantumEngine supports multiple backends simultaneously:

```python
# Same API, different backends
class User(Document):
    name = StringField(required=True)
    class Meta:
        backend = "surrealdb"  # Could be "clickhouse" or "redis"

class Analytics(Document):
    event = StringField(required=True)
    class Meta:
        backend = "clickhouse"  # Optimized for analytics
```

### 2. Document-Centric Design
Everything revolves around `Document` classes that map to database collections/tables:

```python
class Person(Document):
    name = StringField(required=True)
    age = IntField(min_value=0)
    email = EmailField()
    
    class Meta:
        collection = "people"
        backend = "surrealdb"
```

### 3. Backend Specialization
Each backend has unique strengths:
- **SurrealDB**: Graph relations, real-time, transactions
- **ClickHouse**: Analytics, time-series, aggregations  
- **Redis**: Caching, sessions, fast key-value operations

## 🏗️ Codebase Structure

### Key Files to Know

```
src/quantumengine/
├── document.py          # 🔥 CORE: Document classes and metaclass
├── query.py            # 🔥 CORE: QuerySet and Q objects  
├── connection.py       # 🔥 CORE: Connection management
├── fields/
│   ├── base.py         # Field base classes
│   ├── basic.py        # StringField, IntField, etc.
│   └── advanced.py     # EmailField, URLField, etc.
├── backends/
│   ├── base.py         # 🔥 CORE: BaseBackend interface
│   ├── surrealdb.py    # SurrealDB implementation
│   ├── clickhouse.py   # ClickHouse implementation
│   └── redis.py        # Redis implementation
└── exceptions.py       # Custom exceptions
```

### 🔥 Critical Files (Always Review These First)

1. **`document.py`**: Contains `Document`, `RelationDocument`, and `DocumentMetaclass`
2. **`backends/base.py`**: Defines the interface ALL backends must implement
3. **`query.py`**: Query system with `QuerySet`, `RelationQuerySet`, and Q objects
4. **`connection.py`**: Connection management and backend routing

## 🛠️ Development Patterns

### Adding New Features

#### 1. New Field Types
```python
# In src/quantumengine/fields/advanced.py
class MyCustomField(Field):
    def validate(self, value):
        # Validation logic
        return validated_value
    
    def to_db(self, value, backend=None):
        # Convert to database format
        if backend == "surrealdb":
            return surreal_format(value)
        elif backend == "clickhouse":
            return clickhouse_format(value)
        # etc.
    
    def from_db(self, value, backend=None):
        # Convert from database format
        return python_format(value)
```

#### 2. Backend Methods
ALL backend methods must be implemented in ALL backend classes:

```python
# In src/quantumengine/backends/base.py
class BaseBackend:
    async def my_new_method(self, param1, param2):
        raise NotImplementedError()

# Then implement in surrealdb.py, clickhouse.py, redis.py
class SurrealDBBackend(BaseBackend):
    async def my_new_method(self, param1, param2):
        # SurrealDB-specific implementation
        pass
```

#### 3. Document Features
Add new methods to the `Document` class:

```python
# In src/quantumengine/document.py
class Document:
    async def my_new_feature(self):
        backend = self._get_backend()
        table_name = self._get_collection_name()
        return await backend.my_new_method(table_name, self.id)
```

### Testing Requirements

#### 🔴 CRITICAL: Always test across ALL backends
```python
# In tests/
@pytest.mark.parametrize("backend", ["surrealdb", "clickhouse", "redis"])
async def test_my_feature(backend):
    # Test implementation that works with all backends
    pass
```

#### Testing with Real Databases
```bash
# Use uv to run tests in proper context
uv run python test_my_feature.py

# For real database tests
docker-compose up -d  # Start test databases
uv run python test_real_databases.py
```

## 🎯 Common Tasks for AI Agents

### Task 1: Adding a New Field Type
1. Create field class in appropriate `fields/` file
2. Implement `validate()`, `to_db()`, `from_db()` methods
3. Handle backend-specific serialization
4. Add to `fields/__init__.py` exports
5. Write comprehensive tests for all backends
6. Update documentation

### Task 2: Enhancing Backend Functionality  
1. Add method to `BaseBackend` with proper signature
2. Implement in ALL backend classes (`surrealdb.py`, `clickhouse.py`, `redis.py`)
3. Leverage each backend's unique capabilities
4. Add corresponding `Document` method if needed
5. Test with real database connections
6. Update backend capability flags if needed

### Task 3: Query System Enhancement
1. Modify `QuerySet` or add new query methods
2. Update Q objects for complex expressions  
3. Ensure backend-specific query translation works
4. Test query optimization across backends
5. Update query documentation and examples

### Task 4: Connection and Performance
1. Enhance `ConnectionRegistry` for better management
2. Add connection pooling or caching
3. Implement backend-specific optimizations
4. Add performance monitoring and metrics
5. Test under high concurrency

## 🔧 Development Environment

### Setup
```bash
# Install dependencies  
uv sync

# Run basic tests
uv run pytest tests/unit/

# Start databases for integration tests
docker-compose up -d

# Run integration tests
uv run pytest tests/integration/

# Run with real database connections
uv run python example_scripts/working/relation_example.py
```

### Database Connections for Testing
```python
# Standard test connection setup
connection = create_connection(
    url="ws://localhost:8000/rpc",
    namespace="test_ns", 
    database="test_db",
    username="root",
    password="root",
    make_default=True
)
```

## ⚠️ Critical Rules

### 1. Multi-Backend Compatibility
**ALWAYS** ensure features work across ALL supported backends:
- SurrealDB: Graph database with relations
- ClickHouse: Analytical database  
- Redis: Key-value store

### 2. Backward Compatibility
- Never break existing APIs
- Add optional parameters with defaults
- Use deprecation warnings before removing features

### 3. Error Handling
Use appropriate exceptions:
```python
from quantumengine.exceptions import (
    ValidationError,
    DoesNotExist, 
    MultipleObjectsReturned,
    ConnectionError
)
```

### 4. Type Safety
- Use type hints everywhere
- Validate inputs at boundaries
- Handle None values gracefully

### 5. Performance Considerations
- Leverage backend-specific optimizations
- Use async/await properly
- Implement connection pooling where beneficial
- Consider query optimization

## 🚀 Testing Strategy

### Unit Tests
```python
# Test individual components
def test_field_validation():
    field = EmailField()
    assert field.validate("test@example.com") == "test@example.com"
    
    with pytest.raises(ValidationError):
        field.validate("invalid-email")
```

### Integration Tests
```python
# Test backend integration
@pytest.mark.asyncio
async def test_document_save():
    doc = MyDocument(name="test")
    await doc.save()
    assert doc.id is not None
```

### Multi-Backend Tests
```python
# Test across all backends
@pytest.mark.parametrize("backend", ["surrealdb", "clickhouse", "redis"])
async def test_crud_operations(backend):
    # Test CRUD with specified backend
    pass
```

## 📚 Key Resources

### Understanding the Codebase
1. Read `PROJECT_SPEC.md` for architecture overview
2. Study `document.py` for core Document functionality  
3. Review `backends/base.py` for backend interface
4. Examine `query.py` for query system
5. Check `fields/` for field type system

### Example Code
- `example_scripts/working/` contains working examples
- `tests/` contains comprehensive test cases
- Look for `# TODO` comments for improvement opportunities

### Documentation
- Docstrings in code are the primary documentation
- Type hints provide interface contracts
- Test cases show expected behavior

## 🔍 Debugging Tips

### Common Issues
1. **Backend Method Missing**: Check if method exists in ALL backends
2. **Connection Errors**: Verify database is running and connection params are correct
3. **Serialization Issues**: Check `to_db()`/`from_db()` methods handle your data type
4. **Query Translation**: Ensure Q objects translate properly for your backend

### Debugging Tools
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Test specific backend
backend = document._get_backend()
result = await backend.select(table_name, conditions)

# Check query translation
queryset = MyDocument.objects.filter(name="test")
print(queryset.query)  # Shows backend-specific query
```

## 🎖️ Best Practices for AI Agents

1. **Start Small**: Implement simple features first to understand the patterns
2. **Test Early**: Write tests before implementing complex features
3. **Read Existing Code**: Study similar implementations in the codebase
4. **Multi-Backend Thinking**: Always consider how features work across different backends
5. **Performance Aware**: Understand each backend's strengths and optimize accordingly
6. **Documentation**: Update docstrings and examples when adding features
7. **Error Handling**: Provide clear error messages and handle edge cases
8. **Consistency**: Follow existing naming conventions and code patterns

## 🚨 Red Flags to Avoid

- ❌ Implementing features that only work with one backend
- ❌ Breaking backward compatibility without deprecation
- ❌ Adding dependencies without considering all backends
- ❌ Ignoring error handling and edge cases
- ❌ Not updating tests when changing functionality
- ❌ Assuming specific database capabilities without checking
- ❌ Hardcoding connection parameters
- ❌ Missing type hints or documentation

## 🎯 Success Metrics

When you successfully implement a feature:
- ✅ Works consistently across SurrealDB, ClickHouse, and Redis
- ✅ Has comprehensive tests covering all backends  
- ✅ Follows existing code patterns and conventions
- ✅ Includes proper error handling and validation
- ✅ Performance is optimized for each backend's capabilities
- ✅ Documentation and examples are updated
- ✅ Backward compatibility is maintained

This guide should give you everything needed to understand and contribute effectively to QuantumEngine. Focus on the multi-backend architecture and you'll be successful!