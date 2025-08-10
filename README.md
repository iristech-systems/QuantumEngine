<div align="center">
  
# ⚛️ QuantumEngine ⚡
  
  <p>
    <strong>A powerful, multi-backend Object-Document Mapper (ODM) for Python</strong>
  </p>
  
  <p>
    Unified API for both transactional and analytical databases<br>
    Supporting SurrealDB (graph/document) and ClickHouse (columnar analytical) with a single, consistent interface
  </p>
  
  <p>
    <a href="https://iristech-systems.github.io/QuantumEngine-Docs/"><img src="https://img.shields.io/badge/docs-latest-brightgreen.svg" alt="Documentation"></a>
    <a href="https://pypi.org/project/quantumengine/"><img src="https://img.shields.io/pypi/v/quantumengine.svg" alt="PyPI version"></a>
    <a href="https://pypi.org/project/quantumengine/"><img src="https://img.shields.io/pypi/pyversions/quantumengine.svg" alt="Python versions"></a>
    <a href="https://github.com/iristech-systems/QuantumEngine/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
    <a href="https://github.com/iristech-systems/QuantumEngine/actions"><img src="https://github.com/iristech-systems/QuantumEngine/workflows/Tests/badge.svg" alt="Tests"></a>
    <a href="https://pypi.org/project/quantumengine/"><img src="https://img.shields.io/pypi/dm/quantumengine.svg" alt="Downloads"></a>
  </p>
</div>

---

## 📦 Installation

QuantumEngine uses a **modular installation system** - install only the backends you need:

```bash
# Core package only (lightweight)
pip install quantumengine

# With ClickHouse support
pip install quantumengine[clickhouse]

# With SurrealDB support  
pip install quantumengine[surrealdb]

# With Redis support
pip install quantumengine[redis]

# With multiple backends
pip install quantumengine[clickhouse,surrealdb,redis]

# Everything (all backends + dev tools)
pip install quantumengine[all]
```

See [INSTALLATION.md](INSTALLATION.md) for detailed installation options and troubleshooting.

## 🆕 What's New in v0.4.0

- **🔗 Connection Pooling**: Advanced connection pooling across all backends (SurrealDB, ClickHouse, Redis) with configurable pool sizes, health checks, and idle timeouts
- **🎯 Standardized Connection API**: Consistent `create_connection()` interface across all backends
- **🔄 Sync API Support**: Full synchronous API with `async_mode=False` and sync method variants
- **🛡️ Enhanced Parameter Validation**: Clear error messages with suggested fixes for connection parameters  
- **⚡ Redis Backend**: Complete Redis/Dragonfly support for caching and real-time data
- **📊 ClickHouse Connection Pooling**: Full connection pooling implementation for ClickHouse backend with automatic resource management
- **📚 Comprehensive Documentation**: Complete API guide and migration documentation
- **🔧 Backward Compatibility**: Seamless migration from legacy connection APIs

## 🚀 Quick Start

```python
import os
from quantumengine import Document, StringField, IntField, create_connection

# Define a document model
class User(Document):
    username = StringField(required=True)
    email = StringField(required=True)
    age = IntField(min_value=0)
    
    class Meta:
        collection = "users"
        backend = "surrealdb"  # or "clickhouse", "redis"

# Create connection with standardized API
connection = create_connection(
    backend="surrealdb",  # Explicitly specify backend
    url="ws://localhost:8000/rpc",
    namespace="test_ns",
    database="test_db",
    username=os.environ.get("SURREALDB_USER", "root"),
    password=os.environ.get("SURREALDB_PASS", "root"),
    make_default=True
)

# Use async context manager
async with connection:
    # Create table
    await User.create_table()

    # CRUD operations
    user = User(username="alice", email="alice@example.com", age=25)
    await user.save()

    users = await User.objects.filter(age__gt=18).all()
    await User.objects.filter(username="alice").update(age=26)
```

## 🏗️ Architecture

QuantumEngine provides a unified interface for multiple database backends:

- **SurrealDB Backend**: Graph database with native support for relations, transactions, and complex queries
- **ClickHouse Backend**: High-performance columnar database optimized for analytics and time-series data  
- **Redis Backend**: In-memory database for caching, sessions, and real-time data storage

### Multi-Backend Support with Connection Pooling

```python
from quantumengine.connection import PoolConfig

# Configure connection pooling
pool_config = PoolConfig(
    max_size=20,
    min_size=5,
    max_idle_time=300  # seconds
)

# SurrealDB connection for transactional data
user_connection = create_connection(
    backend="surrealdb",
    url="ws://localhost:8000/rpc",
    username="root",
    password="root",
    namespace="production",
    database="users",
    pool_config=pool_config,
    make_default=True
)

# ClickHouse connection for analytics
analytics_connection = create_connection(
    backend="clickhouse",
    url="localhost",  # Standardized parameter
    username="default",
    password="",
    port=8123,  # Auto-set if not provided
    secure=False,  # Auto-set if not provided
    pool_config=pool_config
)

# Redis connection for caching
cache_connection = create_connection(
    backend="redis",
    url="localhost",
    port=6379,  # Auto-set if not provided
    password="cache_password"
)

class User(Document):
    class Meta:
        backend = "surrealdb"

class AnalyticsEvent(Document):
    class Meta:
        backend = "clickhouse"

class SessionCache(Document):
    class Meta:
        backend = "redis"
```

## 📋 Features

### ✅ Core Features
- **🔗 Multi-Backend Architecture**: SurrealDB + ClickHouse + Redis support
- **⚡ Connection Pooling**: Production-ready connection pooling for all backends with configurable pool sizes, health checks, and automatic resource management
- **🎯 Standardized Connection API**: Consistent interface across all backends with parameter validation
- **🔄 Async/Sync APIs**: Complete async/await support with sync alternatives (`create_connection(..., async_mode=False)`)
- **🔥 Intelligent Update System**: Safe partial updates preventing data loss
- **🛡️ Type-Safe Field System**: 15+ field types with validation and backend-specific optimization
- **🔍 Advanced Query System**: Q objects, QueryExpressions, and Pythonic query syntax (`User.age > 30`)
- **📊 Relationship Management**: Graph relations and references with relation documents
- **🏗️ Schema Management**: Both SCHEMAFULL and SCHEMALESS table support with migrations
- **⚙️ Connection Management**: Named connections, default connections, and connection registry
- **🚀 Performance Optimization**: Direct record access, bulk operations, and connection reuse
- **🔧 Migration Tools**: Schema migration, table dropping, and hybrid schema support

### 🔧 Field Types

| Field Type | Description | SurrealDB | ClickHouse | Redis |
|------------|-------------|-----------|------------|-------|
| `StringField` | Text fields with validation | ✅ | ✅ | ✅ |
| `IntField` | Integer with min/max constraints | ✅ | ✅ | ✅ |
| `FloatField` | Floating point numbers | ✅ | ✅ | ✅ |
| `BooleanField` | Boolean values | ✅ | ✅ | ✅ |
| `DateTimeField` | Date and time with timezone | ✅ | ✅ | ✅ |
| `DecimalField` | High-precision decimals | ✅ | ✅ | ✅ |
| `UUIDField` | UUID generation and validation | ✅ | ✅ | ✅ |
| `ListField` | Arrays/lists with typed elements | ✅ | ✅ | ✅ |
| `DictField` | JSON/dictionary storage | ✅ | ✅ |
| `ReferenceField` | References to other documents | ✅ | ❌ |
| `IPAddressField` | IPv4/IPv6 address validation | ✅ | ✅ |
| `DurationField` | Time periods and durations | ✅ | ❌ |
| `RangeField` | Range values with bounds | ✅ | ❌ |
| `OptionField` | Optional field wrapper | ✅ | ❌ |
| `RecordIDField` | SurrealDB record identifiers | ✅ | ❌ |

### 🔍 Query Capabilities

#### Basic Filtering
```python
# Simple filters
users = await User.objects.filter(age__gt=18).all()
users = await User.objects.filter(username__contains="admin").all()
users = await User.objects.filter(active=True).all()

# Count
count = await User.objects.filter(age__gte=21).count()
```

#### Q Objects for Complex Queries
```python
from quantumengine import Q

# Combine conditions
complex_query = Q(age__gte=18) & Q(age__lte=65) & Q(active=True)
users = await User.objects.filter(complex_query).all()

# OR conditions
either_query = Q(role="admin") | Q(permissions__contains="admin")
users = await User.objects.filter(either_query).all()

# NOT conditions
not_query = ~Q(status="banned")
users = await User.objects.filter(not_query).all()

# Raw queries
raw_query = Q.raw("age > 25 AND string::contains(username, 'admin')")
users = await User.objects.filter(raw_query).all()
```

#### QueryExpressions with FETCH
```python
# Fetch related documents (SurrealDB)
expr = QueryExpression(where=Q(published=True)).fetch("author")
posts = await Post.objects.filter(expr).all()

# Complex expressions
complex_expr = (QueryExpression(where=Q(active=True))
               .order_by("created_at", "DESC")
               .limit(10)
               .fetch("profile"))
users = await User.objects.filter(complex_expr).all()
```

### 🔌 Connection Management

#### Standardized Connection API (v0.4.0+)

All backends now use a consistent connection interface:

```python
from quantumengine import create_connection
from quantumengine.connection import PoolConfig

# SurrealDB - Graph database
surrealdb_conn = create_connection(
    backend="surrealdb",
    url="ws://localhost:8000/rpc",
    username="root",
    password="root",
    namespace="production",
    database="main",
    make_default=True
)

# ClickHouse - Analytics database  
clickhouse_conn = create_connection(
    backend="clickhouse",
    url="localhost",
    username="default", 
    password="",
    # port=8123, secure=False  # Auto-set defaults
    make_default=True
)

# Redis - Cache/Session store
redis_conn = create_connection(
    backend="redis", 
    url="localhost",
    # port=6379  # Auto-set default
    password="redis_password",
    make_default=True
)
```

#### Connection Pooling

Connection pooling is available across all backends for optimal performance:

```python
# Configure connection pool
pool_config = PoolConfig(
    max_size=50,        # Maximum connections
    min_size=10,        # Minimum connections  
    max_idle_time=300,  # Idle timeout (seconds)
    validate_on_borrow=True  # Health check on borrow
)

# SurrealDB with connection pooling
surrealdb_conn = create_connection(
    backend="surrealdb",
    url="ws://localhost:8000/rpc",
    pool_config=pool_config,
    make_default=True
)

# ClickHouse with connection pooling
clickhouse_conn = create_connection(
    backend="clickhouse", 
    url="localhost",
    pool_config=pool_config,
    make_default=True
)

# Redis with connection pooling
redis_conn = create_connection(
    backend="redis",
    url="localhost",
    pool_config=pool_config,
    make_default=True
)
```

#### Sync API Support

```python
# Async API (default)
async_conn = create_connection(
    backend="surrealdb",
    url="ws://localhost:8000/rpc",
    auto_connect=True,
    async_mode=True  # Default
)

# Sync API  
sync_conn = create_connection(
    backend="surrealdb", 
    url="ws://localhost:8000/rpc",
    auto_connect=True,
    async_mode=False
)

# Use sync connection
with sync_conn:
    # Sync operations
    User.create_table_sync()
    user = User(name="Alice")
    user.save_sync()
    
    # Sync queries
    users = User.objects.all_sync()
    active_users = User.objects.filter_sync(active=True).all_sync()
```

### 🔗 Relationships (SurrealDB)

#### Document References
```python
class Post(Document):
    title = StringField(required=True)
    author = ReferenceField(User, required=True)
    categories = ListField(field_type=ReferenceField(Category))
    
    class Meta:
        collection = "posts"
        backend = "surrealdb"
```

#### Graph Relations
```python
# Create relations between documents
await author1.relate_to("collaborated_with", author2, project="Novel")

# Fetch relations
collaborators = await author1.fetch_relation("collaborated_with")

# Resolve relations (get related documents)
related_authors = await author1.resolve_relation("collaborated_with")
```

#### Relation Documents
```python
class AuthorCollaboration(RelationDocument):
    project_name = StringField(required=True)
    start_date = DateTimeField()
    contribution_percent = FloatField()
    
    class Meta:
        collection = "collaborated_with"

# Create relation with metadata
relation = await AuthorCollaboration.create_relation(
    author1, author2,
    project_name="Science Fiction Novel",
    start_date=datetime.now(),
    contribution_percent=60.0
)
```

### 📊 Schema Management

#### Table Creation
```python
# SCHEMAFULL tables (strict schema)
await User.create_table(schemafull=True)

# SCHEMALESS tables (flexible schema)  
await User.create_table(schemafull=False)

# Backend-specific table creation
await analytics_backend.create_table(
    AnalyticsEvent,
    engine="MergeTree",
    order_by="(event_time, user_id)"
)
```

#### Drop Tables & Migration Support
```python
from quantumengine import (
    generate_drop_statements, generate_migration_statements,
    drop_tables_from_module
)

# Drop table functionality
await User.drop_table(if_exists=True)
User.drop_table_sync(if_exists=True)

# Generate drop statements for migration scripts
drop_statements = generate_drop_statements(User)
# ['REMOVE INDEX IF EXISTS idx_email ON users;', 
#  'REMOVE FIELD IF EXISTS email ON users;', ...]

# Generate migration between document versions
migration = generate_migration_statements(UserV1, UserV2, schemafull=True)
print(migration['up'])    # Forward migration statements
print(migration['down'])  # Rollback migration statements

# Drop all tables in a module
await drop_tables_from_module('myapp.models')
```

#### Hybrid Schema Support
```python
class Product(Document):
    # Always defined in schema
    name = StringField(required=True, define_schema=True)
    price = FloatField(define_schema=True)
    
    # Only defined in SCHEMAFULL tables
    description = StringField()
    metadata = DictField()
    
    class Meta:
        collection = "products"
```

#### Index Management
```python
class User(Document):
    username = StringField(required=True)
    email = StringField(required=True)
    
    class Meta:
        collection = "users"
        indexes = [
            {"name": "user_username_idx", "fields": ["username"], "unique": True},
            {"name": "user_email_idx", "fields": ["email"], "unique": True},
            {"name": "user_age_idx", "fields": ["age"]}
        ]

# Create indexes
await User.create_indexes()
```

### ⚡ Performance Features

#### Direct Record Access
```python
# Optimized ID-based queries use direct record access
users = await User.objects.filter(id__in=['user:1', 'user:2']).all()

# Convenience methods for ID operations
users = await User.objects.get_many([1, 2, 3]).all()
users = await User.objects.get_range(100, 200, inclusive=True).all()
```

#### Query Analysis
```python
# Explain query execution plan
plan = await User.objects.filter(age__gt=25).explain()

# Get index suggestions
suggestions = User.objects.filter(age__lt=30).suggest_indexes()
```

#### Bulk Operations
```python
# Bulk updates
updated = await User.objects.filter(active=False).update(status="inactive")

# Bulk deletes
deleted_count = await User.objects.filter(last_login__lt=cutoff_date).delete()
```

### 🔄 Sync API Support

```python
# Create sync connection
connection = create_connection(
    url="ws://localhost:8000/rpc",
    namespace="test_ns",
    database="test_db", 
    username=os.environ.get("SURREALDB_USER"),
    password=os.environ.get("SURREALDB_PASS"),
    async_mode=False
)

with connection:
    # Synchronous operations
    User.create_table_sync(schemafull=True)
    
    user = User(username="alice", email="alice@example.com")
    user.save_sync()
    
    users = User.objects.all_sync()
    user = User.objects.get_sync(id=user_id)
    user.delete_sync()
```

### 📈 DataGrid Helpers

```python
from quantumengine import get_grid_data, parse_datatables_params

# Efficient grid operations for web interfaces
result = await get_grid_data(
    User,                      # Document class
    request_args,              # Request parameters
    search_fields=['username', 'email'],
    custom_filters={'active': 'active'},
    default_sort='created_at'
)

# DataTables integration
params = parse_datatables_params(request_args)
result = format_datatables_response(total, rows, draw)
```

## 📦 Installation

```bash
pip install quantumengine

# For SurrealDB support
pip install surrealdb

# For ClickHouse support  
pip install clickhouse-connect
```

## 🔧 Configuration

### Environment Variables
```python
import os
from quantumengine import create_connection

# Using environment variables
connection = create_connection(
    url=os.environ.get("SURREALDB_URL", "ws://localhost:8000/rpc"),
    namespace=os.environ.get("SURREALDB_NS", "production"),
    database=os.environ.get("SURREALDB_DB", "main"),
    username=os.environ.get("SURREALDB_USER"),
    password=os.environ.get("SURREALDB_PASS"),
    make_default=True
)
```

### Multiple Named Connections
```python
# Main transactional database
main_db = create_connection(
    name="main_db",
    url="ws://localhost:8000/rpc",
    backend="surrealdb",
    make_default=True
)

# Analytics database
analytics_db = create_connection(
    name="analytics_db",
    url="https://analytics.clickhouse.cloud",
    backend="clickhouse"
)

# Use specific connection
await User.create_table(connection=main_db)
await AnalyticsEvent.create_table(connection=analytics_db)
```

## 🏃‍♂️ Quick Examples

### Basic CRUD
```python
# Create
user = User(username="alice", email="alice@example.com", age=25)
await user.save()

# Read
user = await User.objects.get(username="alice")
users = await User.objects.filter(age__gte=18).all()

# Update
user.age = 26
await user.save()

# Delete
await user.delete()
```

### Advanced Queries
```python
from quantumengine import Q, QueryExpression

# Complex filtering
active_adults = await User.objects.filter(
    Q(age__gte=18) & Q(active=True)
).all()

# Fetch relations
posts_with_authors = await Post.objects.filter(
    QueryExpression(where=Q(published=True)).fetch("author")
).all()

# Pagination and sorting
recent_users = await User.objects.filter(
    active=True
).order_by("-created_at").limit(10).all()
```

### Multi-Backend Usage
```python
# User data in SurrealDB (transactional)
class User(Document):
    username = StringField(required=True)
    email = StringField(required=True)
    
    class Meta:
        collection = "users"
        backend = "surrealdb"

# Analytics events in ClickHouse (analytical)
class PageView(Document):
    user_id = StringField(required=True)
    page_url = StringField(required=True)
    timestamp = DateTimeField(required=True)
    
    class Meta:
        collection = "page_views"
        backend = "clickhouse"

# Use both seamlessly
user = await User.objects.get(username="alice")
page_views = await PageView.objects.filter(user_id=str(user.id)).all()
```

### 🔥 NEW in v0.3.0: Intelligent Update System

QuantumEngine now features a comprehensive intelligent update system that prevents data loss and provides safe partial document updates:

```python
# Safe partial updates - only modify specified fields
user = await User.objects.get(username="alice")
await user.update(age=26, status="premium")  # Only updates age and status

# Intelligent save() with change tracking
user = await User.objects.get(username="alice")
user.age = 27
user.email = "alice@newdomain.com"
await user.save()  # Only updates changed fields (age and email)

# Relation updates preserve endpoints
class Friendship(RelationDocument):
    status = StringField(choices=["pending", "accepted", "blocked"])
    since = DateTimeField()
    
    class Meta:
        collection = "friendships"

friendship = await Friendship.objects.get(id="friendship123")
await friendship.update_relation_attributes(status="blocked")
# Preserves in_document and out_document, only updates status

# Multi-backend partial updates
await ClickHouseDoc.update(metrics_count=1500)  # ClickHouse ALTER TABLE UPDATE
await RedisDoc.update(session_data={"active": True})  # Redis hash updates
```

**Key Benefits:**
- **Data Loss Prevention**: Partial updates preserve unchanged fields
- **Change Tracking**: Intelligent save() only updates modified fields  
- **Multi-Backend Support**: Works consistently across SurrealDB, ClickHouse, and Redis
- **Relation Safety**: RelationDocument updates preserve relationship endpoints
- **Backend Optimization**: Uses optimal update syntax for each database type

### Schema Management Examples
```python
# Generate schema statements
from quantumengine import generate_schema_statements, generate_drop_statements

# Create schema
schema_statements = generate_schema_statements(User, schemafull=True)
for stmt in schema_statements:
    print(stmt)

# Drop schema
drop_statements = generate_drop_statements(User)
for stmt in drop_statements:
    print(stmt)

# Generate migration between versions
from quantumengine import generate_migration_statements

class UserV1(Document):
    username = StringField(required=True)
    email = StringField(required=True)

class UserV2(Document):
    username = StringField(required=True)
    email = StringField(required=True)
    active = BooleanField(default=True)  # New field

migration = generate_migration_statements(UserV1, UserV2)
print("UP migration:")
for stmt in migration['up']:
    print(f"  {stmt}")

print("DOWN migration:")
for stmt in migration['down']:
    print(f"  {stmt}")
```

## 🧪 Testing

The codebase includes comprehensive tests demonstrating real database operations:

```bash
# Run working tests
python tests/working/test_multi_backend_real_connections.py
python tests/working/test_clickhouse_simple_working.py
python tests/working/test_working_surrealdb_backend.py

# Run working examples
python example_scripts/working/basic_crud_example.py
python example_scripts/working/multi_backend_example.py
python example_scripts/working/advanced_features_example.py
```

## 📚 Examples

The `example_scripts/working/` directory contains fully functional examples:

- **basic_crud_example.py**: Core CRUD operations
- **advanced_features_example.py**: Complex field types and validation
- **multi_backend_example.py**: Using SurrealDB and ClickHouse together
- **relation_example.py**: Graph relations and RelationDocuments
- **query_expressions_example.py**: Advanced querying with Q objects
- **sync_api_example.py**: Synchronous API usage
- **test_performance_optimizations.py**: Performance features and optimization
- **test_drop_and_migration.py**: Drop table and migration functionality

## 🔄 Backend Capabilities

### SurrealDB Backend Features
- ✅ Graph relations and traversal
- ✅ Transactions 
- ✅ Direct record access
- ✅ Full-text search
- ✅ References between documents
- ✅ Complex data types (Duration, Range, Option)
- ✅ SCHEMAFULL and SCHEMALESS tables

### ClickHouse Backend Features  
- ✅ High-performance analytical queries
- ✅ Bulk operations optimization
- ✅ Time-series data handling
- ✅ Columnar storage benefits
- ✅ Aggregation functions
- ❌ Graph relations (not applicable)
- ❌ Transactions (limited support)

### Backend Detection
```python
# Check backend capabilities
if connection.backend.supports_graph_relations():
    await user.relate_to("follows", other_user)

if connection.backend.supports_bulk_operations():
    await Document.objects.filter(...).bulk_update(status="processed")

# Get backend-specific optimizations
optimizations = connection.backend.get_optimized_methods()
print(optimizations)
```

## ⚡ Performance Features

### Automatic Query Optimizations
- **Direct Record Access**: ID-based queries use `SELECT * FROM user:1, user:2`
- **Range Access**: Range queries use `SELECT * FROM user:1..=100`
- **Bulk Operations**: Optimized batch processing
- **Index Utilization**: Automatic index suggestions

### Measured Performance Improvements
- **Direct Record Access**: Up to 3.4x faster than traditional WHERE clauses
- **Bulk Operations**: Significant improvement for batch processing
- **Memory Efficiency**: Reduced data transfer and memory usage

## 📚 Documentation

### Online Documentation

- **GitHub Pages**: [Full Sphinx Documentation](https://iristech-systems.github.io/QuantumEngine-Docs/) - Complete API reference with examples
- **API Reference**: Detailed class and method documentation
- **Quick Start Guide**: Step-by-step getting started tutorial
- **Module Documentation**: Auto-generated from source code docstrings

### Local Documentation

- **API Reference**: `API_REFERENCE.md` - Complete class and method documentation  
- **Examples**: `example_scripts/working/` - Working examples for all features
- **Tests**: `tests/working/` - Test files demonstrating functionality
- **Sphinx Docs**: `docs/` - Build locally with `cd docs && uv run make html`

### Building Documentation

```bash
# Install dependencies
uv sync

# Build Sphinx documentation
cd docs
uv run make html

# View locally
open docs/_build/html/index.html
```

## 🤝 Contributing

QuantumORM is actively developed with a focus on real-world usage and multi-backend support. See the `tests/working/` directory for examples of tested functionality.

For detailed contribution guidelines, see:
- **CONTRIBUTING.md**: Development setup, Docker instructions, and contribution workflow
- **docs/README.md**: Documentation contribution guidelines

## 🙏 Acknowledgments

QuantumEngine draws significant inspiration from [MongoEngine](https://github.com/MongoEngine/mongoengine), whose elegant document-oriented design patterns and query API have influenced our multi-backend approach. We're grateful to the MongoEngine community for pioneering many of the concepts that make QuantumEngine possible.

## 📄 License

MIT License - see LICENSE file for details.

---

**QuantumEngine**: Unified database access for modern Python applications with comprehensive multi-backend support, schema management, and performance optimizations.