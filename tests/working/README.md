# Working Tests Directory

This directory contains test files that have been verified to work with the current multi-backend implementation using real database connections.

## Credentials

All tests use the same credentials as defined in the environment variables or defaults:

### SurrealDB (Devcontainer)
- URL: `ws://localhost:8000/rpc`
- Username: `root`
- Password: `root`
- Namespace: `test_ns`
- Database: `test_db`

### ClickHouse (Cloud)
- Host: `localhost` (or your ClickHouse server)
- Port: `443` (secure)
- Username: `cis-6c16631`
- Password: Use environment variable `CLICKHOUSE_PASS`
- Database: `analytics`

## Working Test Files

### Core Multi-Backend Tests
- **`test_multi_backend_real_connections.py`** - The main comprehensive multi-backend test
- **`test_multi_backend_features_working.py`** - Enhanced multi-backend features test with fixed field serialization
- **`test_working_surrealdb_backend.py`** - SurrealDB backend architecture test

### Backend-Specific Tests
- **`test_clickhouse_simple_working.py`** - Direct ClickHouse backend operations test

### Field and Component Tests
- **`test_fields_import.py`** - Test field import functionality
- **`test_ipaddressfield.py`** - Test IP address field validation and functionality

## How to Run

All tests should be run with the `uv` context:

```bash
# Run individual tests
uv run python tests/working/test_multi_backend_real_connections.py
uv run python tests/working/test_multi_backend_features_working.py
uv run python tests/working/test_clickhouse_simple_working.py
uv run python tests/working/test_working_surrealdb_backend.py

# Or run from the project root
cd /path/to/quantumengine
uv run python tests/working/[test_file].py
```

## Features Tested

### ✅ Working Features
- Enhanced `create_connection` with `backend` parameter
- Multi-backend connection registration and management
- Backend-specific field serialization (DateTime, Decimal, List, Dict)
- Backend capability detection and conditional features
- Graph relations (SurrealDB) vs traditional SQL (ClickHouse)
- Real database CRUD operations on both backends
- Query building with backend-specific syntax
- Bulk operations and performance optimizations
- Connection cleanup and data management

### 🔧 Fixed Issues
- DictField and ListField serialization for ClickHouse (JSON strings)
- Backend-aware field serialization methods
- Connection registration in unified backend system
- SurrealDB vs ClickHouse query condition building
- Proper handling of nullable columns in ClickHouse ORDER BY

## Architecture Verified

The tests confirm that the multi-backend architecture is fully functional:

1. **Backend Abstraction** - Clean separation between SurrealDB and ClickHouse
2. **Unified API** - Same Document/QuerySet interface works with both backends
3. **Backend-Specific Optimizations** - Each backend can use its strengths
4. **Field Serialization** - Automatic conversion based on backend requirements
5. **Connection Management** - Enhanced `create_connection` supports both backends
6. **Conditional Features** - Graph relations, transactions, etc. based on backend support

All tests pass successfully with real database connections.