# Working Example Scripts Directory

This directory contains example scripts that have been verified to work with the current multi-backend implementation.

## Credentials

All examples use the same credentials as the working tests:

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

## Working Example Scripts

### Multi-Backend Examples
- **`enhanced_create_connection_example.py`** - Demonstrates the enhanced `create_connection` API with backend parameter
- **`multi_backend_example.py`** - Comprehensive multi-backend workflow demonstration
- **`clickhouse_backend_example.py`** - ClickHouse-specific backend usage examples

## How to Run

All examples should be run with the `uv` context:

```bash
# Run individual examples
uv run python example_scripts/working/enhanced_create_connection_example.py
uv run python example_scripts/working/multi_backend_example.py
uv run python example_scripts/working/clickhouse_backend_example.py

# Or run from the project root
cd /path/to/quantumengine
uv run python example_scripts/working/[example_file].py
```

## Features Demonstrated

### Enhanced create_connection API
- Backend parameter for specifying database type
- Automatic connection registration and default setting
- Backend-specific parameter handling (port, secure for ClickHouse)
- Multiple connections per backend type

### Multi-Backend Workflow
- User data in SurrealDB (OLTP)
- Analytics events in ClickHouse (OLAP)
- Unified Document interface across backends
- Backend-specific optimizations

### Field Serialization
- DateTime: ISO format (SurrealDB) vs standard format (ClickHouse)
- Decimal: float (SurrealDB) vs string (ClickHouse)
- Dict/List: native objects (SurrealDB) vs JSON strings (ClickHouse)

### Backend Capabilities
- Graph relations (SurrealDB only)
- Bulk operations (both, optimized differently)
- Transactions (SurrealDB only)
- Analytical functions (ClickHouse optimized)

All examples run successfully and demonstrate the full multi-backend capabilities.