# QuantumEngine Installation Guide

QuantumEngine uses a **modular installation system** that allows you to install only the backends you need, keeping your environment lightweight and efficient.

## 📦 Installation Options

### Core Package (Minimal)
```bash
pip install quantumengine
```
**Includes:** Core ORM functionality, field types, query building  
**Size:** ~5MB  
**Use Case:** When you want to use only specific backends or aren't sure which backends you'll need

### Backend-Specific Installations

#### ClickHouse Only
```bash
pip install quantumengine[clickhouse]
```
**Includes:** Core + ClickHouse backend + clickhouse-connect  
**Use Case:** Data analytics, time-series data, high-performance aggregations

#### SurrealDB Only  
```bash
pip install quantumengine[surrealdb]
```
**Includes:** Core + SurrealDB backend + surrealdb package  
**Use Case:** Graph databases, document storage, real-time applications

#### Multiple Backends
```bash
pip install quantumengine[clickhouse,surrealdb]
```
**Includes:** Core + both backends  
**Use Case:** Multi-database applications, data pipeline systems

### Development Installation
```bash
pip install quantumengine[dev]
```
**Includes:** Core + testing tools (pytest, mypy, black, ruff)  
**Use Case:** Contributing to QuantumEngine or extensive development

### Complete Installation
```bash
pip install quantumengine[all]
```
**Includes:** Everything (all backends + development tools)  
**Use Case:** Full-featured development environment

## 🚀 Quick Start Examples

### ClickHouse-Only Application
```python
# Install: pip install quantumengine[clickhouse]

from quantumengine import Document, create_connection
from quantumengine.fields import StringField, DecimalField, DateTimeField
from quantumengine.fields.clickhouse import LowCardinalityField, ArrayField

class MarketplaceData(Document):
    product_sku = StringField(required=True)
    seller_name = LowCardinalityField(required=True)  # ClickHouse optimization
    tags = ArrayField(LowCardinalityField())          # Array(LowCardinality(String))
    offer_price = DecimalField(max_digits=10, decimal_places=2)
    date_collected = DateTimeField(required=True)
    
    class Meta:
        backend = 'clickhouse'
        collection = 'marketplace_data'

# Connect to ClickHouse
connection = create_connection(
    backend='clickhouse',
    url='localhost',
    database='analytics', 
    username='default',
    password=''
)
```

### SurrealDB-Only Application
```python
# Install: pip install quantumengine[surrealdb]

from quantumengine import Document, create_connection
from quantumengine.fields import StringField, DecimalField, DateTimeField

class UserProfile(Document):
    username = StringField(required=True)
    email = StringField(required=True)
    created_at = DateTimeField(required=True)
    
    class Meta:
        backend = 'surrealdb'
        collection = 'users'

# Connect to SurrealDB
connection = create_connection(
    backend='surrealdb',
    url='ws://localhost:8000/rpc',
    namespace='myapp',
    database='production',
    username='root',
    password='root'
)
```

### Multi-Backend Application
```python
# Install: pip install quantumengine[clickhouse,surrealdb]

from quantumengine import Document, create_connection
from quantumengine.fields import StringField, DecimalField, DateTimeField
from quantumengine.fields.clickhouse import LowCardinalityField

# Analytics data in ClickHouse
class AnalyticsEvent(Document):
    event_type = LowCardinalityField(required=True)
    user_id = StringField(required=True)
    timestamp = DateTimeField(required=True)
    
    class Meta:
        backend = 'clickhouse'
        collection = 'events'

# User data in SurrealDB
class User(Document):
    username = StringField(required=True)
    email = StringField(required=True)
    
    class Meta:
        backend = 'surrealdb'
        collection = 'users'

# Create connections for both backends
clickhouse_conn = create_connection(
    backend='clickhouse',
    url='localhost',
    database='analytics'
)

surrealdb_conn = create_connection(
    backend='surrealdb', 
    url='ws://localhost:8000/rpc',
    namespace='myapp',
    database='production'
)
```

## 🔍 Checking Available Backends

You can check which backends are available in your environment:

```python
from quantumengine.backends import BackendRegistry

# List successfully loaded backends
print("Available backends:", BackendRegistry.list_backends())

# List backends with missing dependencies  
print("Failed backends:", BackendRegistry.list_failed_backends())

# Check if specific backend is available
if BackendRegistry.is_backend_available('clickhouse'):
    print("ClickHouse backend is ready!")
else:
    print("ClickHouse backend needs installation")
```

## ❌ Troubleshooting Installation Issues

### Missing Backend Dependencies

If you see an error like:
```
ImportError: ClickHouse backend requires the 'clickhouse-connect' package. 
Install it with: pip install quantumengine[clickhouse]
```

**Solution:** Install the backend-specific package:
```bash
pip install quantumengine[clickhouse]
# or
pip install quantumengine[surrealdb]
```

### Import Errors

If you get import errors for specific backends:
```python
# This will show you exactly what's missing
from quantumengine.backends import BackendRegistry
failed = BackendRegistry.list_failed_backends()
for backend, error in failed.items():
    print(f"{backend}: {error}")
```

### Version Conflicts

If you experience dependency conflicts:
```bash
# Upgrade to latest versions
pip install --upgrade quantumengine[all]

# Or install in a clean environment
python -m venv quantumengine_env
source quantumengine_env/bin/activate  # On Windows: quantumengine_env\\Scripts\\activate
pip install quantumengine[all]
```

## 🐳 Docker Installation

For containerized deployments, you can use the modular installation in your Dockerfile:

```dockerfile
# Minimal ClickHouse-only container
FROM python:3.11-slim
RUN pip install quantumengine[clickhouse]
COPY . /app
WORKDIR /app

# Or multi-backend container  
FROM python:3.11-slim
RUN pip install quantumengine[all]
COPY . /app
WORKDIR /app
```

## 📊 Installation Size Comparison

| Installation Type | Download Size | Disk Size | Dependencies |
|-------------------|---------------|-----------|-------------|
| `quantumengine` | ~1MB | ~5MB | 1 (typing-extensions) |
| `quantumengine[clickhouse]` | ~15MB | ~45MB | +clickhouse-connect |
| `quantumengine[surrealdb]` | ~8MB | ~25MB | +surrealdb |
| `quantumengine[all]` | ~20MB | ~65MB | All backends |

## 🎯 Recommended Installation Strategies

### For Microservices
```bash
# Install only what each service needs
pip install quantumengine[clickhouse]  # Analytics service
pip install quantumengine[surrealdb]   # User management service
```

### For Data Pipelines
```bash
# Multi-backend for data movement
pip install quantumengine[clickhouse,surrealdb]
```

### For Development
```bash
# Everything for testing and development
pip install quantumengine[all,dev]
```

### For Production
```bash
# Specific backends with pinned versions
pip install quantumengine[clickhouse]==1.0.0
```

The modular installation system ensures you only install what you need while maintaining full compatibility across all backends when required.