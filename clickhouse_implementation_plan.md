# ClickHouse Enhancements Implementation Plan

## Overview
Implementation plan for adding ClickHouse-specific features to QuantumORM based on the marketplace monitoring schema requirements.

## Feature Compatibility Matrix

| Feature | ClickHouse | SurrealDB | Implementation Status |
|---------|------------|-----------|----------------------|
| LowCardinalityField | ✅ Primary | ❌ N/A | ✅ Complete |
| ReplacingMergeTree | ✅ Primary | ❌ N/A | 🚧 In Progress |
| Advanced Partitioning | ✅ Primary | ❌ N/A | ✅ Complete |
| Materialized Columns | ✅ Primary | ❌ N/A | ✅ Complete |
| Bloom Filter Indexes | ✅ Primary | ❌ N/A | ✅ Complete |
| Set Indexes | ✅ Primary | ❌ N/A | ⏳ Pending |
| MinMax Indexes | ✅ Primary | ❌ N/A | ⏳ Pending |
| Array(LowCardinality) | ✅ Primary | 🔄 Both (Array support) | ⏳ Pending |
| TTL Support | ✅ Primary | ❌ N/A | ⏳ Pending |
| Compression Codecs | ✅ Primary | ❌ N/A | ⏳ Pending |
| ClickHouse Functions | ✅ Primary | ❌ N/A | ⏳ Pending |
| Materialized Views | ✅ Primary | ❌ N/A | ⏳ Pending |
| Projections | ✅ Primary | ❌ N/A | ⏳ Pending |

**Legend:**
- ✅ Primary: Main target backend for this feature
- 🔄 Both: Can be implemented for both backends  
- ❌ N/A: Not applicable/supported by backend
- ⏳ Pending: Not yet implemented
- 🚧 In Progress: Currently being worked on
- ✅ Complete: Implementation finished

## Phase 1: Critical Features (Week 1)

### 1.1 LowCardinalityField Implementation
**Priority:** CRITICAL  
**Backends:** ClickHouse Primary  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/fields/clickhouse.py` (new file)
- `src/quantumorm/fields/__init__.py`
- `src/quantumorm/backends/clickhouse.py`

**Implementation Details:**
```python
class LowCardinalityField(StringField):
    """ClickHouse LowCardinality field for enum-like string values."""
    
    def __init__(self, base_type: str = 'String', **kwargs):
        self.base_type = base_type
        super().__init__(**kwargs)
    
    def _to_db_backend_specific(self, value: Any, backend: str) -> Any:
        if backend == 'clickhouse':
            return value  # Store as-is for ClickHouse
        elif backend == 'surrealdb':
            return value  # Fallback to regular string
        return value
```

**ClickHouse Backend Changes:**
```python
def get_field_type(self, field: Any) -> str:
    if isinstance(field, LowCardinalityField):
        return f"LowCardinality({field.base_type})"
```

---

### 1.2 Enhanced Table Creation
**Priority:** CRITICAL  
**Backends:** ClickHouse Primary  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/backends/clickhouse.py`
- `src/quantumorm/document.py` (Meta class enhancements)

**Implementation Details:**
```python
class Meta:
    backend = 'clickhouse'
    table_name = 'marketplace_data'
    engine = 'ReplacingMergeTree'
    engine_params = ['date_collected']
    partition_by = 'toYYYYMM(date_collected)'
    order_by = ['seller_name', 'product_sku_model_number', 'date_collected']
    primary_key = ['seller_name', 'product_sku_model_number']
    settings = {
        'index_granularity': 8192,
        'merge_max_block_size': 8192,
        'ttl_only_drop_parts': 1
    }
```

---

### 1.3 Materialized Columns Support
**Priority:** CRITICAL  
**Backends:** ClickHouse Primary  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/fields/base.py`
- `src/quantumorm/backends/clickhouse.py`

**Implementation Details:**
```python
class Field(Generic[T]):
    def __init__(self, materialized: str = None, **kwargs):
        self.materialized = materialized
        super().__init__(**kwargs)

# Usage in documents:
price_tier = StringField(
    materialized="CASE WHEN offer_price < 50 THEN 'budget' "
                "WHEN offer_price < 200 THEN 'mid' ELSE 'premium' END"
)
```

---

### 1.4 Advanced Indexing Support
**Priority:** CRITICAL  
**Backends:** ClickHouse Primary  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/fields/base.py`
- `src/quantumorm/backends/clickhouse.py`

**Implementation Details:**
```python
class Field(Generic[T]):
    def __init__(self, indexes: List[Dict] = None, **kwargs):
        self.indexes = indexes or []
        super().__init__(**kwargs)

# Usage:
product_sku = StringField(
    indexes=[
        {'type': 'bloom_filter', 'granularity': 3, 'false_positive_rate': 0.01}
    ]
)
marketplace = LowCardinalityField(
    indexes=[
        {'type': 'set', 'granularity': 1, 'max_values': 100}
    ]
)
```

## Phase 2: Important Features (Week 2)

### 2.1 Enhanced ArrayField
**Priority:** HIGH  
**Backends:** 🔄 Both (ClickHouse Primary, SurrealDB Compatible)  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/fields/collection.py`
- `src/quantumorm/backends/clickhouse.py`
- `src/quantumorm/backends/surrealdb.py`

**Implementation Details:**
```python
class ArrayField(Field[List]):
    def __init__(self, element_type: Field, **kwargs):
        self.element_type = element_type
        super().__init__(**kwargs)
    
    def _to_db_backend_specific(self, value: Any, backend: str) -> Any:
        if backend == 'clickhouse':
            # ClickHouse native array support
            return [self.element_type.to_db(item, backend) for item in value]
        elif backend == 'surrealdb':
            # SurrealDB array support
            return [self.element_type.to_db(item, backend) for item in value]
        return value
```

---

### 2.2 TTL Support
**Priority:** HIGH  
**Backends:** ClickHouse Primary  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/document.py`
- `src/quantumorm/backends/clickhouse.py`

**Implementation Details:**
```python
class Meta:
    ttl = 'date_collected + INTERVAL 2 YEAR'
    ttl_delete = True  # DELETE vs TO DISK/VOLUME
```

---

### 2.3 ClickHouse Query Functions
**Priority:** HIGH  
**Backends:** ClickHouse Primary  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/query/clickhouse.py` (new file)
- `src/quantumorm/backends/clickhouse.py`

**Implementation Details:**
```python
class ClickHouseQuerySet(QuerySet):
    def has_array_element(self, field: str, value: Any):
        """Use ClickHouse has() function for array queries."""
        condition = f"has({field}, {self.backend.format_value(value)})"
        clone = self._clone()
        clone.query_parts.append(('__raw__', '=', condition))
        return clone
    
    def to_date(self, field: str, alias: str = None):
        """Use ClickHouse toDate() function."""
        if alias:
            return self.annotate(**{alias: f"toDate({field})"})
        return self.annotate(date=f"toDate({field})")
```

## Phase 3: Optimization Features (Week 3)

### 3.1 Materialized Views
**Priority:** MEDIUM  
**Backends:** ClickHouse Primary  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/materialized_view.py` (enhance existing)
- `src/quantumorm/backends/clickhouse.py`

**Implementation Details:**
```python
class DailyPricesBySeller(MaterializedView):
    class Meta:
        base_table = MarketplaceData
        engine = 'SummingMergeTree'
        partition_by = 'toYYYYMM(date)'
        order_by = ['seller_name', 'product_sku_model_number', 'date']
        
    query = """
    SELECT
        seller_name,
        product_sku_model_number,
        toDate(date_collected) as date,
        avg(offer_price) as avg_price
    FROM marketplace_data
    GROUP BY seller_name, product_sku_model_number, date
    """
```

---

### 3.2 Compression Codecs
**Priority:** LOW  
**Backends:** ClickHouse Primary  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/fields/scalar.py`
- `src/quantumorm/backends/clickhouse.py`

**Implementation Details:**
```python
class StringField(Field[str]):
    def __init__(self, codec: str = None, **kwargs):
        self.codec = codec
        super().__init__(**kwargs)

# Usage:
ad_page_url = StringField(codec="ZSTD(3)")
```

---

### 3.3 Projections Support
**Priority:** LOW  
**Backends:** ClickHouse Primary  
**Status:** ⏳ Pending

**Files to Modify:**
- `src/quantumorm/document.py`
- `src/quantumorm/backends/clickhouse.py`

**Implementation Details:**
```python
class Meta:
    projections = [
        {
            'name': 'price_history_projection',
            'select': ['product_sku_model_number', 'seller_name', 'date_collected', 'offer_price'],
            'order_by': ['product_sku_model_number', 'seller_name', 'date_collected']
        }
    ]
```

## Implementation Timeline

| Week | Features | Status |
|------|----------|--------|
| Week 1 | LowCardinalityField, Enhanced Table Creation, Materialized Columns, Advanced Indexing | ⏳ Pending |
| Week 2 | Enhanced ArrayField, TTL Support, ClickHouse Query Functions | ⏳ Pending |
| Week 3 | Materialized Views, Compression Codecs, Projections | ⏳ Pending |

## Testing Strategy

### Unit Tests Required:
- [ ] LowCardinalityField serialization/deserialization
- [ ] Table creation with ReplacingMergeTree
- [ ] Materialized column generation
- [ ] Index creation (bloom_filter, set, minmax)
- [ ] Array field with LowCardinality elements
- [ ] TTL policy application
- [ ] Query function integration

### Integration Tests Required:
- [ ] Full marketplace schema creation
- [ ] CRUD operations with new field types
- [ ] Query performance with indexes
- [ ] Materialized view functionality

## Success Criteria

1. ✅ **Complete Schema Support** - Full marketplace_data table creation
2. ✅ **Query Compatibility** - All sample queries from schema work
3. ✅ **Performance** - Proper index utilization
4. ✅ **Data Lifecycle** - TTL policies functional
5. ✅ **Backward Compatibility** - Existing QuantumORM code unaffected

## Notes

- All ClickHouse-specific features will gracefully degrade for other backends
- SurrealDB compatibility maintained where possible
- Documentation will be updated for each feature
- Performance benchmarks will be conducted for critical features

---

**Last Updated:** [Date]  
**Next Review:** [Date]  
**Implementation Lead:** [Name]