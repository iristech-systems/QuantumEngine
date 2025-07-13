#!/usr/bin/env python3

"""Working version of simple ClickHouse test using cloud credentials.

This test validates the ClickHouse backend using direct backend operations
with the same credentials as test_multi_backend_real_connections.py.
"""

import asyncio
import os
import sys
import time
from datetime import datetime, timezone, timedelta

# Add src to path for local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import clickhouse_connect
from quantumengine.backends.clickhouse import ClickHouseBackend


async def test_clickhouse_backend():
    """Test ClickHouse backend operations directly."""
    print("🧪 Testing ClickHouse Backend with Cloud Credentials")
    print("="*60)
    
    # Connect to ClickHouse cloud using same credentials as main test
    print("🔧 Connecting to ClickHouse cloud...")
    
    clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
    clickhouse_user = os.environ.get('CLICKHOUSE_USER', 'cis-6c16631')
    clickhouse_pass = os.environ.get('CLICKHOUSE_PASS')
    clickhouse_db = os.environ.get('CLICKHOUSE_DB', 'analytics')
    
    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=8123,
            username=clickhouse_user,
            password=clickhouse_pass,
            database=clickhouse_db,
            secure=False
        )
        
        # Test connection
        result = client.query("SELECT 1 as test")
        print(f"✅ Connected to ClickHouse: {result.result_rows}")
        
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        return False
    
    # Create backend instance
    backend = ClickHouseBackend(client)
    
    # Test table creation
    print("\n📋 Testing table creation...")
    table_name = 'test_simple_clickhouse'
    
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        name String,
        value Float64,
        timestamp DateTime64(3),
        data String
    ) ENGINE = MergeTree()
    ORDER BY (timestamp, id)
    """
    
    try:
        await backend._execute(create_query)
        print(f"✅ Created table: {table_name}")
    except Exception as e:
        print(f"❌ Failed to create table: {e}")
        return False
    
    # Test insert operations
    print("\n📝 Testing insert operations...")
    
    # Single insert
    test_data = {
        'id': 'test_001',
        'name': 'Test Record',
        'value': 123.45,
        'timestamp': datetime.now(timezone.utc),
        'data': '{"test": true}'
    }
    
    try:
        result = await backend.insert(table_name, test_data)
        print(f"✅ Single insert successful: {result['id']}")
    except Exception as e:
        print(f"❌ Single insert failed: {e}")
        return False
    
    # Bulk insert
    bulk_data = []
    for i in range(5):
        bulk_data.append({
            'id': f'bulk_test_{i}',
            'name': f'Bulk Record {i}',
            'value': 100.0 + i * 10,
            'timestamp': datetime.now(timezone.utc) + timedelta(seconds=i),
            'data': f'{{"bulk": {i}}}'
        })
    
    try:
        results = await backend.insert_many(table_name, bulk_data)
        print(f"✅ Bulk insert successful: {len(results)} records")
    except Exception as e:
        print(f"❌ Bulk insert failed: {e}")
        return False
    
    # Test query operations
    print("\n🔍 Testing query operations...")
    
    # Select all
    try:
        all_records = await backend.select(table_name, [])
        print(f"✅ Selected all records: {len(all_records)} found")
    except Exception as e:
        print(f"❌ Select all failed: {e}")
        return False
    
    # Select with conditions
    try:
        conditions = [backend.build_condition('name', '=', 'Test Record')]
        filtered_records = await backend.select(table_name, conditions)
        print(f"✅ Filtered select: {len(filtered_records)} records found")
    except Exception as e:
        print(f"❌ Filtered select failed: {e}")
        return False
    
    # Count records
    try:
        count = await backend.count(table_name, [])
        print(f"✅ Count operation: {count} total records")
    except Exception as e:
        print(f"❌ Count failed: {e}")
        return False
    
    # Test condition building
    print("\n🔧 Testing condition building...")
    
    conditions_tests = [
        ('name', '=', 'Test Record'),
        ('value', '>', 100.0),
        ('value', 'in', [123.45, 110.0, 120.0]),
        ('name', 'like', '%Record%'),
        ('name', 'contains', 'Test'),
        ('data', 'contains', 'test'),
    ]
    
    for field, op, value in conditions_tests:
        try:
            condition = backend.build_condition(field, op, value)
            print(f"✅ Condition ({field} {op} {value}): {condition}")
        except Exception as e:
            print(f"❌ Condition building failed ({field} {op} {value}): {e}")
    
    # Test backend capabilities
    print("\n🎯 Testing backend capabilities...")
    
    capabilities = {
        'transactions': backend.supports_transactions(),
        'graph_relations': backend.supports_graph_relations(),
        'bulk_operations': backend.supports_bulk_operations(),
        'explain': backend.supports_explain(),
        'indexes': backend.supports_indexes(),
        'full_text_search': backend.supports_full_text_search(),
    }
    
    print("ClickHouse capabilities:")
    for cap, supported in capabilities.items():
        status = "✅" if supported else "❌"
        print(f"  {status} {cap}: {supported}")
    
    # Get optimized methods
    optimizations = backend.get_optimized_methods()
    print("\nClickHouse optimizations:")
    for method, description in optimizations.items():
        print(f"  • {method}: {description}")
    
    # Test value formatting
    print("\n🔧 Testing value formatting...")
    
    format_tests = [
        ("string", "test string"),
        ("int", 42),
        ("float", 3.14159),
        ("bool true", True),
        ("bool false", False),
        ("datetime", datetime.now(timezone.utc)),
        ("list", ["a", "b", "c"]),
        ("dict", {"key": "value", "number": 123}),
        ("none", None),
    ]
    
    for name, value in format_tests:
        try:
            formatted = backend.format_value(value)
            print(f"✅ Format {name}: {value} → {formatted}")
        except Exception as e:
            print(f"❌ Format {name} failed: {e}")
    
    # Clean up
    print("\n🧹 Cleaning up...")
    try:
        await backend._execute(f"DROP TABLE IF EXISTS {table_name}")
        print("✅ Test table cleaned up")
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
    
    print("\n" + "="*60)
    print("🎉 ClickHouse Backend Test Completed Successfully!")
    
    return True


async def main():
    """Main test runner."""
    print("🚀 Starting ClickHouse Backend Test")
    
    try:
        success = await test_clickhouse_backend()
        
        if success:
            print("\n✅ All ClickHouse backend tests passed!")
            return True
        else:
            print("\n❌ Some tests failed!")
            return False
            
    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)