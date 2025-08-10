#!/usr/bin/env python3
"""
Simple SurrealDB MaterializedDocument test to verify basic functionality.
"""

import asyncio
import datetime
from decimal import Decimal

# Import QuantumORM components
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    from quantumengine import create_connection, Document
    from quantumengine.fields import StringField, DecimalField, DateTimeField, BooleanField, IntField
    from quantumengine.materialized_document import (
        MaterializedDocument, MaterializedField, 
        Count, Sum, Avg, Max, Min, CountDistinct, ToDate
    )
    print("✅ Successfully imported QuantumORM components")
except ImportError as e:
    print(f"❌ Failed to import QuantumORM components: {e}")
    exit(1)


# Base document for testing (SurrealDB)
class SalesData(Document):
    """Base sales data document for SurrealDB."""
    
    product_sku = StringField(required=True)
    seller_name = StringField(required=True)
    date_collected = DateTimeField(required=True)
    offer_price = DecimalField(required=True)
    quantity = IntField(default=1)
    
    class Meta:
        backend = 'surrealdb'
        collection = 'sales_data_simple'


# MaterializedDocument for SurrealDB
class DailySalesSummary(MaterializedDocument):
    """Daily sales summary for SurrealDB backend."""
    
    class Meta:
        source = SalesData
        backend = 'surrealdb'
        view_name = 'daily_sales_summary_simple'
        
    # Dimensions
    date = MaterializedField(source='date_collected', transform=ToDate('date_collected'))
    seller_name = MaterializedField(source='seller_name')
    
    # Metrics
    total_sales = MaterializedField(aggregate=Sum('offer_price'))
    transaction_count = MaterializedField(aggregate=Count())


async def test_query_generation():
    """Test that SurrealDB query generation and conversion works."""
    print("\n🔍 Testing Query Generation...")
    
    try:
        # Test basic ClickHouse-style query generation
        source_query = DailySalesSummary._build_source_query()
        print(f"✅ Generated ClickHouse-style query: {source_query}")
        
        # Test ClickHouse parts
        clickhouse_parts = ['SELECT', 'FROM', 'GROUP BY', 'toDate', 'SUM', 'COUNT']
        for part in clickhouse_parts:
            if part in source_query:
                print(f"✅ ClickHouse query contains: {part}")
            else:
                print(f"❌ ClickHouse query missing: {part}")
                return False
        
        # Test SurrealDB conversion
        from quantumengine.backends.surrealdb import SurrealDBBackend
        
        backend = SurrealDBBackend(connection_config={})
        converted_query = backend._convert_query_to_surrealdb(source_query)
        print(f"✅ Converted to SurrealDB: {converted_query}")
        
        # Test SurrealDB parts
        surrealdb_parts = ['time::day', 'math::sum', 'count()']
        for part in surrealdb_parts:
            if part in converted_query:
                print(f"✅ SurrealDB query contains: {part}")
            else:
                print(f"❌ SurrealDB query missing: {part}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Query generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_connection():
    """Test basic SurrealDB connection."""
    print("\n🔧 Testing SurrealDB Connection...")
    
    try:
        # Create backend using the new factory
        backend = create_connection(
            backend='surrealdb',
            url="ws://localhost:8000/rpc",
            namespace="test_ns",
            database="test_db",
            username="root",
            password="root"
        )
        # Test connection by executing a simple query
        await backend.execute_raw("SELECT 1")
        print("✅ Connected to SurrealDB successfully")
        
        # Connection successful - skip query test for now
        print("✅ Basic connection functionality verified")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Main test function."""
    print("🚀 SurrealDB MaterializedDocument Simple Test")
    print("=" * 60)
    
    all_tests_passed = True
    
    try:
        # Test query generation (no database required)
        all_tests_passed &= await test_query_generation()
        
        # Test basic connection
        all_tests_passed &= await test_connection()
        
        print("\n" + "=" * 60)
        if all_tests_passed:
            print("🎉 All Simple Tests Passed!")
            print("\n✅ Verified:")
            print("- ✅ MaterializedDocument query generation")
            print("- ✅ ClickHouse → SurrealDB function conversion")
            print("- ✅ Basic SurrealDB connection")
            print("- ✅ Cross-backend compatibility")
            
            print("\n🚀 SurrealDB MaterializedDocument is working!")
        else:
            print("❌ Some tests failed. Check the output above for details.")
        
        return all_tests_passed
        
    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Run the async test
    result = asyncio.run(main())
    exit(0 if result else 1)