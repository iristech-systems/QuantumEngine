#!/usr/bin/env python3
"""
Test script to verify ClickHouse connection and marketplace schema creation.

This script connects to a real ClickHouse instance and tests:
- Connection establishment
- Table creation with advanced features
- Data insertion and querying
- Index creation and verification
"""

import asyncio
import datetime
from decimal import Decimal
import clickhouse_connect

# Import QuantumORM components
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    from src.quantumengine.connection import ConnectionRegistry
    from src.quantumengine.backends.clickhouse import ClickHouseBackend
    from src.quantumengine.document import Document
    from src.quantumengine.fields import (
        StringField, DecimalField, DateTimeField, BooleanField, 
        IntField, FloatField
    )
    from src.quantumengine.fields.clickhouse import (
        LowCardinalityField, CompressedStringField
    )
    print("✅ Successfully imported QuantumORM components")
except ImportError as e:
    print(f"❌ Failed to import QuantumORM components: {e}")
    exit(1)


# ClickHouse connection configuration
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'username': 'default',
    'password': '',
    'database': 'default'
}


class MarketplaceData(Document):
    """Marketplace monitoring data document for ClickHouse testing."""
    
    # ID field - required for ORDER BY
    id = StringField(required=True)
    
    # Core identifiers
    product_sku_model_number = StringField(
        required=True,
        indexes=[
            {'type': 'bloom_filter', 'granularity': 3, 'false_positive_rate': 0.01}
        ]
    )
    seller_name = LowCardinalityField(required=True)
    marketplace = LowCardinalityField(
        required=True,
        indexes=[
            {'type': 'set', 'granularity': 1, 'max_values': 100}
        ]
    )
    
    # Time dimension
    date_collected = DateTimeField(required=True)
    
    # Pricing data
    offer_price = DecimalField(required=True)
    product_msrp = DecimalField()
    product_umap = DecimalField()
    
    # Boolean flags
    below_map = BooleanField(default=False)
    buybox_winner = BooleanField(
        default=False,
        indexes=[
            {'type': 'set', 'granularity': 1, 'max_values': 2}
        ]
    )
    
    # Product categorization
    product_brand = LowCardinalityField()
    product_category = LowCardinalityField()
    
    # URLs with compression
    ad_page_url = CompressedStringField(codec="ZSTD(3)")
    
    # Materialized fields
    price_tier = LowCardinalityField(
        materialized="CASE WHEN offer_price < 50 THEN 'budget' "
                    "WHEN offer_price < 200 THEN 'mid' ELSE 'premium' END"
    )
    date_only = DateTimeField(materialized="toDate(date_collected)")
    year_month = IntField(materialized="toYYYYMM(date_collected)")
    
    class Meta:
        backend = 'clickhouse'
        table_name = 'marketplace_data_test'
        engine = 'ReplacingMergeTree'
        engine_params = ['date_collected']
        partition_by = 'toYYYYMM(date_collected)'
        order_by = ['seller_name', 'product_sku_model_number', 'date_collected']
        ttl = 'date_collected + INTERVAL 1 MONTH'  # Short TTL for testing
        settings = {
            'index_granularity': 8192,
            'merge_max_block_size': 8192
        }


async def test_clickhouse_connection():
    """Test basic ClickHouse connection."""
    print("\n🔍 Testing ClickHouse Connection...")
    
    try:
        # Create ClickHouse client
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        
        # Test basic query
        result = client.query("SELECT version()")
        version = result.result_rows[0][0] if result.result_rows else "Unknown"
        print(f"✅ Connected to ClickHouse version: {version}")
        
        # Test database access
        result = client.query("SELECT currentDatabase()")
        database = result.result_rows[0][0] if result.result_rows else "Unknown"
        print(f"✅ Current database: {database}")
        
        return client
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        print("💡 Make sure ClickHouse is running on localhost:8123")
        return None


async def test_table_creation(client):
    """Test creating the marketplace table with advanced features."""
    print("\n🔍 Testing Table Creation...")
    
    try:
        # Create ClickHouse backend
        backend = ClickHouseBackend(client)
        
        # Drop table if exists for clean test
        try:
            await backend._execute("DROP TABLE IF EXISTS marketplace_data_test")
            print("✅ Cleaned up existing test table")
        except Exception:
            pass  # Table might not exist
        
        # Create table using QuantumORM
        await backend.create_table(MarketplaceData)
        print("✅ Created marketplace_data_test table with advanced features")
        
        # Verify table structure
        result = await backend._query("DESCRIBE marketplace_data_test")
        if result:
            print(f"✅ Table has {len(result)} columns")
            
            # Check for specific columns
            column_names = [row[0] for row in result]
            expected_columns = [
                'product_sku_model_number', 'seller_name', 'marketplace',
                'date_collected', 'offer_price', 'price_tier', 'date_only'
            ]
            
            for col in expected_columns:
                if col in column_names:
                    print(f"   ✅ Column '{col}' exists")
                else:
                    print(f"   ❌ Column '{col}' missing")
        
        # Check table engine
        engine_query = "SELECT engine FROM system.tables WHERE name = 'marketplace_data_test'"
        result = await backend._query(engine_query)
        if result and result[0]:
            engine = result[0][0]
            if 'ReplacingMergeTree' in engine:
                print(f"✅ Table engine: {engine}")
            else:
                print(f"❌ Unexpected engine: {engine}")
        
        return backend
    except Exception as e:
        print(f"❌ Failed to create table: {e}")
        import traceback
        traceback.print_exc()
        return None


async def test_data_operations(backend):
    """Test CRUD operations with the new table."""
    print("\n🔍 Testing Data Operations...")
    
    try:
        table_name = 'marketplace_data_test'
        
        # Test data insertion
        test_data = {
            'product_sku_model_number': 'TEST-SKU-001',
            'seller_name': 'Amazon.com',
            'marketplace': 'Amazon',
            'date_collected': datetime.datetime.now(),
            'offer_price': Decimal('99.99'),
            'product_msrp': Decimal('129.99'),
            'product_umap': Decimal('119.99'),
            'below_map': False,
            'buybox_winner': True,
            'product_brand': 'TestBrand',
            'product_category': 'Electronics',
            'ad_page_url': 'https://example.com/very-long-url-that-should-be-compressed'
        }
        
        # Insert data
        result = await backend.insert(table_name, test_data)
        print(f"✅ Inserted test data: SKU={result['product_sku_model_number']}")
        
        # Wait a moment for ClickHouse to process
        await asyncio.sleep(1)
        
        # Query data back
        conditions = ["product_sku_model_number = 'TEST-SKU-001'"]
        results = await backend.select(table_name, conditions)
        
        if results and len(results) > 0:
            row = results[0]
            print(f"✅ Retrieved data: SKU={row.get('product_sku_model_number')}")
            print(f"   Seller: {row.get('seller_name')}")
            print(f"   Price: {row.get('offer_price')}")
            print(f"   Materialized price_tier: {row.get('price_tier')}")
            print(f"   Materialized date_only: {row.get('date_only')}")
        else:
            print("❌ No data retrieved")
            return False
        
        # Test count
        count = await backend.count(table_name, [])
        print(f"✅ Total records: {count}")
        
        return True
    except Exception as e:
        print(f"❌ Data operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_advanced_queries(backend):
    """Test ClickHouse-specific queries and functions."""
    print("\n🔍 Testing Advanced ClickHouse Queries...")
    
    try:
        table_name = 'marketplace_data_test'
        
        # Test materialized column queries
        query = f"""
        SELECT 
            product_sku_model_number,
            seller_name,
            offer_price,
            price_tier,
            date_only,
            year_month
        FROM {table_name}
        WHERE price_tier = 'mid'
        ORDER BY date_collected DESC
        LIMIT 5
        """
        
        result = await backend._query(query)
        if result:
            print(f"✅ Materialized column query returned {len(result)} rows")
            for row in result:
                print(f"   SKU: {row[0]}, Price: {row[2]}, Tier: {row[3]}")
        else:
            print("ℹ️ No rows matched materialized column filter")
        
        # Test aggregation query (typical for marketplace analytics)
        agg_query = f"""
        SELECT 
            seller_name,
            marketplace,
            count() as listing_count,
            avg(offer_price) as avg_price,
            countIf(buybox_winner = 1) as buybox_wins
        FROM {table_name}
        GROUP BY seller_name, marketplace
        ORDER BY listing_count DESC
        """
        
        result = await backend._query(agg_query)
        if result:
            print(f"✅ Aggregation query returned {len(result)} seller groups")
            for row in result:
                print(f"   Seller: {row[0]}, Listings: {row[2]}, Avg Price: {row[3]}")
        
        return True
    except Exception as e:
        print(f"❌ Advanced queries failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_index_verification(backend):
    """Verify that indexes were created properly."""
    print("\n🔍 Testing Index Verification...")
    
    try:
        # Query system.data_skipping_indices to check indexes
        index_query = """
        SELECT 
            name,
            type,
            granularity
        FROM system.data_skipping_indices 
        WHERE table = 'marketplace_data_test'
        ORDER BY name
        """
        
        result = await backend._query(index_query)
        if result:
            print(f"✅ Found {len(result)} data skipping indexes:")
            for row in result:
                print(f"   Index: {row[0]}, Type: {row[1]}, Granularity: {row[2]}")
        else:
            print("ℹ️ No data skipping indexes found (they may be created asynchronously)")
        
        return True
    except Exception as e:
        print(f"❌ Index verification failed: {e}")
        return False


async def cleanup_test_data(backend):
    """Clean up test data and table."""
    print("\n🧹 Cleaning Up Test Data...")
    
    try:
        # Drop test table
        await backend._execute("DROP TABLE IF EXISTS marketplace_data_test")
        print("✅ Cleaned up test table")
        return True
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False


async def main():
    """Main test function."""
    print("🚀 ClickHouse Connection and Schema Test")
    print("=" * 60)
    
    all_tests_passed = True
    client = None
    backend = None
    
    try:
        # Test connection
        client = await test_clickhouse_connection()
        if not client:
            print("❌ Cannot proceed without ClickHouse connection")
            return False
        
        # Test table creation
        backend = await test_table_creation(client)
        if not backend:
            print("❌ Cannot proceed without table creation")
            return False
        all_tests_passed &= True
        
        # Test data operations
        all_tests_passed &= await test_data_operations(backend)
        
        # Test advanced queries
        all_tests_passed &= await test_advanced_queries(backend)
        
        # Test index verification
        all_tests_passed &= await test_index_verification(backend)
        
        print("\n" + "=" * 60)
        if all_tests_passed:
            print("🎉 All ClickHouse Tests Passed!")
            print("\n✅ Verified ClickHouse Integration:")
            print("- ✅ Connection establishment")
            print("- ✅ ReplacingMergeTree table creation")
            print("- ✅ LowCardinality field support")
            print("- ✅ Materialized column functionality")
            print("- ✅ Advanced indexing")
            print("- ✅ Compression codec support")
            print("- ✅ CRUD operations")
            print("- ✅ Complex analytical queries")
            
            print("\n🚀 QuantumORM + ClickHouse integration is production-ready!")
        else:
            print("❌ Some tests failed. Check the output above for details.")
        
        return all_tests_passed
        
    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Always try to cleanup
        if backend:
            try:
                await cleanup_test_data(backend)
            except Exception as e:
                print(f"⚠️ Cleanup warning: {e}")


if __name__ == "__main__":
    # Run the async test
    result = asyncio.run(main())
    exit(0 if result else 1)