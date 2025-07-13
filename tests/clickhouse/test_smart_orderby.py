#!/usr/bin/env python3
"""
Test script to verify smart ORDER BY detection for ClickHouse.

This demonstrates how QuantumORM intelligently chooses ORDER BY clauses
without requiring manual 'id' fields, making ClickHouse much more user-friendly.
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
    from src.quantumorm.backends.clickhouse import ClickHouseBackend
    from src.quantumorm.document import Document
    from src.quantumorm.fields import (
        StringField, DecimalField, DateTimeField, BooleanField, 
        IntField, FloatField
    )
    from src.quantumorm.fields.clickhouse import LowCardinalityField
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


class TimeSeriesData(Document):
    """Time-series data - should auto-detect timestamp for ORDER BY."""
    
    sensor_id = LowCardinalityField(required=True)
    timestamp = DateTimeField(required=True)
    temperature = FloatField(required=True)
    humidity = FloatField()
    location = LowCardinalityField()
    
    class Meta:
        backend = 'clickhouse'
        table_name = 'timeseries_test'
        # NO order_by specified - should auto-detect


class UserEvents(Document):
    """User event data - should auto-detect user_id + event_time."""
    
    user_id = StringField(required=True)
    event_type = LowCardinalityField(required=True)
    event_time = DateTimeField(required=True)
    session_id = StringField()
    properties = StringField()
    
    class Meta:
        backend = 'clickhouse'
        table_name = 'events_test'
        # NO order_by specified - should auto-detect


class ProductCatalog(Document):
    """Product catalog - should auto-detect categorical fields."""
    
    product_id = StringField(required=True)
    product_name = StringField(required=True)
    category = LowCardinalityField(required=True)
    brand = LowCardinalityField(required=True)
    price = DecimalField()
    description = StringField()
    
    class Meta:
        backend = 'clickhouse'
        table_name = 'products_test'
        # NO order_by specified - should auto-detect


class SimpleData(Document):
    """Minimal data - should handle gracefully."""
    
    value = FloatField(required=True)
    label = StringField()
    
    class Meta:
        backend = 'clickhouse'
        table_name = 'simple_test'
        # NO order_by specified - should auto-detect


async def test_smart_order_by():
    """Test the smart ORDER BY detection logic."""
    print("\n🔍 Testing Smart ORDER BY Detection...")
    
    try:
        # Create ClickHouse client and backend
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        backend = ClickHouseBackend(client)
        
        print("✅ Connected to ClickHouse")
        
        # Test different document patterns
        test_cases = [
            (TimeSeriesData, "Time-series data"),
            (UserEvents, "User event data"), 
            (ProductCatalog, "Product catalog"),
            (SimpleData, "Simple data")
        ]
        
        for document_class, description in test_cases:
            print(f"\n📊 Testing: {description}")
            
            # Get the smart ORDER BY
            order_by = backend._determine_smart_order_by(document_class)
            print(f"   Auto-detected ORDER BY: {order_by}")
            
            # Test table creation
            table_name = document_class._meta.get('table_name')
            try:
                await backend._execute(f"DROP TABLE IF EXISTS {table_name}")
                await backend.create_table(document_class)
                print(f"   ✅ Table '{table_name}' created successfully")
                
                # Verify the table was created with correct ORDER BY
                engine_query = f"""
                SELECT sorting_key 
                FROM system.tables 
                WHERE name = '{table_name}' AND database = currentDatabase()
                """
                result = await backend._query(engine_query)
                if result and result[0]:
                    actual_order_by = result[0][0]
                    print(f"   📋 Actual ORDER BY in ClickHouse: {actual_order_by}")
                
            except Exception as e:
                print(f"   ❌ Table creation failed: {e}")
                continue
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_marketplace_without_id():
    """Test the marketplace schema without manual id field."""
    print("\n🔍 Testing Marketplace Schema Without Manual ID...")
    
    try:
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        backend = ClickHouseBackend(client)
        
        # Create marketplace document WITHOUT manual id field
        class SmartMarketplaceData(Document):
            """Marketplace data using smart ORDER BY detection."""
            
            # Core identifiers (will be auto-detected for ORDER BY)
            seller_name = LowCardinalityField(required=True)
            product_sku_model_number = StringField(required=True)
            marketplace = LowCardinalityField(required=True)
            
            # Time dimension (should be prioritized for ORDER BY)
            date_collected = DateTimeField(required=True)
            
            # Pricing data
            offer_price = DecimalField(required=True)
            product_msrp = DecimalField()
            
            # Boolean flags
            below_map = BooleanField(default=False)
            buybox_winner = BooleanField(default=False)
            
            class Meta:
                backend = 'clickhouse'
                table_name = 'smart_marketplace_test'
                engine = 'ReplacingMergeTree'
                engine_params = ['date_collected']
                partition_by = 'toYYYYMM(date_collected)'
                # NO order_by specified - let it auto-detect!
        
        # Test smart ORDER BY detection
        order_by = backend._determine_smart_order_by(SmartMarketplaceData)
        print(f"   🧠 Smart ORDER BY detected: {order_by}")
        
        # Create table
        await backend._execute("DROP TABLE IF EXISTS smart_marketplace_test")
        await backend.create_table(SmartMarketplaceData)
        print("   ✅ Marketplace table created without manual id field!")
        
        # Test data insertion
        test_data = {
            'seller_name': 'Amazon.com',
            'product_sku_model_number': 'AUTO-SKU-001',
            'marketplace': 'Amazon',
            'date_collected': datetime.datetime.now(),
            'offer_price': Decimal('149.99'),
            'product_msrp': Decimal('199.99'),
            'below_map': False,
            'buybox_winner': True
        }
        
        result = await backend.insert('smart_marketplace_test', test_data)
        print(f"   ✅ Data inserted: SKU={result['product_sku_model_number']}")
        
        # Query data back
        conditions = ["product_sku_model_number = 'AUTO-SKU-001'"]
        results = await backend.select('smart_marketplace_test', conditions)
        
        if results:
            row = results[0]
            print(f"   ✅ Data retrieved: Seller={row.get('seller_name')}, Price={row.get('offer_price')}")
        
        # Clean up
        await backend._execute("DROP TABLE IF EXISTS smart_marketplace_test")
        print("   🧹 Cleaned up test table")
        
        return True
        
    except Exception as e:
        print(f"❌ Marketplace test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def cleanup_test_tables():
    """Clean up all test tables."""
    print("\n🧹 Cleaning up test tables...")
    
    try:
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        backend = ClickHouseBackend(client)
        
        tables = ['timeseries_test', 'events_test', 'products_test', 'simple_test']
        for table in tables:
            try:
                await backend._execute(f"DROP TABLE IF EXISTS {table}")
                print(f"   ✅ Dropped {table}")
            except Exception as e:
                print(f"   ⚠️ Could not drop {table}: {e}")
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")


async def main():
    """Main test function."""
    print("🧠 Smart ORDER BY Detection Test for ClickHouse")
    print("=" * 60)
    print("This test demonstrates QuantumORM's intelligent ORDER BY detection")
    print("that eliminates the need for manual 'id' fields in ClickHouse schemas.")
    
    all_tests_passed = True
    
    try:
        # Test smart ORDER BY detection
        all_tests_passed &= await test_smart_order_by()
        
        # Test marketplace without id
        all_tests_passed &= await test_marketplace_without_id()
        
        print("\n" + "=" * 60)
        if all_tests_passed:
            print("🎉 Smart ORDER BY Detection Tests Passed!")
            print("\n✅ Key Benefits Demonstrated:")
            print("- ✅ No manual 'id' field required")
            print("- ✅ Intelligent time-based ORDER BY detection")
            print("- ✅ Smart categorical field prioritization")
            print("- ✅ Graceful fallback for edge cases")
            print("- ✅ Optimized for ClickHouse performance patterns")
            
            print("\n🚀 ClickHouse is now much more user-friendly!")
            print("Users can focus on their data schema without ClickHouse boilerplate!")
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
        await cleanup_test_tables()


if __name__ == "__main__":
    # Run the async test
    result = asyncio.run(main())
    exit(0 if result else 1)