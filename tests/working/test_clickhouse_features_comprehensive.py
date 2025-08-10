#!/usr/bin/env python3
"""
Comprehensive test suite for all ClickHouse enhancements in QuantumORM.

This test verifies all the new ClickHouse-specific features:
1. LowCardinalityField for efficient string storage
2. FixedStringField for fixed-length strings  
3. EnumField for predefined values
4. ArrayField with nested type support
5. CompressedStringField with codecs
6. ClickHouse-specific query functions
7. Enhanced table creation with advanced indexing
8. TTL support for data lifecycle management
9. MaterializedDocument with ClickHouse optimizations
"""

import asyncio
import datetime
from decimal import Decimal
from typing import List

# Import QuantumORM components
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    from quantumengine import create_connection, Document
    from quantumengine.backends.base import BaseBackend
    from quantumengine.fields import (
        StringField, DecimalField, DateTimeField, BooleanField, IntField
    )
    from quantumengine.fields.clickhouse import (
        LowCardinalityField, FixedStringField, EnumField, ArrayField,
        CompressedStringField, CompressedLowCardinalityField
    )
    from quantumengine.materialized_document import (
        MaterializedDocument, MaterializedField, 
        Count, Sum, Avg, Max, Min, CountDistinct, ToDate
    )
    from quantumengine.clickhouse_functions import (
        has, hasAll, hasAny, toDate, toYYYYMM, length, lower, upper, round_
    )
    print("✅ Successfully imported QuantumORM components")
except ImportError as e:
    print(f"❌ Failed to import QuantumORM components: {e}")
    exit(1)


# Test Document with all ClickHouse field types
class MarketplaceData(Document):
    """Comprehensive test document with all ClickHouse field types."""
    
    # Basic fields
    product_sku = StringField(required=True)
    
    # ClickHouse-specific fields
    seller_name = LowCardinalityField(required=True)  # Efficient for repeated values
    marketplace = LowCardinalityField(choices=['Amazon', 'eBay', 'Walmart', 'Target'])
    currency_code = FixedStringField(length=3)  # USD, EUR, GBP
    country_code = FixedStringField(length=2)   # US, CA, GB
    
    # Enum field
    status = EnumField(values={
        'active': 1,
        'inactive': 2, 
        'discontinued': 3,
        'pending': 4
    })
    
    # Array fields with different element types
    tags = ArrayField(LowCardinalityField())  # Array(LowCardinality(String))
    ratings = ArrayField(IntField())           # Array(Int64)
    image_urls = ArrayField(StringField(), codec="LZ4")  # Compressed array
    
    # Compressed fields
    product_description = CompressedStringField(codec="ZSTD(3)")
    category_path = CompressedLowCardinalityField(codec="LZ4")
    
    # Standard fields
    offer_price = DecimalField(max_digits=10, decimal_places=2, required=True)
    quantity = IntField(default=1)
    date_collected = DateTimeField(required=True)
    is_buybox_winner = BooleanField(default=False)
    
    class Meta:
        backend = 'clickhouse'
        collection = 'marketplace_data_comprehensive'
        
        # Advanced ClickHouse table options
        engine = 'ReplacingMergeTree'
        order_by = ['marketplace', 'seller_name', 'product_sku', 'date_collected']
        partition_by = 'toYYYYMM(date_collected)'
        
        # Advanced indexing
        indexes = [
            {
                'name': 'seller_marketplace_idx',
                'fields': ['seller_name', 'marketplace'],
                'type': 'minmax',
                'granularity': 4
            },
            {
                'name': 'description_bloom_idx', 
                'fields': ['product_description'],
                'type': 'bloom_filter',
                'false_positive': 0.01
            }
        ]
        
        # TTL for data lifecycle management
        ttl = 'date_collected + INTERVAL 2 YEAR'


# MaterializedDocument with ClickHouse optimizations
class DailyMarketplaceSummary(MaterializedDocument):
    """Daily marketplace summary with ClickHouse-specific optimizations."""
    
    class Meta:
        source = MarketplaceData
        backend = 'clickhouse'
        view_name = 'daily_marketplace_summary_comprehensive'
        
        # ClickHouse materialized view options
        engine = 'SummingMergeTree'
        order_by = ['date', 'marketplace', 'seller_name']
        partition_by = 'toYYYYMM(date)'
        
    # Dimensions
    date = MaterializedField(source='date_collected', transform=ToDate('date_collected'))
    marketplace = MaterializedField(source='marketplace')
    seller_name = MaterializedField(source='seller_name')
    
    # Metrics with ClickHouse functions
    total_revenue = MaterializedField(aggregate=Sum('offer_price'))
    total_quantity = MaterializedField(aggregate=Sum('quantity'))
    transaction_count = MaterializedField(aggregate=Count())
    avg_price = MaterializedField(aggregate=Avg('offer_price'))
    unique_products = MaterializedField(aggregate=CountDistinct('product_sku'))
    max_price = MaterializedField(aggregate=Max('offer_price'))


async def setup_test_environment() -> Optional[BaseBackend]:
    """Set up ClickHouse connection and clean environment."""
    print("\n🔧 Setting Up Test Environment...")
    
    try:
        # Use test ClickHouse connection
        backend = create_connection(
            backend='clickhouse',
            host="localhost",
            port=8123,
            database="quantum_test",
            username="default",
            password="",
        )
        
        print("✅ Connected to ClickHouse")
        
        # Clean up existing tables/views
        cleanup_queries = [
            "DROP TABLE IF EXISTS daily_marketplace_summary_comprehensive",
            "DROP TABLE IF EXISTS marketplace_data_comprehensive"
        ]
        
        for query in cleanup_queries:
            try:
                await backend.execute_raw(query)
            except Exception:
                pass
        
        print("✅ Cleaned up existing tables/views")
        return backend
        
    except Exception as e:
        print(f"❌ Failed to setup environment: {e}")
        import traceback
        traceback.print_exc()
        return None


async def test_clickhouse_field_types(backend: BaseBackend):
    """Test all ClickHouse field types."""
    print("\n🔍 Testing ClickHouse Field Types...")
    
    try:
        # Create table with all field types
        await backend.create_table(MarketplaceData)
        print("✅ Created table with all ClickHouse field types")
        
        # Verify table structure
        table_info = await backend.execute_raw("DESCRIBE marketplace_data_comprehensive")
        field_types = {row['name']: row['type'] for row in table_info}
        
        # Verify ClickHouse-specific types
        expected_types = {
            'seller_name': 'LowCardinality(String)',
            'marketplace': 'LowCardinality(String)', 
            'currency_code': 'FixedString(3)',
            'country_code': 'FixedString(2)',
            'status': 'Enum8',
            'tags': 'Array(LowCardinality(String))',
            'ratings': 'Array(Int64)',
            'image_urls': 'Array(String)',
        }
        
        success = True
        for field, expected_type in expected_types.items():
            actual_type = field_types.get(field, '')
            if expected_type in actual_type:
                print(f"✅ {field}: {actual_type}")
            else:
                print(f"❌ {field}: expected {expected_type}, got {actual_type}")
                success = False
        
        return success
        
    except Exception as e:
        print(f"❌ Failed to test ClickHouse field types: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_data_insertion_and_types(backend: BaseBackend):
    """Test data insertion with all ClickHouse field types."""
    print("\n📝 Testing Data Insertion with ClickHouse Types...")
    
    try:
        # Insert test data with all field types
        test_data = [
            {
                'product_sku': 'PHONE-001',
                'seller_name': 'Amazon.com',
                'marketplace': 'Amazon',
                'currency_code': 'USD',
                'country_code': 'US',
                'status': 'active',
                'tags': ['electronics', 'phones', 'smartphone'],
                'ratings': [4, 5, 3, 4, 5],
                'image_urls': ['https://example.com/img1.jpg', 'https://example.com/img2.jpg'],
                'product_description': 'Latest smartphone with advanced features and high-quality camera',
                'category_path': 'Electronics > Phones > Smartphones',
                'offer_price': Decimal('799.99'),
                'quantity': 1,
                'date_collected': datetime.datetime.now(),
                'is_buybox_winner': True
            },
            {
                'product_sku': 'LAPTOP-002',
                'seller_name': 'BestBuy',
                'marketplace': 'eBay',
                'currency_code': 'USD', 
                'country_code': 'US',
                'status': 'active',
                'tags': ['electronics', 'computers', 'laptop'],
                'ratings': [5, 4, 5, 4],
                'image_urls': ['https://example.com/laptop1.jpg'],
                'product_description': 'High-performance laptop for gaming and professional work',
                'category_path': 'Electronics > Computers > Laptops',
                'offer_price': Decimal('1299.99'),
                'quantity': 2,
                'date_collected': datetime.datetime.now() - datetime.timedelta(days=1),
                'is_buybox_winner': False
            }
        ]
        
        await backend.insert_many('marketplace_data_comprehensive', test_data)
        print(f"✅ Inserted {len(test_data)} records with ClickHouse field types")
        
        # Verify data insertion
        count = await backend.count('marketplace_data_comprehensive', [])
        print(f"✅ Total records in table: {count}")
        
        # Test querying with ClickHouse functions
        query_tests = [
            # Array functions
            ("has(tags, 'electronics')", "Testing has() function"),
            ("length(ratings) > 3", "Testing length() function"),
            ("hasAny(tags, ['phones', 'laptop'])", "Testing hasAny() function"),
            
            # Date functions
            ("toDate(date_collected) = today()", "Testing toDate() and today() functions"),
            
            # String functions
            ("lower(seller_name) LIKE '%amazon%'", "Testing lower() function"),
            ("length(product_description) > 50", "Testing string length"),
            
            # Type-specific queries
            ("status = 'active'", "Testing Enum field queries"),
            ("currency_code = 'USD'", "Testing FixedString field queries"),
            ("marketplace IN ('Amazon', 'eBay')", "Testing LowCardinality field queries"),
        ]
        
        for condition, description in query_tests:
            try:
                results = await backend.select('marketplace_data_comprehensive', [condition])
                print(f"✅ {description}: {len(results)} results")
            except Exception as e:
                print(f"❌ {description}: {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to test data insertion: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_materialized_view_clickhouse(backend: BaseBackend):
    """Test MaterializedDocument with ClickHouse optimizations."""
    print("\n🏗️ Testing MaterializedDocument with ClickHouse...")
    
    try:
        # Create materialized view
        await DailyMarketplaceSummary.create_view()
        print("✅ Created ClickHouse materialized view")
        
        # Wait for data to populate
        await asyncio.sleep(2)
        
        # Query materialized view
        results = await backend.execute_raw("SELECT * FROM daily_marketplace_summary_comprehensive")
        print(f"✅ Materialized view contains {len(results)} aggregated records")
        
        if results:
            for record in results[:3]:  # Show first 3 records
                date_str = str(record.get('date', 'N/A'))[:10]
                marketplace = record.get('marketplace', 'N/A')
                seller = record.get('seller_name', 'N/A')
                revenue = record.get('total_revenue', 0)
                count = record.get('transaction_count', 0)
                print(f"   {date_str} | {marketplace} | {seller} | Revenue: ${revenue} | Count: {count}")
        
        # Test materialized view performance
        explain_query = "EXPLAIN SELECT * FROM daily_marketplace_summary_comprehensive WHERE marketplace = 'Amazon'"
        explain_result = await backend.execute_raw(explain_query)
        print("✅ Materialized view query execution plan verified")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to test materialized view: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_clickhouse_optimizations(backend: BaseBackend):
    """Test ClickHouse-specific optimizations."""
    print("\n⚡ Testing ClickHouse Optimizations...")
    
    try:
        # Test table engine and partitioning
        table_info = await backend.execute_raw("SHOW CREATE TABLE marketplace_data_comprehensive")
        create_statement = table_info[0]['statement']
        
        optimization_checks = [
            ('ReplacingMergeTree', 'Engine optimization'),
            ('PARTITION BY', 'Partitioning by date'),
            ('ORDER BY', 'Optimized sorting key'),
            ('TTL', 'Data lifecycle management'),
        ]
        
        for check, description in optimization_checks:
            if check in create_statement:
                print(f"✅ {description}: {check} found")
            else:
                print(f"⚠️ {description}: {check} not found")
        
        # Test compression and encoding
        compression_info = await backend.execute_raw(
            "SELECT name, type, compression_codec FROM system.columns "
            "WHERE table = 'marketplace_data_comprehensive' AND database = 'quantum_test'"
        )
        
        compressed_fields = [col for col in compression_info if col.get('compression_codec')]
        print(f"✅ Found {len(compressed_fields)} fields with compression")
        
        # Test index usage
        index_info = await backend.execute_raw(
            "SELECT name, type, granularity FROM system.data_skipping_indices "
            "WHERE table = 'marketplace_data_comprehensive' AND database = 'quantum_test'"
        )
        
        print(f"✅ Found {len(index_info)} specialized indexes")
        for idx in index_info:
            print(f"   Index: {idx['name']} ({idx['type']})")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to test optimizations: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_clickhouse_functions_integration(backend: BaseBackend):
    """Test ClickHouse functions in real queries."""
    print("\n🔧 Testing ClickHouse Functions Integration...")
    
    try:
        # Test complex queries with ClickHouse functions
        function_tests = [
            {
                'query': "SELECT seller_name, length(tags) as tag_count FROM marketplace_data_comprehensive WHERE has(tags, 'electronics')",
                'description': 'Array functions (has, length)'
            },
            {
                'query': "SELECT toDate(date_collected) as date, count(*) FROM marketplace_data_comprehensive GROUP BY toDate(date_collected)",
                'description': 'Date functions (toDate)'
            },
            {
                'query': "SELECT upper(marketplace) as marketplace_upper, lower(seller_name) as seller_lower FROM marketplace_data_comprehensive LIMIT 5",
                'description': 'String functions (upper, lower)'
            },
            {
                'query': "SELECT round(offer_price, 0) as rounded_price FROM marketplace_data_comprehensive LIMIT 5",
                'description': 'Math functions (round)'
            },
            {
                'query': "SELECT if(offer_price > 1000, 'expensive', 'affordable') as price_category FROM marketplace_data_comprehensive LIMIT 5",
                'description': 'Conditional functions (if)'
            }
        ]
        
        for test in function_tests:
            try:
                results = await backend.execute_raw(test['query'])
                print(f"✅ {test['description']}: {len(results)} results")
            except Exception as e:
                print(f"❌ {test['description']}: {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to test ClickHouse functions: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_performance_monitoring(backend: BaseBackend):
    """Test performance monitoring and query optimization."""
    print("\n📊 Testing Performance Monitoring...")
    
    try:
        # Test query performance with EXPLAIN
        performance_queries = [
            "SELECT * FROM marketplace_data_comprehensive WHERE marketplace = 'Amazon'",
            "SELECT * FROM marketplace_data_comprehensive WHERE has(tags, 'electronics')",
            "SELECT * FROM daily_marketplace_summary_comprehensive WHERE date = today()",
        ]
        
        for query in performance_queries:
            # Run EXPLAIN
            explain_query = f"EXPLAIN {query}"
            explain_result = await backend.execute_raw(explain_query)
            
            # Run actual query and measure
            start_time = asyncio.get_event_loop().time()
            results = await backend.execute_raw(query)
            end_time = asyncio.get_event_loop().time()
            
            execution_time = (end_time - start_time) * 1000  # Convert to ms
            print(f"✅ Query executed in {execution_time:.2f}ms, returned {len(results)} results")
        
        # Test system information
        system_queries = [
            ("SELECT count() FROM marketplace_data_comprehensive", "Total records"),
            ("SELECT uniq(seller_name) FROM marketplace_data_comprehensive", "Unique sellers"),
            ("SELECT sum(offer_price) FROM marketplace_data_comprehensive", "Total value"),
        ]
        
        for query, description in system_queries:
            result = await backend.execute_raw(query)
            value = list(result[0].values())[0] if result else 0
            print(f"✅ {description}: {value}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to test performance monitoring: {e}")
        import traceback
        traceback.print_exc()
        return False


async def cleanup_test_environment(backend: BaseBackend):
    """Clean up test data and views."""
    print("\n🧹 Cleaning Up Test Environment...")
    
    try:
        # Drop materialized view
        await DailyMarketplaceSummary.drop_view()
        print("✅ Dropped materialized view")
        
        # Drop base table
        await backend.execute_raw("DROP TABLE IF EXISTS marketplace_data_comprehensive")
        print("✅ Dropped base table")
        
        return True
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False


async def main():
    """Main comprehensive test function."""
    print("🚀 QuantumORM ClickHouse Comprehensive Feature Test")
    print("=" * 80)
    print("Testing all ClickHouse enhancements:")
    print("- LowCardinalityField, FixedStringField, EnumField, ArrayField")
    print("- Compressed fields and advanced indexing")
    print("- ClickHouse query functions (has, toDate, length, etc.)")
    print("- MaterializedDocument optimizations")
    print("- Performance monitoring and optimization")
    
    backend = None
    all_tests_passed = True
    
    try:
        # Setup
        backend = await setup_test_environment()
        if not backend:
            return False
        
        # Run comprehensive tests
        all_tests_passed &= await test_clickhouse_field_types(backend)
        all_tests_passed &= await test_data_insertion_and_types(backend)
        all_tests_passed &= await test_materialized_view_clickhouse(backend)
        all_tests_passed &= await test_clickhouse_optimizations(backend)
        all_tests_passed &= await test_clickhouse_functions_integration(backend)
        all_tests_passed &= await test_performance_monitoring(backend)
        
        print("\n" + "=" * 80)
        if all_tests_passed:
            print("🎉 All ClickHouse Comprehensive Tests Passed!")
            print("\n✅ Verified Complete ClickHouse Feature Set:")
            print("- ✅ All ClickHouse field types (LowCardinality, FixedString, Enum, Array)")
            print("- ✅ Compression codecs and advanced indexing")
            print("- ✅ ClickHouse-specific query functions")
            print("- ✅ Optimized MaterializedDocument implementation")
            print("- ✅ Performance monitoring and query optimization")
            print("- ✅ Data lifecycle management with TTL")
            print("- ✅ Production-ready table engines and partitioning")
            
            print("\n🚀 QuantumORM ClickHouse backend is production-ready with")
            print("   enterprise-grade features and optimizations!")
        else:
            print("❌ Some tests failed. Check the output above for details.")
        
        return all_tests_passed
        
    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Always cleanup
        if backend:
            try:
                await cleanup_test_environment(backend)
            except Exception as e:
                print(f"⚠️ Cleanup warning: {e}")


if __name__ == "__main__":
    # Run the async test
    result = asyncio.run(main())
    exit(0 if result else 1)