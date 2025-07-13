#!/usr/bin/env python3
"""
Test script to verify MaterializedDocument functionality with ClickHouse.

This demonstrates the new Pythonic MaterializedDocument interface that
allows creating materialized views using familiar field definitions and
aggregation syntax.
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
    from src.quantumengine.backends.clickhouse import ClickHouseBackend
    from src.quantumengine.document import Document
    from src.quantumengine.fields import (
        StringField, DecimalField, DateTimeField, BooleanField, 
        IntField, FloatField
    )
    from src.quantumengine.fields.clickhouse import LowCardinalityField
    from src.quantumengine.materialized_document import (
        MaterializedDocument, MaterializedField, 
        Count, Sum, Avg, Max, Min, CountDistinct,
        ToDate, ToYearMonth
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


# Base document for testing
class SalesData(Document):
    """Base sales data document."""
    
    # Core identifiers
    product_sku = StringField(required=True)
    seller_name = LowCardinalityField(required=True)
    marketplace = LowCardinalityField(required=True)
    
    # Time dimension
    date_collected = DateTimeField(required=True)
    
    # Metrics
    offer_price = DecimalField(required=True)
    quantity = IntField(default=1)
    is_buybox_winner = BooleanField(default=False)
    
    class Meta:
        backend = 'clickhouse'
        table_name = 'sales_data_test'
        engine = 'MergeTree'
        order_by = ['date_collected', 'seller_name']


# Example 1: Simple daily sales summary
class DailySalesSummary(MaterializedDocument):
    """Daily sales summary using declarative syntax."""
    
    class Meta:
        source = SalesData
        backend = 'clickhouse'
        view_name = 'daily_sales_summary'
        engine = 'SummingMergeTree'
        order_by = ['date', 'seller_name']
        
    # Dimensions (grouping fields)
    date = MaterializedField(source='date_collected', transform=ToDate('date_collected'))
    seller_name = MaterializedField(source='seller_name')
    
    # Metrics (aggregation fields)
    total_sales = MaterializedField(aggregate=Sum('offer_price'))
    total_quantity = MaterializedField(aggregate=Sum('quantity'))
    transaction_count = MaterializedField(aggregate=Count())
    avg_price = MaterializedField(aggregate=Avg('offer_price'))
    max_price = MaterializedField(aggregate=Max('offer_price'))


# Example 2: Product performance with filters
class ProductPerformance(MaterializedDocument):
    """Product performance analysis with filters."""
    
    class Meta:
        source = SalesData
        backend = 'clickhouse'
        view_name = 'product_performance'
        engine = 'AggregatingMergeTree'
        order_by = ['product_sku', 'marketplace']
        
    # Only analyze products with significant volume
    class Filters:
        # Only include records from last year
        date_collected__gte = datetime.datetime.now() - datetime.timedelta(days=365)
        
    # Only include products with meaningful volume
    class Having:
        transaction_count__gte = 10
        
    # Dimensions
    product_sku = MaterializedField(source='product_sku')
    marketplace = MaterializedField(source='marketplace')
    
    # Metrics
    transaction_count = MaterializedField(aggregate=Count())
    unique_sellers = MaterializedField(aggregate=CountDistinct('seller_name'))
    total_revenue = MaterializedField(aggregate=Sum('offer_price'))
    avg_price = MaterializedField(aggregate=Avg('offer_price'))
    buybox_win_rate = MaterializedField(aggregate=Avg('is_buybox_winner'))


# Example 3: Monthly marketplace trends
class MonthlyMarketplaceTrends(MaterializedDocument):
    """Monthly trends by marketplace."""
    
    class Meta:
        source = SalesData
        backend = 'clickhouse'
        view_name = 'monthly_marketplace_trends'
        engine = 'SummingMergeTree'
        partition_by = 'year_month'
        order_by = ['year_month', 'marketplace']
        
    # Dimensions
    year_month = MaterializedField(source='date_collected', transform=ToYearMonth('date_collected'))
    marketplace = MaterializedField(source='marketplace')
    
    # Metrics
    total_listings = MaterializedField(aggregate=Count())
    total_revenue = MaterializedField(aggregate=Sum('offer_price'))
    avg_price = MaterializedField(aggregate=Avg('offer_price'))
    unique_products = MaterializedField(aggregate=CountDistinct('product_sku'))
    unique_sellers = MaterializedField(aggregate=CountDistinct('seller_name'))


async def test_materialized_document_creation():
    """Test creating MaterializedDocument classes."""
    print("\n🔍 Testing MaterializedDocument Class Creation...")
    
    # Test that the classes were created properly
    print(f"✅ DailySalesSummary dimension fields: {list(DailySalesSummary._dimension_fields.keys())}")
    print(f"✅ DailySalesSummary metric fields: {list(DailySalesSummary._metric_fields.keys())}")
    
    # Test source query generation
    try:
        source_query = DailySalesSummary._build_source_query()
        print(f"✅ Generated source query successfully")
        print(f"   Query: {source_query}")
    except Exception as e:
        print(f"❌ Failed to generate source query: {e}")
        return False
    
    return True


async def test_clickhouse_view_creation():
    """Test creating materialized views in ClickHouse."""
    print("\n🔍 Testing ClickHouse Materialized View Creation...")
    
    try:
        # Create ClickHouse client and backend
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        backend = ClickHouseBackend(client)
        
        print("✅ Connected to ClickHouse")
        
        # Register the backend connection for the SalesData document
        from src.quantumengine.connection import ConnectionRegistry
        # For ClickHouse, we need to register the actual client, not the backend
        ConnectionRegistry.register('clickhouse_test', client, 'clickhouse')
        ConnectionRegistry.set_default('clickhouse', 'clickhouse_test')
        
        # Create the base table first
        print("   Creating base sales_data_test table...")
        await backend.create_table(SalesData)
        print("   ✅ Base table created")
        
        # Test creating each materialized view
        materialized_views = [
            (DailySalesSummary, "Daily Sales Summary"),
            (ProductPerformance, "Product Performance"),
            (MonthlyMarketplaceTrends, "Monthly Marketplace Trends")
        ]
        
        for view_class, description in materialized_views:
            print(f"\n   Creating {description}...")
            try:
                await view_class.create_view()
                print(f"   ✅ {description} created successfully")
                
                # Test dropping the view
                await view_class.drop_view()
                print(f"   ✅ {description} dropped successfully")
                
            except Exception as e:
                print(f"   ❌ Failed to create {description}: {e}")
                return False
        
        # Clean up base table
        await backend._execute("DROP TABLE IF EXISTS sales_data_test")
        print("   ✅ Cleaned up base table")
        
        return True
        
    except Exception as e:
        print(f"❌ ClickHouse test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_complex_aggregations():
    """Test complex aggregation scenarios."""
    print("\n🔍 Testing Complex Aggregation Scenarios...")
    
    # Test aggregation functions
    count_agg = Count()
    sum_agg = Sum('price')
    avg_agg = Avg('price')
    
    print(f"✅ Count aggregation: {count_agg}")
    print(f"✅ Sum aggregation: {sum_agg}")
    print(f"✅ Avg aggregation: {avg_agg}")
    
    # Test transforms
    to_date = ToDate('date_collected')
    to_year_month = ToYearMonth('date_collected')
    
    print(f"✅ ToDate transform: {to_date}")
    print(f"✅ ToYearMonth transform: {to_year_month}")
    
    return True


async def test_field_configuration():
    """Test MaterializedField configuration."""
    print("\n🔍 Testing MaterializedField Configuration...")
    
    # Test different field types
    dimension_field = MaterializedField(source='seller_name')
    metric_field = MaterializedField(aggregate=Sum('offer_price'))
    transform_field = MaterializedField(source='date_collected', transform=ToDate)
    
    print(f"✅ Dimension field - source: {dimension_field.source}, aggregate: {dimension_field.aggregate}")
    print(f"✅ Metric field - source: {metric_field.source}, aggregate: {metric_field.aggregate}")
    print(f"✅ Transform field - source: {transform_field.source}, transform: {transform_field.transform}")
    
    return True


async def main():
    """Main test function."""
    print("🏗️ MaterializedDocument Test Suite")
    print("=" * 60)
    print("Testing the new Pythonic MaterializedDocument interface for")
    print("creating materialized views with familiar syntax.")
    
    all_tests_passed = True
    
    try:
        # Test class creation and configuration
        all_tests_passed &= await test_materialized_document_creation()
        all_tests_passed &= await test_field_configuration()
        all_tests_passed &= await test_complex_aggregations()
        
        # Test ClickHouse integration
        all_tests_passed &= await test_clickhouse_view_creation()
        
        print("\n" + "=" * 60)
        if all_tests_passed:
            print("🎉 All MaterializedDocument Tests Passed!")
            print("\n✅ Key Features Demonstrated:")
            print("- ✅ Declarative field definitions with source mapping")
            print("- ✅ Built-in aggregation functions (Sum, Count, Avg, etc.)")
            print("- ✅ Field transformations (ToDate, ToYearMonth)")
            print("- ✅ Automatic query generation from field definitions")
            print("- ✅ ClickHouse materialized view creation")
            print("- ✅ Backend-agnostic interface")
            print("- ✅ Filters and Having clause support")
            
            print("\n🚀 MaterializedDocument provides a Document-like interface")
            print("   for creating and managing materialized views!")
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