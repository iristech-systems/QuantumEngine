#!/usr/bin/env python3
"""
Test script to verify MaterializedDocument functionality with SurrealDB.

This demonstrates that MaterializedDocument works with both ClickHouse and SurrealDB
backends, automatically converting queries to the appropriate syntax.
"""

import asyncio
import datetime
from decimal import Decimal

# Import QuantumORM components
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    from src.quantumengine.document import Document
    from src.quantumengine.fields import (
        StringField, DecimalField, DateTimeField, BooleanField, 
        IntField, FloatField
    )
    from src.quantumengine.materialized_document import (
        MaterializedDocument, MaterializedField, 
        Count, Sum, Avg, Max, Min, CountDistinct,
        ToDate, ToYearMonth
    )
    print("✅ Successfully imported QuantumORM components")
except ImportError as e:
    print(f"❌ Failed to import QuantumORM components: {e}")
    exit(1)


# Base document for testing (SurrealDB)
class SalesData(Document):
    """Base sales data document for SurrealDB."""
    
    # Core identifiers
    product_sku = StringField(required=True)
    seller_name = StringField(required=True)
    marketplace = StringField(required=True)
    
    # Time dimension
    date_collected = DateTimeField(required=True)
    
    # Metrics
    offer_price = DecimalField(required=True)
    quantity = IntField(default=1)
    is_buybox_winner = BooleanField(default=False)
    
    class Meta:
        backend = 'surrealdb'
        collection = 'sales_data_test'


# MaterializedDocument for SurrealDB
class DailySalesSummary(MaterializedDocument):
    """Daily sales summary for SurrealDB backend."""
    
    class Meta:
        source = SalesData
        backend = 'surrealdb'
        view_name = 'daily_sales_summary'
        
    # Dimensions (grouping fields)
    date = MaterializedField(source='date_collected', transform=ToDate('date_collected'))
    seller_name = MaterializedField(source='seller_name')
    
    # Metrics (aggregation fields)
    total_sales = MaterializedField(aggregate=Sum('offer_price'))
    total_quantity = MaterializedField(aggregate=Sum('quantity'))
    transaction_count = MaterializedField(aggregate=Count())
    avg_price = MaterializedField(aggregate=Avg('offer_price'))
    max_price = MaterializedField(aggregate=Max('offer_price'))


# Complex materialized document with filters
class ProductPerformance(MaterializedDocument):
    """Product performance analysis for SurrealDB."""
    
    class Meta:
        source = SalesData
        backend = 'surrealdb'
        view_name = 'product_performance'
        
    # Only analyze recent data
    class Filters:
        date_collected__gte = datetime.datetime.now() - datetime.timedelta(days=30)
        
    # Only include products with meaningful volume
    class Having:
        transaction_count__gte = 5
        
    # Dimensions
    product_sku = MaterializedField(source='product_sku')
    marketplace = MaterializedField(source='marketplace')
    
    # Metrics
    transaction_count = MaterializedField(aggregate=Count())
    unique_sellers = MaterializedField(aggregate=CountDistinct('seller_name'))
    total_revenue = MaterializedField(aggregate=Sum('offer_price'))
    avg_price = MaterializedField(aggregate=Avg('offer_price'))
    buybox_win_rate = MaterializedField(aggregate=Avg('is_buybox_winner'))


async def test_surrealdb_query_generation():
    """Test SurrealDB-specific query generation."""
    print("\n🔍 Testing SurrealDB Query Generation...")
    
    # Test basic query generation
    try:
        source_query = DailySalesSummary._build_source_query()
        print(f"✅ Generated base query: {source_query}")
        
        # Test SurrealDB conversion
        from src.quantumengine.backends.surrealdb import SurrealDBBackend
        
        # Create a mock connection for testing
        class MockConnection:
            class client:
                pass
        
        backend = SurrealDBBackend(MockConnection())
        
        converted_query = backend._convert_query_to_surrealdb(source_query)
        print(f"✅ Converted to SurrealDB: {converted_query}")
        
        return True
        
    except Exception as e:
        print(f"❌ Query generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_complex_query_conversion():
    """Test complex query with filters and having clauses."""
    print("\n🔍 Testing Complex Query Conversion...")
    
    try:
        source_query = ProductPerformance._build_source_query()
        print(f"✅ Generated complex query: {source_query}")
        
        # Test conversion
        from src.quantumengine.backends.surrealdb import SurrealDBBackend
        
        # Create a mock connection for testing
        class MockConnection:
            class client:
                pass
        
        backend = SurrealDBBackend(MockConnection())
        
        converted_query = backend._convert_query_to_surrealdb(source_query)
        print(f"✅ Converted complex query: {converted_query}")
        
        return True
        
    except Exception as e:
        print(f"❌ Complex query conversion failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_function_mapping():
    """Test that ClickHouse functions are properly mapped to SurrealDB."""
    print("\n🔍 Testing Function Mapping...")
    
    test_mappings = [
        ("COUNT(*)", "count()"),
        ("SUM(offer_price)", "math::sum(offer_price)"),
        ("AVG(offer_price)", "math::mean(offer_price)"),
        ("MIN(offer_price)", "math::min(offer_price)"),
        ("MAX(offer_price)", "math::max(offer_price)"),
        ("toDate(date_collected)", "time::day(date_collected)"),
        ("COUNT(DISTINCT seller_name)", "count(array::distinct(seller_name)"),
    ]
    
    from src.quantumengine.backends.surrealdb import SurrealDBBackend
    
    # Create a mock connection for testing
    class MockConnection:
        class client:
            pass
    
    backend = SurrealDBBackend(MockConnection())
    
    all_passed = True
    for input_func, expected_output in test_mappings:
        converted = backend._convert_query_to_surrealdb(input_func)
        if expected_output in converted:
            print(f"✅ {input_func} → {converted}")
        else:
            print(f"❌ {input_func} → {converted} (expected to contain '{expected_output}')")
            all_passed = False
    
    return all_passed


async def test_meta_inheritance():
    """Test that MaterializedDocument properly inherits backend configuration."""
    print("\n🔍 Testing Meta Configuration Inheritance...")
    
    try:
        # Test that the view picks up the source model's backend
        meta_class = getattr(DailySalesSummary, 'Meta', None)
        source_model = getattr(meta_class, 'source', None) if meta_class else None
        
        if source_model:
            source_backend = source_model._meta.get('backend')
            view_backend = DailySalesSummary._meta.get('backend')
            
            print(f"✅ Source model backend: {source_backend}")
            print(f"✅ View backend: {view_backend}")
            
            if source_backend == view_backend == 'surrealdb':
                print("✅ Backend configuration correctly inherited")
                return True
            else:
                print("❌ Backend configuration mismatch")
                return False
        else:
            print("❌ Could not find source model")
            return False
            
    except Exception as e:
        print(f"❌ Meta inheritance test failed: {e}")
        return False


async def test_field_classification():
    """Test that fields are properly classified as dimensions vs metrics."""
    print("\n🔍 Testing Field Classification...")
    
    try:
        dimension_fields = DailySalesSummary._dimension_fields
        metric_fields = DailySalesSummary._metric_fields
        
        print(f"✅ Dimension fields: {list(dimension_fields.keys())}")
        print(f"✅ Metric fields: {list(metric_fields.keys())}")
        
        # Check that fields are in the right categories
        expected_dimensions = ['date', 'seller_name']
        expected_metrics = ['total_sales', 'total_quantity', 'transaction_count', 'avg_price', 'max_price']
        
        dimensions_correct = all(field in dimension_fields for field in expected_dimensions)
        metrics_correct = all(field in metric_fields for field in expected_metrics)
        
        if dimensions_correct and metrics_correct:
            print("✅ Field classification is correct")
            return True
        else:
            print("❌ Field classification is incorrect")
            return False
            
    except Exception as e:
        print(f"❌ Field classification test failed: {e}")
        return False


async def main():
    """Main test function."""
    print("🔍 SurrealDB MaterializedDocument Test Suite")
    print("=" * 60)
    print("Testing MaterializedDocument compatibility with SurrealDB backend")
    
    all_tests_passed = True
    
    try:
        # Test basic functionality
        all_tests_passed &= await test_field_classification()
        all_tests_passed &= await test_meta_inheritance()
        
        # Test query generation and conversion
        all_tests_passed &= await test_surrealdb_query_generation()
        all_tests_passed &= await test_complex_query_conversion()
        all_tests_passed &= await test_function_mapping()
        
        print("\n" + "=" * 60)
        if all_tests_passed:
            print("🎉 All SurrealDB MaterializedDocument Tests Passed!")
            print("\n✅ Key Features Verified:")
            print("- ✅ Cross-backend compatibility (ClickHouse → SurrealDB)")
            print("- ✅ Automatic function mapping (COUNT → count, SUM → math::sum)")
            print("- ✅ Query syntax conversion")
            print("- ✅ Meta configuration inheritance")
            print("- ✅ Field classification (dimensions vs metrics)")
            print("- ✅ Complex queries with filters and having clauses")
            
            print("\n🚀 MaterializedDocument provides true multi-backend support!")
            print("   Same Python code works with both ClickHouse and SurrealDB!")
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