#!/usr/bin/env python3
"""
Debug SurrealDB MaterializedDocument step by step.
"""

import asyncio
import datetime
from decimal import Decimal

# Import QuantumORM components
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    from src.quantumengine.connection import create_connection
    from src.quantumengine.backends.surrealdb import SurrealDBBackend
    from src.quantumengine.document import Document
    from src.quantumengine.fields import StringField, DecimalField, DateTimeField, BooleanField, IntField
    from src.quantumengine.materialized_document import (
        MaterializedDocument, MaterializedField, 
        Count, Sum, Avg, Max, Min, CountDistinct, ToDate
    )
    from src.quantumengine.connection import ConnectionRegistry
    print("✅ Successfully imported QuantumORM components")
except ImportError as e:
    print(f"❌ Failed to import QuantumORM components: {e}")
    exit(1)


# Simple base document
class SalesData(Document):
    product_sku = StringField(required=True)
    seller_name = StringField(required=True)
    offer_price = DecimalField(required=True)
    date_collected = DateTimeField(required=True)
    
    class Meta:
        backend = 'surrealdb'
        collection = 'sales_data_debug'


# Simple materialized document
class DailySalesSummary(MaterializedDocument):
    class Meta:
        source = SalesData
        backend = 'surrealdb'
        view_name = 'daily_sales_summary_debug'
        
    date = MaterializedField(source='date_collected', transform=ToDate('date_collected'))
    seller_name = MaterializedField(source='seller_name')
    total_sales = MaterializedField(aggregate=Sum('offer_price'))
    transaction_count = MaterializedField(aggregate=Count())


async def test_debug():
    print("🚀 SurrealDB Debug Test")
    print("=" * 50)
    
    try:
        # Step 1: Test connection
        print("Step 1: Testing connection...")
        connection = create_connection(
            url="ws://localhost:8000/rpc",
            namespace="test_ns",
            database="test_db",
            username="root",
            password="root",
            make_default=True,
            async_mode=True,
        )
        await connection.connect()
        backend = SurrealDBBackend(connection)
        print("✅ Connected successfully")
        
        # Step 2: Test query generation
        print("\nStep 2: Testing query generation...")
        source_query = DailySalesSummary._build_source_query()
        print(f"Generated query: {source_query}")
        
        # Step 3: Test query conversion
        print("\nStep 3: Testing query conversion...")
        converted_query = backend._convert_query_to_surrealdb(source_query)
        print(f"Converted query: {converted_query}")
        
        # Step 4: Test base table creation
        print("\nStep 4: Testing base table creation...")
        await backend.create_table(SalesData)
        print("✅ Base table created")
        
        # Step 5: Test simple data insertion
        print("\nStep 5: Testing data insertion...")
        test_data = {
            'product_sku': 'SKU-001',
            'seller_name': 'TestSeller',
            'offer_price': Decimal('99.99'),
            'date_collected': datetime.datetime.now()
        }
        await backend.insert('sales_data_debug', test_data)
        print("✅ Data inserted")
        
        # Step 6: Test materialized view creation (the problematic step)
        print("\nStep 6: Testing materialized view creation...")
        print(f"Will create view with query: DEFINE TABLE daily_sales_summary_debug AS {converted_query}")
        
        # Try creating the view manually to see the exact error
        try:
            view_query = f"DEFINE TABLE daily_sales_summary_debug AS {converted_query}"
            await backend._execute(view_query)
            print("✅ Materialized view created successfully")
        except Exception as view_error:
            print(f"❌ Materialized view creation failed: {view_error}")
            return False
        
        # Step 7: Test querying the view
        print("\nStep 7: Testing view query...")
        try:
            result = await backend._query("SELECT * FROM daily_sales_summary_debug")
            print(f"✅ View query result: {result}")
        except Exception as query_error:
            print(f"❌ View query failed: {query_error}")
        
        print("\n🎉 Debug test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Debug test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    result = asyncio.run(test_debug())
    exit(0 if result else 1)