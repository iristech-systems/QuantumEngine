"""Example demonstrating the ClickHouse backend for SurrealEngine.

This example shows how to:
1. Connect to ClickHouse using clickhouse-connect
2. Define documents with ClickHouse as the backend
3. Perform CRUD operations
4. Work with time-series data (common ClickHouse use case)
"""

import asyncio
from datetime import datetime, timedelta, timezone
import clickhouse_connect

# Note: This example assumes the Document class and other components
# have been updated to support the backend abstraction.
# For now, this demonstrates the intended usage.

# Example usage (will work once Document class is updated):
"""
# 1. Create a ClickHouse connection
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='',
    database='default'
)

# 2. Register the connection with SurrealEngine
from quantumorm import ConnectionRegistry
ConnectionRegistry.register('clickhouse_main', client, 'clickhouse')
ConnectionRegistry.set_default('clickhouse', 'clickhouse_main')

# 3. Define a document for time-series data
from quantumorm import Document
from quantumorm.fields import StringField, FloatField, DateTimeField, IntField

class PriceMonitoring(Document):
    seller_name = StringField(required=True)
    product_id = StringField(required=True)
    offer_price = FloatField(required=True)
    availability = IntField(default=0)  # 0=out of stock, 1=in stock
    date_collected = DateTimeField(default=datetime.utcnow)
    
    class Meta:
        table_name = 'price_monitoring'
        backend = 'clickhouse'  # Use ClickHouse backend
        # ClickHouse-specific options
        engine = 'MergeTree'
        order_by = 'date_collected, seller_name'
        partition_by = 'toYYYYMM(date_collected)'

# 4. Create the table
async def setup_table():
    await PriceMonitoring.create_table()

# 5. Insert time-series data
async def insert_price_data():
    # Single insert
    price = await PriceMonitoring.objects.create(
        seller_name="Amazon",
        product_id="B08N5WRWNW",
        offer_price=999.99,
        availability=1
    )
    
    # Bulk insert (efficient for ClickHouse)
    prices = []
    base_time = datetime.utcnow()
    
    for i in range(100):
        for seller in ["Amazon", "BestBuy", "Walmart"]:
            prices.append({
                "seller_name": seller,
                "product_id": "B08N5WRWNW",
                "offer_price": 999.99 + (i % 10) * 10,
                "availability": 1 if i % 3 == 0 else 0,
                "date_collected": base_time - timedelta(hours=i)
            })
    
    await PriceMonitoring.objects.bulk_create(prices)

# 6. Query time-series data
async def analyze_prices():
    # Get latest prices by seller
    latest_prices = await PriceMonitoring.objects.filter(
        product_id="B08N5WRWNW"
    ).order_by('-date_collected').limit(10).all()
    
    # Get prices in a time range
    start_date = datetime.utcnow() - timedelta(days=7)
    weekly_prices = await PriceMonitoring.objects.filter(
        date_collected__gte=start_date,
        seller_name="Amazon"
    ).all()
    
    # Count availability by seller
    availability_count = await PriceMonitoring.objects.filter(
        availability=1
    ).count()
    
    # Raw query for complex analytics
    result = await PriceMonitoring.objects.raw(
        '''
        SELECT 
            seller_name,
            avg(offer_price) as avg_price,
            min(offer_price) as min_price,
            max(offer_price) as max_price,
            count() as data_points
        FROM price_monitoring
        WHERE product_id = :product_id
            AND date_collected >= :start_date
        GROUP BY seller_name
        ORDER BY avg_price
        ''',
        params={
            'product_id': 'B08N5WRWNW',
            'start_date': start_date
        }
    )

# 7. Example with another document type
class WebsiteMetrics(Document):
    domain = StringField(required=True)
    page_views = IntField(default=0)
    unique_visitors = IntField(default=0)
    avg_load_time = FloatField()  # in seconds
    timestamp = DateTimeField(default=datetime.utcnow)
    
    class Meta:
        table_name = 'website_metrics'
        backend = 'clickhouse'
        engine = 'SummingMergeTree'  # Automatically sum metrics
        order_by = 'domain, timestamp'
        partition_by = 'toYYYYMM(timestamp)'

# Main execution
async def main():
    # Setup
    await setup_table()
    
    # Insert data
    await insert_price_data()
    
    # Analyze
    await analyze_prices()
    
    print("ClickHouse backend example completed!")

if __name__ == "__main__":
    asyncio.run(main())
"""

# Actual working example with current backend implementation
import sys
import os
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from quantumorm import create_connection

async def test_clickhouse_backend():
    """Test the ClickHouse backend using enhanced create_connection."""
    
    # Import the backend
    from quantumorm.backends.clickhouse import ClickHouseBackend
    from quantumorm.connection import ConnectionRegistry
    
    # Create ClickHouse connection using enhanced create_connection
    connection = create_connection(
        name='clickhouse_test',
        url='localhost',
        username='default',
        password='',
        backend='clickhouse',
        make_default=True,
        port=8123,
        secure=False
    )
    
    # Get the registered client
    client = ConnectionRegistry.get_connection('clickhouse_test')
    
    # Create backend instance
    backend = ClickHouseBackend(client)
    
    # Test basic operations
    table_name = 'test_price_monitoring'
    
    # 1. Create a simple table (manual for now)
    create_query = """
    CREATE TABLE IF NOT EXISTS test_price_monitoring (
        id String,
        seller_name String,
        offer_price Float64,
        date_collected DateTime64(3)
    ) ENGINE = MergeTree()
    ORDER BY (date_collected, seller_name)
    """
    
    try:
        await backend._execute(create_query)
        print("✓ Table created successfully")
    except Exception as e:
        print(f"Table creation error: {e}")
    
    # 2. Test insert
    test_data = {
        'id': '123',
        'seller_name': 'Amazon',
        'offer_price': 99.99,
        'date_collected': datetime.now(timezone.utc)
    }
    
    try:
        result = await backend.insert(table_name, test_data)
        print(f"✓ Inserted: {result}")
    except Exception as e:
        print(f"Insert error: {e}")
    
    # 3. Test select
    try:
        conditions = [backend.build_condition('seller_name', '=', 'Amazon')]
        results = await backend.select(table_name, conditions)
        print(f"✓ Selected {len(results)} records")
        for r in results:
            print(f"  - {r}")
    except Exception as e:
        print(f"Select error: {e}")
    
    # 4. Test bulk insert
    bulk_data = []
    for i in range(5):
        bulk_data.append({
            'id': f'bulk_{i}',
            'seller_name': f'Seller{i % 2}',
            'offer_price': 50.0 + i * 10,
            'date_collected': datetime.now(timezone.utc)
        })
    
    try:
        await backend.insert_many(table_name, bulk_data)
        print(f"✓ Bulk inserted {len(bulk_data)} records")
    except Exception as e:
        print(f"Bulk insert error: {e}")
    
    # 5. Test count
    try:
        count = await backend.count(table_name, [])
        print(f"✓ Total records: {count}")
    except Exception as e:
        print(f"Count error: {e}")
    
    # Clean up
    try:
        await backend._execute(f"DROP TABLE IF EXISTS {table_name}")
        print("✓ Cleaned up test table")
    except Exception as e:
        print(f"Cleanup error: {e}")

if __name__ == "__main__":
    print("Testing ClickHouse Backend Implementation with Enhanced API")
    print("=" * 60)
    
    # Test enhanced create_connection API
    try:
        print("✅ Testing enhanced create_connection with backend parameter")
        asyncio.run(test_clickhouse_backend())
    except Exception as e:
        print(f"❌ Could not connect to ClickHouse: {e}")