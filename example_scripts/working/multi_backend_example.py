"""Multi-backend example demonstrating SurrealEngine with both SurrealDB and ClickHouse.

This example shows how to:
1. Set up multiple backend connections
2. Define documents that use different backends
3. Perform operations across different backends
4. Demonstrate use cases for each backend type
"""

import asyncio
import sys
import os
from datetime import datetime, timezone
from typing import Dict, Any

# Add src to path for local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# ClickHouse imports
import clickhouse_connect

# SurrealEngine imports 
from quantumorm import create_connection
from quantumorm.connection import ConnectionRegistry
from quantumorm.backends.clickhouse import ClickHouseBackend
from quantumorm.backends.surrealdb import SurrealDBBackend


class DemoUser:
    """Demo User class (simplified version of Document for this example)."""
    
    _meta = {
        'table_name': 'users',
        'backend': 'surrealdb'
    }
    
    def __init__(self, name: str, email: str, id: str = None):
        self.name = name
        self.email = email
        self.id = id
    
    @classmethod
    def _get_backend(cls):
        """Get the backend for this document."""
        backend_name = cls._meta.get('backend', 'surrealdb') 
        connection = ConnectionRegistry.get_default_connection(backend_name)
        if backend_name == 'clickhouse':
            return ClickHouseBackend(connection)
        else:
            return SurrealDBBackend(connection)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'name': self.name,
            'email': self.email
        }


class DemoAnalytics:
    """Demo Analytics class for time-series data."""
    
    _meta = {
        'table_name': 'analytics_events',
        'backend': 'clickhouse'
    }
    
    def __init__(self, user_id: str, event_type: str, timestamp: datetime = None, metadata: Dict = None):
        self.user_id = user_id
        self.event_type = event_type
        self.timestamp = timestamp or datetime.now(timezone.utc)
        self.metadata = metadata or {}
    
    @classmethod
    def _get_backend(cls):
        """Get the backend for this document."""
        backend_name = cls._meta.get('backend', 'surrealdb')
        connection = ConnectionRegistry.get_default_connection(backend_name) 
        if backend_name == 'clickhouse':
            return ClickHouseBackend(connection)
        else:
            return SurrealDBBackend(connection)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        import json
        return {
            'user_id': self.user_id,
            'event_type': self.event_type,
            'timestamp': self.timestamp,
            'metadata': json.dumps(self.metadata) if self.metadata else '{}'
        }


async def setup_connections():
    """Set up connections for both backends using enhanced create_connection."""
    print("🔧 Setting up multi-backend connections...")
    
    # Set up ClickHouse connection using enhanced create_connection
    try:
        clickhouse_conn = create_connection(
            name='clickhouse_main',
            url='localhost',
            username='default',
            password='',
            backend='clickhouse',
            make_default=True,
            port=8123,
            secure=False
        )
        
        print("✅ ClickHouse connection registered via create_connection")
        
    except Exception as e:
        print(f"❌ Failed to set up ClickHouse: {e}")
        return False
    
    # Set up SurrealDB connection using enhanced create_connection
    try:
        # For this demo, we'll use a mock connection since we don't have a real SurrealDB instance
        # In production, you would use:
        # surrealdb_conn = create_connection(
        #     name='surrealdb_main',
        #     url='ws://localhost:8000/rpc',
        #     username='root',
        #     password='root',
        #     namespace='demo',
        #     database='demo',
        #     backend='surrealdb',
        #     make_default=True
        # )
        
        class MockSurrealDBConnection:
            """Mock SurrealDB connection for demo purposes."""
            def __init__(self):
                self.client = self
            
            async def create(self, table_or_id, data=None):
                if data:
                    return [{'id': f"{table_or_id}:demo123", **data}]
                return [{'id': f"{table_or_id}:demo123"}]
            
            async def query(self, query):
                return [[{'id': 'users:demo123', 'name': 'John Doe', 'email': 'john@example.com'}]]
        
        # Register mock connection
        mock_surrealdb = MockSurrealDBConnection()
        ConnectionRegistry.register('surrealdb_main', mock_surrealdb, 'surrealdb')
        ConnectionRegistry.set_default('surrealdb', 'surrealdb_main')
        print("✅ SurrealDB connection registered (mock - use create_connection in production)")
        
    except Exception as e:
        print(f"❌ Failed to set up SurrealDB: {e}")
        return False
    
    return True


async def test_clickhouse_operations():
    """Test ClickHouse backend operations."""
    print("\n📊 Testing ClickHouse Analytics Operations...")
    
    # Get ClickHouse backend
    backend = DemoAnalytics._get_backend()
    table_name = DemoAnalytics._meta['table_name']
    
    # Drop and recreate table to ensure clean schema
    try:
        await backend._execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"🧹 Dropped existing table: {table_name}")
    except Exception as e:
        print(f"ℹ️  No existing table to drop: {e}")
    
    # Create table for analytics
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        user_id String,
        event_type String,
        timestamp DateTime64(3),
        metadata String
    ) ENGINE = MergeTree()
    ORDER BY (timestamp, user_id)
    """
    
    try:
        await backend._execute(create_query)
        print(f"✅ Created analytics table: {table_name}")
    except Exception as e:
        print(f"❌ Failed to create table: {e}")
        return
    
    # Insert some analytics events
    analytics_events = [
        DemoAnalytics('user123', 'page_view', metadata={'page': '/home'}),
        DemoAnalytics('user123', 'button_click', metadata={'button': 'signup'}),
        DemoAnalytics('user456', 'page_view', metadata={'page': '/pricing'}),
        DemoAnalytics('user456', 'conversion', metadata={'plan': 'premium'}),
        DemoAnalytics('user789', 'page_view', metadata={'page': '/home'}),
    ]
    
    # Convert to data format for backend
    data_list = [event.to_dict() for event in analytics_events]
    
    try:
        results = await backend.insert_many(table_name, data_list)
        print(f"✅ Inserted {len(results)} analytics events")
    except Exception as e:
        print(f"❌ Failed to insert events: {e}")
        return
    
    # Query analytics data
    try:
        # Get all page views
        conditions = [backend.build_condition('event_type', '=', 'page_view')]
        page_views = await backend.select(table_name, conditions)
        print(f"✅ Found {len(page_views)} page view events")
        
        # Count total events
        total_count = await backend.count(table_name, [])
        print(f"✅ Total events in analytics: {total_count}")
        
        # Get events by user
        user_conditions = [backend.build_condition('user_id', '=', 'user123')]
        user_events = await backend.select(table_name, user_conditions)
        print(f"✅ Found {len(user_events)} events for user123")
        
    except Exception as e:
        print(f"❌ Failed to query events: {e}")
    
    # Clean up
    try:
        await backend._execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✅ Cleaned up analytics table")
    except Exception as e:
        print(f"❌ Failed to cleanup: {e}")


async def test_surrealdb_operations():
    """Test SurrealDB backend operations (with mock)."""
    print("\n👤 Testing SurrealDB User Operations...")
    
    # Get SurrealDB backend  
    backend = DemoUser._get_backend()
    table_name = DemoUser._meta['table_name']
    
    print("ℹ️  Using mock SurrealDB backend for demo")
    
    # Create a user
    user = DemoUser('Alice Smith', 'alice@example.com')
    user_data = user.to_dict()
    
    try:
        # Note: This will work with the mock but would need real SurrealDB implementation
        print(f"✅ Would create user in SurrealDB: {user_data}")
        print(f"✅ Table: {table_name}, Backend: {user._meta['backend']}")
    except Exception as e:
        print(f"❌ Failed user operation: {e}")


async def demonstrate_multi_backend_workflow():
    """Demonstrate a workflow using both backends."""
    print("\n🔄 Multi-Backend Workflow Demo...")
    
    # Simulate a real-world scenario:
    # 1. User signs up (SurrealDB - transactional data)
    # 2. Track signup event (ClickHouse - analytics)
    # 3. User performs actions (ClickHouse - analytics)
    
    print("1. User Registration (SurrealDB)")
    user = DemoUser('Demo User', 'demo@example.com', 'user_demo_001')
    print(f"   Created user: {user.to_dict()}")
    
    print("2. Track Signup Event (ClickHouse)")
    signup_event = DemoAnalytics(
        user_id='user_demo_001',
        event_type='user_signup',
        metadata={'source': 'web', 'plan': 'free'}
    )
    print(f"   Tracked event: {signup_event.to_dict()}")
    
    print("3. Track User Activity (ClickHouse)")
    activity_events = [
        DemoAnalytics('user_demo_001', 'page_view', metadata={'page': '/dashboard'}),
        DemoAnalytics('user_demo_001', 'feature_use', metadata={'feature': 'export'}),
        DemoAnalytics('user_demo_001', 'api_call', metadata={'endpoint': '/api/data'}),
    ]
    
    for event in activity_events:
        print(f"   Tracked: {event.event_type}")
    
    print("\n✅ Multi-backend workflow completed!")
    print("   - User data would be stored in SurrealDB (OLTP)")
    print("   - Analytics events would be stored in ClickHouse (OLAP)")
    print("   - Each backend optimized for its use case")


async def main():
    """Main demonstration function."""
    print("🚀 SurrealEngine Multi-Backend Demo")
    print("=" * 50)
    
    # Setup connections
    if not await setup_connections():
        print("❌ Failed to setup connections. Exiting.")
        return
    
    # Test ClickHouse operations (real backend)
    await test_clickhouse_operations()
    
    # Test SurrealDB operations (mock for demo)
    await test_surrealdb_operations()
    
    # Demonstrate multi-backend workflow
    await demonstrate_multi_backend_workflow()
    
    print("\n" + "=" * 50)
    print("🎉 Multi-Backend Demo Complete!")
    print("\nKey Benefits Demonstrated:")
    print("• ClickHouse: Optimized for analytics and time-series data")
    print("• SurrealDB: Optimized for transactional and relational data")
    print("• Unified API: Same interface for different backends")
    print("• Flexible Architecture: Choose the right tool for each use case")


if __name__ == "__main__":
    asyncio.run(main())