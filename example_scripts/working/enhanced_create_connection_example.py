#!/usr/bin/env python3

"""Enhanced create_connection API demonstration.

This example demonstrates the new multi-backend support in create_connection,
showing how to easily set up different database backends with a unified API.
"""

import asyncio
import sys
import os
from datetime import datetime, timezone
from decimal import Decimal

# Add src to path for local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from quantumorm import Document, StringField, IntField, FloatField, DateTimeField, create_connection
from quantumorm.fields.specialized import DecimalField
from quantumorm.connection import ConnectionRegistry

# === DOCUMENT MODELS ===

class SurrealDBUser(Document):
    """User model for SurrealDB backend."""
    
    username = StringField(required=True)
    email = StringField(required=True)
    balance = DecimalField()
    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    
    class Meta:
        collection = 'users'
        backend = 'surrealdb'

class ClickHouseEvent(Document):
    """Event model for ClickHouse backend."""
    
    event_name = StringField(required=True)
    user_id = StringField()
    value = DecimalField()
    timestamp = DateTimeField(required=True, default=lambda: datetime.now(timezone.utc))
    
    class Meta:
        collection = 'events'
        backend = 'clickhouse'

# === CONNECTION EXAMPLES ===

async def demonstrate_surrealdb_connection():
    """Demonstrate SurrealDB connection with enhanced create_connection."""
    print("🔌 SurrealDB Connection Example")
    print("-" * 40)
    
    try:
        # Enhanced create_connection for SurrealDB
        connection = create_connection(
            name='surrealdb_demo',
            url='ws://localhost:8000/rpc',
            username='root',
            password='root',
            namespace='demo_ns',
            database='demo_db',
            backend='surrealdb',  # Explicit backend specification
            make_default=True
        )
        
        # Connect (this would work with a real SurrealDB instance)
        print("✅ SurrealDB connection created successfully")
        print(f"   Connection registered as: 'surrealdb_demo'")
        print(f"   Set as default for backend: 'surrealdb'")
        
        # The connection is automatically registered and set as default
        # You can now use SurrealDBUser documents normally
        
        # Demo user operations (would work with real connection)
        user = SurrealDBUser(
            username="demo_user",
            email="demo@example.com",
            balance=Decimal("1000.50")
        )
        
        print(f"✅ Created user: {user.username}")
        print(f"   Backend: {user._get_backend().__class__.__name__}")
        print(f"   Serialized data: {user.to_db()}")
        
    except Exception as e:
        print(f"ℹ️  Connection demo (would work with real SurrealDB): {e}")

async def demonstrate_clickhouse_connection():
    """Demonstrate ClickHouse connection with enhanced create_connection."""
    print("\n🔌 ClickHouse Connection Example")
    print("-" * 40)
    
    try:
        # Enhanced create_connection for ClickHouse
        connection = create_connection(
            name='clickhouse_demo',
            url='localhost',  # Can use hostname or full URL
            username='default',
            password='',
            database='default',
            backend='clickhouse',  # Explicit backend specification
            make_default=True,
            # ClickHouse-specific parameters
            port=8123,
            secure=False
        )
        
        print("✅ ClickHouse connection created successfully")
        print(f"   Connection registered as: 'clickhouse_demo'")
        print(f"   Set as default for backend: 'clickhouse'")
        
        # Demo event operations
        event = ClickHouseEvent(
            event_name="user_signup",
            user_id="demo_user",
            value=Decimal("99.99")
        )
        
        print(f"✅ Created event: {event.event_name}")
        print(f"   Backend: {event._get_backend().__class__.__name__}")
        print(f"   Serialized data: {event.to_db()}")
        
    except Exception as e:
        print(f"ℹ️  Connection demo (would work with real ClickHouse): {e}")

async def demonstrate_multiple_connections():
    """Demonstrate managing multiple connections for different backends."""
    print("\n🔗 Multiple Connections Example")
    print("-" * 40)
    
    # Set up multiple connections
    connections = []
    
    try:
        # Primary SurrealDB connection
        surrealdb_primary = create_connection(
            name='surrealdb_primary',
            url='ws://production-surrealdb:8000/rpc',
            username='app_user',
            password='secure_password',
            namespace='production',
            database='main',
            backend='surrealdb'
        )
        connections.append(('SurrealDB Primary', surrealdb_primary))
        
        # Analytics ClickHouse connection
        clickhouse_analytics = create_connection(
            name='clickhouse_analytics',
            url='analytics-clickhouse.company.com',
            username='analytics_user',
            password='analytics_password',
            database='analytics',
            backend='clickhouse',
            port=8123,
            secure=False
        )
        connections.append(('ClickHouse Analytics', clickhouse_analytics))
        
        # Development SurrealDB connection
        surrealdb_dev = create_connection(
            name='surrealdb_dev',
            url='ws://localhost:8000/rpc',
            username='root',
            password='root',
            namespace='dev',
            database='testing',
            backend='surrealdb'
        )
        connections.append(('SurrealDB Dev', surrealdb_dev))
        
        # Show all registered connections
        print("✅ Multiple connections created:")
        for name, conn in connections:
            print(f"   - {name}: {type(conn).__name__}")
        
        # Show connection registry state
        print(f"\n📋 Connection Registry Status:")
        print(f"   SurrealDB connections: {len([c for name, c in connections if 'surrealdb' in name.lower()])}")
        print(f"   ClickHouse connections: {len([c for name, c in connections if 'clickhouse' in name.lower()])}")
        
    except Exception as e:
        print(f"ℹ️  Multiple connections demo: {e}")

def demonstrate_backend_detection():
    """Demonstrate automatic backend detection from document models."""
    print("\n🎯 Backend Detection Example")
    print("-" * 40)
    
    # Create document instances
    user = SurrealDBUser(username="test")
    event = ClickHouseEvent(event_name="test")
    
    # Show backend detection
    user_backend = user._get_backend()
    event_backend = event._get_backend()
    
    print(f"✅ Document backend detection:")
    print(f"   SurrealDBUser → {user_backend.__class__.__name__}")
    print(f"   ClickHouseEvent → {event_backend.__class__.__name__}")
    
    # Show backend capabilities
    print(f"\n🔧 Backend Capabilities:")
    print(f"   SurrealDB supports graph relations: {user_backend.supports_graph_relations()}")
    print(f"   ClickHouse supports graph relations: {event_backend.supports_graph_relations()}")
    print(f"   SurrealDB supports direct record access: {user_backend.supports_direct_record_access()}")
    print(f"   ClickHouse supports direct record access: {event_backend.supports_direct_record_access()}")

def demonstrate_field_serialization():
    """Demonstrate backend-specific field serialization."""
    print("\n🔄 Field Serialization Example")
    print("-" * 40)
    
    # Create instances with same data
    timestamp = datetime.now(timezone.utc)
    decimal_value = Decimal("123.45")
    
    user = SurrealDBUser(
        username="serialization_test",
        email="test@example.com",
        balance=decimal_value,
        created_at=timestamp
    )
    
    event = ClickHouseEvent(
        event_name="serialization_test",
        user_id="test_user",
        value=decimal_value,
        timestamp=timestamp
    )
    
    # Serialize to database format
    user_data = user.to_db()
    event_data = event.to_db()
    
    print("✅ Backend-specific serialization:")
    print(f"   SurrealDB DateTime: {user_data['created_at']} (ISO format)")
    print(f"   ClickHouse DateTime: {event_data['timestamp']} (Standard format)")
    print(f"   SurrealDB Decimal: {user_data['balance']} ({type(user_data['balance']).__name__})")
    print(f"   ClickHouse Decimal: {event_data['value']} ({type(event_data['value']).__name__})")

# === MAIN DEMONSTRATION ===

async def main():
    """Run all enhanced create_connection demonstrations."""
    print("🚀 Enhanced create_connection API Demonstration")
    print("=" * 60)
    
    # Run all demonstrations
    await demonstrate_surrealdb_connection()
    await demonstrate_clickhouse_connection()
    await demonstrate_multiple_connections()
    demonstrate_backend_detection()
    demonstrate_field_serialization()
    
    print("\n" + "=" * 60)
    print("🎉 Enhanced create_connection API Demo Complete!")
    print("\n✨ Key Features Demonstrated:")
    print("  • Unified create_connection API for all backends")
    print("  • Automatic connection registration and management")
    print("  • Backend-specific parameter handling")
    print("  • Multiple connections per backend type")
    print("  • Automatic backend detection from document models")
    print("  • Backend-specific field serialization")
    print("  • Seamless switching between backends")

if __name__ == "__main__":
    asyncio.run(main())