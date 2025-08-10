#!/usr/bin/env python3
"""
Debug script to test individual performance features.
"""

import asyncio
import sys
import os

# Add the src directory to the path so we can import quantumengine
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from quantumengine import Document, create_connection
from quantumengine.fields import StringField, IntField
from surrealdb import RecordID


class User(Document):
    """Test user document for performance testing."""
    name = StringField(required=True)
    age = IntField()
    
    class Meta:
        collection_name = "user"


async def debug_direct_access():
    """Debug direct record access queries."""
    print("🔍 Debugging direct record access...")
    
    # Create connection using modern connection pooling
    async with create_connection(
        url="ws://localhost:8000/rpc",
        namespace="test_ns",
        database="test_db", 
        username="root",
        password="root",
        make_default=True,
        auto_connect=True
    ) as connection:
        # Create test users and collect their actual IDs
        print("Creating test users...")
        created_users = []
        for i in range(1, 6):
            user = User(name=f"user_{i}", age=20 + i)
            await user.save()
            created_users.append(user)
            print(f"  Created {user.name} with ID: {user.id}")
        
        # Get the actual IDs that were created
        actual_ids = [str(user.id) for user in created_users[:3]]
        print(f"  Actual IDs to test: {actual_ids}")
        
        # Test 1: Basic select all
        print("\n1. Basic select all:")
        all_users = await User.objects.all()
        print(f"   Found {len(all_users)} users: {[u.name for u in all_users]}")
        
        # Test 2: Test raw query for direct access with actual IDs
        print("\n2. Testing raw direct access query:")
        raw_query = f"SELECT * FROM {', '.join(actual_ids)}"
        print(f"   Query: {raw_query}")
        try:
            result = await connection.client.query(raw_query)
            print(f"   Result: {result}")
            if result and result[0]:
                print(f"   Found {len(result[0])} records")
                for record in result[0]:
                    print(f"     {record}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 3: Test id__in optimization with actual IDs
        print("\n3. Testing id__in optimization:")
        queryset = User.objects.filter(id__in=actual_ids)
        print(f"   Query: {queryset.get_raw_query()}")
        try:
            users = await queryset.all()
            print(f"   Found {len(users)} users: {[u.name for u in users]}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 4: Test get_many with actual IDs
        print("\n4. Testing get_many:")
        queryset = User.objects.get_many(actual_ids)
        print(f"   Query: {queryset.get_raw_query()}")
        try:
            users = await queryset.all()
            print(f"   Found {len(users)} users: {[u.name for u in users]}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 5: Test basic WHERE query for comparison
        print("\n5. Testing basic WHERE query:")
        queryset = User.objects.filter(age=21)
        print(f"   Query: {queryset.get_raw_query()}")
        try:
            users = await queryset.all()
            print(f"   Found {len(users)} users: {[u.name for u in users]}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 6: Test range syntax (if supported)
        print("\n6. Testing range syntax:")
        raw_range_query = "SELECT * FROM user:1..=3"
        print(f"   Query: {raw_range_query}")
        try:
            result = await connection.client.query(raw_range_query)
            print(f"   Result: {result}")
            if result and result[0]:
                print(f"   Found {len(result[0])} records")
        except Exception as e:
            print(f"   Error: {e}")
            
        print("Connection automatically managed by async context manager")


if __name__ == "__main__":
    asyncio.run(debug_direct_access())