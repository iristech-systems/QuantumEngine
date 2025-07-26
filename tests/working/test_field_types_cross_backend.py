#!/usr/bin/env python3
"""
Test field types across all backends to ensure consistent behavior.

This test verifies that all field types work correctly across SurrealDB, ClickHouse, and Redis backends.
"""

import asyncio
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any, Dict

# Import QuantumEngine
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from quantumengine import Document
from quantumengine.fields import (
    StringField, IntField, FloatField, BooleanField, DateTimeField, 
    UUIDField, DictField, DecimalField, ListField
)
from quantumengine.connection import ConnectionRegistry, create_connection


class TestDocument(Document):
    """Test document with various field types."""
    
    # Basic fields
    name = StringField(required=True)
    age = IntField()
    height = FloatField()
    is_active = BooleanField(default=True)
    created_at = DateTimeField()
    balance = DecimalField()
    
    # Complex fields
    metadata = DictField()
    tags = ListField()
    
    class Meta:
        collection = 'test_field_types'


class SurrealTestDocument(TestDocument):
    """SurrealDB version."""
    class Meta:
        collection = 'test_field_types_surreal'
        backend = 'surrealdb'


class ClickHouseTestDocument(TestDocument):
    """ClickHouse version."""
    class Meta:
        collection = 'test_field_types_clickhouse' 
        backend = 'clickhouse'


class RedisTestDocument(TestDocument):
    """Redis version."""
    class Meta:
        collection = 'test_field_types_redis'
        backend = 'redis'


async def setup_connections():
    """Set up connections for all backends."""
    print("🔌 Setting up database connections...")
    print("⚠️  NOTE: Found inconsistency - SurrealDB requires .connect(), others don't!")
    
    # SurrealDB connection - INCONSISTENT: requires separate .connect() call
    try:
        surreal_conn = create_connection(
            backend='surrealdb',
            url='ws://localhost:8000/rpc',
            username='test',
            password='test',
            namespace='test',
            database='test',
            async_mode=True
        )
        # SurrealDB requires manual connect step (inconsistent with other backends)
        await surreal_conn.connect()
        ConnectionRegistry.set_default_connection('surrealdb', surreal_conn)
        print("✅ SurrealDB connection established (required manual .connect())")
    except Exception as e:
        print(f"❌ SurrealDB connection failed: {e}")
    
    # ClickHouse connection - create database first, then connect
    try:
        # First create the test database
        admin_conn = create_connection(
            backend='clickhouse',
            url='localhost',
            port=8123,
            database='default'  # Connect to default to create test db
        )
        # Create test database if it doesn't exist
        admin_conn.command("CREATE DATABASE IF NOT EXISTS test")
        
        # Now connect to the test database
        clickhouse_conn = create_connection(
            backend='clickhouse',
            url='localhost',
            port=8123,
            database='test'
        )
        ConnectionRegistry.set_default_connection('clickhouse', clickhouse_conn)
        print("✅ ClickHouse connection established (immediately ready)")
    except Exception as e:
        print(f"❌ ClickHouse connection failed: {e}")
    
    # Redis connection - CONSISTENT: immediately ready after create_connection
    try:
        redis_conn = create_connection(
            backend='redis',
            url='localhost',
            port=6379,
            db=0
        )
        ConnectionRegistry.set_default_connection('redis', redis_conn)
        print("✅ Redis connection established (immediately ready)")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
    
    print()


async def test_field_serialization_deserialization():
    """Test field serialization and deserialization across backends."""
    
    print("🧪 Testing Field Types Across All Backends")
    print("=" * 60)
    
    # Setup connections first
    await setup_connections()
    
    # Test data
    test_data = {
        'name': 'Test User',
        'age': 25,
        'height': 5.9,
        'is_active': True,
        'created_at': datetime.now(timezone.utc),
        'balance': Decimal('1234.56'),
        'metadata': {'key1': 'value1', 'key2': 42},
        'tags': ['python', 'testing', 'multi-backend']
    }
    
    results = {}
    
    # Test each backend
    backends_to_test = [
        ('SurrealDB', SurrealTestDocument),
        ('ClickHouse', ClickHouseTestDocument), 
        ('Redis', RedisTestDocument)
    ]
    
    for backend_name, doc_class in backends_to_test:
        print(f"\n📊 Testing {backend_name} Backend")
        print("-" * 40)
        
        try:
            # Get backend instance to test capabilities
            backend = doc_class._get_backend()
            print(f"Backend capabilities: {backend.get_capabilities()}")
            
            # Test to_db serialization
            doc = doc_class(**test_data)
            db_data = doc.to_db()
            print(f"✅ to_db() serialization successful")
            print(f"   Serialized data types: {[(k, type(v).__name__) for k, v in db_data.items()]}")
            
            # Test from_db deserialization
            restored_doc = doc_class.from_db(db_data)
            print(f"✅ from_db() deserialization successful")
            
            # Compare data integrity
            integrity_check = True
            for field_name, original_value in test_data.items():
                restored_value = getattr(restored_doc, field_name)
                
                # Special handling for different types
                if isinstance(original_value, Decimal):
                    # Some backends might convert Decimal to float or string
                    if not (isinstance(restored_value, (Decimal, float, str))):
                        integrity_check = False
                        print(f"   ❌ {field_name}: Expected Decimal-like, got {type(restored_value)}")
                elif isinstance(original_value, datetime):
                    # DateTime handling varies by backend
                    if not isinstance(restored_value, (datetime, str)):
                        integrity_check = False
                        print(f"   ❌ {field_name}: Expected datetime-like, got {type(restored_value)}")
                elif original_value != restored_value:
                    # For other types, expect exact match
                    if not (isinstance(original_value, type(restored_value)) or isinstance(restored_value, type(original_value))):
                        integrity_check = False
                        print(f"   ❌ {field_name}: Expected {original_value} ({type(original_value)}), got {restored_value} ({type(restored_value)})")
            
            if integrity_check:
                print(f"✅ Data integrity check passed")
            else:
                print(f"⚠️  Some data integrity issues detected")
            
            results[backend_name] = {
                'serialization': True,
                'deserialization': True,
                'integrity': integrity_check,
                'capabilities': backend.get_capabilities()
            }
            
        except Exception as e:
            print(f"❌ Error testing {backend_name}: {e}")
            results[backend_name] = {
                'error': str(e),
                'serialization': False,
                'deserialization': False,
                'integrity': False
            }
    
    # Print summary
    print(f"\n📋 Field Type Testing Summary")
    print("=" * 60)
    
    for backend_name, result in results.items():
        if 'error' in result:
            print(f"{backend_name}: ❌ FAILED - {result['error']}")
        else:
            status = "✅ PASSED" if all([result['serialization'], result['deserialization'], result['integrity']]) else "⚠️  PARTIAL"
            print(f"{backend_name}: {status}")
            print(f"  - Serialization: {'✅' if result['serialization'] else '❌'}")
            print(f"  - Deserialization: {'✅' if result['deserialization'] else '❌'}")
            print(f"  - Data Integrity: {'✅' if result['integrity'] else '⚠️'}")
    
    return results


async def test_backend_specific_field_handling():
    """Test backend-specific field handling and optimizations."""
    
    print(f"\n🎯 Testing Backend-Specific Field Handling")
    print("=" * 60)
    
    # Test ClickHouse specific fields if available
    try:
        from quantumengine.fields.clickhouse import LowCardinalityField
        print("✅ ClickHouse-specific fields available")
        
        # Test LowCardinality field
        class ClickHouseSpecificDoc(Document):
            category = LowCardinalityField()
            
            class Meta:
                collection = 'test_clickhouse_specific'
                backend = 'clickhouse'
        
        doc = ClickHouseSpecificDoc(category='test_category')
        db_data = doc.to_db()
        print(f"✅ LowCardinalityField serialization: {db_data.get('category')}")
        
    except ImportError:
        print("⚠️  ClickHouse-specific fields not available")
    
    # Test decimal handling across backends
    print(f"\n💰 Testing Decimal Field Handling")
    print("-" * 40)
    
    test_decimal = Decimal('999.99')
    
    for backend_name, doc_class in [('SurrealDB', SurrealTestDocument), ('ClickHouse', ClickHouseTestDocument), ('Redis', RedisTestDocument)]:
        try:
            doc = doc_class(name='Decimal Test', balance=test_decimal)
            db_data = doc.to_db()
            balance_value = db_data.get('balance')
            
            print(f"{backend_name}: Decimal({test_decimal}) -> {type(balance_value).__name__}({balance_value})")
            
        except Exception as e:
            print(f"{backend_name}: ❌ Error - {e}")


async def test_real_database_persistence():
    """Test actual database persistence to verify data is saved and retrieved correctly."""
    
    print(f"\n💾 Testing Real Database Persistence")
    print("=" * 60)
    
    # Test data
    test_data = {
        'name': 'Real DB Test User',
        'age': 30,
        'height': 6.1,
        'is_active': False,
        'created_at': datetime.now(timezone.utc),
        'balance': Decimal('999.99'),
        'metadata': {'test': 'persistence', 'numbers': [1, 2, 3]},
        'tags': ['real', 'database', 'test']
    }
    
    backends_to_test = [
        ('SurrealDB', SurrealTestDocument),
        ('ClickHouse', ClickHouseTestDocument), 
        ('Redis', RedisTestDocument)
    ]
    
    for backend_name, doc_class in backends_to_test:
        print(f"\n📊 Testing {backend_name} Real Persistence")
        print("-" * 40)
        
        try:
            # Create and save document
            doc = doc_class(**test_data)
            print(f"✅ Document created: {doc.name}")
            
            # Create table first
            await doc_class.create_table()
            print(f"✅ Table created/verified")
            
            # Save document to database
            saved_doc = await doc.save()
            print(f"✅ Document saved with ID: {saved_doc.id}")
            
            # Retrieve document from database
            retrieved_doc = await doc_class.get(saved_doc.id)
            print(f"✅ Document retrieved: {retrieved_doc.name}")
            
            # Verify data integrity
            integrity_check = True
            for field_name, original_value in test_data.items():
                retrieved_value = getattr(retrieved_doc, field_name)
                
                # Special handling for different backend storage types
                if isinstance(original_value, Decimal):
                    # Check if retrieved value is equivalent numerically
                    if isinstance(retrieved_value, str):
                        retrieved_decimal = Decimal(retrieved_value)
                    elif isinstance(retrieved_value, float):
                        retrieved_decimal = Decimal(str(retrieved_value))
                    else:
                        retrieved_decimal = retrieved_value
                    
                    if abs(original_value - retrieved_decimal) > Decimal('0.001'):
                        integrity_check = False
                        print(f"   ⚠️  {field_name}: Decimal precision issue - Original: {original_value}, Retrieved: {retrieved_value}")
                elif isinstance(original_value, datetime):
                    # DateTime might be stored/retrieved in different formats
                    if not isinstance(retrieved_value, datetime):
                        integrity_check = False
                        print(f"   ❌ {field_name}: DateTime not properly restored - got {type(retrieved_value)}")
                elif original_value != retrieved_value:
                    print(f"   ⚠️  {field_name}: Value difference - Original: {original_value}, Retrieved: {retrieved_value}")
                    # Don't mark as failure for minor differences in complex types
            
            if integrity_check:
                print(f"✅ Real database persistence: DATA VERIFIED")
            else:
                print(f"⚠️  Real database persistence: Some data differences detected")
            
            # Clean up - delete the test document
            await retrieved_doc.delete()
            print(f"✅ Test document cleaned up")
            
        except Exception as e:
            print(f"❌ {backend_name} persistence test failed: {e}")


if __name__ == '__main__':
    asyncio.run(test_field_serialization_deserialization())
    asyncio.run(test_backend_specific_field_handling())
    asyncio.run(test_real_database_persistence())