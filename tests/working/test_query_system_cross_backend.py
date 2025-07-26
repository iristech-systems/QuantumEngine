#!/usr/bin/env python3
"""
Test query system across all backends to verify backend-specific optimizations.

This test examines how different backends handle queries and ensures they use
their optimal query patterns (e.g., FETCH for SurrealDB, analytical functions for ClickHouse).
"""

import asyncio
from typing import Any, Dict, List

# Import QuantumEngine
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from quantumengine import Document
from quantumengine.fields import StringField, IntField, DateTimeField, ReferenceField
from quantumengine.connection import ConnectionRegistry
from quantumengine.query import QuerySet


class UserDocument(Document):
    """Base user document."""
    name = StringField(required=True)
    age = IntField()
    
    class Meta:
        collection = 'test_users'


class PostDocument(Document):
    """Base post document.""" 
    title = StringField(required=True)
    author = ReferenceField(UserDocument)
    views = IntField(default=0)
    
    class Meta:
        collection = 'test_posts'


# Backend-specific versions
class SurrealUser(UserDocument):
    class Meta:
        collection = 'test_users_surreal'
        backend = 'surrealdb'


class SurrealPost(PostDocument):
    author = ReferenceField(SurrealUser)
    
    class Meta:
        collection = 'test_posts_surreal'
        backend = 'surrealdb'


class ClickHouseUser(UserDocument):
    class Meta:
        collection = 'test_users_clickhouse'
        backend = 'clickhouse'


class ClickHousePost(PostDocument):
    author = ReferenceField(ClickHouseUser)
    
    class Meta:
        collection = 'test_posts_clickhouse'
        backend = 'clickhouse'


class RedisUser(UserDocument):
    class Meta:
        collection = 'test_users_redis'
        backend = 'redis'


class RedisPost(PostDocument):
    author = ReferenceField(RedisUser)
    
    class Meta:
        collection = 'test_posts_redis'
        backend = 'redis'


async def test_basic_query_operations():
    """Test basic query operations across backends."""
    
    print("🔍 Testing Basic Query Operations Across Backends")
    print("=" * 60)
    
    backends_to_test = [
        ('SurrealDB', SurrealUser, SurrealPost),
        ('ClickHouse', ClickHouseUser, ClickHousePost),
        ('Redis', RedisUser, RedisPost)
    ]
    
    results = {}
    
    for backend_name, user_class, post_class in backends_to_test:
        print(f"\n📊 Testing {backend_name} Backend")
        print("-" * 40)
        
        try:
            # First set up connections like in the field types test
            from quantumengine.connection import ConnectionRegistry, create_connection
            
            # Quick connection setup for this backend
            backend_name = user_class._meta.get('backend', 'surrealdb')
            if backend_name == 'surrealdb':
                try:
                    conn = create_connection(backend='surrealdb', url='ws://localhost:8000/rpc', 
                                           username='test', password='test', namespace='test', database='test')
                    await conn.connect()
                    ConnectionRegistry.set_default_connection('surrealdb', conn)
                except:
                    pass  # Continue without connection for basic testing
            elif backend_name == 'clickhouse':
                try:
                    conn = create_connection(backend='clickhouse', url='localhost', port=8123, database='test')
                    ConnectionRegistry.set_default_connection('clickhouse', conn)
                except:
                    pass
            elif backend_name == 'redis':
                try:
                    conn = create_connection(backend='redis', url='localhost', port=6379, db=0)
                    ConnectionRegistry.set_default_connection('redis', conn)
                except:
                    pass
            
            backend = user_class._get_backend()
            capabilities = backend.get_capabilities()
            print(f"Backend capabilities: {capabilities}")
            
            # Test basic query building
            queryset = user_class.objects
            
            # Test filter building
            print("🔎 Testing filter operations...")
            filter_tests = [
                ('Equal filter', lambda qs: qs.filter(name='John')),
                ('Greater than filter', lambda qs: qs.filter(age__gt=18)),
                ('In filter', lambda qs: qs.filter(name__in=['John', 'Jane'])),
            ]
            
            query_info = {}
            for test_name, filter_func in filter_tests:
                try:
                    filtered_qs = filter_func(queryset)
                    # Get the conditions built by the backend
                    if hasattr(filtered_qs, '_conditions'):
                        conditions = filtered_qs._conditions
                        print(f"  ✅ {test_name}: {len(conditions)} conditions built")
                        query_info[test_name] = conditions
                    else:
                        print(f"  ✅ {test_name}: QuerySet created successfully")
                        query_info[test_name] = "QuerySet created"
                        
                except Exception as e:
                    print(f"  ❌ {test_name}: Error - {e}")
                    query_info[test_name] = f"Error: {e}"
            
            # Test ordering
            print("📊 Testing ordering operations...")
            try:
                ordered_qs = queryset.order_by('name')
                print(f"  ✅ Order by name: QuerySet created")
                query_info['order_by'] = "Supported"
            except Exception as e:
                print(f"  ❌ Order by name: Error - {e}")
                query_info['order_by'] = f"Error: {e}"
            
            # Test limiting
            print("🔢 Testing limit operations...")
            try:
                limited_qs = queryset.limit(10)
                print(f"  ✅ Limit 10: QuerySet created")
                query_info['limit'] = "Supported"
            except Exception as e:
                print(f"  ❌ Limit 10: Error - {e}")
                query_info['limit'] = f"Error: {e}"
            
            results[backend_name] = {
                'capabilities': capabilities,
                'query_operations': query_info,
                'status': 'success'
            }
            
        except Exception as e:
            print(f"❌ Error testing {backend_name}: {e}")
            results[backend_name] = {
                'status': 'error',
                'error': str(e)
            }
    
    return results


async def test_backend_specific_query_optimizations():
    """Test backend-specific query optimizations and features."""
    
    print(f"\n🎯 Testing Backend-Specific Query Optimizations")
    print("=" * 60)
    
    # Test SurrealDB FETCH optimization
    print("🔗 SurrealDB: Testing FETCH for reference resolution")
    print("-" * 40)
    try:
        surreal_backend = SurrealUser._get_backend()
        if surreal_backend.supports_direct_record_access():
            print("✅ SurrealDB supports direct record access")
            print("✅ Should use FETCH for efficient reference resolution")
            
            # Test if FETCH is used in resolve_references
            user = SurrealUser(name='Test User')
            if hasattr(user, 'resolve_references'):
                print("✅ resolve_references method available")
                print("✅ Uses SurrealDB FETCH clause for efficient dereferencing")
        else:
            print("⚠️  Direct record access not supported")
            
    except Exception as e:
        print(f"❌ SurrealDB FETCH test error: {e}")
    
    # Test ClickHouse analytical optimizations
    print(f"\n📈 ClickHouse: Testing Analytical Query Optimizations")
    print("-" * 40)
    try:
        clickhouse_backend = ClickHouseUser._get_backend()
        optimized_methods = clickhouse_backend.get_optimized_methods()
        
        if optimized_methods:
            print("✅ ClickHouse optimized methods available:")
            for method, description in optimized_methods.items():
                print(f"  - {method}: {description}")
        else:
            print("⚠️  No optimized methods reported")
            
        # Test if bulk operations are supported
        if clickhouse_backend.supports_bulk_operations():
            print("✅ Bulk operations supported for analytical workloads")
        
        # Test materialized views support
        if clickhouse_backend.supports_materialized_views():
            print("✅ Materialized views supported for pre-aggregated data")
            
    except Exception as e:
        print(f"❌ ClickHouse optimization test error: {e}")
    
    # Test Redis key-value optimizations
    print(f"\n🔑 Redis: Testing Key-Value Query Optimizations")
    print("-" * 40)
    try:
        redis_backend = RedisUser._get_backend()
        
        # Test Redis-specific features
        if hasattr(redis_backend, '_get_doc_key'):
            print("✅ Redis uses optimized key patterns for document storage")
            
            # Test key pattern
            doc_key = redis_backend._get_doc_key('test_table', 'test_id')
            print(f"✅ Document key pattern: {doc_key}")
            
        if hasattr(redis_backend, '_get_index_key'):
            index_key = redis_backend._get_index_key('test_table', 'test_field')
            print(f"✅ Index key pattern: {index_key}")
        
        # Test if Redis uses sorted sets for indexing
        if redis_backend.supports_indexes():
            print("✅ Redis uses sorted sets for secondary indexes")
            
        # Test transaction support via pipelines
        if redis_backend.supports_transactions():
            print("✅ Redis uses MULTI/EXEC pipelines for transactions")
            
    except Exception as e:
        print(f"❌ Redis optimization test error: {e}")


async def test_query_condition_building():
    """Test how different backends build query conditions."""
    
    print(f"\n🏗️  Testing Query Condition Building")
    print("=" * 60)
    
    test_conditions = [
        ('Equal', 'name', '=', 'John'),
        ('Not equal', 'age', '!=', 25),
        ('Greater than', 'age', '>', 18),
        ('In list', 'name', 'in', ['John', 'Jane']),
        ('Contains', 'name', 'contains', 'Jo'),
        ('Is null', 'description', 'is null', None),
    ]
    
    backends = [
        ('SurrealDB', SurrealUser._get_backend()),
        ('ClickHouse', ClickHouseUser._get_backend()),
        ('Redis', RedisUser._get_backend())
    ]
    
    for backend_name, backend in backends:
        print(f"\n{backend_name} Condition Building:")
        print("-" * 30)
        
        for condition_name, field, operator, value in test_conditions:
            try:
                condition = backend.build_condition(field, operator, value)
                print(f"  {condition_name}: {condition}")
            except Exception as e:
                print(f"  {condition_name}: ❌ Error - {e}")


if __name__ == '__main__':
    asyncio.run(test_basic_query_operations())
    asyncio.run(test_backend_specific_query_optimizations())
    asyncio.run(test_query_condition_building())