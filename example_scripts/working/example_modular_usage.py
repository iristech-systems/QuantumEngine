#!/usr/bin/env python3
"""
Example demonstrating QuantumORM's modular installation system.

This script shows how to:
1. Check which backends are available
2. Use only available backends
3. Handle missing dependencies gracefully
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.quantumengine.backends import BackendRegistry
from src.quantumengine.document import Document
from src.quantumengine.fields import StringField, IntField, DateTimeField
from src.quantumengine.connection import create_connection
import datetime


class UserActivity(Document):
    """Example document that works with any backend."""
    user_id = StringField(required=True)
    action = StringField(required=True)
    timestamp = DateTimeField(required=True)
    score = IntField(default=0)


def demonstrate_backend_discovery():
    """Show how to discover available backends."""
    print("🔍 Backend Discovery")
    print("=" * 40)
    
    # List available backends
    available = BackendRegistry.list_backends()
    print(f"Available backends: {available}")
    
    # List failed backends with reasons
    failed = BackendRegistry.list_failed_backends()
    if failed:
        print("\nBackends with missing dependencies:")
        for backend, error in failed.items():
            print(f"  {backend}: {error}")
    else:
        print("All backends loaded successfully!")
    
    return available


def demonstrate_graceful_fallback(available_backends):
    """Show how to handle missing backends gracefully."""
    print("\n🔄 Graceful Backend Handling")
    print("=" * 40)
    
    # Preferred backend order
    preferred_backends = ['clickhouse', 'surrealdb']
    
    selected_backend = None
    for backend in preferred_backends:
        if backend in available_backends:
            selected_backend = backend
            print(f"Using {backend} backend (preferred)")
            break
    
    if not selected_backend:
        print("No preferred backends available!")
        return None
    
    return selected_backend


def demonstrate_backend_specific_features(backend_name):
    """Show backend-specific field usage."""
    print(f"\n⚡ Backend-Specific Features ({backend_name})")
    print("=" * 40)
    
    if backend_name == 'clickhouse':
        try:
            from src.quantumengine.fields.clickhouse import LowCardinalityField, ArrayField
            
            class ClickHouseUserActivity(UserActivity):
                """ClickHouse-optimized version."""
                action = LowCardinalityField(required=True)  # Efficient for repeated values
                tags = ArrayField(LowCardinalityField())     # Array of low-cardinality strings
                
                class Meta:
                    backend = 'clickhouse'
                    collection = 'user_activity_ch'
            
            print("✅ Using ClickHouse-specific optimizations:")
            print("  - LowCardinalityField for efficient string storage")
            print("  - ArrayField for complex data types")
            
            return ClickHouseUserActivity
            
        except ImportError as e:
            print(f"⚠️ ClickHouse features not available: {e}")
    
    elif backend_name == 'surrealdb':
        class SurrealDBUserActivity(UserActivity):
            """SurrealDB version with graph capabilities."""
            
            class Meta:
                backend = 'surrealdb'
                collection = 'user_activity_sr'
        
        print("✅ Using SurrealDB for:")
        print("  - Graph relationships")
        print("  - Document flexibility")
        print("  - Real-time subscriptions")
        
        return SurrealDBUserActivity
    
    # Fallback to basic document
    class GenericUserActivity(UserActivity):
        class Meta:
            backend = backend_name
            collection = 'user_activity'
    
    return GenericUserActivity


def demonstrate_function_usage():
    """Show that ClickHouse functions work regardless of backend availability."""
    print("\n🔧 ClickHouse Functions (Always Available)")
    print("=" * 40)
    
    try:
        from src.quantumengine.clickhouse_functions import has, toDate, length, lower
        
        # These work even if ClickHouse backend isn't installed
        functions = [
            ("Array check", has('tags', 'important')),
            ("Date conversion", toDate('timestamp')),
            ("String length", length('action')),
            ("Lowercase", lower('user_id')),
        ]
        
        for desc, func in functions:
            print(f"✅ {desc}: {func.to_sql()}")
        
        print("\n💡 Functions generate SQL that can be used when ClickHouse becomes available")
        
    except ImportError as e:
        print(f"❌ Function import failed: {e}")


def demonstrate_connection_creation(backend_name):
    """Show safe connection creation."""
    print(f"\n🔗 Connection Creation ({backend_name})")
    print("=" * 40)
    
    try:
        if backend_name == 'clickhouse':
            connection = create_connection(
                backend='clickhouse',
                url='localhost',
                database='test_db',
                username='default',
                password='',
                auto_connect=False  # Don't actually connect in this demo
            )
            print("✅ ClickHouse connection object created")
            
        elif backend_name == 'surrealdb':
            connection = create_connection(
                backend='surrealdb',
                url='ws://localhost:8000/rpc',
                namespace='demo',
                database='test_db',
                username='root',
                password='root',
                auto_connect=False  # Don't actually connect in this demo
            )
            print("✅ SurrealDB connection object created")
        
        print(f"Connection type: {type(connection)}")
        
    except ImportError as e:
        print(f"⚠️ Backend not available: {str(e)[:100]}...")
    except Exception as e:
        print(f"❌ Connection creation failed: {e}")


def main():
    """Run the modular usage demonstration."""
    print("🚀 QuantumORM Modular Installation Demo")
    print("=" * 60)
    
    # Discover available backends
    available_backends = demonstrate_backend_discovery()
    
    if not available_backends:
        print("\n❌ No backends available. Install with:")
        print("  pip install quantumengine[surrealdb]  # For SurrealDB")
        print("  pip install quantumengine[clickhouse] # For ClickHouse")
        print("  pip install quantumengine[all]        # For everything")
        return
    
    # Select best available backend
    selected_backend = demonstrate_graceful_fallback(available_backends)
    
    if selected_backend:
        # Show backend-specific features
        document_class = demonstrate_backend_specific_features(selected_backend)
        
        # Show connection creation
        demonstrate_connection_creation(selected_backend)
    
    # Functions are always available
    demonstrate_function_usage()
    
    print("\n" + "=" * 60)
    print("🎉 Modular Installation Demo Complete!")
    print("\n✅ Key Benefits:")
    print("- Install only what you need")
    print("- Graceful handling of missing dependencies")
    print("- Backend-specific optimizations when available")
    print("- Core functionality always works")
    print("- Easy migration between different setups")


if __name__ == "__main__":
    main()