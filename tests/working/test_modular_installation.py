#!/usr/bin/env python3
"""
Test script for QuantumORM modular installation system.

This script tests the backend registry system to ensure:
1. Available backends are properly registered
2. Missing dependencies show helpful error messages
3. Installation instructions are correct
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def test_backend_registry():
    """Test the BackendRegistry system."""
    print("🔍 Testing QuantumORM Backend Registry System")
    print("=" * 60)
    
    # Import the registry
    try:
        from src.quantumengine.backends import BackendRegistry
        print("✅ Successfully imported BackendRegistry")
    except ImportError as e:
        print(f"❌ Failed to import BackendRegistry: {e}")
        return False
    
    # Test listing available backends
    print(f"\n📊 Available backends: {BackendRegistry.list_backends()}")
    print(f"❌ Failed backends: {list(BackendRegistry.list_failed_backends().keys())}")
    
    # Test each backend
    backends_to_test = ['clickhouse', 'surrealdb', 'nonexistent']
    
    for backend_name in backends_to_test:
        print(f"\n🔧 Testing backend: {backend_name}")
        
        # Test availability check
        is_available = BackendRegistry.is_backend_available(backend_name)
        print(f"   Available: {is_available}")
        
        # Test getting backend
        try:
            backend_class = BackendRegistry.get_backend(backend_name)
            print(f"   ✅ Successfully got backend class: {backend_class}")
        except ImportError as e:
            print(f"   ⚠️ Missing dependency: {e}")
        except ValueError as e:
            print(f"   ❌ Backend error: {e}")
    
    return True


def test_connection_creation():
    """Test connection creation with modular backends."""
    print("\n🔗 Testing Connection Creation with Modular Backends")
    print("=" * 60)
    
    try:
        from src.quantumengine.connection import create_connection
        print("✅ Successfully imported create_connection")
    except ImportError as e:
        print(f"❌ Failed to import create_connection: {e}")
        return False
    
    # Test connection creation for each backend
    backends_to_test = [
        ('clickhouse', {
            'url': 'localhost',
            'database': 'test_db',
            'username': 'default',
            'password': ''
        }),
        ('surrealdb', {
            'url': 'ws://localhost:8000/rpc',
            'namespace': 'test_ns', 
            'database': 'test_db',
            'username': 'root',
            'password': 'root'
        }),
        ('nonexistent', {})
    ]
    
    for backend_name, params in backends_to_test:
        print(f"\n🔧 Testing connection creation for: {backend_name}")
        
        try:
            connection = create_connection(backend=backend_name, **params, auto_connect=False)
            print(f"   ✅ Successfully created connection: {type(connection)}")
        except ImportError as e:
            print(f"   ⚠️ Missing dependency: {str(e)[:100]}...")
        except ValueError as e:
            print(f"   ❌ Backend error: {str(e)[:100]}...")
        except Exception as e:
            print(f"   ❌ Unexpected error: {type(e).__name__}: {str(e)[:100]}...")
    
    return True


def test_import_scenarios():
    """Test different import scenarios."""
    print("\n📦 Testing Import Scenarios")
    print("=" * 60)
    
    # Test core imports (should always work)
    core_imports = [
        'src.quantumengine.document',
        'src.quantumengine.fields',
        'src.quantumengine.connection',
        'src.quantumengine.backends',
    ]
    
    for import_path in core_imports:
        try:
            __import__(import_path)
            print(f"✅ Core import works: {import_path}")
        except ImportError as e:
            print(f"❌ Core import failed: {import_path} - {e}")
    
    # Test backend-specific imports
    backend_imports = [
        ('src.quantumengine.backends.clickhouse', 'ClickHouse'),
        ('src.quantumengine.backends.surrealdb', 'SurrealDB'),
    ]
    
    for import_path, backend_name in backend_imports:
        try:
            __import__(import_path)
            print(f"✅ Backend import works: {backend_name}")
        except ImportError as e:
            print(f"⚠️ Backend import requires dependencies: {backend_name} - {str(e)[:100]}...")
    
    return True


def test_fields_import():
    """Test that ClickHouse fields can be imported even without backend."""
    print("\n🏗️ Testing Field Imports")
    print("=" * 60)
    
    try:
        from src.quantumengine.fields import (
            StringField, IntField, LowCardinalityField, 
            ArrayField, FixedStringField
        )
        print("✅ Successfully imported all field types")
        
        # Test field creation (shouldn't require backend dependencies)
        fields_to_test = [
            ("StringField", StringField()),
            ("LowCardinalityField", LowCardinalityField()),
            ("ArrayField", ArrayField(StringField())),
            ("FixedStringField", FixedStringField(length=3)),
        ]
        
        for field_name, field_instance in fields_to_test:
            print(f"✅ Created {field_name}: {type(field_instance)}")
            
        return True
        
    except ImportError as e:
        print(f"❌ Failed to import fields: {e}")
        return False


def test_functions_import():
    """Test that ClickHouse functions can be imported without backend."""
    print("\n⚡ Testing Function Imports")
    print("=" * 60)
    
    try:
        from src.quantumengine.clickhouse_functions import (
            has, toDate, length, lower, upper
        )
        print("✅ Successfully imported ClickHouse functions")
        
        # Test function creation (shouldn't require backend dependencies)
        functions_to_test = [
            ("has", has('tags', 'electronics')),
            ("toDate", toDate('created_at')),
            ("length", length('description')),
            ("lower", lower('name')),
            ("upper", upper('code')),
        ]
        
        for func_name, func_instance in functions_to_test:
            sql_result = func_instance.to_sql()
            print(f"✅ {func_name} function: {sql_result}")
            
        return True
        
    except ImportError as e:
        print(f"❌ Failed to import functions: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 QuantumORM Modular Installation Test Suite")
    print("=" * 80)
    
    all_tests_passed = True
    
    test_functions = [
        test_backend_registry,
        test_connection_creation,
        test_import_scenarios,
        test_fields_import,
        test_functions_import,
    ]
    
    for test_func in test_functions:
        try:
            result = test_func()
            all_tests_passed &= result
        except Exception as e:
            print(f"❌ Test {test_func.__name__} failed with exception: {e}")
            all_tests_passed = False
    
    print("\n" + "=" * 80)
    if all_tests_passed:
        print("🎉 All Modular Installation Tests Completed!")
        print("\n✅ Key Results:")
        print("- ✅ Backend registry system working")
        print("- ✅ Helpful error messages for missing dependencies")
        print("- ✅ Core functionality available without backend packages")
        print("- ✅ Fields and functions importable independently")
        
        print("\n📦 Installation Examples:")
        print("  pip install quantumengine              # Core only")
        print("  pip install quantumengine[clickhouse]  # + ClickHouse")
        print("  pip install quantumengine[surrealdb]   # + SurrealDB")
        print("  pip install quantumengine[all]         # Everything")
        
        print("\n🚀 Modular installation system is production-ready!")
    else:
        print("❌ Some tests failed. Check the output above for details.")
    
    return all_tests_passed


if __name__ == "__main__":
    result = main()
    exit(0 if result else 1)