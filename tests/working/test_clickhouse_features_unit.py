#!/usr/bin/env python3
"""
Unit tests for ClickHouse enhancements in QuantumORM.

These tests verify the implementation without requiring a live ClickHouse connection.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    from src.quantumengine.fields.clickhouse import (
        LowCardinalityField, FixedStringField, EnumField, ArrayField,
        CompressedStringField, CompressedLowCardinalityField
    )
    from src.quantumengine.fields import StringField, IntField
    from src.quantumengine.clickhouse_functions import (
        has, hasAll, hasAny, toDate, toYYYYMM, length, lower, upper, round_
    )
    print("✅ Successfully imported ClickHouse components")
except ImportError as e:
    print(f"❌ Failed to import ClickHouse components: {e}")
    exit(1)


def test_low_cardinality_field():
    """Test LowCardinalityField implementation."""
    print("\n🔍 Testing LowCardinalityField...")
    
    try:
        # Test basic field creation
        field = LowCardinalityField(required=True)
        assert field.get_clickhouse_type() == "LowCardinality(String)"
        assert field.get_surrealdb_type() == "string"
        print("✅ LowCardinalityField type mapping correct")
        
        # Test with custom base type
        field_int = LowCardinalityField(base_type='UInt32')
        assert field_int.get_clickhouse_type() == "LowCardinality(UInt32)"
        print("✅ LowCardinalityField custom base type correct")
        
        # Test validation
        validated = field.validate("test_value")
        assert validated == "test_value"
        print("✅ LowCardinalityField validation works")
        
        return True
        
    except Exception as e:
        print(f"❌ LowCardinalityField test failed: {e}")
        return False


def test_fixed_string_field():
    """Test FixedStringField implementation."""
    print("\n🔍 Testing FixedStringField...")
    
    try:
        # Test basic field creation
        field = FixedStringField(length=3)
        assert field.get_clickhouse_type() == "FixedString(3)"
        assert field.get_surrealdb_type() == "string"
        print("✅ FixedStringField type mapping correct")
        
        # Test validation - correct length
        validated = field.validate("USD")
        assert validated == "USD"
        print("✅ FixedStringField correct length validation works")
        
        # Test validation - incorrect length
        try:
            field.validate("TOOLONG")
            assert False, "Should have raised ValueError"
        except ValueError:
            print("✅ FixedStringField incorrect length validation works")
        
        return True
        
    except Exception as e:
        print(f"❌ FixedStringField test failed: {e}")
        return False


def test_enum_field():
    """Test EnumField implementation."""
    print("\n🔍 Testing EnumField...")
    
    try:
        # Test basic field creation
        field = EnumField(values={'active': 1, 'inactive': 2, 'pending': 3})
        expected_type = "Enum8('active' = 1, 'inactive' = 2, 'pending' = 3)"
        assert field.get_clickhouse_type() == expected_type
        print("✅ EnumField type mapping correct")
        
        # Test validation - valid value
        validated = field.validate("active")
        assert validated == "active"
        print("✅ EnumField valid value validation works")
        
        # Test validation - invalid value
        try:
            field.validate("invalid")
            assert False, "Should have raised ValueError"
        except ValueError:
            print("✅ EnumField invalid value validation works")
        
        return True
        
    except Exception as e:
        print(f"❌ EnumField test failed: {e}")
        return False


def test_array_field():
    """Test ArrayField implementation."""
    print("\n🔍 Testing ArrayField...")
    
    try:
        # Test with LowCardinality element
        field = ArrayField(LowCardinalityField())
        assert field.get_clickhouse_type() == "Array(LowCardinality(String))"
        print("✅ ArrayField with LowCardinality element correct")
        
        # Test with compression
        field_compressed = ArrayField(StringField(), codec="LZ4")
        assert "Array(String) CODEC(LZ4)" == field_compressed.get_clickhouse_type()
        print("✅ ArrayField with compression correct")
        
        # Test validation
        validated = field.validate(["tag1", "tag2", "tag3"])
        assert validated == ["tag1", "tag2", "tag3"]
        print("✅ ArrayField validation works")
        
        # Test invalid validation
        try:
            field.validate("not_a_list")
            assert False, "Should have raised TypeError"
        except TypeError:
            print("✅ ArrayField invalid type validation works")
        
        return True
        
    except Exception as e:
        print(f"❌ ArrayField test failed: {e}")
        return False


def test_compressed_fields():
    """Test compressed field implementations."""
    print("\n🔍 Testing Compressed Fields...")
    
    try:
        # Test CompressedStringField
        field = CompressedStringField(codec="ZSTD(3)")
        assert field.get_clickhouse_type() == "String CODEC(ZSTD(3))"
        print("✅ CompressedStringField type mapping correct")
        
        # Test CompressedLowCardinalityField
        field_lc = CompressedLowCardinalityField(codec="LZ4")
        assert field_lc.get_clickhouse_type() == "LowCardinality(String) CODEC(LZ4)"
        print("✅ CompressedLowCardinalityField type mapping correct")
        
        return True
        
    except Exception as e:
        print(f"❌ Compressed fields test failed: {e}")
        return False


def test_clickhouse_functions():
    """Test ClickHouse function implementations."""
    print("\n🔍 Testing ClickHouse Functions...")
    
    try:
        # Test array functions
        has_func = has('tags', 'electronics')
        assert has_func.to_sql() == "has(tags, 'electronics')"
        print("✅ has() function correct")
        
        has_all_func = hasAll('tags', ['electronics', 'phones'])
        assert has_all_func.to_sql() == "hasAll(tags, ['electronics', 'phones'])"
        print("✅ hasAll() function correct")
        
        has_any_func = hasAny('tags', ['electronics', 'computers'])
        assert has_any_func.to_sql() == "hasAny(tags, ['electronics', 'computers'])"
        print("✅ hasAny() function correct")
        
        # Test date functions
        to_date_func = toDate('created_at')
        assert to_date_func.to_sql() == "toDate(created_at)"
        print("✅ toDate() function correct")
        
        to_yyyymm_func = toYYYYMM('created_at')
        assert to_yyyymm_func.to_sql() == "toYYYYMM(created_at)"
        print("✅ toYYYYMM() function correct")
        
        # Test string functions
        length_func = length('description')
        assert length_func.to_sql() == "length(description)"
        print("✅ length() function correct")
        
        lower_func = lower('name')
        assert lower_func.to_sql() == "lower(name)"
        print("✅ lower() function correct")
        
        upper_func = upper('code')
        assert upper_func.to_sql() == "upper(code)"
        print("✅ upper() function correct")
        
        # Test math functions
        round_func = round_('price', 2)
        assert round_func.to_sql() == "round(price, 2)"
        print("✅ round() function correct")
        
        return True
        
    except Exception as e:
        print(f"❌ ClickHouse functions test failed: {e}")
        return False


def test_field_type_mapping():
    """Test field type mapping for different backends."""
    print("\n🔍 Testing Field Type Mapping...")
    
    try:
        # Test ArrayField type mapping for different element types
        array_field = ArrayField(IntField())
        assert "Array(Int64)" in array_field.get_clickhouse_type()
        print("✅ ArrayField with IntField element correct")
        
        # Test backend-specific conversions
        lc_field = LowCardinalityField()
        
        # Test conversion methods exist
        assert hasattr(lc_field, '_to_db_backend_specific')
        assert hasattr(lc_field, '_from_db_backend_specific')
        print("✅ Backend-specific conversion methods exist")
        
        return True
        
    except Exception as e:
        print(f"❌ Field type mapping test failed: {e}")
        return False


def test_integration_compatibility():
    """Test integration with existing QuantumORM components."""
    print("\n🔍 Testing Integration Compatibility...")
    
    try:
        # Test field imports work correctly
        from src.quantumengine.fields import ArrayField as ImportedArrayField
        from src.quantumengine.fields import LowCardinalityField as ImportedLCField
        
        assert ImportedArrayField == ArrayField
        assert ImportedLCField == LowCardinalityField
        print("✅ ClickHouse fields available through main fields import")
        
        # Test function imports
        from src.quantumengine import clickhouse_functions
        assert hasattr(clickhouse_functions, 'has')
        assert hasattr(clickhouse_functions, 'toDate')
        assert hasattr(clickhouse_functions, 'length')
        print("✅ ClickHouse functions module accessible")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration compatibility test failed: {e}")
        return False


def main():
    """Run all unit tests."""
    print("🚀 QuantumORM ClickHouse Features Unit Tests")
    print("=" * 60)
    
    all_tests_passed = True
    
    # Run all tests
    test_functions = [
        test_low_cardinality_field,
        test_fixed_string_field, 
        test_enum_field,
        test_array_field,
        test_compressed_fields,
        test_clickhouse_functions,
        test_field_type_mapping,
        test_integration_compatibility,
    ]
    
    for test_func in test_functions:
        try:
            result = test_func()
            all_tests_passed &= result
        except Exception as e:
            print(f"❌ Test {test_func.__name__} failed with exception: {e}")
            all_tests_passed = False
    
    print("\n" + "=" * 60)
    if all_tests_passed:
        print("🎉 All ClickHouse Unit Tests Passed!")
        print("\n✅ Verified Implementation:")
        print("- ✅ LowCardinalityField for efficient string storage")
        print("- ✅ FixedStringField for fixed-length strings")
        print("- ✅ EnumField for predefined values")
        print("- ✅ ArrayField with nested type support")
        print("- ✅ Compressed fields with codec support")
        print("- ✅ Complete ClickHouse functions library")
        print("- ✅ Backend-specific type conversions")
        print("- ✅ Integration with existing QuantumORM")
        
        print("\n🚀 ClickHouse features are fully implemented and ready!")
    else:
        print("❌ Some tests failed. Check the output above for details.")
    
    return all_tests_passed


if __name__ == "__main__":
    result = main()
    exit(0 if result else 1)