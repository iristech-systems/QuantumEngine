"""Simple test script to validate core field fixes without database operations."""

import asyncio
from decimal import Decimal
from quantumengine import Document
from quantumengine.fields import DecimalField, IntField, FloatField, DurationField, RangeField, OptionField

# Define a simple document class to test field validation
class TestProduct(Document):
    """Test product document for field validation."""
    
    # Test DecimalField with constraints
    price = DecimalField(min_value=0, max_value=10000)
    
    # Test IntField with constraints  
    stock = IntField(min_value=0, default=0)
    
    # Test DurationField
    warranty_period = DurationField()
    
    # Test RangeField with Decimals
    price_range = RangeField(min_type=DecimalField(), max_type=DecimalField())
    
    # Test OptionField with FloatField validation
    discount = OptionField(field_type=FloatField(min_value=0, max_value=100))

    class Meta:
        collection = "test_products"

def test_field_validations():
    """Test field validations without database operations."""
    print("Testing core field fixes (validation only)...")
    
    # Test 1: DecimalField with valid value
    print("\n1. Testing DecimalField with min_value constraint...")
    try:
        product = TestProduct()
        product.price = Decimal("1299.99")
        # Trigger validation by accessing _data
        validated_price = product._fields['price'].validate(product.price)
        print(f"✅ DecimalField validation passed: {validated_price}")
    except Exception as e:
        print(f"❌ DecimalField validation failed: {e}")
    
    # Test 2: DecimalField with invalid value (should fail)
    print("\n2. Testing DecimalField with invalid value...")
    try:
        product = TestProduct()
        product.price = Decimal("-100.00")  # Should fail min_value=0
        validated_price = product._fields['price'].validate(product.price)
        print(f"❌ DecimalField should have rejected negative value: {validated_price}")
    except Exception as e:
        print(f"✅ DecimalField correctly rejected negative value: {e}")
    
    # Test 3: IntField with constraints
    print("\n3. Testing IntField with min_value constraint...")
    try:
        product = TestProduct()
        product.stock = 50
        validated_stock = product._fields['stock'].validate(product.stock)
        print(f"✅ IntField validation passed: {validated_stock}")
    except Exception as e:
        print(f"❌ IntField validation failed: {e}")
    
    # Test 4: DurationField conversion
    print("\n4. Testing DurationField conversion...")
    try:
        product = TestProduct()
        product.warranty_period = "2y"
        # Test to_db conversion
        db_value = product._fields['warranty_period'].to_db(product.warranty_period)
        print(f"✅ DurationField conversion passed: '2y' -> {type(db_value)}")
    except Exception as e:
        print(f"❌ DurationField conversion failed: {e}")
    
    # Test 5: RangeField with Decimals
    print("\n5. Testing RangeField with Decimal values...")
    try:
        product = TestProduct()
        product.price_range = {"min": Decimal("100.00"), "max": Decimal("200.00")}
        validated_range = product._fields['price_range'].validate(product.price_range)
        print(f"✅ RangeField validation passed: {validated_range}")
    except Exception as e:
        print(f"❌ RangeField validation failed: {e}")
    
    # Test 6: OptionField with FloatField
    print("\n6. Testing OptionField with FloatField validation...")
    try:
        product = TestProduct()
        product.discount = 10.0
        validated_discount = product._fields['discount'].validate(product.discount)
        print(f"✅ OptionField validation passed: {validated_discount}")
    except Exception as e:
        print(f"❌ OptionField validation failed: {e}")
    
    print("\n✅ All field validation tests completed!")

if __name__ == "__main__":
    test_field_validations()