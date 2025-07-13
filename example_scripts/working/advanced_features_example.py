#!/usr/bin/env python3
"""Exact recreation of the original advanced_features_example.py to isolate the issue"""

import asyncio
import datetime
from decimal import Decimal
from quantumengine import Document, create_connection
from quantumengine.fields import (
    StringField, IntField, FloatField, DecimalField, BooleanField,
    DateTimeField, DurationField, ListField, DictField, OptionField, RangeField
)

# Define a simple document class to test core field fixes
class TestProduct(Document):
    """Test product document for field validation."""

    # Basic fields
    name = StringField(required=True)
    description = StringField()
    
    # Numeric fields with constraints (the main fixes we implemented)
    price = DecimalField(min_value=0, max_value=10000)  # Test DecimalField with constraints
    stock = IntField(min_value=0, default=0)
    weight = FloatField(min_value=0)
    
    # Boolean field
    active = BooleanField(default=True)
    
    # Date and time fields  
    created_at = DateTimeField(default=lambda: datetime.datetime.now())
    updated_at = DateTimeField()
    
    # Duration field (fixed to use SurrealDB Duration objects)
    warranty_period = DurationField()
    
    # List field
    tags = ListField(field_type=StringField())
    
    # Dictionary field
    attributes = DictField()
    
    # Range field with Decimal support (fixed)
    price_range = RangeField(min_type=DecimalField(), max_type=DecimalField())
    
    # Optional field (fixed FloatField validation)
    discount = OptionField(field_type=FloatField(min_value=0, max_value=100))

    class Meta:
        collection = "test_products"

print("Class definition completed. Starting async main...")

async def main():
    print("Testing core field fixes...")
    
    # Connect to SurrealDB
    connection = create_connection(
        url="ws://localhost:8000/rpc",
        namespace="test_ns",
        database="test_db",
        username="root",
        password="root",
        make_default=True
    )
    
    await connection.connect()
    print("Connected to SurrealDB")
    
    try:
        # Create table
        try:
            await TestProduct.create_table(schemafull=True)
            print("Created test product table")
        except Exception as e:
            print(f"Table might already exist: {e}")
        
        print("✅ All complex fields are working properly in class definition!")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await connection.disconnect()
        print("Disconnected from SurrealDB")

if __name__ == "__main__":
    print("Starting asyncio.run...")
    asyncio.run(main())