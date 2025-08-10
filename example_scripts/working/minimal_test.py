"""Minimal test to isolate the CBOR issue."""

import asyncio
from decimal import Decimal
from quantumengine import Document, create_connection
from quantumengine.fields import StringField, DecimalField

class SimpleProduct(Document):
    """Minimal product document."""
    name = StringField(required=True)
    price = DecimalField(min_value=0)

    class Meta:
        collection = "simple_products"

async def main():
    print("Testing minimal example...")
    
    # Use modern connection pooling with async context manager
    async with create_connection(
        url="ws://localhost:8000/rpc",
        namespace="test_ns",
        database="test_db",
        username="root",
        password="root",
        make_default=True,
        auto_connect=True
    ) as connection:
        print("Connected to SurrealDB")
        # Create table
        await SimpleProduct.create_table(schemafull=True)
        print("Created table")
        
        # Create minimal product
        product = SimpleProduct(
            name="Test Product",
            price=Decimal("99.99")
        )
        
        print("Attempting to save...")
        await product.save()
        print(f"✅ Success: {product.name}")
        
        await product.delete()
        print("Connection automatically managed by async context manager")

if __name__ == "__main__":
    asyncio.run(main())