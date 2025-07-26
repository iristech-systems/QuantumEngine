"""Redis Backend Example for QuantumEngine.

This example demonstrates how to use QuantumEngine with Redis/Dragonfly backend
for high-performance caching and session storage use cases.
"""

import asyncio
import json
from datetime import datetime
from decimal import Decimal

from quantumengine import (
    Document, StringField, IntField, FloatField, BooleanField, 
    DecimalField, DateTimeField, ListField, DictField,
    connect, disconnect_all
)


# Define document models for Redis backend
class User(Document):
    """User document model for Redis backend."""
    
    username = StringField(required=True)
    email = StringField(required=True)
    first_name = StringField()
    last_name = StringField()
    age = IntField(min_value=0)
    is_active = BooleanField(default=True)
    created_at = DateTimeField()
    last_login = DateTimeField()
    
    meta = {
        'collection': 'users',
        'backend': 'redis',
        'indexes': ['username', 'email', 'is_active']
    }


class Session(Document):
    """Session document model for Redis backend."""
    
    session_id = StringField(required=True)
    user_id = StringField(required=True)
    data = DictField()
    created_at = DateTimeField()
    expires_at = DateTimeField()
    ip_address = StringField()
    user_agent = StringField()
    
    meta = {
        'collection': 'sessions',
        'backend': 'redis',
        'indexes': ['session_id', 'user_id', 'expires_at']
    }


class Product(Document):
    """Product document model demonstrating complex types."""
    
    name = StringField(required=True)
    price = DecimalField()
    category = StringField()
    tags = ListField(StringField())
    metadata = DictField()
    stock = IntField(default=0)
    is_featured = BooleanField(default=False)
    created_at = DateTimeField()
    
    meta = {
        'collection': 'products',
        'backend': 'redis',
        'indexes': ['category', 'is_featured', 'stock']
    }


async def setup_database():
    """Set up the Redis database connection and tables."""
    print("🔗 Connecting to Redis...")
    
    # Connect to Redis backend
    await connect(
        backend='redis',
        host='localhost',
        port=6379,
        db=0,
        decode_responses=True
    )
    
    print("✅ Connected to Redis successfully!")
    
    # Create tables
    await User.create_table()
    await Session.create_table()
    await Product.create_table()
    
    print("📋 Created tables: users, sessions, products")


async def demo_basic_crud():
    """Demonstrate basic CRUD operations."""
    print("\n🔧 CRUD Operations Demo")
    print("=" * 40)
    
    # CREATE - Insert users
    print("Creating users...")
    users_data = [
        {
            'username': 'alice_redis',
            'email': 'alice@redis-example.com',
            'first_name': 'Alice',
            'last_name': 'Johnson',
            'age': 28,
            'created_at': datetime.utcnow()
        },
        {
            'username': 'bob_redis',
            'email': 'bob@redis-example.com',
            'first_name': 'Bob',
            'last_name': 'Smith',
            'age': 35,
            'created_at': datetime.utcnow(),
            'is_active': False
        },
        {
            'username': 'charlie_redis',
            'email': 'charlie@redis-example.com',
            'first_name': 'Charlie',
            'last_name': 'Brown',
            'age': 42,
            'created_at': datetime.utcnow()
        }
    ]
    
    created_users = []
    for user_data in users_data:
        user = User(**user_data)
        await user.save()
        created_users.append(user)
        print(f"  ✅ Created user: {user.username} (ID: {user.id})")
    
    # READ - Query users
    print("\nQuerying users...")
    all_users = await User.objects.all()
    print(f"  📊 Total users: {len(all_users)}")
    
    active_users = await User.objects(is_active=True).all()
    print(f"  📊 Active users: {len(active_users)}")
    
    alice = await User.objects(username='alice_redis').first()
    if alice:
        print(f"  👤 Found Alice: {alice.first_name} {alice.last_name}, age {alice.age}")
    
    # UPDATE - Modify user
    print("\nUpdating user...")
    if alice:
        alice.age = 29
        alice.last_login = datetime.utcnow()
        await alice.save()
        print(f"  ✏️  Updated Alice's age to {alice.age}")
    
    # DELETE - Remove inactive users
    print("\nDeleting inactive users...")
    deleted_count = await User.objects(is_active=False).delete()
    print(f"  🗑️  Deleted {deleted_count} inactive users")


async def demo_sessions():
    """Demonstrate session management with Redis."""
    print("\n🔐 Session Management Demo")
    print("=" * 40)
    
    # Get Alice for session demo
    alice = await User.objects(username='alice_redis').first()
    if not alice:
        print("❌ Alice not found - skipping session demo")
        return
    
    # Create session
    session_data = {
        'session_id': 'sess_' + str(datetime.utcnow().timestamp()).replace('.', ''),
        'user_id': alice.id,
        'data': {
            'theme': 'dark',
            'language': 'en',
            'preferences': {
                'notifications': True,
                'auto_save': True
            },
            'cart_items': ['item1', 'item2', 'item3']
        },
        'created_at': datetime.utcnow(),
        'expires_at': datetime.utcnow(),  # In real app, add expiration time
        'ip_address': '192.168.1.100',
        'user_agent': 'Mozilla/5.0 (compatible; QuantumEngine-Redis)'
    }
    
    session = Session(**session_data)
    await session.save()
    print(f"  🔑 Created session: {session.session_id}")
    print(f"  📦 Session data: {json.dumps(session.data, indent=4)}")
    
    # Query session
    retrieved_session = await Session.objects(session_id=session.session_id).first()
    if retrieved_session:
        print(f"  ✅ Retrieved session for user: {retrieved_session.user_id}")
        
        # Update session data
        retrieved_session.data['last_activity'] = datetime.utcnow().isoformat()
        retrieved_session.data['page_count'] = 15
        await retrieved_session.save()
        print("  ✏️  Updated session with activity data")


async def demo_products():
    """Demonstrate product catalog with complex types."""
    print("\n🛍️  Product Catalog Demo")
    print("=" * 40)
    
    # Create products with complex data types
    products_data = [
        {
            'name': 'Quantum Laptop Pro',
            'price': Decimal('1299.99'),
            'category': 'electronics',
            'tags': ['laptop', 'quantum', 'professional', 'high-performance'],
            'metadata': {
                'specifications': {
                    'cpu': 'Quantum Processor X1',
                    'ram': '32GB',
                    'storage': '1TB NVMe SSD',
                    'display': '15.6" 4K OLED'
                },
                'warranty': '3 years',
                'color_options': ['Space Gray', 'Silver', 'Gold']
            },
            'stock': 25,
            'is_featured': True,
            'created_at': datetime.utcnow()
        },
        {
            'name': 'Redis Cache Server',
            'price': Decimal('899.50'),
            'category': 'servers',
            'tags': ['server', 'cache', 'redis', 'enterprise'],
            'metadata': {
                'specifications': {
                    'cpu': '16-core Xeon',
                    'ram': '128GB DDR4',
                    'storage': '2TB NVMe',
                    'network': '10Gbps'
                },
                'power_consumption': '450W',
                'rack_units': 1
            },
            'stock': 10,
            'is_featured': False,
            'created_at': datetime.utcnow()
        },
        {
            'name': 'Database Workstation',
            'price': Decimal('2499.00'),
            'category': 'workstations',
            'tags': ['workstation', 'database', 'high-memory', 'professional'],
            'metadata': {
                'specifications': {
                    'cpu': '32-core ThreadRipper',
                    'ram': '256GB DDR4',
                    'storage': '4TB NVMe RAID',
                    'gpu': 'Professional Graphics Card'
                },
                'certifications': ['Intel', 'AMD', 'NVIDIA'],
                'target_use': 'Database analytics and machine learning'
            },
            'stock': 5,
            'is_featured': True,
            'created_at': datetime.utcnow()
        }
    ]
    
    print("Creating products...")
    for product_data in products_data:
        product = Product(**product_data)
        await product.save()
        print(f"  📦 Created: {product.name} - ${product.price} (Stock: {product.stock})")
    
    # Query products
    print("\nQuerying products...")
    
    # All products
    all_products = await Product.objects.all()
    print(f"  📊 Total products: {len(all_products)}")
    
    # Featured products
    featured = await Product.objects(is_featured=True).all()
    print(f"  ⭐ Featured products: {len(featured)}")
    for product in featured:
        print(f"    - {product.name}: ${product.price}")
    
    # Products by category
    electronics = await Product.objects(category='electronics').all()
    print(f"  💻 Electronics: {len(electronics)}")
    
    # Low stock alert
    low_stock = await Product.objects(stock__lt=15).all()
    print(f"  ⚠️  Low stock items ({len(low_stock)}):")
    for product in low_stock:
        print(f"    - {product.name}: {product.stock} units")


async def demo_performance():
    """Demonstrate Redis performance features."""
    print("\n🚀 Performance Demo")
    print("=" * 40)
    
    import time
    
    # Bulk insert performance test
    print("Testing bulk insert performance...")
    start_time = time.time()
    
    # Create batch of test users
    test_users = []
    for i in range(100):
        user = User(
            username=f'perf_user_{i}',
            email=f'perf_{i}@redis-test.com',
            first_name=f'User',
            last_name=f'{i}',
            age=20 + (i % 50),
            created_at=datetime.utcnow()
        )
        test_users.append(user)
    
    # Bulk insert using Redis pipeline
    await User.objects.insert(test_users)
    
    insert_time = time.time() - start_time
    print(f"  ⚡ Inserted 100 users in {insert_time:.3f} seconds")
    
    # Query performance test
    start_time = time.time()
    result_count = await User.objects.count()
    query_time = time.time() - start_time
    
    print(f"  🔍 Counted {result_count} users in {query_time:.3f} seconds")
    
    # Cleanup performance test users
    await User.objects(username__contains='perf_user_').delete()
    print("  🧹 Cleaned up performance test data")


async def demo_transactions():
    """Demonstrate Redis transaction capabilities."""
    print("\n💳 Transaction Demo")
    print("=" * 40)
    
    from quantumengine import get_db
    
    # Get Redis backend instance
    redis_db = get_db('redis')
    
    print("Testing Redis MULTI/EXEC transaction...")
    
    # Start transaction
    transaction = await redis_db.begin_transaction()
    
    try:
        # Create user within transaction
        tx_user = User(
            username='tx_user',
            email='tx@redis-example.com',
            first_name='Transaction',
            last_name='User',
            age=30,
            created_at=datetime.utcnow()
        )
        await tx_user.save()
        
        # Create session within transaction
        tx_session = Session(
            session_id='tx_session_123',
            user_id=tx_user.id,
            data={'transaction_test': True},
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow(),
            ip_address='127.0.0.1'
        )
        await tx_session.save()
        
        # Commit transaction
        await redis_db.commit_transaction(transaction)
        print("  ✅ Transaction committed successfully")
        
        # Verify data exists
        verified_user = await User.objects(username='tx_user').first()
        verified_session = await Session.objects(session_id='tx_session_123').first()
        
        if verified_user and verified_session:
            print("  ✅ Transaction data verified")
        else:
            print("  ❌ Transaction data verification failed")
            
    except Exception as e:
        # Rollback on error
        await redis_db.rollback_transaction(transaction)
        print(f"  ❌ Transaction rolled back due to error: {e}")


async def cleanup():
    """Clean up the demo data."""
    print("\n🧹 Cleaning up...")
    
    # Drop all collections
    await User.drop_collection()
    await Session.drop_collection()
    await Product.drop_collection()
    
    print("  ✅ Dropped all collections")
    
    # Disconnect
    await disconnect_all()
    print("  ✅ Disconnected from Redis")


async def main():
    """Main demo function."""
    print("🚀 QuantumEngine Redis Backend Demo")
    print("=" * 50)
    
    try:
        await setup_database()
        await demo_basic_crud()
        await demo_sessions()
        await demo_products()
        await demo_performance()
        await demo_transactions()
        
        print("\n🎉 Demo completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await cleanup()


if __name__ == "__main__":
    # Run the demo
    asyncio.run(main())