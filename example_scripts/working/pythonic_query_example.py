#!/usr/bin/env python3
"""
QuantumEngine Pythonic Query Syntax Example

This example demonstrates the new Pythonic query syntax alongside 
the traditional Django-style syntax. Both syntaxes work identically
and can be mixed in the same query.

Run: uv run python example_scripts/working/pythonic_query_example.py
"""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from quantumengine import Document
from quantumengine.fields import StringField, IntField, BooleanField, EmailField
from quantumengine.connection import ConnectionRegistry, create_connection
from quantumengine.query_expressions import Q


class User(Document):
    """Example user document."""
    username = StringField(required=True)
    email = EmailField(required=True)
    age = IntField()
    is_active = BooleanField(default=True)
    role = StringField(default='user')
    
    class Meta:
        collection = 'pythonic_example_users'


class Post(Document):
    """Example post document."""
    title = StringField(required=True)
    content = StringField()
    author = StringField()
    views = IntField(default=0)
    published = BooleanField(default=False)
    
    class Meta:
        collection = 'pythonic_example_posts'


async def setup_connection():
    """Setup database connection."""
    try:
        conn = create_connection(
            backend='surrealdb',
            url='ws://localhost:8000/rpc',
            username='root',
            password='root',
            namespace='examples',
            database='pythonic_queries',
            async_mode=True
        )
        await conn.connect()
        ConnectionRegistry.register('pythonic_example', conn, 'surrealdb')
        ConnectionRegistry.set_default('surrealdb', 'pythonic_example')
        print("✅ Connected to SurrealDB")
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("Make sure SurrealDB is running on localhost:8000")
        return False


async def create_sample_data():
    """Create sample data for demonstration."""
    # Clear existing data
    await User.objects.filter().delete()
    await Post.objects.filter().delete()
    
    # Create users
    users = [
        User(username='alice', email='alice@example.com', age=25, is_active=True, role='admin'),
        User(username='bob', email='bob@example.com', age=30, is_active=True, role='user'),
        User(username='charlie', email='charlie@test.com', age=35, is_active=False, role='moderator'),
        User(username='diana', email='diana@example.com', age=28, is_active=True, role='user'),
        User(username='eve', email='eve@company.com', age=42, is_active=True, role='admin'),
    ]
    
    for user in users:
        await user.save()
    
    # Create posts
    posts = [
        Post(title='Getting Started', content='Welcome to our platform', author='alice', views=150, published=True),
        Post(title='Advanced Tips', content='Pro tips for power users', author='bob', views=89, published=True),
        Post(title='Draft Post', content='Work in progress', author='charlie', views=5, published=False),
        Post(title='News Update', content='Latest developments', author='diana', views=203, published=True),
        Post(title='Tutorial', content='Step by step guide', author='eve', views=445, published=True),
    ]
    
    for post in posts:
        await post.save()
    
    print(f"✅ Created {len(users)} users and {len(posts)} posts")


async def demonstrate_basic_comparisons():
    """Demonstrate basic comparison operators."""
    print("\n" + "="*60)
    print("🔍 BASIC COMPARISONS")
    print("="*60)
    
    # Greater than
    print("\n1. Finding users older than 30:")
    print("   Django style: User.objects.filter(age__gt=30)")
    users_django = await User.objects.filter(age__gt=30).all()
    print(f"   Result: {[u.username for u in users_django]}")
    
    print("   Pythonic style: User.objects.filter(User.age > 30)")
    users_pythonic = await User.objects.filter(User.age > 30).all()
    print(f"   Result: {[u.username for u in users_pythonic]}")
    
    # Less than or equal
    print("\n2. Finding posts with views <= 100:")
    print("   Django style: Post.objects.filter(views__lte=100)")
    posts_django = await Post.objects.filter(views__lte=100).all()
    print(f"   Result: {[p.title for p in posts_django]}")
    
    print("   Pythonic style: Post.objects.filter(Post.views <= 100)")
    posts_pythonic = await Post.objects.filter(Post.views <= 100).all()
    print(f"   Result: {[p.title for p in posts_pythonic]}")


async def demonstrate_multiple_conditions():
    """Demonstrate multiple conditions with AND logic."""
    print("\n" + "="*60)
    print("🔗 MULTIPLE CONDITIONS (AND)")
    print("="*60)
    
    print("\n1. Active users aged 25-35:")
    print("   Django style: User.objects.filter(age__gte=25, age__lte=35, is_active=True)")
    users_django = await User.objects.filter(age__gte=25, age__lte=35, is_active=True).all()
    print(f"   Result: {[u.username for u in users_django]}")
    
    print("   Pythonic style: User.objects.filter(User.age >= 25, User.age <= 35, User.is_active == True)")
    users_pythonic = await User.objects.filter(User.age >= 25, User.age <= 35, User.is_active == True).all()
    print(f"   Result: {[u.username for u in users_pythonic]}")
    
    print("   Mixed style: User.objects.filter(User.age >= 25, User.age <= 35, is_active=True)")
    users_mixed = await User.objects.filter(User.age >= 25, User.age <= 35, is_active=True).all()
    print(f"   Result: {[u.username for u in users_mixed]}")


async def demonstrate_complex_expressions():
    """Demonstrate complex expressions with OR logic."""
    print("\n" + "="*60)
    print("🌐 COMPLEX EXPRESSIONS (OR/AND)")
    print("="*60)
    
    print("\n1. Admins OR users over 35:")
    print("   Django style: User.objects.filter(Q(role='admin') | Q(age__gt=35))")
    query_django = Q(role='admin') | Q(age__gt=35)
    users_django = await User.objects.filter(query_django).all()
    print(f"   Result: {[u.username for u in users_django]}")
    
    print("   Pythonic style: User.objects.filter((User.role == 'admin') | (User.age > 35))")
    query_pythonic = (User.role == 'admin') | (User.age > 35)
    users_pythonic = await User.objects.filter(query_pythonic).all()
    print(f"   Result: {[u.username for u in users_pythonic]}")


async def demonstrate_string_operations():
    """Demonstrate string operations."""
    print("\n" + "="*60)
    print("📝 STRING OPERATIONS")
    print("="*60)
    
    print("\n1. Users with emails ending in '@example.com':")
    print("   Django style: User.objects.filter(email__endswith='@example.com')")
    users_django = await User.objects.filter(email__endswith='@example.com').all()
    print(f"   Result: {[u.username for u in users_django]}")
    
    print("   Pythonic style: User.objects.filter(User.email.endswith('@example.com'))")
    users_pythonic = await User.objects.filter(User.email.endswith('@example.com')).all()
    print(f"   Result: {[u.username for u in users_pythonic]}")


async def demonstrate_objects_direct_call():
    """Demonstrate using objects() directly."""
    print("\n" + "="*60)
    print("📞 DIRECT objects() CALLS")
    print("="*60)
    
    print("\n1. Published posts using objects() directly:")
    print("   Django style: Post.objects(published=True)")
    posts_django = await Post.objects(published=True)
    print(f"   Result: {[p.title for p in posts_django]}")
    
    print("   Pythonic style: Post.objects(Post.published == True)")
    posts_pythonic = await Post.objects(Post.published == True)
    print(f"   Result: {[p.title for p in posts_pythonic]}")
    
    print("   Mixed style: Post.objects(Post.views > 100, published=True)")
    posts_mixed = await Post.objects(Post.views > 100, published=True)
    print(f"   Result: {[p.title for p in posts_mixed]}")


async def demonstrate_method_chaining():
    """Demonstrate method chaining."""
    print("\n" + "="*60)
    print("🔗 METHOD CHAINING")
    print("="*60)
    
    print("\n1. Active users, ordered by age, limited to 3:")
    print("   Django style: User.objects.filter(is_active=True).order_by('age').limit(3)")
    users_django = await User.objects.filter(is_active=True).order_by('age').limit(3).all()
    print(f"   Result: {[(u.username, u.age) for u in users_django]}")
    
    print("   Pythonic style: User.objects.filter(User.is_active == True).order_by('age').limit(3)")
    users_pythonic = await User.objects.filter(User.is_active == True).order_by('age').limit(3).all()
    print(f"   Result: {[(u.username, u.age) for u in users_pythonic]}")
    
    print("   Mixed chaining: User.objects.filter(User.is_active == True).filter(age__gte=25).limit(3)")
    users_mixed = await User.objects.filter(User.is_active == True).filter(age__gte=25).limit(3).all()
    print(f"   Result: {[(u.username, u.age) for u in users_mixed]}")


async def demonstrate_list_operations():
    """Demonstrate list operations."""
    print("\n" + "="*60)
    print("📋 LIST OPERATIONS")
    print("="*60)
    
    roles = ['admin', 'moderator']
    
    print(f"\n1. Users with roles in {roles}:")
    print("   Django style: User.objects.filter(role__in=['admin', 'moderator'])")
    users_django = await User.objects.filter(role__in=roles).all()
    print(f"   Result: {[u.username for u in users_django]}")
    
    print("   Pythonic style: User.objects.filter(User.role.in_(['admin', 'moderator']))")
    users_pythonic = await User.objects.filter(User.role.in_(roles)).all()
    print(f"   Result: {[u.username for u in users_pythonic]}")


async def main():
    """Main demonstration function."""
    print("🚀 QuantumEngine Pythonic Query Syntax Demo")
    print("=" * 60)
    
    # Setup
    if not await setup_connection():
        return
    
    await create_sample_data()
    
    # Run demonstrations
    await demonstrate_basic_comparisons()
    await demonstrate_multiple_conditions()
    await demonstrate_complex_expressions()
    await demonstrate_string_operations()
    await demonstrate_objects_direct_call()
    await demonstrate_method_chaining()
    await demonstrate_list_operations()
    
    print("\n" + "="*60)
    print("✅ DEMONSTRATION COMPLETE")
    print("="*60)
    print("\nKey Benefits of Pythonic Syntax:")
    print("• More intuitive: User.age > 30 vs age__gt=30")
    print("• Better IDE support with autocomplete")
    print("• Type safety and error checking")
    print("• Full backward compatibility")
    print("• Can mix both syntaxes in same query")
    print("\nBoth syntaxes generate identical database queries!")
    
    # Cleanup
    await User.objects.filter().delete()
    await Post.objects.filter().delete()
    print("\n🧹 Cleaned up demo data")


if __name__ == "__main__":
    asyncio.run(main())