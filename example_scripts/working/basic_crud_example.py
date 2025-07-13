import asyncio
from quantumengine import (
    Document, StringField, IntField, FloatField, BooleanField, 
    create_connection, SurrealEngine
)

# Define a simple document model
class User(Document):
    """User document model for basic CRUD operations."""
    
    username = StringField(required=True)
    email = StringField(required=True)
    first_name = StringField()
    last_name = StringField()
    age = IntField(min_value=0)
    active = BooleanField(default=True)
    
    class Meta:
        collection = "users"
        indexes = [
            {"name": "user_username_idx", "fields": ["username"], "unique": True},
            {"name": "user_email_idx", "fields": ["email"], "unique": True}
        ]

async def main():
    # Connect to the database
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
        # Create the table and indexes
        try:
            await User.create_table()
            print("Created users table")
        except Exception as e:
            print(f"Table might already exist: {e}")
        
        try:
            await User.create_indexes()
            print("Created users indexes")
        except Exception as e:
            print(f"Indexes might already exist: {e}")
        
        # CREATE: Create a new user
        user = User(
            username="johndoe",
            email="john.doe@example.com",
            first_name="John",
            last_name="Doe",
            age=30
        )
        
        # Save the user to the database
        await user.save()
        print(f"Created user: {user.username} (ID: {user.id})")
        
        # READ: Retrieve the user by ID
        retrieved_user = await User.objects.get(id=user.id)
        print(f"Retrieved user by ID: {retrieved_user.username} (ID: {retrieved_user.id})")
        
        # READ: Retrieve the user by username
        retrieved_user_by_username = await User.objects.get(username="johndoe")
        print(f"Retrieved user by username: {retrieved_user_by_username.username}")
        
        # READ: List all users
        all_users = await User.objects.all()
        print(f"All users: {[u.username for u in all_users]}")
        
        # UPDATE: Update the user
        user.age = 31
        await user.save()
        print(f"Updated user age to {user.age}")
        
        # Verify the update
        updated_user = await User.objects.get(id=user.id)
        print(f"Verified update: {updated_user.username}'s age is now {updated_user.age}")
        
        # CREATE: Create more users for bulk operations
        users_to_create = [
            User(username=f"user{i}", email=f"user{i}@example.com", 
                 first_name=f"User{i}", last_name="Test", age=20+i)
            for i in range(1, 4)
        ]
        
        # Bulk create using save individually (bulk_create may not exist)
        created_users = []
        for user in users_to_create:
            await user.save()
            created_users.append(user)
        print(f"Bulk created {len(created_users)} users")
        
        # READ: Query with filters
        young_users = await User.objects.filter(age__lt=30).all()
        print(f"Young users (age < 30): {[u.username for u in young_users]}")
        
        active_users = await User.objects.filter(active=True).all()
        print(f"Active users: {[u.username for u in active_users]}")
        
        # READ: Count users
        user_count = await User.objects.count()
        print(f"Total user count: {user_count}")
        
        # UPDATE: Bulk update by setting active=False for young users
        for young_user in young_users:
            young_user.active = False
            await young_user.save()
        print(f"Updated {len(young_users)} young users to inactive")
        
        # Verify the bulk update
        inactive_users = await User.objects.filter(active=False).all()
        print(f"Inactive users: {[u.username for u in inactive_users]}")
        
        # DELETE: Delete a specific user
        first_user_to_delete = created_users[0]
        await first_user_to_delete.delete()
        print(f"Deleted user: {first_user_to_delete.username}")
        
        # DELETE: Delete users with a specific condition
        users_to_delete = await User.objects.filter(age__lt=25).all()
        for user_to_delete in users_to_delete:
            await user_to_delete.delete()
        print(f"Deleted {len(users_to_delete)} users with age < 25")
        
        # Verify the deletions
        remaining_users = await User.objects.all()
        print(f"Remaining users: {[u.username for u in remaining_users]}")
        
        # Clean up - delete all remaining users
        for remaining_user in remaining_users:
            await remaining_user.delete()
        print("Cleaned up all users")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Disconnect from the database
        await connection.disconnect()
        print("Disconnected from SurrealDB")

# Run the async example
if __name__ == "__main__":
    asyncio.run(main())