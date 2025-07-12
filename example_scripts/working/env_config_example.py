import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
import os
from quantumorm import (
    Document, StringField, IntField, create_connection
)

# Define a simple document model
class Config(Document):
    """Configuration document model."""
    
    key = StringField(required=True)
    value = StringField(required=True)
    description = StringField()
    
    class Meta:
        collection = "configs"
        indexes = [
            {"name": "config_key_idx", "fields": ["key"], "unique": True}
        ]

async def main():
    # Get connection parameters from environment variables
    # with fallback values if not set
    url = os.environ.get("SURREALDB_URL", "ws://localhost:8000/rpc")
    namespace = os.environ.get("SURREALDB_NS", "test_ns")
    database = os.environ.get("SURREALDB_DB", "test_db")
    username = os.environ.get("SURREALDB_USER", "root")
    password = os.environ.get("SURREALDB_PASS", "root")
    
    print("=== Connection Configuration ===")
    print(f"URL: {url}")
    print(f"Namespace: {namespace}")
    print(f"Database: {database}")
    print(f"Username: {username}")
    print(f"Password: {'*' * len(password)}")  # Don't print actual password
    
    # Connect to the database using environment variables
    connection = create_connection(
        url=url,
        namespace=namespace,
        database=database,
        username=username,
        password=password,
        make_default=True
    )
    
    await connection.connect()
    print("\nConnected to SurrealDB using environment variables")
    
    try:
        # Create the table and indexes
        await Config.create_table(connection)
        await Config.create_indexes(connection)
        print("Created configs table and indexes")
        
        # Create some configuration entries
        configs = [
            Config(key="app.name", value="SurrealEngine Demo", description="Application name"),
            Config(key="app.version", value="1.0.0", description="Application version"),
            Config(key="app.debug", value="true", description="Debug mode flag"),
            Config(key="db.timeout", value="30", description="Database timeout in seconds")
        ]
        
        # Save configs
        for config in configs:
            await config.save()
        
        print(f"Created {len(configs)} configuration entries")
        
        # Retrieve all configs
        all_configs = await Config.objects.all()
        
        print("\n=== Configuration Entries ===")
        for config in all_configs:
            print(f"{config.key}: {config.value} - {config.description}")
        
        # Retrieve a specific config
        app_name = await Config.objects.get(key="app.name")
        print(f"\nApplication name: {app_name.value}")
        
        # Update a config
        debug_config = await Config.objects.get(key="app.debug")
        debug_config.value = "false"
        await debug_config.save()
        print(f"Updated {debug_config.key} to {debug_config.value}")
        
        # Verify the update
        updated_debug = await Config.objects.get(key="app.debug")
        print(f"Verified update: {updated_debug.key} is now {updated_debug.value}")
        
    finally:
        # Clean up - delete all configs
        all_configs = await Config.objects.all()
        for config in all_configs:
            await config.delete()
        
        # Disconnect from the database
        await connection.disconnect()
        print("\nCleaned up and disconnected from SurrealDB")

# Run the async example
if __name__ == "__main__":
    # You can set environment variables before running:
    # export SURREALDB_URL=ws://db:8000/rpc
    # export SURREALDB_NS=test_ns
    # export SURREALDB_DB=test_db
    # export SURREALDB_USER=root
    # export SURREALDB_PASS=root
    
    # Or set them programmatically for testing:
    # os.environ["SURREALDB_URL"] = "ws://db:8000/rpc"
    # os.environ["SURREALDB_NS"] = "test_ns"
    # os.environ["SURREALDB_DB"] = "test_db"
    # os.environ["SURREALDB_USER"] = "root"
    # os.environ["SURREALDB_PASS"] = "root"
    
    asyncio.run(main())