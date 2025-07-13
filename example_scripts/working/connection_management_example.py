"""
Example script demonstrating the connection management features of SurrealEngine.

This script shows how to use:
1. Connection string parsing
2. Connection pooling
3. Timeouts and retries
4. Automatic reconnection
"""
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import asyncio
import logging
from quantumengine import (
    Document,
    StringField,
    IntField,
    create_connection
)
# Advanced connection management features
from quantumengine.connection import (
    SyncConnectionPool,
    AsyncConnectionPool,
    RetryStrategy
)

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define a sample document class
class User(Document):
    name = StringField()
    age = IntField()

    class Meta:
        collection = "user"

# Connection event listener (not yet implemented)
# class MyConnectionListener(ConnectionEventListener):
#     def on_event(self, event_type, connection, **kwargs):
#         logger.info(f"Connection event: {event_type}")
#         if event_type == ConnectionEvent.RECONNECTED:
#             logger.info("Connection has been reestablished!")

# Example of basic connection
async def basic_connection_example():
    logger.info("=== Basic Connection Example ===")
    
    # Create a basic SurrealDB connection using enhanced API
    connection = create_connection(
        name="surrealdb_basic",
        url="ws://localhost:8000/rpc",
        namespace="test_ns",
        database="test_db",
        username="root",
        password="root",
        backend="surrealdb",  # Explicit backend specification
        make_default=True
    )
    
    await connection.connect()
    logger.info("Connected to SurrealDB")
    
    try:
        # Test the connection with a simple operation
        await User.create_table()
        user = User(name="test_user", age=25)
        await user.save()
        logger.info(f"Created user: {user.name}")
        
        # Clean up
        await user.delete()
        
    except Exception as e:
        logger.error(f"Operation failed: {str(e)}")
    finally:
        await connection.disconnect()
        logger.info("Disconnected from SurrealDB")

# Example of using connection pooling (not yet implemented)
# def connection_pool_example():
    logger.info("=== Connection Pooling Example ===")

    # Create a connection pool
    pool = SyncConnectionPool(
        url="ws://localhost:8000/rpc",
        namespace="test",
        database="test",
        username="root",
        password="root",
        pool_size=3,
        max_idle_time=30,
        connect_timeout=5,
        operation_timeout=10,
        retry_limit=2,
        retry_delay=1.0,
        retry_backoff=2.0,
        validate_on_borrow=True
    )

    # Get connections from the pool
    try:
        # Get the first connection
        conn1 = pool.get_connection()
        logger.info("Got connection 1 from the pool")

        # Get the second connection
        conn2 = pool.get_connection()
        logger.info("Got connection 2 from the pool")

        # Get the third connection
        conn3 = pool.get_connection()
        logger.info("Got connection 3 from the pool")

        # Try to get a fourth connection (should wait for a connection to be returned)
        # This would normally time out after connect_timeout seconds
        # But we'll return a connection before that happens
        logger.info("Returning connection 1 to the pool")
        pool.return_connection(conn1)

        logger.info("Getting connection 4 from the pool")
        conn4 = pool.get_connection()
        logger.info("Got connection 4 from the pool")

        # Return all connections to the pool
        logger.info("Returning all connections to the pool")
        pool.return_connection(conn2)
        pool.return_connection(conn3)
        pool.return_connection(conn4)
    except Exception as e:
        logger.error(f"Error in connection pool example: {str(e)}")
    finally:
        # Close the pool
        pool.close()
        logger.info("Connection pool closed")

# Example of using retry strategy (not yet implemented)
# def retry_strategy_example():
    logger.info("=== Retry Strategy Example ===")

    # Create a retry strategy
    retry = RetryStrategy(retry_limit=3, retry_delay=0.5, retry_backoff=2.0)

    # Define an operation that will fail a few times
    attempt = 0
    def failing_operation():
        nonlocal attempt
        attempt += 1
        if attempt <= 2:
            logger.info(f"Operation attempt {attempt} - Failing deliberately")
            raise RuntimeError("Deliberate failure")
        logger.info(f"Operation attempt {attempt} - Succeeding")
        return "Success!"

    # Execute the operation with retry
    try:
        result = retry.execute_with_retry(failing_operation)
        logger.info(f"Operation result: {result}")
    except Exception as e:
        logger.error(f"Operation failed after retries: {str(e)}")

# Example of using async connection pool (not yet implemented)
# async def async_connection_pool_example():
    logger.info("=== Async Connection Pool Example ===")

    # Create an async connection pool
    pool = AsyncConnectionPool(
        url="ws://localhost:8000/rpc",
        namespace="test",
        database="test",
        username="root",
        password="root",
        pool_size=3,
        max_idle_time=30,
        connect_timeout=5,
        operation_timeout=10,
        retry_limit=2,
        retry_delay=1.0,
        retry_backoff=2.0,
        validate_on_borrow=True
    )

    # Get connections from the pool
    try:
        # Get the first connection
        conn1 = await pool.get_connection()
        logger.info("Got async connection 1 from the pool")

        # Get the second connection
        conn2 = await pool.get_connection()
        logger.info("Got async connection 2 from the pool")

        # Get the third connection
        conn3 = await pool.get_connection()
        logger.info("Got async connection 3 from the pool")

        # Return a connection to the pool
        logger.info("Returning async connection 1 to the pool")
        await pool.return_connection(conn1)

        # Get another connection
        logger.info("Getting async connection 4 from the pool")
        conn4 = await pool.get_connection()
        logger.info("Got async connection 4 from the pool")

        # Return all connections to the pool
        logger.info("Returning all async connections to the pool")
        await pool.return_connection(conn2)
        await pool.return_connection(conn3)
        await pool.return_connection(conn4)
    except Exception as e:
        logger.error(f"Error in async connection pool example: {str(e)}")
    finally:
        # Close the pool
        await pool.close()
        logger.info("Async connection pool closed")

# Example of using async retry strategy (not yet implemented)
# async def async_retry_strategy_example():
    logger.info("=== Async Retry Strategy Example ===")

    # Create a retry strategy
    retry = RetryStrategy(retry_limit=3, retry_delay=0.5, retry_backoff=2.0)

    # Define an async operation that will fail a few times
    attempt = 0
    async def failing_async_operation():
        nonlocal attempt
        attempt += 1
        if attempt <= 2:
            logger.info(f"Async operation attempt {attempt} - Failing deliberately")
            raise RuntimeError("Deliberate failure")
        logger.info(f"Async operation attempt {attempt} - Succeeding")
        return "Success!"

    # Execute the operation with retry
    try:
        result = await retry.execute_with_retry_async(failing_async_operation)
        logger.info(f"Async operation result: {result}")
    except Exception as e:
        logger.error(f"Async operation failed after retries: {str(e)}")

# Main function
async def main():
    # Run the basic example (advanced features commented out until implemented)
    try:
        # Basic connection example
        await basic_connection_example()
        
        logger.info("\n=== Advanced Features Not Yet Available ===")
        logger.info("The following features are planned for future implementation:")
        logger.info("- Connection string parsing")
        logger.info("- Connection pooling")
        logger.info("- Retry strategies")
        logger.info("- Connection event listeners")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
