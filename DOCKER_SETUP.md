# QuantumORM Docker Development Setup

This setup provides a local development environment with both ClickHouse and SurrealDB for testing the multi-backend functionality.

## Quick Start

1. **Start the development environment:**
   ```bash
   ./docker-dev.sh start
   ```

2. **Check service status:**
   ```bash
   ./docker-dev.sh status
   ```

3. **Run the ClickHouse backend tests:**
   ```bash
   ./docker-dev.sh test
   ```

## Services

### ClickHouse
- **HTTP Interface**: http://localhost:8123
- **Native Protocol**: localhost:9000
- **MySQL Protocol**: localhost:9004
- **Default User**: `default` (no password)
- **Dev User**: `dev` (password: `dev123`)

### SurrealDB
- **HTTP Interface**: http://localhost:8000
- **Default Credentials**: `root/root`
- **Database**: Memory storage (for development - no persistence between restarts)
- **WebSocket Interface**: ws://localhost:8000/rpc

## Docker Management Commands

### Start Services
```bash
# Start all services
./docker-dev.sh start

# Start only ClickHouse
./docker-dev.sh start clickhouse

# Start only SurrealDB
./docker-dev.sh start surrealdb
```

### Stop Services
```bash
./docker-dev.sh stop
```

### Restart Services
```bash
./docker-dev.sh restart
```

### View Logs
```bash
# All service logs
./docker-dev.sh logs

# ClickHouse logs only
./docker-dev.sh logs clickhouse

# SurrealDB logs only
./docker-dev.sh logs surrealdb
```

### Connect to ClickHouse
```bash
# Connect to ClickHouse client
./docker-dev.sh clickhouse

# Or manually:
docker-compose exec clickhouse clickhouse-client
```

### Clean Up
```bash
# Remove all containers and volumes
./docker-dev.sh cleanup
```

## Manual Docker Commands

If you prefer to use Docker Compose directly:

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Connect to ClickHouse
docker-compose exec clickhouse clickhouse-client

# Connect to SurrealDB
curl -X POST http://localhost:8000/sql \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'root:root' | base64)" \
  -d '{"query": "INFO FOR DB;"}'
```

## Testing the ClickHouse Backend

### Run the Test Script
```bash
./docker-dev.sh test
```

### Manual Testing
```python
import asyncio
import clickhouse_connect

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='',
    database='default'
)

# Test connection
result = client.query('SELECT version()').result_rows
print(f"ClickHouse version: {result[0][0]}")
```

## Configuration

### ClickHouse Configuration
- **Custom config**: `docker/clickhouse/config.xml`
- **User settings**: `docker/clickhouse/users.xml`
- **Data volume**: `clickhouse-data`
- **Logs volume**: `clickhouse-logs`

### SurrealDB Configuration
- **Storage**: Memory-based (for development)
- **Persistence**: Data is reset on container restart
- **Change to persistent storage**: See "Production Considerations" below

## Development Workflow

1. **Start the environment:**
   ```bash
   ./docker-dev.sh start
   ```

2. **Develop and test your backend code:**
   ```bash
   python example_scripts/clickhouse_backend_example.py
   ```

3. **Monitor logs if needed:**
   ```bash
   ./docker-dev.sh logs clickhouse
   ```

4. **When done, stop the services:**
   ```bash
   ./docker-dev.sh stop
   ```

## Troubleshooting

### ClickHouse Connection Issues
- Ensure port 8123 is available
- Check if ClickHouse is running: `./docker-dev.sh status`
- View logs: `./docker-dev.sh logs clickhouse`

### SurrealDB Connection Issues
- Ensure port 8000 is available
- Check if SurrealDB is running: `./docker-dev.sh status`
- View logs: `./docker-dev.sh logs surrealdb`

### Docker Issues
- Ensure Docker is running
- Check available ports: `netstat -an | grep LISTEN`
- Restart Docker if needed

### Performance Issues
- Increase Docker memory limits if needed
- Adjust ClickHouse memory settings in `docker/clickhouse/config.xml`

## Production Considerations

This setup is for **development only**. For production:

1. **Security**: Change default passwords and restrict network access
2. **Persistence**: Use proper volume mounting for data persistence
3. **Performance**: Tune ClickHouse settings for your workload
4. **Monitoring**: Add proper logging and monitoring
5. **Backup**: Implement backup strategies for your data

### SurrealDB Persistent Storage

The development setup uses memory storage for simplicity. To enable persistent storage:

1. **Update docker-compose.yml**:
   ```yaml
   surrealdb:
     # ... other settings ...
     command: start --user root --pass root --bind 0.0.0.0:8000 rocksdb:/data/database.db
     volumes:
       - surrealdb-data:/data
   ```

2. **Add the volume**:
   ```yaml
   volumes:
     surrealdb-data:
       driver: local
   ```

3. **Set proper permissions** (if needed):
   ```bash
   docker-compose exec surrealdb chown -R 1000:1000 /data
   ```

## Next Steps

Once your development environment is running:

1. Test the ClickHouse backend with your data models
2. Implement the SurrealDB backend refactoring
3. Update the Document class to use backend abstraction
4. Run comprehensive tests with both backends
5. Deploy to your production environment