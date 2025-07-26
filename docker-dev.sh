#!/bin/bash

# Development script for managing QuantumORM Docker environment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Function to start services
start_services() {
    print_header "Starting QuantumORM Development Environment"
    check_docker
    
    if [ "$1" = "clickhouse" ]; then
        print_status "Starting ClickHouse only..."
        docker-compose up -d clickhouse
    elif [ "$1" = "surrealdb" ]; then
        print_status "Starting SurrealDB only..."
        docker-compose up -d surrealdb
    elif [ "$1" = "redis" ]; then
        print_status "Starting Redis only..."
        docker-compose up -d redis
    else
        print_status "Starting all services..."
        docker-compose up -d
    fi
    
    print_status "Waiting for services to be ready..."
    sleep 5
    
    # Check service health
    check_services
}

# Function to stop services
stop_services() {
    print_header "Stopping QuantumORM Development Environment"
    docker-compose down
    print_status "Services stopped"
}

# Function to restart services
restart_services() {
    print_header "Restarting QuantumORM Development Environment"
    docker-compose down
    docker-compose up -d
    sleep 5
    check_services
}

# Function to check service status
check_services() {
    print_header "Service Status"
    
    # Check ClickHouse
    if docker-compose ps clickhouse | grep -q "Up"; then
        if curl -s "http://localhost:8123/" > /dev/null 2>&1; then
            print_status "ClickHouse: ✓ Running and accessible at http://localhost:8123"
        else
            print_warning "ClickHouse: Container running but not accessible"
        fi
    else
        print_warning "ClickHouse: Not running"
    fi
    
    # Check SurrealDB
    if docker-compose ps surrealdb | grep -q "Up"; then
        if curl -s "http://localhost:8000/health" > /dev/null 2>&1; then
            print_status "SurrealDB: ✓ Running and accessible at http://localhost:8000"
        else
            print_warning "SurrealDB: Container running but not accessible"
        fi
    else
        print_warning "SurrealDB: Not running"
    fi
    
    # Check Redis
    if docker-compose ps redis | grep -q "Up"; then
        if docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; then
            print_status "Redis: ✓ Running and accessible at localhost:6379"
        else
            print_warning "Redis: Container running but not accessible"
        fi
    else
        print_warning "Redis: Not running"
    fi
}

# Function to show logs
show_logs() {
    if [ -n "$1" ]; then
        print_header "Logs for $1"
        docker-compose logs -f "$1"
    else
        print_header "All Service Logs"
        docker-compose logs -f
    fi
}

# Function to run tests
run_tests() {
    print_header "Running ClickHouse Backend Tests"
    
    # Check if ClickHouse is running
    if ! curl -s "http://localhost:8123/" > /dev/null 2>&1; then
        print_error "ClickHouse is not running. Start it with: ./docker-dev.sh start clickhouse"
        exit 1
    fi
    
    print_status "Running ClickHouse backend test..."
    python example_scripts/clickhouse_backend_example.py
}

# Function to connect to ClickHouse
connect_clickhouse() {
    print_header "Connecting to ClickHouse"
    
    if ! docker-compose ps clickhouse | grep -q "Up"; then
        print_error "ClickHouse is not running. Start it with: ./docker-dev.sh start clickhouse"
        exit 1
    fi
    
    print_status "Connecting to ClickHouse client..."
    docker-compose exec clickhouse clickhouse-client
}

# Function to connect to Redis
connect_redis() {
    print_header "Connecting to Redis"
    
    if ! docker-compose ps redis | grep -q "Up"; then
        print_error "Redis is not running. Start it with: ./docker-dev.sh start redis"
        exit 1
    fi
    
    print_status "Connecting to Redis CLI..."
    docker-compose exec redis redis-cli
}

# Function to clean up
cleanup() {
    print_header "Cleaning Up QuantumORM Development Environment"
    print_warning "This will remove all containers and volumes!"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose down -v
        docker-compose rm -f
        print_status "Cleanup completed"
    else
        print_status "Cleanup cancelled"
    fi
}

# Function to show help
show_help() {
    cat << EOF
QuantumORM Development Environment Manager

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    start [service]    Start services (clickhouse, surrealdb, redis, or all)
    stop              Stop all services
    restart           Restart all services
    status            Check service status
    logs [service]    Show logs (for specific service or all)
    test              Run ClickHouse backend tests
    clickhouse        Connect to ClickHouse client
    redis             Connect to Redis CLI
    cleanup           Remove all containers and volumes
    help              Show this help

Examples:
    $0 start              # Start all services
    $0 start clickhouse   # Start only ClickHouse
    $0 start redis        # Start only Redis
    $0 logs redis         # Show Redis logs
    $0 test               # Run backend tests
    $0 clickhouse         # Connect to ClickHouse
    $0 redis              # Connect to Redis CLI

EOF
}

# Main script logic
case "$1" in
    start)
        start_services "$2"
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    status)
        check_services
        ;;
    logs)
        show_logs "$2"
        ;;
    test)
        run_tests
        ;;
    clickhouse)
        connect_clickhouse
        ;;
    redis)
        connect_redis
        ;;
    cleanup)
        cleanup
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac