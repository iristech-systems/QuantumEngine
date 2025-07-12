# Contributing to QuantumORM

Thank you for your interest in contributing to QuantumORM! This document provides guidelines and instructions for contributing to the project.

## 🚀 Quick Start

### Prerequisites
- Python 3.12+ 
- Docker and Docker Compose
- uv package manager

### Development Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd quantumengine
   ```

2. **Install dependencies:**
   ```bash
   uv sync
   ```

3. **Set up environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your local configuration if needed
   ```

4. **Start development databases:**
   ```bash
   ./docker-dev.sh start
   ```

5. **Verify setup:**
   ```bash
   ./docker-dev.sh status
   ```

## 🔧 Development Environment

### Docker Services

QuantumORM uses Docker for local development with these services:

- **SurrealDB**: Available at `ws://localhost:8000/rpc`
  - Username: `root`
  - Password: `root`
  - Default namespace: `test_ns`
  - Default database: `test_db`

- **ClickHouse**: Available at `http://localhost:8123`
  - Username: `default`
  - Password: `` (empty)
  - Default database: `default`

### Docker Commands

```bash
# Start all services
./docker-dev.sh start

# Start specific service
./docker-dev.sh start surrealdb
./docker-dev.sh start clickhouse

# Check service status
./docker-dev.sh status

# View logs
./docker-dev.sh logs
./docker-dev.sh logs surrealdb

# Connect to ClickHouse CLI
./docker-dev.sh clickhouse

# Run tests
./docker-dev.sh test

# Stop services
./docker-dev.sh stop

# Clean up (removes volumes)
./docker-dev.sh cleanup
```

## 🧪 Testing

### Running Tests

**Working Tests** (known to pass):
```bash
# Multi-backend tests
uv run python tests/working/test_multi_backend_real_connections.py
uv run python tests/working/test_clickhouse_simple_working.py
uv run python tests/working/test_working_surrealdb_backend.py

# Field and feature tests
uv run python tests/working/test_multi_backend_features_working.py
```

**Example Scripts**:
```bash
# Basic examples
uv run python example_scripts/working/basic_crud_example.py
uv run python example_scripts/working/multi_backend_example.py
uv run python example_scripts/working/advanced_features_example.py

# Specific feature examples
uv run python example_scripts/working/relation_example.py
uv run python example_scripts/working/query_expressions_example.py
uv run python example_scripts/working/sync_api_example.py
```

### Test Database Requirements

Tests require the Docker services to be running:
```bash
./docker-dev.sh start
```

## 📚 Documentation

### Building Documentation

```bash
cd docs
uv run make html
```

View the documentation at `docs/_build/html/index.html`.

### Documentation Guidelines

- Use Google-style docstrings for all classes and methods
- Include examples in docstrings using `>>>` format
- Update API documentation when adding new features
- Ensure all public methods have comprehensive docstrings

## 🎯 Code Guidelines

### Code Style

- Follow PEP 8 style guidelines
- Use type hints for all function parameters and return values
- Write descriptive variable and function names
- Keep functions focused and single-purpose

### Backend Compatibility

QuantumORM supports multiple backends. When adding features:

1. **Check backend capabilities** using the capability detection system
2. **Implement for both backends** when possible (SurrealDB and ClickHouse)
3. **Document backend-specific limitations** in docstrings
4. **Test with both backends** when applicable

Example:
```python
# Check if backend supports feature
if connection.backend.supports_graph_relations():
    # Implement graph relations
    pass
else:
    # Provide alternative or skip
    pass
```

### Field Types

When adding new field types:

1. **Inherit from appropriate base class** (`Field`, `StringField`, etc.)
2. **Implement validation logic** in `validate()` method
3. **Handle database conversion** with `to_db()` and `from_db()` methods
4. **Add comprehensive tests** for both backends
5. **Document backend support** clearly

## 🔄 Pull Request Process

1. **Fork the repository** and create a feature branch
2. **Make your changes** following the code guidelines
3. **Add tests** for new functionality
4. **Update documentation** as needed
5. **Ensure all tests pass** with both backends
6. **Submit a pull request** with a clear description

### Pull Request Requirements

- [ ] All existing tests pass
- [ ] New tests added for new functionality
- [ ] Documentation updated (docstrings and docs/)
- [ ] Code follows style guidelines
- [ ] Backend compatibility considered
- [ ] No cloud credentials or secrets included

## 🐛 Bug Reports

When reporting bugs:

1. **Use the issue template** if available
2. **Include minimal reproduction code**
3. **Specify backend and versions** (Python, SurrealDB, ClickHouse)
4. **Include error messages and logs**
5. **Test with latest Docker setup**

## 💡 Feature Requests

For new features:

1. **Check existing issues** for similar requests
2. **Describe the use case** clearly
3. **Consider backend compatibility**
4. **Provide example usage** if possible
5. **Discuss implementation approach**

## 🔒 Security

- **Never commit credentials** or API keys
- **Use environment variables** for configuration
- **Report security issues privately** to maintainers
- **Keep dependencies updated**

## 📝 Commit Guidelines

Use conventional commit format:

```
type(scope): description

feat(fields): add IPv6 support to IPAddressField
fix(connection): handle connection timeout properly
docs(readme): update installation instructions
test(backends): add ClickHouse performance tests
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `style`, `chore`

## 🤝 Community

- **Be respectful** and inclusive
- **Help others** with questions and issues
- **Share examples** and use cases
- **Provide constructive feedback**

## 📦 Release Process

For maintainers:

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create release notes
4. Tag release
5. Publish to PyPI

## 📞 Getting Help

- **GitHub Issues**: Bug reports and feature requests
- **Discussions**: Questions and community support
- **Documentation**: Comprehensive API reference
- **Examples**: Working code examples in `example_scripts/working/`

Thank you for contributing to QuantumORM! 🎉