Changelog
=========

All notable changes to QuantumEngine will be documented in this file.

The format is based on `Keep a Changelog`_, and this project adheres to `Semantic Versioning`_.

.. _Keep a Changelog: https://keepachangelog.com/en/1.0.0/
.. _Semantic Versioning: https://semver.org/spec/v2.0.0.html

[Unreleased]
------------

Added
~~~~~
- New features that have been added

Changed
~~~~~~~
- Changes in existing functionality

Deprecated
~~~~~~~~~~
- Features that will be removed in upcoming releases

Removed
~~~~~~~
- Features that have been removed

Fixed
~~~~~
- Bug fixes

Security
~~~~~~~~
- Security improvements

[0.1.3] - 2025-07-25
--------------------

Fixed
~~~~~
- Fixed ``create_table()`` method to use backend name string instead of backend instance
- Made ``create_table()`` method backend-aware to properly delegate to ClickHouse backend
- Added support for WHERE clause in MaterializedDocument Meta class
- Updated MaterializedDocumentMetaclass to handle ``where`` attribute for filtering

Added
~~~~~
- Enhanced MaterializedDocument with comprehensive field tracking for metrics
- Added timing and success tracking fields to ScrapingEvent model
- Improved materialized view SQL generation with proper WHERE clause inclusion

[0.1.2] - 2025-07-13
--------------------

Fixed
~~~~~
- Use Unicode symbols for PyPI-compatible logo display
- Improved README formatting for PyPI package page
- Clean commit history without automated attribution

Added
~~~~~
- MongoEngine acknowledgment in documentation
- Professional badge set in README
- Improved error handling for missing backends

[0.1.1] - 2025-07-13
--------------------

Fixed
~~~~~
- GitHub Actions workflow paths for test execution
- Import statements to use installed package names
- Include GitHub workflows in repository for automation

Added
~~~~~
- Complete GitHub Actions workflow for PyPI publishing
- Comprehensive installation guide with modular options
- Backend registry with helpful error messages

[0.1.0] - 2025-07-13
--------------------

Added
~~~~~
- Initial release of QuantumEngine
- Multi-backend support for SurrealDB and ClickHouse
- Modular installation system with optional dependencies
- Complete type safety with py.typed file
- Async/sync API support
- Document-oriented modeling with field validation
- Query system with Q objects and expressions
- Relationship management for SurrealDB
- Schema management tools
- Performance optimizations
- Professional documentation structure
- Comprehensive test suite
- Docker support
- MIT license

Features
~~~~~~~~
- **Core ODM**: Document classes with field validation
- **Multi-Backend**: Seamless switching between SurrealDB and ClickHouse
- **Type Safety**: Full type hints and mypy compatibility
- **Field Types**: 15+ field types including specialized ones
- **Query System**: Advanced filtering, aggregation, and relations
- **Schema Tools**: Table creation, migration, and drop operations
- **Performance**: Direct record access and bulk operations
- **Installation**: Modular pip packages for specific backends
- **Documentation**: Complete Sphinx documentation with examples
- **Testing**: Working test suite with real database connections

Backend Support
~~~~~~~~~~~~~~~
- **SurrealDB**: Graph relations, transactions, full-text search
- **ClickHouse**: High-performance analytics, bulk operations, time-series