import inspect
import importlib
from typing import Any, Dict, List, Optional, Type, Union, Set

from .document import Document


def get_document_classes(module_name: str) -> List[Type[Document]]:
    """Get all Document classes defined in a module.

    Args:
        module_name: The name of the module to search

    Returns:
        A list of Document classes defined in the module
    """
    module = importlib.import_module(module_name)
    document_classes = []

    for name, obj in inspect.getmembers(module):
        # Check if it's a class and a subclass of Document (but not Document itself)
        if (inspect.isclass(obj) and 
            issubclass(obj, Document) and 
            obj.__module__ == module_name and
            obj != Document):
            document_classes.append(obj)

    return document_classes


async def create_tables_from_module(module_name: str, connection: Optional[Any] = None, 
                                   schemafull: bool = True) -> None:
    """Create tables for all Document classes in a module asynchronously.

    Args:
        module_name: The name of the module containing Document classes
        connection: Optional connection to use
        schemafull: Whether to create SCHEMAFULL tables (default: True)
    """
    document_classes = get_document_classes(module_name)

    for doc_class in document_classes:
        await doc_class.create_table(connection=connection, schemafull=schemafull)


def create_tables_from_module_sync(module_name: str, connection: Optional[Any] = None,
                                  schemafull: bool = True) -> None:
    """Create tables for all Document classes in a module synchronously.

    Args:
        module_name: The name of the module containing Document classes
        connection: Optional connection to use
        schemafull: Whether to create SCHEMAFULL tables (default: True)
    """
    document_classes = get_document_classes(module_name)

    for doc_class in document_classes:
        doc_class.create_table_sync(connection=connection, schemafull=schemafull)


def generate_schema_statements(document_class: Type[Document], schemafull: bool = True) -> List[str]:
    """Generate SurrealDB schema statements for a Document class.

    This function generates DEFINE TABLE and DEFINE FIELD statements for a Document class
    without executing them. This is useful for generating schema migration scripts.

    Args:
        document_class: The Document class to generate statements for
        schemafull: Whether to generate SCHEMAFULL tables (default: True)

    Returns:
        A list of SurrealDB schema statements
    """
    statements = []
    collection_name = document_class._get_collection_name()

    # Generate DEFINE TABLE statement
    schema_type = "SCHEMAFULL" if schemafull else "SCHEMALESS"
    table_stmt = f"DEFINE TABLE {collection_name} {schema_type}"

    # Add comment if available
    if document_class.__doc__:
        # Clean up docstring and escape single quotes
        doc = document_class.__doc__.strip().replace("'", "''")
        if doc:
            table_stmt += f" COMMENT '{doc}'"

    statements.append(table_stmt + ";")

    # Generate DEFINE FIELD statements if schemafull or if field is marked with define_schema=True
    for field_name, field in document_class._fields.items():
        # Skip id field as it's handled by SurrealDB
        if field_name == document_class._meta.get('id_field', 'id'):
            continue

        # Only define fields if schemafull or if field is explicitly marked for schema definition
        if schemafull or field.define_schema:
            field_type = document_class._get_field_type_for_surreal(field)
            field_stmt = f"DEFINE FIELD {field.db_field} ON {collection_name} TYPE {field_type}"

            # Add constraints
            if field.required:
                field_stmt += " ASSERT $value != NONE"

            # Add comment if available
            if hasattr(field, '__doc__') and field.__doc__:
                # Clean up docstring and escape single quotes
                doc = field.__doc__.strip().replace("'", "''")
                if doc:
                    field_stmt += f" COMMENT '{doc}'"

            statements.append(field_stmt + ";")

    return statements


def generate_schema_statements_from_module(module_name: str, schemafull: bool = True) -> Dict[str, List[str]]:
    """Generate SurrealDB schema statements for all Document classes in a module.

    Args:
        module_name: The name of the module containing Document classes
        schemafull: Whether to generate SCHEMAFULL tables (default: True)

    Returns:
        A dictionary mapping class names to lists of SurrealDB schema statements
    """
    document_classes = get_document_classes(module_name)
    schema_statements = {}

    for doc_class in document_classes:
        class_name = doc_class.__name__
        statements = generate_schema_statements(doc_class, schemafull=schemafull)
        schema_statements[class_name] = statements

    return schema_statements


def generate_drop_statements(document_class: Type[Document]) -> List[str]:
    """Generate SurrealDB DROP statements for a Document class.

    This function generates REMOVE TABLE and REMOVE FIELD/INDEX statements for a Document class.
    Useful for generating down migration scripts.

    Args:
        document_class: The Document class to generate drop statements for

    Returns:
        A list of SurrealDB drop statements
    """
    statements = []
    collection_name = document_class._get_collection_name()

    # Generate REMOVE INDEX statements first (indexes should be removed before table)
    indexes = document_class._meta.get('indexes', [])
    for index in indexes:
        if isinstance(index, dict):
            index_name = index.get('name', f"idx_{'_'.join(index['fields'])}")
        else:
            # Simple field index
            index_name = f"idx_{index}"
        
        statements.append(f"REMOVE INDEX IF EXISTS {index_name} ON {collection_name};")

    # Generate REMOVE FIELD statements for defined fields
    for field_name, field in document_class._fields.items():
        # Skip id field as it's handled by SurrealDB
        if field_name == document_class._meta.get('id_field', 'id'):
            continue
        
        statements.append(f"REMOVE FIELD IF EXISTS {field.db_field} ON {collection_name};")

    # Generate REMOVE TABLE statement last
    statements.append(f"REMOVE TABLE IF EXISTS {collection_name};")

    return statements


def generate_drop_statements_from_module(module_name: str) -> Dict[str, List[str]]:
    """Generate SurrealDB DROP statements for all Document classes in a module.

    Args:
        module_name: The name of the module containing Document classes

    Returns:
        A dictionary mapping class names to lists of SurrealDB drop statements
    """
    document_classes = get_document_classes(module_name)
    drop_statements = {}

    for doc_class in document_classes:
        class_name = doc_class.__name__
        statements = generate_drop_statements(doc_class)
        drop_statements[class_name] = statements

    return drop_statements


def generate_migration_statements(old_document_class: Type[Document], 
                                 new_document_class: Type[Document],
                                 schemafull: bool = True) -> Dict[str, List[str]]:
    """Generate migration statements between two versions of a Document class.

    Args:
        old_document_class: The old version of the Document class
        new_document_class: The new version of the Document class
        schemafull: Whether to generate statements for SCHEMAFULL tables

    Returns:
        A dictionary with 'up' and 'down' migration statements
    """
    old_collection = old_document_class._get_collection_name()
    new_collection = new_document_class._get_collection_name()
    
    up_statements = []
    down_statements = []

    # Handle table rename
    if old_collection != new_collection:
        # SurrealDB doesn't have RENAME TABLE, so we need to handle this differently
        up_statements.append(f"-- Table renamed from {old_collection} to {new_collection}")
        up_statements.append(f"-- Note: SurrealDB doesn't support table renaming directly.")
        up_statements.append(f"-- You may need to create new table and migrate data manually.")
        down_statements.append(f"-- Reverse table rename from {new_collection} to {old_collection}")

    # Get field differences
    old_fields = set(old_document_class._fields.keys())
    new_fields = set(new_document_class._fields.keys())
    
    # Fields to add (in new but not in old)
    fields_to_add = new_fields - old_fields
    for field_name in fields_to_add:
        field = new_document_class._fields[field_name]
        if field_name == new_document_class._meta.get('id_field', 'id'):
            continue
        
        if schemafull or field.define_schema:
            field_type = new_document_class._get_field_type_for_surreal(field)
            field_stmt = f"DEFINE FIELD {field.db_field} ON {new_collection} TYPE {field_type}"
            
            if field.required:
                field_stmt += " ASSERT $value != NONE"
            
            up_statements.append(field_stmt + ";")
            down_statements.append(f"REMOVE FIELD IF EXISTS {field.db_field} ON {new_collection};")

    # Fields to remove (in old but not in new)
    fields_to_remove = old_fields - new_fields
    for field_name in fields_to_remove:
        field = old_document_class._fields[field_name]
        if field_name == old_document_class._meta.get('id_field', 'id'):
            continue
        
        up_statements.append(f"REMOVE FIELD IF EXISTS {field.db_field} ON {old_collection};")
        
        # For down migration, we need to recreate the field
        if schemafull or field.define_schema:
            field_type = old_document_class._get_field_type_for_surreal(field)
            field_stmt = f"DEFINE FIELD {field.db_field} ON {old_collection} TYPE {field_type}"
            
            if field.required:
                field_stmt += " ASSERT $value != NONE"
            
            down_statements.append(field_stmt + ";")

    # Handle index changes
    old_indexes = old_document_class._meta.get('indexes', [])
    new_indexes = new_document_class._meta.get('indexes', [])
    
    # Convert to comparable format
    def normalize_index(index, table_name):
        if isinstance(index, dict):
            return (index.get('name', f"idx_{'_'.join(index['fields'])}"), tuple(index['fields']), index.get('unique', False))
        else:
            return (f"idx_{index}", (index,), False)
    
    old_index_set = {normalize_index(idx, old_collection) for idx in old_indexes}
    new_index_set = {normalize_index(idx, new_collection) for idx in new_indexes}
    
    # Indexes to add
    indexes_to_add = new_index_set - old_index_set
    for index_name, fields, unique in indexes_to_add:
        index_stmt = f"DEFINE INDEX {index_name} ON {new_collection} COLUMNS {', '.join(fields)}"
        if unique:
            index_stmt += " UNIQUE"
        up_statements.append(index_stmt + ";")
        down_statements.append(f"REMOVE INDEX IF EXISTS {index_name} ON {new_collection};")
    
    # Indexes to remove
    indexes_to_remove = old_index_set - new_index_set
    for index_name, fields, unique in indexes_to_remove:
        up_statements.append(f"REMOVE INDEX IF EXISTS {index_name} ON {old_collection};")
        index_stmt = f"DEFINE INDEX {index_name} ON {old_collection} COLUMNS {', '.join(fields)}"
        if unique:
            index_stmt += " UNIQUE"
        down_statements.append(index_stmt + ";")

    return {
        'up': up_statements,
        'down': down_statements
    }


async def drop_tables_from_module(module_name: str, connection: Optional[Any] = None) -> None:
    """Drop tables for all Document classes in a module asynchronously.

    Args:
        module_name: The name of the module containing Document classes
        connection: Optional connection to use
    """
    document_classes = get_document_classes(module_name)

    # Drop in reverse order to handle dependencies
    for doc_class in reversed(document_classes):
        await doc_class.drop_table(connection=connection)


def drop_tables_from_module_sync(module_name: str, connection: Optional[Any] = None) -> None:
    """Drop tables for all Document classes in a module synchronously.

    Args:
        module_name: The name of the module containing Document classes
        connection: Optional connection to use
    """
    document_classes = get_document_classes(module_name)

    # Drop in reverse order to handle dependencies
    for doc_class in reversed(document_classes):
        doc_class.drop_table_sync(connection=connection)
