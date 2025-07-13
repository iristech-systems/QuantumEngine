# QuantumEngine Documentation

This directory contains the Sphinx documentation for QuantumEngine.

## Building the Documentation

### Prerequisites

Install the documentation dependencies:

```bash
cd docs
pip install -r requirements.txt
```

### Build HTML Documentation

```bash
# Build static HTML
make html

# View the documentation
open _build/html/index.html
```

### Live Reload Development

For live reload during development:

```bash
# Install sphinx-autobuild first
pip install sphinx-autobuild

# Start live reload server
make livehtml
```

The documentation will be available at http://127.0.0.1:8000 and will automatically rebuild when you save changes.

### Other Build Targets

```bash
# Clean build directory
make clean

# Build with warnings as errors (for CI)
make strict

# Build PDF (requires LaTeX)
make latexpdf

# Build EPUB
make epub
```

## Documentation Structure

- `index.rst` - Main landing page
- `installation.rst` - Installation guide
- `quickstart.rst` - Quick start tutorial
- `tutorial.rst` - Comprehensive tutorial
- `api/` - Auto-generated API documentation
- `backends/` - Backend-specific documentation
- `fields/` - Field type documentation
- `queries/` - Query system documentation
- `examples/` - Example code and use cases
- `contributing.rst` - Contribution guidelines
- `changelog.rst` - Version history

## Theme and Styling

The documentation uses the `ansys_sphinx_theme` with custom styling in `_static/custom.css`. The theme provides:

- Responsive design
- Dark/light mode support
- Code syntax highlighting
- Copy button for code blocks
- Navigation breadcrumbs
- GitHub integration

## Writing Documentation

### RestructuredText (RST)

Most documentation files use RST format. Key syntax:

```rst
Section Title
=============

Subsection
----------

**Bold text** and *italic text*

.. code-block:: python

   # Python code example
   from quantumengine import Document

.. note::
   This is a note admonition.

.. warning::
   This is a warning admonition.
```

### Markdown Support

Some files can use Markdown format (configured with MyST parser):

```markdown
# Section Title

## Subsection

**Bold text** and *italic text*

```python
# Python code example
from quantumengine import Document
```

```{note}
This is a note admonition.
```
```

### API Documentation

API documentation is auto-generated from docstrings using Sphinx autodoc:

```python
def example_function(param1: str, param2: int = 0) -> bool:
    """
    Brief description of the function.
    
    Args:
        param1: Description of param1
        param2: Description of param2, defaults to 0
        
    Returns:
        Description of return value
        
    Raises:
        ValueError: When param1 is invalid
        
    Example:
        >>> example_function("test", 5)
        True
    """
    pass
```

## Contributing to Documentation

1. Check for typos and grammatical errors
2. Ensure code examples are working and up-to-date
3. Add examples for new features
4. Update API documentation when adding new classes/methods
5. Test documentation builds locally before submitting PRs

## Deployment

Documentation is automatically built and deployed using GitHub Actions when changes are pushed to the main branch.