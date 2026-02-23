# AGENTS.md - Lead Scraping Automation

## Project Overview

This is a Python-based lead scraping automation tool that uses Playwright to scrape business leads from Google Maps and search engines. The project extracts business names, phone numbers, websites, addresses, and emails.

## Build & Run Commands

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

### Running the Scraper
```bash
# Run the main scraper (interactive mode)
python lead_scraper.py

# Or with environment variables
export DEFAULT_LOCATION="Los Angeles"
export LOG_LEVEL=DEBUG
python lead_scraper.py
```

### Testing
```bash
# Run all tests
pytest

# Run a single test file
pytest tests/test_scraper.py

# Run a single test function
pytest tests/test_scraper.py::TestParseSearchPrompt::test_parse_maps_with_in_keyword -v

# Run tests matching a pattern
pytest -k "parse"

# Run with verbose output
pytest -v

# Run and stop on first failure
pytest -x
```

### Linting & Code Quality
```bash
# Install ruff for linting
pip install ruff

# Run ruff linter
ruff check .

# Run ruff with auto-fix
ruff check --fix .

# Format code with ruff
ruff format .
```

## Code Style Guidelines

### General Conventions
- Language: Python 3.x with async/await patterns
- Minimum Python version: 3.10
- Type hints required for all function signatures
- Maximum line length: 100 characters

### Imports
- Standard library imports first
- Third-party imports second
- Local imports last
- Group imports by type with blank lines between groups
- Sort imports alphabetically within each group

```python
# Correct order:
import asyncio
import logging
import os
import random
import re
import time
from functools import wraps
,from typing import Any Callable, TypeVar

import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
```

### Naming Conventions
- **Functions/variables**: snake_case (e.g., `human_like_scroll`, `business_data`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `SEARCH_CONFIG`, `PHONE_REGEX`)
- **Classes**: PascalCase (e.g., `LeadScraper`, `RateLimiter`)
- **Private functions**: prefix with underscore (e.g., `_private_function`)

### Type Hints
Always use type hints for function parameters and return values:

```python
# Good
async def scrape_listing_details(page, context, listing_element) -> dict[str, str] | None:
    ...

def process_and_clean_data(raw_data: list[dict[str, Any]]) -> pd.DataFrame:
    ...

# Avoid
def process_and_clean_data(raw_data):
    ...
```

### Error Handling
- **Never use bare `except:` clauses** - always specify exception type
- Use specific exception types (e.g., `ValueError`, `TimeoutError`)
- Handle exceptions at the appropriate level
- Log errors appropriately or let them propagate if they should bubble up

```python
# Good
try:
    await page.goto(url, timeout=60000)
except TimeoutError:
    logger.warning(f"Page load timed out for {url}")
except Exception as e:
    logger.error(f"Failed to load {url}: {e}")

# Bad - avoid this
try:
    await page.goto(url)
except:
    pass
```

### Async/Await Patterns
- Use `async def` for functions that await
- Always use `async with` for context managers that support it
- Prefer `asyncio.run()` for main entry point
- Use descriptive variable names for async contexts (e.g., `async with async_playwright() as p:`)

### Code Formatting
- Use f-strings for string formatting
- Use implicit concatenation for long strings when appropriate
- Add spaces around operators: `a + b` not `a+b`
- No trailing whitespace
- One blank line between top-level definitions

### Docstrings
- Use triple quotes `"""` for docstrings
- Follow Google-style docstring format for complex functions
- Keep docstrings concise but informative

```python
async def scrape_google_maps(
    keywords: str,
    location: str,
    max_scrolls: int = 15,
) -> list[dict[str, str]]:
    """Scrape business listings from Google Maps.
    
    Args:
        keywords: Business type to search for
        location: Geographic location to search in
        max_scrolls: Maximum number of scroll operations
    
    Returns:
        List of dictionaries containing business lead data
    """
```

### Data Structures
- Use list/dict comprehensions when appropriate
- Use `pd.DataFrame` for tabular data processing
- Use sets for membership testing (e.g., `seen_businesses`)

### Playwright Specific
- Always use async API: `from playwright.async_api import ...`
- Use explicit waits over sleep when possible
- Use `wait_until` parameter for page navigation
- Launch browsers with stealth settings to avoid detection

### File Organization
- Main logic in `lead_scraper.py`
- Configuration in `config.py` and `.env.example`
- Tests in `tests/` directory
- Helper classes at module level (e.g., `RateLimiter`)
- Helper functions before main functions
- `main()` entry point at the bottom of the file

## Project Structure

```
/home/rayan/coding/lead_scraping_automation/
├── lead_scraper.py       # Main scraper module
├── config.py             # Configuration dataclass
├── requirements.txt      # Python dependencies
├── .env.example          # Environment variable template
├── venv/                 # Virtual environment
├── tests/                # Test suite
│   ├── __init__.py
│   ├── conftest.py       # Pytest fixtures
│   └── test_scraper.py   # Unit tests
└── *.png                 # Debug/screenshot outputs
```

## Key Features

### Rate Limiting
The `RateLimiter` class prevents request spam with configurable delays:
- `min_delay`: Minimum seconds between requests (default: 1.0)
- `max_delay`: Maximum random delay added (default: 2.5)

### Retry Logic
The `@retry_on_failure` decorator automatically retries failed operations:
- `max_retries`: Number of retry attempts (default: 3)
- `delay`: Base delay between retries (default: 1.0s)

### Configuration
- Use `config.py` for programmatic configuration
- Use `.env` file for environment variables
- See `.env.example` for available options

## Common Tasks

### Adding a New Search Source
1. Add new configuration keys to `SEARCH_CONFIG` in `lead_scraper.py`
2. Create a new async function following existing patterns
3. Update `main()` to handle the new search type
4. Add appropriate selectors for the new source

### Modifying Data Extraction
1. Update `scrape_listing_details()` for new fields
2. Modify `process_and_clean_data()` to handle new data
3. Update Excel export column list if needed

### Adding New Tests
1. Add fixtures to `tests/conftest.py` if needed
2. Create test class in `tests/test_scraper.py`
3. Use descriptive test names: `test_<function>_<scenario>`

## Notes for Agents

- This project uses Playwright stealth mode to avoid bot detection
- Google Maps scraping may break if Google changes their DOM structure
- The project outputs results to `leads_output.xlsx`
- Use headless=False for debugging to see browser interactions
- Always use `logger` instead of `print()` for logging
- All async functions should have proper error handling
