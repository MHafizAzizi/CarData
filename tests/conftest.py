"""Pytest bootstrap.

The runnable pipeline files use numeric prefixes (1_scrape.py, 2_migrate.py,
3_clean.py) so the run order is obvious in the file listing. A leading digit is
not a valid Python identifier, so these modules cannot be imported with a normal
`import` statement. This conftest loads them by file path and registers them in
sys.modules under import-friendly aliases so the test suite can do
`from scraper import ...` / `from clean import ...` unchanged.
"""

import importlib.util
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src"

# Make src/ importable so intra-package imports (eagle_client, mudah_client, db)
# resolve while the numeric-prefixed modules are being executed.
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def _load(filename: str, alias: str) -> None:
    spec = importlib.util.spec_from_file_location(alias, _SRC / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)


_load("1_scrape.py", "scraper")
_load("3_clean.py", "clean")
