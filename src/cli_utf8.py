"""Force UTF-8 on stdout/stderr for CLI entry points.

Windows consoles default to cp1252, so logging non-ASCII (model names, ✓/✗,
RM prices) raises UnicodeEncodeError. Each pipeline script calls force_utf8_stdio()
at the top of its main() instead of re-rolling the same try/except.
"""

import sys


def force_utf8_stdio() -> None:
    """Reconfigure stdout+stderr to UTF-8; no-op on streams that can't (pipes)."""
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass
