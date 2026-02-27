from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """Simple structured-ish logging to stdout."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )
