from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass

log = logging.getLogger("peter.daemon")


@dataclass(frozen=True)
class DaemonConfig:
    tick_seconds: float = 5.0
    crash_backoff_seconds: float = 10.0


class _Stop:
    requested: bool = False


def _handle_stop(signum: int, _frame) -> None:
    _Stop.requested = True
    log.info("Received signal %s; shutting down...", signum)


def run(*, cfg: DaemonConfig | None = None) -> int:
    """Run a simple long-lived daemon loop.

    This is intentionally minimal: stdout logging, signal handling,
    and robust retry semantics. Real work should be implemented in a
    dedicated "tick" function or scheduled jobs.
    """

    if cfg is None:
        cfg = DaemonConfig()

    # Signal handlers must be set in the main thread.
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    log.info("PETER daemon starting (tick=%.1fs)", cfg.tick_seconds)

    while not _Stop.requested:
        try:
            # TODO: implement one unit of work here.
            log.debug("tick")
            time.sleep(cfg.tick_seconds)
        except Exception:
            log.exception("Unhandled error in daemon loop; retrying in %.1fs", cfg.crash_backoff_seconds)
            time.sleep(cfg.crash_backoff_seconds)

    log.info("PETER daemon stopped")
    return 0
