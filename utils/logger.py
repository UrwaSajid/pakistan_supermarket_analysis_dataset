"""
Structured, rotating file logger for the scraping pipeline.
Each scraper gets its own named logger that writes to both console and file.
"""

import logging
import logging.handlers
import sys
from pathlib import Path

# Ensure stdout/stderr can handle Unicode on Windows (cp1252 terminals fail otherwise)
for _stream in (sys.stdout, sys.stderr):
    try:
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


class _SafeStreamHandler(logging.StreamHandler):
    """StreamHandler that never raises UnicodeEncodeError."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            stream = self.stream
            try:
                stream.write(msg + self.terminator)
            except UnicodeEncodeError:
                safe = msg.encode("utf-8", errors="replace").decode(
                    stream.encoding or "utf-8", errors="replace"
                )
                stream.write(safe + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

from config.settings import LOGS_DIR

_LOGGERS: dict[str, logging.Logger] = {}
_FMT = "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """
    Return (or create) a named logger.

    Parameters
    ----------
    name  : Logical name, e.g. ``"metro"``, ``"pipeline"``.
    level : Logging level for file handler (console always INFO+).
    """
    if name in _LOGGERS:
        return _LOGGERS[name]

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # ── Console handler (INFO and above) ──────────────────────────────────────
    ch = _SafeStreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(_FMT, _DATE_FMT))

    # ── Rotating file handler (DEBUG and above, 10 MB × 5 files) ─────────────
    log_path = LOGS_DIR / f"{name}.log"
    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(_FMT, _DATE_FMT))

    logger.addHandler(ch)
    logger.addHandler(fh)
    _LOGGERS[name] = logger
    return logger
