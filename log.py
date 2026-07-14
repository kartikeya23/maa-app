"""
Logging setup for the MAA app.

All modules log through child loggers of the 'maa' root
(e.g. logging.getLogger("maa.db")). setup_logging() is idempotent:
the first call attaches handlers, later calls only adjust the level.
"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path

LOG_DIR = Path(__file__).parent / "logs"
LOG_FILE = LOG_DIR / "maa.log"

_FILE_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    console: bool = False,
    verbose: bool = False,
    log_file: Path | None = None,
) -> logging.Logger:
    """Configure the 'maa' root logger.

    console: also emit message-only lines to stdout (for CLI tools).
    verbose: DEBUG level (also enabled by env MAA_DEBUG=1).
    log_file: override log path (tests); defaults to logs/maa.log.
    """
    logger = logging.getLogger("maa")
    debug = verbose or os.environ.get("MAA_DEBUG") == "1"
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    if logger.handlers:
        return logger

    path = log_file or LOG_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.TimedRotatingFileHandler(
        path, when="midnight", backupCount=14, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(_FILE_FORMAT, _DATE_FORMAT))
    logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(console_handler)

    return logger
