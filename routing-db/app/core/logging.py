"""routing-db/app/core/logging.py"""
from __future__ import annotations

import logging
import sys
from typing import Any

from pythonjsonlogger import jsonlogger


def configure_logging(level: str = "INFO", service: str = "routing-db") -> None:
    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    formatter.add_fields = lambda log_record, record, message_dict: (  # type: ignore[method-assign]
        log_record.update({"service": service})
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Quieten noisy libs
    for lib in ("uvicorn.access", "sqlalchemy.engine"):
        logging.getLogger(lib).setLevel(logging.WARNING)
