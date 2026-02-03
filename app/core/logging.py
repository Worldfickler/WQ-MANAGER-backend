from __future__ import annotations

import logging
from logging.config import dictConfig
from pathlib import Path

from app.core.config import settings


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def setup_logging() -> None:
    base_dir = Path(__file__).resolve().parents[2]
    log_dir = base_dir / settings.LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    info_log = log_dir / "app.log"
    error_log = log_dir / "error.log"
    console_log = log_dir / "console.log"

    dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "max_warning": {
                "()": "app.core.logging.MaxLevelFilter",
                "max_level": logging.WARNING,
            }
        },
        "formatters": {
            "default": {
                "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            }
        },
        "handlers": {
            "info_file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": settings.LOG_LEVEL,
                "formatter": "default",
                "filename": str(info_log),
                "when": "midnight",
                "backupCount": 30,
                "encoding": "utf-8",
                "filters": ["max_warning"],
            },
            "error_file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": "ERROR",
                "formatter": "default",
                "filename": str(error_log),
                "when": "midnight",
                "backupCount": 30,
                "encoding": "utf-8",
            },
            "console_file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": "DEBUG",
                "formatter": "default",
                "filename": str(console_log),
                "when": "midnight",
                "backupCount": 30,
                "encoding": "utf-8",
            },
            "console": {
                "class": "logging.StreamHandler",
                "level": settings.LOG_LEVEL,
                "formatter": "default",
            },
        },
        "root": {
            "handlers": ["info_file", "error_file", "console_file", "console"],
            "level": settings.LOG_LEVEL,
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["info_file", "error_file", "console_file", "console"],
                "level": settings.LOG_LEVEL,
                "propagate": False,
            },
            "uvicorn.error": {
                "handlers": ["info_file", "error_file", "console_file", "console"],
                "level": settings.LOG_LEVEL,
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": ["info_file", "error_file", "console_file", "console"],
                "level": settings.LOG_LEVEL,
                "propagate": False,
            },
        },
    })
