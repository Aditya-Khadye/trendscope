"""Structlog configuration. Called once at process start (CLI, tests as needed)."""
from __future__ import annotations

import logging
from typing import Literal

import structlog
from structlog.types import Processor


def configure(
    *,
    level: str = "INFO",
    format: Literal["console", "json"] = "console",
) -> None:
    """Configure structlog + stdlib logging together.

    Idempotent: callable multiple times (uses force=True on basicConfig).
    """
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
        force=True,
    )

    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]
    if format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
