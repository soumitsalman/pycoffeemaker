"""structlog logfmt logging: file when LOG_DIR set, else stderr."""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from functools import wraps
from typing import Any, Callable

import structlog
from structlog.stdlib import ProcessorFormatter

PRE_CHAIN = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_log_level,
    structlog.stdlib.add_logger_name,
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.format_exc_info,
]

_NOISY_LOGGERS = (
    "jieba", "nlp.digestors", "nlp.embedders",
    "asyncprawcore", "asyncpraw", "dammit", "UnicodeDammit",
    "urllib3", "connectionpool",
)
_APP_LOGGERS = ("app", "collectorworker", "analyzerworker", "porterworker", "processingcache")

PREFERRED_KEY_ORDER = ("source", "num_items", "links", "error_type", "error_details")
LOGFMT = ProcessorFormatter(
    processor=structlog.processors.LogfmtRenderer(
        key_order=("timestamp", "level", "logger", "event", *PREFERRED_KEY_ORDER),
        sort_keys=True,
        drop_missing=True,
    ),
    foreign_pre_chain=PRE_CHAIN,
)


def configure_logging(log_file: str | None = None) -> None:
    structlog.configure(
        processors=PRE_CHAIN + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.WARNING)

    sink = logging.FileHandler(log_file) if log_file else logging.StreamHandler(sys.stderr)
    sink.setFormatter(LOGFMT)
    root.addHandler(sink)

    for name in _APP_LOGGERS:
        logging.getLogger(name).setLevel(logging.INFO)
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).propagate = False


get_logger = structlog.stdlib.get_logger
bind_run_context = structlog.contextvars.bind_contextvars
clear_run_context = structlog.contextvars.clear_contextvars


def _log_execution(logger: structlog.stdlib.BoundLogger, source: str, result: Any, start: datetime) -> None:
    num_items = result if isinstance(result, int) else len(result) if isinstance(result, list) else 1
    duration = (datetime.now() - start).total_seconds()
    logger.info(
        event="execution time",
        source=source,
        num_items=num_items,
        duration=int(duration),
        average_duration=duration / num_items,
    )


def _runtime_decorator(logger: structlog.stdlib.BoundLogger, *, async_fn: bool) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            start = datetime.now()
            result = await func(*args, **kwargs)
            _log_execution(logger, func.__name__, result, start)
            return result

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            start = datetime.now()
            result = func(*args, **kwargs)
            _log_execution(logger, func.__name__, result, start)
            return result

        return async_wrapper if async_fn else sync_wrapper

    return decorator


def log_runtime(logger: structlog.stdlib.BoundLogger) -> Callable:
    return _runtime_decorator(logger, async_fn=False)


def log_runtime_async(logger: structlog.stdlib.BoundLogger) -> Callable:
    return _runtime_decorator(logger, async_fn=True)
