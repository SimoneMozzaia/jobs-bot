from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class LogContext:
    """Context fields appended to every log line."""

    app: str = "jobs-bot"
    run_id: str | None = None


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter compatible with systemd/journald."""

    def __init__(self, *, context: LogContext | None = None) -> None:
        super().__init__()
        self._context = context or LogContext()

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "event": getattr(record, "event", None),
            "app": self._context.app,
            "run_id": self._context.run_id,
            "pid": record.process,
        }

        reserved = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "asctime",
        }

        for key, value in record.__dict__.items():
            if key in reserved:
                continue
            if key.startswith("_"):
                continue
            if key in payload:
                continue
            payload[key] = value

        if record.exc_info:
            payload["exc_type"] = record.exc_info[0].__name__
            payload["exc_msg"] = str(record.exc_info[1])

        return json.dumps(payload, ensure_ascii=False)


def configure_logging(*, context: LogContext | None = None) -> logging.Logger:
    """
    Configure root logging to stdout using JSON format.
    Returns an application logger.
    """
    root = logging.getLogger()
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(context=context))

    root.addHandler(handler)
    root.setLevel(logging.INFO)
    root.propagate = False

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return logging.getLogger("jobs-bot")
