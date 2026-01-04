from __future__ import annotations

import json

from jobs_bot.logging_utils import LogContext, configure_logging


def test_structured_logging_emits_json(capsys) -> None:
    logger = configure_logging(context=LogContext(run_id="test-run"))
    logger.info("hello", extra={"event": "unit_test", "foo": "bar"})

    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert payload["run_id"] == "test-run"
    assert payload["event"] == "unit_test"
    assert payload["msg"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["foo"] == "bar"
