from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class LlmEnrichment:
    summary: str
    skills: list[str]
    pros: list[str]
    cons: list[str]
    outreach_target: str
    salary: str | None
    model: str
    total_tokens: int | None


class LlmClientError(RuntimeError):
    """Raised when the LLM client fails or returns invalid payload."""


def _extract_output_text(resp: dict[str, Any]) -> str:
    """
    Responses API returns a list in resp["output"] with message items.
    We extract the first output_text chunk.
    """
    output = resp.get("output") or []
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "message":
            continue
        content = item.get("content") or []
        for c in content:
            if not isinstance(c, dict):
                continue
            if c.get("type") in ("output_text", "text"):
                text = c.get("text")
                if isinstance(text, str) and text.strip():
                    return text
    raise LlmClientError("No output_text found in LLM response.")


def _safe_list_str(x: Any, *, max_items: int) -> list[str]:
    if not isinstance(x, list):
        return []
    out: list[str] = []
    for item in x[:max_items]:
        if isinstance(item, str):
            s = item.strip()
            if s:
                out.append(s)
    return out


class OpenAIResponsesClient:
    """
    Minimal OpenAI Responses API client using requests.

    Uses structured outputs (json_schema) so the model must return valid JSON.
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str = "https://api.openai.com/v1",
        timeout_s: int = 20,
    ) -> None:
        if not api_key.strip():
            raise ValueError("api_key cannot be empty.")
        self._api_key = api_key
        self._model = model
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s

    def enrich_job(
        self,
        *,
        title: str,
        company: str,
        location_raw: str | None,
        workplace_raw: str | None,
        url: str,
        salary_text: str | None,
        raw_text: str | None,
    ) -> LlmEnrichment:
        raw_text_trunc = (raw_text or "").strip()
        if len(raw_text_trunc) > 3500:
            raw_text_trunc = raw_text_trunc[:3500] + "…"

        prompt = (
            "You are a recruiting analyst. Enrich the following job posting.\n"
            "Return ONLY valid JSON that matches the provided JSON Schema.\n\n"
            f"TITLE: {title}\n"
            f"COMPANY: {company}\n"
            f"LOCATION_RAW: {location_raw or ''}\n"
            f"WORKPLACE_RAW: {workplace_raw or ''}\n"
            f"URL: {url}\n"
            f"SALARY_TEXT_FROM_ATS: {salary_text or ''}\n\n"
            "RAW_TEXT:\n"
            f"{raw_text_trunc}\n"
        )

        schema: dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "summary": {"type": "string"},
                "skills": {"type": "array", "items": {"type": "string"}},
                "pros": {"type": "array", "items": {"type": "string"}},
                "cons": {"type": "array", "items": {"type": "string"}},
                "outreach_target": {"type": "string"},
                "salary": {"type": ["string", "null"]},
            },
            "required": ["summary", "skills", "pros", "cons", "outreach_target", "salary"],
        }

        body = {
            "model": self._model,
            "input": prompt,
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "job_enrichment",
                    "strict": True,
                    "schema": schema,
                }
            },
        }

        url_api = f"{self._base_url}/responses"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(
                url_api,
                headers=headers,
                json=body,
                timeout=self._timeout_s,
            )
        except requests.RequestException as e:
            raise LlmClientError(f"HTTP error calling LLM: {e}") from e

        if resp.status_code >= 400:
            raise LlmClientError(f"LLM request failed: {resp.status_code} {resp.text[:500]}")

        data = resp.json()
        text_out = _extract_output_text(data)

        try:
            parsed = json.loads(text_out)
        except json.JSONDecodeError as e:
            raise LlmClientError(f"LLM returned non-JSON output: {text_out[:200]}") from e

        if not isinstance(parsed, dict):
            raise LlmClientError("LLM JSON output is not an object.")

        usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
        total_tokens = usage.get("total_tokens")
        if not isinstance(total_tokens, int):
            total_tokens = None

        summary = str(parsed.get("summary") or "").strip()
        outreach_target = str(parsed.get("outreach_target") or "").strip()

        skills = _safe_list_str(parsed.get("skills"), max_items=30)
        pros = _safe_list_str(parsed.get("pros"), max_items=15)
        cons = _safe_list_str(parsed.get("cons"), max_items=15)

        salary = parsed.get("salary")
        if salary is None:
            salary_out: str | None = None
        else:
            salary_out = str(salary).strip()[:255] if str(salary).strip() else None

        return LlmEnrichment(
            summary=summary,
            skills=skills,
            pros=pros,
            cons=cons,
            outreach_target=outreach_target[:512],
            salary=salary_out,
            model=self._model,
            total_tokens=total_tokens,
        )
