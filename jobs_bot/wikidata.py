from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import urlencode

import requests


WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_API_ENDPOINT = "https://www.wikidata.org/w/api.php"


@dataclass(frozen=True)
class WikidataEntity:
    qid: str
    label: str | None
    official_website: str | None


def _first_binding_value(bindings: list[dict], key: str) -> str | None:
    for b in bindings:
        v = (b.get(key) or {}).get("value")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def resolve_official_website(
    *,
    company_name: str,
    ticker: str | None,
    session: requests.Session,
    timeout_s: int = 20,
    user_agent: str | None = None,
    allow_name_fallback: bool = False,
) -> WikidataEntity | None:
    """Resolve an official website for a company using Wikidata.

    Resolution strategy (precision-first):
      1) If ticker is present, query by stock ticker symbol (P249).
      2) Optionally fall back to entity search by company name.

    Returns None if no website is resolved.
    """
    headers = {}
    if user_agent:
        headers["User-Agent"] = user_agent

    if ticker:
        q = (
            "SELECT ?item ?itemLabel ?website WHERE { "
            f'?item wdt:P249 "{ticker}". '
            "OPTIONAL { ?item wdt:P856 ?website. } "
            'SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } '
            "} LIMIT 10"
        )
        resp = session.get(
            WIKIDATA_SPARQL_ENDPOINT,
            params={"format": "json", "query": q},
            headers={**headers, "Accept": "application/sparql-results+json"},
            timeout=timeout_s,
        )
        resp.raise_for_status()
        data = resp.json() or {}
        bindings = (data.get("results") or {}).get("bindings") or []

        website = _first_binding_value(bindings, "website")
        item = _first_binding_value(bindings, "item")
        label = _first_binding_value(bindings, "itemLabel")
        if item:
            qid = item.rsplit("/", 1)[-1]
            if website:
                return WikidataEntity(qid=qid, label=label, official_website=website)

    if not allow_name_fallback:
        return None

    params = {
        "action": "wbsearchentities",
        "search": company_name,
        "language": "en",
        "format": "json",
        "limit": 5,
        "type": "item",
    }
    resp = session.get(
        f"{WIKIDATA_API_ENDPOINT}?{urlencode(params)}",
        headers=headers,
        timeout=timeout_s,
    )
    resp.raise_for_status()
    data = resp.json() or {}
    hits = data.get("search") or []
    if not hits:
        return None

    qid = (hits[0].get("id") or "").strip()
    label = (hits[0].get("label") or "").strip() or None
    if not qid:
        return None

    entity_url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    resp = session.get(entity_url, headers=headers, timeout=timeout_s)
    resp.raise_for_status()
    entity_data = resp.json() or {}

    entities = entity_data.get("entities") or {}
    ent = entities.get(qid) or {}
    claims = ent.get("claims") or {}
    p856 = claims.get("P856") or []
    for claim in p856:
        dv = ((claim.get("mainsnak") or {}).get("datavalue") or {}).get("value")
        if isinstance(dv, str) and dv.strip():
            return WikidataEntity(qid=qid, label=label, official_website=dv.strip())

    return WikidataEntity(qid=qid, label=label, official_website=None)


def dumps_entity(entity: WikidataEntity | None) -> str:
    if entity is None:
        return "null"
    return json.dumps(
        {
            "qid": entity.qid,
            "label": entity.label,
            "official_website": entity.official_website,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
