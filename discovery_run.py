from __future__ import annotations

import logging

from jobs_bot.config import get_settings
from jobs_bot.db import make_session_factory
from jobs_bot.source_discovery import (
    discover_sources_from_companiesmarketcap,
    iter_inactive_sources_for_verification,
    parse_regions,
)
from jobs_bot.verify_sources import verify_and_promote_sources


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    s = get_settings()

    if s.discovery_enable != 1:
        logger.info("Discovery disabled (DISCOVERY_ENABLE!=1). Nothing to do.")
        return

    session_local = make_session_factory(s)
    regions = parse_regions(s.discovery_regions)

    with session_local() as db:
        for region in regions:
            logger.info("Discovering sources for region=%s", region)
            counts = discover_sources_from_companiesmarketcap(
                db,
                region=region,
                max_companies=s.discovery_max_companies_per_region,
                max_sources_to_upsert=s.discovery_max_sources_per_run,
                request_timeout_s=s.request_timeout_s,
                delay_s=s.discovery_request_delay_s,
                http_user_agent=s.discovery_user_agent,
                wikidata_user_agent=s.wikidata_user_agent,
                wikidata_name_fallback_enable=s.wikidata_name_fallback_enable,
            )
            logger.info("Discovery counts region=%s: %s", region, counts)

        if s.discovery_verify_enable == 1:
            sources = list(
                iter_inactive_sources_for_verification(
                    db,
                    limit=s.discovery_verify_max_per_run,
                    discovered_via_prefix="cmc",
                )
            )
            logger.info("Verifying %s inactive sources...", len(sources))
            ok, failed = verify_and_promote_sources(
                db,
                sources,
                timeout_s=s.request_timeout_s,
                delay_s=s.discovery_request_delay_s,
            )
            logger.info("Verification results: ok=%s failed=%s", ok, failed)


if __name__ == "__main__":
    main()
