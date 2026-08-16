"""Russia drug ingest — Russian State Register of Medicines (GRLS).

Reads the manually-provided GRLS bulk export at the stable path
``background/grls.zip`` (a zip of 8 register ``.xlsx`` files with Cyrillic
names), parses the currently-valid registers into a de-duplicated drug list,
and grounds each drug through the shared grounding pipeline. The shared
translation stage (DeepL via the ``babelon`` translator service) translates the
Cyrillic name to English before lexical grounding, so drug names ground to
canonical ChEBI CURIEs; names DeepL cannot translate fall back to the grounder's
deterministic Cyrillic-transliteration ladder.

GRLS is IP-blocked for anonymous non-Russian sessions, so there is no live
fetch — the "fetch" step is simply locating ``background/grls.zip`` and failing
loudly if it is absent (see ``locate_source.py`` and ``README.md``). This
replaces the legacy ``data/raw/russia/russia_norm.csv`` v1.0.0 intermediate.

The GRLS export carries no indication text, so Russia contributes a drug list
only (``source: RUSSIA``) — no indications or contraindications.

Per-product GRLS deep links — investigation 2026-05-02 (NOT IMPLEMENTED)
-----------------------------------------------------------------------
Goal: replace the generic ``https://grls.rosminzdrav.ru/Default.aspx`` link
emitted in ``on_label_merge`` with per-product URLs of the form
``https://grls.rosminzdrav.ru/Grls_View_v2.aspx?routingGuid=<guid>``.

Findings:

1. URL pattern is real. ``Grls_View_v2.aspx`` accepts a ``routingGuid`` query
   parameter; without one it 302s to ``/Default.aspx``. The detail link is
   constructed in the JS function ``det(routingGuid, isFS)`` on
   ``GRLS.aspx``.
2. Search action: a GET to
   ``GRLS.aspx?isfs=0&regtype=1,6&pageSize=10&order=Registered&orderType=desc&pageNum=1&MnnR=<query>``
   (or ``TradeNmR``/``RegNumber``). The form is an ASP.NET WebForms
   POSTBACK to the same URL with ``__VIEWSTATE``, ``__VIEWSTATEGENERATOR``,
   ``__EVENTVALIDATION`` and the search fields (``ctl00$plate$txtMNN``,
   ``ctl00$plate$txtRegNm``, ``ctl00$plate$txtTorg``, etc.).
3. Anonymous search returns *zero* result rows. The search-result HTML is
   only populated after a successful client-side login flow
   (``window.loginGrlsUser(redirectUrl)``), confirmed by the presence of a
   ``Войти`` (Login) button and the absence of any GridView/data-table
   markup in the response. This was tested both via plain GET with
   query-string filters and via a full POSTBACK preserving viewstate and
   session cookies (``grlsticketn``); both return only the empty form
   shell. WebFetch on the same URLs confirms no per-product links are
   rendered.
4. ``Search.ashx?q=...&t=...`` is a typeahead helper for filling form
   fields (country, lekform, MNN, etc.); it does NOT return product rows
   or routingGuids.
5. As of the ``background/grls.zip`` migration each drug record now carries
   the Cyrillic INN (``original_name_ru``) plus the GRLS registration
   certificate number(s) (``application_number`` / ``application_numbers``).
   A routingGuid is still NOT present in the export, so an unambiguous
   per-product GRLS deep link is still not constructible from these fields
   alone — the search endpoint remains authenticated-only.

Conclusion: Per-product GRLS deep links cannot be implemented without
either (a) re-ingesting the upstream Russia source with the registration
number / Cyrillic MNN preserved, or (b) reverse-engineering and
authenticating against the GRLS login flow (the latter is fragile and
likely against the spirit of GRLS Terms of Use). Until then, the merge
step continues to emit the generic ``Default.aspx`` link.

Action items for a follow-up session:
  * The Cyrillic INN and registration number(s) are now preserved on each
    record. A per-product deep link still needs the GRLS routingGuid, which
    is not present in the bulk export — this would require reverse-
    engineering the authenticated search flow.
"""

import logging
from pathlib import Path

import typer

from medic.grounding.cache import GroundingCache
from medic.grounding.factory import get_grounding_service
from medic.ingest.common import (
    write_drug_source_yaml,
    write_grounding_report,
)
from medic.ingest.grounding import ground_records
from medic.ingest.sanity import check_row_floor, record_source
from medic.ingest.russia.locate_source import GRLS_ZIP_PATH, locate_grls_zip
from medic.ingest.russia.parse_grls import parse_grls_zip
from medic.translation import translate_records

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def main(
    grounding_backend: str = typer.Option("lexical", help="Grounding backend to use"),
    grls_zip: Path = typer.Option(
        GRLS_ZIP_PATH,
        help="Path to the manually-provided GRLS export zip.",
    ),
    force_download: bool = typer.Option(
        False, "--force-download", help="Unused for Russia (manual-acquisition source)."
    ),
) -> None:
    """Ingest Russia drug data from the GRLS export: parse, ground, write output."""
    logging.basicConfig(level=logging.INFO)

    # "Fetch" for Russia is just locating the manually-provided export; this
    # raises a clear, actionable error if the file is missing.
    zip_path = locate_grls_zip(grls_zip)

    records = parse_grls_zip(zip_path)

    # Sanity: refuse a truncated/stale GRLS export before the expensive translation,
    # and stamp the source fingerprint into the manifest for provenance.
    check_row_floor("russia", len(records))
    record_source("russia", str(zip_path), len(records))

    # Stage-0 translation: Russian -> English via DeepL (babelon), cached in the
    # Babelon store. Overwrites ``source_name`` with English where DeepL resolves
    # a name and attaches the ``translation`` object + MEDICNE ``mention_id``.
    # Names DeepL cannot translate keep their Cyrillic value, so the grounder's
    # deterministic Cyrillic-transliteration ladder still catches those.
    translate_records(records, "ru")

    # Ground through the shared pipeline — same call as all other sources.
    grounding_service = get_grounding_service(grounding_backend)
    cache = GroundingCache()
    grounded_records, report = ground_records(
        records, grounding_service, cache, source_name="russia"
    )

    output_dir = Path("kb/drugs/russia")
    write_drug_source_yaml(grounded_records, output_dir, "russia")
    write_grounding_report(report, output_dir, "russia")

    logger.info(
        "Russia ingest complete: %d drugs, %d auto-accepted, %d unresolved",
        report["total_drugs"],
        report["auto_accepted"],
        report["unresolved"],
    )


if __name__ == "__main__":
    app()
