"""DailyMed indication/contraindication extraction pipeline.

Parses raw FDA SPL XML labels, extracts indications and contraindications,
grounds diseases to Mondo CURIEs and drugs to ChEBI CURIEs, filters
allergen/diagnostic agents, and writes structured YAML output.

The SPL-XML mining path is the single acquisition path. SPL XML is acquired
from the DailyMed v2 REST API by ``medic.ingest.dailymed.acquire`` (driven by
the USA-approved drugs in ``products/drug_list.yaml``) into ``data/raw/dailymed/``.
An empty SPL directory is a hard error — the ingester never degrades to any
legacy table.

Source isolation (docs/source-isolation.md): every evidence row emitted here is
USA-jurisdiction only.
"""

import argparse
import hashlib
import logging
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pandas as pd
import yaml

from medic.enrichment.cache import EnrichmentCache
from medic.ingest.common import _clean_for_yaml, should_skip_expensive_calls
from medic.mention import mint_mention_id

logger = logging.getLogger(__name__)

NS = {"v3": "urn:hl7-org:v3"}

# LOINC section codes
LOINC_INDICATIONS = "34067-9"
LOINC_CONTRAINDICATIONS = "34070-3"

# Output paths
KB_INDICATIONS_DIR = Path("kb/indications/dailymed")
PRODUCTS_DIR = Path("products")

# Cache paths
DISEASE_CACHE_PATH = Path("cache/enrichment/dailymed_diseases.json")
CONTRA_DISEASE_CACHE_PATH = Path("cache/enrichment/dailymed_contra_diseases.json")
ALLERGEN_CACHE_PATH = Path("cache/enrichment/dailymed_allergen.json")


# ---------------------------------------------------------------------------
# SPL XML Mining
# ---------------------------------------------------------------------------


def _flatten_section_text(section: ET.Element) -> str:
    """Flatten an SPL section to plain text, including nested subsections.

    Real SPLs keep only a lead-in sentence ("... indicated for the treatment of
    patients with:") in the LOINC-coded section's own <text>, and put the actual
    indication list in <component><section> children. Reading only the direct
    <text> child therefore dropped the whole list on 23% of labels and returned a
    fragment on most of the rest, which left the LLM to supply the missing
    diseases from prior knowledge.

    <title> elements are included because a subsection's disease name frequently
    appears only in its heading. Structural elements (<code>, <ingredient>) are
    skipped so ingredient names cannot leak into the indication text.
    """
    wanted = (f"{{{NS['v3']}}}title", f"{{{NS['v3']}}}text")
    parts: list[str] = []
    for elem in section.iter():
        if elem.tag in wanted:
            raw = ET.tostring(elem, encoding="unicode")
            clean = re.sub(r"<[^>]+>", " ", raw)
            chunk = " ".join(clean.split())
            if chunk:
                parts.append(chunk)
    return " ".join(parts)


def extract_section_text(xml_path: str | Path, loinc_code: str) -> str:
    """Extract free-text from an SPL section identified by LOINC code."""
    tree = ET.parse(xml_path)
    return _extract_section_text_from_root(tree.getroot(), loinc_code)


def extract_active_ingredients(xml_path: str | Path) -> list[str]:
    """Extract active ingredient names from an SPL XML file."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    names: set[str] = set()
    for moiety_elem in root.iter(f"{{{NS['v3']}}}activeMoiety"):
        name = moiety_elem.find("v3:name", NS)
        if name is not None and name.text:
            names.add(name.text.strip().upper())
    return sorted(names)


def _row_from_spl_root(root: ET.Element) -> dict | None:
    """Build a mined row dict from an already-parsed SPL XML root, or None.

    Returns None when the label has no active ingredient or no
    indications/contraindications text worth keeping.
    """
    indications = _extract_section_text_from_root(root, LOINC_INDICATIONS)
    contras = _extract_section_text_from_root(root, LOINC_CONTRAINDICATIONS)
    ingredients = _extract_ingredients_from_root(root)

    # Extract set_id from SPL document element. Real SPL XML carries the setid
    # in the `root` attribute (the `extension` attribute is unused here), so we
    # prefer `root` and only fall back to `extension`.
    set_id_elem = root.find(f"{{{NS['v3']}}}setId")
    set_id = ""
    if set_id_elem is not None:
        set_id = set_id_elem.get("root", "") or set_id_elem.get("extension", "")

    if ingredients and (indications or contras):
        return {
            "drug_names": ingredients,
            "indications_text": indications,
            "contraindications_text": contras,
            "set_id": set_id,
        }
    return None


def mine_spl_labels(data_dir: Path, max_labels: int = 0) -> pd.DataFrame:
    """Mine SPL labels in *data_dir* and return a DataFrame.

    Accepts two on-disk layouts, so both acquisition paths work:

    - **Per-setid XML files** (``<setid>.xml``) written by
      ``medic.ingest.dailymed.acquire`` (the primary path — DailyMed v2 API).
    - **Bulk-release ZIP archives** (``*.zip``), each containing one SPL XML
      (the DailyMed full-release download).

    Returns a DataFrame with columns:
        drug_names, indications_text, contraindications_text, set_id
    """
    if not data_dir.exists():
        logger.warning("DailyMed data directory not found: %s", data_dir)
        return pd.DataFrame()

    xml_files = sorted(data_dir.glob("*.xml"))
    zip_files = sorted(data_dir.glob("*.zip"))
    if not xml_files and not zip_files:
        logger.warning("No .xml or .zip SPL files found in %s", data_dir)
        return pd.DataFrame()

    if max_labels > 0:
        # Prefer XML files first, then top up with ZIPs, up to max_labels.
        xml_files = xml_files[:max_labels]
        remaining = max_labels - len(xml_files)
        zip_files = zip_files[:remaining] if remaining > 0 else []

    logger.info(
        "Processing %d SPL XML files and %d ZIP files from %s",
        len(xml_files), len(zip_files), data_dir,
    )

    rows: list[dict] = []

    for xml_path in xml_files:
        try:
            root = ET.fromstring(xml_path.read_bytes())
            row = _row_from_spl_root(root)
            if row is not None:
                rows.append(row)
        except ET.ParseError as exc:
            logger.warning("Skipping %s: %s", xml_path.name, exc)

    for zf_path in zip_files:
        try:
            with zipfile.ZipFile(zf_path, "r") as zf:
                xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
                if not xml_names:
                    continue
                root = ET.fromstring(zf.read(xml_names[0]))
                row = _row_from_spl_root(root)
                if row is not None:
                    rows.append(row)
        except (zipfile.BadZipFile, ET.ParseError) as exc:
            logger.warning("Skipping %s: %s", zf_path.name, exc)

    logger.info(
        "Mined %d labels with data from %d XML + %d ZIP files",
        len(rows), len(xml_files), len(zip_files),
    )
    return pd.DataFrame(rows)


def _extract_section_text_from_root(root: ET.Element, loinc_code: str) -> str:
    """Extract section text directly from an already-parsed root element.

    Reads the whole matched section subtree — see `_flatten_section_text`.
    """
    for section in root.iter(f"{{{NS['v3']}}}section"):
        code = section.find("v3:code", NS)
        if code is not None and code.get("code") == loinc_code:
            return _flatten_section_text(section)
    return ""


def _extract_ingredients_from_root(root: ET.Element) -> list[str]:
    """Extract active ingredients from an already-parsed root element."""
    names: set[str] = set()
    for moiety_elem in root.iter(f"{{{NS['v3']}}}activeMoiety"):
        name = moiety_elem.find("v3:name", NS)
        if name is not None and name.text:
            names.add(name.text.strip().upper())
    return sorted(names)


# ---------------------------------------------------------------------------
# LLM-based extraction
# ---------------------------------------------------------------------------

_disease_cache: EnrichmentCache | None = None
_contra_disease_cache: EnrichmentCache | None = None
_allergen_cache: EnrichmentCache | None = None


def _get_disease_cache() -> EnrichmentCache:
    global _disease_cache
    if _disease_cache is None:
        _disease_cache = EnrichmentCache(DISEASE_CACHE_PATH)
    return _disease_cache


def _get_contra_disease_cache() -> EnrichmentCache:
    global _contra_disease_cache
    if _contra_disease_cache is None:
        _contra_disease_cache = EnrichmentCache(CONTRA_DISEASE_CACHE_PATH)
    return _contra_disease_cache


def _get_allergen_cache() -> EnrichmentCache:
    global _allergen_cache
    if _allergen_cache is None:
        _allergen_cache = EnrichmentCache(ALLERGEN_CACHE_PATH)
    return _allergen_cache


def _text_hash(text: str) -> str:
    """Deterministic short hash for cache keying."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


# Real disease names rarely exceed 200 chars; anything longer is almost
# certainly LLM prose (refusal explanation, hedge, instruction echo) that
# leaked past the "None" sentinel.
_MAX_DISEASE_NAME_LEN = 200


def _parse_llm_disease_list(text: str) -> list[str]:
    """Parse the LLM's pipe-separated disease list, robust to refusal prose.

    The prompts ask for `disease1|disease2|...` or the literal token `None`.
    In practice the model sometimes returns prose like
    `"None\\n\\nThe text lists diagnostic procedures..."` when the input has
    no diseases. The naive parser treats that whole prose as a single
    "disease" and sends it to the grounder. Two defenses here: (a) treat any
    response that *starts* with "none" as empty, and (b) drop any item over
    `_MAX_DISEASE_NAME_LEN` since real disease names don't run that long.
    """
    stripped = (text or "").strip()
    if not stripped or stripped.lower().startswith("none"):
        return []
    return [
        d.strip()
        for d in stripped.split("|")
        if d.strip()
        and d.strip().lower() != "none"
        and len(d.strip()) <= _MAX_DISEASE_NAME_LEN
    ]


def _screen_negated_indications(diseases: list[str], indication_text: str) -> list[str]:
    """Drop extracted 'indications' the source actually negates/excludes (inversions).

    Deterministic prevention pass (FAILURE_MODES §4.1-4.2): a disease stated only inside
    a negation/exclusion scope ("should not be used in X", "except X") is not an approval
    and must not enter the indication records. Each drop is logged (nothing is dropped
    silently); the offline validator (`just validate-extraction`) is the detection net
    for anything that slips through. Raw LLM output is cached upstream, so re-screening a
    cache hit is free and stays correct if the cue list is tuned.
    """
    from medic.validation.extraction_fidelity import screen_indications

    kept, dropped = screen_indications(diseases, indication_text)
    for d in dropped:
        logger.warning(
            "Dropping negated 'indication' %r (cue: %r) — source states it negatively, "
            "not as an approval", d["disease"], d["reason"],
        )
    return kept


def extract_diseases_from_text(indication_text: str) -> list[str]:
    """Extract disease names from indication free text via LLM.

    The raw LLM extraction is cached; a deterministic negation screen then drops any
    disease the source states negatively (see :func:`_screen_negated_indications`).
    """
    if not indication_text:
        return []

    cache = _get_disease_cache()
    key = _text_hash(indication_text)
    cached = cache.get(key)
    if cached is not None:
        return _screen_negated_indications(cached.get("diseases", []), indication_text)

    if should_skip_expensive_calls():
        return []

    from medic.llm import llm_call
    text = llm_call(
        (
            "Extract all diseases mentioned as therapeutic indications from this text. "
            "Return ONLY a pipe-separated list like: disease1|disease2|disease3\n"
            "If no diseases, return: None\n"
            "Do not infer diseases - only list those explicitly mentioned.\n"
            "Do not include contraindicated conditions.\n"
            "Be specific (e.g., 'type 2 diabetes mellitus' not just 'diabetes').\n\n"
            f"Text: {indication_text[:3000]}"
        ),
        task="extraction",
        max_tokens=500,
        system="You are a biomedical expert. Extract disease names from drug indication text.",
    )
    diseases = _parse_llm_disease_list(text)

    # Cache the RAW extraction (faithful to the LLM); screen on return.
    cache.put(key, {"diseases": diseases, "text_prefix": indication_text[:200]})
    return _screen_negated_indications(diseases, indication_text)


def extract_contraindicated_diseases_from_text(contraindication_text: str) -> list[str]:
    """Extract disease names from contraindication free text via LLM.

    Sister to `extract_diseases_from_text`, but tuned for contraindication
    sections. The indication prompt explicitly instructs the LLM to *exclude*
    contraindicated conditions; passing contra text through it produces empty
    or refusal-shaped output that grounds incorrectly. This function inverts
    that instruction so contra-side callers (DailyMed, EMA §4.3, PMDA) get
    sensible disease lists.

    Used by all contra ingest paths. Cache is namespaced separately
    (`dailymed_contra_diseases.json`) so it cannot collide with the indication
    cache even if the same source text is processed both ways.
    """
    if not contraindication_text:
        return []

    cache = _get_contra_disease_cache()
    key = _text_hash(contraindication_text)
    cached = cache.get(key)
    if cached is not None:
        return cached.get("diseases", [])

    if should_skip_expensive_calls():
        return []

    from medic.llm import llm_call
    text = llm_call(
        (
            "Extract all medical conditions or diseases mentioned as "
            "contraindications in this text. A contraindication is a condition "
            "that makes a drug inappropriate (e.g., 'patients with active "
            "infection', 'severe hepatic impairment', 'pregnancy').\n\n"
            "Return ONLY a pipe-separated list like: condition1|condition2|condition3\n"
            "If no specific medical conditions are listed (e.g., the section only "
            "lists hypersensitivity to the drug itself), return: None\n"
            "Do not infer — only list conditions explicitly mentioned.\n"
            "Be specific (e.g., 'severe hepatic impairment' not just 'liver disease').\n"
            "Exclude generic hypersensitivity to the drug or excipients — that's "
            "trivial and not informative.\n"
            "Exclude pure procedural exclusions (e.g., 'concurrent use of MAOIs') "
            "unless they describe a medical condition.\n\n"
            f"Text: {contraindication_text[:3000]}"
        ),
        task="extraction",
        max_tokens=500,
        system=(
            "You are a biomedical expert. Extract the medical conditions named in "
            "drug contraindication text. Output a pipe-separated list of conditions, "
            "or 'None' if no specific conditions are present."
        ),
    )
    diseases = _parse_llm_disease_list(text)

    cache.put(
        key,
        {"diseases": diseases, "text_prefix": contraindication_text[:200]},
    )
    return diseases


def is_allergen_or_diagnostic(drug_name: str) -> dict:
    """Check if a drug is primarily an allergen or diagnostic agent."""
    cache = _get_allergen_cache()
    key = drug_name.upper()
    cached = cache.get(key)
    if cached is not None:
        return {
            "is_allergen": cached.get("is_allergen", False),
            "is_diagnostic_agent": cached.get("is_diagnostic_agent", False),
        }

    if should_skip_expensive_calls():
        return {"is_allergen": False, "is_diagnostic_agent": False}

    from medic.llm import llm_call
    text = llm_call(
        (
            f"For the drug '{drug_name}', answer these two questions with TRUE or FALSE only:\n"
            "1. Is this primarily used as an allergen for allergy testing?\n"
            "2. Is this primarily used as a radiolabel or diagnostic agent?\n"
            "Format: allergen:TRUE/FALSE,diagnostic:TRUE/FALSE"
        ),
        task="classification",
        max_tokens=100,
        system="You are a pharmaceutical expert.",
    ).lower()
    result = {
        "is_allergen": "allergen:true" in text,
        "is_diagnostic_agent": "diagnostic:true" in text,
    }

    cache.put(key, result)
    return result


# ---------------------------------------------------------------------------
# Grounding
# ---------------------------------------------------------------------------


_GROUNDING_SERVICES: dict[str, object] = {}


def _get_grounding_service(grounding_backend: str):
    """Return a process-cached grounding service for ``grounding_backend``.

    Caching the instance (rather than rebuilding per call via the factory) keeps a
    single lexical SSSOM store handle alive across a whole SPL batch, so the
    disease-grounding decision log can be flushed once at the end.
    """
    svc = _GROUNDING_SERVICES.get(grounding_backend)
    if svc is None:
        from medic.grounding.factory import get_grounding_service

        svc = get_grounding_service(grounding_backend)
        _GROUNDING_SERVICES[grounding_backend] = svc
    return svc


def _flush_dailymed_disease_grounding(grounding_backend: str) -> None:
    """Persist the disease-grounding SSSOM stores once after an SPL batch."""
    svc = _GROUNDING_SERVICES.get(grounding_backend)
    if svc is not None:
        from medic.ingest.grounding import flush_disease_grounding

        flush_disease_grounding(svc)


def _ground_disease(
    disease_name: str, grounding_backend: str, record: dict | None = None
) -> tuple[str, str]:
    """Ground a disease name to (CURIE, label) and attach structured grounding objects.

    When ``record`` is supplied, the deterministic two-stage resolve writes
    ``disease_grounding`` / ``disease_normalization`` onto it (funneled to the product
    association by the on-label merge) and sets the record's disease id/label keys. The
    (id, label) tuple is still returned so existing callers keep working unchanged.
    """
    from medic.ingest.grounding import resolve_disease_onto_record

    service = _get_grounding_service(grounding_backend)
    target = record if record is not None else {}
    disease_id = resolve_disease_onto_record(target, disease_name, service)
    if not disease_id:
        return "", ""
    return disease_id, target["final_normalized_disease_label"]


def _ground_drug(drug_name: str, grounding_backend: str) -> tuple[str, str]:
    """Ground a drug name to (CURIE, label) via the grounding cascade.

    Reuses the process-cached grounding service (via ``_get_grounding_service``) so the
    lexical index and its in-memory SSSOM store are loaded once for the whole SPL batch
    rather than rebuilt per drug — the per-call rebuild was the dominant DailyMed cost.
    """
    service = _get_grounding_service(grounding_backend)
    # Mint and pass the mention id (I-9). The grounding store is keyed by literal, so
    # dropping it here does not merely leave this row un-anchored — it overwrites the
    # MEDICNE id the drug-list ingest already wrote for the same drug name, which blanked
    # 1,285 rows on every full build.
    result = service.ground_drug_best(
        drug_name, mention_id=mint_mention_id(drug_name, "drugs"))
    if result:
        return result.id, result.label
    return "", ""


# ---------------------------------------------------------------------------
# Pipeline: from raw SPL to structured YAML
# ---------------------------------------------------------------------------


def _process_spl_data(
    spl_df: pd.DataFrame, grounding_backend: str
) -> tuple[list[dict], list[dict]]:
    """Process mined SPL data into indication and contraindication records."""
    indication_records: list[dict] = []
    contraindication_records: list[dict] = []

    for _, row in spl_df.iterrows():
        drug_names: list[str] = row["drug_names"]
        indications_text: str = row.get("indications_text", "")
        contras_text: str = row.get("contraindications_text", "")
        set_id: str = row.get("set_id", "")

        # Ground each drug
        for drug_name in drug_names:
            drug_id, drug_label = _ground_drug(drug_name, grounding_backend)
            if not drug_id:
                logger.debug("Could not ground drug: %s", drug_name)
                continue

            # Allergen/diagnostic check (degrade gracefully on LLM/network error;
            # num_retries in llm_call already handles transient blips)
            try:
                allergen_info = is_allergen_or_diagnostic(drug_name)
            except Exception as exc:
                logger.warning("Allergen/diagnostic check failed for %s (%s); assuming neither.",
                               drug_name, exc)
                allergen_info = {"is_allergen": False, "is_diagnostic_agent": False}

            # Extract and ground diseases from indications
            if indications_text:
                try:
                    diseases = extract_diseases_from_text(indications_text)
                except Exception as exc:
                    logger.warning("Disease extraction failed for setid %s (%s); skipping.",
                                   set_id, exc)
                    diseases = []
                for disease_name in diseases:
                    record: dict = {}
                    disease_id, disease_label = _ground_disease(
                        disease_name, grounding_backend, record
                    )
                    if not disease_id:
                        logger.debug("Could not ground disease: %s", disease_name)
                        continue

                    evidence_item = {
                        "source_type": "REGULATORY",
                        "jurisdiction": "USA",
                        "confidence": "HIGH",
                        "approval_status": "APPROVED",
                        "explanation": "FDA-approved indication from DailyMed structured product label",
                        "source_role": "INTERMEDIARY",
                    }
                    # URL policy per SPEC §5.6 / architecture §5.6: reference is
                    # the setid landing page (lookup.cfm), source_document_url is
                    # the deterministic label PDF (downloadpdffile.cfm).
                    if set_id:
                        evidence_item["reference"] = f"https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid={set_id}"
                        evidence_item["source_document_url"] = f"https://dailymed.nlm.nih.gov/dailymed/downloadpdffile.cfm?setid={set_id}"
                        evidence_item["setid"] = set_id
                    else:
                        import urllib.parse
                        evidence_item["reference"] = f"https://dailymed.nlm.nih.gov/dailymed/search.cfm?labeltype=all&query={urllib.parse.quote_plus(drug_label)}"
                    if indications_text:
                        evidence_item["snippet"] = indications_text[:500]
                    # Preserve raw source strings for audit
                    evidence_item["original_drug_label"] = drug_name
                    if set_id:
                        evidence_item["original_drug_id"] = set_id
                    evidence_item["original_disease_label"] = disease_name

                    record.update(
                        {
                            "drug_disease": f"{drug_id}|{disease_id}",
                            "final_normalized_drug_id": drug_id,
                            "final_normalized_drug_label": drug_label,
                            "final_normalized_disease_id": disease_id,
                            "final_normalized_disease_label": disease_label,
                            "fda": True,
                            "ema": False,
                            "pmda": False,
                            "relationship_type": "INDICATION",
                            "indications_text": indications_text,
                            "is_allergen": allergen_info["is_allergen"],
                            "is_diagnostic_agent": allergen_info[
                                "is_diagnostic_agent"
                            ],
                            "set_id": set_id,
                            "evidence": [evidence_item],
                        }
                    )
                    indication_records.append(record)

            # Extract and ground diseases from contraindications
            if contras_text:
                try:
                    contra_diseases = extract_contraindicated_diseases_from_text(contras_text)
                except Exception as exc:
                    logger.warning("Contraindication extraction failed for setid %s (%s); skipping.",
                                   set_id, exc)
                    contra_diseases = []
                for disease_name in contra_diseases:
                    record = {}
                    disease_id, disease_label = _ground_disease(
                        disease_name, grounding_backend, record
                    )
                    if not disease_id:
                        continue

                    contra_evidence = {
                        "source_type": "REGULATORY",
                        "jurisdiction": "USA",
                        "confidence": "HIGH",
                        "approval_status": "APPROVED",
                        "explanation": "FDA contraindication from DailyMed structured product label",
                        "source_role": "INTERMEDIARY",
                    }
                    if set_id:
                        contra_evidence["reference"] = f"https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid={set_id}"
                        contra_evidence["source_document_url"] = f"https://dailymed.nlm.nih.gov/dailymed/downloadpdffile.cfm?setid={set_id}"
                        contra_evidence["setid"] = set_id
                    else:
                        import urllib.parse
                        contra_evidence["reference"] = f"https://dailymed.nlm.nih.gov/dailymed/search.cfm?labeltype=all&query={urllib.parse.quote_plus(drug_label)}"
                    if contras_text:
                        contra_evidence["snippet"] = contras_text[:500]
                    contra_evidence["original_drug_label"] = drug_name
                    if set_id:
                        contra_evidence["original_drug_id"] = set_id
                    contra_evidence["original_disease_label"] = disease_name

                    record.update(
                        {
                            "drug_disease": f"{drug_id}|{disease_id}",
                            "final_normalized_drug_id": drug_id,
                            "final_normalized_drug_label": drug_label,
                            "final_normalized_disease_id": disease_id,
                            "final_normalized_disease_label": disease_label,
                            "fda": True,
                            "ema": False,
                            "pmda": False,
                            "relationship_type": "CONTRAINDICATION",
                            "indications_text": contras_text,
                            "is_allergen": allergen_info["is_allergen"],
                            "is_diagnostic_agent": allergen_info[
                                "is_diagnostic_agent"
                            ],
                            "set_id": set_id,
                            "evidence": [contra_evidence],
                        }
                    )
                    contraindication_records.append(record)

    _flush_dailymed_disease_grounding(grounding_backend)
    return indication_records, contraindication_records


# ---------------------------------------------------------------------------
# Output writing
# ---------------------------------------------------------------------------


def _write_yaml(records: list[dict], path: Path) -> None:
    """Write records to a YAML file with cleaning."""
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned = _clean_for_yaml(records)
    content = yaml.dump(cleaned, default_flow_style=False, allow_unicode=True, width=1000)
    content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
    with open(path, "w") as f:
        f.write(content)
    logger.info("Wrote %d records to %s", len(records), path)


def _write_output(
    indication_records: list[dict], contraindication_records: list[dict]
) -> None:
    """Write per-source YAML and merged product files."""
    # Per-source YAML
    if indication_records:
        _write_yaml(indication_records, KB_INDICATIONS_DIR / "indications.yaml")
    if contraindication_records:
        _write_yaml(
            contraindication_records, KB_INDICATIONS_DIR / "contraindications.yaml"
        )

    if contraindication_records:
        product_path = PRODUCTS_DIR / "contraindication_list.yaml"
        product_path.parent.mkdir(parents=True, exist_ok=True)
        cleaned = _clean_for_yaml({"associations": contraindication_records})
        content = yaml.dump(
            cleaned, default_flow_style=False, allow_unicode=True, width=1000
        )
        content = "".join(
            c for c in content if c == "\n" or c == "\t" or ord(c) >= 32
        )
        with open(product_path, "w") as f:
            f.write(content)
        logger.info(
            "Wrote %d contraindications to %s",
            len(contraindication_records),
            product_path,
        )


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="DailyMed indication/contraindication extraction pipeline"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw/dailymed/"),
        help="Path to directory containing DailyMed SPL ZIP files",
    )
    parser.add_argument(
        "--grounding-backend",
        default="lexical",
        help="Grounding backend to use (default: lexical — the deterministic two-stage grounder)",
    )
    parser.add_argument(
        "--max-labels",
        type=int,
        default=0,
        help="Limit number of labels to process (0 = all)",
    )
    parser.add_argument(
        "--acquire",
        action="store_true",
        help=(
            "Before mining, fetch SPL XML for USA-approved drugs from the "
            "DailyMed v2 API into --data-dir (see medic.ingest.dailymed.acquire)."
        ),
    )
    parser.add_argument(
        "--acquire-limit",
        type=int,
        default=0,
        help="With --acquire, cap the number of drugs to fetch (0 = all).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    # Optionally acquire real SPL XML first (the acquisition step of the single
    # SPL-XML path).
    if args.acquire:
        from medic.ingest.dailymed.acquire import acquire as acquire_spl
        acquire_spl(data_dir=args.data_dir, limit=args.acquire_limit)

    # Single path: mine real SPL XML.
    spl_df = mine_spl_labels(args.data_dir, max_labels=args.max_labels)

    if spl_df.empty:
        raise SystemExit(
            f"No SPL XML found in {args.data_dir}. The SPL-XML path is the only "
            "DailyMed acquisition path. Populate it first with:\n"
            "    just ingest-dailymed-acquire\n"
            "or run this command with --acquire."
        )

    logger.info("Processing %d mined SPL labels (SPL-XML path)", len(spl_df))
    indication_records, contraindication_records = _process_spl_data(
        spl_df, args.grounding_backend
    )

    _write_output(indication_records, contraindication_records)

    # Flush caches. `EnrichmentCache.put` only mutates the in-memory dict, so a cache that
    # is never flushed is silently re-queried on every build. `_contra_disease_cache` was
    # missing here: its 2,484 LLM calls ran on every run, and because the extraction is not
    # deterministic the contraindication count moved between otherwise identical builds
    # (2,399 -> 2,442 -> 2,445) while indications stayed pinned at 6,504. `just determinism`
    # cannot see it — it re-runs the merge twice, never the extraction.
    if _disease_cache is not None:
        _disease_cache.flush()
    if _contra_disease_cache is not None:
        _contra_disease_cache.flush()
    if _allergen_cache is not None:
        _allergen_cache.flush()
    try:
        from medic.ingest.dailymed.setid_lookup import (
            flush_cache as flush_setid_cache,
            log_failure_summary,
            lookup_failure_summary,
        )
        flush_setid_cache()
        log_failure_summary()
        # Persist setid resolution outcomes alongside the indications so they
        # ship with the kb (visible in the next run's QA without re-querying).
        report_path = Path("kb/indications/dailymed/setid_lookup_report.yaml")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            yaml.dump(
                lookup_failure_summary(),
                default_flow_style=False,
                allow_unicode=True,
            )
        )
        logger.info("Wrote DailyMed setid lookup report to %s", report_path)
    except Exception as e:
        logger.debug("setid summary write failed: %s", e)

    logger.info(
        "Done: %d indications, %d contraindications",
        len(indication_records),
        len(contraindication_records),
    )


if __name__ == "__main__":
    main()
