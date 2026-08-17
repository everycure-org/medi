"""Build ``Mention`` dicts (transformation-provenance model) from the legacy stage objects.

The merge steps already carry, per record, the legacy Stage-0/1/2 objects
(``translation`` / ``grounding`` / ``normalization`` — the ``grounding.yaml`` classes)
plus, for diseases, the extraction context (label + supporting text + entailment/negation).
:func:`build_mention` assembles those into a single ``Mention`` (``provenance.yaml``) with an
ordered ``steps`` list — ExtractionStep? -> TranslationStep? -> GroundingStep? ->
NormalizationStep? — so the whole "verbatim string -> canonical id" trail is replayable from
one object (invariant I-8). This is the shared core of "Mention construction"; both
``merge/drug_merge.py`` (drug identity) and ``merge/on_label_merge.py`` (inlined disease) use it.

The step *actions* and *flags* are the controlled enums in ``provenance.yaml``. Nothing here
invents a transform: it re-expresses decisions already recorded by the grounder/translator.
"""

from __future__ import annotations

import logging
import re

from medic.confidence import resolve_confidence
from medic.curie_utils import get_prefix
from medic.mention import mint_mention_id
from medic.versions import deepl_agent, llm_agent, package_version, tool_ref

logger = logging.getLogger(__name__)


def _stamp_confidence(
    step: dict, measured: float | None, *, deterministic: bool = False
) -> None:
    """Stamp ``confidence`` + ``confidence_basis`` on a step (invariant I-11).

    Every step must declare both, so a reader can tell a measured score from an assumed one.
    Before this, an unmeasured step simply omitted ``confidence`` and contributed nothing to the
    aggregate — an unreviewed machine translation cost a record exactly zero.

    Call this **last**, after the tool and agent are stamped: the prior is keyed on the model
    (for a versioned agent) or on the tool and its version, so both must already be on the step.
    """
    value, basis = resolve_confidence(
        step["category"], step.get("method", ""), measured,
        deterministic=deterministic,
        agent=step.get("agent"),
        tool=step.get("tool"),
        tool_version=step.get("tool_version"),
    )
    step["confidence"] = value
    step["confidence_basis"] = basis


def _stamp_tool(step: dict, tool: str, version: str | None = None) -> None:
    """Stamp ``tool`` + ``tool_version`` on a step (version resolved if not given)."""
    if version is None:
        tool, version = tool_ref(tool)
    if tool:
        step["tool"] = tool
    if version:
        step["tool_version"] = version

# translation_status (Babelon) -> StepStatus (uniform lifecycle)
_TRANSLATION_STATUS_TO_STEP = {
    "OFFICIAL": "CONFIRMED",
    "CANDIDATE": "CANDIDATE",
    "UNDER_REVIEW": "UNDER_REVIEW",
    "NOT_TRANSLATED": "MACHINE",
}
_DEEPL_WIKIDATA = "wikidata:Q116709136"


def _vocab(curie: str | None) -> str | None:
    if not curie:
        return None
    try:
        return get_prefix(curie)
    except Exception:
        return None


def _translation_step(translation: dict) -> dict | None:
    source_value = (translation.get("source_value") or "").strip()
    if not source_value:
        return None
    expertise = (translation.get("translator_expertise") or "").upper()
    translator = translation.get("translator") or ""
    status = _TRANSLATION_STATUS_TO_STEP.get(
        (translation.get("translation_status") or "").upper(), "MACHINE")
    is_machine = expertise == "ALGORITHM" or translator == _DEEPL_WIKIDATA
    step: dict = {
        "category": "TRANSLATION",
        "input_value": source_value,
        "output_value": translation.get("translation_value") or "",
        "method": "API" if is_machine else "HUMAN",
    }
    # babelon is the translator service MeDIC calls; its released version is what is knowable.
    _stamp_tool(step, "babelon", package_version("babelon"))
    if translator:
        # the engine behind babelon (DeepL) is the agent
        if translator == _DEEPL_WIKIDATA:
            step["agent"] = deepl_agent()
        else:
            step["agent"] = {
                "agent_id": translator,
                "agent_type": "AI_AGENT" if is_machine else "HUMAN",
                "agent_name": translator,
            }
    step["status"] = status
    if translation.get("source_language"):
        step["source_language"] = translation["source_language"]
    if translation.get("translation_language"):
        step["target_language"] = translation["translation_language"]
    if expertise:
        step["translator_expertise"] = expertise
    step["quality"] = "close"
    flags = []
    if is_machine and status != "CONFIRMED":
        flags.append("unreviewed_machine")
    step["flags"] = flags
    # No translator publishes a per-string score, so this is always a prior. A CONFIRMED human
    # translation is the one case that cannot be wrong for our purposes.
    _stamp_confidence(step, None, deterministic=(not is_machine and status == "CONFIRMED"))
    return step


def _grounding_step(
    grounding: dict, *, input_value: str, applied_rules: list[str] | None = None,
    predicate_id: str = "", flags: list[str] | None = None,
) -> dict | None:
    grounded_id = grounding.get("grounded_id") or ""
    quality = (grounding.get("grounding_quality") or "").strip()
    if not grounded_id and not quality:
        return None
    step: dict = {
        "category": "GROUNDING",
        "input_value": input_value or grounding.get("original_string") or "",
        "output_value": grounded_id,
        # A source-asserted id was never matched against anything; saying LEXICAL_MATCH would
        # claim a match that never ran.
        "method": "SOURCE_ASSERTED" if quality == "source_asserted" else "LEXICAL_MATCH",
    }
    _stamp_tool(step, "medic-lexical-grounder")
    conf = grounding.get("confidence")
    if conf is not None:
        try:
            conf = float(conf)
        except (TypeError, ValueError):
            conf = None
    if grounding.get("grounded_label"):
        step["output_label"] = grounding["grounded_label"]
    vocab = _vocab(grounded_id)
    if vocab:
        step["source_vocabulary"] = vocab
    if grounding.get("components"):
        step["components"] = list(grounding["components"])
    # The Stage-1 preprocessing rules that fired (salt/formulation strip, transliteration,
    # fuzzy edit-1, ...) live in the SSSOM store; funnel them in so the trail is replayable.
    if applied_rules:
        step["applied_rules"] = list(applied_rules)
    if predicate_id:
        step["predicate_id"] = predicate_id
    if quality:
        step["quality"] = quality
    out_flags = list(flags or [])
    if quality == "rxnorm_proposed" and "rxnorm_proposed" not in out_flags:
        out_flags.append("rxnorm_proposed")
    step["flags"] = out_flags
    # A curated grounding is a human decision, so it cannot be "wrong" in the linking sense.
    _stamp_confidence(step, conf, deterministic=(quality == "curated"))
    return step


def _normalization_step(normalization: dict, *, fallback_input: str | None) -> dict | None:
    normalized_id = normalization.get("normalized_id") or ""
    quality = (normalization.get("normalization_quality") or "").strip()
    # `none` is the pre-2026-08-09 spelling of `identity`, still baked into every kb/ record
    # written before the rename. Normalize on read so the step carries a valid enum value —
    # the store loader's alias does not cover records funneled in from kb/.
    if quality in ("", "none"):
        quality = "identity"
    if not normalized_id and not quality:
        return None
    step: dict = {
        "category": "NORMALIZATION",
        "input_value": normalization.get("original_id") or fallback_input or normalized_id,
        "output_value": normalized_id,
        "method": "DETERMINISTIC_RULE",
    }
    # the store records "medic-normalizer/1"; tool_ref splits it (and supplies the component
    # version when the store row carries a bare name).
    _stamp_tool(step, (normalization.get("tool") or "").strip() or "medic-normalizer")
    if normalization.get("normalized_label"):
        step["output_label"] = normalization["normalized_label"]
    vocab = _vocab(normalized_id)
    if vocab:
        step["target_namespace"] = vocab
    if quality:
        step["quality"] = quality
    step["flags"] = ["deprecated_replacement"] if quality == "deprecated_replacement" else []
    # Normalization is a deterministic rule lookup or a curated assertion; either way the hop
    # itself cannot be wrong, so 1.0/DETERMINISTIC is honest rather than generous.
    _stamp_confidence(step, None, deterministic=True)
    return step


#: Recognition-level failure modes — the only flags an ExtractionStep may carry. Claim-level
#: signals (negated_inversion / over_extraction / wrong_section / wrong_pairing) belong on the
#: Assertion: the entity can be recognised perfectly while the asserted relation is wrong.
_EXTRACTION_FLAGS = frozenset({
    "hallucination", "truncated_snippet", "coreference_ambiguity", "scope_narrowed",
})


def _extraction_step(
    extraction: dict, *, original_literal: str, spans: list[dict] | None = None
) -> dict:
    """Build the entity-recognition step (NER for free text, a field read for structured).

    Says nothing about what claim the mention participates in — see :func:`build_assertion`.
    ``confidence`` is how well the source text supports that this string occurs here.

    When ``spans`` is given and the extraction names a ``span_index``, the step's
    ``input_value`` is that span's text and the step records which span it read — making
    ``pipeline[0].input_value == spans[span_index].text`` checkable rather than assumed (I-8b).
    That is what keeps a "Limitations of Use" sentence out of the positive claim's scope.
    """
    span_index = extraction.get("span_index")
    span = None
    if spans is not None and span_index is not None:
        if not 0 <= span_index < len(spans):
            raise IndexError(
                f"span_index {span_index} out of range for {len(spans)} spans")
        span = spans[span_index]
    quote = (span["text"] if span else
             extraction.get("supporting_quote") or extraction.get("input_value")
             or original_literal)
    method = extraction.get("method", "LLM")
    step: dict = {
        "category": "EXTRACTION",
        "input_value": quote,
        "output_value": extraction.get("output_value") or original_literal,
        "method": method,
    }
    # Who ran it: a deterministic source parser, or an LLM pinned to its dated model id.
    if method == "LLM":
        _stamp_tool(step, extraction.get("tool") or "medic-extractor")
        step["agent"] = llm_agent(extraction.get("llm_task", "extraction"))
    else:
        _stamp_tool(step, extraction.get("tool") or "medic-ingest")
    conf = extraction.get("confidence")
    if conf is not None:
        try:
            conf = float(conf)
        except (TypeError, ValueError):
            conf = None
    # Quality must reflect the evidence, not be asserted. `not_stated` says only "this string
    # is not literally in the supporting text" — which is TRUE both for a hallucination and
    # for a correct synonym normalization ("high blood pressure" for "hypertension"). We
    # deliberately do NOT infer the `hallucination` flag from it: telling those two apart
    # needs a paraphrase-aware detector (issues/issue_snippet_entailment_regulatory.md).
    if "quality" in extraction:
        step["quality"] = extraction["quality"]
    elif conf is None:
        step["quality"] = "verbatim"
    elif conf >= 1.0:
        step["quality"] = "verbatim"
    elif conf > 0.0:
        step["quality"] = "canonicalized"
    else:
        step["quality"] = "not_stated"
    step["flags"] = [f for f in (extraction.get("flags") or []) if f in _EXTRACTION_FLAGS]
    if span is not None:
        step["span_index"] = span_index
        step["span_role"] = span["role"]
        start = span["text"].find(step["output_value"])
        if start >= 0:
            step["char_start"] = start
            step["char_end"] = start + len(step["output_value"])
    _stamp_co_mentions(step, extraction, span)
    # Reading a structured field verbatim cannot be wrong; an LLM extraction without a
    # self-reported score falls back to the EXTRACTION.LLM prior.
    _stamp_confidence(step, conf, deterministic=(method != "LLM"))
    return step


def _mint_type(entity_type: str) -> str:
    """The plural form :func:`mint_mention_id` expects (its uuid5 key includes it)."""
    return entity_type if entity_type.endswith("s") else entity_type + "s"


def _stamp_co_mentions(step: dict, extraction: dict, span: dict | None) -> None:
    """Record the other entities the extractor found in the same span (D5).

    Informational only — the chain stays strictly one-in-one-out (I-8). This answers the
    separate question "was this string the only candidate, or one of five?", and the minted
    ids make the sibling records joinable without re-running the extractor.
    """
    co_mentions = extraction.get("co_mentions") or []
    if not co_mentions or span is None:
        return
    own_kind = extraction.get("entity_type") or "disease"
    recorded = []
    for co in co_mentions:
        value = (co.get("value") or "").strip()
        if not value:
            continue
        kind = co.get("entity_type") or own_kind
        entry = {"value": value, "entity_type": kind,
                 "mention_id": mint_mention_id(value, _mint_type(kind))}
        start = span["text"].find(value)
        if start >= 0:
            entry["char_start"] = start
            entry["char_end"] = start + len(value)
        recorded.append(entry)
    if not recorded:
        return
    step["co_mentions"] = recorded
    # index/total count same-entity_type mentions only: "one of five diseases" is the useful
    # signal, and a drug named in the same sentence is not a rival reading of this string.
    same_kind = sum(1 for c in recorded if c["entity_type"] == own_kind)
    step["mention_index"] = 1
    step["mention_total"] = same_kind + 1


#: Trigger phrases that establish a relation, longest-first so the most specific wins.
#: Each maps to a TriggerCueEnum value. Matching is done against the source text and the
#: matched span is stored verbatim — an *extractive* rationale that can be verified, rather
#: than a generated explanation that can only be believed.
_TRIGGER_PATTERNS: list[tuple[str, str]] = [
    ("is contraindicated in", "contraindication_phrase"),
    ("is contraindicated for", "contraindication_phrase"),
    ("are contraindicated in", "contraindication_phrase"),
    ("contraindicated in", "contraindication_phrase"),
    ("contraindicated for", "contraindication_phrase"),
    # bare forms: "…during pregnancy is contraindicated (see WARNINGS)"
    ("is contraindicated", "contraindication_phrase"),
    ("are contraindicated", "contraindication_phrase"),
    ("contraindicated", "contraindication_phrase"),
    ("should not be used in", "contraindication_phrase"),
    ("should not be administered to", "contraindication_phrase"),
    ("should not receive", "contraindication_phrase"),
    ("do not use in", "contraindication_phrase"),
    ("do not administer to", "contraindication_phrase"),
    ("do not give to", "contraindication_phrase"),
    ("is not recommended in", "contraindication_phrase"),
    ("are not recommended in", "contraindication_phrase"),
    ("hypersensitivity to", "contraindication_phrase"),
    ("is indicated for the treatment of", "indication_phrase"),
    ("are indicated for the treatment of", "indication_phrase"),
    ("is indicated in the treatment of", "indication_phrase"),
    ("are indicated in the treatment of", "indication_phrase"),
    ("is indicated as an adjunct to", "indication_phrase"),
    ("are indicated as an adjunct to", "indication_phrase"),
    ("is indicated for", "indication_phrase"),
    ("are indicated for", "indication_phrase"),
    ("is indicated in", "indication_phrase"),
    ("are indicated in", "indication_phrase"),
    ("is indicated as", "indication_phrase"),
    ("are indicated as", "indication_phrase"),
    ("is indicated to", "indication_phrase"),
    ("are indicated to", "indication_phrase"),
    ("indicated as an adjunct to", "indication_phrase"),
    ("indicated for the treatment of", "indication_phrase"),
    ("indicated to control", "indication_phrase"),
    ("indicated for", "indication_phrase"),
    ("indicated in", "indication_phrase"),
    ("indicated as", "indication_phrase"),
    ("indicated to", "indication_phrase"),
    ("for the treatment of", "treatment_verb"),
    ("for the management of", "treatment_verb"),
    ("for the prevention of", "treatment_verb"),
    ("for the prophylaxis of", "treatment_verb"),
    ("prevention and treatment of", "treatment_verb"),
    ("treatment and prevention of", "treatment_verb"),
    ("prophylaxis and treatment of", "treatment_verb"),
    ("treatment and prophylaxis of", "treatment_verb"),
    ("prophylaxis of", "treatment_verb"),
    ("used to treat", "treatment_verb"),
    ("used in the treatment of", "treatment_verb"),
]

#: Which cues are consistent with which asserted relation. A quote may contain BOTH an
#: indication phrase and a contraindication phrase (a "Limitations of Use" paragraph is the
#: classic case), so the cue that AGREES with the claim wins; a disagreeing cue is only
#: reported when it is the sole relational cue present — and then it is a review signal.
_CUE_RELATIONSHIP = {
    "indication_phrase": "INDICATION",
    "treatment_verb": "INDICATION",
    "contraindication_phrase": "CONTRAINDICATION",
}


def find_trigger(source_text: str, relationship: str | None = None) -> tuple[str, str]:
    """Locate the cue phrase that establishes a relation in ``source_text``.

    Returns ``(trigger_span, trigger_cue)`` where the span is the **verbatim** substring as it
    appears in the source (original casing), so it can be checked against ``input_value``.
    Deterministic and offline — no model call, nothing to rationalize.

    When ``relationship`` is given, a cue **consistent** with it is always preferred: a label
    that says "…are indicated as an adjunct… Limitations of Use: …should not be used in…"
    contains both kinds of cue, and the indication phrase is the one that governs an extracted
    indication. Only when no consistent cue exists is a contradicting one returned — which the
    caller treats as a review signal, not as truth.

    Returns ``("", "none_found")`` when no cue is present at all.
    """
    if not source_text:
        return "", "none_found"
    haystack = source_text.lower()
    matches: list[tuple[int, str, str]] = []
    for phrase, cue in _TRIGGER_PATTERNS:
        # Word-boundary matched: "contraindicated in" CONTAINS "indicated in", so a plain
        # substring search would read a contraindication as an indication.
        m = re.search(rf"\b{re.escape(phrase)}", haystack)
        if m:
            matches.append((m.start(), phrase, cue))
    if not matches:
        return "", "none_found"
    if relationship:
        consistent = [m for m in matches
                      if _CUE_RELATIONSHIP.get(m[2]) == relationship.upper()]
        if consistent:
            matches = consistent
    # earliest match wins; ties broken by the longer (more specific) phrase
    idx, phrase, cue = min(matches, key=lambda m: (m[0], -len(m[1])))
    return source_text[idx:idx + len(phrase)], cue


def combine_confidence(*values: float | None) -> float | None:
    """Product of the supplied confidences (``None`` ignored); ``None`` if none supplied.

    This is the ASSERTION-level combination (subject x object x relationship). For the
    pipeline-level product see :func:`medic.confidence.chain_confidence`, and for cross-source
    aggregation see :func:`medic.confidence.noisy_or` — which combines in the opposite
    direction, because more steps means more chance of error while more sources means more
    corroboration.
    """
    present = []
    for v in values:
        if v is None:
            continue
        try:
            present.append(float(v))
        except (TypeError, ValueError):
            continue
    if not present:
        return None
    out = 1.0
    for v in present:
        out *= v
    return round(out, 6)


def build_confidence_breakdown(
    subject: float | None, object_: float | None, relationship: float | None,
    *, basis: str = "MEASURED",
) -> dict:
    """The nested four-part confidence on an Assertion (I-11).

    Every component is required, so an unknown one becomes 1.0 rather than being omitted —
    omitting it used to mean "no contribution", which let an unresolved entity cost nothing.
    ``overall`` is always the product, so the arithmetic is checkable on the record.

    These are DATA-QUALITY numbers — how sure we are the entities and relation were read
    correctly — never evidence strength about the claim (docs/sepio-sieve-alignment.md §3).
    """
    def _f(value: float | None) -> float:
        if value is None:
            return 1.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 1.0

    s, o, r = _f(subject), _f(object_), _f(relationship)
    return {"subject": round(s, 6), "object": round(o, 6), "relationship": round(r, 6),
            "overall": round(s * o * r, 6), "basis": basis}


def build_assertion(
    *,
    supporting_quote: str | None = None,
    method: str = "LLM",
    tool: str | None = None,
    tool_version: str | None = None,
    relationship: str | None = None,
    subject_confidence: float | None = None,
    object_confidence: float | None = None,
    relationship_confidence: float | None = None,
    section_warrant: str | None = None,
    status: str | None = None,
    negated: bool = False,
    negation_cue: str | None = None,
    flags: list[str] | None = None,
) -> dict:
    """Build the claim-level provenance object (how the *relation* was read from the source).

    ``confidence`` is a nested ``ConfidenceBreakdown``: all four components are required
    (I-11), because a partial score is not comparable across records and a missing component
    used to default to "no contribution". Its ``overall`` is the **product** of the three
    inputs, all recorded explicitly so the arithmetic is auditable on the record itself:

    * ``subject_confidence`` — how well the subject entity resolved (MeDIC: the drug),
    * ``object_confidence`` — how well the object entity resolved (the disease),
    * ``relationship_confidence`` — how well the text supports the relation itself.

    The rationale is **extractive**: :func:`find_trigger` locates the cue phrase verbatim in
    the supporting quote and records it as ``trigger_span`` + ``trigger_cue``. No generated
    explanation is stored — a post-hoc narration cannot be verified, and a confident-sounding
    one for a wrong extraction is worse than none.
    """
    assertion: dict = {"method": method}
    if supporting_quote:
        assertion["input_value"] = supporting_quote
    _stamp_tool(assertion, tool or "medic-extractor", tool_version)
    # An LLM-read claim is pinned to its dated model id (FAILURE_MODES 13.1).
    if method == "LLM":
        assertion["agent"] = llm_agent("extraction")
    if relationship:
        assertion["relationship"] = relationship
    assertion["confidence"] = build_confidence_breakdown(
        subject_confidence, object_confidence, relationship_confidence)
    span, cue = find_trigger(supporting_quote or "", relationship)
    out_flags = list(flags or [])
    if span:
        assertion["trigger_span"] = span
        # The only relational cue in the text contradicts the relation we recorded — most
        # often a "Limitations of Use" clause quoted as if it governed the claim. Flag for
        # review (FAILURE_MODES 3.5/4.1) rather than trusting either reading.
        cue_rel = _CUE_RELATIONSHIP.get(cue)
        if relationship and cue_rel and cue_rel != relationship.upper():
            if "wrong_section" not in out_flags:
                out_flags.append("wrong_section")
    elif section_warrant:
        # No phrase cue, but the section this text came from asserts the relation by
        # construction (curated in conf/section_warrants.yaml) — structural provenance.
        cue = "section_header"
        assertion["section_warrant"] = section_warrant
    assertion["trigger_cue"] = cue
    if status:
        assertion["status"] = status
    if negation_cue:
        assertion["negation_cue"] = negation_cue
    if negated and "negated_inversion" not in out_flags:
        out_flags.append("negated_inversion")
    assertion["flags"] = out_flags
    return assertion


def build_mention(
    original_literal: str,
    entity_type: str,
    *,
    mention_id: str | None = None,
    source: str | None = None,
    source_language: str | None = None,
    source_spans: list[dict] | None = None,
    spans: list[dict] | None = None,
    translation: dict | None = None,
    grounding: dict | None = None,
    normalization: dict | None = None,
    extraction: dict | None = None,
    applied_rules: list[str] | None = None,
    grounding_predicate: str = "",
    grounding_flags: list[str] | None = None,
    merge_normalization: dict | None = None,
    resolved_id: str | None = None,
    resolved_label: str | None = None,
) -> dict:
    """Assemble a Mention dict (ordered steps) from the legacy stage objects.

    ``entity_type`` is the display kind (``"drug"`` / ``"disease"``). ``mention_id`` is
    normally taken from the source record (minted at ingest, I-9); if absent it is minted
    deterministically from ``(entity_type + 's', original_literal)`` to stay consistent
    with :func:`medic.mention.mint_mention_id`.
    """
    if not mention_id:
        mint_type = entity_type if entity_type.endswith("s") else entity_type + "s"
        mention_id = mint_mention_id(original_literal, mint_type)

    steps: list[dict] = []
    if extraction:
        extraction.setdefault("entity_type", entity_type)
        steps.append(_extraction_step(
            extraction, original_literal=original_literal, spans=spans))
    if translation:
        tstep = _translation_step(translation)
        if tstep:
            steps.append(tstep)
    # The grounder sees the English translation for non-English sources.
    grounding_input = ""
    if translation and translation.get("translation_value"):
        grounding_input = translation["translation_value"]
    if grounding:
        gstep = _grounding_step(
            grounding, input_value=grounding_input, applied_rules=applied_rules,
            predicate_id=grounding_predicate, flags=grounding_flags)
        if gstep:
            steps.append(gstep)
    if normalization:
        nstep = _normalization_step(
            normalization, fallback_input=(grounding or {}).get("grounded_id"))
        if nstep:
            steps.append(nstep)
    # A normalization applied at MERGE time (e.g. the disease-list xref hop HP -> MONDO) is a
    # real transformation and must be a named step, or the chain would not end where the
    # record actually ends (I-8).
    if merge_normalization:
        mstep = _normalization_step(
            merge_normalization,
            fallback_input=steps[-1].get("output_value") if steps else None)
        if mstep:
            steps.append(mstep)

    if resolved_id is None:
        resolved_id = ((normalization or {}).get("normalized_id")
                       or (grounding or {}).get("grounded_id") or None)
    if resolved_label is None:
        resolved_label = ((normalization or {}).get("normalized_label")
                          or (grounding or {}).get("grounded_label") or None)

    mention: dict = {
        "id": mention_id,
        "original_literal": original_literal,
        "entity_type": entity_type,
    }
    if source:
        mention["mention_source"] = source
    if source_language:
        mention["source_language"] = source_language
    # `spans` is the typed form (TextSpanRoleEnum + document); `source_spans` is the untyped
    # legacy one, removed in Plan 3 once every caller passes typed spans.
    if spans:
        mention["source_spans"] = spans
    elif source_spans:
        mention["source_spans"] = source_spans
    if steps:
        mention["resolution"] = _build_resolution(steps)
    if resolved_id:
        mention["resolved_id"] = resolved_id
    if resolved_label:
        mention["resolved_label"] = resolved_label
    return mention


def validate_mention_chain(mention: dict) -> list[str]:
    """Return the chain-invariant violations on a built Mention (empty = intact).

    Enforces I-8 end to end, including the part ``_build_resolution`` cannot see: that the
    chain actually ends where the record says it resolved. A mention that asserts a
    ``resolved_id`` the pipeline never produced has provenance that does not explain it.
    """
    problems: list[str] = []
    resolution = mention.get("resolution") or {}
    pipeline = resolution.get("pipeline") or []
    mid = mention.get("id", "?")
    if not pipeline:
        if mention.get("resolved_id"):
            problems.append(f"{mid}: resolved_id set but no pipeline")
        return problems
    for i, (prev, nxt) in enumerate(zip(pipeline, pipeline[1:])):
        if prev.get("output_value") != nxt.get("input_value"):
            problems.append(
                f"{mid}: step {i} {prev.get('category')} output != step {i+1} "
                f"{nxt.get('category')} input")
    if resolution.get("input_value") != pipeline[0].get("input_value"):
        problems.append(f"{mid}: resolution.input_value != pipeline[0].input_value")
    if resolution.get("output_value") != pipeline[-1].get("output_value"):
        problems.append(f"{mid}: resolution.output_value != pipeline[-1].output_value")
    resolved_id = mention.get("resolved_id")
    if resolved_id and resolution.get("output_value") != resolved_id:
        problems.append(
            f"{mid}: chain ends at {resolution.get('output_value')!r} but resolved_id is "
            f"{resolved_id!r}")
    return problems


def _build_resolution(steps: list[dict]) -> dict:
    """Wrap the ordered steps in a Resolution container: aggregate in/out + product
    confidence + the chaining-enforced pipeline.

    Chaining invariant (I-8): each step's output_value must equal the next step's
    input_value. We enforce it by construction — coercing input[i+1] := output[i] — and
    log any mismatch found in the source objects so a real data gap is visible, not hidden.
    """
    for prev, nxt in zip(steps, steps[1:]):
        prev_out = prev.get("output_value", "")
        if nxt.get("input_value", "") != prev_out:
            logger.warning(
                "provenance chain break: %s.output_value=%r != %s.input_value=%r; coercing",
                prev.get("category"), prev_out, nxt.get("category"), nxt.get("input_value"),
            )
            nxt["input_value"] = prev_out
    # aggregate confidence = product of per-step confidences (missing treated as 1.0)
    confidence = 1.0
    for s in steps:
        c = s.get("confidence")
        if c is not None:
            try:
                confidence *= float(c)
            except (TypeError, ValueError):
                pass
    return {
        "input_value": steps[0].get("input_value", ""),
        "output_value": steps[-1].get("output_value", ""),
        "confidence": round(confidence, 6),
        "pipeline": steps,
    }


def validate_source_assertion(assertion: dict) -> list[str]:
    """Violations of the assertion-level invariants (design spec §7). Empty = intact.

    I-8b span anchoring, I-10 source consistency, I-11 confidence completeness,
    I-12 terminal normalization. The I-10 check is the one that would have caught the defect
    that started this work: an Indian indication carrying a Russian drug trail.
    """
    problems: list[str] = []
    source = assertion.get("source", "")
    document = assertion.get("document", "")
    spans = assertion.get("spans") or []

    for role in ("drug", "disease"):
        mention = assertion.get(role)
        if not isinstance(mention, dict):
            continue
        problems.extend(validate_mention_chain(mention))
        if mention.get("mention_source") and mention["mention_source"] != source:
            problems.append(
                f"{document}: {role}.mention_source={mention['mention_source']!r} != "
                f"assertion.source={source!r} (I-10)")
        pipeline = (mention.get("resolution") or {}).get("pipeline") or []
        for step in pipeline:
            if step.get("confidence") is None:
                problems.append(
                    f"{document}: {role} {step.get('category')} has no confidence (I-11)")
            if not step.get("confidence_basis"):
                problems.append(
                    f"{document}: {role} {step.get('category')} has no confidence_basis (I-11)")
            idx = step.get("span_index")
            if step.get("category") == "EXTRACTION" and idx is not None and spans:
                if not 0 <= idx < len(spans):
                    problems.append(f"{document}: {role} span_index {idx} out of range (I-8b)")
                elif spans[idx].get("text") != step.get("input_value"):
                    problems.append(
                        f"{document}: {role} extraction input != spans[{idx}].text (I-8b)")
        if pipeline and ":" in str(pipeline[-1].get("output_value", "")):
            if pipeline[-1].get("category") != "NORMALIZATION":
                problems.append(
                    f"{document}: {role} chain ends in {pipeline[-1].get('category')}, "
                    f"expected NORMALIZATION (I-12)")

    for i, span in enumerate(spans):
        # A span with no document is as much an I-10 gap as one with the wrong document: it
        # cannot say which source it came from.
        if not span.get("document"):
            problems.append(f"{document}: spans[{i}] has no document (I-10)")
        elif document and span["document"] != document:
            problems.append(
                f"{document}: spans[{i}].document={span['document']!r} != "
                f"assertion.document (I-10)")

    conf = (assertion.get("assertion") or {}).get("confidence")
    if isinstance(conf, dict):
        missing = [k for k in ("subject", "object", "relationship", "overall", "basis")
                   if conf.get(k) is None]
        if missing:
            problems.append(f"{document}: confidence missing {', '.join(missing)} (I-11)")
        else:
            expected = float(conf["subject"]) * float(conf["object"]) * float(conf["relationship"])
            if abs(float(conf["overall"]) - expected) > 1e-6:
                problems.append(
                    f"{document}: confidence.overall {conf['overall']} != product "
                    f"{expected:.6f} (I-11)")
    return problems


def validate_pair(pair: dict) -> list[str]:
    """Violations of the pair-level invariants, including every assertion's (I-13)."""
    from medic.confidence import corroboration

    problems: list[str] = []
    assertions = pair.get("assertions") or []
    for a in assertions:
        problems.extend(validate_source_assertion(a))
    conf = pair.get("confidence")
    if isinstance(conf, dict):
        if conf.get("n_assertions") != len(assertions):
            problems.append(
                f"n_assertions {conf.get('n_assertions')} != {len(assertions)} assertions (I-13)")
        by_source: dict[str, list[float]] = {}
        for a in assertions:
            value = ((a.get("assertion") or {}).get("confidence") or {}).get("overall")
            if value is not None:
                by_source.setdefault(a.get("source", ""), []).append(float(value))
        if "n_sources" in conf and conf.get("n_sources") != len(by_source):
            problems.append(
                f"n_sources {conf.get('n_sources')} != {len(by_source)} distinct sources (I-13)")
        expected = corroboration(by_source)
        if abs(float(conf.get("overall", -1)) - expected) > 1e-5:
            problems.append(
                f"confidence.overall {conf.get('overall')} != corroboration {expected:.6f} "
                f"over {len(by_source)} source(s) (I-13)")
    return problems
