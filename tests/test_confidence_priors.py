"""Confidence priors, resolution and aggregation (spec D7, D8; invariant I-11).

Distinct from tests/test_confidence.py, which covers the lexical grounder's
Jaro-Winkler scoring in medic.grounding.confidence.

Every confidence in MeDIC is a data-quality number — how sure we are the linking is right —
never an evidence-strength number about the claim. See docs/sepio-sieve-alignment.md §3.
"""

import pytest

from medic.confidence import (
    load_defaults,
    chain_confidence,
    index_priors,
    load_priors,
    noisy_or,
    prior_scope,
    resolve_confidence,
    weakest_basis,
)

LLM_AGENT = {
    "agent_type": "AI_AGENT",
    "agent_name": "anthropic/claude-haiku-4-5-20251001",
    "agent_version": "claude-haiku-4-5-20251001",
}
DEEPL_AGENT = {"agent_id": "wikidata:Q116709136", "agent_type": "AI_AGENT", "agent_name": "DeepL"}


# --- resolve_confidence: every step gets a value AND a declared basis ---------------------

def test_measured_value_is_kept_and_marked_measured():
    assert resolve_confidence("GROUNDING", "LEXICAL_MATCH", 0.765) == (0.765, "MEASURED")


def test_deterministic_step_is_one_point_zero():
    assert resolve_confidence(
        "NORMALIZATION", "DETERMINISTIC_RULE", None, deterministic=True
    ) == (1.0, "DETERMINISTIC")


def test_structured_field_read_has_no_score_but_cannot_be_wrong():
    assert resolve_confidence(
        "EXTRACTION", "STRUCTURED_FIELD", None, deterministic=True
    ) == (1.0, "DETERMINISTIC")


TRANSLATION_PRIOR = [{
    "category": "TRANSLATION", "method": "API",
    "tool": "babelon", "tool_version": "0.3.6",
    "value": 0.90, "calibrated": False, "rationale": "test",
}]
EXTRACTION_PRIOR = [{
    "category": "EXTRACTION", "method": "LLM",
    "agent_name": "anthropic/claude-haiku-4-5-20251001",
    "agent_version": "claude-haiku-4-5-20251001",
    "value": 0.80, "calibrated": False, "rationale": "test",
}]


def test_unmeasured_nondeterministic_step_falls_back_to_a_prior():
    assert resolve_confidence(
        "TRANSLATION", "API", None, agent=DEEPL_AGENT, tool="babelon",
        tool_version="0.3.6", priors=TRANSLATION_PRIOR,
    ) == (0.90, "PRIOR")


def test_a_gap_with_no_family_default_at_all_is_a_loud_failure():
    with pytest.raises(KeyError, match="no family default"):
        resolve_confidence("TRANSLATION", "HUMAN", None, tool="babelon",
                           tool_version="0.3.6", priors=TRANSLATION_PRIOR, defaults=[])


# --- scoping: LLMs by model, everything else by tool + version ----------------------------

def test_a_versioned_agent_is_scoped_to_its_model():
    assert prior_scope(LLM_AGENT, "medic-extractor", "1") == (
        "agents", "anthropic/claude-haiku-4-5-20251001", "claude-haiku-4-5-20251001")


def test_deepl_has_no_version_so_it_falls_to_the_tool_scope():
    """DeepL publishes no engine version; the babelon release is what is knowable."""
    assert prior_scope(DEEPL_AGENT, "babelon", "0.3.6") == ("tools", "babelon", "0.3.6")


def test_an_agentless_step_is_scoped_to_its_tool():
    assert prior_scope(None, "medic-lexical-grounder", "1") == (
        "tools", "medic-lexical-grounder", "1")


def test_a_model_bump_does_not_inherit_the_old_models_value():
    """The point of model scoping: a bumped model gets its family value, not the stale one."""
    hand_tuned = [dict(EXTRACTION_PRIOR[0], value=0.42, calibrated=True)]
    bumped = dict(LLM_AGENT, agent_name="anthropic/claude-haiku-9-9-20991231",
                  agent_version="claude-haiku-9-9-20991231")
    value, _ = resolve_confidence("EXTRACTION", "LLM", None, agent=bumped,
                                  priors=hand_tuned, defaults=DEFAULTS)
    assert value == 0.85
    assert value != 0.42


def test_a_tool_version_bump_does_not_inherit_the_old_versions_value():
    hand_tuned = [dict(TRANSLATION_PRIOR[0], value=0.42, calibrated=True)]
    value, _ = resolve_confidence("TRANSLATION", "API", None, tool="babelon",
                                  tool_version="0.4.0", priors=hand_tuned, defaults=DEFAULTS)
    assert value == 0.95
    assert value != 0.42


# --- the config is records, not identifiers-as-keys ---------------------------------------

def test_priors_load_as_a_list_of_records():
    priors = load_priors()
    assert isinstance(priors, list)
    assert all(isinstance(p, dict) for p in priors)
    assert {"category", "method", "value", "calibrated", "rationale"} <= set(priors[0])


def test_every_shipped_prior_names_exactly_one_scope():
    for record in load_priors():
        has_agent = bool(record.get("agent_name"))
        has_tool = bool(record.get("tool"))
        assert has_agent != has_tool, record


def test_a_record_naming_both_scopes_is_rejected():
    bad = [dict(TRANSLATION_PRIOR[0], agent_name="x", agent_version="y")]
    with pytest.raises(ValueError, match="exactly one of"):
        index_priors(bad)


def test_a_record_naming_neither_scope_is_rejected():
    bad = [{"category": "TRANSLATION", "method": "API", "value": 0.9,
            "calibrated": False, "rationale": "test"}]
    with pytest.raises(ValueError, match="exactly one of"):
        index_priors(bad)


def test_a_duplicate_prior_is_rejected_rather_than_last_one_wins():
    with pytest.raises(ValueError, match="duplicate confidence prior"):
        index_priors(TRANSLATION_PRIOR + TRANSLATION_PRIOR)


def test_shipped_priors_cover_the_producers_that_need_them():
    index = index_priors(load_priors())
    assert index[("agents", "anthropic/claude-haiku-4-5-20251001",
                  "claude-haiku-4-5-20251001", "EXTRACTION", "LLM")] == 0.85
    assert index[("tools", "babelon", "0.3.6", "TRANSLATION", "API")] == 0.95
    assert index[("tools", "medic-lexical-grounder", "1",
                  "GROUNDING", "SOURCE_ASSERTED")] == 0.70


def test_weakest_basis_prefers_prior_then_measured():
    assert weakest_basis(["DETERMINISTIC", "MEASURED", "PRIOR"]) == "PRIOR"
    assert weakest_basis(["DETERMINISTIC", "MEASURED"]) == "MEASURED"
    assert weakest_basis(["DETERMINISTIC", "DETERMINISTIC"]) == "DETERMINISTIC"
    assert weakest_basis([]) == "DETERMINISTIC"


# --- aggregation: chains decay, sources corroborate ---------------------------------------

def test_chain_confidence_is_the_product_with_the_weakest_basis():
    """The Russia etifoxine chain from the design spec §4.2."""
    steps = [
        {"confidence": 1.0, "confidence_basis": "DETERMINISTIC"},
        {"confidence": 0.90, "confidence_basis": "PRIOR"},
        {"confidence": 0.765, "confidence_basis": "MEASURED"},
        {"confidence": 1.0, "confidence_basis": "DETERMINISTIC"},
    ]
    value, basis = chain_confidence(steps)
    assert value == pytest.approx(0.6885)
    assert basis == "PRIOR"


def test_chain_confidence_of_an_empty_pipeline_is_one_and_deterministic():
    assert chain_confidence([]) == (1.0, "DETERMINISTIC")


def test_chain_confidence_rejects_a_step_missing_its_confidence():
    """I-11: a step without a confidence is a bug, not a free 1.0."""
    with pytest.raises(ValueError, match="missing confidence"):
        chain_confidence([{"category": "GROUNDING"}])


def test_noisy_or_rewards_corroboration():
    assert noisy_or([0.7, 0.7]) == pytest.approx(0.91)
    assert noisy_or([0.72, 0.620]) == pytest.approx(0.8936)


def test_noisy_or_of_one_value_is_that_value():
    assert noisy_or([0.72]) == pytest.approx(0.72)


def test_noisy_or_of_nothing_is_zero():
    assert noisy_or([]) == 0.0


def test_the_config_validates_against_its_linkml_class():
    """conf/confidence_priors.yaml is schema-governed like every other MeDIC data structure."""
    import subprocess

    out = subprocess.run(
        ["uv", "run", "linkml-validate",
         "--schema", "src/medic/schema/provenance.yaml",
         "--target-class", "ConfidencePriorSet",
         "conf/confidence_priors.yaml"],
        capture_output=True, text=True,
    )
    assert out.returncode == 0, f"validate failed:\n{out.stdout}\n{out.stderr}"


# --- auto-minting on first use of an unseen producer --------------------------------------

DEFAULTS = [
    {"family": "DEEPL", "applies_to": "TOOL", "name_matches": "babelon",
     "value": 0.95, "rationale": "t"},
    {"family": "HAIKU", "applies_to": "AGENT", "name_matches": "haiku",
     "value": 0.85, "rationale": "t"},
    {"family": "SONNET", "applies_to": "AGENT", "name_matches": "sonnet",
     "value": 0.90, "rationale": "t"},
    {"family": "OPUS", "applies_to": "AGENT", "name_matches": "opus",
     "value": 0.95, "rationale": "t"},
    {"family": "FABLE", "applies_to": "AGENT", "name_matches": "fable",
     "value": 0.97, "rationale": "t"},
]


@pytest.mark.parametrize("model,expected", [
    ("anthropic/claude-haiku-9-9-20991231", 0.85),
    ("anthropic/claude-sonnet-5", 0.90),
    ("anthropic/claude-opus-5", 0.95),
    ("anthropic/claude-fable-5", 0.97),
])
def test_an_unseen_model_mints_its_family_value(model, expected):
    agent = {"agent_type": "AI_AGENT", "agent_name": model, "agent_version": model.split("/")[-1]}
    value, basis = resolve_confidence(
        "EXTRACTION", "LLM", None, agent=agent, priors=[], defaults=DEFAULTS)
    assert (value, basis) == (expected, "PRIOR")


def test_a_babelon_version_bump_still_gets_the_deepl_family_value():
    """Family values are version-independent, so a tool bump does not break the build."""
    value, basis = resolve_confidence(
        "TRANSLATION", "API", None, agent=DEEPL_AGENT, tool="babelon",
        tool_version="9.9.9", priors=[], defaults=DEFAULTS)
    assert (value, basis) == (0.95, "PRIOR")


def test_a_producer_matching_no_family_still_raises():
    with pytest.raises(KeyError, match="no family default"):
        resolve_confidence("GROUNDING", "LEXICAL_MATCH", None,
                           tool="some-new-grounder", tool_version="1",
                           priors=[], defaults=DEFAULTS)


def test_an_ambiguous_family_match_raises():
    ambiguous = DEFAULTS + [{"family": "ALSO_HAIKU", "applies_to": "AGENT",
                             "name_matches": "claude", "value": 0.5, "rationale": "t"}]
    with pytest.raises(ValueError, match="matches several prior families"):
        resolve_confidence("EXTRACTION", "LLM", None, agent=LLM_AGENT,
                           priors=[], defaults=ambiguous)


def test_an_existing_prior_is_never_replaced_by_a_family_default():
    """A human edit must survive; minting only fills genuine gaps."""
    curated = [dict(EXTRACTION_PRIOR[0], value=0.42, calibrated=True)]
    value, _ = resolve_confidence("EXTRACTION", "LLM", None, agent=LLM_AGENT,
                                  priors=curated, defaults=DEFAULTS)
    assert value == 0.42


def test_minting_writes_a_schema_valid_record_and_does_not_clobber_the_file(tmp_path):
    import shutil
    import subprocess

    import yaml as _yaml

    target = tmp_path / "confidence_priors.yaml"
    shutil.copy("conf/confidence_priors.yaml", target)
    before = _yaml.safe_load(target.read_text())

    agent = {"agent_type": "AI_AGENT", "agent_name": "anthropic/claude-opus-5",
             "agent_version": "claude-opus-5"}
    value, basis = resolve_confidence(
        "EXTRACTION", "LLM", None, agent=agent, path=str(target))
    assert (value, basis) == (0.95, "PRIOR")

    after = _yaml.safe_load(target.read_text())
    assert len(after["priors"]) == len(before["priors"]) + 1
    assert after["defaults"] == before["defaults"]          # comments/defaults untouched
    assert before["priors"] == after["priors"][:len(before["priors"])]   # nothing rewritten

    minted = after["priors"][-1]
    assert minted["agent_name"] == "anthropic/claude-opus-5"
    assert minted["value"] == 0.95
    assert minted["auto_generated"] is True
    assert minted["calibrated"] is False
    assert minted["family"] == "OPUS"

    out = subprocess.run(
        ["uv", "run", "linkml-validate", "--schema", "src/medic/schema/provenance.yaml",
         "--target-class", "ConfidencePriorSet", str(target)],
        capture_output=True, text=True,
    )
    assert out.returncode == 0, f"minted file invalid:\n{out.stdout}\n{out.stderr}"


def test_minting_is_idempotent(tmp_path):
    import shutil

    import yaml as _yaml

    target = tmp_path / "confidence_priors.yaml"
    shutil.copy("conf/confidence_priors.yaml", target)
    agent = {"agent_type": "AI_AGENT", "agent_name": "anthropic/claude-sonnet-5",
             "agent_version": "claude-sonnet-5"}
    for _ in range(3):
        resolve_confidence("EXTRACTION", "LLM", None, agent=agent, path=str(target))
    priors = _yaml.safe_load(target.read_text())["priors"]
    minted = [p for p in priors if p.get("agent_name") == "anthropic/claude-sonnet-5"]
    assert len(minted) == 1


def test_shipped_defaults_match_the_agreed_family_values():
    values = {d["family"]: d["value"] for d in load_defaults()}
    assert values == {"DEEPL": 0.95, "HAIKU": 0.85, "SONNET": 0.90,
                      "OPUS": 0.95, "FABLE": 0.97}
