"""Tests for the version strings stamped into transformation provenance."""

import re

from medic.versions import (
    COMPONENT_VERSIONS,
    deepl_agent,
    llm_agent,
    medic_release,
    package_version,
    tool_ref,
)


def test_medic_release_is_stable_not_a_commit_string():
    # uv-dynamic-versioning yields e.g. "1.0.0.post70.dev0+774bf04"; stamping that would
    # rewrite every product record on every commit. We keep only MAJOR.MINOR.PATCH.
    rel = medic_release()
    assert re.fullmatch(r"\d+(\.\d+){0,2}", rel), rel
    assert "+" not in rel and "dev" not in rel and "post" not in rel


def test_component_versions_are_hand_bumped():
    assert tool_ref("medic-lexical-grounder") == (
        "medic-lexical-grounder", COMPONENT_VERSIONS["medic-lexical-grounder"])


def test_tool_ref_splits_legacy_name_slash_version():
    assert tool_ref("medic-normalizer/1") == ("medic-normalizer", "1")


def test_tool_ref_unregistered_medic_component_falls_back_to_release():
    name, ver = tool_ref("medic-ingest-china")
    assert name == "medic-ingest-china"
    assert ver == medic_release()


def test_tool_ref_third_party_uses_distribution_version():
    name, ver = tool_ref("babelon")
    assert name == "babelon"
    assert ver == package_version("babelon")
    assert ver  # babelon is a declared dependency, so it must resolve


def test_llm_agent_pins_the_dated_model_id():
    a = llm_agent("extraction")
    assert a["agent_type"] == "AI_AGENT"
    # the version-bearing part is the model id, without the litellm provider prefix
    assert "/" not in a.get("agent_version", "")
    assert a["agent_version"]


def test_deepl_agent_omits_an_invented_version():
    a = deepl_agent()
    assert a["agent_id"] == "wikidata:Q116709136"
    assert a["agent_type"] == "AI_AGENT"
    # DeepL publishes no engine version — better absent than invented
    assert "agent_version" not in a
