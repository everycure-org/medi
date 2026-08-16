"""Version strings stamped into transformation provenance.

Every step in a ``Mention.resolution.pipeline`` (and every ``Assertion``) records **what ran
it**: a ``tool`` + ``tool_version``, and for non-deterministic steps an ``agent`` +
``agent_version``. This module is the single place those strings are resolved, so the same
run always stamps the same values and a reviewer can tell which code/model produced a record.

**Why not the full package version.** ``importlib.metadata.version("medic")`` returns a
uv-dynamic-versioning string like ``1.0.0.post70.dev0+774bf04`` — it embeds the git commit and
therefore changes on *every commit*. Stamping that into ~10k product records would rewrite
every record on every commit and drown the diffs that matter. So:

* MeDIC's own deterministic components carry a **hand-bumped component version**
  (:data:`COMPONENT_VERSIONS`) — bump it when the component's *behaviour* changes. This
  follows the convention already in the normalization store (``medic-normalizer/1``).
* Where a released MeDIC version is genuinely wanted, :func:`medic_release` returns only the
  stable ``MAJOR.MINOR.PATCH`` part, which changes on release rather than on commit.
* Third-party tools carry their real distribution version (babelon, …).
* LLM agents carry the **dated model id**, which is the one that actually matters
  (FAILURE_MODES 13.1: a model upgrade silently changes extraction output).
"""

from __future__ import annotations

import re
from functools import lru_cache

#: Hand-bumped versions of MeDIC's own deterministic components. Bump a value when that
#: component's behaviour changes in a way that could alter its output — that is the signal a
#: reviewer reads off a record, not the git commit.
COMPONENT_VERSIONS = {
    "medic-lexical-grounder": "1",
    "medic-normalizer": "1",
    "medic-ingest": "1",
    "medic-extractor": "1",
}

#: Wikidata id for DeepL, the engine behind the babelon translation service.
DEEPL_WIKIDATA = "wikidata:Q116709136"


@lru_cache(maxsize=None)
def medic_release() -> str:
    """MeDIC's released version — the stable ``MAJOR.MINOR.PATCH`` part only.

    Deliberately drops the ``.postN.devN+<commit>`` suffix that uv-dynamic-versioning adds, so
    the value changes on release rather than on every commit (see the module docstring).
    """
    try:
        from importlib.metadata import version
        raw = version("medic")
    except Exception:
        return ""
    m = re.match(r"^(\d+(?:\.\d+){0,2})", raw)
    return m.group(1) if m else raw


@lru_cache(maxsize=None)
def package_version(distribution: str) -> str:
    """Installed version of a third-party distribution (``""`` if not installed)."""
    try:
        from importlib.metadata import version
        return version(distribution)
    except Exception:
        return ""


def tool_ref(tool: str) -> tuple[str, str]:
    """``(tool, tool_version)`` for a named tool.

    MeDIC components resolve against :data:`COMPONENT_VERSIONS`; anything else is looked up as
    an installed distribution. Accepts a legacy ``"name/version"`` string and splits it.
    """
    if "/" in tool:
        name, _, ver = tool.partition("/")
        return name, ver
    if tool in COMPONENT_VERSIONS:
        return tool, COMPONENT_VERSIONS[tool]
    if tool.startswith("medic-"):
        # A MeDIC component with no hand-bumped entry (e.g. a per-source ingest parser):
        # fall back to the released MeDIC version, which still says "this code produced it".
        return tool, medic_release()
    return tool, package_version(tool)


def llm_agent(task: str = "extraction") -> dict:
    """The `ProvenanceAgent` for an LLM-run step, pinned to the dated model id.

    ``agent_version`` is the model id MeDIC would use for ``task`` right now (resolved through
    the same ``medic.llm.get_model`` precedence the caller uses: explicit > env > config), so a
    record says exactly which model produced it.
    """
    model = ""
    try:
        from medic.llm import get_model
        model = get_model(task) or ""
    except Exception:
        model = ""
    # litellm format is "provider/model-id"; the model id is the version-bearing part.
    _, _, model_id = model.rpartition("/")
    agent: dict = {"agent_type": "AI_AGENT", "agent_name": model or "llm"}
    if model_id:
        agent["agent_version"] = model_id
    return agent


def deepl_agent() -> dict:
    """The `ProvenanceAgent` for a DeepL machine translation.

    DeepL publishes no per-translation engine version, so ``agent_version`` is deliberately
    omitted rather than invented — the *tool* version (the babelon release that called it) is
    what is actually knowable, and that goes on the step.
    """
    return {
        "agent_id": DEEPL_WIKIDATA,
        "agent_type": "AI_AGENT",
        "agent_name": "DeepL",
    }
