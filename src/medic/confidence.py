"""Confidence resolution and aggregation: every step gets a value and a declared basis.

Invariant I-11 requires each transformation step to carry both ``confidence`` and
``confidence_basis``, so a reader can tell a measured score from an assumed one. Before this,
unmeasured steps simply omitted ``confidence`` and silently contributed nothing to the
aggregate — an unreviewed machine translation cost a record exactly zero.

Two aggregations live here and they run in **opposite directions**, deliberately:

* :func:`chain_confidence` — a resolution pipeline. Confidence **decays** multiplicatively:
  every step is another chance to have linked the wrong entity.
* :func:`noisy_or` — independent source assertions about one pair. Confidence **accumulates**:
  two regulators agreeing is corroboration.

Every number here is a DATA-QUALITY number (how sure are we the linking is right), never an
evidence-strength number about the claim. Mapping one onto the other would convert "unsure of
the identifier" into "weak evidence for the treatment" — see docs/sepio-sieve-alignment.md §3.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Iterable, Mapping

import yaml

logger = logging.getLogger(__name__)

PRIORS_PATH = "conf/confidence_priors.yaml"

#: Weakest first. A chain's basis is the weakest basis any of its steps carries.
_BASIS_ORDER = ("PRIOR", "MEASURED", "DETERMINISTIC")


def load_defaults(path: str = PRIORS_PATH) -> list[dict]:
    """The ``ConfidencePriorDefault`` family rules from ``conf/confidence_priors.yaml``."""
    with open(path) as fh:
        data = yaml.safe_load(fh) or {}
    return list(data.get("defaults") or [])


def load_priors(path: str = PRIORS_PATH) -> list[dict]:
    """The ``ConfidencePrior`` records from ``conf/confidence_priors.yaml``, in file order.

    The file is a **list of records**, not a nested map keyed by identifiers: identifiers as
    mapping keys cannot be schema-validated, cannot carry their own metadata (``calibrated``,
    ``rationale``), and force composite keys to be smashed into strings. The tuple keys below
    exist only in memory, built by :func:`index_priors`.
    """
    with open(path) as fh:
        data = yaml.safe_load(fh) or {}
    return list(data.get("priors") or [])


def _record_key(record: dict) -> tuple:
    """The lookup key for one prior record, validating the exactly-one-scope rule."""
    has_agent = bool(record.get("agent_name"))
    has_tool = bool(record.get("tool"))
    if has_agent == has_tool:
        raise ValueError(
            f"confidence prior must name exactly one of (agent_name, tool), got {record!r}"
        )
    if has_agent:
        if not record.get("agent_version"):
            raise ValueError(f"agent-scoped prior needs an agent_version: {record!r}")
        scope = ("agents", record["agent_name"], record["agent_version"])
    else:
        if not record.get("tool_version"):
            raise ValueError(f"tool-scoped prior needs a tool_version: {record!r}")
        scope = ("tools", record["tool"], str(record["tool_version"]))
    return (*scope, record["category"], record["method"])


def index_priors(records: list[dict]) -> dict[tuple, float]:
    """Index prior records for lookup. Raises on a duplicate key rather than last-one-wins."""
    index: dict[tuple, float] = {}
    for record in records:
        key = _record_key(record)
        if key in index:
            raise ValueError(f"duplicate confidence prior for {key}")
        index[key] = float(record["value"])
    return index


@functools.lru_cache(maxsize=4)
def _load_index(path: str = PRIORS_PATH) -> dict[tuple, float]:
    """Cached lookup index. Invalidated by :func:`append_prior` when a record is minted."""
    return index_priors(load_priors(path))


def match_default(scope: tuple[str, str, str], defaults: list[dict]) -> dict:
    """The family default covering a producer scope. Raises if none or several match.

    Matching is a case-insensitive substring of the producer's name — the ``agent_name`` for a
    versioned model, the tool name for a tool — and is deliberately version-independent: a
    Haiku is a Haiku, whatever its date.
    """
    kind, name, _version = scope
    wanted = "AGENT" if kind == "agents" else "TOOL"
    lowered = (name or "").lower()
    hits = [
        d for d in defaults
        if d.get("applies_to") == wanted and (d.get("name_matches") or "").lower() in lowered
    ]
    if len(hits) > 1:
        families = ", ".join(sorted(str(d.get("family")) for d in hits))
        raise ValueError(f"producer {name!r} matches several prior families: {families}")
    if not hits:
        raise KeyError(
            f"no confidence prior and no family default for {wanted.lower()} {name!r}: add a "
            f"prior, or add a family default to {PRIORS_PATH}. Inventing a number for an "
            f"unknown producer would be worse than failing."
        )
    return hits[0]


def mint_prior(
    scope: tuple[str, str, str], category: str, method: str, default: dict
) -> dict:
    """Build the ``ConfidencePrior`` record for a producer's first use of ``category/method``."""
    kind, name, version = scope
    record = {"category": category, "method": method}
    if kind == "agents":
        record["agent_name"] = name
        record["agent_version"] = version
    else:
        record["tool"] = name
        record["tool_version"] = version
    record.update({
        "value": float(default["value"]),
        "calibrated": False,
        "auto_generated": True,
        "family": default["family"],
        "rationale": (
            f"Auto-minted from the {default['family']} family default on first use of "
            f"{name} {version} for {category}/{method}. Not calibrated — review and set "
            f"calibrated: true once measured."
        ),
    })
    return record


def _as_yaml_block(record: dict) -> str:
    """Render one prior as a YAML list item, appended textually to preserve the file's comments."""
    order = ["category", "method", "agent_name", "agent_version", "tool", "tool_version",
             "value", "calibrated", "auto_generated", "family", "rationale"]
    lines = []
    for key in order:
        if key not in record:
            continue
        value = record[key]
        if key == "rationale":
            lines.append("  rationale: >-")
            lines.extend(f"    {chunk}" for chunk in _wrap(str(value), 92))
        elif isinstance(value, bool):
            lines.append(f"  {key}: {'true' if value else 'false'}")
        elif isinstance(value, float):
            lines.append(f"  {key}: {value}")
        elif key.endswith("_version") or key == "tool_version":
            lines.append(f"  {key}: '{value}'")
        else:
            lines.append(f"  {key}: {value}")
    body = "\n".join(lines)
    return "\n" + "- " + body[2:] + "\n"


def _wrap(text: str, width: int) -> list[str]:
    words, line, out = text.split(), "", []
    for word in words:
        if line and len(line) + 1 + len(word) > width:
            out.append(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    if line:
        out.append(line)
    return out


def append_prior(record: dict, path: str = PRIORS_PATH) -> None:
    """Append a minted prior to the config and invalidate the cached index.

    Textual append rather than a YAML round-trip, so the file's explanatory comments survive.
    This file is a build output in the same sense as ``mappings/*.sssom.tsv``: written by the
    pipeline, git-tracked, and hand-editable afterwards.
    """
    with open(path, "a") as fh:
        fh.write(_as_yaml_block(record))
    _load_index.cache_clear()


def prior_scope(
    agent: dict | None, tool: str | None, tool_version: str | None
) -> tuple[str, str, str]:
    """The scope a step's prior is looked up under: ``(kind, identity, version)``.

    A step run by a **versioned model** is scoped to that model, because a model upgrade
    silently changes output (FAILURE_MODES 13.1) and a prior calibrated on one model says
    nothing about the next. Everything else is scoped to its tool and version — including
    DeepL, which publishes no engine version, so the babelon release that called it is what is
    actually knowable.
    """
    if isinstance(agent, dict) and agent.get("agent_version"):
        name = agent.get("agent_name") or agent["agent_version"]
        return "agents", name, agent["agent_version"]
    return "tools", tool or "", str(tool_version or "")


def resolve_confidence(
    category: str,
    method: str,
    measured: float | None,
    *,
    deterministic: bool = False,
    agent: dict | None = None,
    tool: str | None = None,
    tool_version: str | None = None,
    priors: list[dict] | None = None,
    defaults: list[dict] | None = None,
    path: str = PRIORS_PATH,
) -> tuple[float, str]:
    """Return ``(confidence, basis)`` for one transformation step.

    A measured score wins. Otherwise a deterministic step is 1.0 — it cannot be wrong.
    Otherwise a prior is looked up under the producing model or tool (see :func:`prior_scope`).

    On a miss the prior is **minted from its family default and written to the config**, so a
    model or tool version bump neither inherits a stale value nor breaks the build; the new
    record lands in the diff for a human to calibrate. A producer matching no family still
    raises — inventing a number for an unknown tool would be worse than failing.

    Passing ``priors`` explicitly (tests) disables the write.
    """
    if measured is not None:
        return float(measured), "MEASURED"
    if deterministic:
        return 1.0, "DETERMINISTIC"
    in_memory = priors is not None
    index = index_priors(priors) if in_memory else _load_index(path)
    scope = prior_scope(agent, tool, tool_version)
    key = (*scope, category, method)
    if key in index:
        return index[key], "PRIOR"
    record = mint_prior(
        scope, category, method,
        match_default(scope, defaults if defaults is not None else load_defaults(path)),
    )
    if not in_memory:
        append_prior(record, path)
        logger.info(
            "Minted %s confidence prior %.2f for %s %s (%s/%s) — review it in %s",
            record["family"], record["value"], scope[1], scope[2], category, method, path,
        )
    return float(record["value"]), "PRIOR"


def weakest_basis(bases: Iterable[str]) -> str:
    """The weakest basis in a chain — PRIOR if any step assumed, else MEASURED, else DETERMINISTIC.

    An all-deterministic chain is the strongest thing we can say, so it is the empty default.
    """
    seen = set(bases)
    for basis in _BASIS_ORDER:
        if basis in seen:
            return basis
    return "DETERMINISTIC"


def chain_confidence(steps: list[dict]) -> tuple[float, str]:
    """Aggregate a resolution pipeline: the product of its step confidences.

    Every step must declare a confidence (I-11); a missing one raises rather than being treated
    as 1.0, which is the silent-inflation bug this replaces.
    """
    product, bases = 1.0, []
    for step in steps:
        if step.get("confidence") is None:
            raise ValueError(
                f"step missing confidence: {step.get('category', '<no category>')}"
            )
        product *= float(step["confidence"])
        bases.append(step.get("confidence_basis") or "PRIOR")
    return product, weakest_basis(bases)


def noisy_or(values: Iterable[float]) -> float:
    """Aggregate independent corroborating sources: ``1 - prod(1 - v)``.

    Two independent regulators each at 0.7 give 0.91. This is the opposite direction from
    :func:`chain_confidence` — more steps is more chance of error, more sources is more
    corroboration.

    **Independence is a precondition, not a decoration.** Feeding this correlated values
    silently manufactures certainty; use :func:`corroboration` on anything grouped by source.
    """
    complement = 1.0
    seen = False
    for value in values:
        complement *= 1.0 - float(value)
        seen = True
    return 1.0 - complement if seen else 0.0


def corroboration(by_source: Mapping[str, Iterable[float]]) -> float:
    """Aggregate a pair's assertions, counting each source once (I-13).

    Noisy-OR's premise is that its inputs fail independently. Two documents from *one*
    regulator do not: a generic label and its twenty relabellings carry the same sentence,
    read by the same extractor and grounded against the same index, so their errors are
    perfectly correlated. Treating them as separate terms is how
    ``hydrochlorothiazide -> hypertension`` reached exactly 1.0 — the maximum value in the
    product — off 24 copies of one DailyMed label, and how three EPARs of one molecule put
    ``raloxifene -> spinal fractures`` at 0.999.

    So each source contributes **once**, at its best-resolved attestation, and the noisy-OR
    runs across sources. Two regulators at 0.7 still give 0.91; one regulator saying it
    twenty-four times still gives 0.7. Corroboration means a second opinion, not a louder
    first one.

    Taking the best rather than the mean within a source is deliberate: these are
    data-quality numbers (did we link the right entity?), and one clean attestation from a
    regulator is evidence the entity resolves, however many messier restatements sit
    beside it.
    """
    best = []
    for values in by_source.values():
        floats = [float(v) for v in values]
        if floats:
            best.append(max(floats))
    return noisy_or(best)
