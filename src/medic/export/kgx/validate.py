"""The KGX conformance gate.

Checks a built graph against the *installed* Biolink model rather than against a
hand-maintained belief about what Biolink contains. Run by ``just validate-kgx`` and by
``tests/test_kgx_export.py``.

Severity is deliberate: anything that would make a consumer reject or mis-model the graph is
an **error**; anything that is merely unusual — an identifier prefix Biolink's list does not
mention, which MeDIC legitimately produces for ``DRON:`` and ``MedDRA:`` — is a **warning**
with a count, so real problems are not buried under noise.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from medic.export.kgx import biolink as bl

ERROR = "error"
WARNING = "warning"


@dataclass(frozen=True)
class Problem:
    severity: str
    message: str


@dataclass
class Report:
    problems: list[Problem] = field(default_factory=list)

    @property
    def errors(self) -> list[Problem]:
        return [p for p in self.problems if p.severity == ERROR]

    @property
    def warnings(self) -> list[Problem]:
        return [p for p in self.problems if p.severity == WARNING]

    @property
    def ok(self) -> bool:
        return not self.errors

    def error(self, message: str) -> None:
        self.problems.append(Problem(ERROR, message))

    def warn(self, message: str) -> None:
        self.problems.append(Problem(WARNING, message))


def _known_slot(name: str) -> bool:
    return name.replace("_", " ") in bl.biolink_slots()


def _check_properties(record: dict, kind: str, report: Report) -> None:
    """Every un-namespaced property must be a Biolink slot; extensions must be namespaced."""
    for key in record:
        if key.startswith(bl.EXTENSION_PREFIX):
            bare = key[len(bl.EXTENSION_PREFIX):]
            if _known_slot(bare):
                report.error(
                    f"{kind} property '{key}' shadows Biolink slot '{bare}' — rename it"
                )
            continue
        if not _known_slot(key):
            report.error(
                f"{kind} property '{key}' is not a Biolink slot and is not namespaced "
                f"'{bl.EXTENSION_PREFIX}'"
            )


def _check_node(node: dict, report: Report, prefix_warnings: Counter) -> None:
    node_id = node.get("id", "")
    if not node_id:
        report.error("node without an id")
    _check_properties(node, "node", report)

    categories = node.get("category") or []
    if isinstance(categories, str):
        categories = [categories]
    if not categories:
        report.error(f"node {node_id} has no category")
    classes = bl.biolink_classes()
    for category in categories:
        if bl.model_name(category) not in classes:
            report.error(f"node {node_id} has unknown category '{category}'")
            continue
        allowed = bl.id_prefixes(category)
        prefix = node_id.split(":", 1)[0]
        if allowed and prefix not in allowed:
            prefix_warnings[(prefix, category)] += 1


def _check_edge(edge: dict, node_ids: set[str], report: Report) -> None:
    edge_id = edge.get("id", "")
    _check_properties(edge, "edge", report)

    predicate = edge.get("predicate", "")
    if not bl.is_predicate(predicate):
        report.error(f"edge {edge_id} has unknown predicate '{predicate}'")

    for end in ("subject", "object"):
        ref = edge.get(end)
        if not ref:
            report.error(f"edge {edge_id} has no {end}")
        elif ref not in node_ids:
            report.error(f"edge {edge_id} {end} '{ref}' has no node (dangling endpoint)")

    for slot in bl.SINGLE_VALUED_EDGE_SLOTS:
        if isinstance(edge.get(slot), list):
            report.error(
                f"edge {edge_id} slot '{slot}' is a list; Biolink defines it as single-valued"
            )

    level = edge.get("knowledge_level")
    if level and level not in bl.enum_values("KnowledgeLevelEnum"):
        report.error(f"edge {edge_id} has unknown knowledge_level '{level}'")
    agent = edge.get("agent_type")
    if agent and agent not in bl.enum_values("AgentTypeEnum"):
        report.error(f"edge {edge_id} has unknown agent_type '{agent}'")

    # Invariant I-1, echoed at the export boundary: a source may only speak for its own
    # jurisdiction. Cross-jurisdiction merging happens on the pair, never on one assertion.
    #
    # An unrecognised source is an ERROR, not a skip. The old default turned "I do not know
    # what this source is" into "there is nothing to check" — which is the failure mode where a
    # gate reports success precisely when it has lost track of the data. It hid every India
    # edge for the whole life of the export, because the map was keyed by authority (CDSCO)
    # and the exporter writes the ingester name (INDIA). The map is closed: a source appearing
    # here without an entry is exactly what a reader needs to be told about.
    source = (edge.get("medic_source") or "").upper()
    jurisdiction = (edge.get("medic_jurisdiction") or "").upper()
    if source and source not in bl.SOURCE_JURISDICTION:
        report.error(
            f"edge {edge_id} has source '{source}', which is not in SOURCE_JURISDICTION — "
            f"source isolation cannot be checked for it. Add it to the map."
        )
    else:
        expected = bl.SOURCE_JURISDICTION.get(source)
        if expected is not None and jurisdiction and jurisdiction != expected:
            report.error(
                f"edge {edge_id} violates source isolation: source '{source}' speaks for "
                f"{expected} but the edge claims jurisdiction '{jurisdiction}'"
            )


def check(nodes: list[dict], edges: list[dict]) -> Report:
    """Validate a built graph. Returns a report; ``report.ok`` is False on any error."""
    report = Report()
    prefix_warnings: Counter = Counter()

    node_ids: set[str] = set()
    for node in nodes:
        _check_node(node, report, prefix_warnings)
        node_ids.add(node.get("id", ""))

    for edge in edges:
        _check_edge(edge, node_ids, report)

    for (prefix, category), count in sorted(prefix_warnings.items()):
        report.warn(
            f"{count} node(s) use prefix '{prefix}' which Biolink does not list for "
            f"{category}"
        )

    # Not an error — the graph is still valid — but an edge whose primary source is the
    # aggregator means MeDIC could not attribute the claim, and that gap should be counted
    # rather than disappear into a plausible-looking infores id.
    unattributed = sum(
        1 for e in edges if e.get("primary_knowledge_source") == bl.AGGREGATOR
    )
    if unattributed:
        report.warn(
            f"{unattributed} edge(s) fall back to the aggregator "
            f"'{bl.AGGREGATOR}' as their primary knowledge source (unmapped source)"
        )
    return report


def _read_jsonl(path: Path) -> list[dict]:
    with open(path) as handle:
        return [json.loads(line) for line in handle if line.strip()]


def check_files(nodes_path: Path, edges_path: Path) -> Report:
    return check(_read_jsonl(nodes_path), _read_jsonl(edges_path))
