"""Tests for the release asset manifest.

The manifest exists because `just gh-release` used to glob `exports/*.{csv,xlsx,jsonl,tsv}`.
A glob makes shipping the default: the KGX export added 57 MB of new release assets —
including 4.6 MB of verbatim EMA/PMDA/DailyMed label text — and nothing asked whether they
could lawfully ship. Under the manifest an unlisted file is refused, so adding an export
forces a decision instead of inheriting one.
"""

from __future__ import annotations

import textwrap

import pytest
import yaml

from medic import release_assets as ra


def _manifest(tmp_path, body: str):
    path = tmp_path / "release_assets.yaml"
    path.write_text(textwrap.dedent(body))
    return path


BASIC = """
    notice: "Contains data from {sources}."
    sources:
      ema:
        name: European Medicines Agency
        attribution: required
      orangebook:
        name: FDA Orange Book
        attribution: courtesy
    assets:
      - path: products/drug_list.yaml
        sources: [ema, orangebook]
        ship: true
      - path: products/adverse_event_list.yaml
        sources: []
        ship: false
        note: MedDRA via FAERS/PVLens, held pending MSSO terms
"""


def _touch(root, rel, text="- a\n"):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def test_shippable_paths_exclude_held_assets(tmp_path):
    manifest = _manifest(tmp_path, BASIC)
    root = tmp_path / "repo"
    _touch(root, "products/drug_list.yaml")
    _touch(root, "products/adverse_event_list.yaml", "associations: []\n")

    plan = ra.plan(ra.load(manifest), root)
    assert [a.path for a in plan.ship] == ["products/drug_list.yaml"]
    assert [a.path for a in plan.held] == ["products/adverse_event_list.yaml"]


def test_unlisted_file_is_refused(tmp_path):
    """The whole point: a new export cannot ship by accident."""
    manifest = _manifest(tmp_path, BASIC)
    root = tmp_path / "repo"
    _touch(root, "products/drug_list.yaml")
    _touch(root, "exports/medic_edges.jsonl", '{"a":1}\n')

    plan = ra.plan(ra.load(manifest), root)
    assert plan.unlisted == ["exports/medic_edges.jsonl"]
    assert not plan.ok
    assert "medic_edges.jsonl" in plan.problem_report()


def test_listed_but_missing_asset_is_reported_not_shipped(tmp_path):
    manifest = _manifest(tmp_path, BASIC)
    root = tmp_path / "repo"
    root.mkdir()

    plan = ra.plan(ra.load(manifest), root)
    assert "products/drug_list.yaml" in plan.missing
    assert plan.ship == []


def test_empty_product_stub_is_not_shipped_even_if_listed(tmp_path):
    """Retains the recipe's old guard: `associations: []` must never become an asset."""
    manifest = _manifest(tmp_path, """
        notice: "x {sources}"
        sources: {}
        assets:
          - path: products/adverse_event_list.yaml
            sources: []
            ship: true
    """)
    root = tmp_path / "repo"
    _touch(root, "products/adverse_event_list.yaml", "associations: []\n")

    plan = ra.plan(ra.load(manifest), root)
    assert plan.ship == []
    assert [a.path for a in plan.empty] == ["products/adverse_event_list.yaml"]


def test_empty_stub_check_applies_only_to_products(tmp_path):
    """A dict-shaped YAML under exports/ is not a stub just because it has no `- ` lines."""
    manifest = _manifest(tmp_path, """
        notice: "x {sources}"
        sources: {}
        assets:
          - path: exports/medic_kgx_metadata.yaml
            sources: []
            medic_authored: true
            ship: true
    """)
    root = tmp_path / "repo"
    _touch(root, "exports/medic_kgx_metadata.yaml", "name: MeDIC\nnodes:\n  total: 5\n")

    plan = ra.plan(ra.load(manifest), root)
    assert [a.path for a in plan.ship] == ["exports/medic_kgx_metadata.yaml"]
    assert plan.empty == []


def test_notice_is_a_single_flowing_paragraph(tmp_path):
    """It gets embedded in release notes and file headers; hard-wrapped source lists read
    as ragged text once {sources} expands."""
    manifest = _manifest(tmp_path, """
        notice: |
          Contains data from {sources}. Data has been
          edited by the MeDIC pipeline.
        sources:
          ema: {name: EMA, attribution: required}
        assets:
          - path: products/drug_list.yaml
            sources: [ema]
            ship: true
    """)
    root = tmp_path / "repo"
    _touch(root, "products/drug_list.yaml")

    notice = ra.plan(ra.load(manifest), root).notice()
    assert "\n" not in notice
    assert notice == "Contains data from EMA. Data has been edited by the MeDIC pipeline."


def test_notice_names_only_attribution_required_sources_that_actually_ship(tmp_path):
    manifest = _manifest(tmp_path, BASIC)
    root = tmp_path / "repo"
    _touch(root, "products/drug_list.yaml")

    notice = ra.plan(ra.load(manifest), root).notice()
    assert "European Medicines Agency" in notice
    assert "FDA Orange Book" not in notice   # courtesy only, not a legal obligation


def test_notice_is_empty_when_nothing_requiring_attribution_ships(tmp_path):
    manifest = _manifest(tmp_path, BASIC)
    root = tmp_path / "repo"
    _touch(root, "products/adverse_event_list.yaml", "associations: []\n")

    assert ra.plan(ra.load(manifest), root).notice() == ""


def test_unknown_source_key_is_an_error(tmp_path):
    manifest = _manifest(tmp_path, """
        notice: "x {sources}"
        sources:
          ema: {name: EMA, attribution: required}
        assets:
          - path: products/drug_list.yaml
            sources: [ema, nosuchsource]
            ship: true
    """)
    with pytest.raises(ValueError, match="nosuchsource"):
        ra.load(manifest)


# ---------------------------------------------------------------------------
# The real manifest in conf/ — guards it against drift
# ---------------------------------------------------------------------------
def test_repo_manifest_loads_and_every_asset_declares_sources():
    manifest = ra.load(ra.DEFAULT_MANIFEST)
    assert manifest.assets
    for asset in manifest.assets:
        assert asset.path.startswith(("products/", "exports/"))
        if asset.ship:
            assert asset.sources or asset.medic_authored, (
                f"{asset.path} ships without declaring its sources; if it is MeDIC's own "
                "output say so with medic_authored: true"
            )


def test_medic_authored_assets_are_declared_not_inferred(tmp_path):
    """A file with no upstream sources must say it is MeDIC's own, not just omit them."""
    manifest = _manifest(tmp_path, """
        notice: "x {sources}"
        sources: {}
        assets:
          - path: exports/infores_medic.yaml
            sources: []
            medic_authored: true
            ship: true
    """)
    asset = ra.load(manifest).assets[0]
    assert asset.medic_authored is True


def test_repo_manifest_holds_the_adverse_event_product():
    """MedDRA reaches MeDIC through FAERS/PVLens; that product stays held."""
    manifest = ra.load(ra.DEFAULT_MANIFEST)
    ae = next(a for a in manifest.assets if a.path.endswith("adverse_event_list.yaml"))
    assert ae.ship is False
    assert ae.note


def test_repo_manifest_notice_matches_the_licensing_document():
    """The notice shipped with a release must be the one LICENSING.md commits to."""
    licensing = (ra.REPO_ROOT / "LICENSING.md").read_text()
    manifest = ra.load(ra.DEFAULT_MANIFEST)
    for fragment in ("European Medicines Agency", "Pharmaceuticals and Medical Devices",
                     "Data has been edited"):
        assert fragment in manifest.notice, f"{fragment!r} missing from manifest notice"
        assert fragment in licensing, f"{fragment!r} missing from LICENSING.md"


def test_repo_manifest_covers_every_file_currently_in_exports_and_products():
    """Fails the moment a new export appears without a licensing decision."""
    plan = ra.plan(ra.load(ra.DEFAULT_MANIFEST), ra.REPO_ROOT)
    assert plan.unlisted == [], (
        "these files have no manifest entry, so nobody decided whether they may ship: "
        f"{plan.unlisted}"
    )


def test_manifest_yaml_is_valid_and_documented():
    raw = yaml.safe_load(ra.DEFAULT_MANIFEST.read_text())
    assert set(raw) >= {"notice", "sources", "assets"}
    for key, source in raw["sources"].items():
        assert source.get("attribution") in {"required", "courtesy", "none"}, key


# ---------------------------------------------------------------------------
# Licence terms and the general NOTICE that covers every release asset
# ---------------------------------------------------------------------------
def test_manifest_declares_passthrough_terms():
    """The core statement: upstream licences survive MeDIC's processing unchanged."""
    lic = ra.load(ra.DEFAULT_MANIFEST).license
    assert "remains in force" in lic.passthrough
    assert "stricter" in lic.passthrough
    assert lic.terms_url.endswith("LICENSING.md")


def test_notice_includes_the_passthrough_statement(tmp_path):
    """Naming the sources is not enough — a consumer must be told the terms carry over."""
    manifest = _manifest(tmp_path, """
        notice: "Contains data from {sources}."
        license:
          medic_contribution: https://creativecommons.org/licenses/by/4.0/
          medic_assertions_offered_as: https://creativecommons.org/publicdomain/zero/1.0/
          passthrough: "Upstream licences remain in force; the stricter term governs."
          terms_url: https://example.invalid/LICENSING.md
        sources:
          ema: {name: EMA, attribution: required}
        assets:
          - path: products/drug_list.yaml
            sources: [ema]
            ship: true
    """)
    root = tmp_path / "repo"
    _touch(root, "products/drug_list.yaml")

    notice = ra.plan(ra.load(manifest), root).notice()
    assert "Contains data from EMA." in notice
    assert "Upstream licences remain in force" in notice


def test_notice_document_covers_every_shipping_asset(tmp_path):
    """One file, applying to the whole release — not a per-format formalism."""
    manifest = _manifest(tmp_path, """
        notice: "Contains data from {sources}."
        license:
          medic_contribution: https://creativecommons.org/licenses/by/4.0/
          medic_assertions_offered_as: https://creativecommons.org/publicdomain/zero/1.0/
          passthrough: "Upstream licences remain in force; the stricter term governs."
          terms_url: https://example.invalid/LICENSING.md
        sources:
          ema: {name: EMA, attribution: required}
          orangebook: {name: FDA Orange Book, attribution: courtesy}
        assets:
          - path: products/drug_list.yaml
            sources: [ema, orangebook]
            ship: true
          - path: exports/ema.xlsx
            sources: [ema]
            ship: true
          - path: products/adverse_event_list.yaml
            sources: []
            ship: false
            note: held
    """)
    root = tmp_path / "repo"
    _touch(root, "products/drug_list.yaml")
    _touch(root, "exports/ema.xlsx")
    _touch(root, "products/adverse_event_list.yaml", "associations: []\n")

    doc = ra.plan(ra.load(manifest), root).notice_document()
    assert "products/drug_list.yaml" in doc
    assert "exports/ema.xlsx" in doc            # every shipping asset is listed
    assert "Upstream licences remain in force" in doc
    assert "EMA" in doc
    assert "https://example.invalid/LICENSING.md" in doc

    # A held asset is named only under the "not released" heading — recording *why*
    # something was withheld is useful, but it must never read as part of the release.
    table, _, withheld = doc.partition("## Deliberately not released")
    assert "adverse_event_list" not in table
    assert "adverse_event_list" in withheld


def test_notice_document_marks_which_sources_require_attribution(tmp_path):
    manifest = _manifest(tmp_path, """
        notice: "Contains data from {sources}."
        license:
          medic_contribution: https://creativecommons.org/licenses/by/4.0/
          medic_assertions_offered_as: https://creativecommons.org/publicdomain/zero/1.0/
          passthrough: "Upstream licences remain in force."
          terms_url: https://example.invalid/LICENSING.md
        sources:
          ema: {name: EMA, attribution: required}
          orangebook: {name: FDA Orange Book, attribution: courtesy}
        assets:
          - path: products/drug_list.yaml
            sources: [ema, orangebook]
            ship: true
    """)
    root = tmp_path / "repo"
    _touch(root, "products/drug_list.yaml")

    doc = ra.plan(ra.load(manifest), root).notice_document()
    required = doc[doc.index("EMA"):]
    assert "required" in required.lower()


def test_repo_manifest_ships_the_notice_file():
    """NOTICE.md must itself be a declared release asset, or it never reaches anyone."""
    manifest = ra.load(ra.DEFAULT_MANIFEST)
    notice = next((a for a in manifest.assets if a.path.endswith("NOTICE.md")), None)
    assert notice is not None, "conf/release_assets.yaml does not ship a NOTICE"
    assert notice.ship is True
    assert notice.medic_authored is True


def test_notice_document_states_the_passthrough_once(tmp_path):
    """It has its own Licence section; repeating it in Attribution just pads the file."""
    manifest = _manifest(tmp_path, """
        notice: "Contains data from {sources}."
        license:
          medic_contribution: https://creativecommons.org/licenses/by/4.0/
          medic_assertions_offered_as: https://creativecommons.org/publicdomain/zero/1.0/
          passthrough: "Upstream licences remain in force; the stricter term governs."
          terms_url: https://example.invalid/LICENSING.md
        sources:
          ema: {name: EMA, attribution: required}
        assets:
          - path: products/drug_list.yaml
            sources: [ema]
            ship: true
    """)
    root = tmp_path / "repo"
    _touch(root, "products/drug_list.yaml")

    doc = ra.plan(ra.load(manifest), root).notice_document()
    assert doc.count("Upstream licences remain in force") == 1


def test_inline_notice_still_carries_the_passthrough(tmp_path):
    """Formats with a single notice field (KGX metadata, SSSOM comment) get both halves."""
    manifest = _manifest(tmp_path, """
        notice: "Contains data from {sources}."
        license:
          medic_contribution: https://creativecommons.org/licenses/by/4.0/
          medic_assertions_offered_as: https://creativecommons.org/publicdomain/zero/1.0/
          passthrough: "Upstream licences remain in force."
          terms_url: https://example.invalid/LICENSING.md
        sources:
          ema: {name: EMA, attribution: required}
        assets:
          - path: products/drug_list.yaml
            sources: [ema]
            ship: true
    """)
    root = tmp_path / "repo"
    _touch(root, "products/drug_list.yaml")
    plan = ra.plan(ra.load(manifest), root)
    assert "Upstream licences remain in force" in plan.notice()
    assert "Upstream licences remain in force" not in plan.notice(with_passthrough=False)


# ---------------------------------------------------------------------------
# Build vintage: did this asset come from *this* build?
# ---------------------------------------------------------------------------
def _vintage_tree(tmp_path, ages_hours: dict[str, float]):
    """A tree whose manifest ships every named asset, each aged by the given hours."""
    import os
    import time

    (tmp_path / "products").mkdir()
    (tmp_path / "exports").mkdir()
    now = time.time()
    entries = []
    for rel, hours in ages_hours.items():
        path = tmp_path / rel
        path.write_text("- a\n")
        os.utime(path, (now - hours * 3600, now - hours * 3600))
        entries.append({"path": rel, "sources": [], "ship": True, "medic_authored": True})
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(yaml.dump({"notice": "", "sources": {}, "assets": entries}))
    return ra.load(manifest_path)


def test_a_uniform_build_is_not_stale(tmp_path):
    manifest = _vintage_tree(tmp_path, {
        "products/indication_list.yaml": 0.5,
        "exports/medic_edges.jsonl": 0.2,
    })
    resolved = ra.plan(manifest, tmp_path)
    assert resolved.stale == []
    assert resolved.ok


def test_an_export_from_an_older_build_is_refused(tmp_path):
    """The tree that motivated this: exports five days older than the products they describe."""
    manifest = _vintage_tree(tmp_path, {
        "products/indication_list.yaml": 0.1,
        "exports/medic_statements.tsv": 125,
    })
    resolved = ra.plan(manifest, tmp_path)
    assert [p for p, _ in resolved.stale] == ["exports/medic_statements.tsv"]
    assert not resolved.ok
    assert "mix artefacts from different runs" in resolved.problem_report()


def test_a_stale_product_is_caught_too(tmp_path):
    """Not just exports — a disease_list from last week is the same defect."""
    manifest = _vintage_tree(tmp_path, {
        "products/indication_list.yaml": 0.1,
        "products/disease_list.yaml": 126,
    })
    resolved = ra.plan(manifest, tmp_path)
    assert [p for p, _ in resolved.stale] == ["products/disease_list.yaml"]


def test_normal_build_spread_is_tolerated(tmp_path):
    """A full build writes its outputs over hours; that is not a mixed release."""
    manifest = _vintage_tree(tmp_path, {
        "products/indication_list.yaml": 0.1,
        "exports/medic_edges.jsonl": 6,
    })
    assert ra.plan(manifest, tmp_path).stale == []


def test_the_spread_bound_is_tunable(tmp_path):
    manifest = _vintage_tree(tmp_path, {
        "products/indication_list.yaml": 0.1,
        "exports/medic_edges.jsonl": 30,
    })
    assert ra.plan(manifest, tmp_path).stale  # default 24h
    assert ra.plan(manifest, tmp_path, max_age_spread_hours=48).stale == []


def test_staleness_can_be_switched_off(tmp_path):
    manifest = _vintage_tree(tmp_path, {
        "products/indication_list.yaml": 0.1,
        "exports/medic_edges.jsonl": 900,
    })
    assert ra.plan(manifest, tmp_path, max_age_spread_hours=0).stale == []
