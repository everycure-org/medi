"""Which build outputs may be published, and the notice that must travel with them.

`just gh-release` used to glob `exports/*.{csv,xlsx,jsonl,tsv}`, which made publication the
default: a new export became a release asset with nobody deciding it could. This module
replaces the glob with an explicit manifest (`conf/release_assets.yaml`) where an unlisted
file is refused, and derives the attribution notice from the sources the *shipping* assets
actually draw on — so the notice cannot drift out of sync with what is published.

Terms per source live in `LICENSING.md`; this file is the machine-readable projection of
the decisions recorded there.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import typer
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = REPO_ROOT / "conf" / "release_assets.yaml"

#: Directories the manifest is responsible for. A file here that no entry names is refused.
MANAGED_DIRS = ("products", "exports")

#: Files in a managed directory that are never release assets.
IGNORED_NAMES = {".gitkeep", "diff_report.md"}

app = typer.Typer(add_completion=False)


@dataclass(frozen=True)
class Source:
    key: str
    name: str
    attribution: str  # required | courtesy | none


@dataclass(frozen=True)
class Asset:
    path: str
    sources: tuple[str, ...]
    ship: bool
    note: str = ""
    #: MeDIC's own output, drawing on no upstream source (the KGX metadata file, the
    #: proposed infores entry). Declared rather than inferred from an empty `sources`,
    #: so "no upstream terms" is never confused with "nobody filled this in".
    medic_authored: bool = False


@dataclass(frozen=True)
class License:
    """MeDIC's licence position, stated once in the manifest and flowed everywhere."""

    #: What MeDIC grants over a released file as a whole.
    medic_contribution: str
    #: MeDIC's own mapping assertions, offered without conditions.
    medic_assertions_offered_as: str
    #: The passthrough: processing does not launder upstream terms.
    passthrough: str
    terms_url: str


@dataclass(frozen=True)
class Manifest:
    notice_template: str
    sources: dict[str, Source]
    assets: tuple[Asset, ...]
    license: License | None = None

    @property
    def notice(self) -> str:
        """The notice as it would read if every attribution-required source shipped."""
        return self._render(
            [s.name for s in self.sources.values() if s.attribution == "required"]
        )

    def _render(self, names: list[str]) -> str:
        if not names:
            return ""
        if len(names) == 1:
            joined = names[0]
        else:
            joined = ", ".join(names[:-1]) + " and " + names[-1]
        # One flowing paragraph: the notice is embedded in release notes and file headers,
        # and the template's hard wraps read as ragged text once {sources} expands.
        return " ".join(self.notice_template.format(sources=joined).split())


def load(path: Path | str = DEFAULT_MANIFEST) -> Manifest:
    raw = yaml.safe_load(Path(path).read_text())

    sources = {
        key: Source(key=key, name=value["name"], attribution=value["attribution"])
        for key, value in (raw.get("sources") or {}).items()
    }

    assets = []
    for entry in raw.get("assets") or []:
        keys = tuple(entry.get("sources") or ())
        unknown = [k for k in keys if k not in sources]
        if unknown:
            raise ValueError(
                f"{entry['path']} names source(s) not declared in the manifest: "
                f"{', '.join(unknown)}"
            )
        assets.append(Asset(
            path=entry["path"],
            sources=keys,
            ship=bool(entry.get("ship", False)),
            note=(entry.get("note") or "").strip(),
            medic_authored=bool(entry.get("medic_authored", False)),
        ))

    lic = raw.get("license") or {}
    return Manifest(
        notice_template=raw.get("notice", ""),
        sources=sources,
        assets=tuple(assets),
        license=License(
            medic_contribution=lic.get("medic_contribution", ""),
            medic_assertions_offered_as=lic.get("medic_assertions_offered_as", ""),
            passthrough=" ".join((lic.get("passthrough") or "").split()),
            terms_url=lic.get("terms_url", ""),
        ) if lic else None,
    )


#: How far apart the shipping assets' mtimes may be before the release is treated as mixed.
#: A full `just build-all` writes its outputs over a long stretch, so some spread is normal;
#: five days is not. Tunable per invocation — the point is that *some* bound exists.
DEFAULT_MAX_AGE_SPREAD_HOURS = 24


@dataclass
class Plan:
    manifest: Manifest
    ship: list[Asset] = field(default_factory=list)
    held: list[Asset] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    empty: list[Asset] = field(default_factory=list)
    unlisted: list[str] = field(default_factory=list)
    #: (path, hours behind the newest shipping asset) for assets left over from an older build.
    stale: list[tuple[str, float]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """False when something needs a human decision before a release can go out."""
        return not self.unlisted and not self.stale

    def notice(self, with_passthrough: bool = True) -> str:
        """Attribution notice covering exactly the assets that will ship.

        ``with_passthrough`` appends the licence-passthrough sentence, which is what a
        format with a single notice field (the KGX metadata, the SSSOM ``#comment:``)
        needs. The NOTICE document sets it False — it states the passthrough under its own
        Licence heading, and repeating it just pads the file.
        """
        keys: list[str] = []
        for asset in self.ship:
            for key in asset.sources:
                source = self.manifest.sources.get(key)
                if source and source.attribution == "required" and key not in keys:
                    keys.append(key)
        # Manifest order, not first-seen order, so the notice is stable across builds.
        ordered = [k for k in self.manifest.sources if k in keys]
        text = self.manifest._render([self.manifest.sources[k].name for k in ordered])
        # Naming the sources is not enough: a consumer must also be told the upstream terms
        # carry over. The two always travel together.
        passthrough = (self.manifest.license.passthrough
                       if (self.manifest.license and with_passthrough) else "")
        if text and passthrough:
            return f"{text} {passthrough}"
        return text

    def notice_document(self) -> str:
        """The general NOTICE shipped with the whole release.

        The per-format notices (KGX metadata, SSSOM header) reach only their own file. This
        one covers every asset, which is what a release actually needs — most of what MeDIC
        ships is CSV, XLSX and JSONL, none of which can carry a header.
        """
        lic = self.manifest.license
        lines = [
            "# NOTICE",
            "",
            "This notice applies to **every asset in this MeDIC release**.",
            "",
            "## Attribution",
            "",
            self.notice(with_passthrough=False)
            or "(no source in this release requires attribution)",
            "",
            "## Licence",
            "",
        ]
        if lic:
            lines += [
                f"- **This release as a whole:** {lic.medic_contribution}",
                f"- **MeDIC's own mapping assertions:** offered as "
                f"{lic.medic_assertions_offered_as}",
                f"- **Full terms, per source:** {lic.terms_url}",
                "",
                lic.passthrough,
                "",
            ]
        lines += [
            "## What is in this release, and whose data it contains",
            "",
            "| Asset | Sources | Attribution |",
            "|---|---|---|",
        ]
        for asset in self.ship:
            names = [self.manifest.sources[k] for k in asset.sources
                     if k in self.manifest.sources]
            if not names:
                sources = "MeDIC" if asset.medic_authored else "—"
                required = "—"
            else:
                sources = ", ".join(s.name for s in names)
                required = ("required" if any(s.attribution == "required" for s in names)
                            else "courtesy")
            lines.append(f"| `{asset.path}` | {sources} | {required} |")
        held = [a for a in self.held]
        if held:
            lines += [
                "",
                "## Deliberately not released",
                "",
            ]
            for asset in held:
                lines.append(f"- `{asset.path}` — {asset.note}")
        return "\n".join(lines).rstrip() + "\n"

    def problem_report(self) -> str:
        lines = []
        if self.unlisted:
            lines.append(
                "Refusing to release: these files are in products/ or exports/ but no "
                "manifest entry says whether they may ship. Add them to "
                "conf/release_assets.yaml with their contributing sources."
            )
            lines += [f"  - {p}" for p in self.unlisted]
        if self.stale:
            if lines:
                lines.append("")
            lines.append(
                "Refusing to release: these assets are older than the rest of the build, so "
                "the release would mix artefacts from different runs — CSV views describing "
                "one build shipped alongside YAML products from another. `just gh-release` "
                "packages what is on disk and does not rebuild; run `just build-all` first, "
                "or pass --max-age-spread-hours if the spread is genuinely expected."
            )
            lines += [f"  - {p}  ({hours:.0f}h behind the newest asset)"
                      for p, hours in self.stale]
        return "\n".join(lines)


def _is_empty_product(rel: str, path: Path) -> bool:
    """A product stub such as ``associations: []`` carries no records and is not an asset.

    Only applies under ``products/``. A dict-shaped YAML elsewhere — the KGX metadata file
    — has no ``- `` lines and is not a stub; treating "no list items" as "empty" silently
    dropped it from the release.
    """
    if not rel.startswith("products/"):
        return False
    if path.suffix != ".yaml" or not path.is_file():
        return False
    try:
        with open(path) as handle:
            for line in handle:
                if line.lstrip().startswith("- "):
                    return False
    except OSError:
        return False
    return True


def plan(
    manifest: Manifest,
    root: Path | str = REPO_ROOT,
    *,
    max_age_spread_hours: float = DEFAULT_MAX_AGE_SPREAD_HOURS,
) -> Plan:
    """Resolve the manifest against a working tree.

    Answers two questions, and they are different: *may* this file ship (the manifest), and
    did it come from *this* build (the mtime spread). The manifest exists so a new export
    forces a licensing decision rather than shipping by default; the same argument applies to
    an export left over from a run five days ago, which is what `just gh-release` would
    otherwise publish alongside freshly-built products.

    mtime is a proxy for a build id, not a build id: `touch` defeats it, and it does not
    survive a `git checkout`. Both are acceptable here because products/ and exports/ are
    gitignored build outputs that are only ever written by a build.
    """
    root = Path(root)
    result = Plan(manifest=manifest)
    listed = {a.path for a in manifest.assets}

    for asset in manifest.assets:
        full = root / asset.path
        if not full.exists():
            result.missing.append(asset.path)
            continue
        if not asset.ship:
            result.held.append(asset)
            continue
        if _is_empty_product(asset.path, full):
            result.empty.append(asset)
            continue
        if full.stat().st_size == 0:
            result.empty.append(asset)
            continue
        result.ship.append(asset)

    for directory in MANAGED_DIRS:
        base = root / directory
        if not base.is_dir():
            continue
        for child in sorted(base.iterdir()):
            rel = f"{directory}/{child.name}"
            if child.is_dir() or child.name in IGNORED_NAMES or rel in listed:
                continue
            result.unlisted.append(rel)

    result.stale = _stale_assets(result.ship, root, max_age_spread_hours)
    return result


def _stale_assets(
    ship: list[Asset], root: Path, max_age_spread_hours: float
) -> list[tuple[str, float]]:
    """Shipping assets left behind by an earlier build, newest asset taken as "this build"."""
    if max_age_spread_hours <= 0 or len(ship) < 2:
        return []
    mtimes: dict[str, float] = {}
    for asset in ship:
        try:
            mtimes[asset.path] = (root / asset.path).stat().st_mtime
        except OSError:  # resolved as shipping a moment ago; treat a race as not-stale
            continue
    if not mtimes:
        return []
    newest = max(mtimes.values())
    cutoff = max_age_spread_hours * 3600
    return sorted(
        ((path, (newest - mtime) / 3600) for path, mtime in mtimes.items()
         if newest - mtime > cutoff),
        key=lambda row: -row[1],
    )


# ---------------------------------------------------------------------------
# CLI — consumed by `just gh-release`
# ---------------------------------------------------------------------------
@app.command("list")
def list_command(
    root: Path = typer.Option(REPO_ROOT),
    manifest: Path = typer.Option(DEFAULT_MANIFEST),
    max_age_spread_hours: float = typer.Option(DEFAULT_MAX_AGE_SPREAD_HOURS),
) -> None:
    """Print the shippable asset paths, one per line.

    Exits non-zero on an unlisted file, or on an asset left over from an earlier build.
    """
    resolved = plan(load(manifest), root, max_age_spread_hours=max_age_spread_hours)
    if not resolved.ok:
        typer.echo(resolved.problem_report(), err=True)
        raise typer.Exit(code=1)
    for asset in resolved.ship:
        typer.echo(asset.path)


@app.command("notice")
def notice_command(
    root: Path = typer.Option(REPO_ROOT),
    manifest: Path = typer.Option(DEFAULT_MANIFEST),
) -> None:
    """Print the attribution notice covering exactly the assets that will ship."""
    typer.echo(plan(load(manifest), root).notice())


@app.command("notice-file")
def notice_file_command(
    out: Path = typer.Option(Path("exports/NOTICE.md")),
    root: Path = typer.Option(REPO_ROOT),
    manifest: Path = typer.Option(DEFAULT_MANIFEST),
) -> None:
    """Write the general NOTICE covering every asset in the release.

    Written before the asset list is resolved, so the notice is always in the release it
    describes and always reflects what that release actually contains.
    """
    resolved = plan(load(manifest), root)
    target = root / out if not out.is_absolute() else out
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(resolved.notice_document())
    typer.echo(f"wrote {target}")


@app.command("check")
def check_command(
    root: Path = typer.Option(REPO_ROOT),
    manifest: Path = typer.Option(DEFAULT_MANIFEST),
    max_age_spread_hours: float = typer.Option(DEFAULT_MAX_AGE_SPREAD_HOURS),
) -> None:
    """Report the full release plan; non-zero if anything needs a decision."""
    resolved = plan(load(manifest), root, max_age_spread_hours=max_age_spread_hours)
    stale = dict(resolved.stale)
    for asset in resolved.ship:
        if asset.path in stale:
            typer.echo(f"  STALE   {asset.path}  ({stale[asset.path]:.0f}h behind the build)")
        else:
            typer.echo(f"  SHIP    {asset.path}")
    for asset in resolved.held:
        typer.echo(f"  HELD    {asset.path}  — {asset.note.splitlines()[0] if asset.note else ''}")
    for asset in resolved.empty:
        typer.echo(f"  EMPTY   {asset.path}  (no records; not shipped)")
    for path in resolved.missing:
        typer.echo(f"  MISSING {path}  (not built)")
    if not resolved.ok:
        typer.echo("", err=True)
        typer.echo(resolved.problem_report(), err=True)
        raise typer.Exit(code=1)
    typer.echo(f"\n{len(resolved.ship)} asset(s) cleared for release.")


if __name__ == "__main__":
    app()
