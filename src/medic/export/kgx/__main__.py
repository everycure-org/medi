"""CLI entry points: ``just export-kgx`` and ``just validate-kgx``."""

from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path

import typer

from medic.export.kgx import EDGES_FILE, EXPORTS_DIR, NODES_FILE, export_kgx, validate

app = typer.Typer(add_completion=False)


@app.command()
def export(
    products_dir: Path = typer.Option(Path("products"), help="Directory holding products."),
    exports_dir: Path = typer.Option(EXPORTS_DIR, help="Directory to write exports into."),
    check: bool = typer.Option(True, help="Run the Biolink conformance gate afterwards."),
) -> None:
    """Build the KGX graph from the products."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    nodes, edges = export_kgx(products_dir, exports_dir, build_date=date.today().isoformat())
    if check and not _report(validate.check(nodes, edges)):
        raise typer.Exit(code=1)


@app.command("validate")
def validate_command(
    exports_dir: Path = typer.Option(EXPORTS_DIR, help="Directory holding the KGX files."),
) -> None:
    """Validate an already-built KGX graph against the pinned Biolink model."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    report = validate.check_files(exports_dir / NODES_FILE, exports_dir / EDGES_FILE)
    if not _report(report):
        raise typer.Exit(code=1)


def _report(report) -> bool:
    for problem in report.warnings:
        print(f"WARNING: {problem.message}", file=sys.stderr)
    for problem in report.errors[:50]:
        print(f"ERROR: {problem.message}", file=sys.stderr)
    if report.errors:
        print(f"\n{len(report.errors)} error(s); KGX export is not Biolink-conformant.",
              file=sys.stderr)
        return False
    print(f"KGX export is Biolink-conformant ({len(report.warnings)} warning(s)).")
    return True


if __name__ == "__main__":
    app()
