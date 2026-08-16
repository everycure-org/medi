"""Re-mint MEDICNE ids after the identity-normalization fix (release2 item 11).

`mint_mention_id` used to normalize with `base_normalize`, which strips bracketed qualifiers.
That collapsed distinct substances onto one id — human vs porcine insulin, `[131I]` vs `[123I]` —
and because the id is the join key into the Babelon translation store (I-9), one substance's
translation attached to another and the grounder was handed the wrong English string.

Only bracket-bearing literals change id: ~987 store rows and ~317 kb records out of ~35,000.
Diseases are untouched — no disease literal carries a bracketed qualifier.

**This is literal-driven, never id-driven.** Mapping old id -> new id would be ambiguous for
exactly the rows that matter: a colliding old id maps to two different new ids, and picking one
would silently mis-assign the other. So every row is re-minted from the literal it actually
carries, and anything whose literal cannot be recovered is reported rather than guessed.

The grounding store needs care: `subject_id` is pinned to the ORIGINAL literal while
`subject_label` may be the English translation, so a translated row cannot be re-minted from its
own label. Those are resolved by joining the Babelon store on `translation_value`.

Idempotent: re-running changes nothing once the ids are current.
"""

from __future__ import annotations

import argparse
import csv
import glob
import sys
import uuid
from pathlib import Path

import yaml

from medic.grounding.lexical.preprocess import base_normalize
from medic.mention import MEDICNE_NAMESPACE, mint_mention_id


def legacy_mention_id(surface_form: str, entity_type: str) -> str:
    """The id this string would have had under the pre-fix, bracket-stripping mint.

    Kept here rather than in `medic.mention`: production code must have exactly one way to mint
    an id, and a `legacy=True` flag on the real function is how both schemes stay alive forever.
    """
    key = f"{entity_type}\t{base_normalize(surface_form)}"
    return f"MEDICNE:{uuid.uuid5(MEDICNE_NAMESPACE, key)}"


BABELON = "mappings/drug_translation.babelon.tsv"
GROUNDING = {"mappings/drug_grounding.sssom.tsv": "drugs",
             "mappings/disease_grounding.sssom.tsv": "diseases"}


def _read_tsv(path: str) -> tuple[list[str], list[str], list[dict]]:
    text = Path(path).read_text()
    comments = [ln for ln in text.splitlines(keepends=True) if ln.startswith("#")]
    reader = csv.DictReader(
        (ln for ln in text.splitlines(keepends=True) if not ln.startswith("#")), delimiter="\t")
    return comments, list(reader.fieldnames or []), list(reader)


def _write_tsv(path: str, comments: list[str], fields: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="") as fh:
        fh.writelines(comments)
        w = csv.DictWriter(fh, fieldnames=fields, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def remint_babelon(dry_run: bool) -> dict:
    """Babelon rows carry the original literal in `source_value`, so this is unambiguous."""
    if not Path(BABELON).exists():
        return {"changed": 0, "unresolved": 0}
    comments, fields, rows = _read_tsv(BABELON)
    changed = 0
    for row in rows:
        literal = (row.get("source_value") or "").strip()
        if not literal:
            continue
        new = mint_mention_id(literal, "drugs")
        if row.get("subject_id") != new:
            row["subject_id"] = new
            changed += 1
    if not dry_run:
        _write_tsv(BABELON, comments, fields, rows)
    return {"changed": changed, "unresolved": 0}


def _translation_index() -> dict[str, str]:
    """English translation_value -> the NEW id of its original foreign literal."""
    if not Path(BABELON).exists():
        return {}
    _c, _f, rows = _read_tsv(BABELON)
    out: dict[str, str] = {}
    for row in rows:
        value = (row.get("translation_value") or "").strip()
        literal = (row.get("source_value") or "").strip()
        if value and literal:
            out.setdefault(value, mint_mention_id(literal, "drugs"))
    return out


def remint_grounding(path: str, entity_type: str, dry_run: bool) -> dict:
    comments, fields, rows = _read_tsv(path)
    index = _translation_index() if entity_type == "drugs" else {}
    changed = unresolved = 0
    for row in rows:
        label = (row.get("subject_label") or "").strip()
        current = (row.get("subject_id") or "").strip()
        if not label:
            continue
        # A row whose id is the mint of its own label is untranslated: re-mint directly.
        if current == legacy_mention_id(label, entity_type) or current == "":
            new = mint_mention_id(label, entity_type)
        elif label in index:
            # Translated: the label is English, the id belongs to the foreign original.
            new = index[label]
        elif current == mint_mention_id(label, entity_type):
            continue  # already current
        else:
            unresolved += 1
            continue
        if current != new:
            row["subject_id"] = new
            changed += 1
    if not dry_run:
        _write_tsv(path, comments, fields, rows)
    return {"changed": changed, "unresolved": unresolved}


def remint_kb(dry_run: bool) -> dict:
    """kb records carry their own literal, so every one is unambiguous."""
    changed = files = 0
    for path in sorted(glob.glob("kb/drugs/*/*.yaml") + glob.glob("kb/indications/*/*.yaml")):
        if path.endswith(("grounding_report.yaml", "setid_lookup_report.yaml")):
            continue
        try:
            recs = yaml.safe_load(open(path))
        except Exception:
            continue
        if not isinstance(recs, list):
            continue
        entity = "drugs" if "/drugs/" in path else "diseases"
        touched = 0
        for rec in recs:
            literal = (rec.get("original_literal") or rec.get("source_name") or "").strip()
            if not literal or "mention_id" not in rec:
                continue
            new = mint_mention_id(literal, entity)
            if rec["mention_id"] != new:
                rec["mention_id"] = new
                touched += 1
        if touched and not dry_run:
            # Same dump settings the ingesters use (medic.ingest.common), so re-minting
            # does not reflow the file and bury the id changes in a formatting diff.
            content = yaml.dump(recs, default_flow_style=False, allow_unicode=True)
            Path(path).write_text(content)
        if touched:
            files += 1
            changed += touched
    return {"changed": changed, "files": files}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    total_unresolved = 0
    r = remint_babelon(args.dry_run)
    print(f"  {BABELON}: {r['changed']} subject_ids re-minted")
    for path, entity in GROUNDING.items():
        if not Path(path).exists():
            continue
        r = remint_grounding(path, entity, args.dry_run)
        total_unresolved += r["unresolved"]
        print(f"  {path}: {r['changed']} re-minted, {r['unresolved']} unresolved")
    r = remint_kb(args.dry_run)
    print(f"  kb/: {r['changed']} mention_ids re-minted across {r['files']} files")

    if total_unresolved:
        print(f"\n{total_unresolved} grounding rows could not be resolved to a literal — their "
              f"subject_id was left alone rather than guessed.", file=sys.stderr)
    if args.dry_run:
        print("\n(dry run — nothing written)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
