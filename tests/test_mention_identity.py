"""MEDICNE minting must not collapse substances that differ inside brackets (I-9).

`base_normalize` strips bracketed qualifiers — correct for *matching*, where "aspirin [tablet]"
should reach "aspirin", but destructive for *identity*, where the bracket is often the only thing
distinguishing two substances. Minting on it made human and porcine insulin share an id, and the
id is the join key into the Babelon translation store, so one substance's translation attached to
another and the grounder was handed the wrong English string.
"""

from medic.mention import identity_normalize, mint_mention_id


def test_bracketed_qualifiers_distinguish_substances():
    human = "Инсулин растворимый [человеческий генно-инженерный]"
    porcine = "Инсулин растворимый [свиной монокомпонентный]"
    assert mint_mention_id(human, "drugs") != mint_mention_id(porcine, "drugs")


def test_isotopes_are_distinguished():
    assert mint_mention_id("Sodium iodide [131I]", "drugs") != \
        mint_mention_id("Sodium iodide [123I]", "drugs")


def test_combination_components_are_distinguished():
    assert mint_mention_id("Калия хлорид+[Натрия хлорид]", "drugs") != \
        mint_mention_id("Калия хлорид+[Декстроза]", "drugs")


def test_trivial_variation_still_collapses():
    """The point of normalizing at all: case, whitespace and unicode are not identity."""
    assert mint_mention_id("Абакавир", "drugs") == mint_mention_id(" абакавир ", "drugs")
    assert mint_mention_id("LEVETIRACETAM", "drugs") == mint_mention_id("Levetiracetam", "drugs")
    assert mint_mention_id("foo  bar", "drugs") == mint_mention_id("foo bar", "drugs")


def test_a_bracket_still_normalizes_case_and_space_inside_itself():
    assert mint_mention_id("Sodium iodide [131I]", "drugs") == \
        mint_mention_id("sodium  iodide  [131i]", "drugs")


def test_entity_type_still_separates_namespaces():
    assert mint_mention_id("aspirin", "drugs") != mint_mention_id("aspirin", "diseases")


def test_identity_normalize_keeps_brackets_where_base_normalize_drops_them():
    from medic.grounding.lexical.preprocess import base_normalize

    s = "Sodium iodide [131I]"
    assert "131i" in identity_normalize(s)
    assert "131i" not in base_normalize(s)


def test_no_mention_id_serves_two_different_bracketed_substances():
    """The regression this exists to prevent, measured on the real corpus.

    Deliberately scoped to bracket content rather than "any textual difference": 58 ids
    legitimately serve more than one spelling — non-breaking vs ordinary space, fullwidth vs
    ASCII parentheses, roman numeral U+2160 vs Latin I — and those *are* the same substance.
    NFKD folding them is the point. What must never collapse is two different bracketed
    qualifiers.
    """
    import collections
    import glob
    import re

    import yaml

    brackets = re.compile(r"\[([^\]]*)\]")
    ids = collections.defaultdict(set)
    for path in glob.glob("kb/drugs/*/*.yaml"):
        if path.endswith("grounding_report.yaml"):
            continue
        try:
            recs = yaml.safe_load(open(path)) or []
        except Exception:
            continue
        if not isinstance(recs, list):
            continue
        for rec in recs:
            lit = (rec.get("original_literal") or rec.get("source_name") or "").strip()
            if lit:
                ids[mint_mention_id(lit, "drugs")].add(lit)

    conflicts = {
        k: v for k, v in ids.items()
        if len({tuple(brackets.findall(x.casefold())) for x in v}) > 1
    }
    assert not conflicts, (
        f"{len(conflicts)} mention ids serve substances with different bracketed qualifiers, "
        f"e.g. {list(conflicts.values())[:2]}")
