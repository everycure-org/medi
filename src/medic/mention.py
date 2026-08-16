"""MEDICNE named-entity minting — a stable id for every extracted source mention.

MeDIC assigns each source string a **MeDIC named entity** id (``MEDICNE:<uuid5>``)
at extraction time, in the spirit of a named-entity-linking (NEL) system. That id
is the anchor for the full transformation trail (invariant I-8): translation
(Babelon), grounding (SSSOM) and normalization (SSSOM) all reference the same
``MEDICNE`` subject, so the path from the verbatim string to the final canonical
ontology term is join-able for the user-facing UI.

The id is a deterministic ``uuid5`` of ``(entity_type, base-normalized surface
form)``, so:

* the **same surface form always mints the same id** — reruns are byte-identical
  and offline (no counter, no state, no randomness);
* granularity is **per surface form**, not per occurrence — every record that
  carries the string ``Абакавир`` shares one ``MEDICNE`` id, which is exactly what
  we want for de-duplicating translation/grounding work and for the UI.

The verbatim source string is never mutated (I-7); the mint only *reads* it.
"""

from __future__ import annotations

import uuid

import re
import unicodedata

from medic.grounding.lexical.preprocess import _DASHES, _QUOTES

_WS = re.compile(r"\s+")

# Fixed namespace for MeDIC named entities. Derived deterministically from a
# stdlib namespace constant (NOT random) so the mapping surface-form -> id is
# reproducible from source alone. Do NOT change this value — every already-minted
# MEDICNE id depends on it.
MEDICNE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://w3id.org/monarch-initiative/medic/MEDICNE")


def identity_normalize(s: str) -> str:
    """Non-semantic normalization for **identity**: case, accents, dashes, quotes, whitespace.

    Deliberately NOT :func:`~medic.grounding.lexical.preprocess.base_normalize`, which also
    strips bracketed qualifiers. That is right for *matching* — "aspirin [tablet]" should reach
    "aspirin" — and wrong for *identity*, because a bracket is frequently the only thing
    separating two substances:

        Инсулин растворимый [человеческий генно-инженерный]   human recombinant insulin
        Инсулин растворимый [свиной монокомпонентный]         porcine monocomponent insulin
        Sodium iodide [131I] / [123I]                          different isotopes

    Minting on the matching normalization collapsed those onto one MEDICNE id. Since the id is
    the join key into the Babelon translation store (I-9), the collision attached one
    substance's translation to another and the grounder was handed the wrong English string.
    """
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))  # strip accents
    for a, b in {**_DASHES, **_QUOTES}.items():
        s = s.replace(a, b)
    return _WS.sub(" ", s.casefold()).strip()


def mint_mention_id(surface_form: str, entity_type: str) -> str:
    """Mint the deterministic ``MEDICNE:<uuid5>`` id for a source mention.

    ``entity_type`` is the coarse kind of the mention (``"drugs"`` / ``"diseases"``)
    so an identical string used as a drug and as a disease gets distinct ids.
    The surface form is identity-normalized before hashing, so trivially-different
    spellings (whitespace/case/unicode) collapse to one id — but anything that changes
    *which substance is meant*, notably a bracketed qualifier, does not. See
    :func:`identity_normalize` for why that differs from the grounder's normalization.

    >>> mint_mention_id("Абакавир", "drugs") == mint_mention_id(" абакавир ", "drugs")
    True
    """
    key = f"{entity_type}\t{identity_normalize(surface_form)}"
    return f"MEDICNE:{uuid.uuid5(MEDICNE_NAMESPACE, key)}"


# The two uniform fields every source record carries from extraction onward:
#   ``original_literal`` — the verbatim source string as extracted (I-7 faithful);
#   ``mention_id``       — its stable MEDICNE id (the single identifier of the mention).
ORIGINAL_LITERAL_KEY = "original_literal"
MENTION_ID_KEY = "mention_id"


def assign_mention(
    record: dict,
    entity_type: str = "drugs",
    *,
    literal: str | None = None,
) -> str:
    """Stamp ``record`` with its ``original_literal`` + ``mention_id`` (idempotent-ish).

    This is the **single way** to identify the original source literal, called at
    extraction time by every ingester so the ``MEDICNE`` id travels with the record
    from the very start (I-9). The literal is taken (in order) from the explicit
    ``literal`` argument, an already-set ``original_literal``, or ``source_name``
    (the fallback for English sources whose source string is not later translated).

    Non-English sources pass ``literal`` = the verbatim foreign string **before**
    the translation stage overwrites ``source_name`` with English, so the id stays
    pinned to the original literal, not the translation.
    """
    if literal is not None:
        record[ORIGINAL_LITERAL_KEY] = literal
    lit = (record.get(ORIGINAL_LITERAL_KEY) or record.get("source_name") or "").strip()
    record.setdefault(ORIGINAL_LITERAL_KEY, lit)
    mention_id = mint_mention_id(lit, entity_type)
    record[MENTION_ID_KEY] = mention_id
    return mention_id
