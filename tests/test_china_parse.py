"""Tests for the China (CDE / NMPA) ingester.

Pure-function unit tests for date normalization, CSV parsing / dedup, and the
translation stage wiring. No network calls: the translation stage is exercised
either with a pre-seeded Babelon store (so DeepL is never constructed) or with
``MEDIC_SKIP_EXPENSIVE_CALLS`` set.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from medic.ingest.china.locate_source import locate_cde_csv
from medic.ingest.china.parse_cde import normalize_china_date, parse_cde_csv
from medic.translation import TranslationService, translate_records


# ---------------------------------------------------------------------------
# Date normalization
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2019/10/21", "20191021"),   # dominant year-first slash form
        ("2019/9/10", "20190910"),    # single-digit month/day
        ("20080618", "20080618"),     # already compact
        ("2021-01-07", "20210107"),   # dash
        ("2018.07.06", "20180706"),   # dot
        ("2020年8月5日", "20200805"),   # CJK ideographs
        ("2021年05月11日", "20210511"),
        ("2002-10-.24", "20021024"),  # stray punctuation
        ("‘2021-01-07", "20210107"),  # stray leading quote
        ("'2021-01-07", "20210107"),
        ("", ""),
        ("garbage", ""),
        ("2021/13/01", ""),           # implausible month
    ],
)
def test_normalize_china_date(raw, expected):
    assert normalize_china_date(raw) == expected


# ---------------------------------------------------------------------------
# CSV parsing / dedup / source isolation (pre-translation)
# ---------------------------------------------------------------------------
def _write_csv(path: Path, rows: list[tuple[str, str]]) -> None:
    lines = ["drug_name,approval_date"]
    lines += [f"{n},{d}" for n, d in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_parse_cde_csv_dedups_and_isolates(tmp_path):
    csv_path = tmp_path / "cde.csv"
    _write_csv(
        csv_path,
        [
            ("来那度胺胶囊", "2019/10/21"),
            ("来那度胺胶囊", "2013/1/22"),   # earlier date for same name
            ("盐酸二甲双胍片", "2018/7/12"),
        ],
    )

    records = parse_cde_csv(csv_path)

    assert len(records) == 2  # deduped by Chinese name
    by_zh = {r["original_name_zh"]: r for r in records}

    # Source isolation (I-1): every record is CHINA, nothing else.
    assert all(r["source"] == "CHINA" for r in records)

    lena = by_zh["来那度胺胶囊"]
    # Pre-translation: source_name is the verbatim Chinese (I-7 faithful).
    assert lena["source_name"] == "来那度胺胶囊"
    assert lena["approval_date"] == "20130122"          # earliest date kept


def test_parse_cde_csv_limit(tmp_path):
    csv_path = tmp_path / "cde.csv"
    _write_csv(csv_path, [("布洛芬颗粒", "2019/10/18"), ("盐酸二甲双胍片", "2018/7/12")])
    records = parse_cde_csv(csv_path, limit=1)
    assert len(records) == 1


def test_parse_cde_csv_missing_column(tmp_path):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("name,date\nfoo,2019/1/1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="drug_name"):
        parse_cde_csv(csv_path)


# ---------------------------------------------------------------------------
# Translation stage wiring (offline — seeded Babelon store, no DeepL)
# ---------------------------------------------------------------------------
def _seed(service: TranslationService, mapping: dict[str, str]) -> None:
    """Pre-fill a Babelon store so ``translate_records`` never calls DeepL."""
    for zh, en in mapping.items():
        mid = service.mention_id(zh)
        service.store.upsert_source(mid, zh, "zh", service.translation_language)
        service.store.set_translation(mid, en, translator="wikidata:Q116709136")


def test_translate_records_sets_english_and_trail(tmp_path):
    store_path = str(tmp_path / "drug_translation.babelon.tsv")
    svc = TranslationService(store_path, "zh")
    _seed(svc, {"来那度胺胶囊": "lenalidomide", "盐酸二甲双胍片": "metformin"})

    records = [
        {"source": "CHINA", "source_name": "来那度胺胶囊", "original_name_zh": "来那度胺胶囊"},
        {"source": "CHINA", "source_name": "盐酸二甲双胍片", "original_name_zh": "盐酸二甲双胍片"},
    ]
    translate_records(records, "zh", translation_service=svc)

    lena = records[0]
    # source_name replaced with English; grounder sees English.
    assert lena["source_name"] == "lenalidomide"
    # Full trail anchored on a MEDICNE id.
    assert lena["mention_id"].startswith("MEDICNE:")
    trail = lena["translation"]
    assert trail["subject_id"] == lena["mention_id"]
    assert trail["source_value"] == "来那度胺胶囊"       # verbatim Chinese kept (I-7)
    assert trail["source_language"] == "zh"
    assert trail["translation_value"] == "lenalidomide"
    assert trail["translation_language"] == "en-us"
    assert trail["translator_expertise"] == "ALGORITHM"

    assert records[1]["source_name"] == "metformin"


def test_translate_records_offline_leaves_untranslated(tmp_path, monkeypatch):
    """With MEDIC_SKIP_EXPENSIVE_CALLS the Chinese name is left as-is, no DeepL."""
    monkeypatch.setenv("MEDIC_SKIP_EXPENSIVE_CALLS", "1")
    store_path = str(tmp_path / "drug_translation.babelon.tsv")
    records = [{"source": "CHINA", "source_name": "布洛芬颗粒", "original_name_zh": "布洛芬颗粒"}]

    translate_records(records, "zh", store_path=store_path)

    r = records[0]
    assert r["source_name"] == "布洛芬颗粒"                     # unchanged (won't ground)
    assert r["mention_id"].startswith("MEDICNE:")
    assert r["translation"]["translation_status"] == "NOT_TRANSLATED"


# ---------------------------------------------------------------------------
# Fail-loud when the manual source is missing
# ---------------------------------------------------------------------------
def test_locate_cde_csv_missing_raises(tmp_path):
    missing = tmp_path / "nope.csv"
    with pytest.raises(FileNotFoundError, match="manual-acquisition"):
        locate_cde_csv(missing)


def test_locate_cde_csv_present_returns(tmp_path):
    present = tmp_path / "cde.csv"
    present.write_text("drug_name,approval_date\n", encoding="utf-8")
    assert locate_cde_csv(present) == present
