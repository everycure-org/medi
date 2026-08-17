"""Tests for the HuggingFace disease-list ingest row builder.

Regression coverage for the upstream shape change where the ``synonyms`` column
of ``everycure/disease-list`` switched from a semicolon-separated string to a
native list/array. ``pd.isna()`` on an array raises
``ValueError: The truth value of an array ... is ambiguous``; the ingest must
handle both the list-like and the semicolon-string shape.
"""

import numpy as np
import pandas as pd

from medic.ingest.disease_list.__main__ import (
    _build_disease_from_hf_row,
    _clean,
    _to_str_list,
)


def test_to_str_list_handles_ndarray():
    val = np.array(["syndactyly type 1d", "SD1d", ""])
    assert _to_str_list(val) == ["syndactyly type 1d", "SD1d"]


def test_to_str_list_handles_semicolon_string():
    val = "UMLS:C5679981; Orphanet:295193; GARD:0021216"
    assert _to_str_list(val) == ["UMLS:C5679981", "Orphanet:295193", "GARD:0021216"]


def test_to_str_list_handles_missing():
    assert _to_str_list(None) == []
    assert _to_str_list(float("nan")) == []
    assert _to_str_list("") == []
    assert _to_str_list(np.array([], dtype=object)) == []


def test_clean_scalar_unchanged():
    assert _clean("MONDO:0017545") == "MONDO:0017545"
    assert _clean(float("nan")) == ""
    assert _clean(None) == ""


def test_build_disease_with_ndarray_synonyms():
    """The exact failure from the build: synonyms arrives as an ndarray."""
    row = pd.Series(
        {
            "id": "MONDO:0017545",
            "name": "Zygodactyly type 4",
            "definition": "",
            "synonyms": np.array(["syndactyly type 1d", "SD1d"]),
            "subsets": "mondo:mondo_txgnn_other; mondo:mondo_top_grouping_disease",
            "crossreferences": "UMLS:C5679981; Orphanet:295193",
        }
    )
    df_columns = list(row.index)

    disease = _build_disease_from_hf_row(row, df_columns)

    assert disease is not None
    assert disease["category_class"] == "MONDO:0017545"
    assert disease["label"] == "Zygodactyly type 4"
    assert disease["synonyms"] == ["syndactyly type 1d", "SD1d"]
    assert disease["subsets"] == [
        "mondo:mondo_txgnn_other",
        "mondo:mondo_top_grouping_disease",
    ]
    assert disease["crossreferences"] == ["UMLS:C5679981", "Orphanet:295193"]
