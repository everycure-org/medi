"""Tests for the research batch candidate picker."""


import pytest

from scripts import research_batch_candidates as rbc


def test_is_curated_true_when_yaml_exists(tmp_path, monkeypatch):
    kb = tmp_path / "kb" / "research"
    kb.mkdir(parents=True)
    (kb / "MONDO_0001234.yaml").write_text("associations: []\n")
    monkeypatch.setattr(rbc, "KB_RESEARCH_DIR", kb)

    assert rbc.is_curated("MONDO:0001234") is True


def test_is_curated_false_when_yaml_missing(tmp_path, monkeypatch):
    kb = tmp_path / "kb" / "research"
    kb.mkdir(parents=True)
    monkeypatch.setattr(rbc, "KB_RESEARCH_DIR", kb)

    assert rbc.is_curated("MONDO:0009999") is False


def test_ensure_queue_seeds_from_priority_when_missing(tmp_path, monkeypatch):
    priority = tmp_path / "priority.tsv"
    priority.write_text("mondo id\tmondo label\nMONDO:0001\tdisease A\n")
    queue = tmp_path / "queue.tsv"
    monkeypatch.setattr(rbc, "PRIORITY_PATH", priority)
    monkeypatch.setattr(rbc, "QUEUE_PATH", queue)

    seeded = rbc.ensure_queue_exists()

    assert seeded is True
    assert queue.exists()
    assert queue.read_text() == priority.read_text()


def test_ensure_queue_returns_false_when_already_present(tmp_path, monkeypatch):
    priority = tmp_path / "priority.tsv"
    priority.write_text("mondo id\tmondo label\nMONDO:0001\tdisease A\n")
    queue = tmp_path / "queue.tsv"
    queue.write_text("mondo id\tmondo label\nMONDO:0002\tdisease B\n")  # different content
    monkeypatch.setattr(rbc, "PRIORITY_PATH", priority)
    monkeypatch.setattr(rbc, "QUEUE_PATH", queue)

    seeded = rbc.ensure_queue_exists()

    assert seeded is False
    # Existing queue is preserved, not overwritten
    assert "MONDO:0002" in queue.read_text()


def test_ensure_queue_raises_if_priority_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(rbc, "PRIORITY_PATH", tmp_path / "missing.tsv")
    monkeypatch.setattr(rbc, "QUEUE_PATH", tmp_path / "queue.tsv")

    with pytest.raises(FileNotFoundError):
        rbc.ensure_queue_exists()


def test_read_queue_skips_header_blank_and_comments(tmp_path, monkeypatch):
    queue = tmp_path / "queue.tsv"
    queue.write_text(
        "mondo id\tmondo label\textra\n"
        "MONDO:0001\tdisease A\tcol3\n"
        "\n"
        "# MONDO:0002\tdisease B\tcol3\n"
        "MONDO:0003\tdisease C\tcol3\n"
    )
    monkeypatch.setattr(rbc, "QUEUE_PATH", queue)

    rows = rbc.read_queue()

    assert rows == [("MONDO:0001", "disease A"), ("MONDO:0003", "disease C")]


def test_read_queue_strips_whitespace(tmp_path, monkeypatch):
    queue = tmp_path / "queue.tsv"
    queue.write_text(
        "mondo id\tmondo label\n"
        "  MONDO:0001  \t  disease A  \n"
    )
    monkeypatch.setattr(rbc, "QUEUE_PATH", queue)

    rows = rbc.read_queue()

    assert rows == [("MONDO:0001", "disease A")]


def test_read_queue_skips_rows_with_too_few_columns(tmp_path, monkeypatch):
    queue = tmp_path / "queue.tsv"
    queue.write_text(
        "mondo id\tmondo label\n"
        "MONDO:0001\n"  # missing label
        "MONDO:0002\tdisease B\n"
    )
    monkeypatch.setattr(rbc, "QUEUE_PATH", queue)

    rows = rbc.read_queue()

    assert rows == [("MONDO:0002", "disease B")]


def test_pick_next_n_filters_curated_and_limits(tmp_path, monkeypatch, capsys):
    queue = tmp_path / "queue.tsv"
    queue.write_text(
        "mondo id\tmondo label\n"
        "MONDO:0001\tdisease A\n"
        "MONDO:0002\tdisease B\n"
        "MONDO:0003\tdisease C\n"
        "MONDO:0004\tdisease D\n"
    )
    kb = tmp_path / "kb" / "research"
    kb.mkdir(parents=True)
    # disease B is already curated -> must be skipped
    (kb / "MONDO_0002.yaml").write_text("associations: []\n")

    monkeypatch.setattr(rbc, "QUEUE_PATH", queue)
    monkeypatch.setattr(rbc, "KB_RESEARCH_DIR", kb)
    monkeypatch.setattr(rbc, "PRIORITY_PATH", queue)  # unused but set for safety

    rc = rbc.main(["--count", "2"])
    out = capsys.readouterr().out.strip().splitlines()

    assert rc == 0
    assert out == ["MONDO:0001\tdisease A", "MONDO:0003\tdisease C"]


def test_pick_handles_fewer_than_requested(tmp_path, monkeypatch, capsys):
    queue = tmp_path / "queue.tsv"
    queue.write_text(
        "mondo id\tmondo label\n"
        "MONDO:0001\tdisease A\n"
    )
    kb = tmp_path / "kb" / "research"
    kb.mkdir(parents=True)

    monkeypatch.setattr(rbc, "QUEUE_PATH", queue)
    monkeypatch.setattr(rbc, "KB_RESEARCH_DIR", kb)
    monkeypatch.setattr(rbc, "PRIORITY_PATH", queue)

    rc = rbc.main(["--count", "10"])
    out = capsys.readouterr().out.strip().splitlines()

    assert rc == 0
    assert out == ["MONDO:0001\tdisease A"]
