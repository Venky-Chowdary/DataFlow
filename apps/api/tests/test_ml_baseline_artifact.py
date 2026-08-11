"""Optional automap boost: JSON artifact, deterministic scoring, fail-soft."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.ml_baseline import CharNgramBaseline, load_baseline
from services.semantic_mapper import ml_baseline_path, ml_baseline_status

VOCAB = ["payment_amount", "customer_id", "currency_code", "order_date"]


def test_shipped_artifact_is_json_and_loads():
    path = ml_baseline_path()
    assert path.suffix == ".json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["targets"] == sorted(payload["targets"])
    assert load_baseline(path) is not None


def test_status_reports_available_artifact():
    status = ml_baseline_status()
    assert status["available"] is True
    assert status["role"] == "optional_boost"


@pytest.mark.parametrize(
    "source,expected",
    [
        ("paymentAmount", "payment_amount"),
        ("cust_id", "customer_id"),
        ("Currency Code", "currency_code"),
        ("order_dt", "order_date"),
    ],
)
def test_predicts_nearest_target(source: str, expected: str):
    target, score = CharNgramBaseline(VOCAB).predict_target(source)
    assert target == expected
    assert 0.0 < score <= 1.0


def test_identical_name_scores_one():
    _, score = CharNgramBaseline(VOCAB).predict_target("payment_amount")
    assert score == pytest.approx(1.0)


def test_scoring_is_deterministic_across_instances():
    a = CharNgramBaseline(VOCAB).predict_target("cust_identifier")
    b = CharNgramBaseline(VOCAB).predict_target("cust_identifier")
    assert a == b


def test_empty_vocabulary_and_empty_source_are_safe():
    assert CharNgramBaseline([]).predict_target("anything") == ("", 0.0)
    assert CharNgramBaseline(VOCAB).predict_target("") == ("", 0.0)


def test_missing_artifact_returns_none(tmp_path: Path):
    assert load_baseline(tmp_path / "absent.json") is None


@pytest.mark.parametrize(
    "payload",
    [
        {"schema_version": 99, "targets": ["a"]},
        {"schema_version": 1, "targets": "not-a-list"},
        {"schema_version": 1, "targets": [1, 2]},
    ],
)
def test_malformed_artifact_fails_closed(tmp_path: Path, payload: dict):
    art = tmp_path / "baseline.json"
    art.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError):
        load_baseline(art)
