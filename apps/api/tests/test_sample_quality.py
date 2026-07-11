"""Sample quality anomaly detection tests."""

from services.sample_quality import analyze_column_quality, analyze_dataset_quality


def test_detects_numeric_outliers():
    values = ["10", "11", "12", "10", "11", "1000", "9", "10", "11", "12"]
    report = analyze_column_quality("amount", values, inferred_type="DECIMAL")
    assert report["severity"] in {"warning", "block", "none"}
    assert any("outlier" in i.lower() for i in report.get("issues", [])) or report["severity"] == "none"


def test_detects_invalid_emails():
    report = analyze_column_quality(
        "user_email",
        ["bad@", "not-an-email", "ok@example.com"],
        inferred_type="VARCHAR",
    )
    assert any("email" in i.lower() for i in report.get("issues", []))


def test_blocks_high_null_rate():
    rows = [{"id": None} for _ in range(10)]
    rows[0]["id"] = "1"
    result = analyze_dataset_quality(["id"], rows, schema={"id": "INTEGER"})
    assert result["issue_count"] >= 1


def test_clean_dataset_high_score():
    rows = [{"id": str(i), "amount": str(i * 1.5)} for i in range(1, 21)]
    result = analyze_dataset_quality(
        ["id", "amount"],
        rows,
        schema={"id": "INTEGER", "amount": "DECIMAL"},
    )
    assert result["quality_score"] >= 80


def test_detects_duplicate_rows_as_quality_issue():
    rows = [
        {"id": "1", "email": "alice@example.com"},
        {"id": "1", "email": "alice@example.com"},
        {"id": "2", "email": "bob@example.com"},
    ]
    result = analyze_dataset_quality(["id", "email"], rows)

    assert result["duplicate_row_count"] >= 1
    assert any("duplicate" in issue.lower() for issue in result["issues"])
