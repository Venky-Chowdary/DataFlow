"""Encoding / collation evidence surfaces as first-class G3/G9 signals."""

from services.data_integrity import run_integrity_audit
from services.type_system import is_case_insensitive_collation, unique_equality_key


def test_encoding_control_char_blocks_g9_path_strict():
    """Format-control chars produce encoding_anomalies that G9 consumes."""
    report = run_integrity_audit(
        source_columns=["title"],
        mappings=[{"source": "title", "target": "title", "confidence": 0.99}],
        sample_rows=[{"title": "hello\u200bworld"}],
        validation_mode="strict",
    )
    enc = next(c for c in report["checks"] if c["check"] == "encoding_anomalies")
    assert enc["blocks_transfer"] is True
    assert report["blocks_transfer"] is True
    findings = enc.get("encoding_findings") or []
    assert findings
    assert any("U+200B" in (f.get("chars") or []) or "200B" in str(f) for f in findings)
    assert any(
        (f.get("suggested_transform") == "strip_controls") for f in findings
    )


def test_replacement_char_is_encoding_mismatch_evidence():
    report = run_integrity_audit(
        source_columns=["name"],
        mappings=[{"source": "name", "target": "name", "confidence": 0.99}],
        sample_rows=[{"name": "bad\ufffdname"}],
        validation_mode="strict",
    )
    enc = next(c for c in report["checks"] if c["check"] == "encoding_anomalies")
    assert enc["blocks_transfer"] is True
    assert any("FFFD" in str(i) or "replacement" in str(i).lower() for i in enc["issues"])


def test_ci_collation_casefold_equality_matrix():
    """CI / CITEXT uniqueness must casefold — classic migration killer evidence."""
    assert is_case_insensitive_collation("VARCHAR(50) COLLATE utf8mb4_unicode_ci")
    assert is_case_insensitive_collation("CITEXT")
    assert not is_case_insensitive_collation("VARCHAR(50)")
    a = unique_equality_key("Abc", "VARCHAR COLLATE utf8mb4_unicode_ci", force_casefold=True)
    b = unique_equality_key("abc", "VARCHAR COLLATE utf8mb4_unicode_ci", force_casefold=True)
    assert a == b
