"""D295 ratios, D399 empty JSON, D297 CSV null, D318 embedded CRLF, D374 empty run.

Each case is the fixture named in the 17 Nov 2026 story. A pass is that
fixture only. It is not a live retest of the original job.
"""

from __future__ import annotations

import io

_RATIO = "1.4285714285714286"
_VISA = "4111111111111111"


def test_a_ratio_is_not_a_card_and_a_real_pan_still_is():
    from services.compliance_guard import detect_pii_fields, embedded_pii_categories

    ratios = [{"RATIO": str((i + 1) / 7)} for i in range(8)]
    report = detect_pii_fields(
        ["RATIO"],
        ratios,
        column_types={"RATIO": "BINARY_DOUBLE"},
    )
    assert "pci_pan" not in (report["field_risk"].get("RATIO") or [])
    assert embedded_pii_categories(_RATIO) == []

    cards = detect_pii_fields(["value"], [{"value": _VISA}] * 6)
    assert "pci_pan" in cards["field_risk"].get("value", [])


def test_empty_json_is_not_a_clear_value_and_a_clear_payload_still_is():
    from services.pii_mask_transform import clear_tokens, mask_pii_cell

    for empty in ("{}", "[]", "", {"a": {}}, []):
        assert clear_tokens(empty) == set()
        assert clear_tokens(empty) & clear_tokens(empty) == set()

    source = '{"email":"person@example.com","empty":{}}'
    masked = mask_pii_cell(source, source)
    assert "person@example.com" not in masked
    assert clear_tokens(source) & clear_tokens(masked) == set()
    assert clear_tokens(source) & clear_tokens(source)


def test_unquoted_csv_empty_is_null_and_quoted_empty_stays_empty():
    from services.coercion_probe import analyze_coercion
    from services.csv_codec import iter_csv_records
    from services.transform_engine import apply_transform
    from src.transfer.file_stream import file_empty_cell_bind

    rows = list(iter_csv_records(io.StringIO('id,payload\n30,\n31,""\n')))
    assert rows[1][1] is None
    assert rows[2][1] == ""
    assert file_empty_cell_bind(None, "nvarchar(max)") is None
    assert file_empty_cell_bind("", "nvarchar(max)") == ""
    assert file_empty_cell_bind("", "json") == ""
    assert file_empty_cell_bind("", "integer") is None

    assert apply_transform(None, "json") == (None, None)
    # '' is not JSON text: the transform reports it instead of inventing a value;
    # the shared empty-cell bind decides per carrier (NVARCHAR keeps '', above).
    value, error = apply_transform("", "json")
    assert value is None and "Empty value cannot coerce to json" in str(error)

    report = analyze_coercion(
        sample_rows=[{"payload": None}, {"payload": ""}],
        mappings=[
            {
                "source": "payload",
                "target": "payload",
                "source_type": "TEXT",
                "target_type": "NVARCHAR(MAX)",
                "transform": "json",
                "create_new": True,
            }
        ],
        source_types={"payload": "TEXT"},
        dest_types={"payload": "NVARCHAR(MAX)"},
        dest_db_type="sqlserver",
        table_exists=False,
        validation_mode="balanced",
        empty_cells_as_null=True,
    )
    assert report["has_blocking_failures"] is False
    assert "EMPTY_VALUE_NOT_NULLABLE" not in str(report)


def test_a_quoted_crlf_survives_the_csv_text_open(tmp_path):
    from services.csv_codec import iter_csv_records, open_delimited_text

    path = tmp_path / "note.csv"
    path.write_bytes(b'id,note\r\n1,"multi\r\nline"\r\n')
    with open_delimited_text(path) as handle:
        rows = list(iter_csv_records(handle))
    assert rows[1][1] == "multi\r\nline"


def test_a_run_with_no_source_is_refused_before_a_job():
    from src.routers.transfer_router import incomplete_transfer_run_detail

    neither = incomplete_transfer_run_detail(has_source=False, has_destination=False)
    assert "source" in neither and "destination" in neither
    assert "no job was started" in neither
    dest_only = incomplete_transfer_run_detail(has_source=False, has_destination=True)
    assert "no source" in dest_only
    assert "no job was started" in dest_only
    assert incomplete_transfer_run_detail(has_source=True, has_destination=False) == ""
