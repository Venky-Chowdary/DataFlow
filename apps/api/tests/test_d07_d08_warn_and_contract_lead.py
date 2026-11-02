"""D-07/D-08: untyped TEXT accept must warn; the deduped contract fields must be
named upfront when incremental_deduped lacks them.

D-07: CSV COPY fast path typed every undeclared column TEXT and said nothing —
the run read as a clean accept while arbitrary values landed in a text carrier.
D-08: a create with sync_mode=incremental_deduped and no stream contract failed
with "Missing primary key" without naming the stream_contracts fields the
operator must declare (primary_key, cursor_field, cursor_semantics).
"""

import types

from services.copy_csv_local import _format_csv_copy, _pairs_and_ddls
from services.preflight_cursor_gate import build_sync_contract_gate


def _fast_path_result():
    return types.SimpleNamespace(
        source_rows=3,
        target_checksum="abc",
        source_checksum="abc",
        proof_scope="full",
        source_snapshot={},
        rows_copied=3,
    )


# --- D-07 ---


def test_pairs_and_ddls_reports_untyped_columns():
    mappings = [
        {"source": "id", "target": "id", "target_type": "integer"},
        {"source": "note", "target": "note"},  # no declared type anywhere
    ]
    pairs, ddls, declared, untyped = _pairs_and_ddls(mappings, {}, "sqlite")
    assert untyped == ["note"]


def test_typed_columns_do_not_warn():
    mappings = [{"source": "id", "target": "id", "target_type": "integer"}]
    _, _, _, untyped = _pairs_and_ddls(mappings, {}, "sqlite")
    assert untyped == []


def test_dest_summary_carries_untyped_text_warning():
    _, _, summary, _ = _format_csv_copy(
        result=_fast_path_result(),
        dest_type="sqlite",
        dest_table="t",
        filename="in.csv",
        sync_mode="full_refresh_append",
        replace_destination=False,
        untyped_columns=["note"],
    )
    warnings = summary.get("warnings") or []
    assert warnings and "TEXT" in warnings[0] and "note" in warnings[0]


def test_dest_summary_silent_when_all_typed():
    _, _, summary, _ = _format_csv_copy(
        result=_fast_path_result(),
        dest_type="sqlite",
        dest_table="t",
        filename="in.csv",
        sync_mode="full_refresh_append",
        replace_destination=False,
        untyped_columns=[],
    )
    assert not summary.get("warnings")


# --- D-08 ---


def _gate(**overrides):
    kwargs = dict(
        sync="incremental_deduped",
        validation="strict",
        dest="postgresql",
        src="postgresql",
        kind="database",
        source_columns=None,
        pass_status="passed",
        block_status="failed",
    )
    kwargs.update(overrides)
    return build_sync_contract_gate([], **kwargs)


def test_deduped_no_contract_names_required_fields():
    gate = _gate()
    issues = (gate.get("details") or {}).get("issues") or gate.get("issues") or []
    text = " ".join(issues)
    assert "stream_contracts" in text
    assert "primary_key" in text
    assert "cursor_field" in text
    assert "cursor_semantics" in text


def test_deduped_contract_without_pk_names_field():
    gate = build_sync_contract_gate(
        [{"name": "orders", "cursor_field": "updated_at",
          "cursor_semantics": "commit_timestamp"}],
        sync="incremental_deduped",
        validation="strict",
        dest="postgresql",
        src="postgresql",
        kind="database",
        source_columns=None,
        pass_status="passed",
        block_status="failed",
    )
    issues = (gate.get("details") or {}).get("issues") or gate.get("issues") or []
    text = " ".join(issues)
    assert "primary_key" in text and "stream_contracts" in text
