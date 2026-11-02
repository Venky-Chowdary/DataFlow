"""D38 SCD2 minors — stamp semantics.

- ``change_column``: a declared source change-time column drives each
  version's ``valid_from``/``valid_to`` — not the load clock.
- Gapless chain: the closed version's ``valid_to`` equals the new
  version's ``valid_from`` (per-key close stamps, not one load stamp).
- Zone: stamp columns are declared ``timestamptz`` so zone-capable
  engines keep the offset instead of a naive wall clock.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta


def test_change_column_drives_valid_from():
    from services.scd2_engine import _row_change_stamp, resolve_scd2_cols

    cols = resolve_scd2_cols({"change_column": "updated_at"})
    run_ts = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
    row = {"id": 1, "updated_at": "2026-10-24T09:15:30+00:00"}
    stamp = _row_change_stamp(row, cols, run_ts)
    assert stamp == datetime(2026, 10, 24, 9, 15, 30, tzinfo=timezone.utc)


def test_change_column_missing_or_bad_falls_back_to_run_stamp():
    from services.scd2_engine import _row_change_stamp, resolve_scd2_cols

    cols = resolve_scd2_cols({"change_column": "updated_at"})
    run_ts = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
    assert _row_change_stamp({"updated_at": None}, cols, run_ts) is run_ts
    assert _row_change_stamp({"updated_at": "not-a-date"}, cols, run_ts) is run_ts
    assert _row_change_stamp({}, cols, run_ts) is run_ts


def test_change_column_naive_input_is_utc():
    from services.scd2_engine import _row_change_stamp, resolve_scd2_cols

    cols = resolve_scd2_cols({"change_column": "updated_at"})
    run_ts = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
    stamp = _row_change_stamp(
        {"updated_at": "2026-10-24T09:15:30"}, cols, run_ts
    )
    assert stamp.tzinfo is not None
    assert stamp.utcoffset() == timedelta(0)


def test_change_column_offset_is_normalized_to_utc():
    from services.scd2_engine import _row_change_stamp, resolve_scd2_cols

    cols = resolve_scd2_cols({"change_column": "updated_at"})
    run_ts = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
    stamp = _row_change_stamp(
        {"updated_at": "2026-10-24T14:15:30+05:00"}, cols, run_ts
    )
    assert stamp == datetime(2026, 10, 24, 9, 15, 30, tzinfo=timezone.utc)


def test_no_change_column_uses_run_stamp():
    from services.scd2_engine import _row_change_stamp, resolve_scd2_cols

    cols = resolve_scd2_cols({})
    run_ts = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
    assert _row_change_stamp({"updated_at": "2020-01-01"}, cols, run_ts) is run_ts


def test_expire_rows_closes_each_key_at_its_own_stamp():
    """valid_to of the closing version == valid_from of the new version."""
    from services.scd2_engine import _expire_rows

    class _Res:
        rowcount = 1

    class _Conn:
        def __init__(self):
            self.params = []

        def execute(self, sql, params):
            self.params.append(dict(params))
            return _Res()

    conn = _Conn()
    ts_newer = datetime(2026, 10, 24, 9, 0, tzinfo=timezone.utc)
    ts_older = datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)
    run_ts = datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc)
    cols = {"is_current": "is_current", "valid_to": "valid_to",
            "valid_from": "valid_from", "row_hash": "row_hash"}
    n = _expire_rows(
        conn, "t", ["id"], {"1", "2"}, run_ts, "sqlite",
        cols=cols, key_stamps={"1": ts_newer, "2": ts_older},
    )
    assert n == 2
    stamps = {p["ts"] for p in conn.params}
    assert stamps == {ts_newer, ts_older}
    assert run_ts not in stamps


def test_stamp_columns_are_tz_aware_type():
    from services.scd2_engine import resolve_scd2_cols, _stamp_set
    import inspect
    import services.scd2_engine as eng

    src = inspect.getsource(eng)
    assert '"timestamptz"' in src, "stamp columns must be tz-aware, not naive datetime"


def test_resolve_scd2_cols_accepts_change_column_aliases():
    from services.scd2_engine import resolve_scd2_cols

    assert resolve_scd2_cols({"change_column": "updated_at"})["change_column"] == "updated_at"
    assert resolve_scd2_cols({"change_time_column": "cdc_ts"})["change_column"] == "cdc_ts"
    assert resolve_scd2_cols({})["change_column"] is None
