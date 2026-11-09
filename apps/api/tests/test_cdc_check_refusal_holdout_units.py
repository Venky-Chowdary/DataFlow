"""QA4 units: CHECK refusals are held out of the CDC census, findings count per row."""

from __future__ import annotations

from services.cdc_engine import ChangeBatch
from services.cdc_value_digest import _without_held_out
from services.row_conservation import KeyCensusAccumulator
from src.transfer import cdc_transfer as ct
from src.transfer.reconcile_step import _held_out_keys

PG_CHECK = 'new row for relation "t" violates check constraint "ck_t"'
MYSQL_CHECK = "(3819, \"Check constraint 'ck_t' is violated.\")"
PG_UNIQUE = 'duplicate key value violates unique constraint "t_pkey"'
ORA_FK = "ORA-02291: integrity constraint (APP.FK_T) violated - parent key not found"


def _detail(key: str, reason: str, row: int = 1) -> dict:
    return {"row": row, "source_pk": key, "source_pk_proven": True, "reason": reason,
            "values": {"id": key, "_df_lsn": f"0/{key}0"}}


def test_check_refusal_holds_the_key_out_unique_does_not():
    summary = {"rejected_details": [
        _detail("3", PG_CHECK), _detail("4", MYSQL_CHECK), _detail("6", PG_UNIQUE),
        _detail("7", ORA_FK),
    ]}
    assert _held_out_keys(summary, "id") == {"3", "4"}


def test_accumulator_withdraws_refused_keys_of_the_open_batch():
    acc = KeyCensusAccumulator()
    acc.begin_live_batch()
    acc.add_batch([("3",), ("4",), ("5",)], dest_hits=0)
    assert acc.withdraw_rejected([("3",), ("4",)], stored=set()) == 2
    census = acc.to_census()
    assert census is not None
    assert (census.unique_batch_keys, census.dest_preexisting, census.held_out_keys) == (1, 0, 2)
    assert census.expected_delta == 1


def test_accumulator_keeps_a_key_an_earlier_batch_wrote():
    acc = KeyCensusAccumulator()
    acc.begin_live_batch()
    acc.add_batch([("3",)], dest_hits=0)
    acc.keep_last_live_batch()
    acc.begin_live_batch()
    acc.add_batch([("3",), ("5",)], dest_hits=1)
    assert acc.withdraw_rejected([("3",)], stored={("3",)}) == 0
    census = acc.to_census()
    assert census is not None
    assert (census.unique_batch_keys, census.held_out_keys) == (2, 0)


def test_accumulator_withdraws_a_stored_key_as_an_update_not_an_insert():
    acc = KeyCensusAccumulator()
    acc.begin_live_batch()
    acc.add_batch([("1",), ("5",)], dest_hits=1)
    acc.withdraw_rejected([("1",)], stored={("1",)})
    census = acc.to_census()
    assert census is not None
    assert (census.unique_batch_keys, census.dest_preexisting, census.expected_delta) == (1, 0, 1)


def test_two_batches_findings_count_as_two_rows(monkeypatch):
    import services.quarantine_dlq as dlq

    persisted: list[list[dict]] = []
    monkeypatch.setattr(dlq, "persist_rejected_rows",
                        lambda **k: persisted.append(k["rejected_details"]))
    monkeypatch.setattr(ct, "_supersede_rejected_images", lambda *a, **k: None)
    state = ct.CdcState()
    for key in ("3", "4"):
        merged = ct._merge_cdc_dest_summary(
            state, {"rejected_rows": 1, "rejected_details": [_detail(key, PG_CHECK)]},
            job_id="j1", change=ChangeBatch(inserts=[{"id": key}]), key_cols=["id"],
        )
    assert [d["row"] for d in merged["rejected_details"]] == [1, 2]
    assert merged["rejected_rows"] == 2
    from src.transfer.job_quarantine import rows_with_findings

    assert rows_with_findings(merged["rejected_details"]) == 2
    assert [len(p) for p in persisted] == [1, 1]


def test_image_repair_skips_rows_with_an_open_finding():
    state = ct.CdcState()
    state.accumulated_rejected_details = [_detail("3", PG_CHECK)]
    rows = [{"id": 3, "name": "toolong"}, {"id": 5, "name": "fine"}]
    assert ct._without_open_quarantine(state, rows, ["id"]) == [rows[1]]


def test_value_proof_drops_held_out_source_rows():
    rows = [{"ID": 3}, {"ID": 4}, {"ID": 5}]
    pairs = [("ID", "id")]
    assert _without_held_out(rows, pairs, {"3", "4"}, "id") == [{"ID": 5}]
    assert _without_held_out(rows, pairs, None, "id") == rows
    assert _without_held_out(rows, pairs, {"3"}, "other") == rows


def test_oracle_foreign_key_refusal_is_not_withdrawn_from_the_census():
    class _Acc:
        def withdraw_rejected(self, keys, stored):
            raise AssertionError(f"withdrew {keys}")

    summary = {"rejected_details": [_detail("7", ORA_FK)]}
    assert ct._census_without_rejected_keys(
        _Acc(), summary, key_cols=["id"], dest_type="oracle", dest_cfg={}, dest_table="t",
    ) is None


def test_a_held_out_key_a_later_batch_writes_is_no_longer_held_out():
    acc = KeyCensusAccumulator()
    acc.begin_live_batch()
    acc.add_batch([("3",), ("4",)], dest_hits=0)
    acc.withdraw_rejected([("3",), ("4",)], stored=set())
    acc.begin_live_batch()
    acc.add_batch([("3",)], dest_hits=0)
    acc.begin_live_batch()
    census = acc.to_census()
    assert census is not None
    assert (census.unique_batch_keys, census.dest_preexisting, census.held_out_keys) == (1, 0, 1)


def test_a_reversed_batch_keeps_its_key_held_out():
    acc = KeyCensusAccumulator()
    acc.begin_live_batch()
    acc.add_batch([("3",), ("5",)], dest_hits=0)
    acc.withdraw_rejected([("3",)], stored=set())
    acc.begin_live_batch()
    acc.add_batch([("3",)], dest_hits=0)
    acc.reverse_last_live_batch()
    census = acc.to_census()
    assert census is not None
    assert (census.unique_batch_keys, census.held_out_keys) == (1, 1)


def test_a_key_whose_other_image_landed_is_not_withdrawn(monkeypatch):
    import services.dest_precount as dp

    monkeypatch.setattr(dp, "destination_key_hits", lambda *a, **k: 1)

    class _Acc:
        def withdraw_rejected(self, keys, stored):
            raise AssertionError(f"withdrew {keys}")

    summary = {"rejected_details": [_detail("3", PG_CHECK)]}
    change = ChangeBatch(inserts=[{"id": "3", "n": -1}], updates=[{"id": "3", "n": 2}])
    assert ct._census_without_rejected_keys(
        _Acc(), summary, key_cols=["id"], dest_type="postgresql", dest_cfg={},
        dest_table="t", change=change,
    ) is None


def test_value_proof_drops_held_out_rows_whatever_the_column_case():
    rows = [{"id": 3}, {"id": 5}]
    assert _without_held_out(rows, [("ID", "id")], {"3"}, "id") == [{"id": 5}]
