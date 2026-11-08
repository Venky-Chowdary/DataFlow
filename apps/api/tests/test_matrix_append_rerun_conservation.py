"""Append reruns into an object or document destination.

f9e088bca counts keyed inserts+updates as rows_written. That ledger is the
keyed census. Mongo without ``_id``, S3, and SFTP stay on append-delta, so a
rerun whose artifact count does not grow is still a shortfall. An append that
keeps the prior rows closes when the measured count grows by the batch.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.row_conservation import DEST_READBACK, account_population


def _ledger(*, before: int, after: int, read: int, mode: str = "incremental_append"):
    return account_population(
        rows_read=read,
        dest_count=after,
        dest_count_source=DEST_READBACK,
        dest_count_before=before,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=read,
        sync_mode=mode,
    )


def test_zero_growth_append_rerun_stays_a_shortfall():
    ledger = _ledger(before=100, after=100, read=15)
    assert ledger.balanced is False
    assert ledger.conservation_kind == "append_delta"
    assert "15" in ledger.note
    assert "growth (0" in ledger.note


def test_append_that_keeps_prior_rows_closes():
    ledger = _ledger(before=100, after=115, read=15)
    assert ledger.balanced is True
    assert "100" in ledger.note and "115" in ledger.note


def test_full_refresh_append_rerun_with_no_growth_stays_open():
    ledger = _ledger(before=40, after=40, read=40, mode="full_refresh_append")
    assert ledger.balanced is False
    assert "growth (0" in ledger.note


@pytest.fixture
def data_cell_world(monkeypatch):
    from tests.scale import matrix

    class Destination:
        name = "sqlite"

        def __init__(self):
            self.writes = 0
            self.mode = ""
            self.failure = ""
            self.landed = None
            self.claimed = 0
            self.quarantined = 0
            self.fail_readback = False
            self.bad_first_checksum = False
            self.keyless = None
            self.count_calls = []
            self.checksum_calls = []

        def create_table(self, table, columns, **options):
            self.keyless = options.get("keyless", False)

        def drop(self, table):
            pass

        def endpoint(self, table):
            return table

        def count(self, table):
            self.count_calls.append(self.writes)
            if self.fail_readback and self.writes:
                raise OSError("readback unavailable")
            if self.landed is not None:
                return self.landed
            return 5 * self.writes if self.mode == "full_refresh_append" else 5

        def checksum(self, table, columns):
            self.checksum_calls.append(self.writes)
            total = 42 * self.writes if self.mode == "full_refresh_append" else 42
            if self.bad_first_checksum and self.writes == 1:
                total += 1
            return SimpleNamespace(total=total, hex=f"{total:016x}")

    destination = Destination()
    source = SimpleNamespace(name="sqlite", endpoint=lambda table: table)
    cache = SimpleNamespace(table=lambda *args: "source", source_types=lambda *args: {})
    monkeypatch.setattr(matrix, "projection", lambda *args: (["id", "note_null"], {}))
    monkeypatch.setattr(matrix, "domain_contract_columns", lambda *args: {})
    monkeypatch.setattr(matrix, "expected_checksum", lambda *args: SimpleNamespace(total=42))

    def execute(*args, mode, **kwargs):
        destination.writes += 1
        destination.mode = mode
        return SimpleNamespace(
            success=not destination.failure, error=destination.failure,
            records_transferred=destination.claimed, elapsed_seconds=1,
            destination_summary={"quarantined_rows": destination.quarantined},
            reconciliation={}, row_accounting={}, validation_plan={"passed": True},
        ), f"run-{destination.writes}", 1

    monkeypatch.setattr(matrix, "execute", execute)

    def run(mode, shape):
        return matrix.run_data_cell(source, destination, mode, shape, rows=5, cache=cache, prefix="test")

    return destination, run


@pytest.mark.parametrize("shape", ["create_new", "dest_exists_compatible", "dest_exists_missing_column"])
@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append", "incremental_append", "upsert"])
def test_each_data_shape_proves_second_run_semantics(data_cell_world, mode, shape):
    destination, run = data_cell_world
    cell = run(mode, shape)

    assert cell.status == "pass", cell.reason
    assert cell.run_ids == ["run-1", "run-2"]
    assert cell.expected_rows == (10 if mode == "full_refresh_append" else 5)
    assert destination.count_calls == [1, 2]
    assert destination.checksum_calls == [1, 2]
    assert [proof["outcome"] for proof in cell.run_proofs] == ["passed", "passed"]
    assert [proof["destination_rows"] for proof in cell.run_proofs] == (
        [5, 10] if mode == "full_refresh_append" else [5, 5]
    )
    assert all(proof["checksum_match"] is True for proof in cell.run_proofs)
    if mode == "full_refresh_append":
        assert destination.keyless is True


def test_second_overwrite_cannot_hide_first_run_corruption(data_cell_world):
    destination, run = data_cell_world
    destination.bad_first_checksum = True

    cell = run("full_refresh_overwrite", "create_new")

    assert cell.status == "fail"
    assert "run 1" in cell.reason
    assert "checksum" in cell.reason
    assert cell.run_ids == ["run-1"]
    assert cell.run_proofs[0]["outcome"] == "failed"
    assert cell.run_proofs[0]["checksum_match"] is False


def test_clean_population_cannot_pass_with_quarantine_only_accounting(data_cell_world):
    destination, run = data_cell_world
    destination.quarantined = 1

    cell = run("full_refresh_overwrite", "create_new")

    assert cell.status == "fail"
    assert "quarantined=1" in cell.reason
    assert cell.run_ids == ["run-1"]


@pytest.mark.parametrize("failure, landed, claimed, broken_probe, status", [
    ("G13 unmapped source column note_null", 0, 0, False, "pass (refused)"),
    ("password authentication failed", 0, 0, False, "fail"),
    ("G13 unmapped source column note_null", 1, 0, False, "fail"),
    ("G13 unmapped source column note_null", 0, 1, False, "fail"),
    ("G13 unmapped source column note_null", 0, 0, True, "fail"),
])
def test_blocking_cell_requires_specific_refusal_and_real_zero_writes(
    data_cell_world, failure, landed, claimed, broken_probe, status,
):
    from tests.scale import matrix

    destination, _ = data_cell_world
    destination.failure = failure
    destination.landed = 0
    destination.claimed = claimed
    source = SimpleNamespace(name="sqlite", endpoint=lambda table: table)
    cache = SimpleNamespace(table=lambda *args: "source")
    original_count = destination.count

    def count(table):
        if destination.writes:
            destination.landed = landed
            destination.fail_readback = broken_probe
        return original_count(table)

    destination.count = count
    cell = matrix.run_blocking_cell(
        source, destination, "g13_extra_source_column", rows=5, cache=cache, prefix="test",
    )

    assert cell.status == status, cell.reason
    if status == "pass (refused)":
        assert cell.dest_rows == 0
        assert cell.rows_per_sec == 0


def test_relational_summary_keeps_refusals_out_of_transferred_passes():
    from tests.scale.matrix import Cell, summarize

    cells = [
        Cell("sqlite", "sqlite", "upsert", "create_new", status="pass"),
        Cell("sqlite", "sqlite", "upsert", "missing", status="pass (refused)"),
        Cell("sqlite", "sqlite", "upsert", "missing", status="fail"),
    ]
    report = summarize(cells, {}, {}, 5)

    assert report["pass"] == report["refused"] == report["fail"] == 1


@pytest.mark.parametrize("allow_absent, missing", [(True, True), (False, True), (True, False)])
def test_refusal_probe_distinguishes_absent_table_from_outage(tmp_path, allow_absent, missing):
    import sqlite3
    from tests.scale.matrix import _count_refusal_population

    def count(table):
        if not missing:
            raise OSError("readback unavailable")
        with sqlite3.connect(tmp_path / "empty.db") as connection:
            return connection.execute('SELECT COUNT(*) FROM "missing"').fetchone()[0]

    destination = SimpleNamespace(count=count)
    if allow_absent and missing:
        assert _count_refusal_population(destination, "missing", allow_absent=True) == 0
    else:
        with pytest.raises((sqlite3.OperationalError, OSError)):
            _count_refusal_population(destination, "missing", allow_absent=allow_absent)


@pytest.mark.parametrize("failure, landed, claimed, status", [
    ("schema_mismatch: missing target column note_null", 0, 0, "pass (refused)"),
    ("password authentication failed", 0, 0, "fail"),
    ("schema_mismatch: missing target column note_null", 1, 0, "fail"),
    ("schema_mismatch: missing target column note_null", 0, 1, "fail"),
])
def test_missing_column_refusal_requires_specific_reason_and_zero_write_proof(
    data_cell_world, failure, landed, claimed, status,
):
    destination, run = data_cell_world
    destination.failure = failure
    destination.landed = landed
    destination.claimed = claimed

    cell = run("full_refresh_append", "dest_exists_missing_column")

    assert cell.status == status, cell.reason
    assert destination.count_calls == [1]
    assert cell.dest_rows == landed
    assert cell.run_ids == ["run-1"]
    if status == "pass (refused)":
        assert cell.expected_rows == 0
        assert cell.checksum_match is None
        assert cell.run_proofs[0]["outcome"] == "refused"
        assert cell.run_proofs[0]["destination_rows"] == 0
        assert cell.run_proofs[0]["proof_kind"] == "independent_zero_write"


@pytest.mark.parametrize("shape", ["create_new", "dest_exists_compatible", "dest_exists_missing_column"])
@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append", "incremental_append", "upsert"])
def test_sqlite_whole_transfer_modes_with_preflight_and_independent_proof(tmp_path, monkeypatch, mode, shape):
    import src.transfer.engine as engine_module
    import services.mongodb_service as stores
    import services.sync_cursor as cursors
    from tests.scale import matrix, sql_engines

    store = stores.MemoryMongoDBService()
    monkeypatch.setattr(stores, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(stores, "_mongodb_service", store)
    monkeypatch.setattr(engine_module, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(cursors, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(cursors, "_mongo_cursors", lambda: None)
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(sql_engines, "getenv_brand_str", lambda *args: str(tmp_path / "source"))
    source = sql_engines.SQLiteEngine()
    monkeypatch.setattr(sql_engines, "getenv_brand_str", lambda *args: str(tmp_path / "destination"))
    destination = sql_engines.SQLiteEngine()
    cache = matrix.SourceCache(rows=25, prefix="whole_flow")
    observed = []
    original_execute = matrix.execute

    def capture(*args, **kwargs):
        response = original_execute(*args, **kwargs)
        observed.append({
            "success": response[0].success, "claimed": response[0].records_transferred,
            "error": response[0].error,
        })
        return response

    monkeypatch.setattr(matrix, "execute", capture)
    try:
        cell = matrix.run_data_cell(
            source, destination, mode, shape, rows=25, cache=cache, prefix="whole_flow",
        )

        if shape == "dest_exists_missing_column":
            assert cell.status == "pass (refused)", (cell.reason, observed)
            assert len(cell.run_ids) == 1
            assert cell.dest_rows == cell.expected_rows == 0
            assert "schema" in cell.reason.lower()
            assert "note_null" in cell.reason.lower()
            assert cell.run_proofs[0]["claimed_rows"] == 0
            return
        assert cell.status == "pass", cell.reason
        assert len(cell.run_ids) == 2
        assert cell.dest_rows == (50 if mode == "full_refresh_append" else 25)
        assert cell.checksum_match is True
        assert len(cell.run_proofs) == 2
        assert all(proof["preflight_present"] for proof in cell.run_proofs)
        assert cell.rejected == cell.quarantined == cell.coerced_null == cell.skipped_rows == 0
        assert not any("preflight evidence missing" in note for note in cell.notes)
    finally:
        cache.drop_all({"sqlite": source})
