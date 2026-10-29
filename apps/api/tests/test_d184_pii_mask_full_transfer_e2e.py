"""D184 - mask_pii / hash_pii on a column that holds '' completes a FULL transfer.

#344 added mask_pii/hash_pii to ``_KEEP_EMPTY_TRANSFORMS``; the owner asked for
proof that the whole transfer works, not only preflight. These tests drive the
route QA uses (``start_transfer`` with ``column_transforms`` -> confirm -> job)
PG -> MySQL and PG -> PG, and the engine on a CSV file -> PG, with columns that
hold SQL NULL, '', whitespace, unicode and real PII, then read the destination.
"""

from __future__ import annotations

import csv
import uuid

import pytest

from tests import value_fidelity_route as r

# id -> (email, phone). '' / whitespace / NULL / unicode in each masked column.
_ROWS = {
    1: ("alice@example.com", "+1 415 555 0100"),
    2: ("", ""),
    3: (None, None),
    4: ("   ", "\t"),
    5: ("Łukasz.山田@例え.jp", "０９０－１２３４－５６７８"),
    6: ("bob@example.org", " "),
}
_NON_EMPTY = {1, 5, 6}


@pytest.fixture
def client(tmp_path, monkeypatch):
    with r.make_client(tmp_path, monkeypatch) as c:
        yield c


def _assert_masked(out: dict, transform: str) -> None:
    assert set(out) == set(_ROWS), out
    for i, (email, phone) in _ROWS.items():
        got_email, got_phone = out[i]
        for src, got in ((email, got_email), (phone, got_phone)):
            if src is None:
                assert got is None, (i, got)  # NULL stays NULL
            elif src.strip() == "":
                # Nothing to protect: an empty cell stays empty, never NULL,
                # never a masked token.
                assert got is not None and got.strip() == "", (i, repr(got))
            else:
                assert got not in (None, "", src), (i, got)
                if transform == "hash_pii":
                    assert len(got) >= 16, got
    # Equal plaintext hashes the same; distinct plaintext does not collide.
    if transform == "hash_pii":
        assert out[1][0] != out[6][0]


@pytest.mark.parametrize("transform", ["mask_pii", "hash_pii"])
@pytest.mark.parametrize("dest_kind", ["mysql", "postgresql"])
def test_pg_source_full_transfer_with_pii_transform_on_empty_cells(client, dest_kind, transform):
    pg = r.pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"d184_{dest_kind[:5]}_{transform[:4]}_{token}"
    dest_table = table + "_out"
    cur = pg.cursor()
    try:
        cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, email TEXT, phone VARCHAR(40))")
        for i, (email, phone) in _ROWS.items():
            cur.execute(f"INSERT INTO {table} VALUES (%s, %s, %s)", (i, email, phone))  # nosec B608
        src_id, dst_id = r.connectors(token, dest_kind)
        for _ in range(2):  # run 2 lands in the table run 1 created
            r.run(
                client,
                source_connector_id=src_id, source_table=table,
                dest_connector_id=dst_id, dest_table=dest_table,
                sync_mode="full_refresh_overwrite", primary_key="id",
                column_transforms={"email": transform, "phone": transform},
            )
            rows = r.query(
                dest_kind,
                f"SELECT id, email, phone FROM {r.quote(dest_kind, dest_table)}",  # nosec B608
            )
            _assert_masked({int(a): (b, c) for a, b, c in rows}, transform)
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        r.drop(dest_kind, dest_table)


@pytest.mark.parametrize("transform", ["mask_pii", "hash_pii"])
def test_csv_file_to_pg_full_transfer_with_pii_transform(tmp_path, monkeypatch, transform):
    """CSV -> PG through the engine: unquoted empty is NULL, quoted "" is ''."""
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)

    pg = r.pg_conn()
    dest_table = f"d184_csv_{transform[:4]}_{uuid.uuid4().hex[:8]}"
    path = tmp_path / "people.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        fh.write("id,email,phone\n")
        w = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC, lineterminator="\n")
        for i, (email, phone) in _ROWS.items():
            if email is None:  # unquoted empty field = NULL (C04)
                fh.write(f"{i},,\n")
            else:
                w.writerow([i, email, phone])
    cfg = dict(r.dest_config("postgresql"))
    cfg.pop("type")
    try:
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv", table="people"),
            source_path=str(path),
            destination=EndpointConfig(
                kind="database", format="postgresql", table=dest_table,
                host=cfg["host"], port=cfg["port"], database=cfg["database"],
                username=cfg["username"], password=cfg["password"], schema="public",
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.99},
                {"source": "email", "target": "email", "confidence": 0.99, "transform": transform},
                {"source": "phone", "target": "phone", "confidence": 0.99, "transform": transform},
            ],
        )
        for _ in range(2):  # run 2 lands in the table run 1 created
            job_id = "d184csv" + uuid.uuid4().hex[:10]
            fake.update_job_status(job_id, "pending", transfer_request={})
            result = UniversalTransferEngine().execute_tracked(request, job_id)
            # Pre-fix: Gate-8 re-hashed the writer's already-hashed sample and
            # rolled the load back ("Read-back sample verification failed").
            assert result.success, result.error
            cur = pg.cursor()
            cur.execute(f'SELECT id, email, phone FROM public."{dest_table}"')  # nosec B608
            _assert_masked({int(a): (b, c) for a, b, c in cur.fetchall()}, transform)
    finally:
        pg.cursor().execute(f'DROP TABLE IF EXISTS public."{dest_table}"')  # nosec B608
        pg.close()


@pytest.mark.parametrize("transform", ["mask_pii", "hash_pii"])
def test_qa_case_mysql_to_new_pg_with_pii_transform(client, transform):
    """QA D184: MySQL -> new PG, mask_pii on a column holding '' and NULLs."""
    token = uuid.uuid4().hex[:8]
    table = f"d184_my_{transform[:4]}_{token}"
    dest_table = table + "_out"
    r.execute("mysql", f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, email TEXT, "
                       "phone VARCHAR(40)) CHARACTER SET utf8mb4")
    try:
        for i, (email, phone) in _ROWS.items():
            r.execute("mysql", f"INSERT INTO `{table}` VALUES (%s, %s, %s)", (i, email, phone))  # nosec B608
        src_id, dst_id = r.connectors(token, "postgresql", src_kind="mysql")
        for _ in range(2):
            r.run(
                client,
                source_connector_id=src_id, source_table=table,
                dest_connector_id=dst_id, dest_table=dest_table,
                sync_mode="full_refresh_overwrite", primary_key="id",
                column_transforms={"email": transform, "phone": transform},
            )
            rows = r.query("postgresql", f'SELECT id, email, phone FROM "{dest_table}"')  # nosec B608
            _assert_masked({int(a): (b, c) for a, b, c in rows}, transform)
    finally:
        r.drop("mysql", table)
        r.drop("postgresql", dest_table)
