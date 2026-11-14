"""DATA-1: a source TRUNCATE reaches the CDC destination; extras never pass.

Before the fix ``pgoutput_decoder`` ignored the T message, so after
``TRUNCATE`` + ``INSERT 100`` the destination kept the old ids next to 100 and
the run reported success with Gate-8 passed (a false green).
"""

from __future__ import annotations

import struct

import pytest

from tests import w2dat_live as live


def _relation(oid: int, schema: str, table: str, cols: list[str]) -> bytes:
    body = struct.pack("!i", oid) + schema.encode() + b"\0" + table.encode() + b"\0"
    body += struct.pack("!b", ord("d")) + struct.pack("!h", len(cols))
    for col in cols:
        body += struct.pack("!b", 1) + col.encode() + b"\0" + struct.pack("!ii", 23, -1)
    return b"R" + body


def test_decoder_turns_a_truncate_message_into_one_change_per_relation():
    from connectors.pgoutput_decoder import PgOutputDecoder, changes_for_tables

    dec = PgOutputDecoder()
    dec.feed(_relation(11, "public", "orders", ["id"]))
    dec.feed(_relation(12, "public", "other", ["id"]))
    msg = b"T" + struct.pack("!i", 2) + struct.pack("!b", 0) + struct.pack("!ii", 11, 12)
    changes = changes_for_tables(dec, msg, schema="public", tables={"orders"})
    assert [(c.op, c.relation) for c in changes] == [("truncate", "orders")]


def test_an_undecodable_message_stops_the_stream():
    from connectors.pgoutput_decoder import PgOutputDecoder
    from services.data_errors import CdcDecodeFailed

    dec = PgOutputDecoder()
    with pytest.raises(CdcDecodeFailed) as info:
        dec.feed(b"T" + struct.pack("!i", 1) + struct.pack("!b", 0) + struct.pack("!i", 99))
    assert info.value.code == "CDC_DECODE_FAILED"


def test_buffer_truncate_drops_earlier_changes_and_marks_the_batch():
    from services.cdc_multi_table import MultiTableTransactionBuffer

    buf = MultiTableTransactionBuffer()
    buf.begin("1")
    buf.insert("orders", {"id": 1}, pk="1")
    buf.truncate("orders")
    buf.insert("orders", {"id": 100}, pk="100")
    (batch,) = buf.commit(resume_token="t")
    assert batch.truncated is True
    assert [r["id"] for r in batch.inserts] == [100]
    buf.begin("2")
    buf.truncate("orders")
    (only,) = buf.commit(resume_token="t2")
    assert only.truncated and only.total_changes == 1


def test_cdc_value_reconcile_never_passes_with_destination_extras():
    from services.reconcile_coverage import CDC_SOURCE_IMAGE_VALUES
    from services.reconciliation import reconcile

    report = reconcile(source_rows=1, target_rows=3, source_checksum="abc",
                       target_checksum="abc", checksum_scope=CDC_SOURCE_IMAGE_VALUES)
    assert report.passed is False
    assert report.population_proof is False
    assert "2 row(s) the source no longer has" in report.message


@pytest.mark.parametrize("dest_kind", ["pg", "sqlite", "mysql"])
def test_source_truncate_empties_the_destination(dest_kind, tmp_path):
    live.need_logical_pg()
    u = live.uid()
    src = f"w2dat_d1_{u}"
    dst = f"w2dat_d1d_{u}"
    if dest_kind == "mysql":
        live.need_mysql()
        dest = live.ep_my(dst)
    elif dest_kind == "sqlite":
        dest = live.ep_sqlite(tmp_path / "d1.db", dst)
    else:
        dest = live.ep_pg(dst)
    cols = ["id", "v"]
    try:
        live.pg(f'CREATE TABLE public."{src}" (id integer primary key, v text)')
        live.pg(f'INSERT INTO public."{src}" VALUES (3, \'c\'), (5, \'e\')')
        first = live.run(live.ep_pg(src), dest, "cdc", key="id", cols=cols)
        assert first.success, first.error
        live.pg(f'TRUNCATE public."{src}"')
        live.pg(f'INSERT INTO public."{src}" VALUES (100, \'after\')')
        second = live.run(live.ep_pg(src), dest, "cdc", key="id", cols=cols)
        if dest_kind == "pg":
            ids = [r[0] for r in live.pg(f'SELECT id FROM public."{dst}" ORDER BY id')]
        elif dest_kind == "mysql":
            ids = [r[0] for r in live.my(f"SELECT id FROM `{dst}` ORDER BY id")]
        else:
            import sqlite3

            with sqlite3.connect(tmp_path / "d1.db") as conn:
                ids = [int(r[0]) for r in conn.execute(f'SELECT id FROM "{dst}" ORDER BY id')]
        assert ids == [100], (ids, second.error)
        assert second.success, second.error
        assert (second.destination_summary or {}).get("source_truncated") is True
    finally:
        live.drop_pg(src, dst)
        if dest_kind == "mysql":
            live.drop_my(dst)
        live.drop_cdc_artifacts(src)


def test_only_a_declared_delete_mode_off_explains_destination_extras():
    """The r13 contract: delete_mode=off keeps leftovers on purpose and says so."""
    from src.transfer.reconcile_step import _declared_off_leftover

    kept = {"status": "kept", "delete_mode": "off", "declared": True, "leftover": 1}
    assert _declared_off_leftover({"snapshot_leftover": kept}, "t") == 1
    assert _declared_off_leftover({"snapshot_leftover": {"t": kept}}, "t") == 1
    assert _declared_off_leftover({"snapshot_leftover": {**kept, "declared": False}}, "t") == 0
    assert _declared_off_leftover({"snapshot_leftover": {**kept, "delete_mode": "hard"}}, "t") == 0
    assert _declared_off_leftover({"snapshot_leftover": {**kept, "status": "unmeasured"}}, "t") == 0
    assert _declared_off_leftover({}, "t") == 0
