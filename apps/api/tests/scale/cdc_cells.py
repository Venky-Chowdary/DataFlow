"""CDC cells at 100K: log-based capture, hard deletes, cursor persistence, lag.

What separates real CDC from a cursor poll is a **hard delete**. A poll reads
``WHERE cursor > watermark``; a row that no longer exists produces no tuple, so
the destination keeps it forever and the operator's row counts drift apart
silently. Every CDC cell here therefore deletes rows at the source after the
snapshot and requires the destination — read on an independent connection — to
have lost exactly those keys. A route that returns success while the deleted
keys survive at the destination is recorded as a defect, not as a pass.

The second thing measured is the snapshot → log handoff. The engine must
consistently switch from reading the table to reading the log with no gap (a row
changed during the snapshot must not be lost) and no duplicate (it must not be
applied twice as an insert). At 100K rows the handoff window is wide enough for
either to show up in the count and the checksum.
"""

from __future__ import annotations

import re
from typing import Any, Sequence

from tests.scale import live_engines as L
from tests.scale.matrix import Cell, Matrix, contract, fill, run_transfer

COLS: list[str] = ["id", "region", "amount", "note", "updated_at"]
SHAPE = "id BIGINT PK, region TEXT, amount NUMERIC(12,2), note TEXT NULL, updated_at TIMESTAMP"

#: Applied to the source after the snapshot, carried only by the log reader.
INSERTS = 2000
UPDATES = 1000
DELETES = 1000


def pg_ep(table: str) -> Any:
    from tests.typed_fidelity_helpers import pg_endpoint

    return pg_endpoint(table)


def mysql_ep(table: str) -> Any:
    from tests.typed_fidelity_helpers import mysql_endpoint

    return mysql_endpoint(table)


def mongo_ep(collection: str) -> Any:
    from tests.typed_fidelity_helpers import mongo_endpoint

    return mongo_endpoint(collection)


def cdc_contract(stream: str, *, snapshot_mode: str = "initial") -> list[dict[str, Any]]:
    return [
        contract(
            stream,
            "cdc",
            primary_key="id",
            cursor_field="updated_at",
            cursor_semantics="modification_timestamp",
            snapshot_mode=snapshot_mode,
        )
    ]


# --------------------------------------------------------------------------
# independent reads of persisted CDC state
# --------------------------------------------------------------------------

def persisted_cursor(source_object: str, dest_object: str = "") -> dict[str, Any]:
    """Read the route's stored watermark straight out of the cursor store.

    Deliberately not via ``services.sync_cursor``: this is the same claim the
    engine makes about itself, so it is verified on a driver connection to the
    store the engine wrote to.
    """
    pattern = f":{source_object}→" + (f".*:{dest_object}:" if dest_object else "")
    client = L.mongo_client()
    try:
        for db in ("datatransfer", L.MONGO_DB):
            coll = client[db]["sync_cursors"]
            doc = coll.find_one({"key": {"$regex": pattern}})
            if doc:
                return {
                    "watermark": str(doc.get("watermark") or ""),
                    "metadata": dict(doc.get("metadata") or {}),
                }
    except Exception as exc:  # noqa: BLE001 — absence is the result
        print(f"    mongo cursor lookup skipped: {exc}")
    finally:
        client.close()
    from services.sync_cursor import get_watermark_record, list_cursor_keys

    for key in list_cursor_keys():
        if f":{source_object}→" in key and (not dest_object or f":{dest_object}:" in key):
            watermark, metadata = get_watermark_record(key)
            return {"watermark": str(watermark or ""), "metadata": metadata}
    return {"watermark": "", "metadata": {}}


def _release_route_leases(job_slug: str) -> list[str]:
    """Force-release CDC consumer leases left behind by this route's jobs.

    Crash-injection kills a consumer mid-stream, so its lease outlives it and
    the next run is refused as a concurrent consumer. Both steps go through the
    operator path — enumerate (``list_lease_views``), then break the ones this
    route owns (``force_release_lease``) — so nothing here reaches into the
    store, and a lease belonging to another route is never touched.
    """
    from services.cdc_lease import force_release_lease, list_lease_views

    released: list[str] = []
    for view in list_lease_views():
        key = str(view.get("cursor_key") or "")
        owned = job_slug and job_slug in (
            f"{key}|{view.get('resource')}|{view.get('holder_job_id')}"
        )
        if not owned:
            continue
        if force_release_lease(key, reason="scale harness reset", actor="scale").get(
            "released"
        ):
            released.append(key)
    return released


def mongo_query(where: str) -> dict[str, Any]:
    """Translate the delete-window predicate the cells use into a Mongo filter.

    The delete proof is a range read (``id BETWEEN lo AND hi``) that has to hit
    the destination store itself; only the shapes the cells emit are accepted so
    an unrecognised predicate cannot silently count every document as a match.
    """
    text = where.strip().removeprefix("WHERE ").strip()
    if not text:
        return {}
    match = re.fullmatch(r"id BETWEEN (\d+) AND (\d+)", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"no Mongo translation for destination predicate: {where!r}")
    return {"id": {"$gte": int(match.group(1)), "$lte": int(match.group(2))}}


def cdc_summary(result: Any) -> dict[str, Any]:
    dest = dict(getattr(result, "destination_summary", None) or {})
    inner = dest.get("cdc")
    return dict(inner) if isinstance(inner, dict) else dest


def _lag_fields(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        k: summary.get(k)
        for k in (
            "cdc_plugin",
            "cdc_slot_name",
            "cdc_capture_instance",
            "cdc_delivery",
            "cdc_lag_seconds",
            "cdc_lag_basis",
            "cdc_heartbeat_age_sec",
            "replication_lag_bytes",
            "watermark",
            "inserts",
            "updates",
            "deletes",
        )
        if summary.get(k) is not None
    }


# --------------------------------------------------------------------------
# one generic CDC route, parameterized per source engine
# --------------------------------------------------------------------------

class CdcRoute:
    """A source engine's CDC route: seed, mutate and read back independently."""

    def __init__(
        self, name: str, *, dialect: str, dest: str = "postgresql", tag: str = ""
    ) -> None:
        self.name = name
        self.dialect = dialect
        self.dest = dest
        self.tag = tag
        # Source object is per route *pair*: two routes reading the same table
        # share one cursor key, and the second would read the first's watermark.
        suffix = f"{tag}{dialect[:2]}{dest[:2]}"
        self.src_object = f"sc_cdc_{suffix}_s"
        self.dst_object = f"sc_cdc_{suffix}_d"
        self.job_slug = f"sccdc{suffix}"

    # -- source side -------------------------------------------------------
    @property
    def tz_aware(self) -> bool:
        """A Mongo destination needs an instant, not a zoneless wall clock."""
        return self.dest == "mongodb"

    def seed(self, rows: int) -> None:
        if self.dialect == "postgresql":
            L.seed_pg_scale(self.src_object, rows, tz_aware=self.tz_aware)
        elif self.dialect == "mysql":
            L.seed_mysql_scale(self.src_object, rows)
        else:
            L.seed_mongo_scale(self.src_object, rows)

    def source_endpoint(self) -> Any:
        return {
            "postgresql": pg_ep,
            "mysql": mysql_ep,
            "mongodb": mongo_ep,
        }[self.dialect](self.src_object)

    def dest_endpoint(self) -> Any:
        return {
            "postgresql": pg_ep,
            "mysql": mysql_ep,
            "mongodb": mongo_ep,
        }[self.dest](self.dst_object)

    def source_count(self) -> int:
        if self.dialect == "postgresql":
            return L.pg_count(self.src_object)
        if self.dialect == "mysql":
            return L.mysql_count(self.src_object)
        return L.mongo_count(self.src_object)

    def source_projection(self) -> list[tuple]:
        if self.dialect == "postgresql":
            return L.pg_projection(self.src_object, COLS)
        if self.dialect == "mysql":
            return L.mysql_projection(self.src_object, COLS)
        return L.mongo_projection(self.src_object, COLS)

    def mutate(self, base_rows: int) -> tuple[int, int]:
        """Insert, update and hard-delete at the source; return the delete range."""
        del_lo, del_hi = 2001, 2000 + DELETES
        if self.dialect == "postgresql":
            L.pg_append_scale(
                self.src_object, INSERTS, start=base_rows + 1,
                tz_aware=self.tz_aware,
            )
            L.pg_exec(
                f'UPDATE public."{self.src_object}" SET amount = amount + 5, '
                f"note = 'cdc-upd', updated_at = updated_at + interval '1 day' "
                f"WHERE id <= {UPDATES}"
            )
            L.pg_exec(
                f'DELETE FROM public."{self.src_object}" '
                f"WHERE id BETWEEN {del_lo} AND {del_hi}"
            )
        elif self.dialect == "mysql":
            L.mysql_append_scale(self.src_object, INSERTS, start=base_rows + 1)
            L.mysql_exec(
                f"UPDATE `{self.src_object}` SET amount = amount + 5, "
                f"note = 'cdc-upd', updated_at = DATE_ADD(updated_at, INTERVAL 1 DAY) "
                f"WHERE id <= {UPDATES}"
            )
            L.mysql_exec(
                f"DELETE FROM `{self.src_object}` "
                f"WHERE id BETWEEN {del_lo} AND {del_hi}"
            )
        else:
            client = L.mongo_client()
            try:
                coll = client[L.MONGO_DB][self.src_object]
                coll.insert_many(
                    [L.mongo_doc(i) for i in range(base_rows + 1, base_rows + 1 + INSERTS)],
                    ordered=False,
                )
                coll.update_many({"id": {"$lte": UPDATES}}, {"$set": {"note": "cdc-upd"}})
                coll.delete_many({"id": {"$gte": del_lo, "$lte": del_hi}})
            finally:
                client.close()
        return del_lo, del_hi

    # -- destination side (independent driver connection) ------------------
    def dest_count(self, where: str = "") -> int:
        if self.dest == "postgresql":
            return L.pg_count(self.dst_object, where)
        if self.dest == "mysql":
            return L.mysql_count(self.dst_object, where)
        return L.mongo_count(self.dst_object, mongo_query(where))

    def dest_projection(self, cols: Sequence[str] = tuple(COLS)) -> list[tuple]:
        if self.dest == "postgresql":
            return L.pg_projection(self.dst_object, cols)
        if self.dest == "mysql":
            return L.mysql_projection(self.dst_object, cols)
        return L.mongo_projection(self.dst_object, cols)

    def drop(self) -> None:
        if self.dialect == "postgresql":
            L.pg_drop(self.src_object)
        elif self.dialect == "mysql":
            L.mysql_drop(self.src_object)
        else:
            L.mongo_drop(self.src_object)
        if self.dest == "postgresql":
            L.pg_drop(self.dst_object)
        elif self.dest == "mysql":
            L.mysql_drop(self.dst_object)
        else:
            L.mongo_drop(self.dst_object)

    # -- the run -----------------------------------------------------------
    def run(self, job_id: str, *, snapshot_mode: str = "initial") -> tuple[Any, float, str]:
        return run_transfer(
            self.source_endpoint(),
            self.dest_endpoint(),
            mode="cdc",
            contracts=cdc_contract(self.src_object, snapshot_mode=snapshot_mode),
            cols=COLS,
            job_id=job_id,
            # Mongo mints ``_id`` server-side: a real source column, so it is
            # declared omitted rather than dropped silently (gate G13).
            omit=["_id"] if self.dialect == "mongodb" else [],
        )

    def cursor(self) -> dict[str, Any]:
        return persisted_cursor(self.src_object, self.dst_object)

    # -- state reset -------------------------------------------------------
    def reset_state(self) -> dict[str, Any]:
        """Clear replication slots, cursors and leases this route owns.

        A previous (or killed) run leaves a slot accumulating WAL, a persisted
        watermark and a live consumer lease behind. Re-running on top of that
        state measures the leftovers, not the route, so every cell starts from
        a declared clean slate.
        """
        cleared: dict[str, Any] = {"slots": [], "cursors": 0, "leases": []}
        for (slot,) in L.pg_fetch(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE %s",
            (f"%{self.src_object.replace('_', '')}%",),
        ) or []:
            L.pg_fetch("SELECT pg_drop_replication_slot(%s)", (slot,))
            cleared["slots"].append(slot)
        for (slot,) in L.pg_fetch(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE %s",
            (f"%{self.src_object}%",),
        ) or []:
            L.pg_fetch("SELECT pg_drop_replication_slot(%s)", (slot,))
            cleared["slots"].append(slot)
        client = L.mongo_client()
        try:
            for db in ("datatransfer", L.MONGO_DB):
                res = client[db]["sync_cursors"].delete_many(
                    {"key": {"$regex": self.src_object}}
                )
                cleared["cursors"] += int(res.deleted_count)
        except Exception as exc:  # noqa: BLE001 — absence is fine
            print(f"    mongo cursor reset skipped: {exc}")
        finally:
            client.close()
        cleared["leases"] = _release_route_leases(self.job_slug)
        return cleared


def drain(route: CdcRoute, job_id: str, *, max_polls: int = 12
          ) -> tuple[Any, float, str, int]:
    """Poll until the destination stops moving; return the last poll's result.

    One ``poll()`` is a *bounded window*: the reader stops at ``batch_size``
    events (or its wait deadline) and returns, exactly as a continuous worker
    would between beats. Grading a route on a single window would score the
    window size, not the route — a 2,000-event change set delivered 1,000 at a
    time would read as "half the inserts lost, then duplicated". Draining is
    bounded and the poll count is published with the cell.
    """
    total = 0.0
    polls = 0
    last: Any = None
    run_id = ""
    prev_dest = -1
    stable = 0
    while polls < max_polls:
        last, elapsed, run_id = route.run(job_id)
        polls += 1
        total += elapsed
        if not last.success:
            break
        dest = route.dest_count()
        # Two quiet windows, not one: a window can end on its wait deadline with
        # events still queued behind it (Mongo delivers a delete after the
        # insert burst), so a single quiet poll is not proof the log is drained.
        stable = stable + 1 if dest == prev_dest else 0
        prev_dest = dest
        if stable >= 2:
            break
    return last, total, run_id, polls


def run_cdc_route(matrix: Matrix, route: CdcRoute, rows: int) -> list[Cell]:
    """Snapshot → stream handoff, then insert/update/delete, then an idle re-run."""
    label = f"{route.dialect}→{route.dest}"
    job_id = route.job_slug
    route.drop()
    reset = route.reset_state()
    route.seed(rows)

    # ---- cell 1: snapshot + handoff -------------------------------------
    snap = Cell(route=label, mode="cdc snapshot+log handoff", schema_shape=SHAPE)
    result, elapsed, run_id = route.run(job_id)
    fill(snap, result, elapsed, run_id)
    snap.detail["state_reset"] = reset
    snap.source_rows = route.source_count()
    if result.success:
        snap.dest_rows = route.dest_count()
        snap.source_checksum = L.checksum(route.source_projection())
        snap.dest_checksum = L.checksum(route.dest_projection())
        summary = cdc_summary(result)
        snap.delivery = str(summary.get("cdc_delivery") or "")
        snap.detail = _lag_fields(summary)
        cursor = route.cursor()
        snap.detail["persisted_watermark"] = cursor["watermark"]
        ok = (
            snap.dest_rows == snap.source_rows
            and snap.source_checksum == snap.dest_checksum
            and bool(cursor["watermark"])
        )
        snap.mark(
            ok,
            "snapshot landed and cursor persisted"
            if ok
            else f"dest={snap.dest_rows} vs src={snap.source_rows}, "
            f"checksum_match={snap.source_checksum == snap.dest_checksum}",
        )
    matrix.add(snap)
    if snap.status != "pass":
        return [snap]

    before_watermark = snap.detail.get("persisted_watermark", "")

    # ---- cell 2: insert / update / hard delete propagation ---------------
    dml = Cell(route=label, mode="cdc insert+update+delete", schema_shape=SHAPE)
    del_lo, del_hi = route.mutate(rows)
    result2, elapsed2, run_id2, polls2 = drain(route, job_id)
    fill(dml, result2, elapsed2, run_id2)
    dml.source_rows = route.source_count()
    if result2.success:
        dml.dest_rows = route.dest_count()
        dml.source_checksum = L.checksum(route.source_projection())
        dml.dest_checksum = L.checksum(route.dest_projection())
        summary2 = cdc_summary(result2)
        dml.delivery = str(summary2.get("cdc_delivery") or "")
        dml.detail = _lag_fields(summary2)
        dml.detail["poll_windows_to_drain"] = polls2
        survivors = route.dest_count(f"id BETWEEN {del_lo} AND {del_hi}")
        dml.detail["deleted_keys_surviving_at_destination"] = survivors
        cursor2 = route.cursor()
        dml.detail["persisted_watermark"] = cursor2["watermark"]
        dml.delete_capture = "yes" if survivors == 0 else "no"
        advanced = bool(cursor2["watermark"]) and cursor2["watermark"] != before_watermark
        dml.detail["watermark_advanced"] = advanced
        ok = (
            survivors == 0
            and dml.dest_rows == dml.source_rows
            and dml.source_checksum == dml.dest_checksum
            and advanced
        )
        dml.mark(
            ok,
            f"hard deletes propagated ({DELETES}), updates applied ({UPDATES}), "
            f"inserts applied ({INSERTS}) across {polls2} poll window(s)"
            if ok
            else f"deleted keys still at destination={survivors}, "
            f"dest={dml.dest_rows} src={dml.source_rows}, "
            f"checksum_match={dml.source_checksum == dml.dest_checksum}, "
            f"watermark_advanced={advanced}",
        )
    matrix.add(dml)

    # ---- cell 3: idle re-run must not duplicate --------------------------
    idle = Cell(route=label, mode="cdc idle re-run (no dup)", schema_shape=SHAPE)
    result3, elapsed3, run_id3 = route.run(job_id)
    fill(idle, result3, elapsed3, run_id3)
    idle.source_rows = route.source_count()
    if result3.success:
        idle.dest_rows = route.dest_count()
        idle.source_checksum = L.checksum(route.source_projection())
        idle.dest_checksum = L.checksum(route.dest_projection())
        idle.delivery = str(cdc_summary(result3).get("cdc_delivery") or "")
        ok = (
            idle.dest_rows == dml.dest_rows
            and idle.source_checksum == idle.dest_checksum
        )
        idle.mark(
            ok,
            "no change captured, destination unchanged"
            if ok
            else f"destination moved from {dml.dest_rows} to {idle.dest_rows} "
            f"with no source change, "
            f"checksum_match={idle.source_checksum == idle.dest_checksum}",
        )
    matrix.add(idle)
    return [snap, dml, idle]


def postgres_cdc(matrix: Matrix, rows: int) -> None:
    if not L.reachable("localhost", 5432):
        matrix.add(Cell(route="postgresql→postgresql", mode="cdc").skip("PostgreSQL 5432 unreachable"))
        return
    wal = L.pg_fetch("SHOW wal_level")[0][0]
    if str(wal) != "logical":
        matrix.add(
            Cell(route="postgresql→postgresql", mode="cdc").skip(
                f"wal_level={wal}, logical decoding impossible"
            )
        )
        return
    run_cdc_route(matrix, CdcRoute("pg", dialect="postgresql", dest="postgresql"), rows)
    run_cdc_route(matrix, CdcRoute("pg-my", dialect="postgresql", dest="mysql"), rows)
    if L.reachable("localhost", 27017):
        run_cdc_route(
            matrix, CdcRoute("pg-mo", dialect="postgresql", dest="mongodb"), rows
        )


def mysql_cdc(matrix: Matrix, rows: int) -> None:
    if not L.reachable("localhost", 3306):
        matrix.add(Cell(route="mysql→postgresql", mode="cdc").skip("MySQL 3306 unreachable"))
        return
    fmt = L.mysql_fetch("SELECT @@binlog_format, @@gtid_mode")[0]
    if str(fmt[0]).upper() != "ROW":
        matrix.add(
            Cell(route="mysql→postgresql", mode="cdc").skip(
                f"binlog_format={fmt[0]}, row-level capture impossible"
            )
        )
        return
    try:
        import pymysqlreplication  # noqa: F401
    except ImportError:
        matrix.add(
            Cell(route="mysql→postgresql", mode="cdc").skip(
                "pymysqlreplication not installed"
            )
        )
        return
    run_cdc_route(matrix, CdcRoute("my", dialect="mysql", dest="postgresql"), rows)
    run_cdc_route(matrix, CdcRoute("my-my", dialect="mysql", dest="mysql"), rows)


def mongo_cdc(matrix: Matrix, rows: int) -> None:
    if not L.reachable("localhost", 27017):
        matrix.add(Cell(route="mongodb→postgresql", mode="cdc").skip("MongoDB 27017 unreachable"))
        return
    rs = L.mongo_replica_set()
    if not rs:
        matrix.add(
            Cell(route="mongodb→postgresql", mode="cdc").skip(
                "standalone mongod: change streams require a replica set"
            )
        )
        return
    run_cdc_route(matrix, CdcRoute("mo", dialect="mongodb", dest="postgresql"), rows)
    if L.reachable("localhost", 3306):
        run_cdc_route(matrix, CdcRoute("mo-my", dialect="mongodb", dest="mysql"), rows)
