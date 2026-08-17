"""Timezone-aware carriers must survive the destination DDL and the digest.

Two defects this covers, both of which produced a *wrong verdict* rather than a
wrong write:

1. ``generic_sql`` had no entry in the TZ-aware DDL map, so a ``TIMESTAMPTZ``
   source materialized as bare (NTZ) ``TIMESTAMP``. The write-time guard then
   quarantined every offset-bearing row — a collapse the type map invented, not
   one the destination imposed.
2. MongoDB/Elasticsearch stamp the same ``date`` token for logical date and
   datetime because BSON has no calendar-date type (it stores a UTC-millisecond
   instant). Fingerprinting that token as SQL ``DATE`` truncated time-of-day on
   destination read-back, so a faithful
   instant mismatched its own source digest (and real clock loss would have been
   hidden).
"""

from __future__ import annotations

import datetime as dt
import sqlite3
import uuid
from pathlib import Path

from services.reconciliation import fingerprint_for_reconcile
from services.type_system import ddl_type, instant_date_carrier
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

AWARE = "2024-06-01T08:30:00+05:30"
# What a driver hands back for a stored instant: naive, already UTC-normalized.
READ_BACK_UTC = dt.datetime(2024, 6, 1, 3, 0)  # noqa: DTZ001
READ_BACK_OTHER_CLOCK = dt.datetime(2024, 6, 1, 4, 0)  # noqa: DTZ001


class TestGenericSqlTimezoneCarrier:
    def test_aware_source_keeps_an_aware_generic_sql_carrier(self) -> None:
        assert ddl_type("generic_sql", "TIMESTAMPTZ").upper() == "TIMESTAMPTZ"
        assert ddl_type("generic_sql", "TIMESTAMP WITH TIME ZONE").upper() == (
            "TIMESTAMPTZ"
        )
        assert ddl_type("generic_sql", "DATETIMEOFFSET").upper() == "TIMESTAMPTZ"

    def test_wall_clock_source_is_not_promoted_to_an_aware_carrier(self) -> None:
        assert ddl_type("generic_sql", "TIMESTAMP_NTZ").upper() == "TIMESTAMP"
        assert ddl_type("generic_sql", "TIMESTAMP WITHOUT TIME ZONE").upper() == (
            "TIMESTAMP"
        )

    def test_offset_bearing_rows_are_written_not_quarantined(
        self, tmp_path: Path
    ) -> None:
        suffix = uuid.uuid4().hex[:8]
        src_path = tmp_path / f"src_{suffix}.db"
        conn = sqlite3.connect(src_path)
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, seen_at TIMESTAMPTZ)")
        conn.execute("INSERT INTO events VALUES (1, ?)", (AWARE,))
        conn.commit()
        conn.close()

        dst_path = tmp_path / f"dst_{suffix}.db"
        request = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(src_path),
                connection_string=f"sqlite:///{src_path}",
                table="events",
            ),
            destination=EndpointConfig(
                kind="database",
                format="generic_sql",
                database=str(dst_path),
                connection_string=f"sqlite:///{dst_path}",
                table=f"events_{suffix}",
            ),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="strict",
            mappings=[
                {"source": "id", "target": "id", "target_type": "BIGINT"},
                {"source": "seen_at", "target": "seen_at", "target_type": "TIMESTAMPTZ"},
            ],
        )

        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])

        assert result.success, result.error
        assert result.records_transferred == 1
        recon = result.reconciliation or {}
        assert recon.get("rejected_rows") == 0
        assert recon.get("checksum_match") is True

        rows = sqlite3.connect(dst_path).execute(
            f"SELECT seen_at FROM events_{suffix}"
        ).fetchall()
        assert len(rows) == 1
        # 08:30+05:30 is 03:00 UTC — the instant, never the stripped wall clock.
        assert "03:00:00" in str(rows[0][0])


class TestBsonDateTokenIsAnInstant:
    def test_document_stores_resolve_date_to_an_instant_carrier(self) -> None:
        assert instant_date_carrier("mongodb", "date") == "TIMESTAMPTZ"
        assert instant_date_carrier("elasticsearch", "DATE") == "TIMESTAMPTZ"
        assert instant_date_carrier("postgresql", "DATE") == "DATE"
        assert instant_date_carrier("mongodb", "string") == "string"

    def test_time_of_day_survives_the_mongo_read_back_digest(self) -> None:
        assert (
            fingerprint_for_reconcile(
                READ_BACK_UTC, ddl_type="date", engine="mongodb"
            )
            == "2024-06-01T03:00:00"
        )
        assert (
            fingerprint_for_reconcile(AWARE, ddl_type="date", engine="mongodb")
            == "2024-06-01T03:00:00"
        )

    def test_a_different_clock_still_mismatches(self) -> None:
        assert fingerprint_for_reconcile(
            READ_BACK_OTHER_CLOCK, ddl_type="date", engine="mongodb"
        ) != fingerprint_for_reconcile(
            READ_BACK_UTC, ddl_type="date", engine="mongodb"
        )

    def test_sql_date_columns_still_truncate(self) -> None:
        assert (
            fingerprint_for_reconcile(
                READ_BACK_UTC, ddl_type="DATE", engine="postgresql"
            )
            == "2024-06-01T00:00:00"
        )
