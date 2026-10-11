"""Phase F4 — CDC transport selection + streaming buffer ack semantics."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_default_transport_is_auto(monkeypatch):
    monkeypatch.delenv("DATAFLOW_CDC_PG_TRANSPORT", raising=False)
    monkeypatch.delenv("DATAWRAP_CDC_PG_TRANSPORT", raising=False)
    from connectors.postgresql_cdc_transport import selected_pg_cdc_transport

    assert selected_pg_cdc_transport() == "auto"


def test_streaming_transport_selected(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CDC_PG_TRANSPORT", "streaming")
    from connectors.postgresql_cdc_transport import selected_pg_cdc_transport

    assert selected_pg_cdc_transport() == "streaming"


def test_streaming_buffer_ack_keeps_interleaved_txn():
    """A txn that began before an earlier commit has smaller BEGIN/change LSNs.

    Dropping by ``lsn <= ack`` discarded it (silent loss). Ack drops through the
    acked COMMIT only.
    """
    from connectors.postgresql_cdc_transport import PeekedChange, StreamingBuffer

    buf = StreamingBuffer()
    buf.extend(
        [
            PeekedChange(lsn="0/1C71D88", payload=b"B"),
            PeekedChange(lsn="0/1C71D88", payload=b"I"),
            PeekedChange(lsn="0/1C71E38", payload=b"C", is_commit=True),
            PeekedChange(lsn="0/1C71CA8", payload=b"B"),
            PeekedChange(lsn="0/1C71CA8", payload=b"I"),
            PeekedChange(lsn="0/1C71E38", payload=b"I"),
            PeekedChange(lsn="0/1C71EE8", payload=b"C", is_commit=True),
        ]
    )
    assert buf.drop_through_commit("0/1C71E38") == 3
    assert [x.lsn for x in buf.items] == ["0/1C71CA8", "0/1C71CA8", "0/1C71E38", "0/1C71EE8"]


def test_streaming_buffer_poll_redelivers_until_ack_and_never_splits_txn():
    from connectors.postgresql_cdc_transport import PeekedChange, StreamingBuffer

    buf = StreamingBuffer()
    buf.extend(
        [
            PeekedChange(lsn="0/10", payload=b"B"),
            PeekedChange(lsn="0/10", payload=b"I"),
            PeekedChange(lsn="0/10", payload=b"I"),
            PeekedChange(lsn="0/20", payload=b"C", is_commit=True),
            PeekedChange(lsn="0/30", payload=b"B"),
        ]
    )
    first = buf.committed(limit=2)
    assert [x.payload for x in first] == [b"B", b"I", b"I", b"C"]
    assert buf.committed(limit=2) == first  # no ack -> redelivered
    buf.drop_through_commit("0/20")
    assert buf.committed() == []  # open txn is never returned


def test_streaming_buffer_ack_before_redelivery_skips_on_arrival():
    """After a reconnect the acked txn may be resent later; drop it on arrival."""
    from connectors.postgresql_cdc_transport import PeekedChange, StreamingBuffer

    buf = StreamingBuffer()
    buf.drop_through_commit("0/20")
    buf.extend(
        [
            PeekedChange(lsn="0/10", payload=b"B"),
            PeekedChange(lsn="0/20", payload=b"C", is_commit=True),
            PeekedChange(lsn="0/30", payload=b"B"),
            PeekedChange(lsn="0/40", payload=b"C", is_commit=True),
        ]
    )
    assert [x.lsn for x in buf.committed()] == ["0/30", "0/40"]


def test_lsn_round_trip():
    from connectors.postgresql_cdc_transport import int_to_lsn, lsn_to_int

    assert int_to_lsn(lsn_to_int("1C/71EE8")) == "1C/71EE8"


def test_open_streaming_returns_none_when_peek_mode(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CDC_PG_TRANSPORT", "peek")
    from connectors.postgresql_cdc_transport import open_streaming_transport_or_none

    assert (
        open_streaming_transport_or_none(
            dsn_kwargs={"host": "127.0.0.1"},
            slot_name="df_test",
            publication_name="df_pub",
        )
        is None
    )


def test_change_stream_wires_transport_helpers():
    src = Path(_API_ROOT / "connectors" / "postgresql_change_stream.py").read_text(
        encoding="utf-8"
    )
    assert "_peek_or_stream_rows" in src
    assert "_ensure_streaming_transport" in src
    assert "postgresql_cdc_transport" in src


def test_pg_capability_marks_streaming_default_with_peek_fallback():
    """F4: streaming is the default only because the live streaming ITs prove it."""
    from services.connector_capability_registry import get_connector_capability

    cap = get_connector_capability("postgresql")
    assert cap.get("cdc_transport_default") == "auto"
    assert cap.get("cdc_streaming_status") == "default_with_peek_fallback"
    assert cap.get("supports_streaming") is True
    assert cap.get("bulk_export_status") == "implemented_pg_copy"


def test_warehouse_bulk_export_marked_planned():
    """F3 honesty: Snowflake/BQ source unload stays Planned until wired."""
    from services.connector_capability_registry import get_connector_capability

    assert get_connector_capability("snowflake").get("bulk_export_status") == "planned"
    assert get_connector_capability("bigquery").get("bulk_export_status") == "planned"
