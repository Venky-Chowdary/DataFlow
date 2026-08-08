"""Phase F4 — CDC transport selection + streaming buffer ack semantics."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_default_transport_is_peek(monkeypatch):
    monkeypatch.delenv("DATAFLOW_CDC_PG_TRANSPORT", raising=False)
    monkeypatch.delenv("DATAWRAP_CDC_PG_TRANSPORT", raising=False)
    from connectors.postgresql_cdc_transport import selected_pg_cdc_transport

    assert selected_pg_cdc_transport() == "peek"


def test_streaming_transport_selected(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CDC_PG_TRANSPORT", "streaming")
    from connectors.postgresql_cdc_transport import selected_pg_cdc_transport

    assert selected_pg_cdc_transport() == "streaming"


def test_streaming_buffer_drop_upto_lsn():
    from connectors.postgresql_cdc_transport import PeekedChange, StreamingBuffer

    buf = StreamingBuffer()
    buf.extend(
        [
            PeekedChange(lsn="0/16B8", payload=b"a"),
            PeekedChange(lsn="0/16C0", payload=b"b"),
            PeekedChange(lsn="0/16D0", payload=b"c"),
        ]
    )
    dropped = buf.drop_upto_lsn("0/16C0")
    assert dropped == 2
    assert [x.lsn for x in buf.items] == ["0/16D0"]


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
