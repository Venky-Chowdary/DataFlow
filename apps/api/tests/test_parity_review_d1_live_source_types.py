"""Review D1: Parity Studio's live column types re-implemented
``value_readback._live_source_types`` and skipped its source-read driver
translation and catalog guard: a pgvector source was introspected as
"pgvector" (no such catalog reader) instead of through PostgreSQL, so the widen
fix never learned the source type. The source side now uses the shared helper.
"""

from __future__ import annotations

from types import SimpleNamespace


def test_source_types_go_through_the_shared_helper(monkeypatch):
    import src.transfer.adapters as adapters
    import src.transfer.adapters_introspect as introspect
    from services import parity_evidence, parity_fixes
    from services.parity_evidence import TableEvidence

    source = SimpleNamespace(format="pgvector", table="docs", collection="")
    dest = SimpleNamespace(format="postgresql", table="docs", collection="")
    monkeypatch.setattr(parity_evidence, "_endpoints", lambda _job: (None, source, dest))
    monkeypatch.setattr(adapters, "resolve_connector_config", lambda ep: {"type": ep.format})
    calls: list[tuple[str, str]] = []

    def introspect_types(db_type, _cfg, table, cols):
        calls.append((db_type, table))
        return {c: "numeric(12,4)" for c in cols}

    monkeypatch.setattr(introspect, "_introspect_table_schema", introspect_types)
    job = {"transfer_request": {"mappings": [{"source": "amount", "target": "amount"}]}}
    ev = TableEvidence(name="docs", check="key_diff", rows_source=1, rows_destination=1)
    live = parity_fixes._live_types(job, ev, ["amount"])
    assert ("postgresql", "docs") in calls and ("pgvector", "docs") not in calls, calls
    assert live["source"] == {"amount": "numeric(12,4)"}


def test_a_source_without_a_catalog_reader_is_not_introspected(monkeypatch):
    import src.transfer.adapters as adapters
    import src.transfer.adapters_introspect as introspect
    from services import parity_evidence, parity_fixes
    from services.parity_evidence import TableEvidence

    source = SimpleNamespace(format="mongodb", table="docs", collection="")
    dest = SimpleNamespace(format="postgresql", table="docs", collection="")
    monkeypatch.setattr(parity_evidence, "_endpoints", lambda _job: (None, source, dest))
    monkeypatch.setattr(adapters, "resolve_connector_config", lambda ep: {"type": ep.format})
    calls: list[str] = []
    monkeypatch.setattr(introspect, "_introspect_table_schema",
                        lambda db_type, _c, _t, cols: calls.append(db_type) or {c: "text" for c in cols})
    ev = TableEvidence(name="docs", check="key_diff", rows_source=1, rows_destination=1)
    live = parity_fixes._live_types({"transfer_request": {}}, ev, ["amount"])
    assert "mongodb" not in calls and live["source"] == {}
