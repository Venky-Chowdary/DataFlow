"""Wave 58: PG introspect preserves specialty carriers for bind/DDL SSOT."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_pg_specialty_introspect_carriers():
    from services.schema_introspect import _pg_to_logical
    from services.type_system import ddl_type

    cases = {
        "inet": "INET",
        "cidr": "CIDR",
        "macaddr": "MACADDR",
        "macaddr8": "MACADDR8",
        "point": "POINT",
        "line": "LINE",
        "lseg": "LSEG",
        "box": "BOX",
        "path": "PATH",
        "polygon": "POLYGON",
        "circle": "CIRCLE",
        "pg_lsn": "PG_LSN",
        "oid": "OID",
        "tid": "TID",
        "xid": "XID",
        "xid8": "XID8",
        "cid": "CID",
        "hstore": "HSTORE",
        "xml": "XML",
        "ltree": "LTREE",
        "tsvector": "TSVECTOR",
        "jsonb": "JSONB",
        "int4range": "INT4RANGE",
        "tstzmultirange": "TSTZMULTIRANGE",
        "txid_snapshot": "TXID_SNAPSHOT",
        "pg_snapshot": "PG_SNAPSHOT",
    }
    for raw, want in cases.items():
        assert _pg_to_logical(raw) == want, raw
        assert ddl_type("postgresql", want) == want, want

    # Integers keep width — specialty split must not invent OID for bigint.
    assert _pg_to_logical("bigint") == "BIGINT"
    assert _pg_to_logical("integer") == "INT4"
