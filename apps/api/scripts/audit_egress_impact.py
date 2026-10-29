"""Read-only: which saved connectors would the ENFORCING egress rule refuse?

The newer egress sites (connector save, transfer reads/writes, introspection,
precount, shared HTTP/SMTP/Mongo clients, redirects) only audit by default.
Setting ``DATAFLOW_EGRESS_ENFORCE_NEW=1`` makes them refuse private, loopback
and CGNAT hosts. Run this against the connector store BEFORE flipping that
flag to get the number of saved connectors that would stop working, and why:

    python scripts/audit_egress_impact.py                      # store as configured
    python scripts/audit_egress_impact.py --file connectors.json
    MONGO=... python scripts/audit_egress_impact.py --mongo-url-env MONGO

It applies the same rule the runtime applies (workspace ``egress_allowlist``
from the tenant store and ``DATAFLOW_EGRESS_ALLOWLIST`` included, file engines
exempt) and resolves host names through DNS. It never writes to any store and
never prints connection strings or secrets: only the host that was refused.
Metadata and link-local hosts are listed too; those are refused regardless of
the flag.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Iterable

import audit_common

# Dial-target fields the runtime checks (services.egress_guard._cfg_dial_values).
_FIELDS = ("host", "endpoint_url", "connection_string")
_EXTRA_FIELDS = ("base_url", "url", "api_url", "registry_url", "schema_registry_url")


def _targets(connector: Any) -> Iterable[tuple[str, str]]:
    for field in _FIELDS:
        value = str(getattr(connector, field, "") or "").strip()
        if value:
            yield field, value
    extra = getattr(connector, "extra", None) or {}
    for field in _EXTRA_FIELDS:
        value = str(extra.get(field) or "").strip()
        if value:
            yield f"extra.{field}", value


def affected_connectors(connectors: Iterable[Any]) -> list[dict[str, Any]]:
    """One row per refused (connector, field, host)."""
    from services.egress_guard import host_class, iter_refusals

    rows: list[dict[str, Any]] = []
    for connector in connectors:
        for field, value in _targets(connector):
            for host, reason in iter_refusals(
                value,
                workspace_id=str(connector.workspace_id or ""),
                db_type=str(connector.type or ""),
            ):
                host_kind = host_class(host)
                rows.append(
                    {
                        "name": connector.name,
                        "id": connector.id,
                        "workspace": connector.workspace_id or "(legacy)",
                        "type": connector.type,
                        "field": field,
                        "host": host,
                        "host_class": host_kind,
                        "reason": reason,
                        "refused_regardless_of_flag": host_kind in {"metadata", "link-local"},
                    }
                )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    audit_common.add_store_args(parser)
    args = parser.parse_args(argv)

    connectors = audit_common.load_saved_connectors(args)
    rows = affected_connectors(connectors)
    names = {(r["workspace"], r["id"]) for r in rows}
    if args.json:
        print(
            json.dumps(
                {"saved_connectors": len(connectors), "would_be_refused": len(names), "rows": rows},
                indent=2,
            )
        )
        return 0
    for r in rows:
        print(
            f"{r['workspace']:<24} {r['name']:<32} {r['type']:<14} "
            f"{r['host_class']:<10} {r['field']}={r['host']}  [{r['reason']}]"
        )
    by_class: dict[str, int] = {}
    for r in rows:
        by_class[r["host_class"]] = by_class.get(r["host_class"], 0) + 1
    print(
        f"\n{len(names)} of {len(connectors)} saved connectors would be refused under "
        "DATAFLOW_EGRESS_ENFORCE_NEW=1"
        + (f" ({', '.join(f'{k}: {v}' for k, v in sorted(by_class.items()))})" if by_class else "")
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
