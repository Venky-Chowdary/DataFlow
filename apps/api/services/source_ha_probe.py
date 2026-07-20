"""Source HA role probe — SQL Server Always On AG + Oracle Data Guard.

Honesty
-------
Reads live catalog/DMV rows. Does **not** claim dual-node failover IT or
automatic listener reconnect. On a single-node host the probe returns
``STANDALONE`` / ``PRIMARY`` (Oracle) — that is correct, not a fake AG.
Continuous CDC across an AG/DG retention gap remains fail-closed via
``cdc_cursor_gap`` (reset watermark + re-snapshot).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class SourceHaProbe:
    dialect: str
    role: str
    ha_enabled: bool
    topology: str
    group_name: str | None = None
    replica_name: str | None = None
    db_unique_name: str | None = None
    open_mode: str | None = None
    protection_mode: str | None = None
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def job_fields(self) -> dict[str, Any]:
        """Flat fields for job documents / Theater / Results."""
        return {
            "source_ha_role": self.role,
            "source_ha_topology": self.topology,
            "source_ha_enabled": self.ha_enabled,
            "source_ha_group": self.group_name or self.db_unique_name,
            "source_ha_replica": self.replica_name,
            "source_ha_open_mode": self.open_mode,
            "source_ha_message": self.message,
        }


def classify_sqlserver_ag_rows(rows: list[dict[str, Any]]) -> SourceHaProbe:
    """Classify local AG replica role from ``sys.dm_hadr_*`` join rows."""
    if not rows:
        return SourceHaProbe(
            dialect="sqlserver",
            role="STANDALONE",
            ha_enabled=False,
            topology="none",
            message="Not joined to an Always On availability group (single-node / non-AG).",
        )
    row = rows[0]
    role = str(row.get("role_desc") or row.get("ROLE_DESC") or "UNKNOWN").strip().upper() or "UNKNOWN"
    group = row.get("ag_name") or row.get("AG_NAME") or row.get("group_name")
    replica = row.get("replica_server_name") or row.get("REPLICA_SERVER_NAME")
    return SourceHaProbe(
        dialect="sqlserver",
        role=role,
        ha_enabled=True,
        topology="availability_group",
        group_name=str(group) if group else None,
        replica_name=str(replica) if replica else None,
        message=(
            f"Always On AG role={role}"
            + (f" group={group}" if group else "")
            + (f" replica={replica}" if replica else "")
        ),
        details={
            "operational_state": row.get("operational_state_desc") or row.get("OPERATIONAL_STATE_DESC"),
            "connected_state": row.get("connected_state_desc") or row.get("CONNECTED_STATE_DESC"),
            "sync_health": row.get("synchronization_health_desc") or row.get("SYNCHRONIZATION_HEALTH_DESC"),
        },
    )


def classify_oracle_dg_row(row: dict[str, Any] | None) -> SourceHaProbe:
    """Classify Oracle database role from ``v$database``."""
    if not row:
        return SourceHaProbe(
            dialect="oracle",
            role="UNKNOWN",
            ha_enabled=False,
            topology="unknown",
            message="v$database returned no row.",
        )
    role = str(row.get("DATABASE_ROLE") or row.get("database_role") or "UNKNOWN").strip().upper()
    open_mode = row.get("OPEN_MODE") or row.get("open_mode")
    protection = row.get("PROTECTION_MODE") or row.get("protection_mode")
    unique = row.get("DB_UNIQUE_NAME") or row.get("db_unique_name")
    broker = str(row.get("DATAGUARD_BROKER") or row.get("dataguard_broker") or "").strip().upper()
    ha = role not in {"PRIMARY", ""} and role != "UNKNOWN"
    # PRIMARY with broker ENABLED still means DG topology may exist.
    if broker in {"ENABLED", "YES"} or role in {
        "PHYSICAL STANDBY",
        "LOGICAL STANDBY",
        "SNAPSHOT STANDBY",
    }:
        ha = True
    topology = "data_guard" if ha or broker in {"ENABLED", "YES"} else "none"
    if role == "PRIMARY" and topology == "none":
        message = "Oracle PRIMARY — no Data Guard broker/standby role detected on this instance."
    else:
        message = f"Oracle DATABASE_ROLE={role}" + (f" open_mode={open_mode}" if open_mode else "")
    return SourceHaProbe(
        dialect="oracle",
        role=role.replace(" ", "_") if role else "UNKNOWN",
        ha_enabled=bool(ha),
        topology=topology,
        db_unique_name=str(unique) if unique else None,
        open_mode=str(open_mode) if open_mode else None,
        protection_mode=str(protection) if protection else None,
        message=message,
        details={"dataguard_broker": broker or None},
    )


_SQLSERVER_AG_SQL = """
SELECT
    ag.name AS ag_name,
    ar.replica_server_name AS replica_server_name,
    ars.role_desc AS role_desc,
    ars.operational_state_desc AS operational_state_desc,
    ars.connected_state_desc AS connected_state_desc,
    ars.synchronization_health_desc AS synchronization_health_desc
FROM sys.dm_hadr_availability_replica_states AS ars
INNER JOIN sys.availability_replicas AS ar
    ON ars.replica_id = ar.replica_id
INNER JOIN sys.availability_groups AS ag
    ON ars.group_id = ag.group_id
WHERE ars.is_local = 1
"""

_ORACLE_DG_SQL = """
SELECT DATABASE_ROLE, OPEN_MODE, PROTECTION_MODE, DATAGUARD_BROKER, DB_UNIQUE_NAME
FROM v$database
"""


def probe_sqlserver_ha(conn: Any) -> SourceHaProbe:
    """Run AG DMV query on an open SQLAlchemy connection."""
    import sqlalchemy as sa

    try:
        result = conn.execute(sa.text(_SQLSERVER_AG_SQL))
        rows = [dict(r._mapping) for r in result]
        return classify_sqlserver_ag_rows(rows)
    except Exception as exc:
        # Express / non-AG editions often lack these views — treat as standalone.
        text = str(exc).lower()
        if any(
            token in text
            for token in (
                "dm_hadr",
                "availability",
                "invalid object",
                "permission",
                "not supported",
            )
        ):
            return SourceHaProbe(
                dialect="sqlserver",
                role="STANDALONE",
                ha_enabled=False,
                topology="none",
                message=f"AG DMVs unavailable — treating as non-AG ({exc.__class__.__name__}).",
                details={"error": str(exc)[:300]},
            )
        return SourceHaProbe(
            dialect="sqlserver",
            role="UNKNOWN",
            ha_enabled=False,
            topology="unknown",
            message=f"AG probe failed: {exc}",
            details={"error": str(exc)[:300]},
        )


def probe_oracle_ha(conn: Any) -> SourceHaProbe:
    """Run ``v$database`` role query on an open SQLAlchemy connection."""
    import sqlalchemy as sa

    try:
        result = conn.execute(sa.text(_ORACLE_DG_SQL))
        row = result.mappings().first()
        return classify_oracle_dg_row(dict(row) if row else None)
    except Exception as exc:
        return SourceHaProbe(
            dialect="oracle",
            role="UNKNOWN",
            ha_enabled=False,
            topology="unknown",
            message=f"Data Guard probe failed: {exc}",
            details={"error": str(exc)[:300]},
        )


def probe_source_ha(cfg: dict[str, Any]) -> SourceHaProbe:
    """Open a real engine from ``cfg`` and probe HA role (sqlserver / oracle)."""
    dialect = str(cfg.get("type") or cfg.get("format") or "").lower()
    if dialect in {"mssql", "sql_server", "microsoft_sql_server", "azure_sql_database"}:
        dialect = "sqlserver"
    if dialect not in {"sqlserver", "oracle"}:
        return SourceHaProbe(
            dialect=dialect or "unknown",
            role="N/A",
            ha_enabled=False,
            topology="none",
            message=f"HA probe not applicable for dialect '{dialect}'.",
        )

    from connectors.generic_sql import _engine

    engine = _engine({**cfg, "type": dialect})
    with engine.connect() as conn:
        if dialect == "sqlserver":
            return probe_sqlserver_ha(conn)
        return probe_oracle_ha(conn)


def probe_source_ha_safe(cfg: dict[str, Any]) -> SourceHaProbe:
    """Never raises — returns UNKNOWN on connection failure."""
    try:
        return probe_source_ha(cfg)
    except Exception as exc:
        dialect = str(cfg.get("type") or cfg.get("format") or "unknown").lower()
        return SourceHaProbe(
            dialect=dialect,
            role="UNKNOWN",
            ha_enabled=False,
            topology="unknown",
            message=f"HA probe connection failed: {exc}",
            details={"error": str(exc)[:300]},
        )


def attach_source_ha(cdc: Any, src_cfg: dict[str, Any] | None) -> SourceHaProbe | None:
    """Probe once and stash on the CDC adapter for lag/checkpoint fields."""
    if not src_cfg:
        return None
    dialect = str(src_cfg.get("type") or "").lower()
    if dialect not in {
        "sqlserver",
        "mssql",
        "oracle",
        "sql_server",
        "microsoft_sql_server",
        "azure_sql_database",
    }:
        return None
    probe = probe_source_ha_safe(src_cfg)
    try:
        setattr(cdc, "_source_ha", probe)
    except Exception:
        pass
    return probe
