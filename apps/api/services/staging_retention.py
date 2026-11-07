"""Retention of the pre-ingestion stage (``<table>_df_staging``).

QA round 11 (M10): every ``write_via_staging`` run left its stage in the
customer's schema; it was only ever dropped by the *next* run of the same
target. The rule now (see ``services.pre_ingestion_staging``):

* a clean promote drops the stage at once;
* a stage is kept only when the promote was blocked or rows were held back —
  the run says so — and it is stamped with the time it was kept;
* a kept stage older than :data:`STAGING_RETENTION_TTL_SECONDS` is dropped by
  the next staging run into that schema (:func:`reap_expired_stages`).

The stamp is a table comment, so it needs no shared state and survives an API
restart. An engine that cannot carry a comment keeps its stage until that
target's next run replaces it (the behaviour before this rule).
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any

logger = logging.getLogger(__name__)

#: How long a kept stage stays for inspection before the reaper drops it.
STAGING_RETENTION_TTL_SECONDS = 7 * 24 * 3600

_STAMP = "staging-retained-at:"
_STAMP_RE = re.compile(r"staging-retained-at:(\d{9,11})\b")


def retained_comment(now: float | None = None) -> str:
    from services.brand import PRODUCT_NAME

    return f"{PRODUCT_NAME.lower()} {_STAMP}{int(now if now is not None else time.time())}"


def retained_at(comment: Any) -> int | None:
    match = _STAMP_RE.search("" if comment is None else str(comment))
    return int(match.group(1)) if match else None


def _engine_for(destination: Any) -> tuple[Any, str, str]:
    from connectors.generic_sql import get_sql_schema, get_sqlalchemy_engine
    from src.transfer.adapters import resolve_connector_config

    cfg = resolve_connector_config(destination)
    engine = get_sqlalchemy_engine(cfg)
    dialect = str(getattr(getattr(engine, "dialect", None), "name", "") or "")
    return engine, dialect, str(get_sql_schema(cfg) or "")


def stamp_retained(stage_endpoint: Any) -> bool:
    """Stamp a kept stage with the time it was kept. Never fatal."""
    import sqlalchemy as sa

    from services.mirror_engine import _qualified_name
    from services.engine_pool import release_engine

    table = str(getattr(stage_endpoint, "table", "") or "")
    engine = None
    try:
        engine, dialect, schema = _engine_for(stage_endpoint)
        qualified = _qualified_name(table, schema, dialect)
        text = retained_comment()
        if dialect in {"postgresql", "oracle", "snowflake", "redshift"}:
            stmt = sa.text(f"COMMENT ON TABLE {qualified} IS '{text}'")  # nosec B608 - literal stamp
        elif dialect in {"mysql", "mariadb"}:
            stmt = sa.text(f"ALTER TABLE {qualified} COMMENT = '{text}'")  # nosec B608
        else:
            return False
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        return True
    except Exception as exc:  # noqa: BLE001 - retention stamp is best effort
        logger.debug("staging retention stamp skipped for %s: %s", table, exc)
        return False
    finally:
        if engine is not None:
            release_engine(engine)


def reap_expired_stages(destination: Any, suffixes: tuple[str, ...]) -> list[str]:
    """Drop kept stages in the destination's schema whose stamp is past the TTL.

    Runs on its own connection (never the write connection). Unstamped stages
    are left alone: their age is unknowable and a concurrent run may own one.
    """
    import sqlalchemy as sa

    from services.mirror_engine import _qualified_name
    from services.staging_reaper import drop_staging_table
    from services.engine_pool import release_engine

    engine = None
    dropped: list[str] = []
    try:
        engine, dialect, schema = _engine_for(destination)
        inspector = sa.inspect(engine)
        names = [
            n for n in inspector.get_table_names(schema=schema or None)
            if any(str(n).lower().endswith(s.lower()) for s in suffixes)
        ]
        now = time.time()
        with engine.connect() as conn:
            for name in names:
                try:
                    comment = (inspector.get_table_comment(name, schema=schema or None) or {}).get("text")
                except Exception:  # noqa: BLE001 - no comment support: keep it
                    continue
                stamped = retained_at(comment)
                if stamped is None or now - stamped < STAGING_RETENTION_TTL_SECONDS:
                    continue
                drop_staging_table(conn, _qualified_name(name, schema, dialect))
                dropped.append(str(name))
    except Exception as exc:  # noqa: BLE001 - a sweep never fails the transfer
        logger.debug("staging retention sweep skipped: %s", exc)
    finally:
        if engine is not None:
            release_engine(engine)
    if dropped:
        logger.info("reaped %d expired staging table(s): %s", len(dropped), ", ".join(dropped))
    return dropped
