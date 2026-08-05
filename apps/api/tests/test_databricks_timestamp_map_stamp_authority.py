"""Map≡CREATE — Databricks TIMESTAMP stamp authority (Bugbot high).

Databricks TIMESTAMP is session-TZ aware. Rejecting it and rematerializing via
ddl_type invents TIMESTAMP_NTZ (wall-clock), silently flipping Map polarity.
"""

from __future__ import annotations

import sqlalchemy as sa

from connectors.generic_sql import _sa_type_for_logical
from services.type_system import materialize_dest_ddl


def test_databricks_map_timestamp_stamp_not_rewritten_to_ntz():
    """Explicit Map TIMESTAMP must CREATE as TIMESTAMP — never TIMESTAMP_NTZ."""
    assert materialize_dest_ddl("databricks", "TIMESTAMP") == "TIMESTAMP"
    # Wall-clock sources still land on TIMESTAMP_NTZ via create-new / foreign aliases.
    assert materialize_dest_ddl("databricks", "DATETIME2") == "TIMESTAMP_NTZ"
    assert materialize_dest_ddl("databricks", "TIMESTAMP_NTZ") == "TIMESTAMP_NTZ"


def test_databricks_timestamptz_rematerializes_to_session_timestamp():
    """No TIMESTAMPTZ type name — session TIMESTAMP is the aware sink."""
    assert materialize_dest_ddl("databricks", "TIMESTAMPTZ") == "TIMESTAMP"
    assert materialize_dest_ddl("databricks", "TIMESTAMP_LTZ") == "TIMESTAMP"


def test_databricks_timestamp_sa_is_timezone_aware():
    wire = materialize_dest_ddl("databricks", "TIMESTAMP")
    sa_t = _sa_type_for_logical(wire, "databricks", "databricks")
    assert isinstance(sa_t, sa.DateTime)
    assert sa_t.timezone is True


def test_databricks_timestamp_ntz_sa_is_naive():
    wire = materialize_dest_ddl("databricks", "TIMESTAMP_NTZ")
    sa_t = _sa_type_for_logical(wire, "databricks", "databricks")
    assert isinstance(sa_t, sa.DateTime)
    assert sa_t.timezone is False


def test_generic_sql_explicit_databricks_timestamp_stamp_honored():
    """Writer path: explicit target_type TIMESTAMP must survive materialize."""
    explicit = "TIMESTAMP"
    derived = materialize_dest_ddl("databricks", explicit)
    assert derived == "TIMESTAMP"
    # Dest-native TIMESTAMP on Databricks is session-TZ; SA bind must be aware.
    sa_t = _sa_type_for_logical(derived, "databricks", "databricks")
    assert sa_t.timezone is True
