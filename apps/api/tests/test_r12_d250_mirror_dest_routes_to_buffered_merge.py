"""D250: mirror to SQL Server / Oracle passed preflight (pf_1dd60ed5fb44,
pf_02788c333ae6) and the run then failed with "mirror streaming transfer is
currently implemented for SQL destinations; 'sqlserver' is not yet supported",
0 rows. The buffered merge path supports those destinations (live: SQL Server
mirror 30/30, then delete/update/insert converge); only the streaming staging
merge does not. Routing now sends such runs to the buffered path, and both read
one capability set.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest


def _request(dest, sync_mode, *, source="postgresql"):
    return SimpleNamespace(
        source=SimpleNamespace(kind="database", format=source, table="t"),
        destination=SimpleNamespace(kind="database", format=dest, table="t"),
        sync_mode=sync_mode,
        stream_contracts=None,
        priority_column=None,
    )


@pytest.mark.parametrize("dest", ["sqlserver", "oracle"])
@pytest.mark.parametrize("sync_mode", ["mirror", "full_refresh_mirror", "scd2"])
def test_staging_merge_unsupported_destination_takes_buffered_path(sync_mode, dest):
    from src.transfer.engine import takes_streaming_route

    assert takes_streaming_route(_request(dest, sync_mode)) is False


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
def test_staging_merge_destination_keeps_streaming(dest):
    from src.transfer.engine import takes_streaming_route

    assert takes_streaming_route(_request(dest, "mirror")) is True


@pytest.mark.parametrize("sync_mode", ["full_refresh_append", "incremental_upsert"])
def test_other_sync_modes_to_sqlserver_keep_streaming(sync_mode):
    from src.transfer.engine import takes_streaming_route

    assert takes_streaming_route(_request("sqlserver", sync_mode)) is True


def test_contract_sync_mode_decides_the_route():
    from src.transfer.engine import takes_streaming_route

    req = _request("sqlserver", "full_refresh_append")
    req.stream_contracts = [{"stream": "t", "sync_mode": "mirror", "primary_key": ["id"]}]
    assert takes_streaming_route(req) is False


def test_streaming_refusal_names_the_destination_not_sql_destinations():
    from services.multi_stream_plan import staging_merge_dest_refusal
    from src.transfer.stream_scd2 import stream_scd2_mirror_transfer

    src = SimpleNamespace(format="postgresql", table="t", kind="database")
    dest = SimpleNamespace(format="sqlserver", table="t", kind="database")
    with pytest.raises(NotImplementedError) as exc:
        stream_scd2_mirror_transfer(src, dest, [], {}, sync_mode="mirror")
    assert str(exc.value) == staging_merge_dest_refusal("mirror", "sqlserver")
    assert "SQL destinations" not in str(exc.value)
