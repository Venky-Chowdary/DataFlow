"""D254: the SQL Server CDC snapshot handoff covers commits capture has not harvested.

``fn_cdc_get_max_lsn`` only reaches what the capture agent has scanned. On QAENV
the agent was 25 minutes behind, so the first run started below two commits the
snapshot already held and replayed their older images over it.
"""

from __future__ import annotations

from connectors.sqlserver_cdc_native import SqlServerNativeCdc
try:
    from connectors.sqlserver_cdc_native import _log_stats_lsn_to_hex
except ImportError:
    _log_stats_lsn_to_hex = lambda t: None

CFG = {"host": "localhost", "database": "dataflow", "username": "sa", "password": "x"}
CAPTURED_MAX = "0000002f000014c00003"
CAPTURE_START = "0000002e000060bc0084"


class _Cursor:
    def __init__(self, log_end: str | None, *, refuse_log_stats: bool = False) -> None:
        self.log_end = log_end
        self.refuse = refuse_log_stats
        self._row: tuple | None = None

    def execute(self, query: str, params: tuple | None = None) -> None:
        if "dm_db_log_stats" in query:
            if self.refuse:
                raise PermissionError("VIEW DATABASE STATE permission denied")
            self._row = (self.log_end,)
        elif "fn_cdc_get_max_lsn" in query:
            self._row = (CAPTURED_MAX,)
        elif "fn_cdc_get_min_lsn" in query or "cdc.change_tables" in query:
            self._row = (CAPTURE_START,)
        else:
            raise AssertionError(query)

    def fetchone(self):
        return self._row


def _reader() -> SqlServerNativeCdc:
    return SqlServerNativeCdc(CFG, table="qaenv_seed", primary_key="id", schema="dbo")


def test_handoff_starts_after_commits_the_capture_agent_has_not_harvested() -> None:
    # 14C1 / 14C3 were committed (log end 14C3:0003) but not yet in the CT table.
    cur = _Cursor("0000002F:000014C3:0003")
    assert _reader()._snapshot_handoff_lsn(cur, "dbo_qaenv_seed") == "0000002f000014c30003"


def test_handoff_keeps_captured_max_when_the_log_end_is_not_newer() -> None:
    cur = _Cursor("0000002F:000014C0:0001")
    assert _reader()._snapshot_handoff_lsn(cur, "dbo_qaenv_seed") == CAPTURED_MAX


def test_handoff_falls_back_to_captured_max_when_log_stats_is_refused() -> None:
    cur = _Cursor(None, refuse_log_stats=True)
    assert _reader()._snapshot_handoff_lsn(cur, "dbo_qaenv_seed") == CAPTURED_MAX


def test_log_stats_lsn_matches_the_binary_lsn_layout() -> None:
    assert _log_stats_lsn_to_hex("00000041:00007ED0:0001") == "0000004100007ed00001"
    assert _log_stats_lsn_to_hex("41:7ed0:1") == "0000004100007ed00001"
    assert _log_stats_lsn_to_hex("") == ""
    assert _log_stats_lsn_to_hex("not-an-lsn") == ""
