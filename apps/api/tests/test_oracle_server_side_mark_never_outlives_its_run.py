"""A same-instance Oracle copy's server-side mark lives only inside its run.

``mark_server_side_oracle_copy`` set a ContextVar with no reset. A copy that
ran outside a run's ``server_side_copy_scope`` (the live copy tests call
``copy_oracle_to_oracle`` directly) left it True on the thread, so every later
grade on that thread treated an Oracle offset/instant pair as written server
side and asked for no Risk Contract (oracle-live:
test_grade_follows_the_session_read_back[oracle-...] and
test_server_side_copy_counts_only_once_committed failed after
test_oracle_oracle_copy's live legs). A worker thread reused for the next run
would carry it the same way.
"""

from __future__ import annotations

import contextvars

from services.oracle_session_scope import (
    bind_oracle_utc_pin,
    clear_server_side_oracle_copy,
    mark_server_side_oracle_copy,
    oracle_same_instance,
    server_side_copy_scope,
)
from services.source_engine_scope import bind_source_engine
from services.type_coercion import is_lossy_coercion

_SAME = {"host": "db1", "port": 1521, "service_name": "FREEPDB1"}
_ZONE = "TIMESTAMP(6) WITH TIME ZONE"


def _later_grade_on_the_same_thread() -> tuple[bool, bool]:
    with bind_source_engine("oracle"), bind_oracle_utc_pin(False, "not checked"):
        return oracle_same_instance(), is_lossy_coercion(_ZONE, _ZONE, dest_db="oracle")


def test_a_copy_outside_a_run_leaves_no_mark_for_the_next_grade() -> None:
    def scenario() -> tuple[bool, bool]:
        assert mark_server_side_oracle_copy(_SAME, dict(_SAME)) is True  # same instance
        return _later_grade_on_the_same_thread()

    same, lossy = contextvars.copy_context().run(scenario)
    assert same is False and lossy is True


def test_inside_a_run_the_mark_counts_and_ends_with_the_run() -> None:
    def scenario() -> tuple[bool, bool, bool]:
        with server_side_copy_scope():
            mark_server_side_oracle_copy(_SAME, dict(_SAME))
            inside = oracle_same_instance()
        after_same, after_lossy = _later_grade_on_the_same_thread()
        return inside, after_same, after_lossy

    assert contextvars.copy_context().run(scenario) == (True, False, True)


def test_a_clear_outside_a_run_is_harmless() -> None:
    def scenario() -> bool:
        clear_server_side_oracle_copy()
        with server_side_copy_scope():
            mark_server_side_oracle_copy(_SAME, dict(_SAME))
            clear_server_side_oracle_copy()
            return oracle_same_instance()

    assert contextvars.copy_context().run(scenario) is False
