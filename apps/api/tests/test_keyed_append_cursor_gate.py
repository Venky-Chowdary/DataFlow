"""Validate warns: append into Redis / DynamoDB with an incremental cursor.

Review M3. Append into Redis or DynamoDB no longer overwrites a stored key
(``SET NX`` / conditional ``PutItem``): a changed row an incremental cursor
re-reads is now a rejected row instead of a silent overwrite. Validate says
so before the run and suggests upsert.
"""

from __future__ import annotations

import pytest

from services.duplicate_policy import keyed_append_cursor_gate


@pytest.mark.parametrize("dest", ["redis", "dynamodb"])
def test_append_with_cursor_into_keyed_store_warns(dest):
    gate = keyed_append_cursor_gate(
        dest, sync_mode="incremental_append", duplicate_policy="", cursor_column="updated_at"
    )
    assert gate is not None
    assert gate["status"] == "warn"
    assert "upsert" in gate["message"].lower() and "rejected" in gate["message"].lower()
    assert gate["details"]["cursor_column"] == "updated_at"


@pytest.mark.parametrize(
    ("dest", "sync_mode", "policy", "cursor"),
    [
        ("redis", "incremental_append", "", ""),  # no cursor: nothing is re-read
        ("redis", "incremental_upsert", "", "updated_at"),  # upsert updates the key
        ("dynamodb", "incremental_append", "upsert", "updated_at"),
        ("dynamodb", "incremental_append", "skip_duplicates", "updated_at"),
        ("redis", "full_refresh_overwrite", "", "updated_at"),
        ("postgresql", "incremental_append", "", "updated_at"),
    ],
)
def test_no_warning_when_changed_keys_are_not_rejected(dest, sync_mode, policy, cursor):
    assert keyed_append_cursor_gate(
        dest, sync_mode=sync_mode, duplicate_policy=policy, cursor_column=cursor
    ) is None


def test_validate_wires_the_warning():
    import inspect

    import services.preflight_service as ps

    assert "keyed_append_cursor_gate(" in inspect.getsource(ps)
