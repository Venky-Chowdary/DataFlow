"""Follow-up review 5 (L2): the flat-root assignment command checks the workspace and labels claims.

``scripts/iceberg_assign_flat_root.py --assign`` accepted any workspace id (a
typo assigned legacy data to nobody, or to a workspace created later with that
id), and ``--list`` printed the workspaces of every saved connector naming an
entry as plain "named_by" hints, so a connector workspace B saved to name A's
legacy folder looked like a claim as good as A's own.

Now ``--assign`` refuses a workspace that does not exist; ``--list`` shows
each claiming connector with its workspace, and a claim from a workspace other
than the entry's owner (the assigned one) is marked unverified. ``--assign``
also prints the claims from other workspaces as unverified.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

WS_A = "ws-alpha"
WS_B = "ws-bravo"


@pytest.fixture
def root(tmp_path, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ICEBERG_ROOT", "CONFINE_LOCAL_FILES", "REQUIRE_AUTH"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    jail = tmp_path / "iceroot"
    (jail / "legacy").mkdir(parents=True)
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(jail))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr("services.team_store.get_workspace",
                        lambda ws: SimpleNamespace(id=ws) if ws in {WS_A, WS_B} else None)
    connectors = [
        SimpleNamespace(id="conn-a", type="iceberg", workspace_id=WS_A, connection_string="", warehouse="legacy",
                        database="", region="", extra={}),
        SimpleNamespace(id="conn-b", type="iceberg", workspace_id=WS_B, connection_string="", warehouse="legacy",
                        database="", region="", extra={}),
    ]
    monkeypatch.setattr("services.connector_store.list_connectors", lambda *a, **k: connectors)
    return jail.resolve()


def test_assign_refuses_an_unknown_workspace(root, capsys):
    from scripts.iceberg_assign_flat_root import main

    assert main(["--assign", "legacy", "--workspace", "ws-typo"]) != 0
    assert "does not exist" in capsys.readouterr().err
    from services.iceberg_workspace_root import flat_owners

    assert flat_owners(root) == {}


def test_list_shows_each_claim_with_its_workspace_and_marks_others_unverified(root, capsys):
    from scripts.iceberg_assign_flat_root import main

    assert main(["--list"]) == 0
    out = capsys.readouterr().out
    assert f"conn-a@{WS_A}" in out and f"conn-b@{WS_B}" in out
    assert main(["--assign", "legacy", "--workspace", WS_A]) == 0
    assigned = capsys.readouterr().out
    assert f"conn-b@{WS_B} (unverified" in assigned
    assert main(["--list"]) == 0
    out = capsys.readouterr().out
    assert f"conn-a@{WS_A}" in out and f"conn-a@{WS_A} (unverified" not in out
    assert f"conn-b@{WS_B} (unverified" in out
