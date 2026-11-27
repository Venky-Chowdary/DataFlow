"""Review 2 items 5 and 7: the previous release's type rules match only its own stamps.

Item 5: against a CURRENT stamp, a Map edited from BOOLEAN to BIT(1) (or
BIT(64) to varbit) passed because the fallback rebuilt it under the previous
release's rules, which spell both the same. Only a stamp in a previous-release
format is matched under those rules; the DDL fingerprint (which has no format
of its own) is matched under them only when a Decision Artifact stamp is
enforced alongside it.

Item 7: a stored artifact PAYLOAD the previous release stamped (a Studio
request retried after the upgrade) for bit(1) / varbit -> MySQL was refused
"DDL identity diverged"; it is now rebuilt under that release's rules like a
schedule's stamp.
"""

from __future__ import annotations

import pytest

from services.conversion_contract import (
    DdlIdentityError,
    approved_mapping_ddl_fingerprint,
    assert_ddl_identity,
)
from services.decision_kernel.execute_gate import (
    build_artifact_from_mappings,
    enforce_decision_artifact,
)
from services.decision_kernel.legacy_rules import legacy_plan_rules
from services.decision_kernel.models import base_content_hash, compute_content_hash


def _maps(target_type: str, source_type: str) -> list[dict]:
    return [
        {"source": "id", "target": "id", "source_type": "integer", "target_type": "integer",
         "confidence": 0.95, "create_new": True},
        {"source": "c", "target": "c", "source_type": source_type, "target_type": target_type,
         "confidence": 0.9, "create_new": True},
    ]


def _artifact(maps: list[dict]):
    return build_artifact_from_mappings(
        maps, dest_db="mysql", source_db="postgresql", source_fingerprint="", dest_fingerprint="",
        sync_mode="incremental_append", error_policy="quarantine", route_id="validate:mysql",
        tenant_id="anonymous", artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )


@pytest.mark.parametrize("stamped, edited, source", [
    ("BOOLEAN", "BIT(1)", "bit(1)"),
    ("BIT(64)", "bit varying", "bit varying"),
])
def test_a_current_stamp_refuses_an_edit_the_old_rules_would_spell_the_same(stamped, edited, source):
    stamp = _artifact(_maps(stamped, source)).content_hash
    err, art = enforce_decision_artifact(
        mappings=_maps(edited, source), dest_db="mysql", source_db="postgresql",
        approved_content_hash=stamp, source_fingerprint="", dest_fingerprint="",
        destination_table_exists=False, sync_mode="incremental_append", error_policy="quarantine",
    )
    assert err and art is None
    ddl = approved_mapping_ddl_fingerprint(_maps(stamped, source), dest_db="mysql")
    with pytest.raises(DdlIdentityError):
        assert_ddl_identity(ddl, _maps(edited, source), dest_db="mysql")


@pytest.mark.parametrize("source", ["bit(1)", "bit varying"])
def test_a_previous_release_payload_retried_after_the_upgrade_runs(source):
    maps = _maps(source, source)
    with legacy_plan_rules():
        old = _artifact(maps)
    payload = {**old.to_dict(), "content_hash": base_content_hash(old.to_dict())}
    err, art = enforce_decision_artifact(
        mappings=maps, dest_db="mysql", source_db="postgresql",
        approved_content_hash=payload["content_hash"], artifact_payload=payload,
        source_fingerprint="", dest_fingerprint="", destination_table_exists=False,
        sync_mode="incremental_append", error_policy="quarantine", route_id="validate:mysql",
    )
    assert err is None, err
    assert art is not None
    # The same payload in the current format is not rebuilt under old rules.
    current_fmt = {**old.to_dict(), "content_hash": compute_content_hash(old.to_dict())}
    err2, _ = enforce_decision_artifact(
        mappings=maps, dest_db="mysql", source_db="postgresql",
        approved_content_hash=current_fmt["content_hash"], artifact_payload=current_fmt,
        source_fingerprint="", dest_fingerprint="", destination_table_exists=False,
        sync_mode="incremental_append", error_policy="quarantine", route_id="validate:mysql",
    )
    assert err2 and "DDL identity diverged" in err2
