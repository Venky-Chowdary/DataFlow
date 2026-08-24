"""GA Module C — Execute refuses missing DDL identity after Validate."""

from __future__ import annotations

from src.transfer.engine import _enforce_ddl_identity


def test_enforce_ddl_identity_fails_when_fingerprint_missing_after_validate():
    err = _enforce_ddl_identity(
        {"proof_bundle": {"ddl_identity": {}}},
        [{"source": "a", "target": "a", "target_type": "TEXT"}],
        dest_db="postgresql",
    )
    assert err is not None
    assert "missing" in err.lower() or "refuse" in err.lower()


def test_enforce_ddl_identity_passes_when_hash_matches():
    from services.conversion_contract import approved_mapping_ddl_fingerprint

    maps = [{"source": "a", "target": "a", "target_type": "TEXT"}]
    fp = approved_mapping_ddl_fingerprint(maps, dest_db="postgresql")
    err = _enforce_ddl_identity(
        {"proof_bundle": {"ddl_identity": {"ddl_identity_hash": fp}}},
        maps,
        dest_db="postgresql",
    )
    assert err is None


def test_enforce_ddl_identity_skips_without_preflight_when_no_mappings():
    assert _enforce_ddl_identity(None, [], dest_db="postgresql") is None


def test_enforce_ddl_identity_fails_without_preflight_when_mappings_present():
    """UI Execute without Validate still fails closed (skip_preflight=False)."""
    err = _enforce_ddl_identity(
        None,
        [{"source": "a", "target": "a", "target_type": "TEXT"}],
        dest_db="postgresql",
    )
    assert err is not None
    assert "preflight" in err.lower() or "validate" in err.lower()


def test_enforce_ddl_identity_inline_stamp_when_skip_preflight():
    """API/CLI/scheduler: skip_preflight stamps fingerprint inline (audit §2.2)."""
    err = _enforce_ddl_identity(
        None,
        [{"source": "a", "target": "a", "target_type": "TEXT"}],
        dest_db="postgresql",
        skip_preflight=True,
    )
    assert err is None


def test_enforce_ddl_identity_accepts_stamped_hash_without_preflight():
    from services.conversion_contract import approved_mapping_ddl_fingerprint

    maps = [{"source": "a", "target": "a", "target_type": "TEXT"}]
    fp = approved_mapping_ddl_fingerprint(maps, dest_db="sqlite")
    err = _enforce_ddl_identity(
        None,
        maps,
        dest_db="sqlite",
        approved_ddl_identity_hash=fp,
    )
    assert err is None
