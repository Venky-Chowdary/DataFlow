"""Guard: route success tokens must never look like introspection errors."""

from src.transfer.registry import validate_transfer


def test_validate_transfer_success_message_is_human_readable():
    ok, msg = validate_transfer("database", "mongodb", "database", "mongodb")
    assert ok is True
    assert msg.lower() != "supported"
    assert msg.lower().startswith("live route:")


def test_validate_transfer_rejects_unknown_combo():
    ok, msg = validate_transfer("database", "mongodb", "database", "not_a_real_dest_xyz")
    assert ok is False
    assert "not yet live" in msg.lower() or "not" in msg.lower()
