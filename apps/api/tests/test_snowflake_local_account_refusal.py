"""A "local" Snowflake account without fakesnow is refused cleanly (review 9).

``local`` / ``localhost`` / ``fakesnow`` accounts are the fakesnow emulator, a
test tool the production image does not ship (requirements-dev.txt). The login
path did a bare ``import fakesnow``, so on a production server such an account
surfaced a raw ``ModuleNotFoundError``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

API = Path(__file__).resolve().parents[1]


@pytest.fixture
def no_fakesnow(monkeypatch):
    for name in [m for m in sys.modules if m == "fakesnow" or m.startswith("fakesnow.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setitem(sys.modules, "fakesnow", None)  # import raises ImportError


def _login(account: str):
    pytest.importorskip("snowflake.connector")
    from connectors import snowflake_conn

    return snowflake_conn._login(
        account=account, username="u", password="p", database="D", schema="S",
        warehouse="", connection_string="", role="", private_key="",
        private_key_passphrase="", auth_mode="",
    )


@pytest.mark.parametrize("account", ["local", "localhost", "FakeSnow"])
def test_a_local_account_without_fakesnow_is_refused_with_a_clear_message(account, no_fakesnow):
    with pytest.raises(RuntimeError) as info:
        _login(account)
    text = str(info.value)
    assert "local Snowflake emulator (fakesnow)" in text
    assert "not installed on this server" in text
    assert "account identifier" in text and "No connection was attempted" in text
    assert "Traceback" not in text and "/" not in text


def test_the_connection_test_reports_the_refusal(no_fakesnow):
    pytest.importorskip("snowflake.connector")
    from connectors.snowflake import test_snowflake

    result = test_snowflake(
        host="local", port=443, database="D", username="u", password="p",
        schema="S", connection_string="", ssl=True,
    )
    assert result.ok is False
    assert "local Snowflake emulator (fakesnow)" in (result.error or "")


def test_the_requirements_comment_says_what_happens_without_fakesnow():
    text = (API / "requirements.txt").read_text(encoding="utf-8")
    block = text[text.index("# Local emulators (moto, fakesnow)"):text.index("sqlglot~=")]
    assert "refuses" in block and "local" in block
    assert 'report "unavailable" without them' not in block
