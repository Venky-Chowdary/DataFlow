from __future__ import annotations

import builtins
import logging
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

import connectors.sdk as sdk
from connectors.sdk import SingerTapBridge, SingerTapError, StreamSchema
from connectors.sdk.http_declarative import DeclarativeHttpConnector


def _fake_tap(tmp_path: Path, *, empty_discover: bool = False) -> SingerTapBridge:
    script = tmp_path / "fake_tap.py"
    script.write_text(
        """
import json
import sys
from pathlib import Path

args = sys.argv[1:]
config = {}
if "--config" in args:
    config = json.loads(Path(args[args.index("--config") + 1]).read_text())
if "--check" in args:
    print("error: unrecognized arguments: --check", file=sys.stderr)
    raise SystemExit(2)
if "--discover" in args:
    if not config.get("empty_discover"):
        print(json.dumps({
            "type": "SCHEMA",
            "stream": "contacts",
            "schema": {"type": "object", "properties": {"id": {"type": "string"}}},
            "key_properties": ["id"],
        }))
    raise SystemExit(0)
print(json.dumps({
    "type": "SCHEMA",
    "stream": "contacts",
    "schema": {"type": "object", "properties": {"id": {"type": "string"}}},
    "key_properties": ["id"],
}))
print(json.dumps({"type": "RECORD", "stream": "contacts", "record": {"id": "1"}}))
""",
        encoding="utf-8",
    )
    return SingerTapBridge(
        {
            "tap_command": [sys.executable, str(script)],
            "tap_config": {"empty_discover": empty_discover, "token": "test-secret"},
        }
    )


def test_stream_schema_sync_mode_invariants() -> None:
    assert StreamSchema(name="contacts").supported_sync_modes == ["full_refresh"]
    with pytest.raises(ValueError, match="unsupported sync mode"):
        StreamSchema(name="contacts", supported_sync_modes=["daily"])
    with pytest.raises(
        ValueError,
        match="Stream 'contacts' advertises incremental sync without a cursor_field",
    ):
        StreamSchema(name="contacts", supported_sync_modes=["full_refresh", "incremental"])
    StreamSchema(
        name="contacts",
        cursor_field="updated_at",
        supported_sync_modes=["incremental"],
    )


def test_declarative_discovery_only_advertises_implemented_modes() -> None:
    connector = DeclarativeHttpConnector(
        {
            "api_key": "test",
            "spec": {
                "base_url": "https://example.test/",
                "streams": [
                    {"name": "full", "path": "full"},
                    {
                        "name": "cursor_without_param",
                        "path": "a",
                        "cursor_field": "updated_at",
                    },
                    {
                        "name": "incremental",
                        "path": "b",
                        "cursor_field": "updated_at",
                        "cursor_param": "since",
                    },
                ],
            },
        }
    )

    modes = {stream.name: stream.supported_sync_modes for stream in connector.discover()}
    assert modes == {
        "full": ["full_refresh"],
        "cursor_without_param": ["full_refresh"],
        "incremental": ["full_refresh", "incremental"],
    }


def test_singer_check_fallback_requires_successful_discovery(tmp_path: Path) -> None:
    bridge = _fake_tap(tmp_path)

    ok, message = bridge.check()

    assert ok is True
    assert message == "Singer tap --discover OK (1 streams); tap has no --check"


def test_singer_check_fallback_rejects_empty_discovery(tmp_path: Path) -> None:
    bridge = _fake_tap(tmp_path, empty_discover=True)

    ok, message = bridge.check()

    assert ok is False
    assert message.startswith("unverified: tap has no --check and --discover produced no streams")
    assert "(exit 0):" in message


@pytest.mark.parametrize(
    ("outcome", "expected_fragment"),
    [
        (
            subprocess.TimeoutExpired(
                cmd=["tap", "--discover", "planted-secret"],
                timeout=120,
            ),
            "timed out",
        ),
        (FileNotFoundError("planted-secret"), "FileNotFoundError"),
        (
            subprocess.CompletedProcess(
                args=["tap", "--discover"],
                returncode=3,
                stdout="",
                stderr=(
                    "planted-secret "
                    "https://api.example.test/items?access_token=planted-secret "
                    + "x" * 800
                ),
            ),
            "status 3",
        ),
        (
            subprocess.CompletedProcess(
                args=["tap", "--discover"],
                returncode=0,
                stdout="not-json planted-secret",
                stderr="",
            ),
            "parseable streams",
        ),
    ],
)
def test_singer_discover_raises_typed_errors(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    outcome: Exception | subprocess.CompletedProcess[str],
    expected_fragment: str,
) -> None:
    secret = "planted-secret"
    bridge = SingerTapBridge(
        {
            "tap_command": ["tap", "--discover"],
            "tap_config": {"api_key": secret},
        }
    )

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(sdk.subprocess, "run", fake_run)
    with caplog.at_level(logging.DEBUG, logger=sdk.__name__):
        with pytest.raises(SingerTapError) as raised:
            bridge.discover()

    assert expected_fragment in str(raised.value)
    assert len(str(raised.value)) <= 600
    assert secret not in str(raised.value)
    assert secret not in caplog.text


def test_builtin_import_failures_are_warnings_without_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "loader-planted-secret"
    original_import = builtins.__import__

    def fail_declarative_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "connectors.sdk" and "http_declarative" in fromlist:
            raise ImportError(secret)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fail_declarative_import)
    with caplog.at_level(logging.WARNING, logger=sdk.__name__):
        sdk._load_builtin_connectors()

    messages = [record.getMessage() for record in caplog.records]
    warning_messages = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
    ]
    assert any(
        "connectors.sdk.http_declarative" in message and "ImportError" in message
        for message in warning_messages
    )
    assert secret not in "\n".join(messages)


def test_singer_config_and_state_files_are_removed_after_subprocesses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    bridge = _fake_tap(tmp_path)

    assert bridge.check()[0] is True
    assert list(tmp_path.glob("*.json")) == []
    assert [stream.name for stream in bridge.discover()] == ["contacts"]
    assert list(tmp_path.glob("*.json")) == []
    batches = list(bridge.read("contacts", state={"bookmark": "old"}))
    assert batches[0].records == [{"id": "1"}]
    assert list(tmp_path.glob("*.json")) == []
