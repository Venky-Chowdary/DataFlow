from __future__ import annotations

import io
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from connectors.sdk import (
    BaseConnector,
    ConnectorDescriptor,
    SingerTapError,
    get_descriptor,
)
from connectors.sdk.declarative.errors import ConnectorAuthError, SchemaDriftError
from connectors.sdk.declarative.incremental import run_sync
from connectors.sdk.declarative.schema import detect_stream_drift
from tests.connector_certification.fixture_server import FixtureServer

StepStatus = Literal["pass", "fail", "skip"]
ConnectorFactory = Callable[
    [str, str, Sequence[Mapping[str, Any]], Callable[[float], None] | None],
    BaseConnector,
]
FixtureRoutes = Callable[
    [FixtureServer, str, Sequence[Mapping[str, Any]]],
    None,
]
FixtureMutation = Callable[
    [Sequence[Mapping[str, Any]]],
    tuple[list[dict[str, Any]], set[str], set[str]],
]


@dataclass(frozen=True)
class CertificationCase:
    connector_factory: ConnectorFactory
    fixture_routes: FixtureRoutes
    stream: str
    mutate_fixture: FixtureMutation
    connector_id: str
    fixture_records: tuple[Mapping[str, Any], ...]
    primary_key: tuple[str, ...]
    cursor_field: str
    secret: str
    unsupported_steps: frozenset[str] = frozenset()
    assert_incremental_cursor: Callable[
        [FixtureServer, Mapping[str, Any]], None
    ] | None = None
    sync_runner: Callable[..., int] = run_sync


@dataclass(frozen=True)
class CertificationStep:
    name: str
    status: StepStatus
    reason: str


@dataclass
class CertificationReport:
    connector_id: str
    evidence: str
    steps: list[CertificationStep] = field(default_factory=list)

    @property
    def failed_steps(self) -> list[CertificationStep]:
        return [step for step in self.steps if step.status == "fail"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "connector_id": self.connector_id,
            "evidence": self.evidence,
            "steps": [
                {"name": step.name, "status": step.status, "reason": step.reason}
                for step in self.steps
            ],
        }


def _make_certification_connector(
    case: CertificationCase,
    base_url: str,
    phase: str,
    records: Sequence[Mapping[str, Any]],
    sleep: Callable[[float], None] | None = None,
) -> BaseConnector:
    connector = case.connector_factory(base_url, phase, records, sleep)
    requester = getattr(connector, "requester", None)
    if requester is not None and sleep is not None:
        requester.sleep = sleep
    return connector


def _state_cursor(state: Mapping[str, Any], cursor_field: str) -> Any:
    if cursor_field in state:
        return state[cursor_field]
    if "cursor" in state:
        return state["cursor"]
    for value in state.values():
        if isinstance(value, Mapping):
            found = _state_cursor(value, cursor_field)
            if found is not None:
                return found
    return None


def certify(case: CertificationCase) -> CertificationReport:
    descriptor: ConnectorDescriptor | None = get_descriptor(case.connector_id)
    report = CertificationReport(
        connector_id=case.connector_id,
        evidence=descriptor.evidence if descriptor is not None else "unknown",
    )
    if descriptor is None:
        report.steps.append(
            CertificationStep("registry", "fail", "registered connector has no descriptor")
        )
        return report

    def factory(
        base_url: str,
        phase: str,
        records: Sequence[Mapping[str, Any]] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> BaseConnector:
        return _make_certification_connector(
            case,
            base_url,
            phase,
            records if records is not None else case.fixture_records,
            sleep,
        )

    def run_step(name: str, action: Callable[[], str | None]) -> None:
        if name in case.unsupported_steps:
            reason = descriptor.certification_skips.get(name)
            if not reason or not reason.strip():
                report.steps.append(
                    CertificationStep(name, "fail", "unlisted or empty descriptor skip")
                )
            else:
                report.steps.append(CertificationStep(name, "skip", reason))
            return
        try:
            reason = action() or f"{name} completed"
            if name in descriptor.certification_skips:
                report.steps.append(
                    CertificationStep(
                        name,
                        "fail",
                        f"stale descriptor skip: {descriptor.certification_skips[name]}",
                    )
                )
            else:
                report.steps.append(CertificationStep(name, "pass", reason))
        except Exception as exc:
            message = str(exc).replace(case.secret, "[REDACTED]")
            report.steps.append(
                CertificationStep(name, "fail", f"{type(exc).__name__}: {message}")
            )

    def spec_step() -> str:
        spec = factory("", "spec").spec()
        json.dumps(spec, allow_nan=False)
        connection = spec.get("connectionSpecification")
        if not isinstance(connection, dict):
            raise AssertionError("spec must contain connectionSpecification")
        properties = connection.get("properties")
        if not isinstance(properties, dict):
            raise AssertionError("connectionSpecification must contain properties")
        for key in connection.get("required", []):
            if key not in properties:
                raise AssertionError(f"required spec property {key!r} is missing")
        return "JSON-serializable connection specification with required keys"

    run_step("spec", spec_step)

    def check_step() -> str:
        with FixtureServer() as fixture:
            case.fixture_routes(fixture, "check", case.fixture_records)
            ok, message = factory(fixture.base_url, "check").check()
        if not ok:
            raise AssertionError(f"fixture connection check failed: {message}")
        return "fixture-backed check returned true"

    run_step("check", check_step)

    def auth_step() -> str:
        capture = io.StringIO()
        handler = logging.StreamHandler(capture)
        root = logging.getLogger()
        previous_level = root.level
        root.setLevel(logging.DEBUG)
        root.addHandler(handler)
        try:
            with FixtureServer() as fixture:
                case.fixture_routes(fixture, "auth", case.fixture_records)
                connector = factory(fixture.base_url, "auth")
                ok, check_message = connector.check()
                if ok:
                    raise AssertionError("bad credentials unexpectedly passed check()")
                if case.secret in str(check_message):
                    raise AssertionError("check() exposed the planted secret")
                try:
                    list(connector.read(case.stream))
                except (ConnectorAuthError, SingerTapError) as exc:
                    if case.secret in str(exc):
                        raise AssertionError("read() exposed the planted secret")
                else:
                    raise AssertionError("read() did not raise a typed auth error")
            if case.secret in capture.getvalue():
                raise AssertionError("authentication failure logged the planted secret")
            if case.connector_id == "singer_tap":
                return "typed error; auth vs other not distinguishable from tap exit"
            return "check() failed safely and read() raised a typed auth error"
        finally:
            root.removeHandler(handler)
            root.setLevel(previous_level)
            handler.close()

    run_step("bad_credentials", auth_step)

    def discover_step() -> str:
        streams = factory("", "discover").discover()
        stream_schema = next((item for item in streams if item.name == case.stream), None)
        if stream_schema is None:
            raise AssertionError(f"discover() omitted stream {case.stream!r}")
        properties = stream_schema.json_schema.get("properties", {})
        for key in (*case.primary_key, case.cursor_field):
            if key and key not in properties:
                raise AssertionError(f"discovered schema omitted required field {key!r}")
        discovered_modes = {
            mode for item in streams for mode in item.supported_sync_modes
        }
        if discovered_modes != set(descriptor.sync_modes):
            raise AssertionError(
                f"descriptor sync modes {descriptor.sync_modes!r} differ from discovery "
                f"{sorted(discovered_modes)!r}"
            )
        return "stream, primary key, cursor, and sync modes match discovery"

    run_step("discover", discover_step)

    full_state: dict[str, Any] = {}

    def full_read_step() -> str:
        nonlocal full_state
        with FixtureServer() as fixture:
            case.fixture_routes(fixture, "full_read", case.fixture_records)
            batches = list(factory(fixture.base_url, "full_read").read(case.stream))
        records = [record for batch in batches for record in batch.records]
        full_state = dict(batches[-1].state) if batches else {}
        if len(records) != len(case.fixture_records):
            raise AssertionError(
                f"expected {len(case.fixture_records)} fixture rows, got {len(records)}"
            )
        keys = [tuple(record.get(key) for key in case.primary_key) for record in records]
        if len(keys) != len(set(keys)):
            raise AssertionError("full read returned duplicate primary keys")
        expected = {
            json.dumps(dict(record), sort_keys=True, allow_nan=False)
            for record in case.fixture_records
        }
        actual = {
            json.dumps(record, sort_keys=True, allow_nan=False) for record in records
        }
        if actual != expected:
            raise AssertionError("full read rows do not match the fixture population")
        return f"read all {len(records)} unique fixture rows"

    run_step("full_read", full_read_step)

    def incremental_step() -> str:
        if "incremental" not in descriptor.sync_modes:
            raise AssertionError("incremental is absent without an authorized skip")
        mutated, changed_ids, boundary_ids = case.mutate_fixture(case.fixture_records)
        with FixtureServer() as fixture:
            case.fixture_routes(fixture, "incremental", mutated)
            batches = list(
                factory(fixture.base_url, "incremental", mutated).read(
                    case.stream,
                    state=full_state,
                )
            )
            if case.assert_incremental_cursor is not None:
                case.assert_incremental_cursor(fixture, full_state)
        records = [record for batch in batches for record in batch.records]
        ids = {str(record.get(case.primary_key[0])) for record in records}
        if not changed_ids <= ids:
            raise AssertionError(
                f"incremental read omitted changed rows: {sorted(changed_ids - ids)}"
            )
        if not ids <= changed_ids | boundary_ids:
            raise AssertionError("incremental read returned rows outside changes/boundary peers")
        end_state = batches[-1].state if batches else full_state
        old_cursor = _state_cursor(full_state, case.cursor_field)
        new_cursor = _state_cursor(end_state, case.cursor_field)
        if old_cursor is not None and new_cursor is not None and str(new_cursor) < str(old_cursor):
            raise AssertionError("incremental cursor regressed")
        return "incremental rows are bounded to changes/boundary peers; cursor is monotonic"

    run_step("incremental_read", incremental_step)

    def resume_step() -> str:
        events: list[tuple[str, int]] = []
        written: list[dict[str, Any]] = []
        states: list[dict[str, Any]] = []

        def write(records: list[dict[str, Any]]) -> None:
            events.append(("write", len(records)))
            written.extend(records)

        def load_state(_stream: str) -> Mapping[str, Any] | None:
            return states[-1] if states else None

        def save_state(_stream: str, state: dict[str, Any]) -> None:
            events.append(("save", len(written)))
            states.append(state)

        with FixtureServer() as fixture:
            case.fixture_routes(fixture, "fault", case.fixture_records)
            broken_source = factory(fixture.base_url, "fault")
            failed = False
            try:
                case.sync_runner(
                    broken_source, case.stream, write, load_state, save_state
                )
            except Exception:
                failed = True
            if not failed:
                raise AssertionError("injected page-one fault did not fail the first sync")
            if not states or not any(kind == "write" for kind, _ in events):
                raise AssertionError("fault occurred before a durable page checkpoint")
            if any(
                kind == "save" and (index == 0 or events[index - 1][0] != "write")
                for index, (kind, _count) in enumerate(events)
            ):
                raise AssertionError("checkpoint state was saved before its page was written")
            case.fixture_routes(fixture, "resume", case.fixture_records)
            case.sync_runner(
                factory(fixture.base_url, "resume"),
                case.stream,
                write,
                load_state,
                save_state,
            )
        ids = {str(record.get(case.primary_key[0])) for record in written}
        expected_ids = {
            str(record.get(case.primary_key[0])) for record in case.fixture_records
        }
        if ids != expected_ids:
            raise AssertionError(
                f"restart population mismatch: expected {sorted(expected_ids)}, got {sorted(ids)}"
            )
        return "restart recovered the population; every state save followed a write"

    run_step("resume_after_failure", resume_step)

    def rate_limit_step() -> str:
        slept: list[float] = []

        def fake_sleep(seconds: float) -> None:
            slept.append(seconds)

        with FixtureServer() as fixture:
            case.fixture_routes(fixture, "rate_limit", case.fixture_records)
            batches = list(
                factory(
                    fixture.base_url,
                    "rate_limit",
                    case.fixture_records,
                    fake_sleep,
                ).read(case.stream)
            )
            if len(fixture.request_log) < 2:
                raise AssertionError("429 response was not retried")
        records = [record for batch in batches for record in batch.records]
        if len(records) != len(case.fixture_records):
            raise AssertionError("read did not complete after the rate-limit retry")
        if not slept or max(slept) < 2:
            raise AssertionError("fake sleep did not honor Retry-After")
        return f"429 retried; fake sleep observed {max(slept):g}s"

    run_step("rate_limit", rate_limit_step)

    def schema_drift_step() -> str:
        schema = next(
            item for item in factory("", "schema_drift").discover()
            if item.name == case.stream
        )
        old_schema = schema.json_schema
        new_schema = json.loads(json.dumps(old_schema))
        properties = new_schema["properties"]
        changed_field = next(
            (key for key in properties if key not in case.primary_key),
            case.cursor_field,
        )
        old_type = properties[changed_field].get("type", "string")
        properties[changed_field]["type"] = "integer" if old_type != "integer" else "string"
        properties["certification_added"] = {"type": "string"}
        drift = detect_stream_drift(
            old_schema,
            new_schema,
            primary_key=case.primary_key,
            cursor_field=case.cursor_field,
        )
        if "certification_added" not in drift["added"] or not drift["type_changed"]:
            raise AssertionError("schema drift omitted added-field/type changes")
        for field_name in (*case.primary_key, case.cursor_field):
            removed = json.loads(json.dumps(old_schema))
            removed["properties"].pop(field_name, None)
            try:
                detect_stream_drift(
                    old_schema,
                    removed,
                    primary_key=case.primary_key,
                    cursor_field=case.cursor_field,
                )
            except SchemaDriftError:
                continue
            raise AssertionError(f"schema drift accepted removal of {field_name!r}")
        return "added/type changes reported; primary-key and cursor removal rejected"

    run_step("schema_drift", schema_drift_step)

    step_names = {step.name for step in report.steps}
    for skipped_name, reason in descriptor.certification_skips.items():
        matching = [step for step in report.steps if step.name == skipped_name]
        if skipped_name not in step_names:
            report.steps.append(
                CertificationStep(
                    skipped_name,
                    "fail",
                    "descriptor skip does not name a lifecycle step",
                )
            )
        elif not reason.strip():
            report.steps = [
                CertificationStep(step.name, "fail", "descriptor skip reason is empty")
                if step.name == skipped_name
                else step
                for step in report.steps
            ]
        elif matching and matching[0].status == "skip" and matching[0].reason != reason:
            report.steps = [
                CertificationStep(
                    step.name,
                    "fail",
                    "skip reason does not match the descriptor",
                )
                if step.name == skipped_name
                else step
                for step in report.steps
            ]
    for step in report.steps:
        if step.status == "skip" and step.name not in descriptor.certification_skips:
            report.steps = [
                CertificationStep(
                    item.name,
                    "fail",
                    "step skipped without a descriptor reason",
                )
                if item is step
                else item
                for item in report.steps
            ]
    return report
