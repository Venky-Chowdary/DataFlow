"""Route certification is earned from test results, never from list membership.

A PRODUCTION_SKU route whose matrix test failed, skipped or never ran must not
read the same as a route whose test passed with every required check.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.route_certification import (
    DATA_CASES,
    EVIDENCE_PROPERTY,
    REQUIRED_CHECKS,
    RouteEvidence,
    build_route_matrix,
    load_route_matrix,
    parse_junit,
    route_label,
)

PG_MYSQL = ("database", "postgresql", "database", "mysql")
PG_SQLITE = ("database", "postgresql", "database", "sqlite")
CSV_PG = ("file", "csv", "database", "postgresql")
MYSQL_PG = ("database", "mysql", "database", "postgresql")
JSON_KAFKA = ("file", "json", "database", "kafka")
ROUTES = [PG_MYSQL, PG_SQLITE, CSV_PG, MYSQL_PG, JSON_KAFKA]


def _prop(route, mode, *, checks=(), cases=(), refusal=None) -> str:
    payload = {
        "route": list(route),
        "sync_mode": mode,
        "checks": list(checks),
        "cases": list(cases),
    }
    if refusal:
        payload["refusal"] = refusal
    value = json.dumps(payload).replace('"', "&quot;")
    return f'<property name="{EVIDENCE_PROPERTY}" value="{value}"/>'


def _case(name: str, props: list[str], outcome: str = "passed", message: str = "") -> str:
    body = f"<properties>{''.join(props)}</properties>"
    if outcome == "failed":
        body += f'<failure message="{message}">trace</failure>'
    elif outcome == "error":
        body += f'<error message="{message}">trace</error>'
    elif outcome == "skipped":
        body += f'<skipped type="pytest.skip" message="{message}">skip</skipped>'
    return f'<testcase classname="tests.test_x" name="{name}" time="0.1">{body}</testcase>'


def _junit(tmp_path: Path, cases: list[str]) -> Path:
    xml = (
        '<?xml version="1.0" encoding="utf-8"?><testsuites><testsuite name="pytest">'
        + "".join(cases)
        + "</testsuite></testsuites>"
    )
    path = tmp_path / "junit.xml"
    path.write_text(xml, encoding="utf-8")
    return path


def _cell(matrix: dict, route, mode: str) -> dict:
    label = route_label(*route)
    for cell in matrix["cells"]:
        if cell["route"] == label and cell["sync_mode"] == mode:
            return cell
    raise AssertionError(f"no cell for {label} {mode}")


def _four_outcomes(tmp_path: Path) -> dict:
    ow = "full_refresh_overwrite"
    junit = _junit(
        tmp_path,
        [
            _case(
                "t_pass",
                [
                    _prop(PG_MYSQL, ow),
                    _prop(PG_MYSQL, ow, checks=REQUIRED_CHECKS, cases=("nulls", "unicode")),
                ],
            ),
            _case(
                "t_fail",
                [_prop(PG_SQLITE, ow)],
                outcome="failed",
                message="reconciliation failed",
            ),
            _case(
                "t_skip",
                [_prop(CSV_PG, ow)],
                outcome="skipped",
                message="destination database/postgresql not reachable",
            ),
        ],
    )
    return build_route_matrix(parse_junit(junit), routes=ROUTES)


def test_passed_failed_skipped_and_absent_routes_get_distinct_statuses(tmp_path):
    matrix = _four_outcomes(tmp_path)
    ow = "full_refresh_overwrite"

    verified = _cell(matrix, PG_MYSQL, ow)
    assert verified["status"] == "verified"
    assert verified["checks_missing"] == []
    assert verified["evidence"][0]["test"] == "tests.test_x::t_pass"

    failed = _cell(matrix, PG_SQLITE, ow)
    assert failed["status"] == "failed"
    assert "reconciliation failed" in failed["evidence"][0]["message"]

    skipped = _cell(matrix, CSV_PG, ow)
    assert skipped["status"] == "skipped"
    assert skipped["skip_reasons"] == ["destination database/postgresql not reachable"]

    absent = _cell(matrix, MYSQL_PG, ow)
    assert absent["status"] == "unverified"
    assert absent["evidence"] == []

    summary = matrix["summary"]
    assert summary["tests_passed"] == 1
    assert summary["tests_failed"] == 1
    assert summary["tests_skipped"] == 1
    assert summary["cells_by_status"]["verified"] == 1
    # Skips are reported apart from passes, never folded into them.
    assert summary["cells_by_status"]["skipped"] == 1


def test_a_pass_without_every_required_check_is_partial_not_verified(tmp_path):
    ow = "full_refresh_overwrite"
    junit = _junit(
        tmp_path,
        [_case("t_writer_ack", [_prop(PG_MYSQL, ow, checks=("preflight", "write"))])],
    )
    cell = _cell(build_route_matrix(parse_junit(junit), routes=ROUTES), PG_MYSQL, ow)
    assert cell["status"] == "partial"
    assert cell["checks_missing"] == ["independent_readback", "rerun"]


def test_one_failure_outranks_any_number_of_passes(tmp_path):
    ow = "full_refresh_overwrite"
    full = _prop(PG_MYSQL, ow, checks=REQUIRED_CHECKS)
    junit = _junit(
        tmp_path,
        [
            _case("t_a", [full]),
            _case("t_b", [full]),
            _case("t_c", [_prop(PG_MYSQL, ow)], outcome="error", message="boom"),
        ],
    )
    cell = _cell(build_route_matrix(parse_junit(junit), routes=ROUTES), PG_MYSQL, ow)
    assert cell["status"] == "failed"


def test_cases_covered_and_missing_are_reported_per_cell(tmp_path):
    matrix = _four_outcomes(tmp_path)
    cell = _cell(matrix, PG_MYSQL, "full_refresh_overwrite")
    assert cell["cases_covered"] == ["nulls", "unicode"]
    assert set(cell["cases_missing"]) == set(DATA_CASES) - {"nulls", "unicode"}


def test_refusal_needs_a_reason_and_zero_write_proof(tmp_path):
    junit = _junit(
        tmp_path,
        [
            _case(
                "t_refused",
                [
                    _prop(
                        JSON_KAFKA,
                        "full_refresh_overwrite",
                        checks=("zero_writes",),
                        refusal="Kafka topics are append-only",
                    )
                ],
            ),
            _case(
                "t_refused_unproven",
                [_prop(JSON_KAFKA, "upsert", refusal="Kafka has no key-idempotent write")],
            ),
        ],
    )
    matrix = build_route_matrix(parse_junit(junit), routes=ROUTES)
    proven = _cell(matrix, JSON_KAFKA, "full_refresh_overwrite")
    assert proven["status"] == "refused"
    assert proven["refusal_reason"] == "Kafka topics are append-only"
    unproven = _cell(matrix, JSON_KAFKA, "upsert")
    assert unproven["status"] == "partial"
    assert unproven["checks_missing"] == ["zero_writes"]


def test_refusal_without_reason_is_rejected():
    with pytest.raises(ValueError):
        RouteEvidence(lambda *_: None, JSON_KAFKA, "upsert").refused("", zero_writes=True)


def test_unknown_check_or_case_is_rejected():
    ev = RouteEvidence(lambda *_: None, PG_MYSQL, "full_refresh_overwrite")
    with pytest.raises(ValueError):
        ev.prove("looks_fine")
    with pytest.raises(ValueError):
        ev.prove("write", cases=("everything",))


def test_evidence_for_a_route_outside_the_sku_is_kept_but_flagged(tmp_path):
    off = ("database", "redshift", "database", "postgresql")
    junit = _junit(tmp_path, [_case("t_off", [_prop(off, "upsert", checks=REQUIRED_CHECKS)])])
    matrix = build_route_matrix(parse_junit(junit), routes=ROUTES)
    cell = _cell(matrix, off, "upsert")
    assert cell["status"] == "verified"
    assert cell["in_production_sku"] is False


def test_real_pytest_record_property_round_trips(tmp_path, pytester_like=None):
    """record_property output from a real pytest run parses back into a cell."""
    import subprocess
    import sys

    test_file = tmp_path / "test_emit.py"
    test_file.write_text(
        "import pytest\n"
        "from services.route_certification import RouteEvidence\n"
        "def test_emit(record_property):\n"
        "    ev = RouteEvidence(record_property, ('database','postgresql','database','mysql'), 'upsert')\n"
        "    ev.prove('preflight', 'write', 'independent_readback', 'rerun', cases=('unicode',))\n"
        "def test_skip(record_property):\n"
        "    RouteEvidence(record_property, ('file','csv','database','postgresql'), 'upsert')\n"
        "    pytest.skip('postgres down')\n",
        encoding="utf-8",
    )
    junit = tmp_path / "out.xml"
    api_root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(test_file),
            "-q",
            "-p",
            "no:cacheprovider",
            "-p",
            "no:randomly",
            f"--junitxml={junit}",
            "--rootdir",
            str(tmp_path),
        ],
        cwd=api_root,
        capture_output=True,
        text=True,
        env={**__import__("os").environ, "PYTHONPATH": str(api_root)},
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    matrix = build_route_matrix(parse_junit(junit), routes=ROUTES)
    assert _cell(matrix, PG_MYSQL, "upsert")["status"] == "verified"
    skipped = _cell(matrix, CSV_PG, "upsert")
    assert skipped["status"] == "skipped"
    assert skipped["skip_reasons"] == ["postgres down"]


def test_missing_artifact_loads_as_no_evidence(tmp_path):
    assert load_route_matrix(tmp_path / "absent.json") is None


def test_route_rollup_is_verified_only_when_every_mode_is_certified(tmp_path):
    from services.route_certification import route_certification_rollup

    matrix = _four_outcomes(tmp_path)
    rollup = route_certification_rollup(matrix, route_label(*PG_MYSQL))
    # Overwrite verified; append and upsert have no evidence.
    assert rollup["status"] == "partial"
    assert [m["status"] for m in rollup["modes"]] == ["verified", "unverified", "unverified"]
    assert route_certification_rollup(matrix, route_label(*PG_SQLITE))["status"] == "failed"
    assert route_certification_rollup(None, route_label(*PG_SQLITE))["status"] == "unverified"


def test_collection_time_skip_is_attributed_through_the_marker(tmp_path):
    """A skip applied at collection (absent emulator) still lands on its cell."""
    import subprocess
    import sys

    (tmp_path / "conftest.py").write_text(
        "import pytest\n"
        "from services.route_certification import declare_marked_items\n"
        "def pytest_configure(config):\n"
        "    config.addinivalue_line('markers', 'route_cert(route, sync_mode): cell')\n"
        "def pytest_collection_modifyitems(config, items):\n"
        "    declare_marked_items(items)\n"
        "    for item in items:\n"
        "        item.add_marker(pytest.mark.skip(reason='MongoDB not reachable on this runner'))\n",
        encoding="utf-8",
    )
    (tmp_path / "test_marked.py").write_text(
        "import pytest\n"
        "@pytest.mark.route_cert(route=('database','postgresql','database','mysql'), sync_mode='upsert')\n"
        "def test_marked():\n"
        "    raise AssertionError('never runs')\n",
        encoding="utf-8",
    )
    junit = tmp_path / "out.xml"
    api_root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(tmp_path / "test_marked.py"),
            "-q",
            "-p",
            "no:cacheprovider",
            "-p",
            "no:randomly",
            f"--junitxml={junit}",
            "--rootdir",
            str(tmp_path),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env={**__import__("os").environ, "PYTHONPATH": str(api_root)},
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    cell = _cell(build_route_matrix(parse_junit(junit), routes=ROUTES), PG_MYSQL, "upsert")
    assert cell["status"] == "skipped"
    assert cell["skip_reasons"] == ["MongoDB not reachable on this runner"]
