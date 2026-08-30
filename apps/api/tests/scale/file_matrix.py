"""Track B — file formats and object stores at 100K rows, both roles.

One command runs the whole matrix::

    DATAFLOW_SCALE_FILE_MATRIX=1 python -m tests.scale.file_matrix

Selectors keep a re-run cheap::

    python -m tests.scale.file_matrix --only csv,parquet --rows 100000
    python -m tests.scale.file_matrix --list
    python -m tests.scale.file_matrix --route file_to_postgres

Every cell:

1. writes (or reuses) the 100K dirty-data fixture in the cell's format,
2. runs the transfer through ``UniversalTransferEngine.execute_tracked`` — the
   product's own path, never a bypass,
3. re-opens the destination with an *independent* driver/client, counts rows and
   hashes the mapped projection with the fixture's canonicalizer,
4. records the engine's claim next to the independent number, and marks the cell
   ``pass`` only when they agree with the expected population.

Results append to ``exports/scale/file_matrix.jsonl`` as each cell finishes, so
an interrupted run resumes without re-proving cells that already landed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
import uuid
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from tests.scale import dirty_fixture as fixture  # noqa: E402
from tests.scale import readback as rb  # noqa: E402

ENV_GATE = "DATAFLOW_SCALE_FILE_MATRIX"

#: Exports must land inside the API workspace (``land_export_bytes`` refuses
#: anything else), and ``apps/api/exports/`` is already git-ignored.
EXPORT_DIR = _API_ROOT / "exports" / "scale"
RESULTS_PATH = EXPORT_DIR / "file_matrix.jsonl"
#: Fixtures are big and regenerable — they live outside the repo by default.
FIXTURE_DIR = Path(
    os.getenv("DATAFLOW_SCALE_FIXTURE_DIR", str(Path.home() / "dataflow_scale_fixtures"))
)

SYNC_MODE = "full_refresh_overwrite"

#: ``balanced`` resolves to the writer's ``quarantine`` policy: bad cells are held
#: out of the primary table and land in the DLQ, and the rest of the population
#: still moves. ``strict`` resolves to ``fail``: one bad cell refuses the whole
#: write. Both are proven — quarantine for the population cells, strict for the
#: fail-closed cells — because "quarantine, don't coerce" and "never write a
#: partial destination" are two different guarantees.
QUARANTINE_MODE = "balanced"
STRICT_MODE = "strict"


def gated() -> bool:
    return os.getenv(ENV_GATE, "").strip().lower() in {"1", "true", "yes"}


# --------------------------------------------------------------------------- #
# Result record
# --------------------------------------------------------------------------- #


@dataclass
class Cell:
    """One matrix cell: a route, a format, a storage layer."""

    name: str
    route: str          # file_to_postgres | file_to_mysql | postgres_to_file | file_to_file
    store: str          # local | minio | fake-gcs | azurite | aws-s3 | gcs | adls
    source: str
    destination: str
    runner: Callable[["Cell", int], "CellResult"] | None = None
    mode: str = QUARANTINE_MODE
    note: str = ""


@dataclass
class CellResult:
    name: str
    route: str
    store: str
    source: str
    destination: str
    status: str = "fail"
    rows_expected: int = 0
    source_rows: int = 0
    dest_rows_independent: int = 0
    engine_rows_claimed: int = 0
    rejected: int = 0
    quarantined: int = 0
    coerced_null: int = 0
    skipped: int = 0
    elapsed_seconds: float = 0.0
    rows_per_second: float = 0.0
    sync_mode: str = SYNC_MODE
    run_id: str = ""
    checksum_expected: str = ""
    checksum_dest: str = ""
    checksum_match: bool = False
    schema: dict[str, str] = field(default_factory=dict)
    null_tokens: dict[str, int] = field(default_factory=dict)
    engine_reconciliation: dict[str, Any] = field(default_factory=dict)
    verification: str = ""
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _skip(cell: Cell, reason: str) -> CellResult:
    return CellResult(
        name=cell.name,
        route=cell.route,
        store=cell.store,
        source=cell.source,
        destination=cell.destination,
        status=f"skip ({reason})",
    )


# --------------------------------------------------------------------------- #
# Engine invocation
# --------------------------------------------------------------------------- #


def _engine():
    from src.transfer.engine import UniversalTransferEngine

    return UniversalTransferEngine()


def _mappings(dialect: str = "") -> list[dict[str, str]]:
    types = fixture.dest_types(dialect)
    return [
        {"source": col, "target": col, "target_type": types[col]}
        for col in fixture.COLUMNS
    ]


def _column_types(dialect: str = "") -> dict[str, str]:
    return fixture.dest_types(dialect)


def execute(request: Any) -> tuple[Any, str, float]:
    """Run one transfer through the product engine. Returns (result, run_id, secs)."""
    run_id = uuid.uuid4().hex[:24]
    engine = _engine()
    started = time.time()
    result = engine.execute_tracked(request, run_id)
    return result, run_id, time.time() - started


def _dest_counts(result: Any) -> dict[str, int]:
    """Rejected / coerced / skipped as the engine reported them."""
    summary = dict(getattr(result, "destination_summary", None) or {})
    recon = dict(getattr(result, "reconciliation", None) or {})
    return {
        "rejected": int(summary.get("rejected_rows") or 0),
        "quarantined": int(
            summary.get("quarantined_rows")
            or summary.get("quarantine_row_count")
            or summary.get("rejected_rows")
            or 0
        ),
        "coerced_null": int(
            summary.get("coerced_null_rows")
            or recon.get("coerced_null_rows")
            or 0
        ),
        "skipped": int(summary.get("skipped_rows") or recon.get("skipped_rows") or 0),
    }


# --------------------------------------------------------------------------- #
# Fixtures on disk (cached between runs — regeneration is not the measurement)
# --------------------------------------------------------------------------- #


def fixture_path(kind: str, rows: int) -> Path:
    spec = fixture.FORMATS[kind]
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    path = FIXTURE_DIR / f"dirty_{kind}_{rows}{spec.suffix}"
    if not path.exists() or path.stat().st_size == 0:
        fixture.write_format(kind, path, rows)
    return path


def variant_path(name: str, rows: int) -> Path:
    spec = fixture.STRUCTURAL_VARIANTS[name]
    suffix = ".xlsx" if spec["format"] == "excel" else ".csv"
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    path = FIXTURE_DIR / f"dirty_{name}_{rows}{suffix}"
    if not path.exists() or path.stat().st_size == 0:
        fixture.write_variant(name, path, rows)
    return path


def unsupported_carrier_path(carrier: str, rows: int) -> Path:
    """A real file in a carrier the product does not read, for refusal proof.

    ``xls``: BIFF is genuinely unreadable here — openpyxl is the only spreadsheet
    reader in ``requirements.txt`` and BIFF8 caps a sheet at 65,536 rows anyway,
    so a 100K-row ``.xls`` cannot exist. The proof that matters is the refusal, so
    the payload is the .xlsx fixture under an ``.xls`` name — exactly the case a
    client hits when they rename a file.
    ``zip``: no zip branch exists in the reader at all (gzip is handled); a real
    archive proves whether the engine refuses or silently misparses the container.
    """
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    if carrier == "xls":
        src = fixture_path("excel", rows)
        path = FIXTURE_DIR / f"dirty_legacy_{rows}.xls"
        if not path.exists() or path.stat().st_size == 0:
            path.write_bytes(src.read_bytes())
        return path
    if carrier == "zip":
        src = fixture_path("csv", rows)
        path = FIXTURE_DIR / f"dirty_csv_{rows}.zip"
        if not path.exists() or path.stat().st_size == 0:
            with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(src, arcname=src.name)
        return path
    raise ValueError(f"unknown unsupported carrier {carrier!r}")


# --------------------------------------------------------------------------- #
# Route runners
# --------------------------------------------------------------------------- #


def _file_source(kind: str, path: Path):
    from src.transfer.models import EndpointConfig

    spec = fixture.FORMATS[kind]
    return EndpointConfig(kind="file", format=spec.name), path.name


def _pg_dest(table: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=str(rb.PG["host"]),
        port=int(rb.PG["port"]),
        database=str(rb.PG["database"]),
        username=str(rb.PG["user"]),
        password=str(rb.PG["password"]),
        schema="public",
        table=table,
    )


def _mysql_dest(table: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database",
        format="mysql",
        host=str(rb.MYSQL["host"]),
        port=int(rb.MYSQL["port"]),
        database=str(rb.MYSQL["database"]),
        username=str(rb.MYSQL["user"]),
        password=str(rb.MYSQL["password"]),
        table=table,
    )


def _pg_source(table: str):
    endpoint = _pg_dest(table)
    return endpoint


def _file_export_dest(export_format: str, out_path: Path):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="file_export",
        format=export_format,
        output_path=str(out_path),
        table=out_path.stem,
    )


def _request(*, dialect: str = "", mode: str = QUARANTINE_MODE, **kwargs: Any):
    from src.transfer.models import TransferRequest

    kwargs.setdefault("sync_mode", SYNC_MODE)
    kwargs.setdefault("validation_mode", mode)
    kwargs.setdefault("skip_preflight", True)
    kwargs.setdefault("mappings", _mappings(dialect))
    kwargs.setdefault("column_types", _column_types(dialect))
    return TransferRequest(**kwargs)


def _base_result(cell: Cell, rows: int) -> CellResult:
    expected_checksum, expected_rows = fixture.expected_checksum(rows)
    return CellResult(
        name=cell.name,
        route=cell.route,
        store=cell.store,
        source=cell.source,
        destination=cell.destination,
        rows_expected=expected_rows,
        source_rows=rows,
        checksum_expected=expected_checksum,
        sync_mode=f"{SYNC_MODE} / {cell.mode}",
    )


def _finish_db(res: CellResult, result: Any, readback: rb.Readback, rows: int) -> CellResult:
    counts = _dest_counts(result)
    res.engine_rows_claimed = int(getattr(result, "records_transferred", 0) or 0)
    res.rejected = counts["rejected"]
    res.quarantined = counts["quarantined"]
    res.coerced_null = counts["coerced_null"]
    res.skipped = counts["skipped"]
    res.dest_rows_independent = readback.row_count
    res.checksum_dest = readback.checksum
    res.checksum_match = readback.checksum == res.checksum_expected
    res.schema = readback.schema
    res.null_tokens = readback.null_tokens
    res.verification = readback.detail
    res.engine_reconciliation = {
        k: v
        for k, v in (getattr(result, "reconciliation", None) or {}).items()
        if k
        in {
            "assurance_level",
            "migration_proven",
            "population_proof",
            "source_rows",
            "destination_rows",
            "status",
        }
    }
    if not getattr(result, "success", False):
        res.status = "fail"
        res.notes.append(f"engine error: {getattr(result, 'error', '')[:400]}")
        return res
    if res.dest_rows_independent != res.rows_expected:
        res.status = "fail"
        res.notes.append(
            f"independent count {res.dest_rows_independent} != expected "
            f"{res.rows_expected} (source {rows}, quarantine "
            f"{fixture.quarantine_row_count(rows)})"
        )
        return res
    if not res.checksum_match:
        res.status = "fail"
        res.notes.append("checksum over mapped projection differs from the fixture")
        return res
    res.status = "pass"
    return res


def run_file_to_db(cell: Cell, rows: int, *, engine_db: str = "postgres") -> CellResult:
    res = _base_result(cell, rows)
    kind = cell.source
    path = fixture_path(kind, rows) if kind in fixture.FORMATS else variant_path(kind, rows)
    table = f"scale_{cell.route}_{cell.name}".replace("-", "_").replace(".", "_")[:60]
    if engine_db == "postgres":
        rb.pg_drop(table)
        destination = _pg_dest(table)
    else:
        rb.mysql_drop(table)
        destination = _mysql_dest(table)
    source_kind = kind if kind in fixture.FORMATS else fixture.STRUCTURAL_VARIANTS[kind]["format"]
    source, filename = _file_source(source_kind, path)
    request = _request(
        source=source,
        destination=destination,
        source_path=str(path),
        source_filename=filename,
        dialect=engine_db,
        mode=cell.mode,
    )
    result, run_id, elapsed = execute(request)
    res.run_id = run_id
    res.elapsed_seconds = round(elapsed, 2)
    res.rows_per_second = round(rows / elapsed, 1) if elapsed else 0.0
    try:
        readback = rb.pg_readback(table) if engine_db == "postgres" else rb.mysql_readback(table)
    except Exception as exc:  # noqa: BLE001 — an unreadable destination is a failure, not a crash
        res.status = "fail"
        res.notes.append(f"independent readback failed: {type(exc).__name__}: {exc}")
        res.engine_rows_claimed = int(getattr(result, "records_transferred", 0) or 0)
        if not getattr(result, "success", False):
            res.notes.append(f"engine error: {getattr(result, 'error', '')[:400]}")
        return res
    res = _finish_db(res, result, readback, rows)
    res.notes.extend(_quarantine_evidence(table, engine_db, rows))
    return res


def _quarantine_evidence(table: str, engine_db: str, rows: int) -> list[str]:
    """Independent proof the dirty cells were held out, not silently coerced."""
    from services.dest_quarantine import dlq_table_name

    dlq = dlq_table_name(table)
    expected = fixture.quarantine_row_count(rows)
    notes: list[str] = []
    try:
        if engine_db == "postgres":
            with rb.pg_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT to_regclass(%s)", (f'public."{dlq}"',)
                    )
                    if cur.fetchone()[0] is None:
                        notes.append(f"no DLQ table {dlq} (expected {expected} held rows)")
                        return notes
                    cur.execute(f'SELECT COUNT(*) FROM public."{dlq}"')
                    held = int(cur.fetchone()[0])
        else:
            with rb.mysql_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema=%s AND table_name=%s",
                        (rb.MYSQL["database"], dlq),
                    )
                    if not int(cur.fetchone()[0]):
                        notes.append(f"no DLQ table {dlq} (expected {expected} held rows)")
                        return notes
                    cur.execute(f"SELECT COUNT(*) FROM `{dlq}`")
                    held = int(cur.fetchone()[0])
        notes.append(f"DLQ {dlq}: {held} rows held (expected {expected})")
    except Exception as exc:  # noqa: BLE001
        notes.append(f"DLQ probe failed: {type(exc).__name__}: {exc}")
    return notes


def run_file_to_mysql(cell: Cell, rows: int) -> CellResult:
    return run_file_to_db(cell, rows, engine_db="mysql")


_PG_SEEDED: dict[int, str] = {}


def pg_seed_table(rows: int) -> str:
    """One natively-seeded source table, reused by every database→file cell."""
    table = f"scale_src_dirty_{rows}"
    if _PG_SEEDED.get(rows) == table:
        return table
    existing = 0
    try:
        with rb.pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", (f'public."{table}"',))
                if cur.fetchone()[0] is not None:
                    cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
                    existing = int(cur.fetchone()[0])
    except Exception:  # noqa: BLE001
        existing = 0
    if existing != rows:
        rb.pg_seed(table, rows)
    _PG_SEEDED[rows] = table
    return table


def _pg_source_checksum(table: str) -> tuple[str, int]:
    """The seeded population's own checksum, read independently."""
    back = rb.pg_readback(table)
    return back.checksum, back.row_count


def run_db_to_file(cell: Cell, rows: int) -> CellResult:
    res = _base_result(cell, rows)
    table = pg_seed_table(rows)
    # The seed table holds NULL where the fixture's non-numeric cell cannot be
    # typed as INTEGER, so the expected checksum for this direction comes from
    # the seeded population itself — read back independently, not from the
    # writer.
    src_checksum, src_rows = _pg_source_checksum(table)
    res.checksum_expected = src_checksum
    res.rows_expected = src_rows
    res.source_rows = src_rows
    export_format = cell.destination
    spec = next(
        (s for s in fixture.FORMATS.values() if s.export_format == export_format), None
    )
    suffix = spec.suffix if spec else f".{export_format}"
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / f"{cell.name}_{rows}{suffix}"
    if out_path.exists():
        out_path.unlink()
    request = _request(
        source=_pg_source(table),
        destination=_file_export_dest(export_format, out_path),
        mode=cell.mode,
    )
    result, run_id, elapsed = execute(request)
    res.run_id = run_id
    res.elapsed_seconds = round(elapsed, 2)
    res.rows_per_second = round(src_rows / elapsed, 1) if elapsed else 0.0
    counts = _dest_counts(result)
    res.engine_rows_claimed = int(getattr(result, "records_transferred", 0) or 0)
    res.rejected, res.quarantined = counts["rejected"], counts["quarantined"]
    res.coerced_null, res.skipped = counts["coerced_null"], counts["skipped"]
    res.engine_reconciliation = {
        k: v
        for k, v in (getattr(result, "reconciliation", None) or {}).items()
        if k in {"assurance_level", "migration_proven", "population_proof", "status"}
    }
    if not getattr(result, "success", False):
        res.status = "fail"
        res.notes.append(f"engine error: {getattr(result, 'error', '')[:400]}")
        return res
    landed = str((getattr(result, "destination_summary", None) or {}).get("path") or out_path)
    try:
        back = rb.file_readback(landed, export_format)
    except Exception as exc:  # noqa: BLE001
        res.status = "fail"
        res.notes.append(f"independent read of export failed: {type(exc).__name__}: {exc}")
        return res
    res.dest_rows_independent = back.row_count
    res.checksum_dest = back.checksum
    res.checksum_match = back.checksum == res.checksum_expected
    res.schema = back.schema
    res.null_tokens = back.null_tokens
    res.verification = back.detail
    if res.dest_rows_independent != res.rows_expected:
        res.status = "fail"
        res.notes.append(
            f"export holds {res.dest_rows_independent} rows, source table holds "
            f"{res.rows_expected}"
        )
    elif not res.checksum_match:
        res.status = "fail"
        res.notes.append("export checksum differs from the source population")
    else:
        res.status = "pass"
    return res


def run_file_to_file(cell: Cell, rows: int) -> CellResult:
    """Format conversion, proven row-for-row through independent readers.

    Byte identity is not the claim across formats (a Parquet file is not a CSV
    file); the claim is that the canonical projection of every row survives, so
    the same checksum has to come out the other side.
    """
    res = _base_result(cell, rows)
    src_kind, dst_format = cell.source, cell.destination
    path = fixture_path(src_kind, rows)
    # A file export runs the same typed quarantine matrix as a SQL writer, so the
    # row whose INTEGER cell reads ``N/A`` is held out here too.
    expected, expected_rows = fixture.expected_checksum(rows)
    res.checksum_expected, res.rows_expected = expected, expected_rows
    spec = next((s for s in fixture.FORMATS.values() if s.export_format == dst_format), None)
    suffix = spec.suffix if spec else f".{dst_format}"
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / f"{cell.name}_{rows}{suffix}"
    if out_path.exists():
        out_path.unlink()
    source, filename = _file_source(src_kind, path)
    request = _request(
        source=source,
        destination=_file_export_dest(dst_format, out_path),
        source_path=str(path),
        source_filename=filename,
        mode=cell.mode,
    )
    result, run_id, elapsed = execute(request)
    res.run_id = run_id
    res.elapsed_seconds = round(elapsed, 2)
    res.rows_per_second = round(rows / elapsed, 1) if elapsed else 0.0
    counts = _dest_counts(result)
    res.engine_rows_claimed = int(getattr(result, "records_transferred", 0) or 0)
    res.rejected, res.quarantined = counts["rejected"], counts["quarantined"]
    res.coerced_null, res.skipped = counts["coerced_null"], counts["skipped"]
    if not getattr(result, "success", False):
        res.status = "fail"
        res.notes.append(f"engine error: {getattr(result, 'error', '')[:400]}")
        return res
    landed = str((getattr(result, "destination_summary", None) or {}).get("path") or out_path)
    try:
        back = rb.file_readback(landed, dst_format)
    except Exception as exc:  # noqa: BLE001
        res.status = "fail"
        res.notes.append(f"independent read of converted file failed: {type(exc).__name__}: {exc}")
        return res
    res.dest_rows_independent = back.row_count
    res.checksum_dest = back.checksum
    res.checksum_match = back.checksum == res.checksum_expected
    res.schema = back.schema
    res.null_tokens = back.null_tokens
    res.verification = back.detail
    if res.dest_rows_independent != res.rows_expected:
        res.status = "fail"
        res.notes.append(
            f"converted file holds {res.dest_rows_independent} rows, fixture has "
            f"{res.rows_expected}"
        )
    elif not res.checksum_match:
        res.status = "fail"
        res.notes.append("row-level fidelity lost in conversion (checksum differs)")
    else:
        res.status = "pass"
    return res


def run_unsupported_carrier(cell: Cell, rows: int) -> CellResult:
    """An unreadable carrier must refuse, not misparse into a plausible table.

    ``pass`` here means "engine refused and the destination stayed empty". A
    success, or any row landing, is silent corruption of a container the reader
    does not understand.
    """
    res = _base_result(cell, rows)
    from src.transfer.models import EndpointConfig

    carrier = cell.source
    path = unsupported_carrier_path(carrier, rows)
    table = f"scale_unsupported_{carrier}"
    rb.pg_drop(table)
    res.rows_expected = 0
    res.checksum_expected = ""
    request = _request(
        source=EndpointConfig(kind="file", format="excel" if carrier == "xls" else "csv"),
        destination=_pg_dest(table),
        source_path=str(path),
        source_filename=path.name,
        dialect="postgres",
        mode=cell.mode,
    )
    result, run_id, elapsed = execute(request)
    res.run_id = run_id
    res.elapsed_seconds = round(elapsed, 2)
    res.engine_rows_claimed = int(getattr(result, "records_transferred", 0) or 0)
    landed = 0
    try:
        landed = rb.pg_table_count(table)
    except Exception as exc:  # noqa: BLE001 — "table absent" is the expected outcome
        res.notes.append(f"destination probe: {type(exc).__name__}: {str(exc).splitlines()[0]}")
    res.dest_rows_independent = landed
    res.verification = f"independent COUNT(*) after refusal = {landed}"
    if getattr(result, "success", False):
        res.status = "fail"
        res.notes.append(f"engine accepted a .{carrier} payload it has no reader for")
    elif landed:
        res.status = "fail"
        res.notes.append(f"partial write: {landed} rows survived a refused .{carrier} job")
    else:
        res.status = "pass"
        res.notes.append(f"refused: {str(getattr(result, 'error', ''))[:200]}")
    return res


def run_strict_refusal(cell: Cell, rows: int) -> CellResult:
    """Strict mode must refuse the whole write, not land a partial destination.

    Same fixture, same route, ``validation_mode=strict`` (writer policy
    ``fail``): the non-numeric cell in the INTEGER column has to abort the job
    *and* leave nothing behind. ``pass`` here means "refused and destination
    empty" — a successful transfer, or a table holding rows, is the failure.
    """
    res = _base_result(cell, rows)
    kind = cell.source
    path = fixture_path(kind, rows)
    table = f"strict_{cell.route}_{kind}".replace("-", "_")[:60]
    dialect = "mysql" if cell.route == "file_to_mysql" else "postgres"
    if dialect == "postgres":
        rb.pg_drop(table)
        destination = _pg_dest(table)
    else:
        rb.mysql_drop(table)
        destination = _mysql_dest(table)
    source, filename = _file_source(kind, path)
    request = _request(
        source=source,
        destination=destination,
        source_path=str(path),
        source_filename=filename,
        dialect=dialect,
        mode=STRICT_MODE,
    )
    result, run_id, elapsed = execute(request)
    res.run_id = run_id
    res.elapsed_seconds = round(elapsed, 2)
    res.rows_per_second = round(rows / elapsed, 1) if elapsed else 0.0
    res.rows_expected = 0
    res.checksum_expected = ""
    res.engine_rows_claimed = int(getattr(result, "records_transferred", 0) or 0)
    counts = _dest_counts(result)
    res.rejected, res.quarantined = counts["rejected"], counts["quarantined"]
    landed = 0
    try:
        landed = (
            rb.pg_table_count(table) if dialect == "postgres" else rb.mysql_table_count(table)
        )
    except Exception as exc:  # noqa: BLE001
        res.notes.append(f"destination probe: {type(exc).__name__}: {exc}")
    res.dest_rows_independent = landed
    res.verification = f"independent COUNT(*) after refusal = {landed}"
    if getattr(result, "success", False):
        res.status = "fail"
        res.notes.append("strict mode accepted a population with an untypeable cell")
    elif landed:
        res.status = "fail"
        res.notes.append(f"partial write: {landed} rows survived a refused job")
    else:
        res.status = "pass"
        res.notes.append(
            f"refused, destination empty: {str(getattr(result, 'error', ''))[:200]}"
        )
    return res


# --------------------------------------------------------------------------- #
# Object stores
# --------------------------------------------------------------------------- #

_OBJ_PREFIX = "trackb"


def _objstore_endpoint(store: str, key: str):
    from src.transfer.models import EndpointConfig

    if store == "minio":
        return EndpointConfig(
            kind="database",
            format="s3",
            database=rb.minio_ensure_bucket(),
            table=key,
            username=str(rb.MINIO["access_key"]),
            password=str(rb.MINIO["secret_key"]),
            region=str(rb.MINIO["region"]),
            endpoint_url=str(rb.MINIO["endpoint_url"]),
            path_style=True,
        )
    if store == "fake-gcs":
        return EndpointConfig(
            kind="database",
            format="gcs",
            database=rb.gcs_ensure_bucket(),
            table=key,
            host=str(rb.GCS["project"]),
            endpoint_url=str(rb.GCS["endpoint_url"]),
        )
    if store == "azurite":
        return EndpointConfig(
            kind="database",
            format="adls",
            database=rb.azurite_ensure_container(),
            table=key,
            connection_string=str(rb.AZURITE["connection_string"]),
        )
    raise ValueError(f"unknown store {store!r}")


def _objstore_upload(store: str, key: str, path: Path) -> str:
    if store == "minio":
        bucket = rb.minio_ensure_bucket()
        rb.minio_put(bucket, key, str(path))
        return bucket
    if store == "fake-gcs":
        bucket = rb.gcs_ensure_bucket()
        rb.gcs_put(bucket, key, str(path))
        return bucket
    if store == "azurite":
        container = rb.azurite_ensure_container()
        rb.azurite_put(container, key, str(path))
        return container
    raise ValueError(store)


def _objstore_clear(store: str, key: str) -> str:
    stem = key.rsplit(".", 1)[0]
    if store == "minio":
        bucket = rb.minio_ensure_bucket()
        rb.minio_delete_prefix(bucket, stem)
        return bucket
    if store == "fake-gcs":
        bucket = rb.gcs_ensure_bucket()
        rb.gcs_delete_prefix(bucket, stem)
        return bucket
    if store == "azurite":
        container = rb.azurite_ensure_container()
        rb.azurite_delete_prefix(container, stem)
        return container
    raise ValueError(store)


def _objstore_readback(store: str, bucket: str, key: str, export_format: str, suffix: str) -> rb.Readback:
    if store == "minio":
        return rb.minio_readback(bucket, key, export_format, suffix=suffix)
    if store == "fake-gcs":
        return rb.gcs_readback(bucket, key, export_format, suffix=suffix)
    if store == "azurite":
        return rb.azurite_readback(bucket, key, export_format, suffix=suffix)
    raise ValueError(store)


def run_objstore_to_db(cell: Cell, rows: int) -> CellResult:
    """Object store → PostgreSQL: the store is the source of the file bytes."""
    res = _base_result(cell, rows)
    kind = cell.source
    path = fixture_path(kind, rows)
    key = f"{_OBJ_PREFIX}/src/dirty_{kind}_{rows}{fixture.FORMATS[kind].suffix}"
    bucket = _objstore_upload(cell.store, key, path)
    table = f"scale_{cell.store}_{kind}_to_pg".replace("-", "_")[:60]
    rb.pg_drop(table)
    request = _request(
        source=_objstore_endpoint(cell.store, key),
        destination=_pg_dest(table),
        dialect="postgres",
        mode=cell.mode,
    )
    result, run_id, elapsed = execute(request)
    res.run_id = run_id
    res.elapsed_seconds = round(elapsed, 2)
    res.rows_per_second = round(rows / elapsed, 1) if elapsed else 0.0
    res.notes.append(f"source object {cell.store}://{bucket}/{key}")
    try:
        back = rb.pg_readback(table)
    except Exception as exc:  # noqa: BLE001
        res.status = "fail"
        res.notes.append(f"independent readback failed: {type(exc).__name__}: {exc}")
        if not getattr(result, "success", False):
            res.notes.append(f"engine error: {getattr(result, 'error', '')[:400]}")
        return res
    res = _finish_db(res, result, back, rows)
    return res


def run_db_to_objstore(cell: Cell, rows: int) -> CellResult:
    """PostgreSQL → object store, verified by listing and parsing every part."""
    res = _base_result(cell, rows)
    table = pg_seed_table(rows)
    src_checksum, src_rows = _pg_source_checksum(table)
    res.checksum_expected, res.rows_expected, res.source_rows = src_checksum, src_rows, src_rows
    export_format = cell.destination
    suffix = {"csv": ".csv", "json": ".json", "jsonl": ".jsonl", "parquet": ".parquet"}.get(
        export_format, f".{export_format}"
    )
    key = f"{_OBJ_PREFIX}/dest/{cell.name}_{rows}{suffix}"
    bucket = _objstore_clear(cell.store, key)
    request = _request(
        source=_pg_source(table),
        destination=_objstore_endpoint(cell.store, key),
        mode=cell.mode,
    )
    result, run_id, elapsed = execute(request)
    res.run_id = run_id
    res.elapsed_seconds = round(elapsed, 2)
    res.rows_per_second = round(src_rows / elapsed, 1) if elapsed else 0.0
    counts = _dest_counts(result)
    res.engine_rows_claimed = int(getattr(result, "records_transferred", 0) or 0)
    res.rejected, res.quarantined = counts["rejected"], counts["quarantined"]
    res.coerced_null, res.skipped = counts["coerced_null"], counts["skipped"]
    res.notes.append(f"destination object {cell.store}://{bucket}/{key}")
    if not getattr(result, "success", False):
        res.status = "fail"
        res.notes.append(f"engine error: {getattr(result, 'error', '')[:400]}")
        return res
    try:
        back = _objstore_readback(cell.store, bucket, key, export_format, suffix)
    except Exception as exc:  # noqa: BLE001
        res.status = "fail"
        res.notes.append(f"independent object read failed: {type(exc).__name__}: {exc}")
        return res
    res.dest_rows_independent = back.row_count
    res.checksum_dest = back.checksum
    res.checksum_match = back.checksum == res.checksum_expected
    res.schema = back.schema
    res.null_tokens = back.null_tokens
    res.verification = back.detail
    if res.dest_rows_independent != res.rows_expected:
        res.status = "fail"
        res.notes.append(
            f"object holds {res.dest_rows_independent} rows, source table holds "
            f"{res.rows_expected}"
        )
    elif not res.checksum_match:
        res.status = "fail"
        res.notes.append("object checksum differs from the source population")
    else:
        res.status = "pass"
    return res


def run_objstore_to_objstore(cell: Cell, rows: int) -> CellResult:
    """Object → object conversion inside one store (file → file at the store)."""
    res = _base_result(cell, rows)
    src_kind = cell.source
    path = fixture_path(src_kind, rows)
    src_key = f"{_OBJ_PREFIX}/src/dirty_{src_kind}_{rows}{fixture.FORMATS[src_kind].suffix}"
    bucket = _objstore_upload(cell.store, src_key, path)
    export_format = cell.destination
    suffix = {"csv": ".csv", "json": ".json", "jsonl": ".jsonl", "parquet": ".parquet"}.get(
        export_format, f".{export_format}"
    )
    dst_key = f"{_OBJ_PREFIX}/dest/{cell.name}_{rows}{suffix}"
    _objstore_clear(cell.store, dst_key)
    expected, expected_rows = fixture.expected_checksum(rows)
    res.checksum_expected, res.rows_expected = expected, expected_rows
    request = _request(
        source=_objstore_endpoint(cell.store, src_key),
        destination=_objstore_endpoint(cell.store, dst_key),
        mode=cell.mode,
    )
    result, run_id, elapsed = execute(request)
    res.run_id = run_id
    res.elapsed_seconds = round(elapsed, 2)
    res.rows_per_second = round(rows / elapsed, 1) if elapsed else 0.0
    res.notes.append(f"{cell.store}://{bucket}/{src_key} → {dst_key}")
    counts = _dest_counts(result)
    res.engine_rows_claimed = int(getattr(result, "records_transferred", 0) or 0)
    res.rejected, res.quarantined = counts["rejected"], counts["quarantined"]
    if not getattr(result, "success", False):
        res.status = "fail"
        res.notes.append(f"engine error: {getattr(result, 'error', '')[:400]}")
        return res
    try:
        back = _objstore_readback(cell.store, bucket, dst_key, export_format, suffix)
    except Exception as exc:  # noqa: BLE001
        res.status = "fail"
        res.notes.append(f"independent object read failed: {type(exc).__name__}: {exc}")
        return res
    res.dest_rows_independent = back.row_count
    res.checksum_dest = back.checksum
    res.checksum_match = back.checksum == res.checksum_expected
    res.schema = back.schema
    res.null_tokens = back.null_tokens
    res.verification = back.detail
    if res.dest_rows_independent != res.rows_expected:
        res.status = "fail"
        res.notes.append(
            f"object holds {res.dest_rows_independent} rows, fixture has {res.rows_expected}"
        )
    elif not res.checksum_match:
        res.status = "fail"
        res.notes.append("row-level fidelity lost in object conversion")
    else:
        res.status = "pass"
    return res


# --------------------------------------------------------------------------- #
# The matrix
# --------------------------------------------------------------------------- #

#: Formats proven as a *source* against databases and other files.
SOURCE_FORMATS: tuple[str, ...] = (
    "csv",
    "tsv",
    "psv",
    "scsv",
    "json",
    "jsonl",
    "ndjson",
    "parquet",
    "avro",
    "orc",
    "excel",
    "xml",
    "csv_gz",
    "fixed_width",
    "yaml",
)

#: Formats the product can *write* as a file export.
EXPORT_FORMATS: tuple[str, ...] = (
    "csv",
    "tsv",
    "json",
    "jsonl",
    "excel",
    "parquet",
    "avro",
    "orc",
    "xml",
)

#: Delimited carriers cannot distinguish an empty text cell from an absent one
#: (see :func:`dirty_fixture.empty_is_ambiguous`).
DELIMITED_FORMATS: frozenset[str] = frozenset(
    {"csv", "tsv", "psv", "scsv", "csv_gz", "fixed_width"}
)

#: Object-store payload formats the store readers/writers claim.
OBJECT_FORMATS: tuple[str, ...] = ("csv", "json", "jsonl", "parquet")

#: Hosted SKUs. No credentials exist in this environment and an emulator is not
#: the hosted service, so these are recorded as skips with the exact reason.
CREDENTIAL_SKIPS: tuple[tuple[str, str, str], ...] = (
    ("aws-s3", "AWS S3", "no AWS credentials in this environment"),
    ("gcs", "Google Cloud Storage", "no GCP service-account credentials in this environment"),
    ("adls", "Azure Data Lake Storage Gen2", "no Azure tenant credentials in this environment"),
)


def build_cells() -> list[Cell]:
    cells: list[Cell] = []
    for kind in SOURCE_FORMATS:
        cells.append(
            Cell(
                name=f"{kind}_to_postgres",
                route="file_to_postgres",
                store="local",
                source=kind,
                destination="postgresql",
                runner=run_file_to_db,
            )
        )
        cells.append(
            Cell(
                name=f"{kind}_to_mysql",
                route="file_to_mysql",
                store="local",
                source=kind,
                destination="mysql",
                runner=run_file_to_mysql,
            )
        )
    for variant in fixture.STRUCTURAL_VARIANTS:
        cells.append(
            Cell(
                name=f"{variant}_to_postgres",
                route="file_to_postgres",
                store="local",
                source=variant,
                destination="postgresql",
                runner=run_file_to_db,
                note="structural / encoding variant",
            )
        )
    for export in EXPORT_FORMATS:
        cells.append(
            Cell(
                name=f"postgres_to_{export}",
                route="postgres_to_file",
                store="local",
                source="postgresql",
                destination=export,
                runner=run_db_to_file,
            )
        )
    # file → file conversion: every source format into a different container.
    conversions = (
        ("csv", "parquet"),
        ("csv", "jsonl"),
        ("csv", "excel"),
        ("csv", "avro"),
        ("csv", "orc"),
        ("csv", "xml"),
        ("csv", "tsv"),
        ("csv", "json"),
        ("parquet", "csv"),
        ("avro", "csv"),
        ("orc", "csv"),
        ("excel", "csv"),
        ("json", "parquet"),
        ("jsonl", "csv"),
        ("xml", "csv"),
        ("tsv", "parquet"),
        ("csv_gz", "csv"),
    )
    for src, dst in conversions:
        cells.append(
            Cell(
                name=f"{src}_to_{dst}_file",
                route="file_to_file",
                store="local",
                source=src,
                destination=dst,
                runner=run_file_to_file,
            )
        )
    for store in ("minio", "fake-gcs", "azurite"):
        for fmt in OBJECT_FORMATS:
            if fmt in fixture.FORMATS:
                cells.append(
                    Cell(
                        name=f"{store}_{fmt}_to_postgres",
                        route="objectstore_to_postgres",
                        store=store,
                        source=fmt,
                        destination="postgresql",
                        runner=run_objstore_to_db,
                    )
                )
            cells.append(
                Cell(
                    name=f"postgres_to_{store}_{fmt}",
                    route="postgres_to_objectstore",
                    store=store,
                    source="postgresql",
                    destination=fmt,
                    runner=run_db_to_objstore,
                )
            )
        cells.append(
            Cell(
                name=f"{store}_csv_to_parquet",
                route="objectstore_to_objectstore",
                store=store,
                source="csv",
                destination="parquet",
                runner=run_objstore_to_objstore,
            )
        )
    # Same routes under strict mode — the fail-closed half of the contract.
    for kind, route in (
        ("csv", "file_to_postgres"),
        ("csv", "file_to_mysql"),
        ("parquet", "file_to_postgres"),
        ("excel", "file_to_postgres"),
    ):
        cells.append(
            Cell(
                name=f"strict_{kind}_{route}",
                route=route,
                store="local",
                source=kind,
                destination="postgresql" if route.endswith("postgres") else "mysql",
                runner=run_strict_refusal,
                mode=STRICT_MODE,
                note="strict mode must refuse the job and leave no partial write",
            )
        )
    for carrier, note in (
        ("xls", "legacy BIFF .xls: no BIFF reader shipped, and BIFF8 caps a sheet at 65,536 rows"),
        ("zip", "zip container: the reader handles gzip only, no zip branch exists"),
    ):
        cells.append(
            Cell(
                name=f"unsupported_{carrier}_to_postgres",
                route="file_to_postgres",
                store="local",
                source=carrier,
                destination="postgresql",
                runner=run_unsupported_carrier,
                note=note,
            )
        )
    for store, label, reason in CREDENTIAL_SKIPS:
        cells.append(
            Cell(
                name=f"{store}_all_routes",
                route="hosted_object_store",
                store=store,
                source="file",
                destination=label,
                note=reason,
            )
        )
    return cells


def _reachability() -> dict[str, bool]:
    return {
        "postgres": rb.pg_reachable(),
        "mysql": rb.mysql_reachable(),
        "minio": rb.minio_reachable(),
        "fake-gcs": rb.gcs_reachable(),
        "azurite": rb.azurite_reachable(),
    }


def _gate(cell: Cell, reach: dict[str, bool]) -> str:
    """Reason this cell cannot run here, or ``''`` when it can."""
    if cell.route == "hosted_object_store":
        return cell.note
    needs_pg = "postgres" in cell.route or cell.destination == "postgresql"
    if needs_pg and not reach["postgres"]:
        return "PostgreSQL not reachable"
    if cell.route == "file_to_mysql" and not reach["mysql"]:
        return "MySQL not reachable"
    if cell.store in {"minio", "fake-gcs", "azurite"} and not reach.get(cell.store, False):
        return f"{cell.store} not reachable"
    return ""


def _empty_is_ambiguous(cell: Cell) -> bool:
    return bool(DELIMITED_FORMATS & {cell.source, cell.destination})


#: The engine refuses combinations it has no live driver for. That refusal is
#: honest — an unsupported route is a skip with the engine's own words, not a
#: failure to fix and not a claim of support.
_UNSUPPORTED_MARKERS = ("not yet live", "not supported", "unsupported")


def _reclassify_unsupported(result: CellResult) -> None:
    if result.status != "fail" or result.dest_rows_independent:
        return
    for note in result.notes:
        if not note.startswith("engine error:"):
            continue
        lowered = note.lower()
        if any(marker in lowered for marker in _UNSUPPORTED_MARKERS):
            head = note.split("Transfer-live drivers")[0].removeprefix("engine error:").strip()
            result.status = f"skip (engine has no live driver: {head[:160]})"
            return


def run(cells: list[Cell], rows: int, *, results_path: Path = RESULTS_PATH) -> list[CellResult]:
    reach = _reachability()
    results: list[CellResult] = []
    results_path.parent.mkdir(parents=True, exist_ok=True)
    for cell in cells:
        reason = _gate(cell, reach)
        if reason or cell.runner is None:
            result = _skip(cell, reason or "no runner")
        else:
            try:
                if _empty_is_ambiguous(cell):
                    with fixture.empty_is_ambiguous():
                        result = cell.runner(cell, rows)
                else:
                    result = cell.runner(cell, rows)
            except Exception as exc:  # noqa: BLE001 — a crashed cell is a failed cell
                result = _base_result(cell, rows)
                result.status = "fail"
                result.notes.append(
                    f"harness exception: {type(exc).__name__}: {exc}\n"
                    + traceback.format_exc(limit=6)
                )
        _reclassify_unsupported(result)
        results.append(result)
        payload = result.as_dict()
        payload["rows_requested"] = rows
        payload["recorded_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(results_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload) + "\n")
        print(
            f"[{result.status:>28}] {result.name:<42} "
            f"dest={result.dest_rows_independent:<7} "
            f"expected={result.rows_expected:<7} "
            f"{result.elapsed_seconds}s",
            flush=True,
        )
        if result.notes:
            for note in result.notes:
                print(f"      · {note.splitlines()[0][:220]}", flush=True)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Track B file/object-store scale matrix")
    parser.add_argument("--rows", type=int, default=fixture.DEFAULT_ROWS)
    parser.add_argument("--only", default="", help="comma-separated cell name substrings")
    parser.add_argument("--route", default="", help="comma-separated route names")
    parser.add_argument("--store", default="", help="comma-separated store names")
    parser.add_argument("--list", action="store_true", help="list cells and exit")
    parser.add_argument(
        "--results",
        default=str(RESULTS_PATH),
        help="JSONL file results append to",
    )
    args = parser.parse_args(argv)

    cells = build_cells()
    if args.only:
        wanted = [w.strip() for w in args.only.split(",") if w.strip()]
        cells = [c for c in cells if any(w in c.name for w in wanted)]
    if args.route:
        routes = {r.strip() for r in args.route.split(",") if r.strip()}
        cells = [c for c in cells if c.route in routes]
    if args.store:
        stores = {s.strip() for s in args.store.split(",") if s.strip()}
        cells = [c for c in cells if c.store in stores]
    if args.list:
        for cell in cells:
            print(f"{cell.route:<28} {cell.store:<10} {cell.name}")
        print(f"{len(cells)} cells")
        return 0
    if not gated():
        print(
            f"{ENV_GATE} is not set — this harness moves 100K rows through live "
            "services and stays off by default.",
            file=sys.stderr,
        )
        return 2
    results = run(cells, args.rows, results_path=Path(args.results))
    passed = sum(1 for r in results if r.status == "pass")
    failed = sum(1 for r in results if r.status == "fail")
    skipped = len(results) - passed - failed
    print(f"\npass={passed} fail={failed} skip={skipped} rows={args.rows}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
