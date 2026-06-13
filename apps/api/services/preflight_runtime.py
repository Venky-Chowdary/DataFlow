"""Runtime preflight adapters — real dry-run, uniqueness probe, capacity."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from preflight.models import PreflightContext, TransferPlan

from services.file_parser import get_file
from services.object_store import storage_status
from services.transform_engine import dry_run_sample


class RuntimePreflightContext(PreflightContext):
    def __init__(
        self,
        plan: TransferPlan,
        *,
        file_id: str | None = None,
        file_size_bytes: int = 0,
        source_db: dict[str, Any] | None = None,
        source_table: str = "",
    ) -> None:
        super().__init__(plan=plan)
        self.file_id = file_id
        self.file_size_bytes = file_size_bytes
        self.source_db = source_db
        self.source_table = source_table

    def _load_sample(self) -> tuple[list[str], list[list[str]], dict[str, str]]:
        if self.file_id:
            record = get_file(self.file_id)
            if not record:
                return [], [], {}
            from services.csv_profiler import parse_csv_preview

            path = Path(record["path"])
            headers, rows, _, _ = parse_csv_preview(path.read_bytes())
            column_types = {c["name"]: c["inferred_type"] for c in record["columns"]}
            return headers, rows, column_types

        if self.source_db and self.source_table:
            from connectors.postgresql_reader import read_table_sample

            headers, rows = read_table_sample(
                host=self.source_db.get("host", ""),
                port=self.source_db.get("port", 5432),
                database=self.source_db.get("database", ""),
                username=self.source_db.get("username", ""),
                password=self.source_db.get("password", ""),
                schema=self.source_db.get("schema", "public"),
                connection_string=self.source_db.get("connection_string", ""),
                ssl=self.source_db.get("ssl", True),
                table=self.source_table,
                limit=100,
            )
            column_types = {c.name: c.inferred_type for c in self.plan.source.columns}
            return headers, rows, column_types

        return [], [], {}

    def probe_unique_constraint(self, columns: list[str]) -> list[dict[str, Any]]:
        if not columns:
            return []
        headers, rows, _ = self._load_sample()
        if not headers:
            return []

        idx_map = {h: i for i, h in enumerate(headers)}
        pk_col = columns[0]
        idx = idx_map.get(pk_col)
        if idx is None:
            for m in self.plan.mappings:
                if m.target == pk_col:
                    idx = idx_map.get(m.source)
                    break
        if idx is None:
            return []

        values = [row[idx] for row in rows if idx < len(row) and row[idx].strip()]
        counts = Counter(values)
        return [{"value": v, "count": c} for v, c in counts.items() if c > 1][:10]

    def run_dry_run(self, sample_size: int = 1000) -> tuple[bool, list[str]]:
        headers, rows, column_types = self._load_sample()
        if not headers:
            return False, ["No source sample available for dry-run"]

        mapping_dicts = [
            {
                "source": m.source,
                "target": m.target,
                "transform": m.transform,
            }
            for m in self.plan.mappings
        ]
        return dry_run_sample(
            headers=headers,
            sample_rows=rows,
            mappings=mapping_dicts,
            column_types=column_types,
            sample_size=min(sample_size, len(rows)),
        )


def compute_capacity(file_size_bytes: int, estimated_rows: int = 0) -> tuple[int, int]:
    """Returns (estimated_bytes_needed, available_staging_bytes)."""
    if file_size_bytes:
        estimated = int(file_size_bytes * 1.35)
    elif estimated_rows:
        estimated = estimated_rows * 256
    else:
        estimated = 1_048_576

    storage = storage_status()
    if storage.get("available"):
        available = 50 * 1024 * 1024 * 1024
    else:
        upload_dir = Path(__file__).resolve().parents[1] / "uploads"
        try:
            import shutil

            usage = shutil.disk_usage(upload_dir)
            available = int(usage.free * 0.9)
        except Exception:
            available = 10 * 1024 * 1024
    return estimated, available
