"""Bounded-memory identity uniqueness.

A payload probe that keeps every key in one Counter, and that re-reads the
file from byte 0 for each page, fails a 2M-row file: the scan caps at 1M
rows and one gate spends minutes re-parsing the prefix. This scanner reads
the spilled object once and hash-partitions keys onto disk. One partition
is counted at a time, so a 10M-row file stays inside a fixed memory budget.
"""

from __future__ import annotations

import tempfile
import zlib
from pathlib import Path
from typing import Any, Iterator

_PARTITIONS = 32
_BATCH = 50_000


class PartitionedUniqueness:
    """External hash partitions. Memory is one partition, not the population."""

    def __init__(self, partitions: int = _PARTITIONS) -> None:
        self.partitions = max(1, int(partitions))
        self.rows = 0
        self._tmp = tempfile.TemporaryDirectory(prefix="df-uniq-")
        self._paths = [
            Path(self._tmp.name) / f"p{i:03d}" for i in range(self.partitions)
        ]
        self._handles = [path.open("w", encoding="utf-8") for path in self._paths]
        self._closed = False
        self.unique_keys = 0
        self.extra_rows = 0

    def add(self, key: tuple[str, ...]) -> None:
        blob = "\x1f".join(key)
        slot = zlib.crc32(blob.encode("utf-8")) % self.partitions
        handle = self._handles[slot]
        handle.write(blob)
        handle.write("\n")
        self.rows += 1

    def _seal(self) -> None:
        if self._closed:
            return
        self._closed = True
        for handle in self._handles:
            handle.close()
        self._handles = []

    def findings(
        self,
        columns: list[str],
        limit: int,
        *,
        identity_cell: Any,
        finding_value: Any,
    ) -> list[dict[str, Any]]:
        self._seal()
        out: list[dict[str, Any]] = []
        cap = max(1, int(limit))
        extra = 0
        unique = 0
        for path in self._paths:
            counts: dict[str, int] = {}
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    token = line[:-1] if line.endswith("\n") else line
                    counts[token] = counts.get(token, 0) + 1
            for token, count in counts.items():
                unique += 1
                if count < 2:
                    continue
                # Example findings stay capped. The extra-row total does not:
                # stopping here reported the example cap (5) as the population.
                extra += count - 1
                if len(out) >= cap:
                    continue
                parts = token.split("\x1f")
                values = [identity_cell(part) for part in parts]
                out.append(
                    {
                        "value": finding_value(columns, tuple(values)),
                        "values": values,
                        "columns": list(columns),
                        "count": int(count),
                    }
                )
        self.unique_keys = unique
        self.extra_rows = extra
        return out

    def close(self) -> None:
        self._seal()
        self._tmp.cleanup()


def annotate_identity_findings(
    findings: list[dict[str, Any]],
    scanner: PartitionedUniqueness,
    *,
    complete: bool,
) -> list[dict[str, Any]]:
    """Attach the scan total the example list does not carry.

    ``findings`` is capped at the probe limit. A finished scan knows the
    population (extra rows, unique keys, rows scanned). A scan that stopped
    early only knows the rows it read, so those counts are marked as a sample
    and must not be described as the file.
    """
    extra = int(scanner.extra_rows or 0)
    unique = int(scanner.unique_keys or 0)
    scanned = int(scanner.rows or 0)
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        if complete:
            finding["population_extra"] = extra
            finding["population_unique"] = unique
            finding["population_scanned"] = scanned
            finding["population_complete"] = True
        else:
            finding["sample_only"] = True
            finding["sample_extra"] = extra
            finding["sample_scanned"] = scanned
    return findings


def iter_identity_records(path: Path, filename: str) -> Iterator[dict[str, Any]]:
    """One pass over the spilled object, using the transfer's file iterator."""
    try:
        from services.file_parser import FileParser
    except ImportError:
        from src.services.file_parser import FileParser
    try:
        from src.transfer.file_stream import _batch_iterator_for_type
    except ImportError:
        from transfer.file_stream import _batch_iterator_for_type

    file_type = FileParser.detect_file_type(filename, b"")
    for batch in _batch_iterator_for_type(
        file_type, path, _BATCH, declared_name=filename
    ):
        for record in batch:
            if isinstance(record, dict):
                yield record


def scan_identity_file(
    path: Path,
    filename: str,
    pk_columns: list[str],
    *,
    limit: int = 5,
    normalize_key: Any,
    identity_cell: Any,
    finding_value: Any,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Prove uniqueness for every row of a spilled file. ``complete`` is True.

    The scan finishes the file. A cap is not a proof, so this does not stop
    at a row budget and then claim the tail was checked.
    """
    scanner = PartitionedUniqueness()
    try:
        saw_header = False
        for record in iter_identity_records(path, filename):
            if not saw_header:
                missing = [col for col in pk_columns if col not in record]
                if missing:
                    raise ValueError(
                        f"identity column(s) {', '.join(missing)} are not present in {filename}"
                    )
                saw_header = True
            scanner.add(tuple(normalize_key(record.get(col)) for col in pk_columns))
        findings = scanner.findings(
            pk_columns,
            limit,
            identity_cell=identity_cell,
            finding_value=finding_value,
        )
        annotate_identity_findings(findings, scanner, complete=True)
        return findings, scanner.rows, True
    finally:
        scanner.close()
