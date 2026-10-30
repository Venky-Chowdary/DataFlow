"""CDC catch-up value proof: source-row fingerprints must be on the dest.

COUNT matching does not see an in-place update. This module fingerprints
each source row through the destination bind and checks that fingerprint
is present on the destination. Extra dest rows do not fail (a changelog
is not the source table; leftover MERGE is a no-op).

Identity mappings are scanned, and so are carries that only declare how a
cell is read: a typed cast, the ``json`` carrier, and ``assume_timezone:ZONE``
(the declared zone is attached to the source cell before it is fingerprinted,
exactly as the writer attaches it). Any other transform is not a cell proof, so
that route stays on ``cdc_source_image_count``. A scan that starts and does not
finish raises ``CdcValueScanIncomplete`` — row count must not complete the
job. A finished scan with a missing fingerprint fails the job.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from typing import Any


_IDENTITY_TRANSFORMS = frozenset(
    {"", "none", "identity", "passthrough"}
)
#: Carries the writer applies without changing the value the source holds.
_CARRIER_TRANSFORMS = frozenset({"json"})
#: Carries the writer re-renders into one canonical spelling of the same value
#: (a GUID's case and braces). The source cell is put through the same
#: transform before it is fingerprinted, exactly as the writer binds it, so a
#: SQL Server ``6F9619FF-...`` and the destination's ``6f9619ff-...`` are one
#: value, and a different GUID is still a missing fingerprint.
_CANONICAL_REWRITES = frozenset({"uuid"})
_PAGE = 500
_MAX_PAGES = 400


class CdcValueScanIncomplete(RuntimeError):
    """The source image could be scanned and was not.

    Row count is not a substitute. The job must fail closed.
    """


@dataclass(frozen=True)
class CdcValueProof:
    source_digest: str
    dest_digest: str
    missing: int
    source_rows: int
    dest_rows: int
    #: Out-of-window drift (D188) when the proof was scoped to a poll window.
    drift: dict[str, Any] = field(default_factory=dict)

    @property
    def matched(self) -> bool:
        return self.missing == 0 and bool(self.source_digest) and (
            self.source_digest == self.dest_digest
        )


def _identity_pairs(mappings: list[dict[str, Any]] | None) -> list[tuple[str, str]] | None:
    """``(source, dest)`` when every mapped column is a pure carry.

    A transform is supposed to change the value. Comparing those cells
    would fail a correct route, so the scan is declined. A typed cast the
    plan stamps (``integer``, ``datetime``…) only carries the value into the
    destination type, and ``canonical_checksum`` renders both sides in that
    type, so it is compared; a cast that did change a cell still leaves a
    source fingerprint missing.
    """
    from services.decision_kernel.findings import TYPED_CAST_TRANSFORMS
    from services.transform_engine import ASSUME_TIMEZONE_PREFIX

    if not mappings:
        return None
    pairs: list[tuple[str, str]] = []
    for raw in mappings:
        if not isinstance(raw, dict):
            return None
        if raw.get("intentional_omit"):
            continue
        source = str(raw.get("source") or "").strip()
        target = str(raw.get("target") or raw.get("source") or "").strip()
        transform = str(raw.get("transform") or "").strip().lower()
        if not source or not target:
            return None
        if (
            transform not in _IDENTITY_TRANSFORMS
            and transform not in TYPED_CAST_TRANSFORMS
            and transform not in _CARRIER_TRANSFORMS
            and transform not in _CANONICAL_REWRITES
            and not transform.startswith(ASSUME_TIMEZONE_PREFIX)
        ):
            return None
        pairs.append((source, target))
    return pairs or None


def _declared_carries(mappings: list[dict[str, Any]] | None) -> dict[str, str]:
    """Destination column -> the read-side carry the writer applies to it.

    ``assume_timezone:ZONE`` attaches the declared zone; ``uuid`` renders the
    canonical GUID spelling.
    """
    from services.transform_engine import ASSUME_TIMEZONE_PREFIX

    zones: dict[str, str] = {}
    for raw in mappings or []:
        if not isinstance(raw, dict) or raw.get("intentional_omit"):
            continue
        transform = str(raw.get("transform") or "").strip()
        if transform.lower() in _CANONICAL_REWRITES:
            transform = transform.lower()
        if (
            transform.lower().startswith(ASSUME_TIMEZONE_PREFIX)
            or transform in _CANONICAL_REWRITES
        ):
            target = str(raw.get("target") or raw.get("source") or "").strip()
            zones[target.lower()] = transform
    return zones


def _apply_carries(
    rows: list[dict[str, Any]], zones: dict[str, str]
) -> list[dict[str, Any]]:
    """Read each source cell the way the writer bound it.

    A zoneless source instant gets the zone the job declared for it, and a GUID
    gets its canonical spelling. The destination holds what the writer built
    from that declaration, so the source cell is read the same way before it is
    fingerprinted. A value the transform cannot read is left as it is and fails
    the comparison.
    """
    if not zones:
        return rows
    from services.transform_engine import apply_transform

    out: list[dict[str, Any]] = []
    for row in rows:
        attached = dict(row)
        for column, transform in zones.items():
            for key in list(attached):
                if str(key).lower() != column or attached[key] is None:
                    continue
                value, error = apply_transform(str(attached[key]), transform)
                if error is None:
                    attached[key] = value
        out.append(attached)
    return out


def _fingerprint_row(
    row: dict[str, Any],
    columns: list[str],
    *,
    engine: str,
    dest_types: dict[str, str] | None,
) -> str:
    from services.reconciliation import canonical_checksum

    return canonical_checksum(
        [row],
        columns,
        dest_db_type=engine,
        dest_types=dest_types,
    )


def compare_row_sets(
    source_rows: list[dict[str, Any]],
    dest_rows: list[dict[str, Any]],
    columns: list[str],
    *,
    engine: str,
    dest_types: dict[str, str] | None = None,
) -> CdcValueProof:
    """Source fingerprints must be a subset of dest fingerprints."""
    src = {
        _fingerprint_row(row, columns, engine=engine, dest_types=dest_types)
        for row in source_rows
    }
    dst = {
        _fingerprint_row(row, columns, engine=engine, dest_types=dest_types)
        for row in dest_rows
    }
    missing = src - dst
    source_digest = hashlib.sha256(
        "\n".join(sorted(src)).encode()
    ).hexdigest()
    if missing:
        dest_digest = hashlib.sha256(
            "\n".join(sorted(dst)).encode()
        ).hexdigest()
        if dest_digest == source_digest:
            dest_digest = "0" * 64
    else:
        dest_digest = source_digest
    return CdcValueProof(
        source_digest=source_digest,
        dest_digest=dest_digest,
        missing=len(missing),
        source_rows=len(source_rows),
        dest_rows=len(dest_rows),
    )


def _as_dicts(headers: list[str], rows: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
            continue
        cells = list(row)
        out.append(
            {
                headers[i]: cells[i] if i < len(cells) else None
                for i in range(len(headers))
            }
        )
    return out


def _rename(rows: list[dict[str, Any]], pairs: list[tuple[str, str]]) -> list[dict[str, Any]]:
    renamed: list[dict[str, Any]] = []
    for row in rows:
        folded = {str(k).lower(): v for k, v in row.items()}
        renamed.append(
            {
                dest: folded.get(src.lower())
                for src, dest in pairs
            }
        )
    return renamed


def _scan_table(
    db_type: str,
    cfg: dict[str, Any],
    table: str,
    columns: list[str],
) -> list[dict[str, Any]] | None:
    from src.transfer.batch_readers import CONTINUATION_KWARG, _read_batch_impl
    from src.transfer.connector_capabilities import resolve_driver_type

    token_kwarg = CONTINUATION_KWARG.get(resolve_driver_type(db_type), "")
    token: Any = None
    offset = 0
    collected: list[dict[str, Any]] = []
    for _page in range(_MAX_PAGES):
        result = _read_batch_impl(
            db_type,
            cfg,
            table,
            columns,
            offset,
            _PAGE,
            **({token_kwarg: token} if token_kwarg and token is not None else {}),
        )
        batch = result[0] if isinstance(result, tuple) else result
        if token_kwarg:
            token = result[1] if isinstance(result, tuple) and len(result) == 2 else None
        headers = [str(h) for h in (getattr(batch, "headers", None) or [])]
        rows = list(getattr(batch, "rows", None) or [])
        if not rows:
            return collected
        if not headers:
            return None
        collected.extend(_as_dicts(headers, rows))
        offset += len(rows)
        if len(rows) < _PAGE:
            return collected
        if token_kwarg and token is None:
            return None
    return None


def prove_cdc_values(
    *,
    source_type: str,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    mappings: list[dict[str, Any]] | None,
    dest_types: dict[str, str] | None = None,
    window: Any = None,
    key_columns: list[str] | None = None,
) -> CdcValueProof | None:
    """Scan both tables.

    ``None`` means the mapping is not an identity carry, so cell compare
    would reject a correct transform. A scan that cannot finish raises
    ``CdcValueScanIncomplete`` instead of pretending the row count is enough.

    ``window`` is the :class:`~services.incremental_drift.RunWindow` a query-CDC
    poll selected (source column names). A poll reads ``cursor > watermark``;
    a source row it did not select — edited without moving the cursor — is
    not this run's to prove. Only in-window rows must be on the destination;
    the rows before the window are compared by ``key_columns`` and returned as
    out-of-window drift, which the caller reports instead of failing (D188).
    """
    if not source_table or not dest_table:
        raise CdcValueScanIncomplete(
            "CDC value scan needs a source table and a destination table."
        )
    pairs = _identity_pairs(mappings)
    if pairs is None:
        return None
    columns = [dest for _src, dest in pairs]
    source_columns = [src for src, _dest in pairs]
    try:
        source_rows = _scan_table(source_type, source_cfg, source_table, source_columns)
        dest_rows = _scan_table(dest_type, dest_cfg, dest_table, columns)
    except Exception as exc:
        raise CdcValueScanIncomplete(
            f"CDC value scan failed: {exc}"
        ) from exc
    if source_rows is None or dest_rows is None:
        raise CdcValueScanIncomplete(
            "CDC value scan did not finish. Row count is not a cell proof."
        )
    carries = _declared_carries(mappings)
    before: list[dict[str, Any]] = []
    later: list[dict[str, Any]] = []
    if window is not None and _window_readable(window, source_columns):
        from services.incremental_drift import BEFORE_WINDOW, IN_WINDOW

        inside: list[dict[str, Any]] = []
        for row in source_rows:
            position = window.position(row)
            if position == IN_WINDOW:
                inside.append(row)
            elif position == BEFORE_WINDOW:
                before.append(row)
            else:
                # Committed after the poll's high-water mark: the next run's.
                later.append(row)
        source_rows = inside
    source_named = _apply_carries(_rename(source_rows, pairs), carries)
    dest_named = _rename(dest_rows, [(column, column) for column in columns])
    proof = compare_row_sets(
        source_named,
        dest_named,
        columns,
        engine=dest_type,
        dest_types=dest_types,
    )
    if window is None or not key_columns or not (before or later):
        return proof
    to_target = {src.lower(): dest for src, dest in pairs}
    dest_keys = [to_target.get(k.lower(), "") for k in key_columns]
    if not all(dest_keys):
        return proof
    from services.incremental_drift import measure_drift

    owned = {
        _row_key(row, dest_keys)
        for row in source_named + _rename(later, pairs)
    }
    drift = measure_drift(
        source_rows=_apply_carries(_rename(before, pairs), carries),
        target_rows=dest_named,
        columns=columns,
        key_cols=dest_keys,
        dest_db_type=dest_type,
        dest_types=dest_types,
        skip_keys=owned,
    )
    return replace(proof, drift=drift) if drift else proof


def _window_readable(window: Any, source_columns: list[str]) -> bool:
    """The window's cursor is a scanned column, so each row can be placed."""
    names = {str(c).lower() for c in source_columns}
    return bool(getattr(window, "cursor", "")) and str(window.cursor).lower() in names


def _row_key(row: dict[str, Any], key_columns: list[str]) -> str:
    """The key ``measure_drift`` indexes a destination-named row by."""
    from services.verification_ladder import _index_by_pk

    keys = list(_index_by_pk([row], list(key_columns)))
    return keys[0] if keys else ""


def identity_rows_missing_on_dest(
    *,
    source_type: str,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    mappings: list[dict[str, Any]] | None,
    dest_types: dict[str, str] | None = None,
) -> list[dict[str, Any]] | None:
    """Source rows whose identity fingerprint is not on the destination.

    ``None`` when the mapping is not an identity carry. Used to upsert a gap
    update the binlog resume already stepped past (DEF-C-005). At-least-once:
    this is the current source image, not a claim the missed event was
    replayed in order.
    """
    if not source_table or not dest_table:
        raise CdcValueScanIncomplete(
            "CDC value scan needs a source table and a destination table."
        )
    pairs = _identity_pairs(mappings)
    if pairs is None:
        return None
    columns = [dest for _src, dest in pairs]
    source_columns = [src for src, _dest in pairs]
    try:
        source_rows = _scan_table(source_type, source_cfg, source_table, source_columns)
        dest_rows = _scan_table(dest_type, dest_cfg, dest_table, columns)
    except Exception as exc:
        raise CdcValueScanIncomplete(f"CDC value scan failed: {exc}") from exc
    if source_rows is None or dest_rows is None:
        raise CdcValueScanIncomplete(
            "CDC value scan did not finish. Row count is not a cell proof."
        )
    renamed_source = _apply_carries(
        _rename(source_rows, pairs), _declared_carries(mappings)
    )
    dest_named = _rename(dest_rows, [(column, column) for column in columns])
    present = {
        _fingerprint_row(row, columns, engine=dest_type, dest_types=dest_types)
        for row in dest_named
    }
    missing: list[dict[str, Any]] = []
    for raw, renamed in zip(source_rows, renamed_source):
        fp = _fingerprint_row(renamed, columns, engine=dest_type, dest_types=dest_types)
        if fp not in present:
            missing.append(raw)
    return missing


def _mapped_pairs(mappings: list[dict[str, Any]] | None) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for raw in mappings or []:
        if not isinstance(raw, dict) or raw.get("intentional_omit"):
            continue
        source = str(raw.get("source") or "").strip()
        target = str(raw.get("target") or raw.get("source") or "").strip()
        if source and target:
            pairs.append((source, target))
    return pairs


def _key_tuple(row: dict[str, Any], columns: list[str]) -> tuple[str, ...]:
    folded = {str(key).lower(): value for key, value in row.items()}
    out: list[str] = []
    for column in columns:
        value = folded.get(column.lower())
        if value is None:
            out.append("")
            continue
        out.append(str(value).strip())
    return tuple(out)


def rows_absent_by_primary_key(
    *,
    source_type: str,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    mappings: list[dict[str, Any]] | None,
    primary_key: str,
) -> list[dict[str, Any]] | None:
    """Source rows whose primary key is not on the destination.

    Identity fingerprints decline when a column is transformed
    (timestamptz → datetime). Inserts the log already stepped past are
    still absent by key. The returned rows are the current source image,
    applied at-least-once. This is not a replay of the missed event.
    ``None`` when the key is not fully mapped.
    """
    if not source_table or not dest_table:
        raise CdcValueScanIncomplete(
            "CDC value scan needs a source table and a destination table."
        )
    wanted = [
        part.strip()
        for part in str(primary_key or "").replace(";", ",").split(",")
        if part.strip()
    ]
    if not wanted:
        return None
    pairs = _mapped_pairs(mappings)
    by_source = {source.lower(): (source, target) for source, target in pairs}
    key_pairs: list[tuple[str, str]] = []
    for name in wanted:
        hit = by_source.get(name.lower())
        if hit is None:
            return None
        key_pairs.append(hit)
    if not pairs:
        return None
    source_columns = [source for source, _target in pairs]
    dest_columns = [target for _source, target in pairs]
    source_keys = [source for source, _target in key_pairs]
    dest_keys = [target for _source, target in key_pairs]
    try:
        source_rows = _scan_table(source_type, source_cfg, source_table, source_columns)
        dest_rows = _scan_table(dest_type, dest_cfg, dest_table, dest_columns)
    except Exception as exc:  # noqa: BLE001 — any scan failure is an incomplete proof
        raise CdcValueScanIncomplete(f"CDC value scan failed: {exc}") from exc
    if source_rows is None or dest_rows is None:
        raise CdcValueScanIncomplete(
            "CDC value scan did not finish. Row count is not a cell proof."
        )
    present = {_key_tuple(row, dest_keys) for row in dest_rows}
    missing: list[dict[str, Any]] = []
    for row in source_rows:
        if _key_tuple(row, source_keys) not in present:
            missing.append(row)
    return missing
