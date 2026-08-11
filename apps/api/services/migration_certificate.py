"""Per-run Migration Certificate — the operator-facing face of the proof pack.

A signed proof pack is a diligence artifact for a machine; a migration lead
needs one page that answers "how many rows did I have, how many landed, where
are the rest, and is that number proven?". This module renders exactly that
from evidence that already exists (Gate-8 reconciliation, the quarantine DLQ,
risk contracts, Map→DDL hashes) — it derives no new correctness facts.

The one judgement it *does* add is **row conservation**: every source row must
be accounted for as written, quarantined, or intentionally skipped. An
unexplained shortfall is the definition of silent data loss, so the certificate
refuses to claim proof when the arithmetic does not close, even if Gate-8 was
otherwise happy.
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from services.signed_proof_pack import export_proof_pack_for_job, sign_body, verify_body

logger = logging.getLogger(__name__)

CERTIFICATE_VERSION = 1

# Assurance levels that justify the word "proven" on a certificate. Anything
# else (sample, writer_ack, none) is reported verbatim without the claim.
_PROVEN_ASSURANCE = "full_checksum"


class RowAccountingError(ValueError):
    """Raised when a caller asks for a certificate from an unusable job doc."""


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def quarantine_reason_breakdown(
    rejected_details: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Group quarantined rows by reason, most frequent first.

    The reason string is what the operator remediates against, so it is grouped
    verbatim rather than bucketed into invented categories.
    """
    counts: Counter[str] = Counter()
    columns: dict[str, set[str]] = {}
    for row in rejected_details or []:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("reason") or "unspecified").strip() or "unspecified"
        counts[reason] += 1
        col = str(row.get("column") or "").strip()
        if col:
            columns.setdefault(reason, set()).add(col)
    return [
        {
            "reason": reason,
            "rows": n,
            "columns": sorted(columns.get(reason, set()))[:12],
        }
        for reason, n in counts.most_common()
    ]


def quarantine_burndown(job_id: str, *, limit: int = 500) -> dict[str, Any]:
    """Quarantined vs replayed rows for a job, from durable DLQ events.

    Answers "1,000 rows failed on Monday — how many are still open?". Returns
    ``available=False`` rather than zeros when the DLQ cannot be read, so an
    unreadable ledger is never rendered as a clean burn-down.
    """
    try:
        from services.quarantine_dlq import list_dlq_events

        events = list_dlq_events(job_id=job_id, limit=limit)
    except Exception as exc:
        logger.warning("quarantine burn-down unavailable for %s: %s", job_id, exc)
        return {
            "available": False,
            "note": "Quarantine DLQ could not be read — open-row count is unknown.",
        }
    quarantined = 0
    replayed = 0
    skip_audit = 0
    for ev in events:
        action = str(ev.get("action") or "")
        rows = _as_int(ev.get("rows"))
        if action in ("quarantine", "quarantine_chunk"):
            quarantined += rows
        elif action == "replay":
            replayed += rows
        elif action == "skip_audit":
            skip_audit += rows
    return {
        "available": True,
        "quarantined": quarantined,
        "replayed": replayed,
        "open": max(0, quarantined - replayed),
        "skip_audit": skip_audit,
        "events": len(events),
    }


def row_accounting(job: dict[str, Any]) -> dict[str, Any]:
    """Conservation ledger: read = written + quarantined + skipped.

    ``rows_read`` comes from Gate-8's source count, which is the only figure
    measured against the source rather than reported by the writer. When it is
    absent the ledger is marked unbalanced-unknown: an unmeasured source count
    cannot be used to prove nothing was lost.
    """
    recon = _dict(job.get("reconciliation"))
    dest = _dict(job.get("destination_summary"))

    rows_written = _as_int(job.get("records_processed") or dest.get("rows"))
    rows_quarantined = _as_int(
        recon.get("rejected_rows") or job.get("rejected_rows") or dest.get("rejected")
    )
    rows_skipped = _as_int(recon.get("rows_skipped") or dest.get("rows_skipped"))
    coerced_null_rows = _as_int(
        recon.get("coerced_null_rows") or job.get("coerced_null_rows")
    )

    raw_read = recon.get("source_rows")
    rows_read = _as_int(raw_read) if raw_read is not None else None

    ledger: dict[str, Any] = {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_quarantined": rows_quarantined,
        "rows_skipped": rows_skipped,
        "rows_coerced_null": coerced_null_rows,
        "rows_read_source": "gate8_source_count" if rows_read is not None else "unmeasured",
    }

    if rows_read is None:
        ledger["balanced"] = False
        ledger["unaccounted"] = None
        ledger["note"] = (
            "Source row count was not measured for this run, so no conservation "
            "check is possible. Rows written cannot be compared against rows read."
        )
        return ledger

    unaccounted = rows_read - (rows_written + rows_quarantined + rows_skipped)
    ledger["unaccounted"] = unaccounted
    ledger["balanced"] = unaccounted == 0
    if unaccounted > 0:
        ledger["note"] = (
            f"{unaccounted} source row(s) are neither written, quarantined, nor "
            "skipped. Treat as potential silent loss and investigate before "
            "accepting this run."
        )
    elif unaccounted < 0:
        ledger["note"] = (
            f"{abs(unaccounted)} more row(s) are accounted for than were read — "
            "duplicate writes or double-counted rejects. Investigate before "
            "accepting this run."
        )
    else:
        ledger["note"] = "Every source row is written, quarantined, or skipped."
    return ledger


def _rejected_details(
    job_id: str, *, embedded: list[Any], expected: int
) -> list[dict[str, Any]]:
    """Prefer the durable DLQ when the job doc only carries a truncated sample.

    Job documents cap embedded rejects, so grouping them alone would under-report
    reasons on exactly the large runs where the breakdown matters most.
    """
    rows = [r for r in embedded if isinstance(r, dict)]
    if expected <= len(rows):
        return rows
    try:
        from services.quarantine_dlq import quarantine_details_from_dlq

        hydrated = quarantine_details_from_dlq(job_id)
    except Exception as exc:
        logger.warning("quarantine hydration failed for %s: %s", job_id, exc)
        return rows
    hydrated = [r for r in hydrated if isinstance(r, dict)]
    return hydrated if len(hydrated) > len(rows) else rows


def physical_state_findings(recon: dict[str, Any]) -> dict[str, Any]:
    """Destination state a row checksum cannot prove, as certificate evidence.

    Today that is generator watermarks: keys can be byte-identical while the
    destination sequence still points inside the migrated range, so the first
    application insert after cutover collides.
    """
    state = _dict(recon.get("physical_state"))
    identity = _dict(state.get("identity_watermark")) or {
        "verified": False,
        "reason": "no generator watermark evidence was captured for this run",
    }
    schema_objects = _dict(state.get("schema_objects")) or {
        "verified": False,
        "reason": "constraints and indexes were not compared for this run",
    }
    referential = _dict(state.get("referential_integrity")) or {
        "verified": False,
        "reason": "destination referential integrity was not scanned for this run",
    }
    return {
        "identity_watermark": identity,
        "schema_objects": {
            "verified": bool(schema_objects.get("verified")),
            "reason": str(schema_objects.get("reason") or ""),
            "absent": list(schema_objects.get("absent") or []),
            "unreadable": list(schema_objects.get("unreadable") or []),
            "aspects": _dict(schema_objects.get("aspects")),
            "advisory": _dict(schema_objects.get("advisory")),
        },
        "referential_integrity": {
            "verified": bool(referential.get("verified")),
            "reason": str(referential.get("reason") or ""),
            "orphan_rows": referential.get("orphan_rows"),
            "orphan_relations": list(referential.get("orphan_relations") or []),
            "unavailable_relations": list(
                referential.get("unavailable_relations") or []
            ),
            "relations": list(referential.get("relations") or []),
        },
    }


def _referential_blockers(physical: dict[str, Any]) -> list[str]:
    """Orphan rows are a broken database, however clean the row counts look."""
    referential = _dict(physical.get("referential_integrity"))
    orphans = [str(r) for r in referential.get("orphan_relations") or []]
    if not orphans:
        return []
    rows = referential.get("orphan_rows")
    counted = f"{rows} child row(s)" if isinstance(rows, int) else "child rows"
    return [
        f"Destination holds {counted} with no parent on {', '.join(orphans)} - "
        "referential integrity did not survive the move."
    ]


_SCHEMA_ASPECT_LABEL = {
    "primary_key": "primary key",
    "unique_constraints": "unique constraint(s)",
    "foreign_keys": "foreign key(s)",
    "indexes": "index(es)",
    "not_null": "NOT NULL constraint(s)",
    "defaults": "column default(s)",
    "check_constraints": "CHECK constraint(s)",
}


def _schema_object_blockers(physical: dict[str, Any]) -> list[str]:
    """Structure the source enforced and the destination demonstrably lacks.

    Matching checksums prove the rows, not the database around them: a load
    that lands every byte into a table whose foreign keys, uniqueness or CHECK
    constraints were never created leaves the destination accepting data the
    source would have rejected. Only *absent* aspects block — an aspect the
    catalog could not be read for stays unknown, and unknown is reported as
    unproven rather than as a violation.
    """
    schema_objects = _dict(physical.get("schema_objects"))
    absent = [str(a) for a in schema_objects.get("absent") or []]
    if not absent:
        return []
    named = ", ".join(_SCHEMA_ASPECT_LABEL.get(a, a) for a in absent)
    return [
        f"Source {named} did not survive the move - the destination accepts "
        "rows the source would have rejected."
    ]


def _foreign_key_carry_blockers(job: dict[str, Any]) -> list[str]:
    """A foreign key the run planned but could not add is not a green run."""
    summary = _dict(_dict(job.get("destination_summary")).get("foreign_keys"))
    if not summary:
        return []
    out: list[str] = []
    counts = _dict(summary.get("counts"))
    if _as_int(summary.get("integrity_violations")):
        out.append(
            f"{_as_int(summary.get('integrity_violations'))} foreign key(s) could not "
            "be enforced on the migrated rows - the destination holds child rows "
            "without a parent."
        )
    unresolved = {
        status: _as_int(count)
        for status, count in counts.items()
        if str(status) in {"failed", "unknown", "unsupported", "planned"}
        and _as_int(count)
    }
    if unresolved:
        detail = ", ".join(f"{count} {status}" for status, count in sorted(unresolved.items()))
        out.append(
            f"Foreign key carry did not complete ({detail}) - the destination is "
            "missing relationships the source enforced."
        )
    if summary.get("cycle"):
        out.append(
            "Foreign keys form a cycle ("
            + ", ".join(str(t) for t in summary.get("cycle") or [])
            + ") - deferred-constraint creation is not supported, so the cycle was "
            "not recreated."
        )
    return out


def _identity_carry_blockers(job: dict[str, Any]) -> list[str]:
    """A source key generator the destination never received is not a green run.

    The rows can be byte-identical and the counter irrelevant: if the source
    column generated its own keys and the destination column does not, the
    client's first insert after cutover has no key to use. Only a decided
    aspect blocks — an aspect that does not apply, or that was carried, says so.
    """
    fidelity = _dict(_dict(job.get("destination_summary")).get("schema_fidelity"))
    items = fidelity.get("items")
    if not isinstance(items, list):
        return []
    out: list[str] = []
    for raw in items:
        item = _dict(raw)
        if str(item.get("aspect")) != "identity_sequence":
            continue
        status = str(item.get("status") or "")
        if status not in {"unsupported", "unknown"}:
            continue
        name = str(item.get("name") or "").strip()
        column = f"'{name}'" if name and name != "*" else "the key column"
        out.append(
            f"Source key generator on {column} was not carried to the "
            f"destination ({status}): {item.get('reason') or 'no reason recorded'}"
        )
    return out


def _identity_blockers(physical: dict[str, Any]) -> list[str]:
    identity = _dict(physical.get("identity_watermark"))
    collisions = [str(c) for c in identity.get("collisions") or []]
    if not collisions:
        return []
    return [
        "Destination identity/sequence generator is behind the migrated data on "
        f"{', '.join(collisions)} - the next application insert will collide on "
        "the primary key."
    ]


def _verdict(
    *,
    pack: dict[str, Any],
    ledger: dict[str, Any],
    job_status: str,
    physical_state: dict[str, Any] | None = None,
    job: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Single headline verdict, degraded by the row ledger.

    Gate-8 assurance alone is not enough: a run can reconcile the rows it
    admits to while quietly dropping rows it never counted, so an unbalanced
    ledger downgrades the verdict regardless of checksum state.
    """
    assurance = _dict(pack.get("assurance"))
    claim_level = str(assurance.get("claim_level") or "none")
    proven = bool(assurance.get("migration_proven")) and claim_level == _PROVEN_ASSURANCE
    blockers: list[str] = []
    if not ledger.get("balanced"):
        blockers.append(str(ledger.get("note") or "row accounting did not balance"))
    for reason in pack.get("proof_incomplete_reasons") or []:
        blockers.append(str(reason))
    blockers.extend(_identity_blockers(physical_state or {}))
    blockers.extend(_referential_blockers(physical_state or {}))
    blockers.extend(_schema_object_blockers(physical_state or {}))
    blockers.extend(_foreign_key_carry_blockers(job or {}))
    blockers.extend(_identity_carry_blockers(job or {}))
    if job_status and job_status not in ("completed", "succeeded", "success"):
        blockers.append(f"Job status is {job_status!r}, not a completed run.")

    if proven and not blockers:
        headline = "MIGRATION PROVEN"
        detail = (
            "Independent source and destination checksums match and every source "
            "row is accounted for."
        )
    elif blockers:
        headline = "NOT PROVEN"
        detail = "This run carries open findings — see blockers."
    else:
        headline = "COMPLETED — NOT PROVEN"
        detail = (
            f"Reconciliation assurance is {claim_level!r}. Row counts reconcile at "
            "that level; per-cell fidelity across the full population is not claimed."
        )
    return {
        "headline": headline,
        "detail": detail,
        "migration_proven": proven and not blockers,
        "assurance_level": claim_level,
        "blockers": blockers,
    }


def build_migration_certificate(
    job: dict[str, Any], *, actor: str = "system"
) -> dict[str, Any]:
    """Build a signed, human-readable certificate for one transfer run."""
    if not isinstance(job, dict) or not (job.get("id") or job.get("_id")):
        raise RowAccountingError("A job document with an id is required")
    job_id = str(job.get("id") or job.get("_id"))

    pack = export_proof_pack_for_job(job, actor=actor)
    ledger = row_accounting(job)
    dest = _dict(job.get("destination_summary"))
    rejected = _rejected_details(
        job_id,
        embedded=dest.get("rejected_details") or job.get("rejected_details") or [],
        expected=_as_int(ledger.get("rows_quarantined")),
    )
    recon = _dict(job.get("reconciliation"))
    physical = physical_state_findings(recon)
    status = str(job.get("status") or "")

    body: dict[str, Any] = {
        "version": CERTIFICATE_VERSION,
        "kind": "migration_certificate",
        "issued_at": datetime.now(timezone.utc).isoformat(),
        "issued_to": actor,
        "job": {
            "job_id": job_id,
            "status": status,
            "workspace_id": str(job.get("workspace_id") or ""),
            "plan_id": str(job.get("plan_id") or ""),
            "sync_mode": str(job.get("sync_mode") or ""),
            "source": _dict(job.get("source")).get("format")
            or str(job.get("source_type") or ""),
            "destination": _dict(job.get("destination")).get("format")
            or str(job.get("destination_type") or ""),
            "started_at": str(job.get("created_at") or ""),
            "finished_at": str(job.get("updated_at") or ""),
        },
        "verdict": _verdict(
            pack=pack,
            ledger=ledger,
            job_status=status,
            physical_state=physical,
            job=job,
        ),
        "row_accounting": ledger,
        "reconciliation": {
            "passed": bool(recon.get("passed")),
            "phase": str(recon.get("phase") or ""),
            "assurance_level": str(recon.get("assurance_level") or ""),
            "checksum_match": recon.get("checksum_match"),
            "checksum_scope": str(recon.get("checksum_scope") or ""),
            "population_proof": bool(recon.get("population_proof")),
            "source_checksum": str(recon.get("source_checksum") or ""),
            "target_checksum": str(recon.get("target_checksum") or ""),
            "message": str(recon.get("message") or ""),
        },
        "quarantine": {
            "rows": ledger.get("rows_quarantined"),
            "detail_rows_available": len(rejected),
            "by_reason": quarantine_reason_breakdown(rejected),
            "burndown": quarantine_burndown(job_id),
            "remediation": (
                "Rows are durable in the quarantine DLQ with original value, "
                "expected type, reason and primary keys. Replay upserts the "
                "stored payload; it does not re-read the source."
            ),
        },
        "physical_state": physical,
        "accepted_risks": pack.get("accepted_risks") or [],
        "hashes": pack.get("hashes") or {},
        "connector_versions": pack.get("connector_versions") or {},
        "delivery_semantics": pack.get("delivery_semantics") or {},
        "rollback_plan": pack.get("rollback_plan") or {},
        "proof_pack": {
            "content_sha256": pack.get("content_sha256"),
            "signature": _dict(pack.get("signature")).get("value"),
            "version": pack.get("version"),
        },
        "not_proven_by_this_certificate": [
            "Per-cell fidelity for every row — only the reconciliation scope above.",
            "Referential integrity for relationships the physical state section "
            "reports as unavailable (missing parent tables, failed scans) — "
            "enforced and scanned relationships are proven.",
            "Exactly-once delivery — CDC and resume are at-least-once with upsert.",
            "Trigger bodies, grants and storage options on the "
            "destination — the physical state section reports only identity "
            "watermarks, keys, unique constraints, check constraints, "
            "foreign keys, trigger timing/events, indexes, "
            "nullability and defaults.",
            "Any claim about rows this job did not read.",
        ],
    }
    return sign_body(body, subject=job_id)


def verify_migration_certificate(cert: dict[str, Any]) -> dict[str, Any]:
    """Verify certificate content hash + HMAC, and that its claim is legitimate."""
    if not isinstance(cert, dict):
        return {"ok": False, "errors": ["certificate must be an object"]}
    job_id = str(_dict(cert.get("job")).get("job_id") or "")
    actual_hash, errors = verify_body(cert, subject=job_id)
    verdict = _dict(cert.get("verdict"))
    if verdict.get("migration_proven"):
        if verdict.get("assurance_level") != _PROVEN_ASSURANCE:
            errors.append("migration_proven claimed without full_checksum assurance")
        if verdict.get("blockers"):
            errors.append("migration_proven claimed while blockers are present")
        if not _dict(cert.get("row_accounting")).get("balanced"):
            errors.append("migration_proven claimed while row accounting is unbalanced")
    return {"ok": not errors, "errors": errors, "content_sha256": actual_hash}


def render_certificate_markdown(cert: dict[str, Any]) -> str:
    """Render the certificate as the one page an operator hands to a client."""
    job = _dict(cert.get("job"))
    verdict = _dict(cert.get("verdict"))
    ledger = _dict(cert.get("row_accounting"))
    recon = _dict(cert.get("reconciliation"))
    quarantine = _dict(cert.get("quarantine"))
    burn = _dict(quarantine.get("burndown"))

    def _n(value: Any) -> str:
        return f"{value:,}" if isinstance(value, int) else "unmeasured"

    lines = [
        "# Migration Certificate",
        "",
        f"**{verdict.get('headline', 'UNKNOWN')}** — {verdict.get('detail', '')}",
        "",
        f"- Job: `{job.get('job_id', '')}` ({job.get('status', '')})",
        f"- Route: {job.get('source', '?')} → {job.get('destination', '?')}"
        f" · sync mode: {job.get('sync_mode') or 'n/a'}",
        f"- Issued: {cert.get('issued_at', '')} to {cert.get('issued_to', '')}",
        "",
        "## Row accounting",
        "",
        "| | Rows |",
        "|---|---:|",
        f"| Read from source | {_n(ledger.get('rows_read'))} |",
        f"| Written to destination | {_n(ledger.get('rows_written'))} |",
        f"| Quarantined | {_n(ledger.get('rows_quarantined'))} |",
        f"| Skipped (stale / duplicate) | {_n(ledger.get('rows_skipped'))} |",
        f"| Unaccounted | {_n(ledger.get('unaccounted'))} |",
        "",
        f"{ledger.get('note', '')}",
        "",
        "## Reconciliation",
        "",
        f"- Assurance: `{recon.get('assurance_level') or 'none'}`"
        f" · phase `{recon.get('phase') or 'n/a'}`",
        f"- Checksum match: {recon.get('checksum_match')}"
        + (f" (scope: {recon['checksum_scope']})" if recon.get("checksum_scope") else ""),
        f"- {recon.get('message', '')}",
        "",
        "## Quarantine",
        "",
        f"{_n(quarantine.get('rows'))} row(s) held out — never silently dropped.",
        "",
    ]
    by_reason = quarantine.get("by_reason") or []
    detail_rows = quarantine.get("detail_rows_available")
    total_rows = quarantine.get("rows")
    if isinstance(detail_rows, int) and isinstance(total_rows, int) and detail_rows < total_rows:
        lines += [
            f"Reason breakdown covers {detail_rows:,} of {total_rows:,} rows — "
            "the remainder is in the DLQ but not in this export.",
            "",
        ]
    if by_reason:
        lines += ["| Reason | Rows | Columns |", "|---|---:|---|"]
        for entry in by_reason:
            cols = ", ".join(entry.get("columns") or []) or "—"
            lines.append(f"| {entry.get('reason', '')} | {entry.get('rows', 0)} | {cols} |")
        lines.append("")
    if burn.get("available"):
        lines.append(
            f"Burn-down: {burn.get('quarantined', 0)} quarantined · "
            f"{burn.get('replayed', 0)} replayed · **{burn.get('open', 0)} open**."
        )
    else:
        lines.append(str(burn.get("note") or "Burn-down unavailable."))
    lines += ["", str(quarantine.get("remediation") or ""), ""]

    physical = _dict(cert.get("physical_state"))
    identity = _dict(physical.get("identity_watermark"))
    if identity:
        lines += ["## Destination physical state", ""]
        if identity.get("supported") is False:
            lines += [f"- Identity/sequence watermark: {identity.get('reason', '')}", ""]
        else:
            for entry in identity.get("checked") or []:
                col = entry.get("column", "")
                if not entry.get("available"):
                    lines.append(f"- `{col}` generator unverified — {entry.get('reason', '')}")
                    continue
                state = (
                    "COLLIDES"
                    if entry.get("collides")
                    else ("repaired forward-only" if entry.get("repaired_to") else "ahead of data")
                )
                lines.append(
                    f"- `{col}` {entry.get('mechanism', 'generator')} "
                    f"`{entry.get('generator', '')}` next={entry.get('next_value')} "
                    f"max={entry.get('max_value')} — {state}"
                )
            if not identity.get("checked"):
                lines.append(
                    f"- Identity/sequence watermark not verified — {identity.get('reason', '')}"
                )
            lines.append("")

    objects = _dict(physical.get("schema_objects"))
    if objects:
        aspects = _dict(objects.get("aspects"))
        if aspects:
            lines += ["| Schema object | State | Missing on destination |", "|---|---|---|"]
            for aspect, detail in aspects.items():
                info = _dict(detail)
                missing = ", ".join(info.get("missing") or []) or "—"
                label = aspect.replace("_", " ")
                if info.get("advisory"):
                    label = f"{label} (advisory)"
                lines.append(
                    f"| {label} | {info.get('status', '')} | {missing} |"
                )
            lines.append("")
            for aspect, detail in aspects.items():
                info = _dict(detail)
                if info.get("advisory") and info.get("status") != "carried":
                    lines += [f"- {info.get('note', '')}", ""]
        else:
            lines += [
                f"- Constraints and indexes not compared — {objects.get('reason', '')}",
                "",
            ]

    referential = _dict(physical.get("referential_integrity"))
    relations = referential.get("relations") or []
    if relations:
        lines += ["| Relationship | Proof | Orphan rows |", "|---|---|---|"]
        for rel in relations:
            info = _dict(rel)
            cols = "+".join(str(c) for c in info.get("columns") or [])
            target = f"`{cols}` → `{info.get('referred_table', '')}`"
            status = str(info.get("status") or "")
            if info.get("available"):
                count = str(info.get("orphan_count", 0))
            else:
                count = f"unavailable — {info.get('reason', '')}"
            lines.append(f"| {target} | {status} | {count} |")
        lines.append("")
    elif referential and referential.get("reason"):
        lines += [f"- Referential integrity: {referential.get('reason')}", ""]

    blockers = verdict.get("blockers") or []
    if blockers:
        lines += ["## Blockers", ""] + [f"- {b}" for b in blockers] + [""]

    risks = cert.get("accepted_risks") or []
    if risks:
        lines += ["## Accepted risk contracts", ""]
        for risk in risks:
            if not isinstance(risk, dict):
                continue
            lines.append(
                f"- `{risk.get('risk_id', '')}` {risk.get('column', '')}: "
                f"{risk.get('reason') or risk.get('description') or ''}"
            )
        lines.append("")

    lines += [
        "## Not proven by this certificate",
        "",
    ] + [f"- {item}" for item in cert.get("not_proven_by_this_certificate") or []]
    lines += [
        "",
        "## Signature",
        "",
        f"- Certificate SHA-256: `{cert.get('content_sha256', '')}`",
        f"- HMAC: `{_dict(cert.get('signature')).get('value', '')}`",
        f"- Proof pack SHA-256: `{_dict(cert.get('proof_pack')).get('content_sha256', '')}`",
        "",
        "Verify with `POST /api/transfer/certificate/verify`.",
    ]
    return "\n".join(lines) + "\n"
