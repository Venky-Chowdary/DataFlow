"""Parity at Scale evidence in the Trust Report (phase 3).

:func:`apply_scale_evidence` turns a table's scan (on the job's
``parity_scale`` block) into the report's :class:`TableEvidence`:

* a scan still running makes the table ``verifying``: the verdict reads
  "Verification running" and sign-off, cutover and "proven" wait;
* a finished keyed scan is a ``segment_diff`` (every row compared inside both
  databases); a keyless one a ``multiset_digest`` ("Content match (no key)");
  both are full checks;
* differences become observations with the causes the cell judge gave
  (Gate-8's own fingerprint and the Parity Studio classifier); missing and
  extra keys are classified like every keyed difference (quarantined keys are
  explained); a keyless difference is content, never attributed to a key;
* a scan that could not compare every unit leaves the table unverifiable for
  the uncovered part (never a pass);
* a kind the CE matrix has not certified for the server pair keeps the table
  from "proven" (design §4.4): ``certified`` is false on the table.
"""

from __future__ import annotations

from typing import Any

from services.parity_evidence import MAX_KEYS_SAMPLE, Observation, TableEvidence

CHECK_SEGMENT = "segment_diff"
CHECK_MULTISET = "multiset_digest"
CHECK_VERIFYING = "verifying"
SCALE_CHECKS = frozenset({CHECK_SEGMENT, CHECK_MULTISET})
CONTENT_MATCH_LABEL = "Content match (no key)"


def _key_texts(keys: list[Any]) -> list[str]:
    from services.parity_scale.judge import wire_text
    from services.parity_scale.store import decode_key

    out = []
    for key in keys or []:
        values = decode_key(key) or ()
        out.append("|".join("" if v is None else (wire_text(v) or "") for v in values))
    return out


def apply_scale_evidence(ev: TableEvidence, job: dict[str, Any]) -> None:
    from services.parity_scale.verification import RUNNING, table_block

    block = table_block(job, ev.name)
    if not block:
        return
    status = block.get("status")
    if status == RUNNING:
        progress = block.get("progress") or {}
        done, queued = int(progress.get("items_done") or 0), int(progress.get("items_queued") or 0)
        ev.check = CHECK_VERIFYING
        # The comparison of every row decides this table: what Gate-8 left open is not restated as differences.
        ev.observations = []
        ev.gate8_unaccounted = False
        ev.capped = False
        ev.note = (
            f"Verification of {ev.name} is running: every row is being compared inside both databases"
            + (f" ({done:,} of {done + queued:,} steps done)." if done + queued else ".")
        )
        return
    if status == "error":
        ev.note = (ev.note + " " if ev.note else "") + (
            str(block.get("error") or "")
            or f"Verification of {ev.name} could not finish; re-verify the table to compare every row."
        )
        return
    evidence = block.get("evidence") or {}
    if not evidence:
        return
    _apply_done(ev, evidence)


def _apply_done(ev: TableEvidence, evidence: dict[str, Any]) -> None:
    from services.parity_classifier import UNEXPLAINED, cause, classify_row

    keyless = evidence.get("check_kind") == CHECK_MULTISET
    ev.check = CHECK_MULTISET if keyless else CHECK_SEGMENT
    population = evidence.get("population") or {}
    # Rows the write contract held out are excluded from the scan's source side: the
    # run read them, so they count among the source rows (read = landed + held + skipped).
    fit_excluded = int(((population.get("held_out") or {}).get("fit_excluded")) or 0)
    ev.rows_source = int(population.get("rows_source") or 0) + fit_excluded
    ev.rows_destination = int(population.get("rows_destination") or 0)
    ev.sampled_rows = 0
    diff = evidence.get("differences") or {}
    at_least = bool(diff.get("at_least"))
    obs: list[Observation] = []
    # Missing and extra rows (content counts for a keyless table).
    for kind, count_field, keys_field in (
        ("missing_in_destination", "missing_in_destination", "missing_keys"),
        ("extra_in_destination", "extra_in_destination", "extra_keys"),
    ):
        rows = int(diff.get(count_field) or 0)
        if not rows:
            continue
        if keyless:
            obs.append(Observation(
                table=ev.name, kind=kind, rows=rows, origin="parity_scale", at_least=at_least,
                cause=cause(UNEXPLAINED, "Rows of content differ; the table has no key, so a difference "
                            "cannot be attributed to a key.", explained=False),
            ))
            continue
        keys = _key_texts(diff.get(keys_field) or [])
        quarantined = [k for k in keys if k in ev.quarantined_keys]
        if quarantined:
            obs.append(Observation(
                table=ev.name, kind=kind, rows=len(quarantined), origin="parity_scale",
                keys_sample=quarantined[:MAX_KEYS_SAMPLE],
                cause=classify_row(kind, key=quarantined[0], quarantined_keys=ev.quarantined_keys),
            ))
        rest = rows - len(quarantined)
        if rest > 0:
            open_keys = [k for k in keys if k not in ev.quarantined_keys]
            obs.append(Observation(
                table=ev.name, kind=kind, rows=rest, origin="parity_scale", at_least=at_least,
                keys_sample=open_keys[:MAX_KEYS_SAMPLE],
                cause=classify_row(kind, key=open_keys[0] if open_keys else None,
                                   outside_this_run=ev.outside_run and kind == "extra_in_destination"),
            ))
    dupes = int(diff.get("duplicate_keys") or 0)
    if dupes:
        obs.append(Observation(
            table=ev.name, kind="duplicate_key", rows=dupes, origin="parity_scale",
            keys_sample=_key_texts(diff.get("duplicate_key_values") or [])[:MAX_KEYS_SAMPLE],
            cause=classify_row("duplicate_key", key=None),
        ))
    # Changed rows, by column and cause, as the cell judge ruled them.
    for column, causes in sorted((diff.get("by_column") or {}).items()):
        for code, slot in sorted(causes.items()):
            obs.append(Observation(
                table=ev.name, kind="value", column=column, rows=int(slot.get("rows") or 0),
                origin="parity_scale", keys_sample=list(slot.get("keys") or [])[:MAX_KEYS_SAMPLE],
                cause={"code": code, "label": slot.get("label") or code,
                       "explained": bool(slot.get("explained")), "detail": slot.get("detail") or ""},
            ))
    unjudged = int(diff.get("rows_not_judged") or 0)
    if unjudged:
        obs.append(Observation(
            table=ev.name, kind="value", rows=unjudged, origin="parity_scale", at_least=True,
            cause=cause(UNEXPLAINED, "These rows differ inside the databases and were not examined cell by "
                        "cell (more differing keys than the report keeps).", explained=False),
        ))
    # The scan compared every row: it supersedes Gate-8's coverage-limited evidence,
    # except what Gate-8 explained (quarantine, skipped by policy, rows from before
    # an append, transforms): that stays explained (services.parity_scale.scope).
    from services.parity_scale.scope import merge_explained

    ev.observations = merge_explained(list(ev.observations), obs, fit_excluded=fit_excluded, table=ev.name)
    ev.gate8_unaccounted = False
    ev.capped = False
    ev.note = ""
    coverage = evidence.get("coverage") or {}
    if not coverage.get("complete"):
        ev.capped = True
        missing = int(coverage.get("units_not_compared") or 0)
        ev.note = (
            f"Verification compared {ev.name} only in part ({missing:,} {'part' if missing == 1 else 'parts'} "
            "could not be compared"
            + ("; most of the table differs the same way" if coverage.get("systematic") else "")
            + "). Re-verify the table to compare every row."
        )
    certification = evidence.get("certification") or {}
    ev.scale = {
        "check_label": (CONTENT_MATCH_LABEL if keyless else "Every row compared inside both databases"),
        "key_attribution": diff.get("key_attribution"),
        "certified": bool(certification.get("all_certified")),
        "certification_pair": certification.get("pair"),
        "mode": evidence.get("mode"),
        "scan_id": evidence.get("scan_id"),
        "ce_stricter_than_fp": int(diff.get("ce_stricter_than_fp") or 0),
        # "pinned" (one snapshot per side) or "not_established": only pinned may read proven.
        "consistency": str((evidence.get("consistency") or {}).get("kind") or "not_established"),
    }
