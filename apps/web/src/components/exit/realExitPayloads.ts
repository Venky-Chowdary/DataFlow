/**
 * Real Reversibility Proof API payloads from an engine run (SQLite tables -> file
 * exports, in process): the board, a system, its rehearsals, proven / signed /
 * incomplete / not proven / failed / running rehearsals, a pack verification and
 * a plan's reversibility report. Since the review fixes (contract 10) also: a
 * stale system (definition changed after a proven rehearsal), a running system,
 * a rehearsal with PII control totals masked, one with a column that has no
 * declared type (not provable), and plan reports carrying the new reason codes
 * (timezone, json_normalize, blank_pad, text_width, enum, row_change for limit /
 * source_filter / rank_rows). Captured from services.exit_* and
 * services.reversibility_sources through the API test harness; the API has no
 * web-fixture writer for this feature. Typed with the contract so tsc checks every
 * field. Not imported by app code.
 *
 * The running rehearsal is typed too: since contract 10.1.5 its tables carry the
 * full RehearsalTable shape (unmeasured fields null, value_parity.method null).
 */
import type { ExitReadiness, ExitRehearsal, ExitSystem, ExitVerifyResult, RehearsalSummary, Trend } from "../../lib/exitReadiness";
import type { ReversibilityReport } from "../../lib/reversibility";

export const realBoard: ExitReadiness = {
  "generated_at": "2026-11-11T20:41:24.053959Z",
  "kpis": {
    "longest_measured_seconds": 0,
    "longest_measured_system_id": "exs_aa5f2aa2b06b",
    "next_regulatory_date": {
      "date": "2027-01-12",
      "label": "Data Act switching charges end",
      "system_id": "exs_aa5f2aa2b06b"
    },
    "overdue": 0,
    "systems": 8,
    "systems_proven": 2
  },
  "systems": [
    {
      "completeness_label": "Not proven (changed since the last rehearsal)",
      "criticality": "important",
      "engine": "sqlite",
      "fits_window": null,
      "id": "exs_fd5250928bdf",
      "last_rehearsal_at": "2026-11-11T20:41:23.569858Z",
      "last_rehearsal_id": "exr_1ed55fa225fe",
      "measured_seconds": 0,
      "name": "Billing",
      "next_due_at": "2027-02-09T20:41:23.569858Z",
      "overdue": false,
      "running_rehearsal_id": null,
      "stale": true,
      "state": "not_proven",
      "tables": 1,
      "window_seconds": 172800
    },
    {
      "completeness_label": "Proven, 2 tables",
      "criticality": "critical",
      "engine": "sqlite",
      "fits_window": true,
      "id": "exs_aa5f2aa2b06b",
      "last_rehearsal_at": "2026-11-11T20:41:19.222967Z",
      "last_rehearsal_id": "exr_87ea0580cbdf",
      "measured_seconds": 0,
      "name": "Core ledger",
      "next_due_at": "2027-02-09T20:41:19.222967Z",
      "overdue": false,
      "running_rehearsal_id": null,
      "stale": false,
      "state": "proven",
      "tables": 2,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Not proven",
      "criticality": "standard",
      "engine": "sqlite",
      "fits_window": null,
      "id": "exs_bbf985b814a8",
      "last_rehearsal_at": "2026-11-11T20:41:19.752771Z",
      "last_rehearsal_id": "exr_16a597f7b00f",
      "measured_seconds": 0,
      "name": "CRM",
      "next_due_at": "2027-02-09T20:41:19.752771Z",
      "overdue": false,
      "running_rehearsal_id": null,
      "stale": false,
      "state": "not_proven",
      "tables": 1,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Proven, 1 table",
      "criticality": "critical",
      "engine": "sqlite",
      "fits_window": true,
      "id": "exs_8a27e123152b",
      "last_rehearsal_at": "2026-11-11T20:41:23.091373Z",
      "last_rehearsal_id": "exr_21a75d2edd90",
      "measured_seconds": 0,
      "name": "Customer master",
      "next_due_at": "2027-02-09T20:41:23.091373Z",
      "overdue": false,
      "running_rehearsal_id": null,
      "stale": false,
      "state": "proven",
      "tables": 1,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Not proven",
      "criticality": "standard",
      "engine": "sqlite",
      "fits_window": null,
      "id": "exs_fdae60e0ad69",
      "last_rehearsal_at": "2026-11-11T20:41:23.336478Z",
      "last_rehearsal_id": "exr_19a4d2a98943",
      "measured_seconds": 0,
      "name": "Notes archive",
      "next_due_at": "2027-02-09T20:41:23.336478Z",
      "overdue": false,
      "running_rehearsal_id": null,
      "stale": false,
      "state": "not_proven",
      "tables": 1,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Incomplete",
      "criticality": "important",
      "engine": "sqlite",
      "fits_window": null,
      "id": "exs_a76fe09e3471",
      "last_rehearsal_at": "2026-11-11T20:41:19.502973Z",
      "last_rehearsal_id": "exr_2137474be617",
      "measured_seconds": 0,
      "name": "Payments",
      "next_due_at": "2027-02-09T20:41:19.502973Z",
      "overdue": false,
      "running_rehearsal_id": null,
      "stale": false,
      "state": "incomplete",
      "tables": 1,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Rehearsal running",
      "criticality": "critical",
      "engine": "sqlite",
      "fits_window": null,
      "id": "exs_3bcdda2d273d",
      "last_rehearsal_at": "2026-11-11T20:41:23.828600Z",
      "last_rehearsal_id": "exr_453883a79c6f",
      "measured_seconds": null,
      "name": "Running one",
      "next_due_at": "2027-02-09T20:41:23.828600Z",
      "overdue": false,
      "running_rehearsal_id": "exr_453883a79c6f",
      "stale": false,
      "state": "running",
      "tables": 1,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Rehearsal failed",
      "criticality": "critical",
      "engine": "sqlite",
      "fits_window": null,
      "id": "exs_277101d9a7d6",
      "last_rehearsal_at": "2026-11-11T20:41:20.001638Z",
      "last_rehearsal_id": "exr_6629ab42c485",
      "measured_seconds": 3,
      "name": "Warehouse",
      "next_due_at": "2027-02-09T20:41:20.001638Z",
      "overdue": false,
      "running_rehearsal_id": null,
      "stale": false,
      "state": "failed",
      "tables": 2,
      "window_seconds": 2592000
    }
  ]
};

export const realSystem: ExitSystem = {
  "created_at": "2026-11-11T20:41:18.560996Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_aa5f2aa2b06b",
  "name": "Core ledger",
  "readiness": {
    "last_proven_rehearsal": {
      "current": true,
      "finished_at": "2026-11-11T20:41:19.484255Z",
      "fits_window": true,
      "id": "exr_87ea0580cbdf",
      "measured_seconds": 0,
      "n": 2,
      "signed": true,
      "started_at": "2026-11-11T20:41:19.222967Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "last_rehearsal": {
      "current": true,
      "finished_at": "2026-11-11T20:41:19.484255Z",
      "fits_window": true,
      "id": "exr_87ea0580cbdf",
      "measured_seconds": 0,
      "n": 2,
      "signed": true,
      "started_at": "2026-11-11T20:41:19.222967Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-09T20:41:19.222967Z",
    "overdue": false,
    "running_rehearsal_id": null,
    "stale": false,
    "stale_reason": null,
    "state": "proven"
  },
  "regulatory_dates": [
    {
      "date": "2027-01-12",
      "label": "Data Act switching charges end"
    }
  ],
  "rehearsal_every_days": 90,
  "rehearsal_schedule": null,
  "required_roles": [
    "engineering",
    "data_owner",
    "compliance"
  ],
  "running_rehearsal_id": null,
  "source": {
    "connector_id": "c1",
    "connector_name": "ledger",
    "engine": "sqlite"
  },
  "tables": [
    {
      "key_columns": [],
      "name": "orders",
      "schema": null
    },
    {
      "key_columns": [],
      "name": "events",
      "schema": null
    }
  ],
  "updated_at": "2026-11-11T20:41:18.560996Z",
  "updated_by": "owner@example.com",
  "version": 5,
  "window_basis": "EU Data Act switching window",
  "workspace_id": "ws1"
};

export const realRehearsals: { rehearsals: RehearsalSummary[]; trend: Trend } = {
  "rehearsals": [
    {
      "current": null,
      "finished_at": "2026-11-11T20:41:19.484255Z",
      "fits_window": true,
      "id": "exr_87ea0580cbdf",
      "measured_seconds": 0,
      "n": 2,
      "signed": true,
      "started_at": "2026-11-11T20:41:19.222967Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    {
      "current": null,
      "finished_at": "2026-11-11T20:41:19.218436Z",
      "fits_window": true,
      "id": "exr_7665c9d04b30",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T20:41:18.565161Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    }
  ],
  "trend": {
    "growth_pct_rows": 0.0,
    "points": [
      {
        "at": "2026-11-11T20:41:18.565161Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_7665c9d04b30",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-11T20:41:19.222967Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_87ea0580cbdf",
        "rows": 63,
        "verdict": "proven"
      }
    ],
    "projection": {
      "basis": "Projection, not a measurement: rehearsal 2 measured 0 s for 63 rows; if the data grows again as it did since rehearsal 1 (63 rows), the next exit takes about 0 s.",
      "kind": "projection",
      "seconds": 0
    }
  }
};

export const realProven: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "2 of 2 declared table(s) extracted. 2 proven complete (rows, key sets, every value and control totals). Value check: population digest on 2.",
    "tables_declared": 2,
    "tables_extracted": 2,
    "tables_failed": 0,
    "tables_incomplete": 0,
    "tables_not_proven": 0,
    "tables_proven": 2,
    "value_methods": {
      "count_only": 0,
      "full_checksum": 0,
      "key_diff": 0,
      "none": 0,
      "population_digest": 2,
      "sampled": 0
    }
  },
  "definition_sha256": "37c6a0114406f8551facf70cc034edf393d286cfcc64f0fa264d4ddcec12e6be",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 0,
    "statement": "Measured 0 s for 2,818 bytes (extract 0 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_87ea0580cbdf/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T20:41:19.484255Z",
  "id": "exr_87ea0580cbdf",
  "n": 2,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
    "parts": [
      {
        "name": "exit_exr_87ea0580cbdf_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      {
        "name": "exit_exr_87ea0580cbdf_0001_events.jsonl",
        "object_key": null,
        "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
        "size_bytes": 71,
        "storage": "local_file",
        "table": "events",
        "upload_parts": null,
        "verified_on_read": true
      }
    ],
    "parts_count": 2,
    "storage": "local_file",
    "total_bytes": 2818
  },
  "signoff": {
    "approvals": [],
    "blocked_reason": null,
    "invalidated_approvals": [],
    "required_roles": [
      "engineering",
      "data_owner",
      "compliance"
    ],
    "signature": null,
    "signed_at": null,
    "status": "open"
  },
  "started_at": "2026-11-11T20:41:19.222967Z",
  "system_id": "exs_aa5f2aa2b06b",
  "system_name": "Core ledger",
  "tables": [
    {
      "control_totals": [
        {
          "column": "amount",
          "extract": {
            "max": "75",
            "min": "1.25",
            "sum": "2287.50"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "75",
            "min": "1.25",
            "sum": "2287.50"
          }
        },
        {
          "column": "id",
          "extract": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.169,
      "job_id": "6af4d2ef20ff77ead7f1a623",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": "1eee5bfc7da0c1b04c6a916bf35c64faeda2366359bccf0bf3ffe7afd66124c9",
        "match": true,
        "source_sha256": "1eee5bfc7da0c1b04c6a916bf35c64faeda2366359bccf0bf3ffe7afd66124c9"
      },
      "name": "orders",
      "part": {
        "name": "exit_exr_87ea0580cbdf_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Rows, key set, every value (by population digest) and control totals match over all 60 row(s); the part verified on read.",
      "row_set": {
        "extract_sha256": "dd7192488fc1fef0a1bdd78a6dda886ac25ad40d9125d341d1384e1d030e36ba",
        "match": true,
        "source_sha256": "dd7192488fc1fef0a1bdd78a6dda886ac25ad40d9125d341d1384e1d030e36ba"
      },
      "rows_extracted": 60,
      "rows_source": 60,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4d2ef20ff77ead7f1a623/parity-report",
        "unexplained": 0
      }
    },
    {
      "control_totals": [
        {
          "column": "qty",
          "extract": {
            "max": "2",
            "min": "1",
            "sum": "4"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "2",
            "min": "1",
            "sum": "4"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.111,
      "job_id": "6af4d2ef20ff77ead7f1a624",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "events",
      "part": {
        "name": "exit_exr_87ea0580cbdf_0001_events.jsonl",
        "object_key": null,
        "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
        "size_bytes": 71,
        "storage": "local_file",
        "table": "events",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Rows, every value (by population digest) and control totals match over all 3 row(s); the part verified on read.",
      "row_set": {
        "extract_sha256": "eb76c1445d7f89d6b45b5fb9396d281876db2ea30b1f27232f45ef9d6eb15a64",
        "match": true,
        "source_sha256": "eb76c1445d7f89d6b45b5fb9396d281876db2ea30b1f27232f45ef9d6eb15a64"
      },
      "rows_extracted": 3,
      "rows_source": 3,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4d2ef20ff77ead7f1a624/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T20:41:19.222967Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T20:41:19.222967Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.404912Z",
      "seconds": 0.159,
      "stage": "extract",
      "started_at": "2026-11-11T20:41:19.246082Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.484255Z",
      "seconds": 0.033,
      "stage": "verify",
      "started_at": "2026-11-11T20:41:19.451585Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.484255Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T20:41:19.484255Z",
      "status": "done"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "sign",
      "started_at": null,
      "status": "pending"
    }
  ],
  "trend": {
    "growth_pct_rows": 0.0,
    "points": [
      {
        "at": "2026-11-11T20:41:18.565161Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_7665c9d04b30",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-11T20:41:19.222967Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_87ea0580cbdf",
        "rows": 63,
        "verdict": "proven"
      }
    ],
    "projection": {
      "basis": "Projection, not a measurement: rehearsal 2 measured 0 s for 63 rows; if the data grows again as it did since rehearsal 1 (63 rows), the next exit takes about 0 s.",
      "kind": "projection",
      "seconds": 0
    }
  },
  "trigger": "manual",
  "verdict": "proven",
  "verdict_reason": "Every one of the 2 declared table(s) was extracted and proven complete.",
  "version": 4,
  "workspace_id": "ws1"
};

export const realSigned: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "2 of 2 declared table(s) extracted. 2 proven complete (rows, key sets, every value and control totals). Value check: population digest on 2.",
    "tables_declared": 2,
    "tables_extracted": 2,
    "tables_failed": 0,
    "tables_incomplete": 0,
    "tables_not_proven": 0,
    "tables_proven": 2,
    "value_methods": {
      "count_only": 0,
      "full_checksum": 0,
      "key_diff": 0,
      "none": 0,
      "population_digest": 2,
      "sampled": 0
    }
  },
  "definition_sha256": "37c6a0114406f8551facf70cc034edf393d286cfcc64f0fa264d4ddcec12e6be",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 0,
    "statement": "Measured 0 s for 2,818 bytes (extract 0 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": {
      "anchored": false,
      "anchored_at": "2026-11-11T20:41:19.496167+00:00",
      "digest": "92cab189af2239d85c2b23b0928d4b25b5ff4153814d0e4272ad22753e2e300f",
      "event_time": null,
      "honesty": "Local stub anchor \u2014 not S3 Object Lock / RFC-3161 TSA, so not write-once: not anchored. Configure DATAFLOW_AUDIT_ANCHOR_PROVIDER=s3 with an Object Lock bucket.",
      "id": "67e19423-472f-460d-ab8f-67d34c579213",
      "kind": "exit_evidence",
      "location": null,
      "provider": "stub",
      "reason": "the local stub is not a write-once store",
      "retention": null,
      "status": "not_anchored",
      "subject": "exit_evidence:exs_aa5f2aa2b06b:exr_87ea0580cbdf",
      "tip_hash": "92cab189af2239d85c2b23b0928d4b25b5ff4153814d0e4272ad22753e2e300f"
    },
    "content_sha256": "92cab189af2239d85c2b23b0928d4b25b5ff4153814d0e4272ad22753e2e300f",
    "export": "/api/v1/exit/rehearsals/exr_87ea0580cbdf/evidence",
    "signed": true
  },
  "finished_at": "2026-11-11T20:41:19.484255Z",
  "id": "exr_87ea0580cbdf",
  "n": 2,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
    "parts": [
      {
        "name": "exit_exr_87ea0580cbdf_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      {
        "name": "exit_exr_87ea0580cbdf_0001_events.jsonl",
        "object_key": null,
        "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
        "size_bytes": 71,
        "storage": "local_file",
        "table": "events",
        "upload_parts": null,
        "verified_on_read": true
      }
    ],
    "parts_count": 2,
    "storage": "local_file",
    "total_bytes": 2818
  },
  "signoff": {
    "approvals": [
      {
        "at": "2026-11-11T20:41:19.489028Z",
        "authority": "workspace admin",
        "content_sha256": "3e04b1d2ae6f856a558b3f3974d551fb36febc4f3a77c03f6493c8a3f4b981df",
        "note": "",
        "role": "engineering",
        "signer_role": "admin",
        "user": "engineering@example.com",
        "version": 4
      },
      {
        "at": "2026-11-11T20:41:19.491243Z",
        "authority": "workspace admin",
        "content_sha256": "3e04b1d2ae6f856a558b3f3974d551fb36febc4f3a77c03f6493c8a3f4b981df",
        "note": "",
        "role": "data_owner",
        "signer_role": "admin",
        "user": "data_owner@example.com",
        "version": 5
      },
      {
        "at": "2026-11-11T20:41:19.493365Z",
        "authority": "workspace admin",
        "content_sha256": "3e04b1d2ae6f856a558b3f3974d551fb36febc4f3a77c03f6493c8a3f4b981df",
        "note": "",
        "role": "compliance",
        "signer_role": "admin",
        "user": "compliance@example.com",
        "version": 6
      }
    ],
    "blocked_reason": null,
    "invalidated_approvals": [],
    "required_roles": [
      "engineering",
      "data_owner",
      "compliance"
    ],
    "signature": "hmac-sha256:7ef0af8d37705ed7a1b03140f2221ba024b780bb2514e1842800ff198d5056c6",
    "signed_at": "2026-11-11T20:41:19.493375Z",
    "status": "signed"
  },
  "started_at": "2026-11-11T20:41:19.222967Z",
  "system_id": "exs_aa5f2aa2b06b",
  "system_name": "Core ledger",
  "tables": [
    {
      "control_totals": [
        {
          "column": "amount",
          "extract": {
            "max": "75",
            "min": "1.25",
            "sum": "2287.50"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "75",
            "min": "1.25",
            "sum": "2287.50"
          }
        },
        {
          "column": "id",
          "extract": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.169,
      "job_id": "6af4d2ef20ff77ead7f1a623",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": "1eee5bfc7da0c1b04c6a916bf35c64faeda2366359bccf0bf3ffe7afd66124c9",
        "match": true,
        "source_sha256": "1eee5bfc7da0c1b04c6a916bf35c64faeda2366359bccf0bf3ffe7afd66124c9"
      },
      "name": "orders",
      "part": {
        "name": "exit_exr_87ea0580cbdf_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Rows, key set, every value (by population digest) and control totals match over all 60 row(s); the part verified on read.",
      "row_set": {
        "extract_sha256": "dd7192488fc1fef0a1bdd78a6dda886ac25ad40d9125d341d1384e1d030e36ba",
        "match": true,
        "source_sha256": "dd7192488fc1fef0a1bdd78a6dda886ac25ad40d9125d341d1384e1d030e36ba"
      },
      "rows_extracted": 60,
      "rows_source": 60,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4d2ef20ff77ead7f1a623/parity-report",
        "unexplained": 0
      }
    },
    {
      "control_totals": [
        {
          "column": "qty",
          "extract": {
            "max": "2",
            "min": "1",
            "sum": "4"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "2",
            "min": "1",
            "sum": "4"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.111,
      "job_id": "6af4d2ef20ff77ead7f1a624",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "events",
      "part": {
        "name": "exit_exr_87ea0580cbdf_0001_events.jsonl",
        "object_key": null,
        "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
        "size_bytes": 71,
        "storage": "local_file",
        "table": "events",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Rows, every value (by population digest) and control totals match over all 3 row(s); the part verified on read.",
      "row_set": {
        "extract_sha256": "eb76c1445d7f89d6b45b5fb9396d281876db2ea30b1f27232f45ef9d6eb15a64",
        "match": true,
        "source_sha256": "eb76c1445d7f89d6b45b5fb9396d281876db2ea30b1f27232f45ef9d6eb15a64"
      },
      "rows_extracted": 3,
      "rows_source": 3,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4d2ef20ff77ead7f1a624/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T20:41:19.222967Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T20:41:19.222967Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.404912Z",
      "seconds": 0.159,
      "stage": "extract",
      "started_at": "2026-11-11T20:41:19.246082Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.484255Z",
      "seconds": 0.033,
      "stage": "verify",
      "started_at": "2026-11-11T20:41:19.451585Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.484255Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T20:41:19.484255Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.493375Z",
      "seconds": 0.0,
      "stage": "sign",
      "started_at": "2026-11-11T20:41:19.493375Z",
      "status": "done"
    }
  ],
  "trend": {
    "growth_pct_rows": 0.0,
    "points": [
      {
        "at": "2026-11-11T20:41:18.565161Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_7665c9d04b30",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-11T20:41:19.222967Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_87ea0580cbdf",
        "rows": 63,
        "verdict": "proven"
      }
    ],
    "projection": {
      "basis": "Projection, not a measurement: rehearsal 2 measured 0 s for 63 rows; if the data grows again as it did since rehearsal 1 (63 rows), the next exit takes about 0 s.",
      "kind": "projection",
      "seconds": 0
    }
  },
  "trigger": "manual",
  "verdict": "proven",
  "verdict_reason": "Every one of the 2 declared table(s) was extracted and proven complete.",
  "version": 8,
  "workspace_id": "ws1"
};

export const realIncomplete: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "1 of 1 declared table(s) extracted. 0 proven complete (rows, key sets, every value and control totals). Value check: population digest on 1.",
    "tables_declared": 1,
    "tables_extracted": 1,
    "tables_failed": 0,
    "tables_incomplete": 1,
    "tables_not_proven": 0,
    "tables_proven": 0,
    "value_methods": {
      "count_only": 0,
      "full_checksum": 0,
      "key_diff": 0,
      "none": 0,
      "population_digest": 1,
      "sampled": 0
    }
  },
  "definition_sha256": "81ebeadc53c356c8c8ad25cef1bd7a6c61bc8dd9b6f08e3f14f9200f50ebfca8",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": null,
    "margin_pct": null,
    "measured_seconds": 0,
    "statement": "Measured 0 s (extract 0 s, verify 0 s), but the rehearsal is not proven (incomplete), so it is not compared with the window.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_2137474be617/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T20:41:19.745152Z",
  "id": "exr_2137474be617",
  "n": 1,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_2137474be617_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": false
      }
    ],
    "parts_count": 1,
    "storage": "local_file",
    "total_bytes": 2747
  },
  "signoff": {
    "approvals": [],
    "blocked_reason": "Only a proven rehearsal can be signed; this one is incomplete: 1 of 1 table(s) differ from the source: orders.",
    "invalidated_approvals": [],
    "required_roles": [
      "engineering",
      "data_owner",
      "compliance"
    ],
    "signature": null,
    "signed_at": null,
    "status": "blocked"
  },
  "started_at": "2026-11-11T20:41:19.502973Z",
  "system_id": "exs_a76fe09e3471",
  "system_name": "Payments",
  "tables": [
    {
      "control_totals": [
        {
          "column": "amount",
          "extract": {
            "max": "73.75",
            "min": "1.25",
            "sum": "2212.50"
          },
          "masked": false,
          "match": false,
          "source": {
            "max": "75",
            "min": "1.25",
            "sum": "2287.50"
          }
        },
        {
          "column": "id",
          "extract": {
            "max": "59",
            "min": "1",
            "sum": "1770"
          },
          "masked": false,
          "match": false,
          "source": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          }
        }
      ],
      "differences": [
        {
          "detail": "The stored part does not match the SHA-256 recorded when it was uploaded.",
          "kind": "seal",
          "rows": null,
          "rows_at_least": false
        },
        {
          "detail": "The extract holds 1 row(s) fewer than the source.",
          "kind": "missing_rows",
          "rows": 1,
          "rows_at_least": true
        },
        {
          "detail": "Control totals of amount differ.",
          "kind": "control_total",
          "rows": null,
          "rows_at_least": false
        },
        {
          "detail": "Control totals of id differ.",
          "kind": "control_total",
          "rows": null,
          "rows_at_least": false
        }
      ],
      "extract_seconds": 0.147,
      "job_id": "6af4d2ef20ff77ead7f1a625",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": "96ff0bfca76b6a9361a220b65ca251fdaf565bcf7788b0a2760ef248535a2d72",
        "match": false,
        "source_sha256": "1eee5bfc7da0c1b04c6a916bf35c64faeda2366359bccf0bf3ffe7afd66124c9"
      },
      "name": "orders",
      "part": {
        "name": "exit_exr_2137474be617_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": false
      },
      "reason": "The stored part does not match the SHA-256 recorded when it was uploaded; The extract holds 1 row(s) fewer than the source; Control totals of amount differ; Control totals of id differ.",
      "row_set": {
        "extract_sha256": "e5eff841e775fc095fdbb2eb868126083b6f2901f960c335ad9942017e0dcfdd",
        "match": false,
        "source_sha256": "dd7192488fc1fef0a1bdd78a6dda886ac25ad40d9125d341d1384e1d030e36ba"
      },
      "rows_extracted": 59,
      "rows_source": 60,
      "schema": null,
      "status": "incomplete",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4d2ef20ff77ead7f1a625/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T20:41:19.502973Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T20:41:19.502973Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.652911Z",
      "seconds": 0.133,
      "stage": "extract",
      "started_at": "2026-11-11T20:41:19.520137Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.745152Z",
      "seconds": 0.022,
      "stage": "verify",
      "started_at": "2026-11-11T20:41:19.723103Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.745152Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T20:41:19.745152Z",
      "status": "done"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "sign",
      "started_at": null,
      "status": "pending"
    }
  ],
  "trend": {
    "growth_pct_rows": null,
    "points": [
      {
        "at": "2026-11-11T20:41:19.502973Z",
        "bytes": 2747,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_2137474be617",
        "rows": 59,
        "verdict": "incomplete"
      }
    ],
    "projection": null
  },
  "trigger": "manual",
  "verdict": "incomplete",
  "verdict_reason": "1 of 1 table(s) differ from the source: orders.",
  "version": 4,
  "workspace_id": "ws1"
};

export const realNotProven: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "1 of 1 declared table(s) extracted. 0 proven complete (rows, key sets, every value and control totals). Value check: count only on 1.",
    "tables_declared": 1,
    "tables_extracted": 1,
    "tables_failed": 0,
    "tables_incomplete": 0,
    "tables_not_proven": 1,
    "tables_proven": 0,
    "value_methods": {
      "count_only": 1,
      "full_checksum": 0,
      "key_diff": 0,
      "none": 0,
      "population_digest": 0,
      "sampled": 0
    }
  },
  "definition_sha256": "81ebeadc53c356c8c8ad25cef1bd7a6c61bc8dd9b6f08e3f14f9200f50ebfca8",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": null,
    "margin_pct": null,
    "measured_seconds": 0,
    "statement": "Measured 0 s (extract 0 s, verify 0 s), but the rehearsal is not proven (not proven), so it is not compared with the window.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_16a597f7b00f/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T20:41:19.992493Z",
  "id": "exr_16a597f7b00f",
  "n": 1,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_16a597f7b00f_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      }
    ],
    "parts_count": 1,
    "storage": "local_file",
    "total_bytes": 2747
  },
  "signoff": {
    "approvals": [],
    "blocked_reason": "Only a proven rehearsal can be signed; this one is not proven: 0 of 1 table(s) proven; 1 could not be proven in full (see each table's reason).",
    "invalidated_approvals": [],
    "required_roles": [
      "engineering",
      "data_owner",
      "compliance"
    ],
    "signature": null,
    "signed_at": null,
    "status": "blocked"
  },
  "started_at": "2026-11-11T20:41:19.752771Z",
  "system_id": "exs_bbf985b814a8",
  "system_name": "CRM",
  "tables": [
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": 0.122,
      "job_id": "6af4d2ef20ff77ead7f1a626",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "orders",
      "part": {
        "name": "exit_exr_16a597f7b00f_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Nothing differs where measured, but the source could not be re-read for a population digest (the read failed (RuntimeError): see the server log for the details), so this table is not proven.",
      "row_set": {
        "extract_sha256": "5d6dff9bdb1694d4530d5f03f62f64794cd77187947685f6930fe270336333ec",
        "match": null,
        "source_sha256": null
      },
      "rows_extracted": 60,
      "rows_source": null,
      "schema": null,
      "status": "not_proven",
      "value_parity": {
        "method": "count_only",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4d2ef20ff77ead7f1a626/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T20:41:19.752771Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T20:41:19.752771Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.883164Z",
      "seconds": 0.111,
      "stage": "extract",
      "started_at": "2026-11-11T20:41:19.772614Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.992493Z",
      "seconds": 0.016,
      "stage": "verify",
      "started_at": "2026-11-11T20:41:19.976060Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:19.992493Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T20:41:19.992493Z",
      "status": "done"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "sign",
      "started_at": null,
      "status": "pending"
    }
  ],
  "trend": {
    "growth_pct_rows": null,
    "points": [
      {
        "at": "2026-11-11T20:41:19.752771Z",
        "bytes": 2747,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_16a597f7b00f",
        "rows": 60,
        "verdict": "not_proven"
      }
    ],
    "projection": null
  },
  "trigger": "manual",
  "verdict": "not_proven",
  "verdict_reason": "0 of 1 table(s) proven; 1 could not be proven in full (see each table's reason).",
  "version": 4,
  "workspace_id": "ws1"
};

export const realFailed: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "1 of 2 declared table(s) extracted. 1 proven complete (rows, key sets, every value and control totals). Value check: population digest on 1, none on 1.",
    "tables_declared": 2,
    "tables_extracted": 1,
    "tables_failed": 1,
    "tables_incomplete": 0,
    "tables_not_proven": 0,
    "tables_proven": 1,
    "value_methods": {
      "count_only": 0,
      "full_checksum": 0,
      "key_diff": 0,
      "none": 1,
      "population_digest": 1,
      "sampled": 0
    }
  },
  "definition_sha256": "694b0d18727f6c711aad8a9d10ac0cb984b85fe3af52d9526d4e80c212d82a24",
  "duration": {
    "basis": "measured",
    "extract_seconds": 3,
    "fits_window": null,
    "margin_pct": null,
    "measured_seconds": 3,
    "statement": "Measured 3 s (extract 3 s, verify 0 s), but the rehearsal is not proven (failed), so it is not compared with the window.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_6629ab42c485/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T20:41:23.074239Z",
  "id": "exr_6629ab42c485",
  "n": 1,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_6629ab42c485_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      }
    ],
    "parts_count": 1,
    "storage": "local_file",
    "total_bytes": 2747
  },
  "signoff": {
    "approvals": [],
    "blocked_reason": "Only a proven rehearsal can be signed; this one is failed: 1 of 2 table(s) could not be extracted: no_such_table.",
    "invalidated_approvals": [],
    "required_roles": [
      "engineering",
      "data_owner",
      "compliance"
    ],
    "signature": null,
    "signed_at": null,
    "status": "blocked"
  },
  "started_at": "2026-11-11T20:41:20.001638Z",
  "system_id": "exs_277101d9a7d6",
  "system_name": "Warehouse",
  "tables": [
    {
      "control_totals": [
        {
          "column": "amount",
          "extract": {
            "max": "75",
            "min": "1.25",
            "sum": "2287.50"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "75",
            "min": "1.25",
            "sum": "2287.50"
          }
        },
        {
          "column": "id",
          "extract": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.139,
      "job_id": "6af4d2f020ff77ead7f1a627",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": "1eee5bfc7da0c1b04c6a916bf35c64faeda2366359bccf0bf3ffe7afd66124c9",
        "match": true,
        "source_sha256": "1eee5bfc7da0c1b04c6a916bf35c64faeda2366359bccf0bf3ffe7afd66124c9"
      },
      "name": "orders",
      "part": {
        "name": "exit_exr_6629ab42c485_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Rows, key set, every value (by population digest) and control totals match over all 60 row(s); the part verified on read.",
      "row_set": {
        "extract_sha256": "dd7192488fc1fef0a1bdd78a6dda886ac25ad40d9125d341d1384e1d030e36ba",
        "match": true,
        "source_sha256": "dd7192488fc1fef0a1bdd78a6dda886ac25ad40d9125d341d1384e1d030e36ba"
      },
      "rows_extracted": 60,
      "rows_source": 60,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4d2f020ff77ead7f1a627/parity-report",
        "unexplained": 0
      }
    },
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": null,
      "job_id": "6af4d2f020ff77ead7f1a628",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "no_such_table",
      "part": null,
      "reason": "The extract job failed: see the server log for the details.",
      "row_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "rows_extracted": null,
      "rows_source": null,
      "schema": null,
      "status": "failed",
      "value_parity": {
        "method": "none",
        "parity_verdict": null,
        "report": "/api/v1/jobs/6af4d2f020ff77ead7f1a628/parity-report",
        "unexplained": null
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T20:41:20.001638Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T20:41:20.001638Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:22.911827Z",
      "seconds": 2.879,
      "stage": "extract",
      "started_at": "2026-11-11T20:41:20.032338Z",
      "status": "failed"
    },
    {
      "finished_at": "2026-11-11T20:41:23.074239Z",
      "seconds": 0.031,
      "stage": "verify",
      "started_at": "2026-11-11T20:41:23.043531Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:23.074239Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T20:41:23.074239Z",
      "status": "done"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "sign",
      "started_at": null,
      "status": "pending"
    }
  ],
  "trend": {
    "growth_pct_rows": null,
    "points": [
      {
        "at": "2026-11-11T20:41:20.001638Z",
        "bytes": 2747,
        "measured_seconds": 3,
        "n": 1,
        "rehearsal_id": "exr_6629ab42c485",
        "rows": 60,
        "verdict": "failed"
      }
    ],
    "projection": null
  },
  "trigger": "manual",
  "verdict": "failed",
  "verdict_reason": "1 of 2 table(s) could not be extracted: no_such_table.",
  "version": 4,
  "workspace_id": "ws1"
};

export const realRunning: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "The rehearsal is still extracting or verifying.",
    "tables_declared": 1,
    "tables_extracted": 0,
    "tables_failed": 0,
    "tables_incomplete": 0,
    "tables_not_proven": 0,
    "tables_proven": 0,
    "value_methods": {}
  },
  "definition_sha256": "81ebeadc53c356c8c8ad25cef1bd7a6c61bc8dd9b6f08e3f14f9200f50ebfca8",
  "duration": {
    "basis": "measured",
    "extract_seconds": null,
    "fits_window": null,
    "margin_pct": null,
    "measured_seconds": null,
    "statement": "Not measured yet: the rehearsal is still running.",
    "verify_seconds": null,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_453883a79c6f/evidence",
    "signed": false
  },
  "finished_at": null,
  "id": "exr_453883a79c6f",
  "n": 1,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": null,
    "parts": [],
    "parts_count": 0,
    "storage": null,
    "total_bytes": 0
  },
  "signoff": {
    "approvals": [],
    "blocked_reason": "The rehearsal is still running; sign-off opens when it is proven.",
    "invalidated_approvals": [],
    "required_roles": [
      "engineering",
      "data_owner",
      "compliance"
    ],
    "signature": null,
    "signed_at": null,
    "status": "blocked"
  },
  "started_at": "2026-11-11T20:41:23.828600Z",
  "system_id": "exs_3bcdda2d273d",
  "system_name": "Running one",
  "tables": [
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": null,
      "job_id": "6af4d2f320ff77ead7f1a62c",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "orders",
      "part": null,
      "reason": "Extracting (running).",
      "row_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "rows_extracted": null,
      "rows_source": null,
      "schema": null,
      "status": "running",
      "value_parity": {
        "method": null,
        "parity_verdict": null,
        "report": null,
        "unexplained": null
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T20:41:23.828600Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T20:41:23.828600Z",
      "status": "done"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "extract",
      "started_at": null,
      "status": "running"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "verify",
      "started_at": null,
      "status": "pending"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "seal",
      "started_at": null,
      "status": "pending"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "sign",
      "started_at": null,
      "status": "pending"
    }
  ],
  "trend": {
    "growth_pct_rows": null,
    "points": [],
    "projection": null
  },
  "trigger": "manual",
  "verdict": "running",
  "verdict_reason": "The rehearsal is still extracting or verifying.",
  "version": 2,
  "workspace_id": "ws1"
};

export const realMasked: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "1 of 1 declared table(s) extracted. 1 proven complete (rows, key sets, every value and control totals). Value check: population digest on 1.",
    "tables_declared": 1,
    "tables_extracted": 1,
    "tables_failed": 0,
    "tables_incomplete": 0,
    "tables_not_proven": 0,
    "tables_proven": 1,
    "value_methods": {
      "count_only": 0,
      "full_checksum": 0,
      "key_diff": 0,
      "none": 0,
      "population_digest": 1,
      "sampled": 0
    }
  },
  "definition_sha256": "09a1ebb4fac9a22bfbc51d6b04ec9c8971272168a681f0ef7b914a13baa2988f",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 0,
    "statement": "Measured 0 s for 821 bytes (extract 0 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_21a75d2edd90/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T20:41:23.327459Z",
  "id": "exr_21a75d2edd90",
  "n": 1,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:c678f87a3ffc0085212a42aa1aaa6395b6dae7830cb626202b75386d4300edde",
    "parts": [
      {
        "name": "exit_exr_21a75d2edd90_0000_customers.jsonl",
        "object_key": null,
        "sha256": "2738dd048706091ad8eea58759f853d5752f4e7bf1523459defc68453f5c2800",
        "size_bytes": 821,
        "storage": "local_file",
        "table": "customers",
        "upload_parts": null,
        "verified_on_read": true
      }
    ],
    "parts_count": 1,
    "storage": "local_file",
    "total_bytes": 821
  },
  "signoff": {
    "approvals": [],
    "blocked_reason": null,
    "invalidated_approvals": [],
    "required_roles": [
      "engineering",
      "data_owner",
      "compliance"
    ],
    "signature": null,
    "signed_at": null,
    "status": "open"
  },
  "started_at": "2026-11-11T20:41:23.091373Z",
  "system_id": "exs_8a27e123152b",
  "system_name": "Customer master",
  "tables": [
    {
      "control_totals": [
        {
          "column": "balance",
          "extract": {
            "max": "210",
            "min": "10.5",
            "sum": "2205.0"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "210",
            "min": "10.5",
            "sum": "2205.0"
          }
        },
        {
          "column": "id",
          "extract": {
            "max": "20",
            "min": "1",
            "sum": "210"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "20",
            "min": "1",
            "sum": "210"
          }
        },
        {
          "column": "zip",
          "extract": {
            "max": null,
            "min": null,
            "sum": null
          },
          "masked": true,
          "match": true,
          "source": {
            "max": null,
            "min": null,
            "sum": null
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.065,
      "job_id": "6af4d2f320ff77ead7f1a629",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": "5df1b51df22269ff9ae9721a2a6de0b338f08d200df6c2c6ec824a7616fccca9",
        "match": true,
        "source_sha256": "5df1b51df22269ff9ae9721a2a6de0b338f08d200df6c2c6ec824a7616fccca9"
      },
      "name": "customers",
      "part": {
        "name": "exit_exr_21a75d2edd90_0000_customers.jsonl",
        "object_key": null,
        "sha256": "2738dd048706091ad8eea58759f853d5752f4e7bf1523459defc68453f5c2800",
        "size_bytes": 821,
        "storage": "local_file",
        "table": "customers",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Rows, key set, every value (by population digest) and control totals match over all 20 row(s); the part verified on read.",
      "row_set": {
        "extract_sha256": "d2f28df0eb17177d307273fd9b4bf60e20a897132478f781711cd7416e3a8c97",
        "match": true,
        "source_sha256": "d2f28df0eb17177d307273fd9b4bf60e20a897132478f781711cd7416e3a8c97"
      },
      "rows_extracted": 20,
      "rows_source": 20,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4d2f320ff77ead7f1a629/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T20:41:23.091373Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T20:41:23.091373Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:23.165765Z",
      "seconds": 0.056,
      "stage": "extract",
      "started_at": "2026-11-11T20:41:23.109592Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:23.327459Z",
      "seconds": 0.014,
      "stage": "verify",
      "started_at": "2026-11-11T20:41:23.313594Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:23.327459Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T20:41:23.327459Z",
      "status": "done"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "sign",
      "started_at": null,
      "status": "pending"
    }
  ],
  "trend": {
    "growth_pct_rows": null,
    "points": [
      {
        "at": "2026-11-11T20:41:23.091373Z",
        "bytes": 821,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_21a75d2edd90",
        "rows": 20,
        "verdict": "proven"
      }
    ],
    "projection": null
  },
  "trigger": "manual",
  "verdict": "proven",
  "verdict_reason": "Every one of the 1 declared table(s) was extracted and proven complete.",
  "version": 4,
  "workspace_id": "ws1"
};

export const realUnprovable: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "1 of 1 declared table(s) extracted. 0 proven complete (rows, key sets, every value and control totals). Value check: population digest on 1.",
    "tables_declared": 1,
    "tables_extracted": 1,
    "tables_failed": 0,
    "tables_incomplete": 0,
    "tables_not_proven": 1,
    "tables_proven": 0,
    "value_methods": {
      "count_only": 0,
      "full_checksum": 0,
      "key_diff": 0,
      "none": 0,
      "population_digest": 1,
      "sampled": 0
    }
  },
  "definition_sha256": "c95f7fb7da3f5fd8f461ab7f81a1cbde220fa70a6f75b05b586c1edbf25debf3",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": null,
    "margin_pct": null,
    "measured_seconds": 0,
    "statement": "Measured 0 s (extract 0 s, verify 0 s), but the rehearsal is not proven (not proven), so it is not compared with the window.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_19a4d2a98943/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T20:41:23.559672Z",
  "id": "exr_19a4d2a98943",
  "n": 1,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:7147b7cfdae6c569a288affb7862d0f7946634a16a2c184646fc72214ec4d4af",
    "parts": [
      {
        "name": "exit_exr_19a4d2a98943_0000_notes.jsonl",
        "object_key": null,
        "sha256": "4762bd3dd3496d2b5c6af92b7e1d7fb429dcdb830aa854fb75a20d1d7fdff3b7",
        "size_bytes": 154,
        "storage": "local_file",
        "table": "notes",
        "upload_parts": null,
        "verified_on_read": true
      }
    ],
    "parts_count": 1,
    "storage": "local_file",
    "total_bytes": 154
  },
  "signoff": {
    "approvals": [],
    "blocked_reason": "Only a proven rehearsal can be signed; this one is not proven: 0 of 1 table(s) proven; 1 could not be proven in full (see each table's reason).",
    "invalidated_approvals": [],
    "required_roles": [
      "engineering",
      "data_owner",
      "compliance"
    ],
    "signature": null,
    "signed_at": null,
    "status": "blocked"
  },
  "started_at": "2026-11-11T20:41:23.336478Z",
  "system_id": "exs_fdae60e0ad69",
  "system_name": "Notes archive",
  "tables": [
    {
      "control_totals": [
        {
          "column": "id",
          "extract": {
            "max": "5",
            "min": "1",
            "sum": "15"
          },
          "masked": false,
          "match": true,
          "source": {
            "max": "5",
            "min": "1",
            "sum": "15"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.035,
      "job_id": "6af4d2f320ff77ead7f1a62a",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": "c223d3b6bbadacc2aefb58e5bba08dc1cacc730717550ed07d41e0eb191bfaa7",
        "match": true,
        "source_sha256": "c223d3b6bbadacc2aefb58e5bba08dc1cacc730717550ed07d41e0eb191bfaa7"
      },
      "name": "notes",
      "part": {
        "name": "exit_exr_19a4d2a98943_0000_notes.jsonl",
        "object_key": null,
        "sha256": "4762bd3dd3496d2b5c6af92b7e1d7fb429dcdb830aa854fb75a20d1d7fdff3b7",
        "size_bytes": 154,
        "storage": "local_file",
        "table": "notes",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Nothing differs where measured, but payload has no declared type, so its values could not be compared exactly, so this table is not proven.",
      "row_set": {
        "extract_sha256": "84c505b86c28139d7503a69f14beeb846ec9bc9be3ed7ff4af4a05667ddd5072",
        "match": true,
        "source_sha256": "84c505b86c28139d7503a69f14beeb846ec9bc9be3ed7ff4af4a05667ddd5072"
      },
      "rows_extracted": 5,
      "rows_source": 5,
      "schema": null,
      "status": "not_proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4d2f320ff77ead7f1a62a/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T20:41:23.336478Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T20:41:23.336478Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:23.377829Z",
      "seconds": 0.031,
      "stage": "extract",
      "started_at": "2026-11-11T20:41:23.346562Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:23.559672Z",
      "seconds": 0.008,
      "stage": "verify",
      "started_at": "2026-11-11T20:41:23.551665Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T20:41:23.559672Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T20:41:23.559672Z",
      "status": "done"
    },
    {
      "finished_at": null,
      "seconds": null,
      "stage": "sign",
      "started_at": null,
      "status": "pending"
    }
  ],
  "trend": {
    "growth_pct_rows": null,
    "points": [
      {
        "at": "2026-11-11T20:41:23.336478Z",
        "bytes": 154,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_19a4d2a98943",
        "rows": 5,
        "verdict": "not_proven"
      }
    ],
    "projection": null
  },
  "trigger": "manual",
  "verdict": "not_proven",
  "verdict_reason": "0 of 1 table(s) proven; 1 could not be proven in full (see each table's reason).",
  "version": 4,
  "workspace_id": "ws1"
};

export const realSystemStale: ExitSystem = {
  "created_at": "2026-11-11T20:41:23.565605Z",
  "created_by": "owner@example.com",
  "criticality": "important",
  "description": "",
  "exit_window_hours": 48,
  "extract_format": "jsonl",
  "id": "exs_fd5250928bdf",
  "name": "Billing",
  "readiness": {
    "last_proven_rehearsal": {
      "current": false,
      "finished_at": "2026-11-11T20:41:23.817148Z",
      "fits_window": true,
      "id": "exr_1ed55fa225fe",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T20:41:23.569858Z",
      "tables_declared": 1,
      "tables_proven": 1,
      "total_bytes": 2747,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "last_rehearsal": {
      "current": false,
      "finished_at": "2026-11-11T20:41:23.817148Z",
      "fits_window": true,
      "id": "exr_1ed55fa225fe",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T20:41:23.569858Z",
      "tables_declared": 1,
      "tables_proven": 1,
      "total_bytes": 2747,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-09T20:41:23.569858Z",
    "overdue": false,
    "running_rehearsal_id": null,
    "stale": true,
    "stale_reason": "The system's source, tables, keys or window changed since rehearsal 1; rehearse it again to prove the current definition.",
    "state": "not_proven"
  },
  "regulatory_dates": [],
  "rehearsal_every_days": 90,
  "rehearsal_schedule": null,
  "required_roles": [
    "engineering",
    "data_owner",
    "compliance"
  ],
  "running_rehearsal_id": null,
  "source": {
    "connector_id": "c1",
    "connector_name": "ledger",
    "engine": "sqlite"
  },
  "tables": [
    {
      "key_columns": [],
      "name": "orders",
      "schema": null
    }
  ],
  "updated_at": "2026-11-11T20:41:23.822321Z",
  "updated_by": "owner@example.com",
  "version": 4,
  "window_basis": "",
  "workspace_id": "ws1"
};

export const realSystemRunning: ExitSystem = {
  "created_at": "2026-11-11T20:41:23.824974Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_3bcdda2d273d",
  "name": "Running one",
  "readiness": {
    "last_proven_rehearsal": null,
    "last_rehearsal": {
      "current": true,
      "finished_at": null,
      "fits_window": null,
      "id": "exr_453883a79c6f",
      "measured_seconds": null,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T20:41:23.828600Z",
      "tables_declared": 0,
      "tables_proven": 0,
      "total_bytes": null,
      "verdict": "running",
      "window_seconds": null
    },
    "next_due_at": "2027-02-09T20:41:23.828600Z",
    "overdue": false,
    "running_rehearsal_id": "exr_453883a79c6f",
    "stale": false,
    "stale_reason": null,
    "state": "running"
  },
  "regulatory_dates": [],
  "rehearsal_every_days": 90,
  "rehearsal_schedule": null,
  "required_roles": [
    "engineering",
    "data_owner",
    "compliance"
  ],
  "running_rehearsal_id": "exr_453883a79c6f",
  "source": {
    "connector_id": "c1",
    "connector_name": "ledger",
    "engine": "sqlite"
  },
  "tables": [
    {
      "key_columns": [],
      "name": "orders",
      "schema": null
    }
  ],
  "updated_at": "2026-11-11T20:41:23.824974Z",
  "updated_by": "owner@example.com",
  "version": 2,
  "window_basis": "",
  "workspace_id": "ws1"
};

export const realReversibilityCodes: ReversibilityReport = {
  "counts": {
    "conditional": 2,
    "invertible": 1,
    "lossy": 5
  },
  "data_checked": {
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "This transfer must be reversible, and it is not: events.created_at, events.payload, events.code, events.span, events.(rows), events.(rows), events.(rows). Remove the lossy transform, widen the destination, or turn off require reversibility.",
    "status": "block"
  },
  "generated_at": "2026-11-11T20:41:24.066095Z",
  "lossy_columns": [
    {
      "column": "created_at",
      "reason": "created_at drops the time zone (timestamp with time zone to timestamp without time zone); the original offset cannot be recovered.",
      "reason_code": "timezone",
      "table": "events"
    },
    {
      "column": "payload",
      "reason": "payload is stored as jsonb, which keeps one value per key in its own key order and spacing; the original json text cannot be rebuilt.",
      "reason_code": "json_normalize",
      "table": "events"
    },
    {
      "column": "(rows)",
      "reason": "the recipe step rank_rows changes which rows land, so the source rows cannot be rebuilt.",
      "reason_code": "row_change",
      "table": "events"
    },
    {
      "column": "(rows)",
      "reason": "the row cap (limit 1000) writes at most 1000 source row(s), so the rows left out cannot be rebuilt from the destination.",
      "reason_code": "row_change",
      "table": "events"
    },
    {
      "column": "(rows)",
      "reason": "the source filter writes only the rows it matches, so the rows it leaves out cannot be rebuilt from the destination.",
      "reason_code": "row_change",
      "table": "events"
    }
  ],
  "require_reversibility": true,
  "rollback_note": "events needs the source kept as the fallback until sign-off.",
  "scope": "plan",
  "subject_id": "plan-2",
  "tables": [
    {
      "columns": [
        {
          "check": null,
          "invertibility": "invertible",
          "reason": "id is copied unchanged.",
          "reason_code": "identity",
          "source": "id",
          "source_type": "BIGINT",
          "target": "id",
          "target_type": "BIGINT",
          "transforms": []
        },
        {
          "check": null,
          "invertibility": "lossy",
          "reason": "created_at drops the time zone (timestamp with time zone to timestamp without time zone); the original offset cannot be recovered.",
          "reason_code": "timezone",
          "source": "created_at",
          "source_type": "timestamp with time zone",
          "target": "created_at",
          "target_type": "timestamp without time zone",
          "transforms": []
        },
        {
          "check": null,
          "invertibility": "lossy",
          "reason": "payload is stored as jsonb, which keeps one value per key in its own key order and spacing; the original json text cannot be rebuilt.",
          "reason_code": "json_normalize",
          "source": "payload",
          "source_type": "json",
          "target": "payload",
          "target_type": "jsonb",
          "transforms": []
        },
        {
          "check": {
            "rows_checked": 2,
            "rows_not_fitting": 1,
            "status": "does_not_fit"
          },
          "invertibility": "conditional",
          "reason": "code is padded to the fixed width of char(10); reversible only if no value ends in a space; 1 of 2 observed value(s) would change.",
          "reason_code": "blank_pad",
          "source": "code",
          "source_type": "varchar(10)",
          "target": "code",
          "target_type": "char(10)",
          "transforms": []
        },
        {
          "check": {
            "rows_checked": 2,
            "rows_not_fitting": 1,
            "status": "does_not_fit"
          },
          "invertibility": "conditional",
          "reason": "span is cast interval to varchar(8); reversible only if every value's text fits the width; 1 of 2 observed value(s) would change.",
          "reason_code": "text_width",
          "source": "span",
          "source_type": "interval",
          "target": "span",
          "target_type": "varchar(8)",
          "transforms": []
        },
        {
          "check": null,
          "invertibility": "lossy",
          "reason": "the recipe step rank_rows changes which rows land, so the source rows cannot be rebuilt.",
          "reason_code": "row_change",
          "source": null,
          "source_type": null,
          "target": null,
          "target_type": null,
          "transforms": [
            "rank_rows"
          ]
        },
        {
          "check": null,
          "invertibility": "lossy",
          "reason": "the row cap (limit 1000) writes at most 1000 source row(s), so the rows left out cannot be rebuilt from the destination.",
          "reason_code": "row_change",
          "source": null,
          "source_type": null,
          "target": null,
          "target_type": null,
          "transforms": [
            "limit"
          ]
        },
        {
          "check": null,
          "invertibility": "lossy",
          "reason": "the source filter writes only the rows it matches, so the rows it leaves out cannot be rebuilt from the destination.",
          "reason_code": "row_change",
          "source": null,
          "source_type": null,
          "target": null,
          "target_type": null,
          "transforms": [
            "source_filter"
          ]
        }
      ],
      "name": "events",
      "reason": "created_at drops the time zone (timestamp with time zone to timestamp without time zone); the original offset cannot be recovered; payload is stored as jsonb, which keeps one value per key in its own key order and spacing; the original json text cannot be rebuilt; the recipe step rank_rows changes which rows land, so the source rows cannot be rebuilt; the row cap (limit 1000) writes at most 1000 source row(s), so the rows left out cannot be rebuilt from the destination; the source filter writes only the rows it matches, so the rows it leaves out cannot be rebuilt from the destination; code is padded to the fixed width of char(10); reversible only if no value ends in a space; 1 of 2 observed value(s) would change (+1 more).",
      "rollback": "keep_source_as_fallback",
      "rollback_note": "events needs the source kept as the fallback until sign-off.",
      "verdict": "partly_reversible"
    }
  ],
  "verdict": "partly_reversible",
  "verdict_reason": "1 table(s): 1 column(s) invertible, 2 conditional and not shown to fit, 5 lossy."
};

export const realReversibilityEnum: ReversibilityReport = {
  "counts": {
    "conditional": 0,
    "invertible": 2,
    "lossy": 0
  },
  "data_checked": {
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "Reversibility is not required for this transfer; the analysis is shown for review.",
    "status": "skip"
  },
  "generated_at": "2026-11-11T20:41:24.066677Z",
  "lossy_columns": [],
  "require_reversibility": false,
  "rollback_note": "tickets can be restored from the destination (every mapping is lossless). Rollback plan: DOCUMENT_ONLY runbook; Datawrap does not undo committed rows.",
  "scope": "plan",
  "subject_id": "plan-3",
  "tables": [
    {
      "columns": [
        {
          "check": null,
          "invertibility": "invertible",
          "reason": "id is copied unchanged.",
          "reason_code": "identity",
          "source": "id",
          "source_type": "INT",
          "target": "id",
          "target_type": "INT",
          "transforms": []
        },
        {
          "check": {
            "rows_checked": 2,
            "rows_not_fitting": 0,
            "status": "fits"
          },
          "invertibility": "conditional",
          "reason": "state is cast varchar(10) to enum('open','closed'); reversible only if every value is one of its labels; every one of the 2 observed value(s) round-trips, so it is invertible for the data seen (rows not sampled are not proven).",
          "reason_code": "enum",
          "source": "state",
          "source_type": "varchar(10)",
          "target": "state",
          "target_type": "enum('open','closed')",
          "transforms": []
        }
      ],
      "name": "tickets",
      "reason": "Every mapping is lossless, and no transform drops information.",
      "rollback": "restore_from_destination",
      "rollback_note": "tickets can be restored from the destination (every mapping is lossless). Rollback plan: DOCUMENT_ONLY runbook; Datawrap does not undo committed rows.",
      "verdict": "reversible"
    }
  ],
  "verdict": "reversible",
  "verdict_reason": "1 table(s): 2 column(s) invertible, 0 conditional and not shown to fit, 0 lossy."
};

export const realVerify: ExitVerifyResult = {
  "anchor": {
    "checked_at": "2026-11-11T20:41:19.500395+00:00",
    "digest": "92cab189af2239d85c2b23b0928d4b25b5ff4153814d0e4272ad22753e2e300f",
    "provider": "stub",
    "reason": "the local stub is not a write-once store",
    "retention": null,
    "status": "not_anchored"
  },
  "content_sha256": "92cab189af2239d85c2b23b0928d4b25b5ff4153814d0e4272ad22753e2e300f",
  "errors": [],
  "kind": "exit_evidence",
  "merkle_ok": true,
  "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
  "note": "The stored extract parts are not re-read here: the Merkle root is recomputed from the part hashes the pack lists. Comparing a stored part with its listed SHA-256 needs the part itself (it may have expired from the export bucket).",
  "ok": true,
  "parts_reread": false,
  "signature_ok": true
};

export const realPlanReversibility: ReversibilityReport = {
  "counts": {
    "conditional": 0,
    "invertible": 1,
    "lossy": 3
  },
  "data_checked": {
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "This transfer must be reversible, and it is not: payments.amount, payments.email, payments.note. Remove the lossy transform, widen the destination, or turn off require reversibility.",
    "status": "block"
  },
  "generated_at": "2026-11-11T20:41:24.065298Z",
  "lossy_columns": [
    {
      "column": "amount",
      "reason": "amount narrows NUMERIC(12,4) to NUMERIC(12,2); values that do not fit lose data; 1 of 2 observed row(s) would lose data.",
      "reason_code": "narrowing",
      "table": "payments"
    },
    {
      "column": "email",
      "reason": "email is hashed (hash_pii); the original value cannot be recovered from the destination.",
      "reason_code": "hash_pii",
      "table": "payments"
    },
    {
      "column": "note",
      "reason": "note is not mapped, so the destination does not hold it.",
      "reason_code": "dropped",
      "table": "payments"
    }
  ],
  "require_reversibility": true,
  "rollback_note": "payments needs the source kept as the fallback until sign-off.",
  "scope": "plan",
  "subject_id": "plan-1",
  "tables": [
    {
      "columns": [
        {
          "check": null,
          "invertibility": "invertible",
          "reason": "id widens INTEGER to BIGINT; every value fits.",
          "reason_code": "widen",
          "source": "id",
          "source_type": "INTEGER",
          "target": "id",
          "target_type": "BIGINT",
          "transforms": []
        },
        {
          "check": {
            "rows_checked": 2,
            "rows_not_fitting": 1,
            "status": "does_not_fit"
          },
          "invertibility": "lossy",
          "reason": "amount narrows NUMERIC(12,4) to NUMERIC(12,2); values that do not fit lose data; 1 of 2 observed row(s) would lose data.",
          "reason_code": "narrowing",
          "source": "amount",
          "source_type": "NUMERIC(12,4)",
          "target": "amount",
          "target_type": "NUMERIC(12,2)",
          "transforms": []
        },
        {
          "check": null,
          "invertibility": "lossy",
          "reason": "email is hashed (hash_pii); the original value cannot be recovered from the destination.",
          "reason_code": "hash_pii",
          "source": "email",
          "source_type": "TEXT",
          "target": "email_hash",
          "target_type": "TEXT",
          "transforms": [
            "hash_pii"
          ]
        },
        {
          "check": null,
          "invertibility": "lossy",
          "reason": "note is not mapped, so the destination does not hold it.",
          "reason_code": "dropped",
          "source": "note",
          "source_type": "TEXT",
          "target": null,
          "target_type": null,
          "transforms": []
        }
      ],
      "name": "payments",
      "reason": "amount narrows NUMERIC(12,4) to NUMERIC(12,2); values that do not fit lose data; 1 of 2 observed row(s) would lose data; email is hashed (hash_pii); the original value cannot be recovered from the destination; note is not mapped, so the destination does not hold it.",
      "rollback": "keep_source_as_fallback",
      "rollback_note": "payments needs the source kept as the fallback until sign-off.",
      "verdict": "partly_reversible"
    }
  ],
  "verdict": "partly_reversible",
  "verdict_reason": "1 table(s): 1 column(s) invertible, 0 conditional and not shown to fit, 3 lossy."
};

/** The signed Exit Evidence Pack (JSON export). */
export const realPack: Record<string, unknown> = {
  "actor": "compliance@example.com",
  "content_sha256": "92cab189af2239d85c2b23b0928d4b25b5ff4153814d0e4272ad22753e2e300f",
  "content_sha256_attested": "3e04b1d2ae6f856a558b3f3974d551fb36febc4f3a77c03f6493c8a3f4b981df",
  "issued_at": "2026-11-11T20:41:19.493375Z",
  "kind": "exit_evidence",
  "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
  "parts": [
    {
      "name": "exit_exr_87ea0580cbdf_0000_orders.jsonl",
      "object_key": null,
      "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
      "size_bytes": 2747,
      "storage": "local_file",
      "table": "orders",
      "upload_parts": null,
      "verified_on_read": true
    },
    {
      "name": "exit_exr_87ea0580cbdf_0001_events.jsonl",
      "object_key": null,
      "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
      "size_bytes": 71,
      "storage": "local_file",
      "table": "events",
      "upload_parts": null,
      "verified_on_read": true
    }
  ],
  "rehearsal": {
    "actor": "owner@example.com",
    "completeness": {
      "statement": "2 of 2 declared table(s) extracted. 2 proven complete (rows, key sets, every value and control totals). Value check: population digest on 2.",
      "tables_declared": 2,
      "tables_extracted": 2,
      "tables_failed": 0,
      "tables_incomplete": 0,
      "tables_not_proven": 0,
      "tables_proven": 2,
      "value_methods": {
        "count_only": 0,
        "full_checksum": 0,
        "key_diff": 0,
        "none": 0,
        "population_digest": 2,
        "sampled": 0
      }
    },
    "definition_sha256": "37c6a0114406f8551facf70cc034edf393d286cfcc64f0fa264d4ddcec12e6be",
    "duration": {
      "basis": "measured",
      "extract_seconds": 0,
      "fits_window": true,
      "margin_pct": 100.0,
      "measured_seconds": 0,
      "statement": "Measured 0 s for 2,818 bytes (extract 0 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
      "verify_seconds": 0,
      "window_seconds": 2592000
    },
    "finished_at": "2026-11-11T20:41:19.484255Z",
    "id": "exr_87ea0580cbdf",
    "n": 2,
    "seal": {
      "algorithm": "sha256-merkle-rfc6962",
      "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
      "parts": [
        {
          "name": "exit_exr_87ea0580cbdf_0000_orders.jsonl",
          "object_key": null,
          "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
          "size_bytes": 2747,
          "storage": "local_file",
          "table": "orders",
          "upload_parts": null,
          "verified_on_read": true
        },
        {
          "name": "exit_exr_87ea0580cbdf_0001_events.jsonl",
          "object_key": null,
          "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
          "size_bytes": 71,
          "storage": "local_file",
          "table": "events",
          "upload_parts": null,
          "verified_on_read": true
        }
      ],
      "parts_count": 2,
      "storage": "local_file",
      "total_bytes": 2818
    },
    "signoff": {
      "approvals": [
        {
          "at": "2026-11-11T20:41:19.489028Z",
          "authority": "workspace admin",
          "content_sha256": "3e04b1d2ae6f856a558b3f3974d551fb36febc4f3a77c03f6493c8a3f4b981df",
          "note": "",
          "role": "engineering",
          "signer_role": "admin",
          "user": "engineering@example.com",
          "version": 4
        },
        {
          "at": "2026-11-11T20:41:19.491243Z",
          "authority": "workspace admin",
          "content_sha256": "3e04b1d2ae6f856a558b3f3974d551fb36febc4f3a77c03f6493c8a3f4b981df",
          "note": "",
          "role": "data_owner",
          "signer_role": "admin",
          "user": "data_owner@example.com",
          "version": 5
        },
        {
          "at": "2026-11-11T20:41:19.493365Z",
          "authority": "workspace admin",
          "content_sha256": "3e04b1d2ae6f856a558b3f3974d551fb36febc4f3a77c03f6493c8a3f4b981df",
          "note": "",
          "role": "compliance",
          "signer_role": "admin",
          "user": "compliance@example.com",
          "version": 6
        }
      ],
      "blocked_reason": null,
      "invalidated_approvals": [],
      "required_roles": [
        "engineering",
        "data_owner",
        "compliance"
      ],
      "signature": null,
      "signed_at": "2026-11-11T20:41:19.493375Z",
      "status": "signed"
    },
    "started_at": "2026-11-11T20:41:19.222967Z",
    "system_id": "exs_aa5f2aa2b06b",
    "system_name": "Core ledger",
    "tables": [
      {
        "control_totals": [
          {
            "column": "amount",
            "extract": {
              "max": "75",
              "min": "1.25",
              "sum": "2287.50"
            },
            "masked": false,
            "match": true,
            "source": {
              "max": "75",
              "min": "1.25",
              "sum": "2287.50"
            }
          },
          {
            "column": "id",
            "extract": {
              "max": "60",
              "min": "1",
              "sum": "1830"
            },
            "masked": false,
            "match": true,
            "source": {
              "max": "60",
              "min": "1",
              "sum": "1830"
            }
          }
        ],
        "differences": [],
        "extract_seconds": 0.169,
        "job_id": "6af4d2ef20ff77ead7f1a623",
        "key_columns": [
          "id"
        ],
        "key_set": {
          "extract_sha256": "1eee5bfc7da0c1b04c6a916bf35c64faeda2366359bccf0bf3ffe7afd66124c9",
          "match": true,
          "source_sha256": "1eee5bfc7da0c1b04c6a916bf35c64faeda2366359bccf0bf3ffe7afd66124c9"
        },
        "name": "orders",
        "part": {
          "name": "exit_exr_87ea0580cbdf_0000_orders.jsonl",
          "object_key": null,
          "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
          "size_bytes": 2747,
          "storage": "local_file",
          "table": "orders",
          "upload_parts": null,
          "verified_on_read": true
        },
        "reason": "Rows, key set, every value (by population digest) and control totals match over all 60 row(s); the part verified on read.",
        "row_set": {
          "extract_sha256": "dd7192488fc1fef0a1bdd78a6dda886ac25ad40d9125d341d1384e1d030e36ba",
          "match": true,
          "source_sha256": "dd7192488fc1fef0a1bdd78a6dda886ac25ad40d9125d341d1384e1d030e36ba"
        },
        "rows_extracted": 60,
        "rows_source": 60,
        "schema": null,
        "status": "proven",
        "value_parity": {
          "method": "population_digest",
          "parity_verdict": "unverifiable",
          "report": "/api/v1/jobs/6af4d2ef20ff77ead7f1a623/parity-report",
          "unexplained": 0
        }
      },
      {
        "control_totals": [
          {
            "column": "qty",
            "extract": {
              "max": "2",
              "min": "1",
              "sum": "4"
            },
            "masked": false,
            "match": true,
            "source": {
              "max": "2",
              "min": "1",
              "sum": "4"
            }
          }
        ],
        "differences": [],
        "extract_seconds": 0.111,
        "job_id": "6af4d2ef20ff77ead7f1a624",
        "key_columns": [],
        "key_set": {
          "extract_sha256": null,
          "match": null,
          "source_sha256": null
        },
        "name": "events",
        "part": {
          "name": "exit_exr_87ea0580cbdf_0001_events.jsonl",
          "object_key": null,
          "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
          "size_bytes": 71,
          "storage": "local_file",
          "table": "events",
          "upload_parts": null,
          "verified_on_read": true
        },
        "reason": "Rows, every value (by population digest) and control totals match over all 3 row(s); the part verified on read.",
        "row_set": {
          "extract_sha256": "eb76c1445d7f89d6b45b5fb9396d281876db2ea30b1f27232f45ef9d6eb15a64",
          "match": true,
          "source_sha256": "eb76c1445d7f89d6b45b5fb9396d281876db2ea30b1f27232f45ef9d6eb15a64"
        },
        "rows_extracted": 3,
        "rows_source": 3,
        "schema": null,
        "status": "proven",
        "value_parity": {
          "method": "population_digest",
          "parity_verdict": "unverifiable",
          "report": "/api/v1/jobs/6af4d2ef20ff77ead7f1a624/parity-report",
          "unexplained": 0
        }
      }
    ],
    "timeline": [
      {
        "finished_at": "2026-11-11T20:41:19.222967Z",
        "seconds": 0.0,
        "stage": "inventory",
        "started_at": "2026-11-11T20:41:19.222967Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-11T20:41:19.404912Z",
        "seconds": 0.159,
        "stage": "extract",
        "started_at": "2026-11-11T20:41:19.246082Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-11T20:41:19.484255Z",
        "seconds": 0.033,
        "stage": "verify",
        "started_at": "2026-11-11T20:41:19.451585Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-11T20:41:19.484255Z",
        "seconds": 0.0,
        "stage": "seal",
        "started_at": "2026-11-11T20:41:19.484255Z",
        "status": "done"
      },
      {
        "finished_at": null,
        "seconds": null,
        "stage": "sign",
        "started_at": null,
        "status": "pending"
      }
    ],
    "trigger": "manual",
    "verdict": "proven",
    "verdict_reason": "Every one of the 2 declared table(s) was extracted and proven complete.",
    "version": 6,
    "workspace_id": "ws1"
  },
  "rehearsal_id": "exr_87ea0580cbdf",
  "signature": {
    "alg": "HMAC-SHA256",
    "key_id": "platform_auth_secret",
    "value": "7ef0af8d37705ed7a1b03140f2221ba024b780bb2514e1842800ff198d5056c6"
  },
  "system": {
    "criticality": "critical",
    "engine": "sqlite",
    "exit_window_hours": 720,
    "name": "Core ledger",
    "window_basis": "EU Data Act switching window"
  },
  "system_id": "exs_aa5f2aa2b06b",
  "version": "1",
  "workspace_id": "ws1"
};
