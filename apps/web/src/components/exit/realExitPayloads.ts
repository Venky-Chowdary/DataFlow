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
 *
 * Since the second review fixes (contract 11): a rehearsal the PII gate held
 * (needs_pii_review) and its system, the system after a PII acknowledgement and
 * the proven rehearsal that carries it, and a plan report whose conditional
 * column fits only the sample (covers_population false), with a source query
 * row change and an Oracle byte-length column.
 *
 * Since the third review fixes (contract 12): systems carry the PII hold an
 * acknowledgement covers (pii_hold, pii_acknowledgement.covers), rehearsals say
 * whether they still prove the current definition (current / stale_reason, with a
 * stale rehearsal), and a plan report whose conditional column is checked on the
 * sample and enforced on every row at Execute.
 */
import type { ExitReadiness, ExitRehearsal, ExitSystem, ExitVerifyResult, RehearsalSummary, Trend } from "../../lib/exitReadiness";
import type { ReversibilityReport } from "../../lib/reversibility";

export const realBoard: ExitReadiness = {
  "generated_at": "2026-11-12T01:39:08.267211Z",
  "kpis": {
    "longest_measured_seconds": 1,
    "longest_measured_system_id": "exs_bce599c1c2b2",
    "next_regulatory_date": {
      "date": "2027-01-12",
      "label": "Data Act switching charges end",
      "system_id": "exs_bce599c1c2b2"
    },
    "overdue": 0,
    "systems": 9,
    "systems_proven": 3
  },
  "systems": [
    {
      "completeness_label": "Not proven (changed since the last rehearsal)",
      "criticality": "important",
      "engine": "sqlite",
      "fits_window": null,
      "id": "exs_5954d859dc03",
      "last_rehearsal_at": "2026-11-12T01:39:05.620289Z",
      "last_rehearsal_id": "exr_fe7f1ce7ce0f",
      "measured_seconds": 0,
      "name": "Billing",
      "next_due_at": "2027-02-10T01:39:05.620289Z",
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
      "id": "exs_bce599c1c2b2",
      "last_rehearsal_at": "2026-11-12T01:38:59.672251Z",
      "last_rehearsal_id": "exr_14de9b944fc1",
      "measured_seconds": 1,
      "name": "Core ledger",
      "next_due_at": "2027-02-10T01:38:59.672251Z",
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
      "id": "exs_045e6ec5ca56",
      "last_rehearsal_at": "2026-11-12T01:39:01.020334Z",
      "last_rehearsal_id": "exr_037298aad7f3",
      "measured_seconds": 0,
      "name": "CRM",
      "next_due_at": "2027-02-10T01:39:01.020334Z",
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
      "id": "exs_bc05aa2ab318",
      "last_rehearsal_at": "2026-11-12T01:39:04.799281Z",
      "last_rehearsal_id": "exr_880f9f37e64c",
      "measured_seconds": 0,
      "name": "Customer master",
      "next_due_at": "2027-02-10T01:39:04.799281Z",
      "overdue": false,
      "running_rehearsal_id": null,
      "stale": false,
      "state": "proven",
      "tables": 1,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Proven, 1 table",
      "criticality": "critical",
      "engine": "sqlite",
      "fits_window": true,
      "id": "exs_f3c2784c966c",
      "last_rehearsal_at": "2026-11-12T01:39:07.476614Z",
      "last_rehearsal_id": "exr_ac482e4d03b9",
      "measured_seconds": 1,
      "name": "HR people",
      "next_due_at": "2027-02-10T01:39:07.476614Z",
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
      "id": "exs_4e2370aa438b",
      "last_rehearsal_at": "2026-11-12T01:39:05.343579Z",
      "last_rehearsal_id": "exr_d6446bcfacc2",
      "measured_seconds": 0,
      "name": "Notes archive",
      "next_due_at": "2027-02-10T01:39:05.343579Z",
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
      "id": "exs_caa265b06cc8",
      "last_rehearsal_at": "2026-11-12T01:39:00.466149Z",
      "last_rehearsal_id": "exr_b3c3bc583b51",
      "measured_seconds": 0,
      "name": "Payments",
      "next_due_at": "2027-02-10T01:39:00.466149Z",
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
      "id": "exs_18cae43b6b93",
      "last_rehearsal_at": "2026-11-12T01:39:06.204463Z",
      "last_rehearsal_id": "exr_779644fab493",
      "measured_seconds": null,
      "name": "Running one",
      "next_due_at": "2027-02-10T01:39:06.204463Z",
      "overdue": false,
      "running_rehearsal_id": "exr_779644fab493",
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
      "id": "exs_4617d2dabe6b",
      "last_rehearsal_at": "2026-11-12T01:39:01.617174Z",
      "last_rehearsal_id": "exr_5353b7ee789e",
      "measured_seconds": 3,
      "name": "Warehouse",
      "next_due_at": "2027-02-10T01:39:01.617174Z",
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
  "created_at": "2026-11-12T01:38:56.531320Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_bce599c1c2b2",
  "name": "Core ledger",
  "pii_acknowledgement": null,
  "pii_hold": null,
  "readiness": {
    "last_proven_rehearsal": {
      "current": true,
      "finished_at": "2026-11-12T01:39:00.424284Z",
      "fits_window": true,
      "id": "exr_14de9b944fc1",
      "measured_seconds": 1,
      "n": 2,
      "signed": true,
      "started_at": "2026-11-12T01:38:59.672251Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "last_rehearsal": {
      "current": true,
      "finished_at": "2026-11-12T01:39:00.424284Z",
      "fits_window": true,
      "id": "exr_14de9b944fc1",
      "measured_seconds": 1,
      "n": 2,
      "signed": true,
      "started_at": "2026-11-12T01:38:59.672251Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-10T01:38:59.672251Z",
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
  "updated_at": "2026-11-12T01:38:56.531320Z",
  "updated_by": "owner@example.com",
  "version": 5,
  "window_basis": "EU Data Act switching window",
  "workspace_id": "ws1"
};

export const realRehearsals: { rehearsals: RehearsalSummary[]; trend: Trend } = {
  "rehearsals": [
    {
      "current": null,
      "finished_at": "2026-11-12T01:39:00.424284Z",
      "fits_window": true,
      "id": "exr_14de9b944fc1",
      "measured_seconds": 1,
      "n": 2,
      "signed": true,
      "started_at": "2026-11-12T01:38:59.672251Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    {
      "current": null,
      "finished_at": "2026-11-12T01:38:59.657988Z",
      "fits_window": true,
      "id": "exr_5dc72d8d53a5",
      "measured_seconds": 1,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-12T01:38:58.286173Z",
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
        "at": "2026-11-12T01:38:58.286173Z",
        "bytes": 2818,
        "measured_seconds": 1,
        "n": 1,
        "rehearsal_id": "exr_5dc72d8d53a5",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-12T01:38:59.672251Z",
        "bytes": 2818,
        "measured_seconds": 1,
        "n": 2,
        "rehearsal_id": "exr_14de9b944fc1",
        "rows": 63,
        "verdict": "proven"
      }
    ],
    "projection": {
      "basis": "Projection, not a measurement: rehearsal 2 measured 1 s for 63 rows; if the data grows again as it did since rehearsal 1 (63 rows), the next exit takes about 1 s.",
      "kind": "projection",
      "seconds": 1
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
    "tables_needs_pii_review": 0,
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
  "current": true,
  "definition_sha256": "0f84ec8fc40910f9ef8263ac61c624e04f5ee46d163595a999e6b41707c5fa04",
  "duration": {
    "basis": "measured",
    "extract_seconds": 1,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 1,
    "statement": "Measured 1 s for 2,818 bytes (extract 1 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_14de9b944fc1/evidence",
    "signed": false
  },
  "finished_at": "2026-11-12T01:39:00.424284Z",
  "id": "exr_14de9b944fc1",
  "n": 2,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
    "parts": [
      {
        "name": "exit_exr_14de9b944fc1_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      {
        "name": "exit_exr_14de9b944fc1_0001_events.jsonl",
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
  "stale_reason": null,
  "started_at": "2026-11-12T01:38:59.672251Z",
  "system_id": "exs_bce599c1c2b2",
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
      "extract_seconds": 0.568,
      "job_id": "6af518b3b8526454d2d5a3e9",
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
        "name": "exit_exr_14de9b944fc1_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
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
        "report": "/api/v1/jobs/6af518b3b8526454d2d5a3e9/parity-report",
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
      "extract_seconds": 0.438,
      "job_id": "6af518b3b8526454d2d5a3ea",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "events",
      "part": {
        "name": "exit_exr_14de9b944fc1_0001_events.jsonl",
        "object_key": null,
        "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
        "size_bytes": 71,
        "storage": "local_file",
        "table": "events",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
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
        "report": "/api/v1/jobs/6af518b3b8526454d2d5a3ea/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:38:59.672251Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:38:59.672251Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:00.297617Z",
      "seconds": 0.551,
      "stage": "extract",
      "started_at": "2026-11-12T01:38:59.746837Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:00.424284Z",
      "seconds": 0.062,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:00.362157Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:00.424284Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:00.424284Z",
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
        "at": "2026-11-12T01:38:58.286173Z",
        "bytes": 2818,
        "measured_seconds": 1,
        "n": 1,
        "rehearsal_id": "exr_5dc72d8d53a5",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-12T01:38:59.672251Z",
        "bytes": 2818,
        "measured_seconds": 1,
        "n": 2,
        "rehearsal_id": "exr_14de9b944fc1",
        "rows": 63,
        "verdict": "proven"
      }
    ],
    "projection": {
      "basis": "Projection, not a measurement: rehearsal 2 measured 1 s for 63 rows; if the data grows again as it did since rehearsal 1 (63 rows), the next exit takes about 1 s.",
      "kind": "projection",
      "seconds": 1
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
    "tables_needs_pii_review": 0,
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
  "current": true,
  "definition_sha256": "0f84ec8fc40910f9ef8263ac61c624e04f5ee46d163595a999e6b41707c5fa04",
  "duration": {
    "basis": "measured",
    "extract_seconds": 1,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 1,
    "statement": "Measured 1 s for 2,818 bytes (extract 1 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": {
      "anchored": false,
      "anchored_at": "2026-11-12T01:39:00.448563+00:00",
      "digest": "91a6de1b6aa3f8a0f9c78fed96776f81909d07fd6f010db24ed22fdd01008bc5",
      "event_time": null,
      "honesty": "Local stub anchor \u2014 not S3 Object Lock / RFC-3161 TSA, so not write-once: not anchored. Configure DATAFLOW_AUDIT_ANCHOR_PROVIDER=s3 with an Object Lock bucket.",
      "id": "ca52734f-e912-41a1-b87c-c86663d7f717",
      "kind": "exit_evidence",
      "location": null,
      "provider": "stub",
      "reason": "the local stub is not a write-once store",
      "retention": null,
      "status": "not_anchored",
      "subject": "exit_evidence:exs_bce599c1c2b2:exr_14de9b944fc1",
      "tip_hash": "91a6de1b6aa3f8a0f9c78fed96776f81909d07fd6f010db24ed22fdd01008bc5"
    },
    "content_sha256": "91a6de1b6aa3f8a0f9c78fed96776f81909d07fd6f010db24ed22fdd01008bc5",
    "export": "/api/v1/exit/rehearsals/exr_14de9b944fc1/evidence",
    "signed": true
  },
  "finished_at": "2026-11-12T01:39:00.424284Z",
  "id": "exr_14de9b944fc1",
  "n": 2,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
    "parts": [
      {
        "name": "exit_exr_14de9b944fc1_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      {
        "name": "exit_exr_14de9b944fc1_0001_events.jsonl",
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
        "at": "2026-11-12T01:39:00.435378Z",
        "authority": "workspace admin",
        "content_sha256": "4a39e29fd6e7233403d4fcddba8e5c2a74a72d06d7902f7b2271e100b6c78648",
        "note": "",
        "role": "engineering",
        "signer_role": "admin",
        "user": "engineering@example.com",
        "version": 4
      },
      {
        "at": "2026-11-12T01:39:00.437349Z",
        "authority": "workspace admin",
        "content_sha256": "4a39e29fd6e7233403d4fcddba8e5c2a74a72d06d7902f7b2271e100b6c78648",
        "note": "",
        "role": "data_owner",
        "signer_role": "admin",
        "user": "data_owner@example.com",
        "version": 5
      },
      {
        "at": "2026-11-12T01:39:00.445332Z",
        "authority": "workspace admin",
        "content_sha256": "4a39e29fd6e7233403d4fcddba8e5c2a74a72d06d7902f7b2271e100b6c78648",
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
    "signature": "hmac-sha256:eb41b671f5c2533280a831846cc7e89fdddf7d21bd3c72ac118a90ddbb7522b4",
    "signed_at": "2026-11-12T01:39:00.445342Z",
    "status": "signed"
  },
  "stale_reason": null,
  "started_at": "2026-11-12T01:38:59.672251Z",
  "system_id": "exs_bce599c1c2b2",
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
      "extract_seconds": 0.568,
      "job_id": "6af518b3b8526454d2d5a3e9",
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
        "name": "exit_exr_14de9b944fc1_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
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
        "report": "/api/v1/jobs/6af518b3b8526454d2d5a3e9/parity-report",
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
      "extract_seconds": 0.438,
      "job_id": "6af518b3b8526454d2d5a3ea",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "events",
      "part": {
        "name": "exit_exr_14de9b944fc1_0001_events.jsonl",
        "object_key": null,
        "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
        "size_bytes": 71,
        "storage": "local_file",
        "table": "events",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
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
        "report": "/api/v1/jobs/6af518b3b8526454d2d5a3ea/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:38:59.672251Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:38:59.672251Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:00.297617Z",
      "seconds": 0.551,
      "stage": "extract",
      "started_at": "2026-11-12T01:38:59.746837Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:00.424284Z",
      "seconds": 0.062,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:00.362157Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:00.424284Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:00.424284Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:00.445342Z",
      "seconds": 0.0,
      "stage": "sign",
      "started_at": "2026-11-12T01:39:00.445342Z",
      "status": "done"
    }
  ],
  "trend": {
    "growth_pct_rows": 0.0,
    "points": [
      {
        "at": "2026-11-12T01:38:58.286173Z",
        "bytes": 2818,
        "measured_seconds": 1,
        "n": 1,
        "rehearsal_id": "exr_5dc72d8d53a5",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-12T01:38:59.672251Z",
        "bytes": 2818,
        "measured_seconds": 1,
        "n": 2,
        "rehearsal_id": "exr_14de9b944fc1",
        "rows": 63,
        "verdict": "proven"
      }
    ],
    "projection": {
      "basis": "Projection, not a measurement: rehearsal 2 measured 1 s for 63 rows; if the data grows again as it did since rehearsal 1 (63 rows), the next exit takes about 1 s.",
      "kind": "projection",
      "seconds": 1
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
    "tables_needs_pii_review": 0,
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
  "current": true,
  "definition_sha256": "0284c7c6d7683240bc0bd1e2de282732d4334ff9af14cc4bab1e96cc0756a10c",
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
    "export": "/api/v1/exit/rehearsals/exr_b3c3bc583b51/evidence",
    "signed": false
  },
  "finished_at": "2026-11-12T01:39:01.002037Z",
  "id": "exr_b3c3bc583b51",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_b3c3bc583b51_0000_orders.jsonl",
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
  "stale_reason": null,
  "started_at": "2026-11-12T01:39:00.466149Z",
  "system_id": "exs_caa265b06cc8",
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
            "max": null,
            "min": null,
            "sum": null
          },
          "masked": true,
          "match": false,
          "source": {
            "max": null,
            "min": null,
            "sum": null
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
      "extract_seconds": 0.314,
      "job_id": "6af518b4b8526454d2d5a3eb",
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
        "name": "exit_exr_b3c3bc583b51_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": false
      },
      "pii_held": [],
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
        "report": "/api/v1/jobs/6af518b4b8526454d2d5a3eb/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:39:00.466149Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:39:00.466149Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:00.753361Z",
      "seconds": 0.257,
      "stage": "extract",
      "started_at": "2026-11-12T01:39:00.495890Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:01.002037Z",
      "seconds": 0.092,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:00.909768Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:01.002037Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:01.002037Z",
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
        "at": "2026-11-12T01:39:00.466149Z",
        "bytes": 2747,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_b3c3bc583b51",
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
    "tables_needs_pii_review": 0,
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
  "current": true,
  "definition_sha256": "0284c7c6d7683240bc0bd1e2de282732d4334ff9af14cc4bab1e96cc0756a10c",
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
    "export": "/api/v1/exit/rehearsals/exr_037298aad7f3/evidence",
    "signed": false
  },
  "finished_at": "2026-11-12T01:39:01.592942Z",
  "id": "exr_037298aad7f3",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_037298aad7f3_0000_orders.jsonl",
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
  "stale_reason": null,
  "started_at": "2026-11-12T01:39:01.020334Z",
  "system_id": "exs_045e6ec5ca56",
  "system_name": "CRM",
  "tables": [
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": 0.291,
      "job_id": "6af518b5b8526454d2d5a3ec",
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
        "name": "exit_exr_037298aad7f3_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
      "reason": "Nothing differs where measured, but the source could not be re-read for a population digest (the read failed (RuntimeError): see the server log for the details. Reference eec11c7a9bd14c1f88691464eb27bde3), so this table is not proven.",
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
        "report": "/api/v1/jobs/6af518b5b8526454d2d5a3ec/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:39:01.020334Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:39:01.020334Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:01.298271Z",
      "seconds": 0.236,
      "stage": "extract",
      "started_at": "2026-11-12T01:39:01.062060Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:01.592942Z",
      "seconds": 0.104,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:01.488888Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:01.592942Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:01.592942Z",
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
        "at": "2026-11-12T01:39:01.020334Z",
        "bytes": 2747,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_037298aad7f3",
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
    "tables_needs_pii_review": 0,
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
  "current": true,
  "definition_sha256": "5d6ad9cbc3363ca1f8c67c35d79346d00262f5994b36435c3dc351ff75ec7a7f",
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
    "export": "/api/v1/exit/rehearsals/exr_5353b7ee789e/evidence",
    "signed": false
  },
  "finished_at": "2026-11-12T01:39:04.700388Z",
  "id": "exr_5353b7ee789e",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_5353b7ee789e_0000_orders.jsonl",
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
  "stale_reason": null,
  "started_at": "2026-11-12T01:39:01.617174Z",
  "system_id": "exs_4617d2dabe6b",
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
      "extract_seconds": 0.316,
      "job_id": "6af518b5b8526454d2d5a3ed",
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
        "name": "exit_exr_5353b7ee789e_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
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
        "report": "/api/v1/jobs/6af518b5b8526454d2d5a3ed/parity-report",
        "unexplained": 0
      }
    },
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": null,
      "job_id": "6af518b5b8526454d2d5a3ee",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "no_such_table",
      "part": null,
      "pii_held": [],
      "reason": "The extract job 6af518b5b8526454d2d5a3ee failed: see the server log for the details. Reference 470fbe9a762a4a6ba89d22f154b0df90.",
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
        "report": "/api/v1/jobs/6af518b5b8526454d2d5a3ee/parity-report",
        "unexplained": null
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:39:01.617174Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:39:01.617174Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:04.610510Z",
      "seconds": 2.92,
      "stage": "extract",
      "started_at": "2026-11-12T01:39:01.690800Z",
      "status": "failed"
    },
    {
      "finished_at": "2026-11-12T01:39:04.700388Z",
      "seconds": 0.066,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:04.634304Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:04.700388Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:04.700388Z",
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
        "at": "2026-11-12T01:39:01.617174Z",
        "bytes": 2747,
        "measured_seconds": 3,
        "n": 1,
        "rehearsal_id": "exr_5353b7ee789e",
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
    "tables_needs_pii_review": 0,
    "tables_not_proven": 0,
    "tables_proven": 0,
    "value_methods": {}
  },
  "current": true,
  "definition_sha256": "0284c7c6d7683240bc0bd1e2de282732d4334ff9af14cc4bab1e96cc0756a10c",
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
    "export": "/api/v1/exit/rehearsals/exr_779644fab493/evidence",
    "signed": false
  },
  "finished_at": null,
  "id": "exr_779644fab493",
  "n": 1,
  "pii_acknowledgement": null,
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
  "stale_reason": null,
  "started_at": "2026-11-12T01:39:06.204463Z",
  "system_id": "exs_18cae43b6b93",
  "system_name": "Running one",
  "tables": [
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": null,
      "job_id": "6af518bab8526454d2d5a3f2",
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
      "pii_held": [],
      "reason": "Extracting (pending).",
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
      "finished_at": "2026-11-12T01:39:06.204463Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:39:06.204463Z",
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
    "tables_needs_pii_review": 0,
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
  "current": true,
  "definition_sha256": "52ba54949f382ce50abd8c123e0923f5dec8847391374acde85713363c16128e",
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
    "export": "/api/v1/exit/rehearsals/exr_880f9f37e64c/evidence",
    "signed": false
  },
  "finished_at": "2026-11-12T01:39:05.280558Z",
  "id": "exr_880f9f37e64c",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:c678f87a3ffc0085212a42aa1aaa6395b6dae7830cb626202b75386d4300edde",
    "parts": [
      {
        "name": "exit_exr_880f9f37e64c_0000_customers.jsonl",
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
  "stale_reason": null,
  "started_at": "2026-11-12T01:39:04.799281Z",
  "system_id": "exs_bc05aa2ab318",
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
      "extract_seconds": 0.199,
      "job_id": "6af518b8b8526454d2d5a3ef",
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
        "name": "exit_exr_880f9f37e64c_0000_customers.jsonl",
        "object_key": null,
        "sha256": "2738dd048706091ad8eea58759f853d5752f4e7bf1523459defc68453f5c2800",
        "size_bytes": 821,
        "storage": "local_file",
        "table": "customers",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
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
        "report": "/api/v1/jobs/6af518b8b8526454d2d5a3ef/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:39:04.799281Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:39:04.799281Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:05.094663Z",
      "seconds": 0.186,
      "stage": "extract",
      "started_at": "2026-11-12T01:39:04.908327Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:05.280558Z",
      "seconds": 0.035,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:05.245262Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:05.280558Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:05.280558Z",
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
        "at": "2026-11-12T01:39:04.799281Z",
        "bytes": 821,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_880f9f37e64c",
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
    "tables_needs_pii_review": 0,
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
  "current": true,
  "definition_sha256": "cc31ba14b33f57a704a88d100f69539263f8ec07fd8ffed4e2397d6b9f0beb41",
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
    "export": "/api/v1/exit/rehearsals/exr_d6446bcfacc2/evidence",
    "signed": false
  },
  "finished_at": "2026-11-12T01:39:05.595750Z",
  "id": "exr_d6446bcfacc2",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:7147b7cfdae6c569a288affb7862d0f7946634a16a2c184646fc72214ec4d4af",
    "parts": [
      {
        "name": "exit_exr_d6446bcfacc2_0000_notes.jsonl",
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
  "stale_reason": null,
  "started_at": "2026-11-12T01:39:05.343579Z",
  "system_id": "exs_4e2370aa438b",
  "system_name": "Notes archive",
  "tables": [
    {
      "control_totals": [
        {
          "column": "id",
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
      "extract_seconds": 0.139,
      "job_id": "6af518b9b8526454d2d5a3f0",
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
        "name": "exit_exr_d6446bcfacc2_0000_notes.jsonl",
        "object_key": null,
        "sha256": "4762bd3dd3496d2b5c6af92b7e1d7fb429dcdb830aa854fb75a20d1d7fdff3b7",
        "size_bytes": 154,
        "storage": "local_file",
        "table": "notes",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
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
        "report": "/api/v1/jobs/6af518b9b8526454d2d5a3f0/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:39:05.343579Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:39:05.343579Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:05.500662Z",
      "seconds": 0.129,
      "stage": "extract",
      "started_at": "2026-11-12T01:39:05.371232Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:05.595750Z",
      "seconds": 0.024,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:05.571841Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:05.595750Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:05.595750Z",
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
        "at": "2026-11-12T01:39:05.343579Z",
        "bytes": 154,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_d6446bcfacc2",
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
  "created_at": "2026-11-12T01:39:05.606252Z",
  "created_by": "owner@example.com",
  "criticality": "important",
  "description": "",
  "exit_window_hours": 48,
  "extract_format": "jsonl",
  "id": "exs_5954d859dc03",
  "name": "Billing",
  "pii_acknowledgement": null,
  "pii_hold": null,
  "readiness": {
    "last_proven_rehearsal": {
      "current": false,
      "finished_at": "2026-11-12T01:39:06.172269Z",
      "fits_window": true,
      "id": "exr_fe7f1ce7ce0f",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-12T01:39:05.620289Z",
      "tables_declared": 1,
      "tables_proven": 1,
      "total_bytes": 2747,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "last_rehearsal": {
      "current": false,
      "finished_at": "2026-11-12T01:39:06.172269Z",
      "fits_window": true,
      "id": "exr_fe7f1ce7ce0f",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-12T01:39:05.620289Z",
      "tables_declared": 1,
      "tables_proven": 1,
      "total_bytes": 2747,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-10T01:39:05.620289Z",
    "overdue": false,
    "running_rehearsal_id": null,
    "stale": true,
    "stale_reason": "The system's source connection, tables, keys, window or extract format changed since rehearsal 1; rehearse it again to prove the current definition.",
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
  "updated_at": "2026-11-12T01:39:06.179164Z",
  "updated_by": "owner@example.com",
  "version": 4,
  "window_basis": "",
  "workspace_id": "ws1"
};

export const realSystemRunning: ExitSystem = {
  "created_at": "2026-11-12T01:39:06.191835Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_18cae43b6b93",
  "name": "Running one",
  "pii_acknowledgement": null,
  "pii_hold": null,
  "readiness": {
    "last_proven_rehearsal": null,
    "last_rehearsal": {
      "current": true,
      "finished_at": null,
      "fits_window": null,
      "id": "exr_779644fab493",
      "measured_seconds": null,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-12T01:39:06.204463Z",
      "tables_declared": 1,
      "tables_proven": 0,
      "total_bytes": null,
      "verdict": "running",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-10T01:39:06.204463Z",
    "overdue": false,
    "running_rehearsal_id": "exr_779644fab493",
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
  "running_rehearsal_id": "exr_779644fab493",
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
  "updated_at": "2026-11-12T01:39:06.191835Z",
  "updated_by": "owner@example.com",
  "version": 2,
  "window_basis": "",
  "workspace_id": "ws1"
};

export const realReversibilityCodes: ReversibilityReport = {
  "counts": {
    "conditional": 2,
    "enforced": 0,
    "invertible": 1,
    "lossy": 5
  },
  "data_checked": {
    "covers_population": false,
    "population_rows": null,
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "This transfer must be reversible, and it is not: events: top rows only, row cap, source filter; events.created_at; events.payload; events.code; events.span. Remove the lossy transform, widen the destination, or turn off require reversibility.",
    "status": "block"
  },
  "generated_at": "2026-11-12T01:39:08.346735Z",
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
            "covers_population": false,
            "enforcement": "at_execute",
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
            "covers_population": false,
            "enforcement": "at_execute",
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
    "conditional": 1,
    "enforced": 0,
    "invertible": 1,
    "lossy": 0
  },
  "data_checked": {
    "covers_population": false,
    "population_rows": null,
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "Reversibility is not required for this transfer; the analysis is shown for review.",
    "status": "skip"
  },
  "generated_at": "2026-11-12T01:39:08.347633Z",
  "lossy_columns": [],
  "require_reversibility": false,
  "rollback_note": "tickets needs the source kept as the fallback until sign-off.",
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
            "covers_population": false,
            "rows_checked": 2,
            "rows_not_fitting": 0,
            "status": "fits"
          },
          "invertibility": "conditional",
          "reason": "state is cast varchar(10) to enum('open','closed'); reversible only if every value is one of its labels; it fits the sample (2 observed value(s) round-trip); rows not sampled are not proven (under require reversibility every row is checked while writing).",
          "reason_code": "enum",
          "source": "state",
          "source_type": "varchar(10)",
          "target": "state",
          "target_type": "enum('open','closed')",
          "transforms": []
        }
      ],
      "name": "tickets",
      "reason": "state is cast varchar(10) to enum('open','closed'); reversible only if every value is one of its labels; it fits the sample (2 observed value(s) round-trip); rows not sampled are not proven (under require reversibility every row is checked while writing).",
      "rollback": "keep_source_as_fallback",
      "rollback_note": "tickets needs the source kept as the fallback until sign-off.",
      "verdict": "partly_reversible"
    }
  ],
  "verdict": "partly_reversible",
  "verdict_reason": "1 table(s): 1 column(s) invertible, 1 conditional and not shown to fit, 0 lossy."
};

export const realVerify: ExitVerifyResult = {
  "anchor": {
    "checked_at": "2026-11-12T01:39:00.459485+00:00",
    "digest": "91a6de1b6aa3f8a0f9c78fed96776f81909d07fd6f010db24ed22fdd01008bc5",
    "provider": "stub",
    "reason": "the local stub is not a write-once store",
    "retention": null,
    "status": "not_anchored"
  },
  "content_sha256": "91a6de1b6aa3f8a0f9c78fed96776f81909d07fd6f010db24ed22fdd01008bc5",
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
    "enforced": 0,
    "invertible": 1,
    "lossy": 3
  },
  "data_checked": {
    "covers_population": false,
    "population_rows": null,
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "This transfer must be reversible, and it is not: payments.amount; payments.email; payments.note. Remove the lossy transform, widen the destination, or turn off require reversibility.",
    "status": "block"
  },
  "generated_at": "2026-11-12T01:39:08.345283Z",
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

export const realPiiHeld: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "0 of 1 declared table(s) extracted. 0 proven complete (rows, key sets, every value and control totals). Value check: none on 1.",
    "tables_declared": 1,
    "tables_extracted": 0,
    "tables_failed": 0,
    "tables_incomplete": 0,
    "tables_needs_pii_review": 1,
    "tables_not_proven": 0,
    "tables_proven": 0,
    "value_methods": {
      "count_only": 0,
      "full_checksum": 0,
      "key_diff": 0,
      "none": 1,
      "population_digest": 0,
      "sampled": 0
    }
  },
  "current": true,
  "definition_sha256": "630d5b12a96a4854f39f1be4b74a10d414fb3312e6c14a3792b2b3044acfdad6",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": null,
    "margin_pct": null,
    "measured_seconds": 0,
    "statement": "Measured 0 s (extract 0 s, verify 0 s), but the rehearsal is not proven (needs pii review), so it is not compared with the window.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_86ab69731545/evidence",
    "signed": false
  },
  "finished_at": "2026-11-12T01:39:07.444763Z",
  "id": "exr_86ab69731545",
  "n": 1,
  "pii_acknowledgement": null,
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
    "blocked_reason": "Only a proven rehearsal can be signed; this one is needs pii review: The PII gate held 1 of 1 table(s): people. Acknowledge PII for this system, then rehearse it again.",
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
  "stale_reason": null,
  "started_at": "2026-11-12T01:39:06.741147Z",
  "system_id": "exs_f3c2784c966c",
  "system_name": "HR people",
  "tables": [
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": null,
      "job_id": "6af518bab8526454d2d5a3f3",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "people",
      "part": null,
      "pii_held": [
        {
          "categories": [
            "email"
          ],
          "column": "email"
        },
        {
          "categories": [
            "ssn"
          ],
          "column": "ssn"
        }
      ],
      "reason": "The PII gate held the extract job 6af518bab8526454d2d5a3f3: email (email); ssn (ssn) need PII review. A person acknowledges PII for this system (who and why are audited and kept in the evidence), then rehearses it again.",
      "row_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "rows_extracted": null,
      "rows_source": null,
      "schema": null,
      "status": "needs_pii_review",
      "value_parity": {
        "method": "none",
        "parity_verdict": null,
        "report": "/api/v1/jobs/6af518bab8526454d2d5a3f3/parity-report",
        "unexplained": null
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:39:06.741147Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:39:06.741147Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:07.179522Z",
      "seconds": 0.273,
      "stage": "extract",
      "started_at": "2026-11-12T01:39:06.906627Z",
      "status": "failed"
    },
    {
      "finished_at": "2026-11-12T01:39:07.444763Z",
      "seconds": 0.108,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:07.336412Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:07.444763Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:07.444763Z",
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
    "points": [
      {
        "at": "2026-11-12T01:39:06.741147Z",
        "bytes": 0,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_86ab69731545",
        "rows": null,
        "verdict": "needs_pii_review"
      }
    ],
    "projection": null
  },
  "trigger": "manual",
  "verdict": "needs_pii_review",
  "verdict_reason": "The PII gate held 1 of 1 table(s): people. Acknowledge PII for this system, then rehearse it again.",
  "version": 4,
  "workspace_id": "ws1"
};

export const realSystemPii: ExitSystem = {
  "created_at": "2026-11-12T01:39:06.724131Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_f3c2784c966c",
  "name": "HR people",
  "pii_acknowledgement": null,
  "pii_hold": {
    "n": 1,
    "rehearsal_id": "exr_86ab69731545",
    "tables": [
      {
        "columns": [
          {
            "categories": [
              "email"
            ],
            "column": "email"
          },
          {
            "categories": [
              "ssn"
            ],
            "column": "ssn"
          }
        ],
        "table": "people"
      }
    ]
  },
  "readiness": {
    "last_proven_rehearsal": null,
    "last_rehearsal": {
      "current": true,
      "finished_at": "2026-11-12T01:39:07.444763Z",
      "fits_window": null,
      "id": "exr_86ab69731545",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-12T01:39:06.741147Z",
      "tables_declared": 1,
      "tables_proven": 0,
      "total_bytes": 0,
      "verdict": "needs_pii_review",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-10T01:39:06.741147Z",
    "overdue": false,
    "running_rehearsal_id": null,
    "stale": false,
    "stale_reason": null,
    "state": "needs_pii_review"
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
      "name": "people",
      "schema": null
    }
  ],
  "updated_at": "2026-11-12T01:39:06.724131Z",
  "updated_by": "owner@example.com",
  "version": 3,
  "window_basis": "",
  "workspace_id": "ws1"
};

export const realSystemPiiAcked: ExitSystem = {
  "created_at": "2026-11-12T01:39:06.724131Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_f3c2784c966c",
  "name": "HR people",
  "pii_acknowledgement": {
    "actor": "dpo@example.com",
    "at": "2026-11-12T01:39:07.456545Z",
    "covers": [
      {
        "columns": [
          {
            "categories": [
              "email"
            ],
            "column": "email"
          },
          {
            "categories": [
              "ssn"
            ],
            "column": "ssn"
          }
        ],
        "table": "people"
      }
    ],
    "current": true,
    "reason": "DPA clause 9 allows the exit extract"
  },
  "pii_hold": {
    "n": 1,
    "rehearsal_id": "exr_86ab69731545",
    "tables": [
      {
        "columns": [
          {
            "categories": [
              "email"
            ],
            "column": "email"
          },
          {
            "categories": [
              "ssn"
            ],
            "column": "ssn"
          }
        ],
        "table": "people"
      }
    ]
  },
  "readiness": {
    "last_proven_rehearsal": null,
    "last_rehearsal": {
      "current": true,
      "finished_at": "2026-11-12T01:39:07.444763Z",
      "fits_window": null,
      "id": "exr_86ab69731545",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-12T01:39:06.741147Z",
      "tables_declared": 1,
      "tables_proven": 0,
      "total_bytes": 0,
      "verdict": "needs_pii_review",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-10T01:39:06.741147Z",
    "overdue": false,
    "running_rehearsal_id": null,
    "stale": false,
    "stale_reason": null,
    "state": "needs_pii_review"
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
      "name": "people",
      "schema": null
    }
  ],
  "updated_at": "2026-11-12T01:39:07.457116Z",
  "updated_by": "dpo@example.com",
  "version": 4,
  "window_basis": "",
  "workspace_id": "ws1"
};

export const realPiiAcked: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "1 of 1 declared table(s) extracted. 1 proven complete (rows, key sets, every value and control totals). Value check: population digest on 1.",
    "tables_declared": 1,
    "tables_extracted": 1,
    "tables_failed": 0,
    "tables_incomplete": 0,
    "tables_needs_pii_review": 0,
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
  "current": true,
  "definition_sha256": "630d5b12a96a4854f39f1be4b74a10d414fb3312e6c14a3792b2b3044acfdad6",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 1,
    "statement": "Measured 1 s for 2,350 bytes (extract 0 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_ac482e4d03b9/evidence",
    "signed": false
  },
  "finished_at": "2026-11-12T01:39:08.243181Z",
  "id": "exr_ac482e4d03b9",
  "n": 2,
  "pii_acknowledgement": {
    "actor": "dpo@example.com",
    "at": "2026-11-12T01:39:07.456545Z",
    "covers": [
      {
        "columns": [
          {
            "categories": [
              "email"
            ],
            "column": "email"
          },
          {
            "categories": [
              "ssn"
            ],
            "column": "ssn"
          }
        ],
        "table": "people"
      }
    ],
    "reason": "DPA clause 9 allows the exit extract"
  },
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:7a45c7608f48f2583ec4d3721972c8a5748a62c77b4e35be6f01363282aaa32f",
    "parts": [
      {
        "name": "exit_exr_ac482e4d03b9_0000_people.jsonl",
        "object_key": null,
        "sha256": "6a22e327228cb62aaf0d445a6b5ae996ec7592a60ee28eac5b51bcd43c147dbf",
        "size_bytes": 2350,
        "storage": "local_file",
        "table": "people",
        "upload_parts": null,
        "verified_on_read": true
      }
    ],
    "parts_count": 1,
    "storage": "local_file",
    "total_bytes": 2350
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
  "stale_reason": null,
  "started_at": "2026-11-12T01:39:07.476614Z",
  "system_id": "exs_f3c2784c966c",
  "system_name": "HR people",
  "tables": [
    {
      "control_totals": [
        {
          "column": "id",
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
      "extract_seconds": 0.389,
      "job_id": "6af518bbb8526454d2d5a3f4",
      "key_columns": [
        "id"
      ],
      "key_set": {
        "extract_sha256": "e916e73cee0f79e8c83f7aa804ec9fe30e7195876218a546d0f36291802ea561",
        "match": true,
        "source_sha256": "e916e73cee0f79e8c83f7aa804ec9fe30e7195876218a546d0f36291802ea561"
      },
      "name": "people",
      "part": {
        "name": "exit_exr_ac482e4d03b9_0000_people.jsonl",
        "object_key": null,
        "sha256": "6a22e327228cb62aaf0d445a6b5ae996ec7592a60ee28eac5b51bcd43c147dbf",
        "size_bytes": 2350,
        "storage": "local_file",
        "table": "people",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
      "reason": "Rows, key set, every value (by population digest) and control totals match over all 29 row(s); the part verified on read.",
      "row_set": {
        "extract_sha256": "ef4b84c56ebd40b0717d4979473a17543aff6095a2bebbe8e77c1e3e818a92f7",
        "match": true,
        "source_sha256": "ef4b84c56ebd40b0717d4979473a17543aff6095a2bebbe8e77c1e3e818a92f7"
      },
      "rows_extracted": 29,
      "rows_source": 29,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af518bbb8526454d2d5a3f4/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:39:07.476614Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:39:07.476614Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:07.891102Z",
      "seconds": 0.362,
      "stage": "extract",
      "started_at": "2026-11-12T01:39:07.529321Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:08.243181Z",
      "seconds": 0.139,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:08.104052Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:08.243181Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:08.243181Z",
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
        "at": "2026-11-12T01:39:06.741147Z",
        "bytes": 0,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_86ab69731545",
        "rows": null,
        "verdict": "needs_pii_review"
      },
      {
        "at": "2026-11-12T01:39:07.476614Z",
        "bytes": 2350,
        "measured_seconds": 1,
        "n": 2,
        "rehearsal_id": "exr_ac482e4d03b9",
        "rows": 29,
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

export const realReversibilityReview2: ReversibilityReport = {
  "counts": {
    "conditional": 0,
    "enforced": 2,
    "invertible": 3,
    "lossy": 1
  },
  "data_checked": {
    "covers_population": false,
    "population_rows": null,
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "This transfer must be reversible, and it is not: CONTACTS: source query. Remove the lossy transform, widen the destination, or turn off require reversibility.",
    "status": "block"
  },
  "generated_at": "2026-11-12T01:39:08.366166Z",
  "lossy_columns": [
    {
      "column": "(rows)",
      "reason": "the source is a query, so the table cannot be rebuilt from what it returns (a query can select, filter or rewrite rows and values).",
      "reason_code": "row_change",
      "table": "CONTACTS"
    }
  ],
  "require_reversibility": true,
  "rollback_note": "CONTACTS needs the source kept as the fallback until sign-off.",
  "scope": "plan",
  "subject_id": "plan-4",
  "tables": [
    {
      "columns": [
        {
          "check": null,
          "invertibility": "invertible",
          "reason": "id is cast BIGINT to NUMBER(19) without losing information.",
          "reason_code": "lossless_cast",
          "source": "id",
          "source_type": "BIGINT",
          "target": "id",
          "target_type": "NUMBER(19)",
          "transforms": []
        },
        {
          "check": {
            "covers_population": false,
            "enforcement": "at_execute",
            "rows_checked": 2,
            "rows_not_fitting": 0,
            "status": "fits"
          },
          "invertibility": "conditional",
          "reason": "name is stored in VARCHAR2(10), which counts bytes, not characters; reversible only if every value's UTF-8 text fits; checked on the sample (2 observed value(s) round-trip); enforced on every row at Execute.",
          "reason_code": "byte_length",
          "source": "name",
          "source_type": "varchar(10)",
          "target": "name",
          "target_type": "VARCHAR2(10)",
          "transforms": []
        },
        {
          "check": {
            "covers_population": false,
            "enforcement": "at_execute",
            "rows_checked": 2,
            "rows_not_fitting": 0,
            "status": "fits"
          },
          "invertibility": "conditional",
          "reason": "email is normalised (upper); reversible only if no value changes; checked on the sample (2 observed value(s) round-trip); enforced on every row at Execute.",
          "reason_code": "normalize",
          "source": "email",
          "source_type": "TEXT",
          "target": "email",
          "target_type": "CLOB",
          "transforms": [
            "upper"
          ]
        },
        {
          "check": null,
          "invertibility": "lossy",
          "reason": "the source is a query, so the table cannot be rebuilt from what it returns (a query can select, filter or rewrite rows and values).",
          "reason_code": "row_change",
          "source": null,
          "source_type": null,
          "target": null,
          "target_type": null,
          "transforms": [
            "source_query"
          ]
        }
      ],
      "name": "CONTACTS",
      "reason": "the source is a query, so the table cannot be rebuilt from what it returns (a query can select, filter or rewrite rows and values).",
      "rollback": "keep_source_as_fallback",
      "rollback_note": "CONTACTS needs the source kept as the fallback until sign-off.",
      "verdict": "partly_reversible"
    }
  ],
  "verdict": "partly_reversible",
  "verdict_reason": "1 table(s): 3 column(s) invertible (2 of them conditional and enforced on every row), 0 conditional and not shown to fit, 1 lossy."
};

export const realStale: ExitRehearsal = {
  "actor": "owner@example.com",
  "completeness": {
    "statement": "1 of 1 declared table(s) extracted. 1 proven complete (rows, key sets, every value and control totals). Value check: population digest on 1.",
    "tables_declared": 1,
    "tables_extracted": 1,
    "tables_failed": 0,
    "tables_incomplete": 0,
    "tables_needs_pii_review": 0,
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
  "current": false,
  "definition_sha256": "0284c7c6d7683240bc0bd1e2de282732d4334ff9af14cc4bab1e96cc0756a10c",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 0,
    "statement": "Measured 0 s for 2,747 bytes (extract 0 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_fe7f1ce7ce0f/evidence",
    "signed": false
  },
  "finished_at": "2026-11-12T01:39:06.172269Z",
  "id": "exr_fe7f1ce7ce0f",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_fe7f1ce7ce0f_0000_orders.jsonl",
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
  "stale_reason": "The system's source connection, tables, keys, window or extract format changed since this rehearsal; it no longer proves the system as it is defined now. Rehearse it again.",
  "started_at": "2026-11-12T01:39:05.620289Z",
  "system_id": "exs_5954d859dc03",
  "system_name": "Billing",
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
      "extract_seconds": 0.289,
      "job_id": "6af518b9b8526454d2d5a3f1",
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
        "name": "exit_exr_fe7f1ce7ce0f_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "pii_held": [],
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
        "report": "/api/v1/jobs/6af518b9b8526454d2d5a3f1/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-12T01:39:05.620289Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-12T01:39:05.620289Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:06.071027Z",
      "seconds": 0.263,
      "stage": "extract",
      "started_at": "2026-11-12T01:39:05.808339Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:06.172269Z",
      "seconds": 0.068,
      "stage": "verify",
      "started_at": "2026-11-12T01:39:06.104011Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-12T01:39:06.172269Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-12T01:39:06.172269Z",
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
        "at": "2026-11-12T01:39:05.620289Z",
        "bytes": 2747,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_fe7f1ce7ce0f",
        "rows": 60,
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

export const realReversibilityReview3: ReversibilityReport = {
  "counts": {
    "conditional": 0,
    "enforced": 1,
    "invertible": 2,
    "lossy": 0
  },
  "data_checked": {
    "covers_population": false,
    "population_rows": null,
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "Every table is reversible. 1 conditional column(s): checked on the sample; enforced on every row at Execute.",
    "status": "pass"
  },
  "generated_at": "2026-11-12T01:39:08.371991Z",
  "lossy_columns": [],
  "require_reversibility": true,
  "rollback_note": "contacts can be restored from the destination (every mapping is lossless). Rollback plan: DOCUMENT_ONLY runbook; Datawrap does not undo committed rows.",
  "scope": "plan",
  "subject_id": "plan-5",
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
          "check": {
            "covers_population": false,
            "enforcement": "at_execute",
            "rows_checked": 2,
            "rows_not_fitting": 0,
            "status": "fits"
          },
          "invertibility": "conditional",
          "reason": "email is normalised (upper); reversible only if no value changes; checked on the sample (2 observed value(s) round-trip); enforced on every row at Execute.",
          "reason_code": "normalize",
          "source": "email",
          "source_type": "TEXT",
          "target": "email",
          "target_type": "TEXT",
          "transforms": [
            "upper"
          ]
        }
      ],
      "name": "contacts",
      "reason": "Every mapping is lossless or enforced: email is checked on the sample and enforced on every row at Execute.",
      "rollback": "restore_from_destination",
      "rollback_note": "contacts can be restored from the destination (every mapping is lossless). Rollback plan: DOCUMENT_ONLY runbook; Datawrap does not undo committed rows.",
      "verdict": "reversible"
    }
  ],
  "verdict": "reversible",
  "verdict_reason": "1 table(s): 2 column(s) invertible (1 of them conditional and enforced on every row), 0 conditional and not shown to fit, 0 lossy."
};

/** The signed Exit Evidence Pack (JSON export). */
export const realPack: Record<string, unknown> = {
  "actor": "compliance@example.com",
  "content_sha256": "91a6de1b6aa3f8a0f9c78fed96776f81909d07fd6f010db24ed22fdd01008bc5",
  "content_sha256_attested": "4a39e29fd6e7233403d4fcddba8e5c2a74a72d06d7902f7b2271e100b6c78648",
  "issued_at": "2026-11-12T01:39:00.445342Z",
  "kind": "exit_evidence",
  "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
  "parts": [
    {
      "name": "exit_exr_14de9b944fc1_0000_orders.jsonl",
      "object_key": null,
      "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
      "size_bytes": 2747,
      "storage": "local_file",
      "table": "orders",
      "upload_parts": null,
      "verified_on_read": true
    },
    {
      "name": "exit_exr_14de9b944fc1_0001_events.jsonl",
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
      "tables_needs_pii_review": 0,
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
    "definition_sha256": "0f84ec8fc40910f9ef8263ac61c624e04f5ee46d163595a999e6b41707c5fa04",
    "duration": {
      "basis": "measured",
      "extract_seconds": 1,
      "fits_window": true,
      "margin_pct": 100.0,
      "measured_seconds": 1,
      "statement": "Measured 1 s for 2,818 bytes (extract 1 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
      "verify_seconds": 0,
      "window_seconds": 2592000
    },
    "finished_at": "2026-11-12T01:39:00.424284Z",
    "id": "exr_14de9b944fc1",
    "n": 2,
    "pii_acknowledgement": null,
    "seal": {
      "algorithm": "sha256-merkle-rfc6962",
      "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
      "parts": [
        {
          "name": "exit_exr_14de9b944fc1_0000_orders.jsonl",
          "object_key": null,
          "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
          "size_bytes": 2747,
          "storage": "local_file",
          "table": "orders",
          "upload_parts": null,
          "verified_on_read": true
        },
        {
          "name": "exit_exr_14de9b944fc1_0001_events.jsonl",
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
          "at": "2026-11-12T01:39:00.435378Z",
          "authority": "workspace admin",
          "content_sha256": "4a39e29fd6e7233403d4fcddba8e5c2a74a72d06d7902f7b2271e100b6c78648",
          "note": "",
          "role": "engineering",
          "signer_role": "admin",
          "user": "engineering@example.com",
          "version": 4
        },
        {
          "at": "2026-11-12T01:39:00.437349Z",
          "authority": "workspace admin",
          "content_sha256": "4a39e29fd6e7233403d4fcddba8e5c2a74a72d06d7902f7b2271e100b6c78648",
          "note": "",
          "role": "data_owner",
          "signer_role": "admin",
          "user": "data_owner@example.com",
          "version": 5
        },
        {
          "at": "2026-11-12T01:39:00.445332Z",
          "authority": "workspace admin",
          "content_sha256": "4a39e29fd6e7233403d4fcddba8e5c2a74a72d06d7902f7b2271e100b6c78648",
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
      "signed_at": "2026-11-12T01:39:00.445342Z",
      "status": "signed"
    },
    "started_at": "2026-11-12T01:38:59.672251Z",
    "system_id": "exs_bce599c1c2b2",
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
        "extract_seconds": 0.568,
        "job_id": "6af518b3b8526454d2d5a3e9",
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
          "name": "exit_exr_14de9b944fc1_0000_orders.jsonl",
          "object_key": null,
          "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
          "size_bytes": 2747,
          "storage": "local_file",
          "table": "orders",
          "upload_parts": null,
          "verified_on_read": true
        },
        "pii_held": [],
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
          "report": "/api/v1/jobs/6af518b3b8526454d2d5a3e9/parity-report",
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
        "extract_seconds": 0.438,
        "job_id": "6af518b3b8526454d2d5a3ea",
        "key_columns": [],
        "key_set": {
          "extract_sha256": null,
          "match": null,
          "source_sha256": null
        },
        "name": "events",
        "part": {
          "name": "exit_exr_14de9b944fc1_0001_events.jsonl",
          "object_key": null,
          "sha256": "ac5873f24e420d2b64b1cb7be50ee55bf0618b29edd401761febb5f6cb485a47",
          "size_bytes": 71,
          "storage": "local_file",
          "table": "events",
          "upload_parts": null,
          "verified_on_read": true
        },
        "pii_held": [],
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
          "report": "/api/v1/jobs/6af518b3b8526454d2d5a3ea/parity-report",
          "unexplained": 0
        }
      }
    ],
    "timeline": [
      {
        "finished_at": "2026-11-12T01:38:59.672251Z",
        "seconds": 0.0,
        "stage": "inventory",
        "started_at": "2026-11-12T01:38:59.672251Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-12T01:39:00.297617Z",
        "seconds": 0.551,
        "stage": "extract",
        "started_at": "2026-11-12T01:38:59.746837Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-12T01:39:00.424284Z",
        "seconds": 0.062,
        "stage": "verify",
        "started_at": "2026-11-12T01:39:00.362157Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-12T01:39:00.424284Z",
        "seconds": 0.0,
        "stage": "seal",
        "started_at": "2026-11-12T01:39:00.424284Z",
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
  "rehearsal_id": "exr_14de9b944fc1",
  "signature": {
    "alg": "HMAC-SHA256",
    "key_id": "platform_auth_secret",
    "value": "eb41b671f5c2533280a831846cc7e89fdddf7d21bd3c72ac118a90ddbb7522b4"
  },
  "system": {
    "criticality": "critical",
    "engine": "sqlite",
    "exit_window_hours": 720,
    "name": "Core ledger",
    "window_basis": "EU Data Act switching window"
  },
  "system_id": "exs_bce599c1c2b2",
  "version": "1",
  "workspace_id": "ws1"
};
