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
 */
import type { ExitReadiness, ExitRehearsal, ExitSystem, ExitVerifyResult, RehearsalSummary, Trend } from "../../lib/exitReadiness";
import type { ReversibilityReport } from "../../lib/reversibility";

export const realBoard: ExitReadiness = {
  "generated_at": "2026-11-11T22:01:33.378545Z",
  "kpis": {
    "longest_measured_seconds": 0,
    "longest_measured_system_id": "exs_4857369012ad",
    "next_regulatory_date": {
      "date": "2027-01-12",
      "label": "Data Act switching charges end",
      "system_id": "exs_4857369012ad"
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
      "id": "exs_d07c621a8dfe",
      "last_rehearsal_at": "2026-11-11T22:01:32.331419Z",
      "last_rehearsal_id": "exr_0f0738c160c0",
      "measured_seconds": 0,
      "name": "Billing",
      "next_due_at": "2027-02-09T22:01:32.331419Z",
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
      "id": "exs_4857369012ad",
      "last_rehearsal_at": "2026-11-11T22:01:28.592087Z",
      "last_rehearsal_id": "exr_5367d6650e59",
      "measured_seconds": 0,
      "name": "Core ledger",
      "next_due_at": "2027-02-09T22:01:28.592087Z",
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
      "id": "exs_8a7b27a7a231",
      "last_rehearsal_at": "2026-11-11T22:01:29.147168Z",
      "last_rehearsal_id": "exr_11729acf41cc",
      "measured_seconds": 0,
      "name": "CRM",
      "next_due_at": "2027-02-09T22:01:29.147168Z",
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
      "id": "exs_9b601d1cc01b",
      "last_rehearsal_at": "2026-11-11T22:01:31.865507Z",
      "last_rehearsal_id": "exr_a4462206189d",
      "measured_seconds": 0,
      "name": "Customer master",
      "next_due_at": "2027-02-09T22:01:31.865507Z",
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
      "id": "exs_bfaa393cbe77",
      "last_rehearsal_at": "2026-11-11T22:01:33.129529Z",
      "last_rehearsal_id": "exr_1811929347a3",
      "measured_seconds": 0,
      "name": "HR people",
      "next_due_at": "2027-02-09T22:01:33.129529Z",
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
      "id": "exs_1eee6b5eeb14",
      "last_rehearsal_at": "2026-11-11T22:01:32.106809Z",
      "last_rehearsal_id": "exr_27e65a0b33a0",
      "measured_seconds": 0,
      "name": "Notes archive",
      "next_due_at": "2027-02-09T22:01:32.106809Z",
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
      "id": "exs_8ed9f34da399",
      "last_rehearsal_at": "2026-11-11T22:01:28.885903Z",
      "last_rehearsal_id": "exr_1447baffd983",
      "measured_seconds": 0,
      "name": "Payments",
      "next_due_at": "2027-02-09T22:01:28.885903Z",
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
      "id": "exs_be7956721598",
      "last_rehearsal_at": "2026-11-11T22:01:32.600027Z",
      "last_rehearsal_id": "exr_27bfefa5d35e",
      "measured_seconds": null,
      "name": "Running one",
      "next_due_at": "2027-02-09T22:01:32.600027Z",
      "overdue": false,
      "running_rehearsal_id": "exr_27bfefa5d35e",
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
      "id": "exs_7c8c5532d507",
      "last_rehearsal_at": "2026-11-11T22:01:29.393107Z",
      "last_rehearsal_id": "exr_800ddd461348",
      "measured_seconds": 2,
      "name": "Warehouse",
      "next_due_at": "2027-02-09T22:01:29.393107Z",
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
  "created_at": "2026-11-11T22:01:27.927883Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_4857369012ad",
  "name": "Core ledger",
  "pii_acknowledgement": null,
  "readiness": {
    "last_proven_rehearsal": {
      "current": true,
      "finished_at": "2026-11-11T22:01:28.867452Z",
      "fits_window": true,
      "id": "exr_5367d6650e59",
      "measured_seconds": 0,
      "n": 2,
      "signed": true,
      "started_at": "2026-11-11T22:01:28.592087Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "last_rehearsal": {
      "current": true,
      "finished_at": "2026-11-11T22:01:28.867452Z",
      "fits_window": true,
      "id": "exr_5367d6650e59",
      "measured_seconds": 0,
      "n": 2,
      "signed": true,
      "started_at": "2026-11-11T22:01:28.592087Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-09T22:01:28.592087Z",
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
  "updated_at": "2026-11-11T22:01:27.927883Z",
  "updated_by": "owner@example.com",
  "version": 5,
  "window_basis": "EU Data Act switching window",
  "workspace_id": "ws1"
};

export const realRehearsals: { rehearsals: RehearsalSummary[]; trend: Trend } = {
  "rehearsals": [
    {
      "current": null,
      "finished_at": "2026-11-11T22:01:28.867452Z",
      "fits_window": true,
      "id": "exr_5367d6650e59",
      "measured_seconds": 0,
      "n": 2,
      "signed": true,
      "started_at": "2026-11-11T22:01:28.592087Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    {
      "current": null,
      "finished_at": "2026-11-11T22:01:28.587822Z",
      "fits_window": true,
      "id": "exr_b6c9f4376ca5",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T22:01:27.932386Z",
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
        "at": "2026-11-11T22:01:27.932386Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_b6c9f4376ca5",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-11T22:01:28.592087Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_5367d6650e59",
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
  "definition_sha256": "1b5f16e8664dd005b6e7908a2ac4143caf88a9df6e69a14ae2fbee8595ca5127",
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
    "export": "/api/v1/exit/rehearsals/exr_5367d6650e59/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T22:01:28.867452Z",
  "id": "exr_5367d6650e59",
  "n": 2,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
    "parts": [
      {
        "name": "exit_exr_5367d6650e59_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      {
        "name": "exit_exr_5367d6650e59_0001_events.jsonl",
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
  "started_at": "2026-11-11T22:01:28.592087Z",
  "system_id": "exs_4857369012ad",
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
      "extract_seconds": 0.152,
      "job_id": "6af4e5b8277708378a598847",
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
        "name": "exit_exr_5367d6650e59_0000_orders.jsonl",
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
        "report": "/api/v1/jobs/6af4e5b8277708378a598847/parity-report",
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
      "extract_seconds": 0.094,
      "job_id": "6af4e5b8277708378a598848",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "events",
      "part": {
        "name": "exit_exr_5367d6650e59_0001_events.jsonl",
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
        "report": "/api/v1/jobs/6af4e5b8277708378a598848/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T22:01:28.592087Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:28.592087Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:28.757619Z",
      "seconds": 0.142,
      "stage": "extract",
      "started_at": "2026-11-11T22:01:28.616087Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:28.867452Z",
      "seconds": 0.048,
      "stage": "verify",
      "started_at": "2026-11-11T22:01:28.819464Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:28.867452Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T22:01:28.867452Z",
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
        "at": "2026-11-11T22:01:27.932386Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_b6c9f4376ca5",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-11T22:01:28.592087Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_5367d6650e59",
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
  "definition_sha256": "1b5f16e8664dd005b6e7908a2ac4143caf88a9df6e69a14ae2fbee8595ca5127",
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
      "anchored_at": "2026-11-11T22:01:28.878973+00:00",
      "digest": "e9f4c5145575a8977f5a267f9cd2aa4cdf4febf7095c538d492bb9471a2d6510",
      "event_time": null,
      "honesty": "Local stub anchor \u2014 not S3 Object Lock / RFC-3161 TSA, so not write-once: not anchored. Configure DATAFLOW_AUDIT_ANCHOR_PROVIDER=s3 with an Object Lock bucket.",
      "id": "3a3248af-fb53-4f7f-81f4-134bda90d6f1",
      "kind": "exit_evidence",
      "location": null,
      "provider": "stub",
      "reason": "the local stub is not a write-once store",
      "retention": null,
      "status": "not_anchored",
      "subject": "exit_evidence:exs_4857369012ad:exr_5367d6650e59",
      "tip_hash": "e9f4c5145575a8977f5a267f9cd2aa4cdf4febf7095c538d492bb9471a2d6510"
    },
    "content_sha256": "e9f4c5145575a8977f5a267f9cd2aa4cdf4febf7095c538d492bb9471a2d6510",
    "export": "/api/v1/exit/rehearsals/exr_5367d6650e59/evidence",
    "signed": true
  },
  "finished_at": "2026-11-11T22:01:28.867452Z",
  "id": "exr_5367d6650e59",
  "n": 2,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
    "parts": [
      {
        "name": "exit_exr_5367d6650e59_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      {
        "name": "exit_exr_5367d6650e59_0001_events.jsonl",
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
        "at": "2026-11-11T22:01:28.871763Z",
        "authority": "workspace admin",
        "content_sha256": "c8c1f3b836b288e5dd0cde826ea3bb2c7aa0c6bdcdcaaf15dd0f6518cbd5dc9d",
        "note": "",
        "role": "engineering",
        "signer_role": "admin",
        "user": "engineering@example.com",
        "version": 4
      },
      {
        "at": "2026-11-11T22:01:28.873765Z",
        "authority": "workspace admin",
        "content_sha256": "c8c1f3b836b288e5dd0cde826ea3bb2c7aa0c6bdcdcaaf15dd0f6518cbd5dc9d",
        "note": "",
        "role": "data_owner",
        "signer_role": "admin",
        "user": "data_owner@example.com",
        "version": 5
      },
      {
        "at": "2026-11-11T22:01:28.875610Z",
        "authority": "workspace admin",
        "content_sha256": "c8c1f3b836b288e5dd0cde826ea3bb2c7aa0c6bdcdcaaf15dd0f6518cbd5dc9d",
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
    "signature": "hmac-sha256:5e4a9fb24b19b72d2af34141b0b01bf4b773b58eef594b77908649ae94407555",
    "signed_at": "2026-11-11T22:01:28.875620Z",
    "status": "signed"
  },
  "started_at": "2026-11-11T22:01:28.592087Z",
  "system_id": "exs_4857369012ad",
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
      "extract_seconds": 0.152,
      "job_id": "6af4e5b8277708378a598847",
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
        "name": "exit_exr_5367d6650e59_0000_orders.jsonl",
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
        "report": "/api/v1/jobs/6af4e5b8277708378a598847/parity-report",
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
      "extract_seconds": 0.094,
      "job_id": "6af4e5b8277708378a598848",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "events",
      "part": {
        "name": "exit_exr_5367d6650e59_0001_events.jsonl",
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
        "report": "/api/v1/jobs/6af4e5b8277708378a598848/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T22:01:28.592087Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:28.592087Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:28.757619Z",
      "seconds": 0.142,
      "stage": "extract",
      "started_at": "2026-11-11T22:01:28.616087Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:28.867452Z",
      "seconds": 0.048,
      "stage": "verify",
      "started_at": "2026-11-11T22:01:28.819464Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:28.867452Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T22:01:28.867452Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:28.875620Z",
      "seconds": 0.0,
      "stage": "sign",
      "started_at": "2026-11-11T22:01:28.875620Z",
      "status": "done"
    }
  ],
  "trend": {
    "growth_pct_rows": 0.0,
    "points": [
      {
        "at": "2026-11-11T22:01:27.932386Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_b6c9f4376ca5",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-11T22:01:28.592087Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_5367d6650e59",
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
  "definition_sha256": "5b8b59cca0b59611edcb86e130cdd7d32d0cff9c890413599b4332a68c3fe3b7",
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
    "export": "/api/v1/exit/rehearsals/exr_1447baffd983/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T22:01:29.138761Z",
  "id": "exr_1447baffd983",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_1447baffd983_0000_orders.jsonl",
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
  "started_at": "2026-11-11T22:01:28.885903Z",
  "system_id": "exs_8ed9f34da399",
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
      "extract_seconds": 0.125,
      "job_id": "6af4e5b8277708378a598849",
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
        "name": "exit_exr_1447baffd983_0000_orders.jsonl",
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
        "report": "/api/v1/jobs/6af4e5b8277708378a598849/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T22:01:28.885903Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:28.885903Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:29.022107Z",
      "seconds": 0.114,
      "stage": "extract",
      "started_at": "2026-11-11T22:01:28.908381Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:29.138761Z",
      "seconds": 0.026,
      "stage": "verify",
      "started_at": "2026-11-11T22:01:29.112300Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:29.138761Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T22:01:29.138761Z",
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
        "at": "2026-11-11T22:01:28.885903Z",
        "bytes": 2747,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_1447baffd983",
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
  "definition_sha256": "5b8b59cca0b59611edcb86e130cdd7d32d0cff9c890413599b4332a68c3fe3b7",
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
    "export": "/api/v1/exit/rehearsals/exr_11729acf41cc/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T22:01:29.386446Z",
  "id": "exr_11729acf41cc",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_11729acf41cc_0000_orders.jsonl",
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
  "started_at": "2026-11-11T22:01:29.147168Z",
  "system_id": "exs_8a7b27a7a231",
  "system_name": "CRM",
  "tables": [
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": 0.108,
      "job_id": "6af4e5b9277708378a59884a",
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
        "name": "exit_exr_11729acf41cc_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Nothing differs where measured, but the source could not be re-read for a population digest (the read failed (RuntimeError): see the server log for the details. Reference c4e5d460b284486d96fa2a6d152ada31), so this table is not proven.",
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
        "report": "/api/v1/jobs/6af4e5b9277708378a59884a/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T22:01:29.147168Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:29.147168Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:29.265995Z",
      "seconds": 0.098,
      "stage": "extract",
      "started_at": "2026-11-11T22:01:29.167759Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:29.386446Z",
      "seconds": 0.015,
      "stage": "verify",
      "started_at": "2026-11-11T22:01:29.371672Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:29.386446Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T22:01:29.386446Z",
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
        "at": "2026-11-11T22:01:29.147168Z",
        "bytes": 2747,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_11729acf41cc",
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
  "definition_sha256": "80d9cb53423af2e10ea040a088e567539548ef33e69eb77578baa5d961d20eb8",
  "duration": {
    "basis": "measured",
    "extract_seconds": 2,
    "fits_window": null,
    "margin_pct": null,
    "measured_seconds": 2,
    "statement": "Measured 2 s (extract 2 s, verify 0 s), but the rehearsal is not proven (failed), so it is not compared with the window.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_800ddd461348/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T22:01:31.855000Z",
  "id": "exr_800ddd461348",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_800ddd461348_0000_orders.jsonl",
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
  "started_at": "2026-11-11T22:01:29.393107Z",
  "system_id": "exs_7c8c5532d507",
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
      "extract_seconds": 0.135,
      "job_id": "6af4e5b9277708378a59884b",
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
        "name": "exit_exr_800ddd461348_0000_orders.jsonl",
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
        "report": "/api/v1/jobs/6af4e5b9277708378a59884b/parity-report",
        "unexplained": 0
      }
    },
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": null,
      "job_id": "6af4e5b9277708378a59884c",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "no_such_table",
      "part": null,
      "reason": "The extract job 6af4e5b9277708378a59884c failed: see the server log for the details. Reference 8cbee4fdfd954114b4d847650c434275.",
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
        "report": "/api/v1/jobs/6af4e5b9277708378a59884c/parity-report",
        "unexplained": null
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T22:01:29.393107Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:29.393107Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:31.686026Z",
      "seconds": 2.275,
      "stage": "extract",
      "started_at": "2026-11-11T22:01:29.411422Z",
      "status": "failed"
    },
    {
      "finished_at": "2026-11-11T22:01:31.855000Z",
      "seconds": 0.031,
      "stage": "verify",
      "started_at": "2026-11-11T22:01:31.824485Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:31.855000Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T22:01:31.855000Z",
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
        "at": "2026-11-11T22:01:29.393107Z",
        "bytes": 2747,
        "measured_seconds": 2,
        "n": 1,
        "rehearsal_id": "exr_800ddd461348",
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
  "definition_sha256": "5b8b59cca0b59611edcb86e130cdd7d32d0cff9c890413599b4332a68c3fe3b7",
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
    "export": "/api/v1/exit/rehearsals/exr_27bfefa5d35e/evidence",
    "signed": false
  },
  "finished_at": null,
  "id": "exr_27bfefa5d35e",
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
  "started_at": "2026-11-11T22:01:32.600027Z",
  "system_id": "exs_be7956721598",
  "system_name": "Running one",
  "tables": [
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": null,
      "job_id": "6af4e5bc277708378a598850",
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
      "finished_at": "2026-11-11T22:01:32.600027Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:32.600027Z",
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
  "definition_sha256": "ec3674709db42092bf1db4ab3d121fe84084b24655d9d28eb7a06c63492c3814",
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
    "export": "/api/v1/exit/rehearsals/exr_a4462206189d/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T22:01:32.098168Z",
  "id": "exr_a4462206189d",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:c678f87a3ffc0085212a42aa1aaa6395b6dae7830cb626202b75386d4300edde",
    "parts": [
      {
        "name": "exit_exr_a4462206189d_0000_customers.jsonl",
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
  "started_at": "2026-11-11T22:01:31.865507Z",
  "system_id": "exs_9b601d1cc01b",
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
      "extract_seconds": 0.063,
      "job_id": "6af4e5bb277708378a59884d",
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
        "name": "exit_exr_a4462206189d_0000_customers.jsonl",
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
        "report": "/api/v1/jobs/6af4e5bb277708378a59884d/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T22:01:31.865507Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:31.865507Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:31.936724Z",
      "seconds": 0.058,
      "stage": "extract",
      "started_at": "2026-11-11T22:01:31.878712Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:32.098168Z",
      "seconds": 0.016,
      "stage": "verify",
      "started_at": "2026-11-11T22:01:32.082292Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:32.098168Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T22:01:32.098168Z",
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
        "at": "2026-11-11T22:01:31.865507Z",
        "bytes": 821,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_a4462206189d",
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
  "definition_sha256": "f90f788aa48387bdd22148724e98fbd6daf550195bc46bb483f51a2905a9ae13",
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
    "export": "/api/v1/exit/rehearsals/exr_27e65a0b33a0/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T22:01:32.323847Z",
  "id": "exr_27e65a0b33a0",
  "n": 1,
  "pii_acknowledgement": null,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:7147b7cfdae6c569a288affb7862d0f7946634a16a2c184646fc72214ec4d4af",
    "parts": [
      {
        "name": "exit_exr_27e65a0b33a0_0000_notes.jsonl",
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
  "started_at": "2026-11-11T22:01:32.106809Z",
  "system_id": "exs_1eee6b5eeb14",
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
      "extract_seconds": 0.034,
      "job_id": "6af4e5bc277708378a59884e",
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
        "name": "exit_exr_27e65a0b33a0_0000_notes.jsonl",
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
        "report": "/api/v1/jobs/6af4e5bc277708378a59884e/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T22:01:32.106809Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:32.106809Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:32.146957Z",
      "seconds": 0.031,
      "stage": "extract",
      "started_at": "2026-11-11T22:01:32.116315Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:32.323847Z",
      "seconds": 0.006,
      "stage": "verify",
      "started_at": "2026-11-11T22:01:32.317739Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:32.323847Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T22:01:32.323847Z",
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
        "at": "2026-11-11T22:01:32.106809Z",
        "bytes": 154,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_27e65a0b33a0",
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
  "created_at": "2026-11-11T22:01:32.327984Z",
  "created_by": "owner@example.com",
  "criticality": "important",
  "description": "",
  "exit_window_hours": 48,
  "extract_format": "jsonl",
  "id": "exs_d07c621a8dfe",
  "name": "Billing",
  "pii_acknowledgement": null,
  "readiness": {
    "last_proven_rehearsal": {
      "current": false,
      "finished_at": "2026-11-11T22:01:32.586975Z",
      "fits_window": true,
      "id": "exr_0f0738c160c0",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T22:01:32.331419Z",
      "tables_declared": 1,
      "tables_proven": 1,
      "total_bytes": 2747,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "last_rehearsal": {
      "current": false,
      "finished_at": "2026-11-11T22:01:32.586975Z",
      "fits_window": true,
      "id": "exr_0f0738c160c0",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T22:01:32.331419Z",
      "tables_declared": 1,
      "tables_proven": 1,
      "total_bytes": 2747,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-09T22:01:32.331419Z",
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
  "updated_at": "2026-11-11T22:01:32.593134Z",
  "updated_by": "owner@example.com",
  "version": 4,
  "window_basis": "",
  "workspace_id": "ws1"
};

export const realSystemRunning: ExitSystem = {
  "created_at": "2026-11-11T22:01:32.596117Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_be7956721598",
  "name": "Running one",
  "pii_acknowledgement": null,
  "readiness": {
    "last_proven_rehearsal": null,
    "last_rehearsal": {
      "current": true,
      "finished_at": null,
      "fits_window": null,
      "id": "exr_27bfefa5d35e",
      "measured_seconds": null,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T22:01:32.600027Z",
      "tables_declared": 1,
      "tables_proven": 0,
      "total_bytes": null,
      "verdict": "running",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-09T22:01:32.600027Z",
    "overdue": false,
    "running_rehearsal_id": "exr_27bfefa5d35e",
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
  "running_rehearsal_id": "exr_27bfefa5d35e",
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
  "updated_at": "2026-11-11T22:01:32.596117Z",
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
    "population_rows": null,
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "This transfer must be reversible, and it is not: events: top rows only, row cap, source filter; events.created_at; events.payload; events.code; events.span. Remove the lossy transform, widen the destination, or turn off require reversibility.",
    "status": "block"
  },
  "generated_at": "2026-11-11T22:01:33.393297Z",
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
    "invertible": 1,
    "lossy": 0
  },
  "data_checked": {
    "population_rows": null,
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "Reversibility is not required for this transfer; the analysis is shown for review.",
    "status": "skip"
  },
  "generated_at": "2026-11-11T22:01:33.394024Z",
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
          "reason": "state is cast varchar(10) to enum('open','closed'); reversible only if every value is one of its labels; it fits the sample (2 observed value(s) round-trip), but will be blocked at Execute unless the whole table is checked (rows not sampled are not proven).",
          "reason_code": "enum",
          "source": "state",
          "source_type": "varchar(10)",
          "target": "state",
          "target_type": "enum('open','closed')",
          "transforms": []
        }
      ],
      "name": "tickets",
      "reason": "state is cast varchar(10) to enum('open','closed'); reversible only if every value is one of its labels; it fits the sample (2 observed value(s) round-trip), but will be blocked at Execute unless the whole table is checked (rows not sampled are not proven).",
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
    "checked_at": "2026-11-11T22:01:28.883325+00:00",
    "digest": "e9f4c5145575a8977f5a267f9cd2aa4cdf4febf7095c538d492bb9471a2d6510",
    "provider": "stub",
    "reason": "the local stub is not a write-once store",
    "retention": null,
    "status": "not_anchored"
  },
  "content_sha256": "e9f4c5145575a8977f5a267f9cd2aa4cdf4febf7095c538d492bb9471a2d6510",
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
    "population_rows": null,
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "This transfer must be reversible, and it is not: payments.amount; payments.email; payments.note. Remove the lossy transform, widen the destination, or turn off require reversibility.",
    "status": "block"
  },
  "generated_at": "2026-11-11T22:01:33.392109Z",
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
  "definition_sha256": "b7f0b53fade41cbba7e295efae8dc101a7ab013a039a9a0977482e9963bc90e9",
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
    "export": "/api/v1/exit/rehearsals/exr_c8248cbad528/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T22:01:33.106283Z",
  "id": "exr_c8248cbad528",
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
  "started_at": "2026-11-11T22:01:32.848455Z",
  "system_id": "exs_bfaa393cbe77",
  "system_name": "HR people",
  "tables": [
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": null,
      "job_id": "6af4e5bc277708378a598851",
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
      "reason": "The PII gate held the extract job 6af4e5bc277708378a598851: email (email); ssn (ssn) need PII review. A person acknowledges PII for this system (who and why are audited and kept in the evidence), then rehearses it again.",
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
        "report": "/api/v1/jobs/6af4e5bc277708378a598851/parity-report",
        "unexplained": null
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T22:01:32.848455Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:32.848455Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:32.964438Z",
      "seconds": 0.062,
      "stage": "extract",
      "started_at": "2026-11-11T22:01:32.902418Z",
      "status": "failed"
    },
    {
      "finished_at": "2026-11-11T22:01:33.106283Z",
      "seconds": 0.0,
      "stage": "verify",
      "started_at": "2026-11-11T22:01:33.106246Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:33.106283Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T22:01:33.106283Z",
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
        "at": "2026-11-11T22:01:32.848455Z",
        "bytes": 0,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_c8248cbad528",
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
  "created_at": "2026-11-11T22:01:32.840074Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_bfaa393cbe77",
  "name": "HR people",
  "pii_acknowledgement": null,
  "readiness": {
    "last_proven_rehearsal": null,
    "last_rehearsal": {
      "current": true,
      "finished_at": "2026-11-11T22:01:33.106283Z",
      "fits_window": null,
      "id": "exr_c8248cbad528",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T22:01:32.848455Z",
      "tables_declared": 1,
      "tables_proven": 0,
      "total_bytes": 0,
      "verdict": "needs_pii_review",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-09T22:01:32.848455Z",
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
  "updated_at": "2026-11-11T22:01:32.840074Z",
  "updated_by": "owner@example.com",
  "version": 3,
  "window_basis": "",
  "workspace_id": "ws1"
};

export const realSystemPiiAcked: ExitSystem = {
  "created_at": "2026-11-11T22:01:32.840074Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_bfaa393cbe77",
  "name": "HR people",
  "pii_acknowledgement": {
    "actor": "dpo@example.com",
    "at": "2026-11-11T22:01:33.116938Z",
    "current": true,
    "reason": "DPA clause 9 allows the exit extract"
  },
  "readiness": {
    "last_proven_rehearsal": null,
    "last_rehearsal": {
      "current": true,
      "finished_at": "2026-11-11T22:01:33.106283Z",
      "fits_window": null,
      "id": "exr_c8248cbad528",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T22:01:32.848455Z",
      "tables_declared": 1,
      "tables_proven": 0,
      "total_bytes": 0,
      "verdict": "needs_pii_review",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-09T22:01:32.848455Z",
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
  "updated_at": "2026-11-11T22:01:33.122604Z",
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
  "definition_sha256": "b7f0b53fade41cbba7e295efae8dc101a7ab013a039a9a0977482e9963bc90e9",
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 0,
    "statement": "Measured 0 s for 2,350 bytes (extract 0 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_1811929347a3/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T22:01:33.369454Z",
  "id": "exr_1811929347a3",
  "n": 2,
  "pii_acknowledgement": {
    "actor": "dpo@example.com",
    "at": "2026-11-11T22:01:33.116938Z",
    "reason": "DPA clause 9 allows the exit extract"
  },
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:7a45c7608f48f2583ec4d3721972c8a5748a62c77b4e35be6f01363282aaa32f",
    "parts": [
      {
        "name": "exit_exr_1811929347a3_0000_people.jsonl",
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
  "started_at": "2026-11-11T22:01:33.129529Z",
  "system_id": "exs_bfaa393cbe77",
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
      "extract_seconds": 0.1,
      "job_id": "6af4e5bd277708378a598852",
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
        "name": "exit_exr_1811929347a3_0000_people.jsonl",
        "object_key": null,
        "sha256": "6a22e327228cb62aaf0d445a6b5ae996ec7592a60ee28eac5b51bcd43c147dbf",
        "size_bytes": 2350,
        "storage": "local_file",
        "table": "people",
        "upload_parts": null,
        "verified_on_read": true
      },
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
        "report": "/api/v1/jobs/6af4e5bd277708378a598852/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T22:01:33.129529Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T22:01:33.129529Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:33.237125Z",
      "seconds": 0.089,
      "stage": "extract",
      "started_at": "2026-11-11T22:01:33.147842Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:33.369454Z",
      "seconds": 0.017,
      "stage": "verify",
      "started_at": "2026-11-11T22:01:33.351968Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T22:01:33.369454Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T22:01:33.369454Z",
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
        "at": "2026-11-11T22:01:32.848455Z",
        "bytes": 0,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_c8248cbad528",
        "rows": null,
        "verdict": "needs_pii_review"
      },
      {
        "at": "2026-11-11T22:01:33.129529Z",
        "bytes": 2350,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_1811929347a3",
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
    "conditional": 2,
    "invertible": 1,
    "lossy": 1
  },
  "data_checked": {
    "population_rows": 3000,
    "rows": 2,
    "source": "plan_sample"
  },
  "gate": {
    "id": "g24_reversibility",
    "message": "This transfer must be reversible, and it is not: CONTACTS: source query; CONTACTS.name (fits the sample only; the whole table was not checked); CONTACTS.email (fits the sample only; the whole table was not checked). Remove the lossy transform, widen the destination, or turn off require reversibility.",
    "status": "block"
  },
  "generated_at": "2026-11-11T22:01:33.396713Z",
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
            "rows_checked": 2,
            "rows_not_fitting": 0,
            "status": "fits"
          },
          "invertibility": "conditional",
          "reason": "name is stored in VARCHAR2(10), which counts bytes, not characters; reversible only if every value's UTF-8 text fits; it fits the sample (2 observed value(s) round-trip), but will be blocked at Execute unless the whole table is checked (rows not sampled are not proven).",
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
            "rows_checked": 2,
            "rows_not_fitting": 0,
            "status": "fits"
          },
          "invertibility": "conditional",
          "reason": "email is normalised (upper); reversible only if no value changes; it fits the sample (2 observed value(s) round-trip), but will be blocked at Execute unless the whole table is checked (rows not sampled are not proven).",
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
      "reason": "the source is a query, so the table cannot be rebuilt from what it returns (a query can select, filter or rewrite rows and values); name is stored in VARCHAR2(10), which counts bytes, not characters; reversible only if every value's UTF-8 text fits; it fits the sample (2 observed value(s) round-trip), but will be blocked at Execute unless the whole table is checked (rows not sampled are not proven); email is normalised (upper); reversible only if no value changes; it fits the sample (2 observed value(s) round-trip), but will be blocked at Execute unless the whole table is checked (rows not sampled are not proven).",
      "rollback": "keep_source_as_fallback",
      "rollback_note": "CONTACTS needs the source kept as the fallback until sign-off.",
      "verdict": "partly_reversible"
    }
  ],
  "verdict": "partly_reversible",
  "verdict_reason": "1 table(s): 1 column(s) invertible, 2 conditional and not shown to fit, 1 lossy."
};

/** The signed Exit Evidence Pack (JSON export). */
export const realPack: Record<string, unknown> = {
  "actor": "compliance@example.com",
  "content_sha256": "e9f4c5145575a8977f5a267f9cd2aa4cdf4febf7095c538d492bb9471a2d6510",
  "content_sha256_attested": "c8c1f3b836b288e5dd0cde826ea3bb2c7aa0c6bdcdcaaf15dd0f6518cbd5dc9d",
  "issued_at": "2026-11-11T22:01:28.875620Z",
  "kind": "exit_evidence",
  "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
  "parts": [
    {
      "name": "exit_exr_5367d6650e59_0000_orders.jsonl",
      "object_key": null,
      "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
      "size_bytes": 2747,
      "storage": "local_file",
      "table": "orders",
      "upload_parts": null,
      "verified_on_read": true
    },
    {
      "name": "exit_exr_5367d6650e59_0001_events.jsonl",
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
    "definition_sha256": "1b5f16e8664dd005b6e7908a2ac4143caf88a9df6e69a14ae2fbee8595ca5127",
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
    "finished_at": "2026-11-11T22:01:28.867452Z",
    "id": "exr_5367d6650e59",
    "n": 2,
    "pii_acknowledgement": null,
    "seal": {
      "algorithm": "sha256-merkle-rfc6962",
      "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
      "parts": [
        {
          "name": "exit_exr_5367d6650e59_0000_orders.jsonl",
          "object_key": null,
          "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
          "size_bytes": 2747,
          "storage": "local_file",
          "table": "orders",
          "upload_parts": null,
          "verified_on_read": true
        },
        {
          "name": "exit_exr_5367d6650e59_0001_events.jsonl",
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
          "at": "2026-11-11T22:01:28.871763Z",
          "authority": "workspace admin",
          "content_sha256": "c8c1f3b836b288e5dd0cde826ea3bb2c7aa0c6bdcdcaaf15dd0f6518cbd5dc9d",
          "note": "",
          "role": "engineering",
          "signer_role": "admin",
          "user": "engineering@example.com",
          "version": 4
        },
        {
          "at": "2026-11-11T22:01:28.873765Z",
          "authority": "workspace admin",
          "content_sha256": "c8c1f3b836b288e5dd0cde826ea3bb2c7aa0c6bdcdcaaf15dd0f6518cbd5dc9d",
          "note": "",
          "role": "data_owner",
          "signer_role": "admin",
          "user": "data_owner@example.com",
          "version": 5
        },
        {
          "at": "2026-11-11T22:01:28.875610Z",
          "authority": "workspace admin",
          "content_sha256": "c8c1f3b836b288e5dd0cde826ea3bb2c7aa0c6bdcdcaaf15dd0f6518cbd5dc9d",
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
      "signed_at": "2026-11-11T22:01:28.875620Z",
      "status": "signed"
    },
    "started_at": "2026-11-11T22:01:28.592087Z",
    "system_id": "exs_4857369012ad",
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
        "extract_seconds": 0.152,
        "job_id": "6af4e5b8277708378a598847",
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
          "name": "exit_exr_5367d6650e59_0000_orders.jsonl",
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
          "report": "/api/v1/jobs/6af4e5b8277708378a598847/parity-report",
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
        "extract_seconds": 0.094,
        "job_id": "6af4e5b8277708378a598848",
        "key_columns": [],
        "key_set": {
          "extract_sha256": null,
          "match": null,
          "source_sha256": null
        },
        "name": "events",
        "part": {
          "name": "exit_exr_5367d6650e59_0001_events.jsonl",
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
          "report": "/api/v1/jobs/6af4e5b8277708378a598848/parity-report",
          "unexplained": 0
        }
      }
    ],
    "timeline": [
      {
        "finished_at": "2026-11-11T22:01:28.592087Z",
        "seconds": 0.0,
        "stage": "inventory",
        "started_at": "2026-11-11T22:01:28.592087Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-11T22:01:28.757619Z",
        "seconds": 0.142,
        "stage": "extract",
        "started_at": "2026-11-11T22:01:28.616087Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-11T22:01:28.867452Z",
        "seconds": 0.048,
        "stage": "verify",
        "started_at": "2026-11-11T22:01:28.819464Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-11T22:01:28.867452Z",
        "seconds": 0.0,
        "stage": "seal",
        "started_at": "2026-11-11T22:01:28.867452Z",
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
  "rehearsal_id": "exr_5367d6650e59",
  "signature": {
    "alg": "HMAC-SHA256",
    "key_id": "platform_auth_secret",
    "value": "5e4a9fb24b19b72d2af34141b0b01bf4b773b58eef594b77908649ae94407555"
  },
  "system": {
    "criticality": "critical",
    "engine": "sqlite",
    "exit_window_hours": 720,
    "name": "Core ledger",
    "window_basis": "EU Data Act switching window"
  },
  "system_id": "exs_4857369012ad",
  "version": "1",
  "workspace_id": "ws1"
};
