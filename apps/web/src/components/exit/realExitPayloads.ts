/**
 * Real Reversibility Proof API payloads from an engine run (SQLite tables -> file
 * exports, in process): the board, a system, its rehearsals, proven / signed /
 * incomplete / not proven / failed / running rehearsals, a pack verification and
 * a plan's reversibility report. Captured from services.exit_* and
 * services.reversibility_sources through the API test harness; the API has no
 * web-fixture writer for this feature. Typed with the contract so tsc checks every
 * field. Not imported by app code.
 *
 * The running rehearsal is kept untyped: the API sends its tables without the
 * measured fields, which normalizeRehearsal fills (see lib/exitReadiness.ts).
 */
import type { ExitReadiness, ExitRehearsal, ExitSystem, ExitVerifyResult, RehearsalSummary, Trend } from "../../lib/exitReadiness";
import type { ReversibilityReport } from "../../lib/reversibility";

export const realBoard: ExitReadiness = {
  "generated_at": "2026-11-11T19:31:50.344889Z",
  "kpis": {
    "longest_measured_seconds": 3,
    "longest_measured_system_id": "exs_e9a7a544b738",
    "next_regulatory_date": {
      "date": "2027-01-12",
      "label": "Data Act switching charges end",
      "system_id": "exs_3c67e3fff094"
    },
    "overdue": 0,
    "systems": 5,
    "systems_proven": 1
  },
  "systems": [
    {
      "completeness_label": "Proven, 2 tables",
      "criticality": "critical",
      "engine": "sqlite",
      "fits_window": true,
      "id": "exs_3c67e3fff094",
      "last_rehearsal_at": "2026-11-11T19:31:46.536908Z",
      "last_rehearsal_id": "exr_90022030a84e",
      "measured_seconds": 0,
      "name": "Core ledger",
      "next_due_at": "2027-02-09T19:31:46.536908Z",
      "overdue": false,
      "state": "proven",
      "tables": 2,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Not proven",
      "criticality": "standard",
      "engine": "sqlite",
      "fits_window": true,
      "id": "exs_1afa319d4674",
      "last_rehearsal_at": "2026-11-11T19:31:47.035485Z",
      "last_rehearsal_id": "exr_219628279dba",
      "measured_seconds": 0,
      "name": "CRM",
      "next_due_at": "2027-02-09T19:31:47.035485Z",
      "overdue": false,
      "state": "not_proven",
      "tables": 1,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Incomplete",
      "criticality": "important",
      "engine": "sqlite",
      "fits_window": true,
      "id": "exs_2f3bca0bb85f",
      "last_rehearsal_at": "2026-11-11T19:31:46.798074Z",
      "last_rehearsal_id": "exr_2188ef8f37ff",
      "measured_seconds": 0,
      "name": "Payments",
      "next_due_at": "2027-02-09T19:31:46.798074Z",
      "overdue": false,
      "state": "incomplete",
      "tables": 1,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Rehearsal running",
      "criticality": "critical",
      "engine": "sqlite",
      "fits_window": null,
      "id": "exs_fbb58d0b8b07",
      "last_rehearsal_at": "2026-11-11T19:31:50.120721Z",
      "last_rehearsal_id": "exr_395736bc4a21",
      "measured_seconds": null,
      "name": "Running one",
      "next_due_at": "2027-02-09T19:31:50.120721Z",
      "overdue": false,
      "state": "running",
      "tables": 1,
      "window_seconds": 2592000
    },
    {
      "completeness_label": "Rehearsal failed",
      "criticality": "critical",
      "engine": "sqlite",
      "fits_window": true,
      "id": "exs_e9a7a544b738",
      "last_rehearsal_at": "2026-11-11T19:31:47.271260Z",
      "last_rehearsal_id": "exr_75b5ddcd5523",
      "measured_seconds": 3,
      "name": "Warehouse",
      "next_due_at": "2027-02-09T19:31:47.271260Z",
      "overdue": false,
      "state": "failed",
      "tables": 2,
      "window_seconds": 2592000
    }
  ]
};

export const realSystem: ExitSystem = {
  "created_at": "2026-11-11T19:31:42.633057Z",
  "created_by": "owner@example.com",
  "criticality": "critical",
  "description": "",
  "exit_window_hours": 720,
  "extract_format": "jsonl",
  "id": "exs_3c67e3fff094",
  "name": "Core ledger",
  "readiness": {
    "last_proven_rehearsal": {
      "finished_at": "2026-11-11T19:31:46.776321Z",
      "fits_window": true,
      "id": "exr_90022030a84e",
      "measured_seconds": 0,
      "n": 2,
      "signed": false,
      "started_at": "2026-11-11T19:31:46.536908Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "last_rehearsal": {
      "finished_at": "2026-11-11T19:31:46.776321Z",
      "fits_window": true,
      "id": "exr_90022030a84e",
      "measured_seconds": 0,
      "n": 2,
      "signed": false,
      "started_at": "2026-11-11T19:31:46.536908Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    "next_due_at": "2027-02-09T19:31:46.536908Z",
    "overdue": false,
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
  "updated_at": "2026-11-11T19:31:42.633057Z",
  "updated_by": "owner@example.com",
  "version": 5,
  "window_basis": "EU Data Act switching window",
  "workspace_id": "ws1"
};

export const realRehearsals: { rehearsals: RehearsalSummary[]; trend: Trend } = {
  "rehearsals": [
    {
      "finished_at": "2026-11-11T19:31:46.776321Z",
      "fits_window": true,
      "id": "exr_90022030a84e",
      "measured_seconds": 0,
      "n": 2,
      "signed": false,
      "started_at": "2026-11-11T19:31:46.536908Z",
      "tables_declared": 2,
      "tables_proven": 2,
      "total_bytes": 2818,
      "verdict": "proven",
      "window_seconds": 2592000
    },
    {
      "finished_at": "2026-11-11T19:31:46.532300Z",
      "fits_window": true,
      "id": "exr_6e93424528d1",
      "measured_seconds": 0,
      "n": 1,
      "signed": false,
      "started_at": "2026-11-11T19:31:45.092535Z",
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
        "at": "2026-11-11T19:31:45.092535Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_6e93424528d1",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-11T19:31:46.536908Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_90022030a84e",
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
    "export": "/api/v1/exit/rehearsals/exr_90022030a84e/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T19:31:46.776321Z",
  "id": "exr_90022030a84e",
  "n": 2,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
    "parts": [
      {
        "name": "exit_exr_90022030a84e_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      {
        "name": "exit_exr_90022030a84e_0001_events.jsonl",
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
  "started_at": "2026-11-11T19:31:46.536908Z",
  "system_id": "exs_3c67e3fff094",
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
          "match": true,
          "source": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.163,
      "job_id": "6af4c2a2f82ae80fc99c92ee",
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
        "name": "exit_exr_90022030a84e_0000_orders.jsonl",
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
        "extract_sha256": "dc7c91eaa0a841541002fd2524244f38a07ef02a7d9777ce08dae151a518d9a2",
        "match": true,
        "source_sha256": "dc7c91eaa0a841541002fd2524244f38a07ef02a7d9777ce08dae151a518d9a2"
      },
      "rows_extracted": 60,
      "rows_source": 60,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4c2a2f82ae80fc99c92ee/parity-report",
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
          "match": true,
          "source": {
            "max": "2",
            "min": "1",
            "sum": "4"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.079,
      "job_id": "6af4c2a2f82ae80fc99c92ef",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "events",
      "part": {
        "name": "exit_exr_90022030a84e_0001_events.jsonl",
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
        "extract_sha256": "4f6ed7c1ef95af02d8de1fd48ffab7055f457767fc5640bbad39fe1c4132194f",
        "match": true,
        "source_sha256": "4f6ed7c1ef95af02d8de1fd48ffab7055f457767fc5640bbad39fe1c4132194f"
      },
      "rows_extracted": 3,
      "rows_source": 3,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4c2a2f82ae80fc99c92ef/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T19:31:46.536908Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T19:31:46.536908Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:46.715322Z",
      "seconds": 0.152,
      "stage": "extract",
      "started_at": "2026-11-11T19:31:46.562851Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:46.776321Z",
      "seconds": 0.008,
      "stage": "verify",
      "started_at": "2026-11-11T19:31:46.768655Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:46.776321Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T19:31:46.776321Z",
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
        "at": "2026-11-11T19:31:45.092535Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_6e93424528d1",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-11T19:31:46.536908Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_90022030a84e",
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
      "anchored_at": "2026-11-11T19:31:46.789428+00:00",
      "digest": "0ddd2fdd00cc11950f8ce2b90f57a1abb9bf35120c72bca93a2a62cb53e98892",
      "event_time": null,
      "honesty": "Local stub anchor \u2014 not S3 Object Lock / RFC-3161 TSA, so not write-once: not anchored. Configure DATAFLOW_AUDIT_ANCHOR_PROVIDER=s3 with an Object Lock bucket.",
      "id": "10aea8c0-4fb3-43bf-95bc-36e4b9233d3c",
      "kind": "exit_evidence",
      "location": null,
      "provider": "stub",
      "reason": "the local stub is not a write-once store",
      "retention": null,
      "status": "not_anchored",
      "subject": "exit_evidence:exs_3c67e3fff094:exr_90022030a84e",
      "tip_hash": "0ddd2fdd00cc11950f8ce2b90f57a1abb9bf35120c72bca93a2a62cb53e98892"
    },
    "content_sha256": "0ddd2fdd00cc11950f8ce2b90f57a1abb9bf35120c72bca93a2a62cb53e98892",
    "export": "/api/v1/exit/rehearsals/exr_90022030a84e/evidence",
    "signed": true
  },
  "finished_at": "2026-11-11T19:31:46.776321Z",
  "id": "exr_90022030a84e",
  "n": 2,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
    "parts": [
      {
        "name": "exit_exr_90022030a84e_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      {
        "name": "exit_exr_90022030a84e_0001_events.jsonl",
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
        "at": "2026-11-11T19:31:46.781484Z",
        "authority": "workspace admin",
        "content_sha256": "c72054957cdb2b50748df0b4ac849ac9b985aa7dab623bf174be996573df00e4",
        "note": "",
        "role": "engineering",
        "signer_role": "admin",
        "user": "engineering@example.com",
        "version": 4
      },
      {
        "at": "2026-11-11T19:31:46.783860Z",
        "authority": "workspace admin",
        "content_sha256": "c72054957cdb2b50748df0b4ac849ac9b985aa7dab623bf174be996573df00e4",
        "note": "",
        "role": "data_owner",
        "signer_role": "admin",
        "user": "data_owner@example.com",
        "version": 5
      },
      {
        "at": "2026-11-11T19:31:46.786240Z",
        "authority": "workspace admin",
        "content_sha256": "c72054957cdb2b50748df0b4ac849ac9b985aa7dab623bf174be996573df00e4",
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
    "signature": "hmac-sha256:20c9295dcd38315d979f8d1b74041304ec8c0020b1830d4f98d95e14022cd4b7",
    "signed_at": "2026-11-11T19:31:46.786250Z",
    "status": "signed"
  },
  "started_at": "2026-11-11T19:31:46.536908Z",
  "system_id": "exs_3c67e3fff094",
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
          "match": true,
          "source": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.163,
      "job_id": "6af4c2a2f82ae80fc99c92ee",
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
        "name": "exit_exr_90022030a84e_0000_orders.jsonl",
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
        "extract_sha256": "dc7c91eaa0a841541002fd2524244f38a07ef02a7d9777ce08dae151a518d9a2",
        "match": true,
        "source_sha256": "dc7c91eaa0a841541002fd2524244f38a07ef02a7d9777ce08dae151a518d9a2"
      },
      "rows_extracted": 60,
      "rows_source": 60,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4c2a2f82ae80fc99c92ee/parity-report",
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
          "match": true,
          "source": {
            "max": "2",
            "min": "1",
            "sum": "4"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.079,
      "job_id": "6af4c2a2f82ae80fc99c92ef",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "events",
      "part": {
        "name": "exit_exr_90022030a84e_0001_events.jsonl",
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
        "extract_sha256": "4f6ed7c1ef95af02d8de1fd48ffab7055f457767fc5640bbad39fe1c4132194f",
        "match": true,
        "source_sha256": "4f6ed7c1ef95af02d8de1fd48ffab7055f457767fc5640bbad39fe1c4132194f"
      },
      "rows_extracted": 3,
      "rows_source": 3,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4c2a2f82ae80fc99c92ef/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T19:31:46.536908Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T19:31:46.536908Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:46.715322Z",
      "seconds": 0.152,
      "stage": "extract",
      "started_at": "2026-11-11T19:31:46.562851Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:46.776321Z",
      "seconds": 0.008,
      "stage": "verify",
      "started_at": "2026-11-11T19:31:46.768655Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:46.776321Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T19:31:46.776321Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:46.786250Z",
      "seconds": 0.0,
      "stage": "sign",
      "started_at": "2026-11-11T19:31:46.786250Z",
      "status": "done"
    }
  ],
  "trend": {
    "growth_pct_rows": 0.0,
    "points": [
      {
        "at": "2026-11-11T19:31:45.092535Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_6e93424528d1",
        "rows": 63,
        "verdict": "proven"
      },
      {
        "at": "2026-11-11T19:31:46.536908Z",
        "bytes": 2818,
        "measured_seconds": 0,
        "n": 2,
        "rehearsal_id": "exr_90022030a84e",
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
  "duration": {
    "basis": "measured",
    "extract_seconds": 0,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 0,
    "statement": "Measured 0 s for 2,703 bytes (extract 0 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_2188ef8f37ff/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T19:31:47.026668Z",
  "id": "exr_2188ef8f37ff",
  "n": 1,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:128be5546c74340cffa0cca29b4b3bd1336a73f64c8d537338239eee85e1cf34",
    "parts": [
      {
        "name": "exit_exr_2188ef8f37ff_0000_orders.jsonl",
        "object_key": null,
        "sha256": "56db791bca92ae8e57a59c9afa07024bbed03638f751bee342e6498bf35166ad",
        "size_bytes": 2703,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      }
    ],
    "parts_count": 1,
    "storage": "local_file",
    "total_bytes": 2703
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
  "started_at": "2026-11-11T19:31:46.798074Z",
  "system_id": "exs_2f3bca0bb85f",
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
      "extract_seconds": 0.111,
      "job_id": "6af4c2a2f82ae80fc99c92f0",
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
        "name": "exit_exr_2188ef8f37ff_0000_orders.jsonl",
        "object_key": null,
        "sha256": "56db791bca92ae8e57a59c9afa07024bbed03638f751bee342e6498bf35166ad",
        "size_bytes": 2703,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "The extract holds 1 row(s) fewer than the source.; Control totals of amount differ.; Control totals of id differ.",
      "row_set": {
        "extract_sha256": "9d843979709fa7226484037f6c31b71723515a3fc429c25edd71ff3f0005f120",
        "match": false,
        "source_sha256": "dc7c91eaa0a841541002fd2524244f38a07ef02a7d9777ce08dae151a518d9a2"
      },
      "rows_extracted": 59,
      "rows_source": 60,
      "schema": null,
      "status": "incomplete",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4c2a2f82ae80fc99c92f0/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T19:31:46.798074Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T19:31:46.798074Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:46.917090Z",
      "seconds": 0.101,
      "stage": "extract",
      "started_at": "2026-11-11T19:31:46.816079Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:47.026668Z",
      "seconds": 0.007,
      "stage": "verify",
      "started_at": "2026-11-11T19:31:47.019392Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:47.026668Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T19:31:47.026668Z",
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
        "at": "2026-11-11T19:31:46.798074Z",
        "bytes": 2703,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_2188ef8f37ff",
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
    "export": "/api/v1/exit/rehearsals/exr_219628279dba/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T19:31:47.262997Z",
  "id": "exr_219628279dba",
  "n": 1,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_219628279dba_0000_orders.jsonl",
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
  "started_at": "2026-11-11T19:31:47.035485Z",
  "system_id": "exs_1afa319d4674",
  "system_name": "CRM",
  "tables": [
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": 0.124,
      "job_id": "6af4c2a3f82ae80fc99c92f1",
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
        "name": "exit_exr_219628279dba_0000_orders.jsonl",
        "object_key": null,
        "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
        "size_bytes": 2747,
        "storage": "local_file",
        "table": "orders",
        "upload_parts": null,
        "verified_on_read": true
      },
      "reason": "Nothing differs where measured, but the source could not be re-read for a population digest (the read failed (RuntimeError): see the server log for the details.), so this table is not proven.",
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
        "report": "/api/v1/jobs/6af4c2a3f82ae80fc99c92f1/parity-report",
        "unexplained": 0
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T19:31:47.035485Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T19:31:47.035485Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:47.170315Z",
      "seconds": 0.113,
      "stage": "extract",
      "started_at": "2026-11-11T19:31:47.057779Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:47.262997Z",
      "seconds": 0.002,
      "stage": "verify",
      "started_at": "2026-11-11T19:31:47.261309Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:47.262997Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T19:31:47.262997Z",
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
        "at": "2026-11-11T19:31:47.035485Z",
        "bytes": 2747,
        "measured_seconds": 0,
        "n": 1,
        "rehearsal_id": "exr_219628279dba",
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
  "duration": {
    "basis": "measured",
    "extract_seconds": 3,
    "fits_window": true,
    "margin_pct": 100.0,
    "measured_seconds": 3,
    "statement": "Measured 3 s for 2,747 bytes (extract 3 s, verify 0 s) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    "verify_seconds": 0,
    "window_seconds": 2592000
  },
  "evidence": {
    "anchor": null,
    "content_sha256": null,
    "export": "/api/v1/exit/rehearsals/exr_75b5ddcd5523/evidence",
    "signed": false
  },
  "finished_at": "2026-11-11T19:31:50.112746Z",
  "id": "exr_75b5ddcd5523",
  "n": 1,
  "seal": {
    "algorithm": "sha256-merkle-rfc6962",
    "merkle_root": "sha256:36c61d644152b69cd0d7138485893e63920113f5aee9e5c52b42e40641efbba1",
    "parts": [
      {
        "name": "exit_exr_75b5ddcd5523_0000_orders.jsonl",
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
  "started_at": "2026-11-11T19:31:47.271260Z",
  "system_id": "exs_e9a7a544b738",
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
          "match": true,
          "source": {
            "max": "60",
            "min": "1",
            "sum": "1830"
          }
        }
      ],
      "differences": [],
      "extract_seconds": 0.132,
      "job_id": "6af4c2a3f82ae80fc99c92f2",
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
        "name": "exit_exr_75b5ddcd5523_0000_orders.jsonl",
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
        "extract_sha256": "dc7c91eaa0a841541002fd2524244f38a07ef02a7d9777ce08dae151a518d9a2",
        "match": true,
        "source_sha256": "dc7c91eaa0a841541002fd2524244f38a07ef02a7d9777ce08dae151a518d9a2"
      },
      "rows_extracted": 60,
      "rows_source": 60,
      "schema": null,
      "status": "proven",
      "value_parity": {
        "method": "population_digest",
        "parity_verdict": "unverifiable",
        "report": "/api/v1/jobs/6af4c2a3f82ae80fc99c92f2/parity-report",
        "unexplained": 0
      }
    },
    {
      "control_totals": [],
      "differences": [],
      "extract_seconds": null,
      "job_id": "6af4c2a3f82ae80fc99c92f3",
      "key_columns": [],
      "key_set": {
        "extract_sha256": null,
        "match": null,
        "source_sha256": null
      },
      "name": "no_such_table",
      "part": null,
      "reason": "The extract job ended failed: Table was not found. Driver reported: no such table: no_such_table. This does not say the destination was missing, and it is not a capacity problem.",
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
        "report": "/api/v1/jobs/6af4c2a3f82ae80fc99c92f3/parity-report",
        "unexplained": null
      }
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T19:31:47.271260Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T19:31:47.271260Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:50.080945Z",
      "seconds": 2.786,
      "stage": "extract",
      "started_at": "2026-11-11T19:31:47.294918Z",
      "status": "failed"
    },
    {
      "finished_at": "2026-11-11T19:31:50.112746Z",
      "seconds": 0.007,
      "stage": "verify",
      "started_at": "2026-11-11T19:31:50.105844Z",
      "status": "done"
    },
    {
      "finished_at": "2026-11-11T19:31:50.112746Z",
      "seconds": 0.0,
      "stage": "seal",
      "started_at": "2026-11-11T19:31:50.112746Z",
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
        "at": "2026-11-11T19:31:47.271260Z",
        "bytes": 2747,
        "measured_seconds": 3,
        "n": 1,
        "rehearsal_id": "exr_75b5ddcd5523",
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

export const realVerify: ExitVerifyResult = {
  "anchor": {
    "checked_at": "2026-11-11T19:31:46.794521+00:00",
    "digest": "0ddd2fdd00cc11950f8ce2b90f57a1abb9bf35120c72bca93a2a62cb53e98892",
    "provider": "stub",
    "reason": "the local stub is not a write-once store",
    "retention": null,
    "status": "not_anchored"
  },
  "content_sha256": "0ddd2fdd00cc11950f8ce2b90f57a1abb9bf35120c72bca93a2a62cb53e98892",
  "errors": [],
  "kind": "exit_evidence",
  "merkle_ok": true,
  "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
  "ok": true,
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
  "generated_at": "2026-11-11T19:31:50.354279Z",
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

/** As sent while running: tables carry only name, status, reason, job_id, key_columns, schema. */
export const realRunningRaw: unknown = {
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
    "export": "/api/v1/exit/rehearsals/exr_395736bc4a21/evidence",
    "signed": false
  },
  "finished_at": null,
  "id": "exr_395736bc4a21",
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
  "started_at": "2026-11-11T19:31:50.120721Z",
  "system_id": "exs_fbb58d0b8b07",
  "system_name": "Running one",
  "tables": [
    {
      "job_id": "6af4c2a6f82ae80fc99c92f4",
      "key_columns": [
        "id"
      ],
      "name": "orders",
      "reason": "Extracting (running).",
      "schema": null,
      "status": "running"
    }
  ],
  "timeline": [
    {
      "finished_at": "2026-11-11T19:31:50.120721Z",
      "seconds": 0.0,
      "stage": "inventory",
      "started_at": "2026-11-11T19:31:50.120721Z",
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

/** The signed Exit Evidence Pack (JSON export). */
export const realPack: Record<string, unknown> = {
  "actor": "compliance@example.com",
  "content_sha256": "0ddd2fdd00cc11950f8ce2b90f57a1abb9bf35120c72bca93a2a62cb53e98892",
  "content_sha256_attested": "c72054957cdb2b50748df0b4ac849ac9b985aa7dab623bf174be996573df00e4",
  "issued_at": "2026-11-11T19:31:46.786250Z",
  "kind": "exit_evidence",
  "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
  "parts": [
    {
      "name": "exit_exr_90022030a84e_0000_orders.jsonl",
      "object_key": null,
      "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
      "size_bytes": 2747,
      "storage": "local_file",
      "table": "orders",
      "upload_parts": null,
      "verified_on_read": true
    },
    {
      "name": "exit_exr_90022030a84e_0001_events.jsonl",
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
    "finished_at": "2026-11-11T19:31:46.776321Z",
    "id": "exr_90022030a84e",
    "n": 2,
    "seal": {
      "algorithm": "sha256-merkle-rfc6962",
      "merkle_root": "sha256:46d0c7557b1ef6755c22ae599995ed8b33fe207828674f39cf13c0487bb2ef7d",
      "parts": [
        {
          "name": "exit_exr_90022030a84e_0000_orders.jsonl",
          "object_key": null,
          "sha256": "7ab3620fb9acf2b692029ceec82ef29368ce085482540483a79669221f119cbb",
          "size_bytes": 2747,
          "storage": "local_file",
          "table": "orders",
          "upload_parts": null,
          "verified_on_read": true
        },
        {
          "name": "exit_exr_90022030a84e_0001_events.jsonl",
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
          "at": "2026-11-11T19:31:46.781484Z",
          "authority": "workspace admin",
          "content_sha256": "c72054957cdb2b50748df0b4ac849ac9b985aa7dab623bf174be996573df00e4",
          "note": "",
          "role": "engineering",
          "signer_role": "admin",
          "user": "engineering@example.com",
          "version": 4
        },
        {
          "at": "2026-11-11T19:31:46.783860Z",
          "authority": "workspace admin",
          "content_sha256": "c72054957cdb2b50748df0b4ac849ac9b985aa7dab623bf174be996573df00e4",
          "note": "",
          "role": "data_owner",
          "signer_role": "admin",
          "user": "data_owner@example.com",
          "version": 5
        },
        {
          "at": "2026-11-11T19:31:46.786240Z",
          "authority": "workspace admin",
          "content_sha256": "c72054957cdb2b50748df0b4ac849ac9b985aa7dab623bf174be996573df00e4",
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
      "signed_at": "2026-11-11T19:31:46.786250Z",
      "status": "signed"
    },
    "started_at": "2026-11-11T19:31:46.536908Z",
    "system_id": "exs_3c67e3fff094",
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
            "match": true,
            "source": {
              "max": "60",
              "min": "1",
              "sum": "1830"
            }
          }
        ],
        "differences": [],
        "extract_seconds": 0.163,
        "job_id": "6af4c2a2f82ae80fc99c92ee",
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
          "name": "exit_exr_90022030a84e_0000_orders.jsonl",
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
          "extract_sha256": "dc7c91eaa0a841541002fd2524244f38a07ef02a7d9777ce08dae151a518d9a2",
          "match": true,
          "source_sha256": "dc7c91eaa0a841541002fd2524244f38a07ef02a7d9777ce08dae151a518d9a2"
        },
        "rows_extracted": 60,
        "rows_source": 60,
        "schema": null,
        "status": "proven",
        "value_parity": {
          "method": "population_digest",
          "parity_verdict": "unverifiable",
          "report": "/api/v1/jobs/6af4c2a2f82ae80fc99c92ee/parity-report",
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
            "match": true,
            "source": {
              "max": "2",
              "min": "1",
              "sum": "4"
            }
          }
        ],
        "differences": [],
        "extract_seconds": 0.079,
        "job_id": "6af4c2a2f82ae80fc99c92ef",
        "key_columns": [],
        "key_set": {
          "extract_sha256": null,
          "match": null,
          "source_sha256": null
        },
        "name": "events",
        "part": {
          "name": "exit_exr_90022030a84e_0001_events.jsonl",
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
          "extract_sha256": "4f6ed7c1ef95af02d8de1fd48ffab7055f457767fc5640bbad39fe1c4132194f",
          "match": true,
          "source_sha256": "4f6ed7c1ef95af02d8de1fd48ffab7055f457767fc5640bbad39fe1c4132194f"
        },
        "rows_extracted": 3,
        "rows_source": 3,
        "schema": null,
        "status": "proven",
        "value_parity": {
          "method": "population_digest",
          "parity_verdict": "unverifiable",
          "report": "/api/v1/jobs/6af4c2a2f82ae80fc99c92ef/parity-report",
          "unexplained": 0
        }
      }
    ],
    "timeline": [
      {
        "finished_at": "2026-11-11T19:31:46.536908Z",
        "seconds": 0.0,
        "stage": "inventory",
        "started_at": "2026-11-11T19:31:46.536908Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-11T19:31:46.715322Z",
        "seconds": 0.152,
        "stage": "extract",
        "started_at": "2026-11-11T19:31:46.562851Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-11T19:31:46.776321Z",
        "seconds": 0.008,
        "stage": "verify",
        "started_at": "2026-11-11T19:31:46.768655Z",
        "status": "done"
      },
      {
        "finished_at": "2026-11-11T19:31:46.776321Z",
        "seconds": 0.0,
        "stage": "seal",
        "started_at": "2026-11-11T19:31:46.776321Z",
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
  "rehearsal_id": "exr_90022030a84e",
  "signature": {
    "alg": "HMAC-SHA256",
    "key_id": "platform_auth_secret",
    "value": "20c9295dcd38315d979f8d1b74041304ec8c0020b1830d4f98d95e14022cd4b7"
  },
  "system": {
    "criticality": "critical",
    "engine": "sqlite",
    "exit_window_hours": 720,
    "name": "Core ledger",
    "window_basis": "EU Data Act switching window"
  },
  "system_id": "exs_3c67e3fff094",
  "version": "1",
  "workspace_id": "ws1"
};
