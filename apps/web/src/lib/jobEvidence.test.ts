/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobEvidence.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  foreignKeyProblems,
  formatJobRoute,
  identityAlignmentSentence,
  presentChecksumNote,
  presentJobPhases,
  presentStoredEventLog,
  presentStoredExplanation,
  isRestoredEndpointTitle,
  readCoercedNullRows,
  readForeignKeyCarry,
  readIdentityAlignment,
  readJobMappings,
  readJobStreamNames,
  readJobStreams,
  readRejectedDetails,
  readRejectedRows,
  readSchemaFidelity,
  readWriterWarnings,
} from "./jobEvidence.js";

const orders = {
  name: "orders",
  status: "completed",
  records_processed: 50,
  watermark: "50",
  row_accounting: { conservation_kind: "overwrite", dest_count: 50, balanced: true },
};
const users = {
  name: "users",
  status: "completed",
  records_processed: 3,
  cdc_lag_seconds: 1.2,
};

describe("readJobStreams", () => {
  it("reads stream health that lives only on the destination summary", () => {
    const streams = readJobStreams({
      destination_summary: { streams: [orders, users] },
    });
    assert.deepEqual(streams.map((s) => s.name), ["orders", "users"]);
    assert.equal(streams[0].records_processed, 50);
    assert.equal(streams[0].watermark, "50");
    assert.equal(streams[1].cdc_lag_seconds, 1.2);
    assert.equal(streams[0].row_accounting?.conservation_kind, "overwrite");
  });

  it("prefers the summary list when the top-level list is a different copy", () => {
    const streams = readJobStreams({
      streams: [{ name: "stale", status: "running" }],
      destination_summary: { streams: [orders] },
    });
    assert.deepEqual(streams.map((s) => s.name), ["orders"]);
  });

  it("uses the top-level list when the summary has no streams", () => {
    const streams = readJobStreams({
      streams: [users],
      destination_summary: { rejected_rows: 1 },
    });
    assert.deepEqual(streams.map((s) => s.name), ["users"]);
  });

  it("keeps a single named stream", () => {
    const streams = readJobStreams({ destination_summary: { streams: [orders] } });
    assert.equal(streams.length, 1);
    assert.equal(streams[0].name, "orders");
  });

  it("drops unnamed rows and empty payloads", () => {
    assert.deepEqual(readJobStreams(null), []);
    assert.deepEqual(readJobStreams({ destination_summary: { streams: [] } }), []);
    assert.deepEqual(
      readJobStreams({ destination_summary: { streams: [{ status: "completed" }, { name: "  " }] } }),
      [],
    );
  });

  it("accepts a stream key when name is absent", () => {
    const streams = readJobStreams({
      destination_summary: { streams: [{ stream: "payments", status: "failed", error: "timeout" }] },
    });
    assert.equal(streams[0].name, "payments");
    assert.equal(streams[0].error, "timeout");
  });
});

describe("multi-table route", () => {
  it("names every stream instead of the restored endpoint", () => {
    const job = {
      source_name: "orders",
      destination_database: "dataflow",
      destination_collection: "orders",
      destination_summary: {
        multi_stream: true,
        table: "orders",
        streams: [{ name: "customers" }, { name: "orders" }],
      },
    };
    assert.deepEqual(readJobStreamNames(job), ["customers", "orders"]);
    assert.equal(formatJobRoute(job), "customers, orders → dataflow (2 tables)");
    assert.equal(isRestoredEndpointTitle("orders → orders", job), true);
    assert.equal(isRestoredEndpointTitle("Nightly customers", job), false);
  });

  it("reads list-only stream names when the summary was stripped", () => {
    const job = {
      source_name: "orders",
      destination_database: "dataflow",
      destination_collection: "orders",
      stream_names: ["customers", "orders", "orders", " "],
    };
    assert.deepEqual(readJobStreamNames(job), ["customers", "orders"]);
    assert.match(formatJobRoute(job), /customers, orders/);
  });

  it("keeps a single-table route on the stored endpoint", () => {
    const job = {
      source_name: "orders",
      destination_database: "dataflow",
      destination_collection: "orders",
      destination_summary: { streams: [{ name: "orders" }] },
    };
    assert.equal(formatJobRoute(job), "orders → dataflow.orders");
  });
});

describe("identity alignment", () => {
  it("does not call a skipped write-pass fingerprint a mismatch", () => {
    const view = readIdentityAlignment({
      destination_summary: {
        multi_stream: true,
        table: "orders",
        streams: [{ name: "customers" }, { name: "orders" }],
        identity_alignment: {
          identity_hash_aligned: null,
          write_pass_rows: 0,
          reread_rows: 2,
          reason: "write_pass_not_fingerprinted",
        },
      },
    });
    assert.ok(view);
    assert.equal(view.aligned, null);
    assert.equal(view.lastStreamOnly, true);
    assert.equal(view.streamName, "orders");
    const sentence = identityAlignmentSentence(view);
    assert.match(sentence, /Last stream orders/);
    assert.match(sentence, /not a hash mismatch/);
    assert.match(sentence, /not a digest of the other tables/);
    assert.doesNotMatch(sentence, /did not align/);
  });

  it("says when the hashes actually disagreed", () => {
    const view = readIdentityAlignment({
      destination_summary: {
        identity_alignment: {
          identity_hash_aligned: false,
          write_pass_rows: 0,
          reread_rows: 2,
          reason: "write_pass_empty",
        },
      },
    });
    assert.equal(view?.aligned, false);
    assert.equal(view?.lastStreamOnly, false);
    assert.match(identityAlignmentSentence(view!), /did not align/);
    assert.match(identityAlignmentSentence(view!), /write_pass_empty/);
  });
});

describe("presentStoredExplanation", () => {
  const stored = [
    "Transfer: database/postgresql (orders) → database/postgresql (orders)",
    "Sync behavior: Destination will be cleared and fully replaced with source data.",
    "Source inferred 4 columns: id, customer_id, amount, updated_at",
    "Rows written: 4",
  ].join("\n");

  it("rewrites a stored single-table opening when the run has two streams", () => {
    const text = presentStoredExplanation(stored, {
      source_name: "orders",
      destination_database: "dataflow",
      destination_collection: "orders",
      destination_summary: {
        multi_stream: true,
        table: "orders",
        streams: [{ name: "customers" }, { name: "orders" }],
      },
    });
    assert.match(text, /^Transfer: customers, orders → dataflow \(2 tables\)/);
    assert.match(text, /Each selected table is cleared/);
    assert.match(text, /last stream \(orders\)/);
    assert.match(text, /Source inferred 4 columns/);
    assert.doesNotMatch(text, /postgresql \(orders\) →/);
  });

  it("leaves a single-table explanation unchanged", () => {
    const text = presentStoredExplanation(stored, {
      source_name: "orders",
      destination_database: "dataflow",
      destination_collection: "orders",
    });
    assert.equal(text, stored);
  });
});

describe("presentStoredEventLog", () => {
  const job = {
    destination_summary: {
      multi_stream: true,
      table: "orders",
      streams: [{ name: "customers" }, { name: "orders" }],
    },
  };
  const stored = [
    "15:19:52 — Analyzing source table…",
    "15:19:52 — Streaming 2 rows in batches…",
    "15:19:53 — Writing batch 1/1 (2 rows)…",
    "15:19:53 — All rows written — reconciling destination (4 rows: counts + checksum proof)…",
    "15:19:53 — Checksum matches orders (2 rows). Job destination population is 4 across 2 streams (each COUNT(*)). This digest is not the whole job.",
  ];

  it("names the last stream on a stored checksum sentence and does not invent the missing table", () => {
    const lines = presentStoredEventLog(stored, job);
    assert.match(lines[0], /Analyzing the restored endpoint/);
    assert.match(lines[1], /restored endpoint, then each of 2 tables/);
    assert.match(lines[2], /one table's batch, not the job total/);
    assert.doesNotMatch(lines[2], /orders/);
    assert.match(lines[3], /4 rows written across 2 tables/);
    assert.match(lines[3], /last stream \(orders\), not this total/);
    assert.doesNotMatch(lines[3], /counts \+ checksum proof/);
    assert.match(lines[4], /not the whole job/);
  });

  it("leaves a second pass and a single-table log unchanged", () => {
    const once = presentStoredEventLog(stored, job);
    assert.deepEqual(presentStoredEventLog(once, job), once);
    assert.deepEqual(presentStoredEventLog(stored, { source_name: "orders" }), stored);
  });

  it("qualifies a last-stream re-read note that claims migration_proven", () => {
    const note = presentChecksumNote(
      "Independent source re-read after the write pass (scan pagination) — dest read-back can earn full_checksum / migration_proven.",
      job,
    );
    assert.match(note, /does not earn migration_proven for the job/);
    assert.equal(presentChecksumNote(note, job), note);
    assert.doesNotMatch(
      presentChecksumNote("write-pass fingerprints only", job),
      /migration_proven for the job/,
    );
  });
});

describe("presentJobPhases", () => {
  it("says the load line is the last table when several tables were written", () => {
    const phases = presentJobPhases(
      [
        { name: "extract", status: "done" as const, message: "Analyzing the restored endpoint…" },
        { name: "load", status: "done" as const, message: "Wrote 2 rows on orders…" },
      ],
      { destination_summary: { streams: [{ name: "customers" }, { name: "orders" }] } },
    );
    assert.equal(phases[0].message, "Analyzing the restored endpoint…");
    assert.match(phases[1].message || "", /Wrote 2 rows on orders — last table updated, not the only table/);
    assert.equal(presentJobPhases(phases, {
      destination_summary: { streams: [{ name: "customers" }, { name: "orders" }] },
    })[1].message, phases[1].message);
  });

  it("leaves a single-table load line unchanged", () => {
    const phases = [{ name: "load", status: "done" as const, message: "Wrote 2 rows on orders…" }];
    assert.equal(presentJobPhases(phases, { source_name: "orders" })[0].message, phases[0].message);
  });
});

describe("quarantine evidence", () => {
  it("reads rejected and coerced counts from the summary when the job root is empty", () => {
    const job = { destination_summary: { rejected_rows: 4, coerced_null_rows: 1 } };
    assert.equal(readRejectedRows(job), 4);
    assert.equal(readCoercedNullRows(job), 1);
  });

  it("keeps a stamped top-level count", () => {
    const job = {
      rejected_rows: 2,
      destination_summary: { rejected_rows: 9 },
    };
    assert.equal(readRejectedRows(job), 2);
  });

  it("reads writer warnings and a partial foreign-key carry from the summary", () => {
    const job = {
      destination_summary: {
        warnings: ["batch checksum skipped", ""],
        warnings_suppressed: 2,
        foreign_keys: {
          verdict: "partial",
          carried: 1,
          integrity_violations: 0,
          cycle: ["orders", "customers"],
          cycle_resolved: false,
          cycle_note: "Cycle orders, customers is not fully enforced",
          decisions: [
            { name: "fk_ok", status: "carried", dest_table: "orders", referenced_table: "customers" },
            {
              name: "fk_bad",
              status: "unsupported",
              dest_table: "orders",
              referenced_table: "customers",
              reason: "parent type differs",
            },
          ],
        },
      },
    };
    const warnings = readWriterWarnings(job);
    assert.deepEqual(warnings.messages, ["batch checksum skipped"]);
    assert.equal(warnings.suppressed, 2);
    const carry = readForeignKeyCarry(job);
    assert.equal(carry?.verdict, "partial");
    assert.equal(carry?.cycleResolved, false);
    assert.deepEqual(foreignKeyProblems(carry!).map((d) => d.status), ["unsupported"]);
    assert.equal(foreignKeyProblems(carry!)[0].reason, "parent type differs");
  });

  it("treats a missing cycle_resolved flag as unresolved", () => {
    const carry = readForeignKeyCarry({
      destination_summary: {
        foreign_keys: { verdict: "partial", cycle: ["a", "b"], decisions: [] },
      },
    });
    assert.equal(carry?.cycleResolved, null);
  });

  it("returns null when the run recorded no foreign-key carry", () => {
    assert.equal(readForeignKeyCarry({ destination_summary: {} }), null);
    assert.deepEqual(readWriterWarnings({}), { messages: [], suppressed: 0 });
  });

  it("shows summary findings when the job root has no sample", () => {
    const details = readRejectedDetails({
      destination_summary: {
        rejected_details: [{ column: "qty", reason: "not a number", value: "x" }],
      },
    });
    assert.equal(details?.length, 1);
    assert.equal(details?.[0].column, "qty");
  });
});

describe("readJobMappings", () => {
  it("reads multi-table maps from stream contracts when the top-level list is empty", () => {
    const rows = readJobMappings({
      mapping_proof: {},
      transfer_request: {
        mappings: [],
        stream_contracts: [
          {
            name: "orders",
            selected: true,
            mappings: [{ source: "id", target: "id", source_type: "INTEGER", confidence: 0.99 }],
          },
          {
            name: "customers",
            selected: true,
            mappings: [{ source: "email", target: "email", source_type: "TEXT", confidence: 0.99 }],
          },
          { name: "skipped", selected: false, mappings: [{ source: "x", target: "x" }] },
        ],
      },
    });
    assert.deepEqual(rows.map((row) => [row.stream, row.source]), [
      ["orders", "id"],
      ["customers", "email"],
    ]);
  });

  it("prefers a non-empty proof over the request", () => {
    const rows = readJobMappings({
      mapping_proof: { mappings: [{ source: "sku", target: "sku", stream: "items" }] },
      transfer_request: { mappings: [{ source: "other", target: "other" }] },
    });
    assert.deepEqual(rows.map((row) => row.source), ["sku"]);
    assert.equal(rows[0].stream, "items");
  });
});

describe("readSchemaFidelity", () => {
  it("lists unsupported and unmeasured aspects and drops carried and skipped rows", () => {
    const view = readSchemaFidelity({
      destination_summary: {
        schema_fidelity: {
          carried_count: 8,
          unsupported_count: 1,
          unknown_count: 1,
          skipped_count: 18,
          items: [
            { aspect: "primary_key", name: "id", status: "carried" },
            { aspect: "trigger", name: "*", status: "skipped", reason: "none" },
            { aspect: "foreign_key", name: "orders_customer_id_fkey", status: "unsupported", reason: "parent first" },
            { aspect: "enum_domain", name: "*", status: "unknown", reason: "catalog was not read" },
          ],
        },
      },
    });
    assert.equal(view?.unsupported, 1);
    assert.deepEqual(view?.items.map((item) => item.aspect), ["foreign_key", "enum_domain"]);
  });

  it("drops a create-table foreign key once the later carry proved that constraint", () => {
    const view = readSchemaFidelity({
      destination_summary: {
        schema_fidelity: {
          carried_count: 8,
          unsupported_count: 1,
          unknown_count: 2,
          skipped_count: 18,
          items: [
            {
              aspect: "foreign_key",
              name: "orders_customer_id_fkey",
              status: "unsupported",
              reason: "CREATE TABLE does not add this reference",
            },
            { aspect: "enum_domain", name: "*", status: "unknown", reason: "catalog was not read" },
            { aspect: "nested_shape", name: "*", status: "unknown", reason: "catalog was not read" },
          ],
        },
        foreign_keys: {
          verdict: "carried",
          decisions: [
            {
              name: "fk_orders_customer_id",
              status: "carried",
              source_detail: "orders_customer_id_fkey: (customer_id) -> public.customers(id)",
            },
          ],
        },
      },
    });
    assert.equal(view?.unsupported, 0);
    assert.deepEqual(view?.items.map((item) => item.aspect), ["enum_domain", "nested_shape"]);
  });

  it("keeps an unsupported foreign key the carry did not prove", () => {
    const view = readSchemaFidelity({
      destination_summary: {
        schema_fidelity: {
          unsupported_count: 1,
          unknown_count: 0,
          items: [
            { aspect: "foreign_key", name: "orders_customer_id_fkey", status: "unsupported", reason: "not added" },
          ],
        },
        foreign_keys: {
          decisions: [{ name: "fk_other", status: "unsupported", source_detail: "other_fkey: (x) -> t(id)" }],
        },
      },
    });
    assert.equal(view?.unsupported, 1);
    assert.equal(view?.items[0].name, "orders_customer_id_fkey");
  });

  it("returns null when every aspect was carried or measured absent", () => {
    assert.equal(
      readSchemaFidelity({
        destination_summary: {
          schema_fidelity: {
            carried_count: 2,
            unsupported_count: 0,
            unknown_count: 0,
            items: [{ aspect: "primary_key", status: "carried" }],
          },
        },
      }),
      null,
    );
  });
});
