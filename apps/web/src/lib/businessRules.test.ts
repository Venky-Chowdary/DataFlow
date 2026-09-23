/**
 * Run: npx --yes tsx --test apps/web/src/lib/businessRules.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  mergeBusinessRules,
  mergeCompiledShapeSteps,
  ruleReportSummary,
  ruleToEvidence,
  type RuleCompileReport,
} from "./businessRules";
import type { EditableMapping } from "./mapping";

const report: RuleCompileReport = {
  filename: "rules.csv",
  rule_count: 3,
  buckets: { executable: 2, needs_confirmation: 1, conflict: 0 },
  unused_dest_columns: ["unused_flag"],
  unused_dest_count: 1,
  shape_steps: [],
  honesty: "test",
  rules: [
    {
      source_column: "fname",
      dest_column: "first_name",
      rule_text: "Direct",
      kind: "direct",
      kind_label: "Direct map",
      plane: "map",
      confidence: 0.99,
      transform: "none",
      status: "executable",
      provenance: { sheet: "Rules", row: 2 },
    },
    {
      source_column: "status",
      dest_column: "status",
      rule_text: "A → ACTIVE, I → INACTIVE",
      kind: "lookup",
      kind_label: "Code crosswalk",
      plane: "map",
      confidence: 0.98,
      transform: "none",
      code_crosswalk: { A: "ACTIVE", I: "INACTIVE" },
      status: "executable",
      provenance: { sheet: "Rules", row: 3 },
    },
    {
      source_column: "mystery",
      dest_column: "segment",
      rule_text: "use the legacy id unless migrated",
      kind: "unknown",
      kind_label: "Needs review",
      plane: "review",
      confidence: 0.35,
      status: "needs_confirmation",
      issues: ["rule text is not a closed form this compiler can execute"],
      provenance: { sheet: "Rules", row: 4 },
    },
  ],
};

const seed: EditableMapping[] = [
  { source: "fname", target: "", confidence: 0.4, approved: false, transform: "none" },
  { source: "status", target: "status", confidence: 0.8, approved: false, transform: "none" },
  { source: "mystery", target: "", confidence: 0.2, approved: false, transform: "none" },
];

describe("mergeBusinessRules", () => {
  it("binds executable rules and leaves unknown ones for review", () => {
    const next = mergeBusinessRules(seed, report);
    const fname = next.find((m) => m.source === "fname")!;
    assert.equal(fname.target, "first_name");
    assert.equal(fname.approved, true);
    assert.equal(fname.businessRule?.kind, "direct");
    assert.equal(fname.businessRule?.row, 2);

    const status = next.find((m) => m.source === "status")!;
    assert.deepEqual(status.codeCrosswalk, { A: "ACTIVE", I: "INACTIVE" });
    assert.equal(status.approved, true);

    const mystery = next.find((m) => m.source === "mystery")!;
    assert.equal(mystery.approved, false);
    assert.equal(mystery.requiresReview, true);
    assert.equal(mystery.businessRule?.status, "needs_confirmation");
    assert.equal(mystery.target, "");
  });

  it("does not invent a dest column for unused dests", () => {
    const next = mergeBusinessRules(seed, report);
    assert.equal(next.some((m) => m.target === "unused_flag"), false);
  });

  it("summarises unused dest columns as not written", () => {
    assert.match(ruleReportSummary(report), /2 executable/);
    assert.match(ruleReportSummary(report), /1 dest column\(s\) unused/);
  });

  it("summarises inferred headers when the file did not use aliases", () => {
    const inferred: RuleCompileReport = {
      ...report,
      header_roles: [
        { header: "Orig Field", role: "source_column", method: "schema", confidence: 1, reason: "schema" },
        { header: "How to convert", role: "rule", method: "rule_pattern", confidence: 1, reason: "rules" },
      ],
    };
    assert.match(ruleReportSummary(inferred), /headers inferred from file \+ schema/);
  });

  it("does not overwrite an operator-locked dest", () => {
    const locked: EditableMapping[] = [
      { source: "fname", target: "given_name", confidence: 0.9, approved: true, transform: "none" },
    ];
    const next = mergeBusinessRules(locked, report);
    assert.equal(next[0].target, "given_name");
    assert.equal(next[0].businessRule?.status, "conflict");
    assert.equal(next[0].requiresReview, true);
  });

  it("fans one source out to a second dest instead of overwriting", () => {
    const twoDest: RuleCompileReport = {
      ...report,
      rules: [
        report.rules[0],
        {
          ...report.rules[0],
          dest_column: "display_name",
          rule_text: "Direct",
          provenance: { sheet: "Rules", row: 9 },
        },
      ],
    };
    const next = mergeBusinessRules(seed, twoDest);
    const fnameRows = next.filter((m) => m.source === "fname");
    assert.equal(fnameRows.length, 2);
    assert.deepEqual(fnameRows.map((m) => m.target).sort(), ["display_name", "first_name"]);
  });

  it("shows named-rule expansion and never writes an unknown-code default", () => {
    const named: RuleCompileReport = {
      ...report,
      named_rules: ["EmailClean"],
      rules: [
        {
          source_column: "email",
          dest_column: "email",
          rule_text: "%EmailClean%",
          named_rule: "EmailClean",
          resolved_rule: "lowercase + validate email",
          kind: "email",
          kind_label: "Normalize email",
          plane: "map",
          confidence: 0.96,
          transform: "email",
          status: "executable",
        },
        {
          source_column: "status",
          dest_column: "status",
          rule_text: "A → ACTIVE, unmapped → OTHER",
          kind: "lookup",
          kind_label: "Code crosswalk",
          plane: "map",
          confidence: 0.98,
          transform: "none",
          code_crosswalk: { A: "ACTIVE" },
          unknown_code_policy: { action: "default", value: "OTHER" },
          status: "executable",
        },
      ],
    };
    const next = mergeBusinessRules([
      { source: "email", target: "", confidence: 0.4, approved: false, transform: "none" },
      { source: "status", target: "status", confidence: 0.8, approved: false, transform: "none" },
    ], named);
    const email = next.find((m) => m.source === "email")!;
    assert.match(ruleToEvidence(named.rules[0]).text, /EmailClean/);
    assert.match(email.businessRule?.text || "", /lowercase/);
    const status = next.find((m) => m.source === "status")!;
    assert.deepEqual(status.codeCrosswalk, { A: "ACTIVE" });
    assert.equal(Object.prototype.hasOwnProperty.call(status.codeCrosswalk || {}, "OTHER"), false);
    assert.match(ruleReportSummary(named), /1 named rule/);
  });

  it("applies a named IANA zone instead of a zoneless assume_timezone", () => {
    const zoned: RuleCompileReport = {
      ...report,
      rules: [
        {
          source_column: "created_at",
          dest_column: "created_at",
          rule_text: "assume timezone America/New_York",
          kind: "timezone",
          kind_label: "Timezone (review)",
          plane: "map",
          confidence: 0.96,
          transform: "assume_timezone",
          engine_transform: "assume_timezone:America/New_York",
          timezone: "America/New_York",
          status: "executable",
        },
      ],
    };
    const next = mergeBusinessRules(
      [{ source: "created_at", target: "created_at", confidence: 0.8, approved: false, transform: "none" }],
      zoned,
    );
    assert.equal(next[0].transform, "assume_timezone");
    assert.equal(next[0].engineTransform, "assume_timezone:America/New_York");
  });

  it("scopes merge to one selected source table so id does not cross streams", () => {
    const twoTables: RuleCompileReport = {
      ...report,
      source_tables: ["customers", "orders"],
      rules: [
        {
          source_table: "customers",
          source_column: "id",
          dest_column: "customer_id",
          rule_text: "Direct",
          kind: "direct",
          kind_label: "Direct",
          plane: "map",
          confidence: 0.99,
          status: "executable",
        },
        {
          source_table: "orders",
          source_column: "id",
          dest_column: "order_id",
          rule_text: "Direct",
          kind: "direct",
          kind_label: "Direct",
          plane: "map",
          confidence: 0.99,
          status: "executable",
        },
      ],
    };
    const customers = mergeBusinessRules(
      [{ source: "id", target: "", confidence: 0.5, approved: false, transform: "none" }],
      twoTables,
      { sourceTable: "customers" },
    );
    assert.equal(customers[0].target, "customer_id");
    const orders = mergeBusinessRules(
      [{ source: "id", target: "", confidence: 0.5, approved: false, transform: "none" }],
      twoTables,
      { sourceTable: "orders" },
    );
    assert.equal(orders[0].target, "order_id");
  });

  it("merges compiled shape steps without dropping operator steps", () => {
    const existing = [{ op: "trim", column: "email", enabled: true }];
    const compiled = [
      { op: "trim", column: "email", enabled: true },
      { op: "derive_column", options: { to: "annual_salary", expression: "salary * 12" }, enabled: true },
    ];
    const next = mergeCompiledShapeSteps(existing, compiled);
    assert.equal(next.length, 2);
    assert.equal(next[1].op, "derive_column");
  });

  it("does not merge another table's compiled shape steps onto this stream", () => {
    const existing = [{ op: "trim", column: "note", enabled: true }];
    const compiled = [
      { op: "parse_date", column: "signed_on", source_table: "customers", options: { format: "MM/DD/YYYY" }, enabled: true },
      { op: "trim", column: "sku", source_table: "orders", enabled: true },
    ];
    const next = mergeCompiledShapeSteps(existing, compiled, { sourceTable: "customers" });
    assert.equal(next.length, 2);
    assert.equal(next[1].op, "parse_date");
    assert.equal(next.some((step) => step.column === "sku"), false);
  });
});
