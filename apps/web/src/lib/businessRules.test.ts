/**
 * Run: npx --yes tsx --test apps/web/src/lib/businessRules.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  acceptRuleAsDirect,
  canAcceptAsDirect,
  compiledExpression,
  mergeBusinessRules,
  mergeCompiledShapeSteps,
  namedRuleDisplay,
  proofRuleClaim,
  ruleCensus,
  ruleMixSummary,
  ruleEdgeLabel,
  ruleReportSummary,
  ruleReviewHint,
  ruleStatusLabel,
  ruleToEvidence,
  type CompiledRule,
  type RuleCompileReport,
} from "./businessRules";
import { buildPreflightMappings } from "./mapping";
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
      kind: "direct",
      kind_label: "Direct map",
      plane: "review",
      confidence: 0.62,
      transform: "none",
      status: "needs_confirmation",
      issues: ["low-confidence binding — confirm the rename"],
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
    assert.match(ruleMixSummary(report), /^3 rules: 3 mapping, 0 validation$/);
    assert.match(ruleReportSummary(report), /^3 rules: 3 mapping, 0 validation/);
    const census = ruleCensus(report);
    assert.equal(census.total, 3);
    assert.equal(census.mapping, 3);
    assert.equal(census.validation, 0);
    assert.equal(compiledExpression(report.rules[1]), "A → ACTIVE; I → INACTIVE");
  });

  it("shows named rules as R001 · text and honest coverage without inventing execution", () => {
    const named: CompiledRule = {
      ...report.rules[0],
      named_rule: "R001",
      rule_text: "Copy customer_id without modification",
      resolved_rule: "",
    };
    assert.equal(namedRuleDisplay(named), "R001 · Copy customer_id without modification");
    const covered: RuleCompileReport = {
      ...report,
      coverage: {
        detected: 3,
        executable: 2,
        review: 1,
        conflict: 0,
        writes: 2,
        validations: 0,
        percent: 67,
      },
    };
    assert.match(ruleReportSummary(covered), /rule coverage 67%/);
    assert.match(proofRuleClaim(covered), /review remains/);
    const proven: RuleCompileReport = {
      ...report,
      buckets: { executable: 3, needs_confirmation: 0, conflict: 0 },
      coverage: {
        detected: 3, executable: 3, review: 0, conflict: 0, writes: 3, validations: 0, percent: 100,
      },
    };
    assert.equal(proofRuleClaim(proven), "Migration verified against 3 business rules");
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

  it("replaces create-new identity dests with compiled workbook names", () => {
    const rematch: EditableMapping[] = [
      { source: "customer_id", target: "customer_id", confidence: 0.93, approved: true, transform: "none", createNew: true },
      { source: "fname", target: "fname", confidence: 0.93, approved: true, transform: "none", createNew: true },
      { source: "email", target: "email", confidence: 0.93, approved: true, transform: "none", createNew: true },
      { source: "status", target: "status", confidence: 0.93, approved: true, transform: "none", createNew: true },
      { source: "monthly_salary", target: "monthly_salary", confidence: 0.93, approved: true, transform: "none", createNew: true },
      { source: "annual_salary", target: "annual_salary", confidence: 0.93, approved: true, transform: "none", createNew: true },
    ];
    const workbook: RuleCompileReport = {
      ...report,
      rule_count: 5,
      buckets: { executable: 5, needs_confirmation: 0, conflict: 0 },
      unused_dest_columns: [],
      unused_dest_count: 0,
      rules: [
        { ...report.rules[0], source_column: "customer_id", dest_column: "customer_key", kind: "direct", kind_label: "Direct map" },
        { ...report.rules[0], source_column: "fname", dest_column: "first_name" },
        {
          source_column: "email",
          dest_column: "email_address",
          rule_text: "Convert email to lowercase",
          kind: "email",
          kind_label: "Normalize email",
          plane: "map",
          confidence: 0.99,
          transform: "email",
          status: "executable",
        },
        { ...report.rules[1], dest_column: "customer_status" },
        {
          source_column: "monthly_salary",
          map_source: "annual_salary",
          dest_column: "annual_salary",
          rule_text: "monthly_salary * 12",
          kind: "derive",
          kind_label: "Derived value",
          plane: "shape",
          confidence: 0.99,
          transform: "none",
          status: "executable",
        },
      ],
    };
    const next = mergeBusinessRules(rematch, workbook);
    assert.equal(next.find((m) => m.source === "customer_id")?.target, "customer_key");
    // Map rematch after merge (step/analyze effect) must not lock identity dests.
    const rematchAgain = rematch.map((row) => ({
      ...row,
      target: row.source,
      businessRule: undefined,
    }));
    const restamped = mergeBusinessRules(rematchAgain, workbook);
    assert.equal(restamped.find((m) => m.source === "fname")?.target, "first_name");
    assert.equal(restamped.find((m) => m.source === "email")?.target, "email_address");
    assert.equal(next.find((m) => m.source === "fname")?.target, "first_name");
    const email = next.find((m) => m.source === "email")!;
    assert.equal(email.target, "email_address");
    assert.equal(email.transform, "email");
    assert.equal(next.find((m) => m.source === "status")?.target, "customer_status");
    assert.equal(next.find((m) => m.source === "monthly_salary")?.target, "monthly_salary");
    assert.equal(next.find((m) => m.source === "annual_salary")?.target, "annual_salary");
    assert.equal(next.filter((m) => m.source === "fname").length, 1);
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
    assert.doesNotMatch(ruleReportSummary(named), /named mapping|named validation/);
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

describe("operator accept of a review rule", () => {
  it("accepts a bound rename as Direct and refuses a validation dest", () => {
    assert.equal(canAcceptAsDirect(report.rules[2]), true);
    const next = acceptRuleAsDirect(report, 2);
    assert.equal(next.rules[2].status, "executable");
    assert.equal(next.rules[2].kind, "direct");
    assert.equal(next.rules[2].interpretation, "Direct copy");
    assert.equal(next.rules[2].action, "auto");
    assert.equal(next.buckets.executable, 3);
    assert.equal(next.buckets.needs_confirmation, 0);
    const mapped = mergeBusinessRules(seed, next);
    assert.equal(mapped.find((row) => row.source === "mystery")?.target, "segment");
    assert.equal(mapped.find((row) => row.source === "mystery")?.approved, true);

    const validation: CompiledRule = {
      ...report.rules[2],
      dest_column: "V001",
      rule_text: "Must not be null",
      kind: "contract",
    };
    assert.equal(canAcceptAsDirect(validation), false);
    const missing: CompiledRule = {
      ...report.rules[2],
      source_column: "annual_salary",
      dest_column: "annual_salary",
      issues: ['Source column "annual_salary" is not on the selected source.'],
    };
    assert.equal(canAcceptAsDirect(missing), false);
  });

  it("refuses Accept-as-Direct for an unknown rule with bound columns", () => {
    const v007: CompiledRule = {
      ...report.rules[2],
      kind: "unknown",
      kind_label: "Needs review",
      status: "needs_confirmation",
      issues: ["rule text is not a closed form this compiler can execute"],
    };
    assert.equal(canAcceptAsDirect(v007), false);
    // The hint tells the operator to rewrite or add an explicit step.
    assert.match(
      ruleReviewHint(v007, true),
      /Not compiled — rewrite this rule in a supported form, or add an explicit Transform step/,
    );
    // A bound rename still passes.
    assert.equal(canAcceptAsDirect(report.rules[2]), true);
    assert.match(ruleReviewHint(report.rules[2], true), /Accept as Direct/);
  });

  it("tells a direct rule with an unbound column to remap, not rewrite", () => {
    const missing: CompiledRule = {
      ...report.rules[2],
      kind: "direct",
      status: "needs_confirmation",
      issues: ["Destination column “phone_no” is not on the selected destination."],
    };
    assert.equal(canAcceptAsDirect(missing), false);
    const hint = ruleReviewHint(missing, true);
    assert.match(hint, /not on the selected source or destination\. Remap it on Map/);
    assert.doesNotMatch(hint, /rewrite this rule/);
  });

  it("labels pre-run executable rules Ready and shows the rule id once", () => {
    assert.equal(ruleStatusLabel("executable"), "Ready");
    assert.equal(ruleStatusLabel("conflict"), "Conflict");
    assert.equal(ruleStatusLabel("needs_confirmation"), "Needs review");
    const contract: CompiledRule = {
      ...report.rules[0],
      kind: "contract",
      named_rule: "V001",
      dest_column: "email",
    };
    // The df2-rule-id chip already renders the id — the edge must not repeat it.
    assert.equal(ruleEdgeLabel(contract), "email");
  });

  it("does not merge destination contracts onto Map writes", () => {
    const withContract: RuleCompileReport = {
      ...report,
      rules: [
        ...report.rules,
        {
          source_column: "email",
          dest_column: "email_address",
          rule_text: "Must contain @",
          kind: "contract",
          kind_label: "Validate contract",
          interpretation: "Contains @",
          action: "auto",
          plane: "validate",
          confidence: 0.98,
          status: "executable",
          contract: { type: "contains", value: "@" },
        },
      ],
    };
    const next = mergeBusinessRules(
      [{ source: "email", target: "", confidence: 0.4, approved: false, transform: "none" }],
      withContract,
    );
    const email = next.find((row) => row.source === "email");
    assert.equal(email?.target, "");
    assert.equal(email?.businessRule, undefined);
  });
});

describe("rule contracts reach the mapping wire", () => {
  const contractReport: RuleCompileReport = {
    ...report,
    rules: [
      report.rules[0],
      {
        source_column: "fname",
        dest_column: "first_name",
        rule_text: "Must not be null",
        kind: "contract",
        kind_label: "Validate contract",
        plane: "validate",
        confidence: 0.98,
        status: "executable",
        contract: { type: "not_null" },
        enforced_at_execute: true,
        rule_id: "c1",
      },
      {
        source_column: "fname",
        dest_column: "first_name",
        rule_text: "Must be unique",
        kind: "contract",
        kind_label: "Validate contract",
        plane: "validate",
        confidence: 0.98,
        status: "executable",
        contract: { type: "unique" },
        enforced_at_execute: false,
        enforced_post_load: true,
        rule_id: "c2",
      },
    ],
  };

  it("attaches execute- and post-load-enforced contracts, deduped", () => {
    const next = mergeBusinessRules(seed, contractReport);
    const fname = next.find((m) => m.source === "fname")!;
    assert.deepEqual(
      (fname.ruleContracts || []).map((c) => c.rule_id),
      ["c1", "c2"],
    );
    assert.equal(fname.ruleContracts?.[0].type, "not_null");
    assert.equal(fname.ruleContracts?.[1].type, "unique");
    const again = mergeBusinessRules(next, contractReport);
    assert.equal(again.find((m) => m.source === "fname")!.ruleContracts?.length, 2);
  });

  it("emits rule_contracts on the preflight wire", () => {
    const next = mergeBusinessRules(seed, contractReport);
    const wire = buildPreflightMappings([], next);
    const fname = wire.find((m) => m.source === "fname")!;
    assert.deepEqual(
      (fname.rule_contracts || []).map((c) => c.rule_id),
      ["c1", "c2"],
    );
  });

  it("matches a contract with empty dest by source column; unmatched stays off", () => {
    const loose: RuleCompileReport = {
      ...report,
      rules: [
        {
          source_column: "status",
          dest_column: "",
          rule_text: "Must be ACTIVE or INACTIVE",
          kind: "contract",
          kind_label: "Validate contract",
          plane: "validate",
          confidence: 0.98,
          status: "executable",
          contract: { type: "in_set", values: ["ACTIVE", "INACTIVE"] },
          enforced_at_execute: true,
          rule_id: "c3",
        },
        {
          source_column: "ghost",
          dest_column: "nowhere",
          rule_text: "Must not be null",
          kind: "contract",
          kind_label: "Validate contract",
          plane: "validate",
          confidence: 0.98,
          status: "executable",
          contract: { type: "not_null" },
          enforced_at_execute: true,
          rule_id: "c4",
        },
      ],
    };
    const next = mergeBusinessRules(seed, loose);
    assert.equal(next.find((m) => m.source === "status")!.ruleContracts?.length, 1);
    assert.equal(next.some((m) => (m.ruleContracts || []).some((c) => c.rule_id === "c4")), false);
  });
});

describe("review stays review", () => {
  it("a non-executable rule on an approved mapping unapproves it on the wire", () => {
    const approved: EditableMapping[] = [
      { source: "mystery", target: "segment", confidence: 0.95, approved: true, transform: "none" },
    ];
    const next = mergeBusinessRules(approved, report);
    const mystery = next.find((m) => m.source === "mystery")!;
    assert.equal(mystery.approved, false);
    assert.equal(mystery.requiresReview, true);
    const wire = buildPreflightMappings([], next);
    assert.equal(wire[0].requires_review, true);
  });

  it("an executable rule with an unrecognized transform is review, not a direct copy", () => {
    const weird: RuleCompileReport = {
      ...report,
      rules: [
        {
          ...report.rules[0],
          rule_text: "frob the column",
          transform: "frobnicator",
          confidence: 0.99,
        },
      ],
    };
    const next = mergeBusinessRules(seed, weird);
    const fname = next.find((m) => m.source === "fname")!;
    assert.equal(fname.approved, false);
    assert.equal(fname.requiresReview, true);
    assert.ok((fname.businessRule?.issues || []).length > 0);
    const wire = buildPreflightMappings([], next);
    assert.equal(wire.find((m) => m.source === "fname")!.requires_review, true);
  });
});

describe("proofRuleClaim counts only what executes", () => {
  it("validate-only contracts are named, not counted as enforced", () => {
    const mixed: RuleCompileReport = {
      ...report,
      buckets: { executable: 4, needs_confirmation: 0, conflict: 0 },
      coverage: {
        detected: 4, executable: 4, review: 0, conflict: 0,
        writes: 2, validations: 2, contracts_enforced_at_execute: 1, percent: 100,
      },
      rules: [
        report.rules[0],
        report.rules[1],
        {
          source_column: "email", dest_column: "email",
          rule_text: "Must not be null", kind: "contract", kind_label: "Validate contract",
          plane: "validate", confidence: 0.98, status: "executable",
          contract: { type: "not_null" }, enforced_at_execute: true,
        },
        {
          source_column: "cust_id", dest_column: "cust_id",
          rule_text: "Must be unique", kind: "contract", kind_label: "Validate contract",
          plane: "validate", confidence: 0.98, status: "executable",
          contract: { type: "unique" }, enforced_at_execute: false,
          enforced_post_load: true,
        },
      ],
    };
    const claim = proofRuleClaim(mixed);
    assert.match(claim, /4 business rules/);
    assert.match(claim, /1 rule\(s\) are checked after load/);
    assert.doesNotMatch(claim, /not enforced at Run/i);
  });
});
