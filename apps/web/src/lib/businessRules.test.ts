import { describe, expect, it } from "vitest";
import {
  mergeBusinessRules,
  mergeCompiledShapeSteps,
  ruleReportSummary,
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
    expect(fname.target).toBe("first_name");
    expect(fname.approved).toBe(true);
    expect(fname.businessRule?.kind).toBe("direct");
    expect(fname.businessRule?.row).toBe(2);

    const status = next.find((m) => m.source === "status")!;
    expect(status.codeCrosswalk).toEqual({ A: "ACTIVE", I: "INACTIVE" });
    expect(status.approved).toBe(true);

    const mystery = next.find((m) => m.source === "mystery")!;
    expect(mystery.approved).toBe(false);
    expect(mystery.requiresReview).toBe(true);
    expect(mystery.businessRule?.status).toBe("needs_confirmation");
    expect(mystery.target).toBe("");
  });

  it("does not invent a dest column for unused dests", () => {
    const next = mergeBusinessRules(seed, report);
    expect(next.some((m) => m.target === "unused_flag")).toBe(false);
  });

  it("summarises unused dest columns as not written", () => {
    expect(ruleReportSummary(report)).toContain("2 executable");
    expect(ruleReportSummary(report)).toContain("1 dest column(s) unused");
  });

  it("does not overwrite an operator-locked dest", () => {
    const locked: EditableMapping[] = [
      { source: "fname", target: "given_name", confidence: 0.9, approved: true, transform: "none" },
    ];
    const next = mergeBusinessRules(locked, report);
    expect(next[0].target).toBe("given_name");
    expect(next[0].businessRule?.status).toBe("conflict");
    expect(next[0].requiresReview).toBe(true);
  });

  it("merges compiled shape steps without dropping operator steps", () => {
    const existing = [{ op: "trim", column: "email", enabled: true }];
    const compiled = [
      { op: "trim", column: "email", enabled: true },
      { op: "derive_column", options: { to: "annual_salary", expression: "salary * 12" }, enabled: true },
    ];
    const next = mergeCompiledShapeSteps(existing, compiled);
    expect(next).toHaveLength(2);
    expect(next[1].op).toBe("derive_column");
  });
});
