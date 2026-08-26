import assert from "node:assert/strict";
import { test } from "node:test";
import type { ShapeColumnProfile } from "./shape";
import {
  columnFamily,
  columnFindings,
  columnsNeedingAttention,
  findingShare,
  frequentValues,
  isNumericText,
  numericHistogram,
  qualityScore,
  readsAsSummary,
} from "./transformProfile";

function profile(overrides: Partial<ShapeColumnProfile>): ShapeColumnProfile {
  return {
    name: "col",
    rows: 100,
    blanks: 0,
    non_blank: 100,
    distinct: 100,
    distinct_capped: false,
    samples: [],
    logical_type: "string",
    numeric_like: 0,
    integer_like: 0,
    max_scale: 0,
    max_integer_digits: 0,
    min: "",
    max: "",
    untrimmed: 0,
    inner_whitespace: 0,
    sentinels: {},
    non_printable: 0,
    unnormalized_unicode: 0,
    boolean_like: 0,
    scale_counts: {},
    date_formats: [],
    ambiguous_date_order: false,
    max_length: 0,
    ...overrides,
  };
}

test("a clean column charts nothing", () => {
  assert.deepEqual(columnFindings(profile({})), []);
  assert.equal(columnsNeedingAttention([profile({})]), 0);
});

test("findings are ordered widest first and each sentinel is named", () => {
  const findings = columnFindings(
    profile({ blanks: 3, untrimmed: 12, sentinels: { "N/A": 7, unknown: 1 } }),
  );

  assert.deepEqual(
    findings.map((f) => [f.label, f.count]),
    [
      ["Leading/trailing space", 12],
      ["Placeholder “N/A”", 7],
      ["Blank", 3],
      ["Placeholder “unknown”", 1],
    ],
  );
});

test("a one-row finding in a large sample still draws a visible bar", () => {
  assert.equal(findingShare(0, 1000), 0);
  assert.equal(findingShare(1, 1000), 2);
  assert.equal(findingShare(500, 1000), 50);
  assert.equal(findingShare(5, 0), 0);
  assert.equal(findingShare(2000, 1000), 100);
});

test("text that is numeric in every non-blank row is called out", () => {
  assert.equal(isNumericText(profile({ numeric_like: 100, non_blank: 100 })), true);
  assert.equal(isNumericText(profile({ numeric_like: 99, non_blank: 100 })), false);
  assert.equal(
    isNumericText(profile({ logical_type: "integer", numeric_like: 100, non_blank: 100 })),
    false,
  );
  assert.equal(isNumericText(profile({ non_blank: 0, numeric_like: 0 })), false);
});

test("column family follows the profile, not a stacked guess", () => {
  assert.equal(columnFamily(profile({ logical_type: "decimal", non_blank: 10 })), "numeric");
  assert.equal(columnFamily(profile({ logical_type: "string", numeric_like: 100, non_blank: 100 })), "numeric");
  assert.equal(columnFamily(profile({ logical_type: "date" })), "datetime");
  assert.equal(columnFamily(profile({ logical_type: "boolean", boolean_like: 100, non_blank: 100 })), "boolean");
  assert.equal(columnFamily(profile({ logical_type: "string", non_blank: 10 })), "text");
  assert.equal(columnFamily(profile({ rows: 0, non_blank: 0 })), "empty");
});

test("quality score shrinks with findings and never exceeds 100", () => {
  assert.equal(qualityScore(profile({ rows: 100 })), 100);
  assert.equal(qualityScore(profile({ rows: 100, untrimmed: 25 })), 75);
  assert.equal(qualityScore(profile({ rows: 0 })), 0);
});

test("frequent values and numeric histogram are measured from the sample", () => {
  assert.deepEqual(
    frequentValues(["NY", "NY", "LA", "NY", "LA", ""]),
    [
      { value: "NY", count: 3 },
      { value: "LA", count: 2 },
    ],
  );
  const bins = numericHistogram([22.6, 21.4, 22.0, 23, 21]);
  assert.ok(bins.length >= 1);
  assert.equal(bins.reduce((sum, bin) => sum + bin.count, 0), 5);
  assert.deepEqual(numericHistogram([10, 10, 10]), [{ label: "10", count: 3 }]);
  assert.deepEqual(numericHistogram(["1,234", "5,678"]), []);
  assert.deepEqual(numericHistogram(["$1,000.00", "$1,000.00"]), [{ label: "1000", count: 2 }]);
});

test("reads-as states scale, length and date ambiguity when they exist", () => {
  assert.equal(readsAsSummary(profile({ logical_type: "integer" })), "integer");
  assert.equal(
    readsAsSummary(profile({ logical_type: "decimal", max_scale: 6, max_length: 12 })),
    "decimal · up to 6 decimal place(s) · longest 12 char(s)",
  );
  assert.equal(
    readsAsSummary(profile({ logical_type: "date", ambiguous_date_order: true })),
    "date · ambiguous day/month order",
  );
});
