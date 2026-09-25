/**
 * Run: npx --yes tsx --test apps/web/src/lib/studioPicker.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import {
  filterStudioOptions,
  groupStudioOptions,
  studioPickerSelection,
  type StudioPickerOption,
} from "./studioPicker";

const options: StudioPickerOption[] = [
  { value: "trim", label: "Trim whitespace", hint: "trim", group: "Values", meta: "value" },
  { value: "parse_date", label: "Parse a date", hint: "parse_date", group: "Values" },
  { value: "filter_rows", label: "Keep matching rows", hint: "filter_rows", group: "Row count", meta: "moves ledger" },
  { value: "email", label: "email", group: "" },
];

describe("studioPicker", () => {
  it("ranks exact, prefix, then contains", () => {
    assert.equal(filterStudioOptions(options, "trim")[0].value, "trim");
    assert.equal(filterStudioOptions(options, "parse")[0].value, "parse_date");
    assert.ok(filterStudioOptions(options, "rows").some((opt) => opt.value === "filter_rows"));
  });

  it("returns the catalog unfiltered when the query is blank", () => {
    assert.equal(filterStudioOptions(options, "  ").length, options.length);
  });

  it("groups in first-seen order and keeps an ungrouped bucket", () => {
    const grouped = groupStudioOptions(options);
    assert.deepEqual(grouped.map((item) => item.group), ["Values", "Row count", ""]);
    assert.equal(studioPickerSelection(options, "PARSE_DATE")?.label, "Parse a date");
  });
});

describe("Transform step builder uses the studio picker", () => {
  it("does not render a native operation select", () => {
    const webRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
    const builder = readFileSync(join(webRoot, "components/transfer/TransformStepBuilder.tsx"), "utf8");
    assert.match(builder, /StudioPicker/);
    assert.match(builder, /StudioMultiPicker/);
    assert.doesNotMatch(builder, /<select/);
    assert.match(builder, /disabled=\{!canPlan \|\| !operation \|\| Boolean\(missing\)/);
    assert.match(builder, /is-invalid/);
    assert.match(builder, /role="alert"/);
    assert.match(builder, /settleExpressionCheck/);
    assert.match(builder, /parseNumberOption/);
  });

  it("the multi-column picker owns the same listbox keys as the single picker", () => {
    const webRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
    const picker = readFileSync(join(webRoot, "components/ui/StudioPicker.tsx"), "utf8");
    const multi = picker.slice(picker.indexOf("export function StudioMultiPicker"));
    assert.match(multi, /const onKeyDown/);
    assert.match(multi, /Escape/);
    assert.match(multi, /ArrowDown/);
    assert.match(multi, /ArrowUp/);
    assert.match(multi, /Enter/);
    assert.match(multi, /onKeyDown=\{onKeyDown\}/);
  });
});
