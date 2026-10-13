/**
 * Run: npx --yes tsx --test apps/web/src/lib/studioPicker.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  filterStudioOptions,
  groupStudioOptions,
  studioPickerKey,
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

describe("studio picker keyboard", () => {
  it("opens on ArrowDown, moves, and chooses the active option on Enter", () => {
    assert.deepEqual(
      studioPickerKey({ key: "ArrowDown", open: false, activeIndex: 0, optionCount: 3, spaceChooses: false }),
      { type: "open" },
    );
    assert.deepEqual(
      studioPickerKey({ key: "ArrowDown", open: true, activeIndex: 1, optionCount: 3, spaceChooses: false }),
      { type: "move", index: 2 },
    );
    assert.deepEqual(
      studioPickerKey({ key: "ArrowUp", open: true, activeIndex: 0, optionCount: 3, spaceChooses: false }),
      { type: "move", index: 0 },
    );
    assert.deepEqual(
      studioPickerKey({ key: "Enter", open: true, activeIndex: 1, optionCount: 3, spaceChooses: false }),
      { type: "choose", index: 1 },
    );
  });

  it("ignores Enter when the menu is closed and Space unless the caller allows it", () => {
    assert.equal(
      studioPickerKey({ key: "Enter", open: false, activeIndex: 0, optionCount: 3, spaceChooses: true }).type,
      "ignore",
    );
    assert.equal(
      studioPickerKey({ key: " ", open: true, activeIndex: 0, optionCount: 2, spaceChooses: false }).type,
      "ignore",
    );
    assert.deepEqual(
      studioPickerKey({ key: " ", open: true, activeIndex: 1, optionCount: 2, spaceChooses: true }),
      { type: "choose", index: 1 },
    );
  });

  it("closes on Escape and does not choose past the last option", () => {
    assert.equal(
      studioPickerKey({ key: "Escape", open: true, activeIndex: 0, optionCount: 2, spaceChooses: false }).type,
      "close",
    );
    assert.equal(
      studioPickerKey({ key: "Enter", open: true, activeIndex: 4, optionCount: 2, spaceChooses: false }).type,
      "ignore",
    );
    assert.deepEqual(
      studioPickerKey({ key: "ArrowDown", open: true, activeIndex: 2, optionCount: 3, spaceChooses: false }),
      { type: "move", index: 2 },
    );
  });
});
