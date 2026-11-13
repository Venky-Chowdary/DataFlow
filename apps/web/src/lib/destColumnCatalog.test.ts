import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  catalogFromApi,
  catalogFromNames,
  destinationColumnStates,
  filterDestColumns,
  mapTargetIssues,
  normalizeColumnName,
  remapToDestination,
  summarizeMapTargets,
  targetConflicts,
} from "./destColumnCatalog";
import type { EditableMapping } from "./mapping";

const API_CATALOG = {
  version: "dest_column_catalog.v1",
  connected: true,
  table_exists: true,
  dest_type: "postgresql",
  nullability_measured: true,
  can_add_columns: true,
  add_columns_reason: "The writer adds the column (ADD COLUMN) before the first row is written.",
  message: "",
  columns: [
    { name: "legacy_ref", ordinal: 0, type: "BIGINT", nullable: false, default: null, has_default: false, primary_key: true, identity: false, generated: false, required: true },
    { name: "given_name", ordinal: 1, type: "VARCHAR(40)", nullable: false, default: null, has_default: false, primary_key: false, identity: false, generated: false, required: true },
    { name: "surname", ordinal: 2, type: "VARCHAR(60)", nullable: true, default: null, has_default: false, primary_key: false, identity: false, generated: false, required: false },
    { name: "amount", ordinal: 3, type: "DECIMAL(12,2)", nullable: false, default: "0", has_default: true, primary_key: false, identity: false, generated: false, required: false },
    { name: "row_id", ordinal: 4, type: "BIGINT", nullable: false, default: null, has_default: false, primary_key: false, identity: true, generated: false, required: false },
  ],
};

function row(source: string, target: string, extra: Partial<EditableMapping> = {}): EditableMapping {
  return {
    source,
    target,
    confidence: 0.9,
    inferredType: "VARCHAR(40)",
    approved: true,
    transform: "none",
    ...extra,
  };
}

describe("destination column catalog", () => {
  const catalog = catalogFromApi(API_CATALOG);

  it("reads every column with type, nullability, default, key and required", () => {
    assert.equal(catalog.columns.length, 5);
    const ref = catalog.columns[0];
    assert.equal(ref.type, "BIGINT");
    assert.equal(ref.primaryKey, true);
    assert.equal(ref.required, true);
    assert.equal(catalog.columns[3].default, "0");
    assert.equal(catalog.columns[3].hasDefault, true);
    assert.equal(catalog.origin, "api");
  });

  it("names-only fallback never calls a column required or optional", () => {
    const names = catalogFromNames(["a", "b"], { a: "INT" }, true);
    assert.equal(names.columns[0].type, "INT");
    assert.equal(names.columns[0].required, null);
    assert.equal(names.columns[0].nullable, null);
    assert.equal(names.canAddColumns, false);
    assert.equal(catalogFromNames([], {}, false).canAddColumns, true);
  });

  it("states: mapped, required-unmapped (blocking), default, NULL, engine-filled", () => {
    const states = destinationColumnStates(catalog, [
      row("order_id", "legacy_ref"),
      row("first_name", "surname"),
    ]);
    const by = Object.fromEntries(states.map((s) => [s.column.name, s]));
    assert.equal(by.legacy_ref.status, "mapped");
    assert.deepEqual(by.legacy_ref.sources, ["order_id"]);
    assert.equal(by.given_name.status, "required_unmapped");
    assert.equal(by.given_name.blocking, true);
    assert.match(by.given_name.explanation, /NOT NULL, no default: blocks Validate/);
    assert.equal(by.amount.status, "default_unmapped");
    assert.match(by.amount.explanation, /default \(0\)/);
    assert.equal(by.row_id.status, "engine_unmapped");
    assert.equal(by.row_id.blocking, false);
  });

  it("one source per target: two writers are a blocking conflict", () => {
    const rows = [row("first_name", "given_name"), row("name", "GIVEN_NAME"), row("x", "", { transform: "omit" })];
    const conflicts = targetConflicts(rows);
    assert.deepEqual(conflicts.get("given_name"), ["first_name", "name"]);
    const state = destinationColumnStates(catalog, rows).find((s) => s.column.name === "given_name");
    assert.equal(state?.status, "conflict");
    assert.equal(state?.blocking, true);
  });

  it("omitted rows write nothing", () => {
    const states = destinationColumnStates(catalog, [row("first_name", "given_name", { transform: "omit" })]);
    assert.equal(states.find((s) => s.column.name === "given_name")?.status, "required_unmapped");
  });

  it("summary counts new targets, and refuses them only when the API says it cannot add", () => {
    const rows = [row("order_id", "legacy_ref"), row("note", "brand_new")];
    assert.deepEqual(summarizeMapTargets(catalog, rows).newTargets, ["brand_new"]);
    assert.deepEqual(summarizeMapTargets(catalog, rows).refusedNewTargets, []);
    const locked = catalogFromApi({ ...API_CATALOG, can_add_columns: false });
    assert.deepEqual(summarizeMapTargets(locked, rows).refusedNewTargets, ["brand_new"]);
  });
});

describe("remapToDestination", () => {
  const catalog = catalogFromApi(API_CATALOG);

  it("binds an existing column to its physical type and clears stale approval", () => {
    const before = row("order_id", "order_id", {
      inferredType: "BIGINT",
      destType: "INTEGER",
      createNew: true,
      existsInDestination: false,
      assignmentStrategy: "create_compatible_new",
      riskAcknowledged: true,
      fidelity: "preserve",
    });
    const after = remapToDestination(before, "LEGACY_REF", catalog);
    assert.equal(after.target, "legacy_ref", "canonical spelling from the catalog");
    assert.equal(after.destType, "BIGINT");
    assert.equal(after.existsInDestination, true);
    assert.equal(after.createNew, false);
    assert.equal(after.assignmentStrategy, "bind_existing");
    assert.equal(after.approved, false);
    assert.equal(after.riskAcknowledged, false);
    assert.equal(after.fidelity, undefined);
    assert.equal(after.operatorTarget, true);
  });

  it("flags a narrowing target so Approve cannot clear it silently", () => {
    const after = remapToDestination(row("note", "", { inferredType: "VARCHAR(255)" }), "given_name", catalog);
    assert.equal(after.destType, "VARCHAR(40)");
    assert.equal(after.fidelity, "lossy_cast");
    assert.equal(after.typeNarrowing, true);
  });

  it("a name the table lacks becomes a create-new column", () => {
    const after = remapToDestination(row("note", "", { inferredType: "TEXT" }), "notes_text", catalog);
    assert.equal(after.existsInDestination, false);
    assert.equal(after.createNew, true);
    assert.equal(after.assignmentStrategy, "create_compatible_new");
    assert.equal(after.destType, "TEXT");
  });

  it("an empty target unmaps the row", () => {
    const after = remapToDestination(row("note", "surname"), "", catalog);
    assert.equal(after.target, "");
    assert.equal(after.operatorTarget, false);
  });
});

describe("picker search", () => {
  const cols = catalogFromApi(API_CATALOG).columns;
  it("ranks exact, prefix, substring, then word-normalized", () => {
    assert.deepEqual(filterDestColumns(cols, "surname").map((c) => c.name), ["surname"]);
    assert.deepEqual(filterDestColumns(cols, "givenname").map((c) => c.name), ["given_name"]);
    assert.deepEqual(filterDestColumns(cols, "ref").map((c) => c.name), ["legacy_ref"]);
    assert.equal(filterDestColumns(cols, "").length, cols.length);
  });
});

describe("Map target issues (what Validate blocks on)", () => {
  const catalog = catalogFromApi(API_CATALOG);
  it("lists conflicts, required columns, refused names and rows with no destination", () => {
    const rows = [
      row("first_name", "surname"),
      row("last_name", "surname"),
      row("order_id", "legacy_ref"),
      row("note", ""),
    ];
    const issues = mapTargetIssues(summarizeMapTargets(catalog, rows), rows);
    const codes = issues.map((i) => i.code).sort();
    assert.deepEqual(codes, ["no_target", "required_unmapped", "target_conflict"]);
    assert.match(issues.find((i) => i.code === "required_unmapped")!.title, /given_name/);
  });

  it("an overwrite that recreates the table does not hold the live NOT NULL columns", () => {
    const rows = [row("order_id", "legacy_ref")];
    const issues = mapTargetIssues(summarizeMapTargets(catalog, rows), rows, { tableRecreated: true });
    assert.deepEqual(issues, []);
  });
});

describe("normalizeColumnName matches the API's normalize_name", () => {
  for (const [name, norm] of [
    ["OrderID", "order_id"],
    ["firstName", "first_name"],
    ["First Name", "first_name"],
    ["total-balance", "total_balance"],
    ["HTTPStatusCode", "http_status_code"],
    ["Col1Value", "col_1_value"],
  ] as const) {
    it(`${name} → ${norm}`, () => assert.equal(normalizeColumnName(name), norm));
  }
});
