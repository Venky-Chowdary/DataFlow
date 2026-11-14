/** UX-39: "Route ready · convert", "Export file as `json`", "Live route: csv → json". */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { routeFormatLabel, routeMessageText, routeOperationLabel, routePlanItemText } from "./routeLabels.js";

describe("route labels", () => {
  it("names formats and engines", () => {
    assert.equal(routeFormatLabel("json"), "JSON");
    assert.equal(routeFormatLabel("parquet"), "Parquet");
    assert.equal(routeFormatLabel("mysql"), "MySQL");
  });
  it("says what the operation does", () => {
    assert.equal(routeOperationLabel("convert"), "export to a file");
    assert.equal(routeOperationLabel("migration"), "copy between databases");
  });
  it("drops backticks and labels formats in plan lines", () => {
    assert.equal(routePlanItemText("Export file as `json` in server exports folder"), "Export file as JSON in server exports folder");
    assert.equal(routePlanItemText("MySQL table `w.t` with typed columns"), "MySQL table w.t with typed columns");
  });
  it("labels the live-route toast", () => {
    assert.equal(routeMessageText("Live route: csv → json"), "Live route: CSV → JSON");
    assert.equal(routeMessageText("Live route: postgresql → mysql"), "Live route: PostgreSQL → MySQL");
    assert.equal(routeMessageText("Combination x → y not yet live"), "Combination x → y not yet live");
  });
  it("Studio uses them", () => {
    const studio = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "TransferPage.tsx"), "utf8");
    assert.match(studio, /routeOperationLabel\(transferPlan\.operation\)/);
    assert.match(studio, /routePlanItemText\(item\)/);
    assert.match(studio, /routeMessageText\(plan\.message\)/);
  });
  it("a file export does not show table-schema rules", () => {
    const studio = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "TransferPage.tsx"), "utf8");
    assert.match(studio, /\{destKindMode === "database" \? \(\s*<>\s*<p className="df2-label-hint">\s*\{schemaPolicyHonestyLine\(schemaPolicy\)\}/);
  });
});
