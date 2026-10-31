/**
 * Run: npx --yes tsx --test apps/web/src/lib/brandDefaults.test.ts
 *
 * QA handover (9c75ada4, B1): the BigQuery dataset placeholder and default read
 * "dataflow", and Studio copy named the internal `{table}_df_staging` /
 * `_df_lsn` objects. The server owns the default dataset (a brand artifact),
 * so the UI never prefills it, and copy describes the objects instead of
 * naming internal spellings.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { defaultSchemaForDriver } from "./dialectDefaults.js";
import { getConnectorFormConfig } from "./connectorFormConfig.js";

const SRC = join(dirname(fileURLToPath(import.meta.url)), "..");

describe("brand defaults (round 11)", () => {
  it("never prefills the BigQuery dataset", () => {
    assert.equal(defaultSchemaForDriver("bigquery"), "");
    assert.equal(defaultSchemaForDriver("postgresql"), "public");
  });

  it("the BigQuery dataset placeholder is not the product name", () => {
    const text = JSON.stringify(getConnectorFormConfig("bigquery"));
    assert.doesNotMatch(text, /"placeholder":"dataflow"/);
  });

  it("Studio copy does not name internal staging or log-position objects", () => {
    for (const file of [
      "pages/TransferPage.tsx",
      "components/transfer/DestinationAdvancedDrawer.tsx",
      "components/transfer/ValidateDashboard.tsx",
      "components/schedules/ScheduleForm.tsx",
    ]) {
      const code = readFileSync(join(SRC, file), "utf8")
        .split("\n")
        .filter((line) => !/^\s*(\/\/|\/\*|\*)/.test(line))
        .join("\n");
      assert.doesNotMatch(code, /\{table\}_df_staging/, file);
      assert.doesNotMatch(code, /PK \+ _df_lsn/, file);
      assert.doesNotMatch(code, /"dataflow_test"/, file);
      assert.doesNotMatch(code, /placeholder="dataflow"/, file);
    }
  });
});
