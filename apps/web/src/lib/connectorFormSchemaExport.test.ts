/**
 * Run: npx --yes tsx --test apps/web/src/lib/connectorFormSchemaExport.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { exportConnectorFormSchema, serializeConnectorFormSchema } from "./connectorFormSchemaExport.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SCHEMA_PATH = path.resolve(HERE, "..", "..", "..", "api", "data", "connector_form_schema.json");

describe("connector form schema export", () => {
  it("committed apps/api/data/connector_form_schema.json matches connectorFormConfig.ts", () => {
    const committed = readFileSync(SCHEMA_PATH, "utf8");
    assert.equal(
      committed,
      serializeConnectorFormSchema(),
      "stale export — run `npx tsx scripts/export_connector_form_schema.ts` in apps/web",
    );
  });

  it("keeps the fields Pilot explains for the transfer-ready engines", () => {
    const schema = exportConnectorFormSchema();
    const snowflake = schema.snowflake.auth_modes.find((m) => m.value === "user_pass");
    assert.ok(snowflake);
    assert.deepEqual(
      snowflake.fields.map((f) => f.key),
      ["host", "username", "password", "database", "schema", "warehouse", "authRole"],
    );
    assert.equal(snowflake.fields.find((f) => f.key === "authRole")?.optional, true);
    assert.ok(schema.bigquery.auth_modes.some((m) => m.value === "service_account"));
    assert.ok(schema.mongodb.auth_modes[0].fields.some((f) => f.key === "authSource"));
    assert.ok(schema.snowflake.setup_steps.length >= 3);
    assert.deepEqual(schema.postgresql.setup_steps, [], "generic guide is not exported as engine guidance");
    const sqlserverPort = schema.sqlserver.auth_modes[0].fields.find((f) => f.key === "port");
    assert.equal(sqlserverPort?.placeholder, "1433");
  });

  it("never exports validators or secret values, only field metadata", () => {
    for (const form of Object.values(exportConnectorFormSchema())) {
      for (const mode of form.auth_modes) {
        for (const field of mode.fields) {
          assert.deepEqual(
            Object.keys(field).filter((k) => !["key", "label", "type", "optional", "sensitive", "hint", "placeholder"].includes(k)),
            [],
          );
        }
      }
    }
  });
});
