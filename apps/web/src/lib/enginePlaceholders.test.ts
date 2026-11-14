/** UX-31: form placeholders come from the engine being configured. */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { writeHookHint } from "./sourceReadMode.js";

describe("write hook placeholders", () => {
  it("MySQL shows no SQL Server hook", () => {
    for (const when of ["before", "after"] as const) {
      const hint = writeHookHint("mysql", when);
      assert.match(hint, /^CALL /);
      assert.doesNotMatch(hint, /EXEC|dbo\./);
    }
  });
  it("SQL Server shows EXEC, PostgreSQL a schema-qualified CALL", () => {
    assert.match(writeHookHint("sqlserver", "before"), /^EXEC dbo\./);
    assert.match(writeHookHint("postgresql", "after"), /^CALL public\./);
  });
  it("the panel and the connection form take their examples from the engine", () => {
    const src = join(dirname(fileURLToPath(import.meta.url)), "..");
    const panel = readFileSync(join(src, "components", "transfer", "DestProcedurePanel.tsx"), "utf8");
    assert.doesNotMatch(panel, /placeholder="EXEC dbo\.DisableIndexes"/);
    assert.match(panel, /writeHookHint\(destType, "before"\)/);
    const modal = readFileSync(join(src, "components", "ConnectorModal.tsx"), "utf8");
    assert.doesNotMatch(modal, /placeholder="Production Snowflake"/);
    assert.match(modal, /placeholder=\{`Production \$\{connectorDisplayName\(type\)\}`\}/);
  });
});
