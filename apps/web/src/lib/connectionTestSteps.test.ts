/**
 * Connection test as a step checklist (UI pass 2, item 4).
 * Run: npx tsx --test apps/web/src/lib/connectionTestSteps.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ConnectionTestChecklist } from "../components/connections/ConnectionTestChecklist.tsx";
import { connectionTestSteps, grantFixes } from "./connectionTestSteps.ts";

const states = (r: Parameters<typeof connectionTestSteps>[0]) => connectionTestSteps(r).map((s) => `${s.id}:${s.state}`);

describe("connectionTestSteps: where the test stopped, from the API's error code", () => {
  it("a success passes reach and sign-in and never claims catalog or grants", () => {
    assert.deepEqual(states({ success: true }), ["reach:passed", "sign_in:passed", "catalog:not_checked", "privileges:not_checked"]);
  });
  it("stops at the failing step; later steps did not run", () => {
    assert.deepEqual(states({ success: false, code: "network.connect_timeout" }), ["reach:failed", "sign_in:not_run", "catalog:not_run", "privileges:not_run"]);
    assert.deepEqual(states({ success: false, code: "network.tls_untrusted" })[0], "reach:failed");
    assert.deepEqual(states({ success: false, code: "auth.password_rejected" }), ["reach:passed", "sign_in:failed", "catalog:not_run", "privileges:not_run"]);
    assert.deepEqual(states({ success: false, code: "access.database_not_granted" }), ["reach:passed", "sign_in:passed", "catalog:failed", "privileges:not_run"]);
    assert.deepEqual(states({ success: false, code: "object.schema_not_found" })[2], "catalog:failed");
    assert.deepEqual(states({ success: false, code: "access.denied" }), ["reach:passed", "sign_in:passed", "catalog:passed", "privileges:failed"]);
  });
  it("does not guess when the API did not say", () => {
    assert.deepEqual(states({ success: false, code: "unclassified" }), ["reach:unknown", "sign_in:not_run", "catalog:not_run", "privileges:not_run"]);
  });
});

describe("grantFixes", () => {
  it("quotes what the person typed for the engine", () => {
    const pg = grantFixes({ type: "postgresql", username: 'etl"user', database: "sales", schema: "raw" });
    assert.equal(pg.length, 2);
    assert.match(pg[0].sql, /GRANT USAGE ON SCHEMA "raw" TO "etl""user";/);
    assert.match(pg[1].sql, /GRANT USAGE, CREATE ON SCHEMA "raw" TO "etl""user";/);
    const my = grantFixes({ type: "mysql", username: "o'brien", database: "shop" });
    assert.match(my[1].sql, /ON `shop`\.\* TO 'o''brien'@'%';/);
    assert.match(grantFixes({ type: "sqlserver", username: "etl", database: "dw" })[1].sql, /GRANT ALTER ON SCHEMA::\[dbo\] TO \[etl\];/);
    assert.match(grantFixes({ type: "snowflake", role: "LOADER", database: "DW", warehouse: "WH" })[0].sql, /GRANT USAGE ON WAREHOUSE "WH" TO ROLE "LOADER";/);
  });
  it("uses placeholders for what is missing and stays empty for non-SQL sources", () => {
    assert.match(grantFixes({ type: "postgresql" })[0].sql, /TO "<user>";/);
    assert.deepEqual(grantFixes({ type: "csv" }), []);
  });
});

describe("ConnectionTestChecklist", () => {
  it("names every step's state in words and opens the grants on a permission failure", () => {
    const html = renderToStaticMarkup(React.createElement(ConnectionTestChecklist, {
      result: { success: false, code: "access.denied" },
      connection: { type: "postgresql", username: "etl", database: "sales" },
    }));
    assert.match(html, /Reach host<\/span><span class="df2-ctest-word">Passed/);
    assert.match(html, /Privileges<\/span><span class="df2-ctest-word">Failed/);
    assert.match(html, /<details class="df2-ctest-grants" open="">/);
    assert.match(html, /aria-label="Copy grants to write \(destination\)"/);
    const ok = renderToStaticMarkup(React.createElement(ConnectionTestChecklist, { result: { success: true }, connection: { type: "postgresql" } }));
    assert.match(ok, /Not checked by Test/);
    assert.match(ok, /<details class="df2-ctest-grants">/);
  });
});
