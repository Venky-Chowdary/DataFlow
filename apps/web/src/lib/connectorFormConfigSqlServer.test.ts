/**
 * NF-2: the SQL Server form names the driver choice and labels FreeTDS as
 * unverified TLS; Entra ID sign-in is offered.
 * Run: npx --yes tsx --test apps/web/src/lib/connectorFormConfigSqlServer.test.ts
 */
import assert from "node:assert/strict";
import test from "node:test";

import { getConnectorFormConfig } from "./connectorFormConfig.ts";

function userPassFields(type: string) {
  const cfg = getConnectorFormConfig(type);
  const mode = cfg.authModes.find((m) => m.value === "user_pass");
  assert.ok(mode, `${type} has a user_pass mode`);
  return mode.fields;
}

test("SQL Server labels FreeTDS as TLS not verified", () => {
  const driver = userPassFields("sqlserver").find((f) => f.key === "sqlserver_driver");
  assert.ok(driver, "driver field");
  assert.equal(driver.optional, true);
  assert.match(driver.hint ?? "", /freetds: FreeTDS — TLS certificate NOT verified/);
  assert.match(driver.hint ?? "", /verified TLS/);
});

test("SQL Server offers Microsoft Entra ID sign-in modes", () => {
  const auth = userPassFields("sqlserver").find((f) => f.key === "authentication");
  assert.ok(auth, "authentication field");
  for (const mode of ["entra_service_principal", "entra_managed_identity", "entra_access_token"]) {
    assert.match(auth.hint ?? "", new RegExp(mode));
  }
});

test("Other SQL engines do not show the SQL Server driver choice", () => {
  assert.equal(userPassFields("postgresql").some((f) => f.key === "sqlserver_driver"), false);
});

test("Entra ID managed identity needs no password; an access token needs one", () => {
  const cfg = getConnectorFormConfig("sqlserver");
  const mode = cfg.authModes.find((m) => m.value === "user_pass");
  assert.ok(mode);
  const base = { host: "sql.example.test", port: 1433, database: "d" };
  assert.equal(mode.validate({ ...base, authentication: "entra_managed_identity" }), null);
  assert.match(mode.validate({ ...base, authentication: "entra_access_token" }) ?? "", /access token/);
  assert.equal(mode.validate({ ...base, authentication: "entra_access_token", password: "tok" }), null);
  assert.match(mode.validate({ ...base }) ?? "", /Username and password are required/);
});
