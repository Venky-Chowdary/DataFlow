import assert from "node:assert/strict";
import test from "node:test";

import { connectorBrandMark, connectorIconKey } from "./brand-icons";

test("postgres and mysql use their own product marks", () => {
  const postgres = connectorBrandMark("postgres");
  const mysql = connectorBrandMark("mysql");
  assert.ok(postgres);
  assert.ok(mysql);
  assert.equal(postgres.title, "PostgreSQL");
  assert.equal(mysql.title, "MySQL");
  assert.notEqual(postgres.path, mysql.path);
  assert.equal(connectorBrandMark("postgresql")?.path, postgres.path);
});

test("catalog aliases share the product mark", () => {
  assert.equal(connectorIconKey("amazon_rds_mysql"), "mysql");
  assert.equal(connectorIconKey("mssql"), "sqlserver");
  assert.equal(connectorIconKey("google_cloud_storage"), "gcs");
  assert.equal(connectorBrandMark("gcs")?.title, "Google Cloud Storage");
});
