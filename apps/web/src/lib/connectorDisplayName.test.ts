import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { connectorDisplayName } from "./connectorTypes";

describe("connectorDisplayName", () => {
  it("uses vendor spelling for the ids public pages render", () => {
    assert.equal(connectorDisplayName("postgresql"), "PostgreSQL");
    assert.equal(connectorDisplayName("mysql"), "MySQL");
    assert.equal(connectorDisplayName("mongodb"), "MongoDB");
    assert.equal(connectorDisplayName("bigquery"), "BigQuery");
    assert.equal(connectorDisplayName("clickhouse"), "ClickHouse");
  });

  it("never leaves an underscore or a lowercase acronym in a rendered label", () => {
    for (const id of ["generic_sql", "sql_server", "s3", "csv", "json", "gcs"]) {
      const label = connectorDisplayName(id);
      assert.ok(!label.includes("_"), `${id} rendered "${label}" with an underscore`);
      assert.ok(label.length > 0, `${id} rendered an empty label`);
    }
    assert.match(connectorDisplayName("generic_sql"), /SQL/);
  });
});
