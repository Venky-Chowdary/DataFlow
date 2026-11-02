/**
 * Run: npx --yes tsx --test apps/web/src/lib/connectorSetupGuide.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { getConnectorSetupGuide } from "./connectorSetupGuide.js";
import { resolveCatalogIdToType } from "./connectorTypes.js";
import { CONNECTOR_CATALOG } from "./types.js";

const FIRST_CLASS = [
  "postgresql", "mysql", "sqlserver", "oracle", "sqlite", "mongodb",
  "snowflake", "bigquery", "redshift", "databricks", "s3", "gcs",
  "azure_blob", "adls", "sftp", "kafka", "salesforce", "hubspot",
  "stripe", "shopify", "github", "jira", "rest_api", "elasticsearch",
  "dynamodb", "redis", "clickhouse", "iceberg", "email", "duckdb",
  "neo4j", "couchbase", "influxdb", "generic_sql", "singer_tap", "excel",
];

const NEEDS_LINKS = [
  "postgresql", "mysql", "sqlserver", "oracle", "mongodb",
  "snowflake", "bigquery", "s3", "gcs", "azure_blob", "adls",
  "kafka", "salesforce", "hubspot", "stripe", "github", "jira",
  "shopify", "dynamodb", "elasticsearch", "redis", "databricks",
];

describe("connectorSetupGuide", () => {
  it("every first-class connector has a dedicated guide, not the generic fallback", () => {
    for (const id of FIRST_CLASS) {
      const guide = getConnectorSetupGuide(id);
      assert.notEqual(guide.title, "Set up this connection", id);
      assert.ok(guide.steps.length >= 3, `${id} steps`);
    }
  });

  it("credential-bearing connectors link to official vendor docs", () => {
    for (const id of NEEDS_LINKS) {
      const guide = getConnectorSetupGuide(id);
      assert.ok((guide.links ?? []).length >= 1, `${id} has no credential doc links`);
    }
  });

  it("all links are https vendor docs or internal # anchors", () => {
    for (const id of FIRST_CLASS) {
      for (const link of getConnectorSetupGuide(id).links ?? []) {
        assert.match(link.url, /^https:\/\/|^#\//, `${id}: ${link.url}`);
        assert.ok(link.label.length > 3, `${id}: ${link.label}`);
      }
    }
  });

  it("family aliases resolve to their parent's guide", () => {
    assert.match(getConnectorSetupGuide("mariadb").title, /MySQL/);
    assert.match(getConnectorSetupGuide("pgvector").title, /PostgreSQL/);
    assert.match(getConnectorSetupGuide("supabase").title, /PostgreSQL/);
    assert.match(getConnectorSetupGuide("google_cloud_storage").title, /Google/);
    assert.match(getConnectorSetupGuide("parquet").title, /file/i);
    assert.match(getConnectorSetupGuide("excel").title, /Excel/);
  });

  it("every catalog tile resolves to a guide without throwing", () => {
    for (const tile of CONNECTOR_CATALOG) {
      const resolved = resolveCatalogIdToType(tile.id);
      const guide = getConnectorSetupGuide(tile.id, resolved);
      assert.ok(guide.steps.length >= 3, tile.id);
    }
  });
});
