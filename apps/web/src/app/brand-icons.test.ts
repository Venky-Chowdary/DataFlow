import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { connectorBrandMark, connectorIconKey } from "./brand-icons";

const appDir = dirname(fileURLToPath(import.meta.url));

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
  assert.equal(connectorIconKey("sql_server"), "sqlserver");
  assert.equal(connectorIconKey("google_cloud_storage"), "gcs");
  assert.equal(connectorBrandMark("gcs")?.title, "Google Cloud Storage");
});

test("every DB-family catalog tile resolves to a real mark, never a generic glyph", () => {
  const catalog = JSON.parse(
    readFileSync(join(appDir, "../../../../apps/api/data/connector_catalog.json"), "utf8"),
  ) as { connectors: Array<{ id: string; name: string; category: string }> };
  const dbFamily = new Set([
    "database",
    "vector",
    "warehouse",
    "lake",
    "streaming",
    "cloud_storage",
  ]);
  const genericKeys = new Set(["file", "api", "database"]);
  const misses: string[] = [];
  for (const c of catalog.connectors) {
    if (!dbFamily.has(c.category)) continue;
    const key = connectorIconKey(c.id);
    if (genericKeys.has(key)) {
      misses.push(`${c.id} (${c.name}, ${c.category}) -> ${key}`);
    }
  }
  assert.deepEqual(
    misses,
    [],
    `DB-family catalog tiles fell back to a generic glyph:\n${misses.join("\n")}`,
  );
});

test("region/instance variants keep the parent mark", () => {
  const cases: Array<[string, string]> = [
    ["snowflake_aws", "snowflake"],
    ["snowflake_b1fcb2", "snowflake"],
    ["bigquery_eu", "bigquery"],
    ["redshift_serverless", "redshift"],
    ["mongodb_atlas", "mongodb"],
    ["dynamodb_global_tables", "dynamodb"],
    ["postgresql_rds", "postgresql"],
    ["mysql_planetscale", "planetscale"],
    ["postgresql_neon", "neon"],
    ["azure_synapse_dedicated", "synapse"],
    ["google_pub_sub", "pubsub"],
    ["s3_us_east_1", "s3"],
    ["amazon_elasticsearch", "opensearch"],
    ["cloudera_data_platform", "cloudera"],
    ["salesforce_(us_retail)_81ebef", "salesforce"],
    ["ibm_db2_warehouse", "db2"],
    ["oracle_autonomous_warehouse", "oracle"],
    ["amazon_rds_sql_server", "sqlserver"],
    ["google_cloud_sql_sql_server", "sqlserver"],
    ["azure_sql_database", "sqlserver"],
    ["amazon_rds_oracle", "oracle"],
    ["cassandra", "cassandra"],
    ["scylladb", "scylladb"],
    ["azure_cosmos_db", "cosmos_db"],
    ["apache_kafka", "kafka"],
    ["google_pubsub", "pubsub"],
    ["teradata_vantage", "teradata"],
    ["motherduck", "duckdb"],
    ["greenplum", "postgresql"],
    ["oceanbase", "mysql"],
  ];
  for (const [id, expected] of cases) {
    assert.equal(connectorIconKey(id), expected, `${id} -> ${connectorIconKey(id)}`);
  }
});

test("web connector-catalog types never render the default File glyph", () => {
  const types = [
    "postgresql", "pgvector", "qdrant", "weaviate", "pinecone", "milvus",
    "mysql", "mariadb", "sqlserver", "oracle", "sqlite", "cockroachdb",
    "singlestore", "timescaledb", "supabase", "mongodb", "dynamodb",
    "openshift", "cassandra", "couchbase", "redis", "neo4j", "elasticsearch",
    "firebase", "snowflake", "bigquery", "redshift", "databricks", "synapse",
    "teradata", "vertica", "firebolt", "clickhouse", "duckdb", "iceberg",
    "trino", "hive", "druid", "kinesis", "pubsub", "rabbitmq", "pulsar",
    "salesforce", "hubspot", "stripe", "shopify", "github", "jira",
    "intercom", "graphql", "s3", "gcs", "azure_blob", "adls",
  ];
  for (const t of types) {
    const key = connectorIconKey(t);
    assert.notEqual(key, "file", `${t} -> default file glyph`);
  }
  assert.equal(connectorIconKey("sqlserver"), "sqlserver");
  assert.equal(connectorIconKey("oracle"), "oracle");
  assert.equal(connectorIconKey("s3"), "s3");
});
