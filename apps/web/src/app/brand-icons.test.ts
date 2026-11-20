import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  connectorBrandMark,
  connectorIconKey,
  connectorIconKind,
  connectorMarks,
  connectorMonogram,
  dolphinOnly,
  markViewBox,
} from "./brand-icons";
import { ALL_CONNECTORS } from "../lib/provenEvidence";

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

test("no hand-drawn vendor marks: the only path drawn is a Simple Icons path", () => {
  const src = readFileSync(join(appDir, "brand-icons.tsx"), "utf8");
  assert.equal((src.match(/<path\b/g) ?? []).length, 1, "a second <path> means a drawn mark");
  assert.match(src, /<path d=\{mark\.path\}/);
  for (const shape of ["<ellipse", "<circle", "<polygon", "<polyline", "<line "]) {
    assert.ok(!src.includes(shape), `${shape} in brand-icons.tsx: draw no lookalikes`);
  }
  assert.ok(!/drawnMark|DrawnProduct/.test(src));
});

test("every connector on a public page is a real mark, a monogram or a non-vendor glyph", () => {
  // Things that are not a vendor's product: they may use a plain glyph.
  const generic = new Set(["generic_sql", "sftp", "csv", "tsv", "fixed_width", "pdf", "rest_api"]);
  const wrong: string[] = [];
  for (const c of ALL_CONNECTORS) {
    const kind = connectorIconKind(c.driver);
    if (generic.has(c.driver) ? kind !== "generic" : kind === "generic") wrong.push(`${c.driver} -> ${kind}`);
  }
  assert.deepEqual(wrong, []);
});

test("public-page connectors use the Simple Icons mark wherever one exists", () => {
  const cases: Array<[string, string]> = [
    ["postgresql", "postgresql"], ["mysql", "mysql"], ["mongodb", "mongodb"], ["json", "json"],
    ["jsonl", "json"], ["xml", "xml"], ["yaml", "yaml"], ["html", "html5"], ["parquet", "apacheparquet"],
    ["rest_openapi", "openapiinitiative"], ["google_sheets", "googlesheets"], ["Cursor", "cursor"],
    ["Claude Desktop", "claude"],
  ];
  for (const [id, slug] of cases) assert.equal(connectorBrandMark(id)?.slug, slug, id);
  for (const id of ["sqlserver", "oracle", "s3", "salesforce", "dynamodb", "adls", "excel", "docx",
    "iceberg", "pinecone", "weaviate", "redshift", "vertica", "athena", "singer_tap", "VS Code"]) {
    assert.equal(connectorIconKind(id), "monogram", id);
  }
});

test("monograms follow one rule", () => {
  const cases: Array<[string, string]> = [
    ["Oracle", "O"], ["SQL Server", "SQL"], ["Amazon S3", "S3"], ["Amazon DynamoDB", "D"],
    ["Microsoft Azure", "A"], ["IBM Db2", "Db2"], ["Azure Cosmos DB", "CD"], ["VS Code", "VS"],
    ["Apache Iceberg", "I"], ["Amazon EMR", "EMR"],
  ];
  for (const [title, mono] of cases) assert.equal(connectorMonogram(title), mono, title);
});

test("the MySQL mark is the dolphin alone, cut from the official path", () => {
  const mysql = connectorBrandMark("mysql");
  assert.ok(mysql);
  assert.notEqual(mysql.path, "");
  assert.ok(mysql.path.length < 2500, "the wordmark subpaths are gone");
  assert.equal(mysql.path.split(/(?=M)/).length, 2, "eye and body");
  assert.equal(dolphinOnly("M1 14h2v2z" + "m0-10h1v1z"), "M1 4h1v1z");
});

test("every mark has a measured box and one optical size", () => {
  const src = readFileSync(join(appDir, "brand-icons.tsx"), "utf8");
  for (const m of connectorMarks()) {
    const row = new RegExp(`\\n  "?${m.slug}"?: \\[([^\\]]+)\\]`).exec(src);
    assert.ok(row, `${m.slug} has no box: measure it`);
    const [x, y, w, h] = row[1].split(",").map(Number);
    assert.ok(x >= 0 && y >= 0 && x + w <= 24.01 && y + h <= 24.01 && w > 0 && h > 0, `${m.slug} box ${row[1]}`);
  }
  // A square glyph shows at 20 of 24 units; a tall one fills the height.
  assert.equal(markViewBox([0, 0, 24, 24]), "-2.4 -2.4 28.8 28.8");
  assert.equal(markViewBox([6.61, 0, 10.78, 24]), "0 0 24 24");
});
