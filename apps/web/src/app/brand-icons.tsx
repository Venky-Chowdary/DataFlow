/**
 * Connector marks.
 *
 * Databases and SaaS products use the official single-color brand mark from
 * Simple Icons (CC0). Products that set is not allowed to redistribute get a
 * drawn mark in that product's public color. File formats use a document
 * glyph in the Datawrap logo palette.
 */
import type { ReactNode } from "react";
import type { SimpleIcon } from "simple-icons";
import {
  siAirtable,
  siApachecassandra,
  siApachekafka,
  siApacheparquet,
  siClickhouse,
  siCouchbase,
  siDatabricks,
  siDuckdb,
  siElasticsearch,
  siGooglebigquery,
  siGooglecloudstorage,
  siHubspot,
  siInfluxdb,
  siMariadb,
  siMinio,
  siMongodb,
  siMysql,
  siNeo4j,
  siNotion,
  siPostgresql,
  siPresto,
  siQdrant,
  siRedis,
  siShopify,
  siSnowflake,
  siSqlite,
  siStripe,
  siTeradata,
  siTrino,
  siZendesk,
} from "simple-icons";

type Mark = Pick<SimpleIcon, "title" | "hex" | "path">;

const MARKS: Record<string, Mark> = {
  postgresql: siPostgresql,
  mysql: siMysql,
  mariadb: siMariadb,
  mongodb: siMongodb,
  snowflake: siSnowflake,
  bigquery: siGooglebigquery,
  redis: siRedis,
  elasticsearch: siElasticsearch,
  kafka: siApachekafka,
  sqlite: siSqlite,
  clickhouse: siClickhouse,
  databricks: siDatabricks,
  neo4j: siNeo4j,
  couchbase: siCouchbase,
  influxdb: siInfluxdb,
  hubspot: siHubspot,
  stripe: siStripe,
  shopify: siShopify,
  notion: siNotion,
  airtable: siAirtable,
  zendesk: siZendesk,
  parquet: siApacheparquet,
  duckdb: siDuckdb,
  cassandra: siApachecassandra,
  gcs: siGooglecloudstorage,
  minio: siMinio,
  qdrant: siQdrant,
  presto: siPresto,
  trino: siTrino,
  teradata: siTeradata,
};

/** Catalog ids that share one product mark. */
const ALIASES: Record<string, string> = {
  postgres: "postgresql",
  pg: "postgresql",
  amazon_rds: "postgresql",
  amazon_rds_postgres: "postgresql",
  citus: "postgresql",
  cockroach: "postgresql",
  cockroachdb: "postgresql",
  pgvector: "postgresql",
  amazon_rds_mysql: "mysql",
  aurora: "mysql",
  aurora_mysql: "mysql",
  sqlserver: "sqlserver",
  mssql: "sqlserver",
  microsoft_sql_server: "sqlserver",
  azure_sql: "sqlserver",
  azure_sql_database: "sqlserver",
  amazon_rds_sql_server: "sqlserver",
  oracle: "oracle",
  oracle_db: "oracle",
  s3: "s3",
  amazon_s3: "s3",
  aws_s3: "s3",
  redshift: "redshift",
  amazon_redshift: "redshift",
  dynamodb: "dynamodb",
  dynamo: "dynamodb",
  salesforce: "salesforce",
  sfdc: "salesforce",
  excel: "excel",
  xlsx: "excel",
  xls: "excel",
  google_cloud_storage: "gcs",
  adls: "azure",
  azure: "azure",
  azure_blob: "azure",
  iceberg: "iceberg",
  apache_iceberg: "iceberg",
  weaviate: "weaviate",
  pinecone: "pinecone",
  milvus: "milvus",
  csv: "csv",
  tsv: "csv",
  json: "json",
  jsonl: "json",
  ndjson: "json",
  xml: "file",
  yaml: "file",
  html: "file",
  pdf: "pdf",
  docx: "file",
  avro: "file",
  orc: "file",
  fixed_width: "file",
  generic_sql: "database",
  database: "database",
  file: "file",
  rest_api: "api",
  singer_tap: "api",
  email: "file",
  sftp: "file",
};

export function connectorIconKey(id: string): string {
  const key = String(id || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  return ALIASES[key] || key;
}

/** Official mark when one exists. Tests use this to prove aliases. */
export function connectorBrandMark(id: string): Mark | null {
  return MARKS[connectorIconKey(id)] ?? null;
}

function OfficialMark({ mark, size }: { mark: Mark; size: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" role="img" aria-hidden>
      <title>{mark.title}</title>
      <path d={mark.path} fill={`#${mark.hex}`} />
    </svg>
  );
}

function DrawnMark({
  size,
  title,
  children,
}: {
  size: number;
  title: string;
  children: ReactNode;
}) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" role="img" aria-hidden>
      <title>{title}</title>
      {children}
    </svg>
  );
}

function drawnMark(key: string, size: number) {
  switch (key) {
    case "sqlserver":
      return (
        <DrawnMark size={size} title="SQL Server">
          <ellipse cx="12" cy="6" rx="8" ry="3" fill="#0078D4" />
          <path d="M4 6v9c0 1.7 3.6 3 8 3s8-1.3 8-3V6c0 1.7-3.6 3-8 3s-8-1.3-8-3z" fill="#0078D4" />
          <path d="M4 11.2c1.8 1.2 4.7 1.8 8 1.8s6.2-.6 8-1.8" stroke="#fff" strokeWidth="1" fill="none" />
        </DrawnMark>
      );
    case "oracle":
      return (
        <DrawnMark size={size} title="Oracle">
          <rect x="3" y="4.5" width="18" height="2.4" rx="1.2" fill="#C74634" />
          <rect x="3" y="8.7" width="18" height="2.4" rx="1.2" fill="#C74634" />
          <rect x="3" y="12.9" width="18" height="2.4" rx="1.2" fill="#C74634" />
          <rect x="5" y="17.1" width="14" height="2.4" rx="1.2" fill="#C74634" />
        </DrawnMark>
      );
    case "s3":
      return (
        <DrawnMark size={size} title="Amazon S3">
          <path d="M6 8.5h12l-1.2 9.2a2 2 0 0 1-2 1.7H9.2a2 2 0 0 1-2-1.7L6 8.5z" fill="#FF9900" />
          <path d="M5 7.2h14c.4 0 .7.4.6.8l-.3 1H4.7l-.3-1c-.1-.4.2-.8.6-.8z" fill="#232F3E" />
          <path d="M8.2 16.2c1.2.7 2.4 1 3.8 1s2.6-.3 3.8-1" stroke="#232F3E" strokeWidth="1" fill="none" strokeLinecap="round" />
        </DrawnMark>
      );
    case "salesforce":
      return (
        <DrawnMark size={size} title="Salesforce">
          <path d="M8.2 16.8a3.2 3.2 0 0 1-.6-6.3 4.2 4.2 0 0 1 8.1-1.2 3.4 3.4 0 0 1 .7 6.6 3.6 3.6 0 0 1-6.6.9 3.1 3.1 0 0 1-1.6 0z" fill="#00A1E0" />
        </DrawnMark>
      );
    case "redshift":
      return (
        <DrawnMark size={size} title="Amazon Redshift">
          <path d="M12 3 20 7.5v9L12 21 4 16.5v-9L12 3z" fill="#8C4FFF" />
          <circle cx="12" cy="12" r="2.2" fill="#fff" />
        </DrawnMark>
      );
    case "dynamodb":
      return (
        <DrawnMark size={size} title="DynamoDB">
          <ellipse cx="12" cy="6" rx="7" ry="2.6" fill="#4053D6" />
          <path d="M5 6v8.5c0 1.5 3.1 2.6 7 2.6s7-1.1 7-2.6V6" fill="#4053D6" />
          <path d="M5 10.2c1.6 1.1 4.1 1.7 7 1.7s5.4-.6 7-1.7" stroke="#fff" strokeWidth="1" fill="none" />
        </DrawnMark>
      );
    case "azure":
      return (
        <DrawnMark size={size} title="Microsoft Azure">
          <path d="M10.2 4.2 3.8 18.6h5.2l2.4-4.6 4.8 4.6h4.2L13.6 4.2h-3.4z" fill="#0078D4" />
        </DrawnMark>
      );
    case "excel":
      return (
        <DrawnMark size={size} title="Microsoft Excel">
          <path d="M6 3.5h8l4 4V20a1.5 1.5 0 0 1-1.5 1.5H6A1.5 1.5 0 0 1 4.5 20V5A1.5 1.5 0 0 1 6 3.5z" fill="#217346" />
          <path d="M14 3.5v4h4" fill="#185C37" />
          <path d="M8 12h2.2M8 15h2.2M12.2 12H15M12.2 15H15" stroke="#fff" strokeWidth="1.2" strokeLinecap="round" />
        </DrawnMark>
      );
    case "iceberg":
      return (
        <DrawnMark size={size} title="Apache Iceberg">
          <path d="M12 3 21 18H3L12 3z" fill="#1F4E79" />
          <path d="M12 8.5 16.5 16h-9L12 8.5z" fill="#7FDBFF" />
        </DrawnMark>
      );
    case "weaviate":
      return (
        <DrawnMark size={size} title="Weaviate">
          <circle cx="12" cy="12" r="8" fill="#1A1A1A" />
          <circle cx="12" cy="12" r="3" fill="#00D1B2" />
        </DrawnMark>
      );
    case "pinecone":
      return (
        <DrawnMark size={size} title="Pinecone">
          <path d="M12 3.5 18 8v8l-6 4.5L6 16V8l6-4.5z" fill="#0D1B2A" />
          <path d="M12 8.5 15 10.2v3.6L12 15.5 9 13.8v-3.6L12 8.5z" fill="#F5C518" />
        </DrawnMark>
      );
    case "milvus":
      return (
        <DrawnMark size={size} title="Milvus">
          <circle cx="12" cy="12" r="8" fill="#00A1EA" />
          <path d="M8 14.5 12 8l4 6.5" stroke="#fff" strokeWidth="1.6" fill="none" strokeLinejoin="round" />
        </DrawnMark>
      );
    case "csv":
      return (
        <DrawnMark size={size} title="Delimited file">
          <path d="M6 3.5h8l4 4V20a1.5 1.5 0 0 1-1.5 1.5H6A1.5 1.5 0 0 1 4.5 20V5A1.5 1.5 0 0 1 6 3.5z" fill="var(--df-logo-ink)" />
          <path d="M8 12h2.5M8 15h2.5M12.5 12H16M12.5 15H16" stroke="#fff" strokeWidth="1.2" strokeLinecap="round" />
        </DrawnMark>
      );
    case "json":
      return (
        <DrawnMark size={size} title="JSON">
          <path d="M6 3.5h8l4 4V20a1.5 1.5 0 0 1-1.5 1.5H6A1.5 1.5 0 0 1 4.5 20V5A1.5 1.5 0 0 1 6 3.5z" fill="var(--df-logo-strap)" />
          <path d="M9 11c-1 0-1.4.6-1.4 1.5S8 14 9 14M15 11c1 0 1.4.6 1.4 1.5S16 14 15 14" stroke="#fff" strokeWidth="1.2" fill="none" strokeLinecap="round" />
        </DrawnMark>
      );
    case "pdf":
      return (
        <DrawnMark size={size} title="PDF">
          <path d="M6 3.5h8l4 4V20a1.5 1.5 0 0 1-1.5 1.5H6A1.5 1.5 0 0 1 4.5 20V5A1.5 1.5 0 0 1 6 3.5z" fill="var(--df-danger)" />
          <path d="M8.2 16.2v-4.2h1.5a1.5 1.5 0 0 1 0 3H8.2" stroke="#fff" strokeWidth="1.1" fill="none" />
        </DrawnMark>
      );
    case "api":
      return (
        <DrawnMark size={size} title="API">
          <rect x="3.5" y="6" width="17" height="12" rx="2" fill="var(--df-logo-tile)" />
          <path d="M8 10.5 6.2 12 8 13.5M16 10.5 17.8 12 16 13.5M11 14.2l2-4.4" stroke="var(--df-logo-arrow)" strokeWidth="1.3" fill="none" strokeLinecap="round" strokeLinejoin="round" />
        </DrawnMark>
      );
    case "database":
      return (
        <DrawnMark size={size} title="Database">
          <ellipse cx="12" cy="6.5" rx="7" ry="2.6" fill="var(--df-logo-ink)" />
          <path d="M5 6.5v8c0 1.5 3.1 2.7 7 2.7s7-1.2 7-2.7v-8c0 1.5-3.1 2.7-7 2.7S5 8 5 6.5z" fill="var(--df-logo-tile)" />
        </DrawnMark>
      );
    default:
      return (
        <DrawnMark size={size} title="File">
          <path d="M6 3.5h8l4 4V20a1.5 1.5 0 0 1-1.5 1.5H6A1.5 1.5 0 0 1 4.5 20V5A1.5 1.5 0 0 1 6 3.5z" fill="var(--df-logo-tile)" />
          <path d="M8 13h8M8 16h5" stroke="var(--df-logo-arrow)" strokeWidth="1.2" strokeLinecap="round" />
        </DrawnMark>
      );
  }
}

export function ConnectorIcon({ id, size = 32 }: { id: string; size?: number }) {
  const key = connectorIconKey(id);
  const mark = MARKS[key];
  if (mark) return <OfficialMark mark={mark} size={size} />;
  return drawnMark(key, size);
}
