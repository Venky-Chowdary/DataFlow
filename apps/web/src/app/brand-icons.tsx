/**
 * Connector marks.
 *
 * Brands that allow redistribution use the official single-color mark bundled
 * in Simple Icons (CC0, already a dependency). Trademark-restricted brands
 * (Microsoft/Oracle/Amazon/Salesforce have no Simple Icons mark) get a
 * hand-drawn approximation in that product's public color — these are NOT the
 * official logos. Engines with neither an official mark nor a family alias get
 * a drawn database mark in a product/family color, never a shared generic
 * glyph. File formats use a document glyph in the Datawrap logo palette.
 */
import type { ReactNode } from "react";
import type { SimpleIcon } from "simple-icons";
import {
  siAirtable,
  siAlibabacloud,
  siApacheavro,
  siApachecassandra,
  siApachedoris,
  siApachedruid,
  siApacheflink,
  siApachehive,
  siApachekafka,
  siApachenifi,
  siApacheparquet,
  siApachepulsar,
  siApachespark,
  siAsana,
  siAuth0,
  siBackblaze,
  siBitbucket,
  siBox,
  siCircleci,
  siClickhouse,
  siCloudera,
  siCloudflare,
  siCockroachlabs,
  siConfluence,
  siCouchbase,
  siCratedb,
  siDatabricks,
  siDatadog,
  siDigitalocean,
  siDropbox,
  siDuckdb,
  siElasticsearch,
  siFirebase,
  siGithub,
  siGitlab,
  siGooglebigquery,
  siGooglecloud,
  siGooglecloudstorage,
  siGoogledrive,
  siGoogleanalytics,
  siGooglepubsub,
  siGraphql,
  siHubspot,
  siInfluxdb,
  siIntercom,
  siJenkins,
  siJira,
  siLooker,
  siMailchimp,
  siMariadb,
  siMilvus,
  siMinio,
  siMongodb,
  siMqtt,
  siMysql,
  siNeo4j,
  siNeon,
  siNewrelic,
  siNotion,
  siOkta,
  siOpensearch,
  siPagerduty,
  siPaypal,
  siPlanetscale,
  siPostgresql,
  siPresto,
  siQdrant,
  siRabbitmq,
  siRedhatopenshift,
  siRedis,
  siSap,
  siScylladb,
  siShopify,
  siSinglestore,
  siSnowflake,
  siSplunk,
  siSqlite,
  siSquare,
  siStripe,
  siSupabase,
  siSurveymonkey,
  siTeradata,
  siTidb,
  siTimescale,
  siTrello,
  siTrino,
  siTypeform,
  siVitess,
  siWasabi,
  siZoho,
  siZendesk,
  siZoom,
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
  opensearch: siOpensearch,
  kafka: siApachekafka,
  flink: siApacheflink,
  sqlite: siSqlite,
  clickhouse: siClickhouse,
  databricks: siDatabricks,
  neo4j: siNeo4j,
  couchbase: siCouchbase,
  influxdb: siInfluxdb,
  hubspot: siHubspot,
  stripe: siStripe,
  paypal: siPaypal,
  square: siSquare,
  shopify: siShopify,
  notion: siNotion,
  airtable: siAirtable,
  zendesk: siZendesk,
  parquet: siApacheparquet,
  avro: siApacheavro,
  duckdb: siDuckdb,
  cassandra: siApachecassandra,
  gcs: siGooglecloudstorage,
  googlecloud: siGooglecloud,
  minio: siMinio,
  qdrant: siQdrant,
  presto: siPresto,
  trino: siTrino,
  teradata: siTeradata,
  singlestore: siSinglestore,
  supabase: siSupabase,
  firebase: siFirebase,
  timescaledb: siTimescale,
  openshift: siRedhatopenshift,
  hive: siApachehive,
  druid: siApachedruid,
  pulsar: siApachepulsar,
  spark: siApachespark,
  nifi: siApachenifi,
  sap: siSap,
  tidb: siTidb,
  scylladb: siScylladb,
  cratedb: siCratedb,
  vitess: siVitess,
  doris: siApachedoris,
  cloudera: siCloudera,
  planetscale: siPlanetscale,
  neon: siNeon,
  cockroachdb: siCockroachlabs,
  milvus: siMilvus,
  github: siGithub,
  gitlab: siGitlab,
  bitbucket: siBitbucket,
  jira: siJira,
  confluence: siConfluence,
  intercom: siIntercom,
  graphql: siGraphql,
  rabbitmq: siRabbitmq,
  pubsub: siGooglepubsub,
  mqtt: siMqtt,
  asana: siAsana,
  trello: siTrello,
  zoom: siZoom,
  googleanalytics: siGoogleanalytics,
  mailchimp: siMailchimp,
  looker: siLooker,
  datadog: siDatadog,
  newrelic: siNewrelic,
  splunk: siSplunk,
  pagerduty: siPagerduty,
  okta: siOkta,
  auth0: siAuth0,
  circleci: siCircleci,
  jenkins: siJenkins,
  box: siBox,
  dropbox: siDropbox,
  googledrive: siGoogledrive,
  typeform: siTypeform,
  surveymonkey: siSurveymonkey,
  zoho: siZoho,
  wasabi: siWasabi,
  backblaze: siBackblaze,
  digitalocean: siDigitalocean,
  alibabacloud: siAlibabacloud,
  cloudflare: siCloudflare,
};

/**
 * Drawn database/stream marks for products with no redistributable official
 * mark and no family alias — a cylinder in that product's public colour with
 * its real title, never the shared File/API glyph.
 */
const DRAWN_PRODUCTS: Record<string, { title: string; hex: string }> = {
  db2: { title: "IBM Db2", hex: "054ADA" },
  netezza: { title: "Netezza", hex: "1F70C1" },
  informix: { title: "Informix", hex: "FF3E2B" },
  sybase_ase: { title: "Sybase ASE", hex: "0065A8" },
  vertica: { title: "Vertica", hex: "6E1E78" },
  exasol: { title: "Exasol", hex: "003399" },
  firebird: { title: "Firebird", hex: "DD4B39" },
  h2: { title: "H2", hex: "0B3D91" },
  pinot: { title: "Apache Pinot", hex: "00A99D" },
  questdb: { title: "QuestDB", hex: "D14071" },
  risingwave: { title: "RisingWave", hex: "5E2BFF" },
  materialize: { title: "Materialize", hex: "7B3FE4" },
  yugabytedb: { title: "YugabyteDB", hex: "FF4E46" },
  starrocks: { title: "StarRocks", hex: "4B8BF5" },
  dremio: { title: "Dremio", hex: "26B5C0" },
  impala: { title: "Apache Impala", hex: "F7941E" },
  athena: { title: "Amazon Athena", hex: "FF9900" },
  emr: { title: "Amazon EMR", hex: "E15243" },
  kinesis: { title: "Amazon Kinesis", hex: "FF7139" },
  neptune: { title: "Amazon Neptune", hex: "527FFF" },
  documentdb: { title: "Amazon DocumentDB", hex: "C925D1" },
  cosmos_db: { title: "Azure Cosmos DB", hex: "4C5FD5" },
  synapse: { title: "Azure Synapse", hex: "2B88D8" },
  firebolt: { title: "Firebolt", hex: "FF3E00" },
  yellowbrick: { title: "Yellowbrick", hex: "F2A900" },
  actian: { title: "Actian", hex: "F25022" },
  delta_lake: { title: "Delta Lake", hex: "00ADD4" },
  ibm_cos: { title: "IBM Cloud Object Storage", hex: "1261FE" },
};

/** Catalog ids / saved types that share one product mark. */
const ALIASES: Record<string, string> = {
  postgres: "postgresql",
  pg: "postgresql",
  pgvector: "postgresql",
  amazon_rds: "postgresql",
  amazon_rds_postgres: "postgresql",
  amazon_rds_postgresql: "postgresql",
  postgresql_rds: "postgresql",
  postgresql_azure: "postgresql",
  postgresql_cloud_sql: "postgresql",
  azure_database_for_postgresql: "postgresql",
  google_cloud_sql_postgresql: "postgresql",
  citus: "postgresql",
  greenplum: "postgresql",
  alloydb: "postgresql",
  amazon_rds_mysql: "mysql",
  mysql_rds: "mysql",
  mysql_azure: "mysql",
  mysql_cloud_sql: "mysql",
  azure_database_for_mysql: "mysql",
  google_cloud_sql_mysql: "mysql",
  aurora: "mysql",
  amazon_aurora: "mysql",
  aurora_mysql: "mysql",
  mysql_aurora_global: "mysql",
  postgresql_aurora_global: "postgresql",
  oceanbase: "mysql",
  polardb: "mysql",
  gaussdb: "mysql",
  goldendb: "mysql",
  mysql_planetscale: "planetscale",
  postgresql_neon: "neon",
  postgresql_supabase: "supabase",
  motherduck: "duckdb",
  teradata_vantage: "teradata",
  sqlserver: "sqlserver",
  mssql: "sqlserver",
  sql_server: "sqlserver",
  microsoft_sql_server: "sqlserver",
  azure_sql: "sqlserver",
  azure_sql_database: "sqlserver",
  amazon_rds_sql_server: "sqlserver",
  google_cloud_sql_sql_server: "sqlserver",
  oracle: "oracle",
  oracle_db: "oracle",
  amazon_rds_oracle: "oracle",
  oracle_autonomous_warehouse: "oracle",
  sybase: "sybase_ase",
  sap_ase: "sap",
  sap_iq: "sap",
  sap_hana: "sap",
  sap_bw_4hana: "sap",
  ibm_db2_warehouse: "db2",
  azure_synapse: "synapse",
  azure_synapse_dedicated: "synapse",
  azure_synapse_serverless: "synapse",
  synapse_analytics: "synapse",
  azure_cosmos_db: "cosmos_db",
  amazon_kinesis: "kinesis",
  amazon_athena: "athena",
  amazon_emr: "emr",
  amazon_dynamodb: "dynamodb",
  amazon_documentdb: "documentdb",
  amazon_elasticsearch: "opensearch",
  amazon_elasticache_redis: "redis",
  azure_cache_redis: "redis",
  google_memorystore_redis: "redis",
  redis_enterprise: "redis",
  redis_cluster: "redis",
  elastic_cloud: "elasticsearch",
  google_biglake: "bigquery",
  google_bigquery: "bigquery",
  bigquery_us: "bigquery",
  bigquery_eu: "bigquery",
  bigquery_reservations: "bigquery",
  amazon_redshift: "redshift",
  redshift_serverless: "redshift",
  redshift_ra3: "redshift",
  snowflake_aws: "snowflake",
  snowflake_azure: "snowflake",
  snowflake_gcp: "snowflake",
  snowflake_standard: "snowflake",
  snowflake_enterprise: "snowflake",
  mongodb_atlas: "mongodb",
  mongodb_replica_set: "mongodb",
  mongodb_sharded_cluster: "mongodb",
  elasticsearch_aws: "elasticsearch",
  elasticsearch_gcp: "elasticsearch",
  cloudera_data_platform: "cloudera",
  apache_hive: "hive",
  apache_impala: "impala",
  apache_iceberg: "iceberg",
  apache_spark: "spark",
  apache_kafka: "kafka",
  apache_pulsar: "pulsar",
  apache_nifi: "nifi",
  google_pubsub: "pubsub",
  google_pub_sub: "pubsub",
  s3: "s3",
  amazon_s3: "s3",
  aws_s3: "s3",
  s3_us_east_1: "s3",
  s3_eu_west_1: "s3",
  s3_ap_southeast_1: "s3",
  google_cloud_storage: "gcs",
  gcs_us_central1: "gcs",
  gcs_europe_west1: "gcs",
  adls: "azure",
  azure: "azure",
  azure_blob: "azure",
  azure_blob_storage: "azure",
  azure_data_lake: "azure",
  azure_event_hubs: "azure",
  ibm_cloud_object_storage: "ibm_cos",
  oracle_cloud_object_storage: "oracle",
  alibaba_oss: "alibabacloud",
  cloudflare_r2: "cloudflare",
  digitalocean_spaces: "digitalocean",
  wasabi_b2: "wasabi",
  iceberg: "iceberg",
  dynamodb_global_tables: "dynamodb",
  firebase_realtime_db: "firebase",
  salesforce: "salesforce",
  sfdc: "salesforce",
  excel: "excel",
  xlsx: "excel",
  xls: "excel",
  google_sheets: "googledrive",
  weaviate: "weaviate",
  pinecone: "pinecone",
  csv: "csv",
  csv___tsv: "csv",
  csv_tsv: "csv",
  tsv: "csv",
  csv_upload: "csv",
  tsv_upload: "csv",
  csv_sftp: "sftp",
  json: "json",
  json_documents: "json",
  jsonl: "json",
  jsonl_stream: "json",
  json_api_export: "json",
  ndjson: "json",
  xml: "file",
  yaml: "file",
  html: "file",
  pdf: "pdf",
  docx: "file",
  orc: "file",
  fixed_width: "file",
  generic_sql: "database",
  database: "database",
  file: "file",
  rest_api: "api",
  rest_openapi: "api",
  singer_tap: "api",
  email: "file",
  sftp: "file",
  smb: "file",
  ftp: "file",
  webhook: "api",
  soap: "api",
  odata: "api",
  grpc: "api",
};

/**
 * Ordered substring → key fallbacks for catalog ids the alias table does not
 * spell out (region/instance/variant suffixes). Most specific first — a
 * product name must win over the cloud that hosts it.
 */
const CONTAINS: Array<[string, string]> = [
  ["amazon_elasticsearch", "opensearch"],
  ["athenahealth", "api"],
  ["sql_server", "sqlserver"],
  ["synapse", "synapse"],
  ["mssql", "sqlserver"],
  ["sqlserver", "sqlserver"],
  ["supabase", "supabase"],
  ["neon", "neon"],
  ["planetscale", "planetscale"],
  ["timescale", "timescaledb"],
  ["cockroach", "cockroachdb"],
  ["alloydb", "postgresql"],
  ["greenplum", "postgresql"],
  ["citus", "postgresql"],
  ["pgvector", "postgresql"],
  ["postgres", "postgresql"],
  ["motherduck", "duckdb"],
  ["oceanbase", "mysql"],
  ["polardb", "mysql"],
  ["gaussdb", "mysql"],
  ["goldendb", "mysql"],
  ["aurora", "mysql"],
  ["mariadb", "mysql"],
  ["percona", "mysql"],
  ["mysql", "mysql"],
  ["documentdb", "documentdb"],
  ["dynamodb", "dynamodb"],
  ["mongo", "mongodb"],
  ["snowflake", "snowflake"],
  ["biglake", "bigquery"],
  ["bigquery", "bigquery"],
  ["redshift", "redshift"],
  ["elasticache", "redis"],
  ["memorystore", "redis"],
  ["redis", "redis"],
  ["opensearch", "opensearch"],
  ["elastic", "elasticsearch"],
  ["doris", "doris"],
  ["starrocks", "starrocks"],
  ["cratedb", "cratedb"],
  ["tidb", "tidb"],
  ["vitess", "vitess"],
  ["scylla", "scylladb"],
  ["singlestore", "singlestore"],
  ["clickhouse", "clickhouse"],
  ["cassandra", "cassandra"],
  ["scylladb", "scylladb"],
  ["couchbase", "couchbase"],
  ["firebase", "firebase"],
  ["neo4j", "neo4j"],
  ["neptune", "neptune"],
  ["cosmos", "cosmos_db"],
  ["influxdb", "influxdb"],
  ["questdb", "questdb"],
  ["risingwave", "risingwave"],
  ["materialize", "materialize"],
  ["yugabyte", "yugabytedb"],
  ["exasol", "exasol"],
  ["firebolt", "firebolt"],
  ["firebird", "firebird"],
  ["informix", "informix"],
  ["netezza", "netezza"],
  ["sybase", "sybase_ase"],
  ["vertica", "vertica"],
  ["db2", "db2"],
  ["h2", "h2"],
  ["teradata", "teradata"],
  ["actian", "actian"],
  ["yellowbrick", "yellowbrick"],
  ["hive", "hive"],
  ["druid", "druid"],
  ["pinot", "pinot"],
  ["impala", "impala"],
  ["spark", "spark"],
  ["nifi", "nifi"],
  ["flink", "flink"],
  ["dremio", "dremio"],
  ["presto", "presto"],
  ["trino", "trino"],
  ["databricks", "databricks"],
  ["iceberg", "iceberg"],
  ["delta_lake", "delta_lake"],
  ["kafka", "kafka"],
  ["kinesis", "kinesis"],
  ["pub_sub", "pubsub"],
  ["pubsub", "pubsub"],
  ["pulsar", "pulsar"],
  ["rabbitmq", "rabbitmq"],
  ["mqtt", "mqtt"],
  ["qdrant", "qdrant"],
  ["weaviate", "weaviate"],
  ["pinecone", "pinecone"],
  ["milvus", "milvus"],
  ["sqlite", "sqlite"],
  ["duckdb", "duckdb"],
  ["sap_", "sap"],
  ["oracle", "oracle"],
  ["azure", "azure"],
  ["google_cloud_storage", "gcs"],
  ["gcs", "gcs"],
  ["minio", "minio"],
  ["wasabi", "wasabi"],
  ["backblaze", "backblaze"],
  ["object_storage", "s3"],
  ["s3", "s3"],
  ["cloudflare", "cloudflare"],
  ["digitalocean", "digitalocean"],
  ["alibaba", "alibabacloud"],
  ["ibm_cloud", "ibm_cos"],
  ["emr", "emr"],
  ["athena", "athena"],
  ["cloudera", "cloudera"],
  ["graphql", "graphql"],
  ["jira", "jira"],
  ["confluence", "confluence"],
  ["intercom", "intercom"],
  ["zendesk", "zendesk"],
  ["hubspot", "hubspot"],
  ["notion", "notion"],
  ["airtable", "airtable"],
  ["stripe", "stripe"],
  ["paypal", "paypal"],
  ["square", "square"],
  ["shopify", "shopify"],
  ["github", "github"],
  ["gitlab", "gitlab"],
  ["bitbucket", "bitbucket"],
  ["jenkins", "jenkins"],
  ["circleci", "circleci"],
  ["pagerduty", "pagerduty"],
  ["okta", "okta"],
  ["auth0", "auth0"],
  ["datadog", "datadog"],
  ["newrelic", "newrelic"],
  ["new_relic", "newrelic"],
  ["splunk", "splunk"],
  ["asana", "asana"],
  ["trello", "trello"],
  ["zoom", "zoom"],
  ["google_analytics", "googleanalytics"],
  ["mailchimp", "mailchimp"],
  ["looker", "looker"],
  ["typeform", "typeform"],
  ["surveymonkey", "surveymonkey"],
  ["zoho", "zoho"],
  ["salesforce", "salesforce"],
  ["sfdc", "salesforce"],
  ["excel", "excel"],
  ["xlsx", "excel"],
  ["googledrive", "googledrive"],
  ["google_drive", "googledrive"],
  ["google_sheets", "googledrive"],
  ["dropbox", "dropbox"],
  ["box", "box"],
  ["openshift", "openshift"],
  ["parquet", "parquet"],
  ["avro", "avro"],
];

function normalizeIconId(id: string): string {
  return String(id || "")
    .toLowerCase()
    .trim()
    .replace(/\([^)]*\)/g, "")
    .replace(/[\s-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/_[0-9a-f]{5,}$/g, "")
    .replace(/^_+|_+$/g, "");
}

export function connectorIconKey(id: string): string {
  const key = normalizeIconId(id);
  if (!key) return "file";
  if (MARKS[key] || DRAWN_PRODUCTS[key] || drawnKeys.has(key)) return key;
  if (ALIASES[key]) return ALIASES[key];
  for (const [substr, target] of CONTAINS) {
    if (key.includes(substr)) return target;
  }
  // Progressive suffix-strip: snowflake_aws -> snowflake_ -> snowflake.
  let trimmed = key;
  while (trimmed.includes("_")) {
    trimmed = trimmed.slice(0, trimmed.lastIndexOf("_"));
    if (MARKS[trimmed] || DRAWN_PRODUCTS[trimmed] || drawnKeys.has(trimmed)) {
      return trimmed;
    }
    if (ALIASES[trimmed]) return ALIASES[trimmed];
  }
  return "api";
}

const drawnKeys = new Set([
  "sqlserver",
  "oracle",
  "s3",
  "salesforce",
  "redshift",
  "dynamodb",
  "azure",
  "excel",
  "iceberg",
  "weaviate",
  "pinecone",
  "csv",
  "json",
  "pdf",
  "api",
  "database",
]);

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

/** Database cylinder in a product colour — for engines with no official mark. */
function DrawnProductDb({ size, title, hex }: { size: number; title: string; hex: string }) {
  return (
    <DrawnMark size={size} title={title}>
      <ellipse cx="12" cy="6" rx="7.5" ry="2.8" fill={`#${hex}`} />
      <path
        d={`M4.5 6v9.5c0 1.6 3.4 2.8 7.5 2.8s7.5-1.2 7.5-2.8V6c0 1.6-3.4 2.8-7.5 2.8S4.5 7.6 4.5 6z`}
        fill={`#${hex}`}
        opacity="0.85"
      />
      <path
        d="M4.5 11c1.7 1.1 4.5 1.7 7.5 1.7s5.8-.6 7.5-1.7"
        stroke="#fff"
        strokeWidth="0.9"
        fill="none"
        opacity="0.7"
      />
    </DrawnMark>
  );
}

function drawnMark(key: string, size: number) {
  const product = DRAWN_PRODUCTS[key];
  if (product) {
    return <DrawnProductDb size={size} title={product.title} hex={product.hex} />;
  }
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
