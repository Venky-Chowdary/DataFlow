/**
 * Connector marks for the public site.
 *
 * Only the marketing surface imports this module. The signed-in app keeps its
 * own marks in brand-icons.tsx, so the classic Source/Destination screens
 * stay pixel-identical to their parity baselines (e2e/wizard-parity.spec.ts);
 * moving the app to these marks is a product change with new baselines.
 *
 * Three kinds, and nothing else:
 *  - mark: the vendor's real mark, from Simple Icons (CC0-1.0, a runtime
 *    dependency listed in NOTICE). Each glyph is centred on its own bounding
 *    box and scaled to one optical size, so a tall mark (MongoDB) and a square
 *    one (Couchbase) read as the same size.
 *  - monogram: vendors with no Simple Icons mark (several were removed at the
 *    brand owner's request) get one neutral rounded tile with the product's
 *    initials. It is identical in style for every such vendor and imitates
 *    nobody's logo. Never draw a lookalike of a vendor's mark here.
 *  - generic: things that are not a vendor (delimited files, a file server,
 *    an HTTP API, "other SQL databases") use a plain Lucide glyph.
 *
 * Marks keep the brand colour as their fill attribute; the public site's
 * stylesheet (styles/marketing-icons.css) inks every kind in one neutral ink.
 */
import { Database, File, FileSpreadsheet, FileText, FolderSync, Globe, type LucideIcon } from "lucide-react";
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
  siClaude,
  siClickhouse,
  siCloudera,
  siCloudflare,
  siCockroachlabs,
  siConfluence,
  siCouchbase,
  siCratedb,
  siCursor,
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
  siGooglesheets,
  siGraphql,
  siH2database,
  siHtml5,
  siHubspot,
  siInfluxdb,
  siIntercom,
  siJenkins,
  siJira,
  siJson,
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
  siOpenapiinitiative,
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
  siXml,
  siYaml,
  siZoho,
  siZendesk,
  siZoom,
} from "simple-icons";

type Mark = Pick<SimpleIcon, "title" | "slug" | "hex" | "path">;

/**
 * The MySQL mark is the dolphin over the "MySQL" wordmark; at icon size the
 * wordmark is unreadable. Keep only the dolphin's own subpaths (the ones that
 * start above the wordmark) of the official path. Every subpath of this path
 * closes, so each one starts where the last one started.
 */
export function dolphinOnly(path: string): string {
  let sx = 0;
  let sy = 0;
  const kept: string[] = [];
  for (const part of path.split(/(?=[Mm])/)) {
    const head = part.slice(1).match(/^[\s,]*(-?(?:\d+\.?\d*|\.\d+))[\s,]*(-?(?:\d+\.?\d*|\.\d+))/);
    if (!head) continue;
    const [x, y] = [Number(head[1]), Number(head[2])];
    if (part[0] === "M") [sx, sy] = [x, y];
    else [sx, sy] = [sx + x, sy + y];
    if (sy < 13) kept.push(`M${+sx.toFixed(3)} ${+sy.toFixed(3)}${part.slice(1 + head[0].length)}`);
  }
  return kept.join("");
}

const MYSQL: Mark = { ...siMysql, path: dolphinOnly(siMysql.path) };

const MARKS: Record<string, Mark> = {
  postgresql: siPostgresql,
  mysql: MYSQL,
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
  json: siJson,
  xml: siXml,
  yaml: siYaml,
  html: siHtml5,
  h2: siH2database,
  googlesheets: siGooglesheets,
  openapi: siOpenapiinitiative,
  cursor: siCursor,
  claude: siClaude,
};

/**
 * Vendors and projects with no Simple Icons mark. They render as the neutral
 * monogram tile (see `connectorMonogram`), never as a drawn approximation.
 */
const MONOGRAMS: Record<string, string> = {
  sqlserver: "SQL Server",
  oracle: "Oracle",
  s3: "Amazon S3",
  salesforce: "Salesforce",
  redshift: "Amazon Redshift",
  dynamodb: "Amazon DynamoDB",
  azure: "Microsoft Azure",
  excel: "Microsoft Excel",
  word: "Microsoft Word",
  iceberg: "Apache Iceberg",
  orc: "Apache ORC",
  weaviate: "Weaviate",
  pinecone: "Pinecone",
  singer: "Singer",
  vscode: "VS Code",
  db2: "IBM Db2",
  netezza: "Netezza",
  informix: "Informix",
  sybase_ase: "Sybase ASE",
  vertica: "Vertica",
  exasol: "Exasol",
  firebird: "Firebird",
  pinot: "Apache Pinot",
  questdb: "QuestDB",
  risingwave: "RisingWave",
  materialize: "Materialize",
  yugabytedb: "YugabyteDB",
  starrocks: "StarRocks",
  dremio: "Dremio",
  impala: "Apache Impala",
  athena: "Amazon Athena",
  emr: "Amazon EMR",
  kinesis: "Amazon Kinesis",
  neptune: "Amazon Neptune",
  documentdb: "Amazon DocumentDB",
  cosmos_db: "Azure Cosmos DB",
  synapse: "Azure Synapse",
  firebolt: "Firebolt",
  yellowbrick: "Yellowbrick",
  actian: "Actian",
  delta_lake: "Delta Lake",
  ibm_cos: "IBM Cloud Object Storage",
};

/** Not a vendor: a plain glyph, never a brand. */
const GENERIC: Record<string, { title: string; Icon: LucideIcon }> = {
  csv: { title: "Delimited file", Icon: FileSpreadsheet },
  pdf: { title: "PDF", Icon: FileText },
  file: { title: "File", Icon: File },
  fileserver: { title: "File transfer server", Icon: FolderSync },
  api: { title: "HTTP API", Icon: Globe },
  database: { title: "SQL database", Icon: Database },
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
  google_sheets: "googlesheets",
  weaviate: "weaviate",
  pinecone: "pinecone",
  csv: "csv",
  csv___tsv: "csv",
  csv_tsv: "csv",
  tsv: "csv",
  csv_upload: "csv",
  tsv_upload: "csv",
  csv_sftp: "fileserver",
  json_documents: "json",
  jsonl: "json",
  jsonl_stream: "json",
  json_api_export: "json",
  ndjson: "json",
  pdf: "pdf",
  docx: "word",
  microsoft_word: "word",
  html5: "html",
  vs_code: "vscode",
  visual_studio_code: "vscode",
  claude_desktop: "claude",
  claude_code: "claude",
  fixed_width: "file",
  generic_sql: "database",
  database: "database",
  file: "file",
  rest_api: "api",
  rest_openapi: "openapi",
  singer_tap: "singer",
  email: "file",
  sftp: "fileserver",
  smb: "fileserver",
  ftp: "fileserver",
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
  ["google_sheets", "googlesheets"],
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
  if (isKnown(key)) return key;
  if (ALIASES[key]) return ALIASES[key];
  for (const [substr, target] of CONTAINS) {
    if (key.includes(substr)) return target;
  }
  // Progressive suffix-strip: snowflake_aws -> snowflake_ -> snowflake.
  let trimmed = key;
  while (trimmed.includes("_")) {
    trimmed = trimmed.slice(0, trimmed.lastIndexOf("_"));
    if (isKnown(trimmed)) return trimmed;
    if (ALIASES[trimmed]) return ALIASES[trimmed];
  }
  return "api";
}

function isKnown(key: string): boolean {
  return Boolean(MARKS[key] || MONOGRAMS[key] || GENERIC[key]);
}

export type ConnectorIconKind = "mark" | "monogram" | "generic";

/** Which of the three kinds a connector id renders as. */
export function connectorIconKind(id: string): ConnectorIconKind {
  const key = connectorIconKey(id);
  if (MARKS[key]) return "mark";
  if (MONOGRAMS[key]) return "monogram";
  return "generic";
}

/** Official mark when one exists. Tests use this to prove aliases. */
export function connectorBrandMark(id: string): Mark | null {
  return MARKS[connectorIconKey(id)] ?? null;
}

/** Every mark the module can render, for the bounding-box table and its test. */
export function connectorMarks(): Mark[] {
  return Object.values(MARKS);
}

const PARENT = /^(Amazon|AWS|Azure|Microsoft|IBM|Apache|Google)\s+/;

/**
 * Initials for the monogram tile, by one rule for every vendor: drop the
 * parent company, keep a short code name whole (S3, Db2, EMR, SQL, VS), else
 * the initials of the first two words.
 */
export function connectorMonogram(title: string): string {
  const words = title.replace(PARENT, "").trim().split(/\s+/);
  const first = words[0] ?? "";
  if (first.length <= 3 && (/\d/.test(first) || first === first.toUpperCase())) return first;
  return words
    .slice(0, 2)
    .map((w) => w[0]?.toUpperCase() ?? "")
    .join("");
}

/** [x, y, width, height] of each mark's path in its 24-unit box, by slug. */
type Box = [number, number, number, number];

// Measured with getBBox() in Chromium from simple-icons 16.35.0 (MySQL: the
// dolphin only). Re-measure a mark when its path changes; the test checks that
// every mark has a box inside the 24-unit square.
const BOXES: Record<string, Box> = {
  airtable: [0, 1.97, 24, 20.07],
  alibabacloud: [0, 4.52, 24, 14.97],
  apacheavro: [0, 2, 24, 20],
  apachecassandra: [0, 5.77, 24, 12.47],
  apachedoris: [2.94, 0, 18.12, 24],
  apachedruid: [0, 3.19, 24, 17.62],
  apacheflink: [0.05, 0, 23.91, 24],
  apachehive: [0, 0.26, 24, 23.48],
  apachekafka: [4.68, 0, 14.64, 24],
  apachenifi: [3.45, 0, 17.11, 24],
  apacheparquet: [0, 2.59, 24, 18.82],
  apachepulsar: [0, 3.52, 24, 16.95],
  apachespark: [0.28, 0, 23.45, 24],
  asana: [0, 0.91, 24, 22.18],
  auth0: [1.51, 0, 20.97, 24],
  backblaze: [4.74, 0, 14.53, 24],
  bitbucket: [0, 1.21, 24, 21.58],
  box: [0, 5.52, 24, 12.95],
  circleci: [0.18, 0, 23.64, 24],
  claude: [0.01, 0, 23.98, 24],
  clickhouse: [0, 1.34, 24, 21.33],
  cloudera: [0, 0, 24, 24],
  cloudflare: [0, 6.57, 24, 10.85],
  cockroachlabs: [2.4, 0, 19.21, 24],
  confluence: [0, 0.29, 24, 23.42],
  couchbase: [0, 0, 24, 24],
  cratedb: [0, 3, 24, 18],
  cursor: [1.47, 0, 21.06, 24],
  databricks: [0.95, 0, 22.1, 24],
  datadog: [0.79, 0, 22.42, 24],
  digitalocean: [0.01, 0, 23.99, 24],
  dropbox: [0, 1.81, 24, 20.39],
  duckdb: [0, 0, 24, 24],
  elasticsearch: [1.39, 0, 21.21, 24],
  firebase: [2.41, 0, 19.19, 24],
  github: [0, 0.3, 24, 23.41],
  gitlab: [0, 0.44, 24, 23.11],
  googleanalytics: [1.16, 0, 21.68, 24],
  googlebigquery: [0, 0, 24, 24],
  googlecloud: [0, 2.38, 24, 19.25],
  googlecloudstorage: [0, 2.4, 24, 19.2],
  googledrive: [0, 1.49, 24, 21.03],
  googlepubsub: [1.01, 0, 21.99, 24],
  googlesheets: [3.27, 0, 17.45, 24],
  graphql: [1.32, 0, 21.36, 24],
  h2database: [0, 0, 24, 24],
  html5: [1.5, 0, 21, 24],
  hubspot: [0.55, 0, 22.9, 24],
  influxdb: [0.18, 0, 23.63, 24],
  intercom: [0, 0, 24, 24],
  jenkins: [1.12, 0, 21.76, 24],
  jira: [0, 0, 24, 24],
  json: [0.03, 0, 23.94, 24],
  looker: [4.55, 0, 14.89, 24],
  mailchimp: [0.69, 0, 22.61, 24],
  mariadb: [0, 4.26, 24, 15.48],
  milvus: [0, 4, 24, 16],
  minio: [5.76, 0, 12.49, 24],
  mongodb: [6.61, 0, 10.78, 24],
  mqtt: [0, 0.01, 24, 23.99],
  mysql: [14.17, 3.85, 9.83, 9.65],
  neo4j: [0, 0.73, 24, 22.54],
  neon: [0, 0, 24, 24],
  newrelic: [1.61, 0, 20.78, 24],
  notion: [0.49, 0, 23.02, 24],
  okta: [0, 0, 24, 24],
  openapiinitiative: [0, 0, 24, 24],
  opensearch: [0, 0, 24, 24],
  pagerduty: [3.73, 0, 16.54, 24],
  paypal: [1.82, 0, 20.36, 24],
  planetscale: [0, 0, 24, 24],
  postgresql: [0.42, 0, 23.17, 24],
  presto: [0, 0, 24, 24],
  qdrant: [1.61, 0, 20.79, 24],
  rabbitmq: [0, 0, 23.99, 24],
  redhatopenshift: [0, 0.97, 24, 22.06],
  redis: [0, 0.93, 24, 22.15],
  sap: [0, 6.06, 24, 11.87],
  scylladb: [2.27, 0, 19.46, 24],
  shopify: [1.45, 0, 21.1, 24],
  singlestore: [0, 0, 24, 24],
  snowflake: [0, 0.57, 24, 22.87],
  splunk: [0, 8.44, 24, 7.12],
  sqlite: [1.27, 0, 21.46, 24],
  square: [0, 0, 24, 24],
  stripe: [3.43, 0, 17.13, 24],
  supabase: [0.3, 0, 23.39, 24],
  surveymonkey: [0, 3.64, 24, 16.73],
  teradata: [0, 0, 24, 24],
  tidb: [1.61, 0, 20.78, 24],
  timescale: [0, 0.76, 24, 22.47],
  trello: [0, 0, 24, 24],
  trino: [3.91, 0, 16.19, 24],
  typeform: [0, 9.31, 24, 5.39],
  vitess: [0, 1.04, 24, 21.91],
  wasabi: [0, 0, 24, 24],
  xml: [0, 7.05, 24, 9.89],
  yaml: [0, 0.97, 24, 22.06],
  zendesk: [0, 2.9, 24, 18.19],
  zoho: [0, 6.9, 24, 10.21],
  zoom: [0, 9.27, 24, 5.46],
};

/** Every glyph is scaled to this optical size (units of a 24-unit box). */
const OPTICAL = 20;

/**
 * A viewBox centred on the glyph's bounding box that shows it at one optical
 * size: equal area for every mark, never wider or taller than the box.
 */
export function markViewBox(box: Box | undefined): string {
  const [x, y, w, h] = box ?? [0, 0, 24, 24];
  const scale = Math.min(24 / Math.max(w, h), OPTICAL / Math.sqrt(w * h));
  const side = 24 / scale;
  const r = (n: number) => +n.toFixed(3);
  return `${r(x + w / 2 - side / 2)} ${r(y + h / 2 - side / 2)} ${r(side)} ${r(side)}`;
}

function OfficialMark({ mark, size }: { mark: Mark; size: number }) {
  return (
    <svg
      className="df-cicon df-cicon--mark"
      width={size}
      height={size}
      viewBox={markViewBox(BOXES[mark.slug])}
      aria-hidden
      focusable="false"
    >
      <title>{mark.title}</title>
      <path d={mark.path} fill={`#${mark.hex}`} />
    </svg>
  );
}

/** One neutral tile for every vendor without a redistributable mark. */
function MonogramMark({ title, size }: { title: string; size: number }) {
  const text = connectorMonogram(title);
  const fontSize = text.length >= 3 ? 8 : text.length === 2 ? 10 : 12;
  return (
    <svg
      className="df-cicon df-cicon--tile"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      aria-hidden
      focusable="false"
    >
      <title>{title}</title>
      <rect
        x="1.5"
        y="1.5"
        width="21"
        height="21"
        rx="5"
        fill="var(--df-surface-muted)"
        stroke="var(--df-border)"
        strokeWidth="1"
        vectorEffect="non-scaling-stroke"
      />
      <text
        x="12"
        y="12"
        dy="0.36em"
        textAnchor="middle"
        fontFamily="var(--df-font-sans)"
        fontSize={fontSize}
        fontWeight="600"
        fill="var(--df-text-secondary)"
      >
        {text}
      </text>
    </svg>
  );
}

export function ConnectorIcon({ id, size = 32 }: { id: string; size?: number }) {
  const key = connectorIconKey(id);
  const mark = MARKS[key];
  if (mark) return <OfficialMark mark={mark} size={size} />;
  const monogram = MONOGRAMS[key];
  if (monogram) return <MonogramMark title={monogram} size={size} />;
  const { title, Icon } = GENERIC[key] ?? GENERIC.file;
  return (
    <Icon
      className="df-cicon df-cicon--generic"
      width={size}
      height={size}
      strokeWidth={1.75}
      aria-hidden
      focusable="false"
    >
      <title>{title}</title>
    </Icon>
  );
}
