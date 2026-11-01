export interface ConnectorSetupGuideLink {
  label: string;
  url: string;
}

export interface ConnectorSetupGuide {
  title: string;
  steps: string[];
  /** Official vendor docs for where to find/create each credential. */
  links?: ConnectorSetupGuideLink[];
}

const GENERIC: ConnectorSetupGuide = {
  title: "Set up this connection",
  steps: [
    "Enter the host and credentials your admin issued.",
    "Leave optional fields blank unless you were given a specific value.",
    "Test before you save. Connected means the driver reached the system.",
  ],
  links: [{ label: "Datawrap connector docs", url: "#/docs" }],
};

const GUIDES: Record<string, ConnectorSetupGuide> = {
  postgresql: {
    title: "Set up PostgreSQL",
    steps: [
      "Get host, port (default 5432), database, user, and password from your DBA or cloud console (RDS → Connectivity, Cloud SQL → Connections, Neon/Supabase → Dashboard).",
      "Hosted twins (Neon, Supabase, RDS, Aurora Postgres, TimescaleDB, CockroachDB, CNPG) use this same login — paste the host they show, SSL on.",
      "The user needs CONNECT plus SELECT on the source tables, or CREATE/INSERT on the destination schema.",
      "Or paste a connection string: postgresql://user:pass@host:5432/db?sslmode=require.",
      "Test proves the driver can reach the host and log in — a role error is still 'connected', it just means grants are missing.",
    ],
    links: [
      { label: "PostgreSQL connection docs", url: "https://www.postgresql.org/docs/current/libpq-connect.html" },
      { label: "SSL modes (sslmode)", url: "https://www.postgresql.org/docs/current/libpq-ssl.html" },
      { label: "RDS connectivity settings", url: "https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ConnectToPostgreSQLInstance.html" },
    ],
  },
  mysql: {
    title: "Set up MySQL / MariaDB",
    steps: [
      "Get host, port (default 3306), database, user, and password from your DBA or console (RDS → Connectivity, Cloud SQL, PlanetScale → Connect, Aiven → Service).",
      "MariaDB, Percona, PlanetScale, SingleStore, and Aurora MySQL share this driver — same fields.",
      "Grant the user SELECT on source tables or INSERT/CREATE on the destination schema.",
      "Or paste a connection string: mysql://user:pass@host:3306/db.",
      "For CDC or incremental syncs the user also needs REPLICATION privileges on MySQL.",
      "Test proves TCP reachability plus login — Access denied means the password or host grant (user@host) is wrong.",
    ],
    links: [
      { label: "MySQL connecting docs", url: "https://dev.mysql.com/doc/refman/8.4/en/connecting.html" },
      { label: "MySQL account management", url: "https://dev.mysql.com/doc/refman/8.4/en/account-names.html" },
      { label: "MariaDB connecting", url: "https://mariadb.com/kb/en/connecting/" },
    ],
  },
  sqlserver: {
    title: "Set up SQL Server",
    steps: [
      "Get server host, port (default 1433), database, and a login — SQL authentication (user + password) works on-premises and on Azure SQL.",
      "RDS: use the master user or a login you created. Azure SQL: use the server admin or a contained database user — server name is like myserver.database.windows.net.",
      "The login needs db_datareader on source databases or db_datawriter/db_ddladmin on the destination.",
      "If your org uses Windows/Kerberos auth only, ask the DBA for a SQL-authenticated service account.",
      "Test proves the TDS port and login — Login failed for user is auth, not network.",
    ],
    links: [
      { label: "Connect to SQL Server", url: "https://learn.microsoft.com/en-us/sql/connect/sql-server-connection-methods" },
      { label: "Azure SQL connectivity", url: "https://learn.microsoft.com/en-us/azure/azure-sql/database/connect-query-content-reference-guide" },
    ],
  },
  oracle: {
    title: "Set up Oracle",
    steps: [
      "Get host, port (default 1521), service name (or SID), user, and password from your DBA — the service name is usually in tnsnames.ora.",
      "Autonomous Database: download the wallet and use the full connect descriptor or service name it defines.",
      "The user needs SELECT on the source tables (or the destination schema privileges).",
      "Test proves the listener answers and the login works — ORA-01017 is bad credentials, ORA-12541 is host/port.",
    ],
    links: [
      { label: "Oracle naming methods (service name vs SID)", url: "https://docs.oracle.com/en/database/oracle/oracle-database/23/netag/configuring-naming-methods.html" },
      { label: "Autonomous Database connectivity", url: "https://docs.oracle.com/en/cloud/paas/autonomous-database/adbsa/connect.html" },
    ],
  },
  sqlite: {
    title: "Set up SQLite",
    steps: [
      "Provide the path to the .db/.sqlite file — a local path or an object-store URI.",
      "No host, port, or credentials exist — SQLite is a file.",
      "Test proves the file exists and opens as a SQLite database.",
    ],
    links: [{ label: "SQLite docs", url: "https://www.sqlite.org/docs.html" }],
  },
  duckdb: {
    title: "Set up DuckDB",
    steps: [
      "Provide the path to the .duckdb/.ddb file — a local path or an object-store URI.",
      "No host, port, or credentials exist — DuckDB is an embedded database.",
      "Test proves the file opens as a DuckDB database.",
    ],
    links: [{ label: "DuckDB docs", url: "https://duckdb.org/docs/stable/" }],
  },
  mongodb: {
    title: "Set up MongoDB",
    steps: [
      "Atlas: cluster → Connect → Drivers → copy the connection string (mongodb+srv://user:pass@cluster.mongodb.net/db).",
      "Self-hosted: use mongodb://host:27017/database and the user's credentials.",
      "The user needs read on the source databases (find/listCollections) or readWrite on the destination.",
      "Atlas also needs your server's egress IP in Network Access — a timeout usually means the IP is not whitelisted, not bad credentials.",
      "CDC and snapshot-consistent reads need a replica set — standalone mongod cannot do them.",
    ],
    links: [
      { label: "Atlas connect to cluster", url: "https://www.mongodb.com/docs/atlas/connect-to-cluster/" },
      { label: "Atlas network access (IP allowlist)", url: "https://www.mongodb.com/docs/atlas/security/ip-access-list/" },
      { label: "MongoDB connection string format", url: "https://www.mongodb.com/docs/manual/reference/connection-string/" },
    ],
  },
  redis: {
    title: "Set up Redis",
    steps: [
      "Get host, port (default 6379), and password (or username+password on Redis 6+ ACLs).",
      "Valkey, KeyDB, and Dragonfly share this driver — same fields.",
      "Cloud: Elasticache → Endpoint, Redis Cloud → Database endpoint, Upstash → console shows TLS URL.",
      "Enable SSL/TLS if your provider requires it (most managed Redis does).",
      "Test proves PING answers — WRONGPASS is auth, timeout is network/VPC.",
    ],
    links: [
      { label: "Redis connection docs", url: "https://redis.io/docs/latest/develop/connect/" },
      { label: "Redis ACLs (user + password)", url: "https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/" },
    ],
  },
  elasticsearch: {
    title: "Set up Elasticsearch",
    steps: [
      "Get the endpoint URL (https://host:9200) plus an API key or elastic user + password.",
      "Elastic Cloud: deployment → Manage → copy the endpoint, then Security → API keys to mint a key.",
      "The API key needs read on source indices or create/write on the destination index.",
      "Test proves the cluster answers — 401 is a bad key, a certificate error means TLS/self-signed needs attention.",
    ],
    links: [
      { label: "Create an API key", url: "https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html" },
      { label: "Elastic Cloud endpoints", url: "https://www.elastic.co/guide/en/cloud/current/ec-find-endpoint.html" },
    ],
  },
  dynamodb: {
    title: "Set up DynamoDB",
    steps: [
      "Create an IAM access key pair in the AWS console (IAM → Users → Security credentials → Access keys).",
      "Enter Access Key ID, Secret Access Key, and the region your tables live in (e.g. us-east-1).",
      "The IAM policy needs dynamodb:Scan/Query/GetItem on source tables or PutItem/BatchWriteItem on the destination.",
      "Least privilege: scope the policy to the specific table ARNs.",
      "Test proves a list/describe call succeeds — InvalidClientTokenId is a bad key, not a table problem.",
    ],
    links: [
      { label: "Create IAM access keys", url: "https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html" },
      { label: "DynamoDB IAM permissions", url: "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AccessControl.html" },
    ],
  },
  snowflake: {
    title: "Set up Snowflake",
    steps: [
      "Pick the Snowflake tile. AWS, Azure, GCP, Standard, and Enterprise are the same login — not different products.",
      "In Snowsight, open the account menu and copy the account identifier (org-account, for example myorg-acctname).",
      "Paste that in Account host. A browser URL is the host, not a login. Locator-only hosts can return HTTP 404.",
      "Leave Role blank unless you know a role this user can assume.",
      "Test. A 250001 means the host was reached — check username and password. If Snowsight uses MFA, switch to Programmatic access token.",
    ],
    links: [
      { label: "Find your account identifier", url: "https://docs.snowflake.com/en/user-guide/admin-account-identifier" },
      { label: "Programmatic access tokens (MFA accounts)", url: "https://docs.snowflake.com/en/user-guide/programmatic-access-tokens" },
      { label: "Key-pair authentication", url: "https://docs.snowflake.com/en/user-guide/key-pair-auth" },
    ],
  },
  bigquery: {
    title: "Set up BigQuery",
    steps: [
      "In Google Cloud console → IAM & Admin → Service Accounts, create (or pick) a service account.",
      "Grant it BigQuery Data Viewer on the source dataset, or BigQuery Data Editor + BigQuery Job User on the destination project.",
      "Create a JSON key on the Keys tab and paste the whole JSON into Service account key.",
      "Enter the project ID that owns the datasets — the key carries the identity, the project routes billing/quota.",
      "Test proves the service account can list datasets — PERMISSION_DENIED means the roles, not the key file.",
    ],
    links: [
      { label: "Create a service-account key", url: "https://cloud.google.com/iam/docs/keys-create-delete" },
      { label: "BigQuery IAM roles", url: "https://cloud.google.com/bigquery/docs/access-control" },
    ],
  },
  redshift: {
    title: "Set up Redshift",
    steps: [
      "Console → cluster → Properties: copy the endpoint host (no port suffix) and port (default 5439).",
      "Use the master user or a database user with SELECT on source schemas or CREATE/INSERT on the destination schema.",
      "The endpoint is private inside a VPC by default — enable Publicly accessible or run Datawrap in the same VPC, and open the security group to our egress IP.",
      "Test proves TCP + login — connection refused is the security group, password authentication failed is the user.",
    ],
    links: [
      { label: "Connect to a Redshift cluster", url: "https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-to-cluster.html" },
      { label: "Redshift security groups", url: "https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-security-groups.html" },
    ],
  },
  databricks: {
    title: "Set up Databricks",
    steps: [
      "Workspace URL is your host: https://<workspace>.cloud.databricks.com.",
      "SQL warehouse → Connection details gives the HTTP path (e.g. /sql/1.0/warehouses/abc123) — paste it in HTTP path.",
      "User profile → Settings → Developer → Access tokens → Generate new token. PATs expire — copy it immediately.",
      "Test proves the token and HTTP path work against the warehouse.",
    ],
    links: [
      { label: "Create a personal access token", url: "https://docs.databricks.com/en/dev-tools/auth/pat.html" },
      { label: "SQL warehouse connection details", url: "https://docs.databricks.com/en/compute/sql-warehouse.html" },
    ],
  },
  clickhouse: {
    title: "Set up ClickHouse",
    steps: [
      "Get host and HTTP port (default 8123, or 8443 for TLS). ClickHouse Cloud: service → Connect shows the HTTPS endpoint.",
      "Use the default user or a user with SELECT on source tables / INSERT on the destination database.",
      "Self-hosted: open the port in the firewall — ClickHouse listens on 8123 only on localhost unless configured.",
      "Test proves the HTTP interface answers — a refused connection is the bind/firewall, not auth.",
    ],
    links: [{ label: "ClickHouse getting started", url: "https://clickhouse.com/docs/getting-started/quick-start" }],
  },
  iceberg: {
    title: "Set up Apache Iceberg",
    steps: [
      "Iceberg tables live in a catalog — pick the one your platform issues credentials for: AWS Glue, a REST catalog, or Hadoop (warehouse path).",
      "Glue: IAM access keys with glue:GetTable/GetDatabase plus S3 read/write on the warehouse location.",
      "REST catalog (e.g. Polaris, Nessie, Tabular): catalog URI + bearer token or client credentials.",
      "Enter the warehouse location (s3://bucket/warehouse/) and the namespace the tables live in.",
      "Test proves the catalog lists namespaces — a 404 is the URI, a 403 is IAM.",
    ],
    links: [
      { label: "Iceberg REST catalog spec", url: "https://iceberg.apache.org/rest-catalog-spec/" },
      { label: "Iceberg AWS catalog docs", url: "https://iceberg.apache.org/docs/latest/aws/" },
    ],
  },
  s3: {
    title: "Set up Amazon S3",
    steps: [
      "Create an IAM access key pair (IAM → Users → Security credentials → Access keys) or use an instance role.",
      "Enter Access Key ID, Secret Access Key, region, and the bucket name.",
      "Policy needs s3:ListBucket + s3:GetObject to read, s3:PutObject (+ DeleteObject for overwrite) to write.",
      "Least privilege: scope Resource to arn:aws:s3:::bucket and arn:aws:s3:::bucket/*.",
      "S3-compatible stores (MinIO, Wasabi, R2, Spaces): same fields plus the endpoint URL.",
      "Test proves ListBucket works — SignatureDoesNotMatch is the secret key, AccessDenied is the policy.",
    ],
    links: [
      { label: "Create IAM access keys", url: "https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html" },
      { label: "S3 bucket policy examples", url: "https://docs.aws.amazon.com/AmazonS3/latest/userguide/example-bucket-policies.html" },
    ],
  },
  gcs: {
    title: "Set up Google Cloud Storage",
    steps: [
      "Console → IAM & Admin → Service Accounts → create a service account.",
      "Grant Storage Object Viewer (read) or Storage Object Admin (write) on the bucket or project.",
      "Keys tab → Add key → JSON — paste the whole JSON into Service account key.",
      "Enter the bucket name exactly as the console shows it.",
      "Test proves bucket listing works — 403 means the role is missing, invalid key JSON means the file was edited.",
    ],
    links: [
      { label: "Create a service-account key", url: "https://cloud.google.com/iam/docs/keys-create-delete" },
      { label: "GCS IAM roles", url: "https://cloud.google.com/storage/docs/access-control/iam-roles" },
    ],
  },
  azure_blob: {
    title: "Set up Azure Blob Storage",
    steps: [
      "Storage account → Access keys: copy the connection string (or account name + key).",
      "Or use a SAS token scoped to the container: Storage account → Shared access signature → generate with read/write/list as needed.",
      "Enter the container name the files live in.",
      "Test proves container listing works — AuthenticationFailed is the key/SAS, not the account name.",
    ],
    links: [
      { label: "Manage storage account keys", url: "https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage" },
      { label: "Create a SAS token", url: "https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview" },
    ],
  },
  adls: {
    title: "Set up Azure Data Lake (ADLS Gen2)",
    steps: [
      "Same credentials as Blob: storage account access key, connection string, or a container-scoped SAS token.",
      "Enter the filesystem (container) name — ADLS calls buckets filesystems.",
      "For service-principal auth instead of keys: App registrations → client ID + secret, then give it Storage Blob Data Contributor on the account.",
      "Test proves filesystem listing works — AuthorizationFailure is RBAC or the SAS scope.",
    ],
    links: [
      { label: "ADLS Gen2 access control", url: "https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control" },
      { label: "Manage storage account keys", url: "https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage" },
    ],
  },
  sftp: {
    title: "Set up SFTP",
    steps: [
      "Pick SFTP for a remote file host — not a database.",
      "Enter host, port (default 22), username, and either the password or the private key file.",
      "Key auth: the public key must already be in the server's authorized_keys — ask the host admin which they issued.",
      "Enter the remote path to the CSV, Excel, or Parquet file (or directory for folder syncs).",
      "Test before you save. Then use Transfer to write into Postgres or Snowflake.",
    ],
    links: [{ label: "OpenSSH key-based auth", url: "https://www.openssh.com/manual.html" }],
  },
  kafka: {
    title: "Set up Kafka",
    steps: [
      "Get the bootstrap server list (host:9092) from your platform team — Confluent Cloud shows it under Cluster settings.",
      "Auth: PLAIN/SCRAM username+password (Confluent API key + secret), or TLS certs for mTLS clusters.",
      "The principal needs Read/Describe on source topics or Write/Create on destination topics.",
      "Test proves a metadata call answers — SASL authentication failed is the key, timeout is the listener/VPC.",
    ],
    links: [
      { label: "Kafka security config", url: "https://kafka.apache.org/documentation/#security" },
      { label: "Confluent Cloud API keys", url: "https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/overview.html" },
    ],
  },
  salesforce: {
    title: "Set up Salesforce",
    steps: [
      "Use your Salesforce username + password plus the security token (My Settings → Reset My Security Token — it arrives by email).",
      "Or create a Connected App (Setup → App Manager → New Connected App) with OAuth scopes api and refresh_token for client-credentials login.",
      "Enter the instance URL — https://yourorg.salesforce.com (or .sandbox for sandboxes).",
      "Test proves OAuth login works — LOGIN_MUST_USE_SECURITY_TOKEN means append the token to the password.",
    ],
    links: [
      { label: "Create a Connected App", url: "https://help.salesforce.com/s/articleView?id=sf.connected_app_create.htm" },
      { label: "Reset your security token", url: "https://help.salesforce.com/s/articleView?id=sf.user_security_token.htm" },
    ],
  },
  hubspot: {
    title: "Set up HubSpot",
    steps: [
      "Settings → Integrations → Private Apps → create a private app.",
      "Give it the scopes the sync needs: crm.objects.contacts.read (and .write for destinations), plus companies/deals as required.",
      "Copy the access token it shows — paste it in the token field.",
      "Test proves the token answers — 401 is a revoked or mistyped token, 403 is a missing scope.",
    ],
    links: [{ label: "HubSpot private apps", url: "https://developers.hubspot.com/docs/api/private-apps" }],
  },
  stripe: {
    title: "Set up Stripe",
    steps: [
      "Developers → API keys → copy a secret key (sk_live_… or sk_test_… for sandbox data).",
      "Prefer a restricted key with read access on only the resources you sync (charges, customers, invoices).",
      "Paste it in the API key field — treat it like a password, it moves money.",
      "Test proves the key answers — a 401 means the key was rolled or mistyped.",
    ],
    links: [
      { label: "Stripe API keys", url: "https://docs.stripe.com/keys" },
      { label: "Restricted keys", url: "https://docs.stripe.com/keys-best-practices" },
    ],
  },
  github: {
    title: "Set up GitHub",
    steps: [
      "GitHub → Settings → Developer settings → Personal access tokens → fine-grained token.",
      "Scope it to the repositories you sync with contents:read and metadata:read.",
      "Enter the token and the owner/repo or organization to read.",
      "Test proves the token answers — 401 is a bad token, 404 means the repo is not in scope.",
    ],
    links: [{ label: "Create a personal access token", url: "https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token" }],
  },
  jira: {
    title: "Set up Jira Cloud",
    steps: [
      "id.atlassian.com → Security → API tokens → Create API token.",
      "Enter your Atlassian email + the API token + site (yoursite.atlassian.net).",
      "The account needs Browse projects permission on the projects you read.",
      "Test proves /rest/api/3/myself answers — 401 is email/token, not the site URL.",
    ],
    links: [{ label: "Create an API token", url: "https://id.atlassian.com/manage-profile/security/api-tokens" }],
  },
  shopify: {
    title: "Set up Shopify",
    steps: [
      "Admin → Settings → Apps and sales channels → Develop apps → create an app.",
      "Configure Admin API scopes for the resources you sync (read_orders, read_products, read_customers).",
      "Install the app, then copy the Admin API access token (shown once).",
      "Enter your store as yourshop.myshopify.com.",
      "Test proves the token answers — 401 is the token, 404 is the shop domain.",
    ],
    links: [{ label: "Admin API access tokens", url: "https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens" }],
  },
  rest_api: {
    title: "Set up a REST / OpenAPI source",
    steps: [
      "Enter the base URL the vendor documents for API access — it is usually different from the app login page.",
      "Pick the auth style the vendor issues: Bearer token, API key (header or query), Basic auth, or OAuth client credentials.",
      "Get the credential from the vendor's developer console — every SaaS issues keys under Settings → Developers/API.",
      "If the API paginates, name the page/next-cursor parameter in the advanced fields.",
      "Test proves one authenticated GET answers — a 401 is the credential, a 404 is the URL.",
    ],
    links: [{ label: "OpenAPI spec (for API docs)", url: "https://spec.openapis.org/oas/v3.1.0" }],
  },
  graphql: {
    title: "Set up a GraphQL source",
    steps: [
      "Enter the GraphQL endpoint URL (e.g. https://api.vendor.com/graphql).",
      "Most GraphQL APIs take a Bearer token or API-key header — get it from the vendor's developer console.",
      "Provide the query for the objects you sync, or let introspection suggest one.",
      "Test proves the endpoint answers an authenticated POST.",
    ],
    links: [{ label: "GraphQL spec", url: "https://spec.graphql.org/" }],
  },
  excel: {
    title: "Set up Excel",
    steps: [
      "Pick Excel for a workbook file — not a database.",
      "Provide a .xlsx path or object-store URI. Prefer .xlsx over legacy .xls.",
      "Test proves the file is readable. A missing path is not Connected.",
    ],
    links: [],
  },
  file_flat: {
    title: "Set up a file source",
    steps: [
      "Provide the path to the file — a local path or an object-store URI (s3://, gs://, az://).",
      "CSV/TSV: delimiter and header row are detected; set them explicitly if detection guesses wrong.",
      "JSON/JSONL: JSON Lines is one object per line — use it for exports larger than memory.",
      "Parquet/Avro/ORC carry their own schema — mapping is automatic.",
      "Test proves the file opens and parses — a missing path is not Connected.",
    ],
    links: [],
  },
  neo4j: {
    title: "Set up Neo4j",
    steps: [
      "Get the Bolt endpoint (host:7687) — Aura shows it under Connection URI.",
      "Use the neo4j user or a user with read access on the source database.",
      "Aura requires TLS — keep encryption on.",
      "Test proves the Bolt handshake answers — unauthorized is credentials, timeout is routing.",
    ],
    links: [{ label: "Neo4j connection URIs", url: "https://neo4j.com/docs/bolt/current/bolt/" }],
  },
  couchbase: {
    title: "Set up Couchbase",
    steps: [
      "Get the cluster host (couchbase://host or host:8091) plus a bucket-scoped user + password.",
      "Capella: cluster → Connect shows the connection string; Database Access holds the credentials.",
      "The user needs Bucket Read on sources or Bucket Write on the destination.",
      "Test proves the cluster manager answers.",
    ],
    links: [{ label: "Couchbase connections", url: "https://docs.couchbase.com/server/current/learn/clusters-and-availability/connectivity.html" }],
  },
  influxdb: {
    title: "Set up InfluxDB",
    steps: [
      "Get the server URL (http://host:8086 or the Cloud region URL) and an API token.",
      "Cloud: Load Data → API Tokens → generate a read (or all-access) token scoped to the bucket.",
      "Enter organization and bucket — both are required on v2.",
      "Test proves a buckets/query call answers.",
    ],
    links: [{ label: "InfluxDB API tokens", url: "https://docs.influxdata.com/influxdb/v2/admin/tokens/" }],
  },
  email: {
    title: "Set up Email (SMTP)",
    steps: [
      "Get the SMTP host, port (587 starttls or 465 TLS), user, and password/app-password from the mail provider.",
      "Gmail needs an app password (account requires 2-Step Verification first) — the normal password is rejected.",
      "Microsoft 365: smtp.office365.com — ask the admin whether SMTP AUTH is enabled for the mailbox.",
      "Test proves a real authenticated login — 535 is auth, timeout is the port.",
    ],
    links: [
      { label: "Gmail app passwords", url: "https://support.google.com/accounts/answer/185833" },
      { label: "Microsoft 365 SMTP settings", url: "https://learn.microsoft.com/en-us/exchange/mail-flow-best-practices/how-to-set-up-a-multifunction-device-or-application-to-send-email-using-microsoft-365-or-office-365" },
    ],
  },
  generic_sql: {
    title: "Set up a generic SQL source",
    steps: [
      "Paste a SQLAlchemy-style URL: dialect+driver://user:pass@host:port/db.",
      "The dialect must have a driver installed — common ones (mysql+pymysql, postgresql+psycopg2, mssql+pymssql) work out of the box.",
      "Ask the DBA for SELECT on source tables or CREATE/INSERT on the destination.",
      "Test proves the dialect loads, the host answers, and the login works.",
    ],
    links: [{ label: "SQLAlchemy dialect URLs", url: "https://docs.sqlalchemy.org/en/20/core/engines.html" }],
  },
  gitlab: {
    title: "Set up GitLab",
    steps: [
      "GitLab → User Settings → Personal access tokens → create one with read_api scope.",
      "Self-managed GitLab: enter your instance URL (https://gitlab.your-company.com/api/v4); gitlab.com works by default.",
      "The token reads projects, issues, and merge requests across the groups it can see — use a bot/service account for full visibility.",
      "Test proves /projects answers — 401 is the token, 404 is the base URL.",
    ],
    links: [
      { label: "GitLab personal access tokens", url: "https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html" },
      { label: "GitLab REST API", url: "https://docs.gitlab.com/ee/api/rest/" },
    ],
  },
  mailchimp: {
    title: "Set up Mailchimp",
    steps: [
      "Mailchimp → Account → Extras → API keys → Create A Key.",
      "The data center is the key's suffix (key-us21 → dc us21) — it is derived automatically if you only paste the key.",
      "Any username works for Mailchimp basic auth — the key is the credential.",
      "Test proves /lists answers — 401 is the key, not the data center.",
    ],
    links: [
      { label: "Mailchimp Marketing API", url: "https://mailchimp.com/developer/marketing/api/" },
      { label: "About API keys", url: "https://mailchimp.com/help/about-api-keys/" },
    ],
  },
  pipedrive: {
    title: "Set up Pipedrive",
    steps: [
      "Pipedrive → Settings → Personal preferences → API → copy your personal API token.",
      "Domain is your company subdomain — acme in acme.pipedrive.com.",
      "Company API tokens (admin-issued) also work and see shared data.",
      "Test proves /deals answers — 401 is the token.",
    ],
    links: [
      { label: "Pipedrive API reference", url: "https://developers.pipedrive.com/docs/api/v1" },
      { label: "Find your API token", url: "https://support.pipedrive.com/en/article/how-to-find-the-api-token" },
    ],
  },
  klaviyo: {
    title: "Set up Klaviyo",
    steps: [
      "Klaviyo → Settings → API keys → Create Private API Key with read access to the objects you sync.",
      "Private keys start with pk_ — public site keys cannot read the API.",
      "The revision header is pinned to 2024-10-15; no extra config needed.",
      "Test proves /lists answers — 401 is the private key.",
    ],
    links: [
      { label: "Klaviyo API overview", url: "https://developers.klaviyo.com/en/reference/api-overview" },
      { label: "Create a private API key", url: "https://help.klaviyo.com/hc/en-us/articles/7423954176283" },
    ],
  },
  servicenow: {
    title: "Set up ServiceNow",
    steps: [
      "Instance is your subdomain — dev123 in dev123.service-now.com.",
      "Use a service account with the rest_api_explorer or table-level read roles (itil for incidents, approver_user for change).",
      "OAuth is also supported by ServiceNow — basic auth here needs an integration user, not SSO.",
      "Test proves the Table API answers — 401 is credentials, ACL-denied reads return an empty result.",
    ],
    links: [
      { label: "ServiceNow Table API", url: "https://www.servicenow.com/docs/bundle/zurich-api-reference/page/integrate/inbound-rest/concept/c_TableAPI.html" },
      { label: "REST API roles", url: "https://www.servicenow.com/docs/bundle/zurich-api-reference/page/integrate/inbound-rest/concept/c_RESTAPI.html" },
    ],
  },
  freshdesk: {
    title: "Set up Freshdesk",
    steps: [
      "Freshdesk → Profile settings → View API key — the key is the credential (password stays X).",
      "Domain is your helpdesk subdomain — acme in acme.freshdesk.com.",
      "The key inherits the agent's permissions — use an agent that can read tickets/contacts.",
      "Test proves /tickets answers — 401 is the key or domain.",
    ],
    links: [
      { label: "Freshdesk API reference", url: "https://developers.freshdesk.com/api/" },
      { label: "Where is my API key", url: "https://support.freshdesk.com/en/support/solutions/articles/215517" },
    ],
  },
  slack: {
    title: "Set up Slack",
    steps: [
      "api.slack.com/apps → your app → OAuth & Permissions → copy the Bot User OAuth Token (xoxb-…).",
      "The bot needs scopes: users:read, channels:read, groups:read, and channels:history / groups:history for message reads.",
      "Channel ID is only needed if you want the message-history stream — right-click a channel → View channel details → copy the ID (C…).",
      "Test proves users.list answers — invalid_auth is the token, missing_scope is a permission the app still needs.",
    ],
    links: [
      { label: "Slack app management", url: "https://api.slack.com/apps" },
      { label: "Web API methods", url: "https://api.slack.com/methods" },
    ],
  },
  asana: {
    title: "Set up Asana",
    steps: [
      "Asana → Apps → Manage developer apps (or developers.asana.com) → Create a Personal Access Token.",
      "Workspace GID: open the workspace in Asana and copy the numeric gid from the URL, or call /workspaces.",
      "The token reads everything the owning user can see — use a service account for least privilege.",
      "Test proves /workspaces answers — 401 is the token, 404 on tasks is a wrong workspace GID.",
    ],
    links: [
      { label: "Asana developer console", url: "https://developers.asana.com/docs/personal-access-token" },
      { label: "Asana API reference", url: "https://developers.asana.com/reference/rest-api-reference" },
    ],
  },
  calendly: {
    title: "Set up Calendly",
    steps: [
      "calendly.com → Integrations → API & webhooks → Generate a personal access token.",
      "User URI: call GET /users/me with the token — the 'uri' field looks like https://api.calendly.com/users/ABCD…",
      "The token sees the owning user's event types and scheduled events only (not the whole org).",
      "Test proves event_types answers — 401 is the token; an empty collection means the User URI is wrong.",
    ],
    links: [
      { label: "Calendly API docs", url: "https://developer.calendly.com/api-docs" },
      { label: "Personal access tokens", url: "https://help.calendly.com/hc/en-us/articles/223448448-Integrating-with-APIs" },
    ],
  },
  woocommerce: {
    title: "Set up WooCommerce",
    steps: [
      "WordPress admin → WooCommerce → Settings → Advanced → REST API → Add key: Read permission, copy the consumer key (ck_…) and secret (cs_…).",
      "Domain is your store host — shop.example.com (HTTPS required; the REST API refuses plain HTTP for basic auth).",
      "Consumer key is the username field, consumer secret is the password field.",
      "Test proves /orders answers — 401 is the keys, 404 is the domain or a missing REST API rewrite.",
    ],
    links: [
      { label: "WooCommerce REST API docs", url: "https://woocommerce.github.io/woocommerce-rest-api-docs/" },
      { label: "Generate API keys", url: "https://woocommerce.github.io/woocommerce-rest-api-docs/#authentication" },
    ],
  },
  bigcommerce: {
    title: "Set up BigCommerce",
    steps: [
      "BigCommerce admin → Settings → API accounts → Create API account (V2/V3 token) — grant read on Products, Customers, Orders.",
      "Store hash: the alphanumeric segment in your admin URL — store-abc123.mybigcommerce.com → abc123 (or shown on the API accounts page).",
      "Access token goes in the API key field — it is sent as the X-Auth-Token header.",
      "Test proves v3 products answers — 401 is the token, 404 is a wrong store hash.",
    ],
    links: [
      { label: "BigCommerce API accounts", url: "https://support.bigcommerce.com/s/article/Store-API-Accounts" },
      { label: "REST Management API", url: "https://developer.bigcommerce.com/docs/rest-management" },
    ],
  },
  brevo: {
    title: "Set up Brevo",
    steps: [
      "Brevo → SMTP & API → API Keys → Generate a new API key (xkeysib-…).",
      "The key sees contacts, lists, and campaigns for the account — restrict it in Brevo if you want read-only scope.",
      "No domain or tenant field needed — the API key alone identifies the account.",
      "Test proves /contacts answers — 401 is the key.",
    ],
    links: [
      { label: "Brevo API reference", url: "https://developers.brevo.com/reference" },
      { label: "Manage API keys", url: "https://help.brevo.com/hc/en-us/articles/209467485" },
    ],
  },
  singer_tap: {
    title: "Set up a Singer tap",
    steps: [
      "Pick the tap package for the source (tap-postgres, tap-salesforce, tap-mysql…).",
      "Provide the tap's config JSON — each tap's README lists the fields it needs.",
      "Provide catalog/state if you are resuming an extraction.",
      "Test proves the tap runs discovery against the credentials.",
    ],
    links: [{ label: "Singer spec & tap list", url: "https://github.com/singer-io/getting-started" }],
  },
};

/** Family fallbacks: catalog types that share another type's setup flow. */
const FAMILY: Record<string, string> = {
  pgvector: "postgresql",
  supabase: "postgresql",
  timescaledb: "postgresql",
  cockroachdb: "postgresql",
  openshift: "postgresql",
  mariadb: "mysql",
  singlestore: "mysql",
  redshift_compat: "postgresql",
  google_cloud_storage: "gcs",
  minio: "s3",
  tsv: "file_flat",
  jsonl: "file_flat",
  ndjson: "file_flat",
  avro: "file_flat",
  orc: "file_flat",
  xml: "file_flat",
  yaml: "file_flat",
  fixed_width: "file_flat",
  pdf: "file_flat",
  docx: "file_flat",
  html: "file_flat",
  json: "file_flat",
  csv: "file_flat",
  parquet: "file_flat",
};

export function getConnectorSetupGuide(type: string, resolvedType?: string): ConnectorSetupGuide {
  const ids = [type, resolvedType].map((t) => (t || "").toLowerCase().trim()).filter(Boolean);
  for (const id of ids) {
    if (GUIDES[id]) return GUIDES[id];
    const fam = FAMILY[id];
    if (fam && GUIDES[fam]) return GUIDES[fam];
  }
  return GENERIC;
}
