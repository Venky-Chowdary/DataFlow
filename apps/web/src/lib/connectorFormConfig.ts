import { GENERIC_SQL_INFO } from "./genericSqlMap";
import {
  getConnectorDefaults,
  isAwsConnector,
  isGcpConnector,
  isGenericSql,
  resolveCatalogIdToType,
  resolveDriverType,
} from "./connectorTypes";

/** Authentication mode supported by a connector form. */
export type AuthMode =
  | "user_pass"
  | "connection_string"
  | "service_account"
  | "aws_keys"
  | "api_key"
  | "file_path";

/** Single form field descriptor. */
export interface FormField {
  key: string;
  label: string;
  type?: "text" | "number" | "password" | "textarea" | "checkbox";
  placeholder?: string;
  optional?: boolean;
  hint?: string;
  rows?: number;
  /** If true, treat the field as sensitive (never log, show toggle). */
  sensitive?: boolean;
}

/** Auth mode entry with label, fields, and validation. */
export interface AuthModeConfig {
  value: AuthMode;
  label: string;
  fields: FormField[];
  validate: (values: Record<string, unknown>) => string | null;
}

/** Complete per-connector form configuration. */
export interface ConnectorFormConfig {
  type: string;
  label: string;
  defaultAuthMode: AuthMode;
  authModes: AuthModeConfig[];
  commonFields: FormField[];
}

type FieldBuilder = (opts?: { optional?: boolean; placeholder?: string }) => FormField;

const text = (key: string, label: string, opts: Omit<FormField, "key" | "label" | "type"> = {}): FormField => ({
  key,
  label,
  type: "text",
  ...opts,
});

const password = (key: string, label: string, opts: Omit<FormField, "key" | "label" | "type"> = {}): FormField => ({
  key,
  label,
  type: "password",
  sensitive: true,
  ...opts,
});

const textarea = (key: string, label: string, opts: Omit<FormField, "key" | "label" | "type"> = {}): FormField => ({
  key,
  label,
  type: "textarea",
  ...opts,
});

const number = (key: string, label: string, opts: Omit<FormField, "key" | "label" | "type"> = {}): FormField => ({
  key,
  label,
  type: "number",
  ...opts,
});

const checkbox = (key: string, label: string, opts: Omit<FormField, "key" | "label" | "type"> = {}): FormField => ({
  key,
  label,
  type: "checkbox",
  ...opts,
});

function fmt(values: Record<string, unknown>, key: string): string {
  return typeof values[key] === "string" ? (values[key] as string).trim() : "";
}

function required(values: Record<string, unknown>, key: string, label: string): string | null {
  if (!fmt(values, key)) return `${label} is required.`;
  return null;
}

function requiredAny(values: Record<string, unknown>, keys: string[], message: string): string | null {
  if (keys.some((k) => fmt(values, k))) return null;
  return message;
}

function auth(mode: AuthMode, label: string, fields: FormField[], validate: (values: Record<string, unknown>) => string | null): AuthModeConfig {
  return { value: mode, label, fields, validate };
}

/** Generic SQL placeholder derived from the generic SQL map. */
function genericSqlPlaceholder(type: string): string {
  const t = resolveCatalogIdToType(type);
  const info = GENERIC_SQL_INFO[t];
  const base = info?.base ?? type;
  const port = info?.port ?? 5432;

  if (base === "sqlite" || base === "duckdb") return "sqlite:////path/to/db.sqlite";
  if (base === "duckdb") return "duckdb:////path/to/file.duckdb";
  if (base === "mssql+pyodbc") return `mssql+pyodbc://user:pass@host:${port}/db?driver=ODBC+Driver+17+for+SQL+Server`;
  if (base === "oracle+oracledb") return `oracle+oracledb://user:pass@host:${port}/SERVICE`;
  if (base === "ibm_db_sa") return `ibm_db_sa://user:pass@host:${port}/db`;
  if (base === "clickhouse+native") return `clickhouse+native://user:pass@host:${port}/db`;
  if (base === "dremio+flight") return `dremio+flight://user:pass@host:${port}/dremio`;
  if (base === "awsathena+rest") return `awsathena+rest://@athena.us-east-1.amazonaws.com:443/?s3_staging_dir=s3://bucket`;
  if (base === "hana") return `hana://user:pass@host:${port}/DB`;
  if (base === "databricks") return `databricks+thrift://token:dapi***@xxx.cloud.databricks.com:443?http_path=/sql/1.0/endpoints/...`;
  if (base === "presto" || base === "trino") return `${base}://user:pass@host:${port}/catalog`;
  if (base === "druid") return `druid://user:pass@host:${port}/druid/v2/sql`;
  if (base === "teradatasql") return `teradatasql://user:pass@host:${port}/db`;
  if (base.startsWith("mysql")) return `${base}://user:pass@host:${port}/db`;
  if (base.startsWith("postgresql")) return `${base}://user:pass@host:${port}/db`;

  return `${base}://user:pass@host:${port}/db`;
}

/** Build the form configuration for a connector type. */
export function getConnectorFormConfig(type: string): ConnectorFormConfig {
  const resolved = resolveCatalogIdToType(type);
  const driver = resolveDriverType(type);
  const { label, host, port } = getConnectorDefaults(type);

  const isGeneric = isGenericSql(resolved);
  const isAws = isAwsConnector(resolved);
  const isGcp = isGcpConnector(resolved);
  const isS3 = resolved === "s3";
  const isDynamo = resolved === "dynamodb";
  const isRedis = resolved === "redis";
  const isMongo = resolved === "mongodb";
  const isElastic = resolved === "elasticsearch";
  const isSnowflake = resolved === "snowflake";
  const isBigQuery = resolved === "bigquery";
  const isSftp = resolved === "sftp";
  const isEmail = resolved === "email";
  const isSQLite = resolved === "sqlite";
  const isDuckDB = resolved === "duckdb";
  const isFile = ["csv", "tsv", "json", "jsonl", "ndjson", "parquet", "excel"].includes(resolved);
  const isAzure = resolved === "adls";
  const isSaaS = ["salesforce", "hubspot", "stripe"].includes(resolved);

  const authModes: AuthModeConfig[] = [];

  // File formats
  if (isFile) {
    authModes.push(
      auth("file_path", "Local / mounted file path", [text("connection_string", "File path or URL", { placeholder: "/mnt/data/files, s3://bucket/file.csv, or https://host/file.csv" })], (values) =>
        required(values, "connection_string", "File path or URL")
      ),
      auth("connection_string", "URL or object-store URI", [text("connection_string", "URL or URI", { placeholder: "s3://bucket/path or https://host/file.csv" })], (values) =>
        required(values, "connection_string", "URL or URI")
      )
    );
    return {
      type: resolved,
      label,
      defaultAuthMode: "file_path",
      authModes,
      commonFields: [],
    };
  }

  // username + password mode for most connectors
  const userPassFields: FormField[] = [];
  if (isGeneric || ["postgresql", "mysql", "redshift", "mariadb", "cockroachdb", "timescaledb", "singlestore", "supabase", "neon"].includes(resolved)) {
    userPassFields.push(
      text("host", "Host", { placeholder: host || "localhost" }),
      number("port", "Port", { placeholder: String(port || 5432) }),
      text("database", "Database", { placeholder: "mydb" }),
      text("username", "Username"),
      password("password", "Password"),
      checkbox("ssl", "Use SSL / TLS", { hint: "Encrypt the connection to the server." })
    );
  } else if (isMongo) {
    userPassFields.push(
      text("host", "Host", { placeholder: "cluster0.mongodb.net" }),
      number("port", "Port", { placeholder: "27017" }),
      text("database", "Database", { placeholder: "mydb" }),
      text("username", "Username"),
      password("password", "Password"),
      text("authSource", "Auth source", { placeholder: "admin", hint: "Database where the user is defined." }),
      checkbox("ssl", "Use TLS / SSL", { hint: "Required for MongoDB Atlas and most cloud deployments." })
    );
  } else if (isSnowflake) {
    userPassFields.push(
      text("host", "Account host", { placeholder: "account.snowflakecomputing.com" }),
      text("username", "Username"),
      password("password", "Password"),
      text("database", "Database"),
      text("schema", "Schema", { placeholder: "PUBLIC" }),
      text("warehouse", "Warehouse", { placeholder: "COMPUTE_WH" }),
      text("authRole", "Role", { placeholder: "ACCOUNTADMIN", optional: true })
    );
  } else if (isRedis) {
    userPassFields.push(
      text("host", "Host", { placeholder: "localhost" }),
      number("port", "Port", { placeholder: "6379" }),
      text("database", "Database index", { placeholder: "0" }),
      text("username", "Username (ACL)", { optional: true, hint: "Redis 6+ ACL username. Leave blank for legacy auth." }),
      password("password", "Password"),
      checkbox("ssl", "Use TLS / SSL")
    );
  } else if (isElastic) {
    userPassFields.push(
      text("host", "Host", { placeholder: "localhost:9200 or Elastic Cloud endpoint" }),
      text("database", "Index (optional)", { optional: true }),
      text("username", "Username"),
      password("password", "Password"),
      checkbox("ssl", "Use HTTPS / TLS")
    );
  } else if (isSftp) {
    userPassFields.push(
      text("host", "SFTP host", { placeholder: "sftp.example.com" }),
      number("port", "Port", { placeholder: "22" }),
      text("username", "Username"),
      password("password", "Password (optional if using key)", { optional: true }),
      text("database", "Remote path / directory", { optional: true, placeholder: "/uploads" }),
      textarea("privateKey", "SSH private key (optional)", { rows: 4, optional: true, placeholder: "-----BEGIN OPENSSH PRIVATE KEY----- ...", hint: "Paste the private key text. If provided, password is optional." })
    );
  } else if (isEmail) {
    userPassFields.push(
      text("host", "SMTP host", { placeholder: "smtp.gmail.com" }),
      number("port", "Port", { placeholder: "587" }),
      text("authSource", "From address", { placeholder: "noreply@dataflow.com" }),
      text("database", "Recipients", { placeholder: "alice@example.com, bob@example.com" }),
      text("username", "SMTP username"),
      password("password", "SMTP password"),
      checkbox("ssl", "Use TLS / STARTTLS", { hint: "Enable for ports 587/465." })
    );
  } else if (isSQLite || isDuckDB) {
    userPassFields.push(
      text("database", isSQLite ? "Database file / :memory:" : "DuckDB file", { placeholder: isSQLite ? "/path/to/db.sqlite" : "/path/to/file.duckdb" })
    );
  } else if (isAzure) {
    userPassFields.push(
      text("host", "Storage account name", { placeholder: "mystorageaccount" }),
      text("database", "Container / filesystem", { placeholder: "my-container" }),
      text("username", "Account name", { optional: true, hint: "Optional when using connection string." }),
      password("password", "Account key", { optional: true, hint: "Optional when using connection string." })
    );
  }

  // connection string mode
  const connStrFields: FormField[] = [];
  if (isGeneric) {
    connStrFields.push(
      textarea("connection_string", "SQLAlchemy connection URL", {
        rows: 2,
        placeholder: genericSqlPlaceholder(type),
        hint: "Paste the full SQLAlchemy URL for your engine. DataFlow will validate connectivity and introspect schema.",
      })
    );
  } else if (isMongo) {
    connStrFields.push(
      textarea("connection_string", "MongoDB connection string", {
        rows: 2,
        placeholder: "mongodb+srv://user:pass@cluster.mongodb.net/mydb?retryWrites=true&w=majority",
        hint: "mongodb:// or mongodb+srv:// URL. DataFlow will auto-detect authSource and TLS settings.",
      }),
      text("authSource", "Auth source override", { optional: true, placeholder: "admin" }),
      checkbox("ssl", "Force TLS / SSL", { optional: true, hint: "Toggle on if the URI does not include tls=true." })
    );
  } else if (isRedis) {
    connStrFields.push(
      textarea("connection_string", "Redis URL", {
        rows: 2,
        placeholder: "redis://user:pass@localhost:6379/0",
        hint: "redis:// or rediss:// URL. rediss:// enables TLS.",
      })
    );
  } else if (isElastic) {
    connStrFields.push(
      textarea("connection_string", "Elasticsearch URL", {
        rows: 2,
        placeholder: "https://user:pass@localhost:9200",
      })
    );
  } else if (isSftp) {
    connStrFields.push(
      textarea("connection_string", "SFTP URL", {
        rows: 2,
        placeholder: "sftp://user:pass@sftp.example.com:22/path",
        hint: "The path component becomes the remote directory or file.",
      }),
      textarea("privateKey", "SSH private key (optional)", { rows: 4, optional: true })
    );
  } else if (isEmail) {
    connStrFields.push(
      textarea("connection_string", "SMTP URL", {
        rows: 2,
        placeholder: "smtp://user:pass@smtp.gmail.com:587/?from=noreply@dataflow.com&to=alice@example.com",
        hint: "smtp:// or smtps:// URL. Use query params from= and to= for sender/recipients.",
      })
    );
  } else if (isAzure) {
    connStrFields.push(
      textarea("connection_string", "Azure Blob connection string", {
        rows: 2,
        placeholder: "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net",
        hint: "Found under Access keys in the Azure Portal.",
      }),
      text("database", "Container / filesystem", { optional: true })
    );
  } else if (isSnowflake) {
    connStrFields.push(
      textarea("connection_string", "Snowflake URL", {
        rows: 2,
        placeholder: "snowflake://user:pass@account/db/schema?warehouse=COMPUTE_WH&role=ACCOUNTADMIN",
      })
    );
  } else if (isSQLite || isDuckDB) {
    connStrFields.push(
      textarea("connection_string", "SQLite / DuckDB file path", {
        rows: 2,
        placeholder: isSQLite ? "/path/to/db.sqlite" : "/path/to/file.duckdb",
      })
    );
  } else if (isGeneric || ["postgresql", "mysql", "redshift"].includes(resolved)) {
    connStrFields.push(
      textarea("connection_string", "Connection string", {
        rows: 2,
        placeholder: isSnowflake ? "" : genericSqlPlaceholder(type),
      })
    );
  }

  // service account mode
  const saFields: FormField[] = [];
  if (isGcp) {
    saFields.push(
      textarea("serviceAccount", "Service account JSON or file path", {
        rows: 6,
        placeholder: '{\n  "type": "service_account",\n  ...\n}',
        hint: "Paste the JSON contents from Google Cloud, or enter an absolute path to the key file on the server.",
      }),
      text("database", isBigQuery ? "GCP project ID" : "Bucket name")
    );
    if (isBigQuery) {
      saFields.push(text("schema", "Dataset", { optional: true, placeholder: "dataflow" }));
    }
  } else if (isAzure) {
    saFields.push(
      textarea("serviceAccount", "Service principal JSON", {
        rows: 4,
        placeholder: '{\n  "tenantId": "...",\n  "clientId": "...",\n  "clientSecret": "..."\n}',
        hint: "Azure AD application with Storage Blob Data Contributor role.",
      }),
      text("host", "Storage account name", { optional: true }),
      text("database", "Container / filesystem", { optional: true })
    );
  }

  // AWS keys mode
  const awsFields: FormField[] = [];
  if (isAws) {
    const endpointHint = isS3
      ? "For MinIO, LocalStack, Wasabi, or S3-compatible stores."
      : "For DynamoDB Local or private AWS-compatible stacks.";
    awsFields.push(
      text("host", isS3 ? "AWS region or endpoint" : "AWS region or local endpoint", { placeholder: isS3 ? "us-east-1" : "us-east-1" }),
      text("database", isS3 ? "Bucket name" : "Table name"),
      text("username", "Access Key ID"),
      password("password", "Secret Access Key"),
      text("endpointUrl", "Custom endpoint URL (optional)", { optional: true, placeholder: "https://s3.wasabisys.com or http://localhost:9000", hint: endpointHint })
    );
    if (isS3) {
      awsFields.push(checkbox("pathStyle", "Use path-style addressing", { optional: true, hint: "Required for MinIO and some S3-compatible stores." }));
    }
  }

  // API key mode (Elasticsearch + SaaS APIs)
  const apiFields: FormField[] = [];
  if (isElastic) {
    apiFields.push(
      text("host", "Host", { placeholder: "localhost:9200 or Elastic Cloud endpoint" }),
      text("database", "Index (optional)", { optional: true }),
      textarea("apiKey", "API key", {
        rows: 2,
        placeholder: "id:api_key or encoded API key",
        hint: "Enter id:secret for key pairs, or the full encoded key from Elastic Cloud.",
      }),
      checkbox("ssl", "Use HTTPS / TLS")
    );
  }
  if (isSaaS) {
    const defaultObject: Record<string, string> = {
      salesforce: "Account",
      hubspot: "contacts",
      stripe: "customers",
    };
    const placeholder = host || resolved;
    const objectHint = `Default object/table used when none is specified: ${defaultObject[resolved]}.`;
    apiFields.push(
      text("host", "Host / instance URL", { optional: true, placeholder }),
      text("database", "Object / table (optional)", { optional: true, placeholder: defaultObject[resolved] }),
      textarea("apiKey", resolved === "stripe" ? "Secret key" : "API token", {
        rows: 2,
        placeholder: resolved === "stripe" ? "sk_..." : "Paste access token",
        hint: `Paste the ${resolved === "stripe" ? "Stripe secret key" : resolved + " access token"}. ${objectHint}`,
      })
    );
  }

  // Build auth modes for each connector
  if (userPassFields.length) {
    authModes.push(
      auth("user_pass", "Username & password", userPassFields, (values) => {
        if (isSQLite || isDuckDB) {
          return required(values, "database", isSQLite ? "Database file" : "DuckDB file");
        }
        if (!isGcp && !isAws && !isElastic && !isRedis && !isSQLite && !isDuckDB && !fmt(values, "host")) {
          return "Host is required.";
        }
        if (
          !["gcs", "bigquery", "s3", "dynamodb", "adls", "elasticsearch", "redis", "sqlite", "duckdb"].includes(resolved) &&
          (values.port as number) <= 0
        ) {
          return "Port is required.";
        }
        if (
          !["gcs", "bigquery", "s3", "dynamodb", "adls", "elasticsearch", "redis", "sqlite", "duckdb"].includes(resolved) &&
          (!fmt(values, "username") || !fmt(values, "password"))
        ) {
          if (!isSftp) return "Username and password are required.";
        }
        if (isSftp && !fmt(values, "database") && !fmt(values, "connection_string")) {
          return "Remote path is required. Provide it as the SFTP URL or the path field.";
        }
        if (isEmail && !fmt(values, "database")) {
          return "At least one recipient (To) is required.";
        }
        if (isMongo && !fmt(values, "database")) {
          return "Database is required.";
        }
        return null;
      })
    );
  }

  if (connStrFields.length) {
    authModes.push(
      auth("connection_string", isEmail ? "SMTP URL" : isSftp ? "SFTP URL" : "Connection string", connStrFields, (values) =>
        required(values, "connection_string", isEmail ? "SMTP URL" : isSftp ? "SFTP URL" : "Connection string")
      )
    );
  }

  if (saFields.length) {
    const label = isGcp ? "Service account JSON" : "Service principal JSON";
    authModes.push(
      auth("service_account", label, saFields.filter(Boolean) as FormField[], (values) => {
        if (!fmt(values, "serviceAccount")) return `${label} is required.`;
        if (isGcp && !fmt(values, "database")) return isBigQuery ? "GCP project ID is required." : "Bucket name is required.";
        return null;
      })
    );
  }

  if (awsFields.length) {
    authModes.push(
      auth("aws_keys", "AWS access keys", awsFields.filter(Boolean) as FormField[], (values) => {
        if (!fmt(values, "host") && !fmt(values, "database")) {
          return isS3 ? "Region and bucket are required." : "Region and table name are required.";
        }
        const local = fmt(values, "host").includes("localhost") || fmt(values, "host").startsWith("http");
        if ((isS3 || !local) && (!fmt(values, "username") || !fmt(values, "password"))) {
          return "AWS Access Key ID and Secret Access Key are required.";
        }
        return null;
      })
    );
  }

  if (apiFields.length) {
    authModes.push(
      auth("api_key", "API key", apiFields, (values) => {
        if (!isSaaS && !fmt(values, "host")) return "Host is required.";
        if (!fmt(values, "apiKey")) return "API key is required.";
        return null;
      })
    );
  }

  // Fallback for anything not yet configured
  if (authModes.length === 0) {
    authModes.push(
      auth(
        "user_pass",
        "Username & password",
        [
          text("host", "Host", { placeholder: host || "localhost" }),
          number("port", "Port", { placeholder: String(port || 5432) }),
          text("database", "Database"),
          text("username", "Username"),
          password("password", "Password"),
          checkbox("ssl", "Use SSL / TLS"),
        ],
        (values) => {
          if (!fmt(values, "host")) return "Host is required.";
          if ((values.port as number) <= 0) return "Port is required.";
          if (!fmt(values, "username") || !fmt(values, "password")) return "Username and password are required.";
          return null;
        }
      )
    );
  }

  return {
    type: resolved,
    label,
    defaultAuthMode: inferDefaultAuthMode(resolved),
    authModes,
    commonFields: [],
  };
}

function inferDefaultAuthMode(resolved: string): AuthMode {
  if (["s3", "dynamodb"].includes(resolved)) return "aws_keys";
  if (["bigquery", "gcs"].includes(resolved)) return "service_account";
  if (["salesforce", "hubspot", "stripe"].includes(resolved)) return "api_key";
  if (resolved === "elasticsearch") return "api_key";
  if (["csv", "tsv", "json", "jsonl", "ndjson", "parquet", "excel"].includes(resolved)) return "file_path";
  return "user_pass";
}

/** Get the list of auth modes for a connector type. */
export function getAuthModes(type: string): AuthModeConfig[] {
  return getConnectorFormConfig(type).authModes;
}

/** Validate a connector form payload with a clear, field-level message. */
export function validateConnectorPayload(type: string, values: Record<string, unknown>, authMode: AuthMode): string | null {
  const cfg = getConnectorFormConfig(type);
  const mode = cfg.authModes.find((m) => m.value === authMode) || cfg.authModes[0];
  if (!mode) return "Unsupported connector type.";
  return mode.validate(values);
}
