import type { CredentialFields, SampleFileItem } from "@dataflow/design-system";
import { DATABASE_OPTIONS } from "./types";
import { buildConnectionString, detectDatabaseType, type ParsedConnection } from "./connectionString";

export const SAMPLE_FILES: SampleFileItem[] = [
  {
    id: "payments",
    label: "Payments",
    description: "AMT, CUST_ID, TXN_DT — banking feed",
    filename: "sample_payments.csv",
    format: "csv",
  },
  {
    id: "logistics",
    label: "Logistics",
    description: "Shipments, weight, tracking, cities",
    filename: "sample_logistics.csv",
    format: "csv",
  },
  {
    id: "retail",
    label: "Retail orders",
    description: "Orders, SKU, totals, quantities",
    filename: "sample_retail.csv",
    format: "csv",
  },
  {
    id: "synonyms",
    label: "Amount synonyms",
    description: "AMT, AMOUNT, value column variants",
    filename: "sample_synonyms.csv",
    format: "csv",
  },
  {
    id: "hr",
    label: "HR employees",
    description: "JSON array of employee records",
    filename: "sample_hr.json",
    format: "json",
  },
  {
    id: "tsv",
    label: "Payments TSV",
    description: "Tab-delimited payment export",
    filename: "sample_payments.tsv",
    format: "tsv",
  },
];

export function emptyCredentials(type = "postgresql"): CredentialFields {
  const opt = DATABASE_OPTIONS.find((d) => d.id === type)!;
  return {
    type,
    host: "",
    port: opt.defaultPort,
    database: "",
    username: "",
    password: "",
    schema: type === "snowflake" ? "PUBLIC" : "public",
    warehouse: "",
    ssl: true,
    connectionString: "",
  };
}

export function resolveConnectionString(connectionString: string, credentials: CredentialFields): string {
  if (credentials.username.trim() && credentials.host.trim()) {
    const parsed: ParsedConnection = {
      type: credentials.type as ParsedConnection["type"],
      host: credentials.host,
      port: credentials.port,
      database: credentials.database,
      username: credentials.username,
      password: credentials.password,
      schema: credentials.schema,
      connectionString: "",
      ssl: credentials.ssl,
      warehouse: credentials.warehouse,
    };
    return buildConnectionString(parsed);
  }
  return connectionString.trim();
}

export function credentialsFromConnectionString(connStr: string): CredentialFields {
  if (!connStr.trim()) return emptyCredentials();
  const type = detectDatabaseType(connStr);
  return { ...emptyCredentials(type), connectionString: connStr };
}
