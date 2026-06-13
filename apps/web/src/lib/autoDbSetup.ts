import {
  fetchSemanticMappings,
  introspectSchema,
  testConnection,
  type ColumnSchema,
  type MappingResult,
  type SemanticColumnAnalysis,
} from "./api";
import { parseConnectionString, toDatabaseConnection } from "./connectionString";
import type { DatabaseConnection, EndpointConfig } from "./types";
import { emptyEndpoint } from "./types";

export interface AutoConnectProgress {
  phase: "idle" | "source" | "destination" | "schema" | "semantic" | "done" | "error";
  message: string;
}

export interface AutoConnectResult {
  source: EndpointConfig;
  destination: EndpointConfig;
  selectedTables: string[];
  identityMappings: MappingResult[];
  semanticAnalysis?: SemanticColumnAnalysis[];
  error?: string;
}

const WAREHOUSE_TARGETS = [
  "customer_id",
  "payment_amount",
  "transaction_date",
  "account_number",
  "currency_code",
  "reference_number",
  "status",
  "description",
  "origin_city",
  "destination_city",
  "shipment_weight_kg",
  "tracking_number",
];

async function connectDb(
  connectionString: string,
  label: "Source" | "Destination"
): Promise<{ db: DatabaseConnection; ok: boolean; error?: string }> {
  const parsed = parseConnectionString(connectionString);
  const db = toDatabaseConnection(parsed);
  const result = await testConnection(db);
  if (!result.ok) {
    return { db, ok: false, error: result.error ?? `${label} connection failed` };
  }
  return {
    db: { ...db, tables: result.tables ?? [] },
    ok: true,
  };
}

async function loadTableSchema(db: DatabaseConnection, table: string) {
  const schema = await introspectSchema(db, table);
  if (!schema.ok) throw new Error(schema.error ?? `Could not read schema for ${table}`);
  return schema;
}

/** Identity map: every source column → same name on destination (auto-create friendly). */
export function buildIdentityMappings(
  columns: { name: string; inferred_type: string }[]
): MappingResult[] {
  return columns.map((c) => ({
    source: c.name,
    target: c.name,
    confidence: 1,
    reasoning: "Direct column match — full table sync",
    user_override: false,
  }));
}

async function buildSemanticMappings(
  sourceColumns: ColumnSchema[],
  targetColumns: ColumnSchema[],
  fileFormat?: string | null,
  onProgress?: (p: AutoConnectProgress) => void
): Promise<{ mappings: MappingResult[]; semanticAnalysis: SemanticColumnAnalysis[] }> {
  onProgress?.({ phase: "semantic", message: "Running AI semantic column mapping…" });

  const pipeline = await fetchSemanticMappings(
    sourceColumns.map((c) => c.name),
    targetColumns.map((c) => c.name),
    {
      fileFormat,
      sourceSchemas: sourceColumns,
      targetSchemas: targetColumns,
    }
  );

  return {
    mappings: pipeline.mappings,
    semanticAnalysis: pipeline.semantic_analysis ?? [],
  };
}

function inferTableName(fileName: string): string {
  return (
    fileName
      .replace(/\.[^.]+$/, "")
      .replace(/[^a-zA-Z0-9_]/g, "_")
      .slice(0, 40) || "imported_data"
  );
}

/**
 * Paste two connection strings → test both, discover tables, pick schema, build semantic mappings.
 */
export async function autoConnectDatabases(
  sourceConnStr: string,
  destConnStr: string,
  options: {
    selectedTable?: string;
    autoCreateDestination?: boolean;
    onProgress?: (p: AutoConnectProgress) => void;
  } = {}
): Promise<AutoConnectResult> {
  const report = (phase: AutoConnectProgress["phase"], message: string) => {
    options.onProgress?.({ phase, message });
  };

  if (!sourceConnStr.trim() || !destConnStr.trim()) {
    return {
      source: emptyEndpoint("database", "Source"),
      destination: emptyEndpoint("database", "Destination"),
      selectedTables: [],
      identityMappings: [],
      error: "Paste both source and destination connection strings.",
    };
  }

  try {
    report("source", "Connecting to source database…");
    const srcResult = await connectDb(sourceConnStr, "Source");
    if (!srcResult.ok) throw new Error(srcResult.error);

    report("destination", "Connecting to destination database…");
    const destResult = await connectDb(destConnStr, "Destination");
    if (!destResult.ok) throw new Error(destResult.error);

    const tables = srcResult.db.tables;
    if (tables.length === 0) {
      throw new Error("Source connected but no tables were found in the default schema.");
    }

    const table =
      options.selectedTable && tables.includes(options.selectedTable) ? options.selectedTable : tables[0];

    report("schema", `Reading schema for ${table}…`);
    const sourceSchema = await loadTableSchema(srcResult.db, table);

    let destDb = destResult.db;
    let destColumns = destResult.db.targetColumns;

    if (destResult.db.tables.includes(table)) {
      const destSchema = await loadTableSchema(destResult.db, table);
      destColumns = destSchema.columns;
      destDb = { ...destDb, targetColumns: destColumns, tables: destSchema.tables };
    } else if (options.autoCreateDestination !== false) {
      destColumns = sourceSchema.columns;
      destDb = { ...destDb, targetColumns: sourceSchema.columns };
    } else {
      throw new Error(`Table "${table}" not found on destination. Enable auto-create or pick another table.`);
    }

    const sourceCols: ColumnSchema[] = sourceSchema.columns.map((c) => ({
      name: c.name,
      inferred_type: c.inferred_type,
      nullable: c.nullable,
      samples: [],
    }));
    const targetCols: ColumnSchema[] = destColumns.map((c) => ({
      name: c.name,
      inferred_type: c.inferred_type,
      nullable: c.nullable,
      samples: [],
    }));

    const { mappings, semanticAnalysis } = await buildSemanticMappings(
      sourceCols,
      targetCols,
      null,
      options.onProgress
    );

    const sourceEndpoint: EndpointConfig = {
      ...emptyEndpoint("database", "Source"),
      kind: "database",
      connected: true,
      database: {
        ...srcResult.db,
        sourceTable: table,
        targetColumns: sourceSchema.columns,
        tables: sourceSchema.tables.length ? sourceSchema.tables : tables,
      },
    };

    const destEndpoint: EndpointConfig = {
      ...emptyEndpoint("database", "Destination"),
      kind: "database",
      connected: true,
      database: destDb,
    };

    report("done", `Ready — ${mappings.length} semantic mappings · ${table}`);

    return {
      source: sourceEndpoint,
      destination: destEndpoint,
      selectedTables: tables,
      identityMappings: mappings,
      semanticAnalysis,
    };
  } catch (e) {
    const message = e instanceof Error ? e.message : "Auto-connect failed";
    options.onProgress?.({ phase: "error", message });
    return {
      source: emptyEndpoint("database", "Source"),
      destination: emptyEndpoint("database", "Destination"),
      selectedTables: [],
      identityMappings: [],
      error: message,
    };
  }
}

/** File upload → database: semantic mapping from file columns to warehouse schema. */
export async function prepareFileToDatabase(
  source: EndpointConfig,
  destConnStr: string,
  options: { autoCreate?: boolean; onProgress?: (p: AutoConnectProgress) => void } = {}
): Promise<AutoConnectResult> {
  const report = (phase: AutoConnectProgress["phase"], message: string) => {
    options.onProgress?.({ phase, message });
  };

  if (!source.file.fileName || source.file.columns.length === 0) {
    return emptyResult("Upload a file first — CSV, Excel, JSON, PDF, Word, and more.");
  }
  if (!destConnStr.trim()) {
    return emptyResult("Paste a destination database connection string or pick a saved connector.");
  }

  try {
    report("destination", "Connecting to destination database…");
    const destResult = await connectDb(destConnStr, "Destination");
    if (!destResult.ok) throw new Error(destResult.error);

    const fileColumns = source.file.columns;
    const tableName = inferTableName(source.file.fileName);

    let targetColumns: ColumnSchema[] = WAREHOUSE_TARGETS.map((name) => ({
      name,
      inferred_type: "VARCHAR",
      nullable: true,
      samples: [],
    }));

    if (destResult.db.tables.includes(tableName)) {
      report("schema", `Reading destination table ${tableName}…`);
      const destSchema = await loadTableSchema(destResult.db, tableName);
      targetColumns = destSchema.columns.map((c) => ({
        name: c.name,
        inferred_type: c.inferred_type,
        nullable: c.nullable,
        samples: [],
      }));
    } else if (destResult.db.tables.length > 0 && options.autoCreate !== false) {
      const fallbackTable = destResult.db.tables[0];
      report("schema", `Inferring targets from ${fallbackTable}…`);
      const destSchema = await loadTableSchema(destResult.db, fallbackTable);
      targetColumns = destSchema.columns.map((c) => ({
        name: c.name,
        inferred_type: c.inferred_type,
        nullable: c.nullable,
        samples: [],
      }));
    }

    const { mappings, semanticAnalysis } = await buildSemanticMappings(
      fileColumns,
      targetColumns,
      source.file.detectedFormat,
      options.onProgress
    );

    const destEndpoint: EndpointConfig = {
      ...emptyEndpoint("database", "Destination"),
      kind: "database",
      connected: true,
      database: {
        ...destResult.db,
        targetColumns: targetColumns.map((c) => ({
          name: c.name,
          inferred_type: c.inferred_type,
          nullable: c.nullable,
        })),
      },
    };

    const sourceEndpoint: EndpointConfig = {
      ...source,
      kind: "file",
      connected: true,
    };

    report("done", `${mappings.length} columns mapped · AMT/AMOUNT/value synonyms resolved`);

    return {
      source: sourceEndpoint,
      destination: destEndpoint,
      selectedTables: [tableName],
      identityMappings: mappings,
      semanticAnalysis,
    };
  } catch (e) {
    const message = e instanceof Error ? e.message : "File upload preparation failed";
    options.onProgress?.({ phase: "error", message });
    return emptyResult(message);
  }
}

function emptyResult(error: string): AutoConnectResult {
  return {
    source: emptyEndpoint("file", "Source"),
    destination: emptyEndpoint("database", "Destination"),
    selectedTables: [],
    identityMappings: [],
    error,
  };
}

/** Database → file export: connect source, discover schema. */
export async function prepareDatabaseToFile(
  sourceConnStr: string,
  options: { onProgress?: (p: AutoConnectProgress) => void } = {}
): Promise<AutoConnectResult> {
  const report = (phase: AutoConnectProgress["phase"], message: string) => {
    options.onProgress?.({ phase, message });
  };

  if (!sourceConnStr.trim()) return emptyResult("Paste source database connection string.");

  try {
    report("source", "Connecting to source database…");
    const srcResult = await connectDb(sourceConnStr, "Source");
    if (!srcResult.ok) throw new Error(srcResult.error);

    const tables = srcResult.db.tables;
    if (!tables.length) throw new Error("No tables found on source.");

    const table = tables[0];
    report("schema", `Reading ${table}…`);
    const schema = await loadTableSchema(srcResult.db, table);

    report("done", `${schema.columns.length} columns ready for export`);

    return {
      source: {
        ...emptyEndpoint("database", "Source"),
        kind: "database",
        connected: true,
        database: {
          ...srcResult.db,
          sourceTable: table,
          targetColumns: schema.columns,
          tables: schema.tables.length ? schema.tables : tables,
        },
      },
      destination: emptyEndpoint("file", "Destination"),
      selectedTables: tables,
      identityMappings: buildIdentityMappings(schema.columns),
    };
  } catch (e) {
    const message = e instanceof Error ? e.message : "Source connection failed";
    options.onProgress?.({ phase: "error", message });
    return { ...emptyResult(message), source: emptyEndpoint("database", "Source") };
  }
}

/** API → database: validate destination; source is URL. */
export async function prepareApiToDatabase(
  apiUrl: string,
  destConnStr: string,
  options: { onProgress?: (p: AutoConnectProgress) => void } = {}
): Promise<AutoConnectResult> {
  if (!apiUrl.trim()) return emptyResult("Enter API endpoint URL.");
  if (!destConnStr.trim()) return emptyResult("Paste destination connection string.");

  const report = (phase: AutoConnectProgress["phase"], message: string) => {
    options.onProgress?.({ phase, message });
  };

  try {
    report("destination", "Connecting to destination…");
    const destResult = await connectDb(destConnStr, "Destination");
    if (!destResult.ok) throw new Error(destResult.error);

    report("done", "API ingest endpoint configured");

    return {
      source: {
        ...emptyEndpoint("api", "Source"),
        kind: "api",
        connected: true,
        api: { url: apiUrl, authType: "none" },
      },
      destination: {
        ...emptyEndpoint("database", "Destination"),
        kind: "database",
        connected: true,
        database: destResult.db,
      },
      selectedTables: [],
      identityMappings: [
        {
          source: "response_body",
          target: "payload",
          confidence: 0.85,
          reasoning: "API JSON ingest — semantic mapping on run",
        },
      ],
    };
  } catch (e) {
    const message = e instanceof Error ? e.message : "API setup failed";
    options.onProgress?.({ phase: "error", message });
    return emptyResult(message);
  }
}
