import { useCallback, useEffect, useRef, useState } from "react";
import { JobTheater } from "../components/JobTheater";
import { DtIcon } from "../components/DtIcon";
import { EmptyState } from "../components/EmptyState";
import { ConnectorIcon } from "../app/brand-icons";
import { ConnectorSelect } from "../components/ui/ConnectorSelect";
import { SourceKindTiles, type SourceKind } from "../components/ui/SourceKindTiles";
import { StructurePreview } from "../components/ui/StructurePreview";
import { PageFrame } from "../components/ui/PageFrame";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageInsightStrip } from "../components/ui/PageInsightStrip";
import { PageMetricsRow } from "../components/ui/PageMetricsRow";
import { PageShell } from "../components/ui/PageShell";
import { WizardSteps } from "../components/ui/WizardSteps";
import { ButtonLoader, Spinner } from "../components/LoadingState";
import { useToast } from "../components/Toast";
import { PreflightTimeline } from "../components/PreflightTimeline";
import { TransferMapStep } from "./transfer/TransferMapStep";
import { DestinationPicker } from "../components/transfer/DestinationPicker";
import { SourceStepAside } from "../components/transfer/SourceStepAside";
import { ValidateActionsRail } from "../components/transfer/ValidateActionsRail";
import { ProofDashboard } from "../components/transfer/ProofDashboard";
import { TransferRouteBar } from "../components/transfer/TransferRouteBar";
import { useActiveData } from "../lib/DataContext";
import {
  analyzeDbTransfer,
  analyzeFileTransfer,
  analyzeTransferRoute,
  analyzeSchemaEnhanced,
  approveTransferPlan,
  buildColumnSamples,
  createSchedule,
  createTransferPlan,
  fetchTransferCapabilities,
  introspectTransferEndpoints,
  mapTransferColumns,
  mapTransferPlan,
  preflightTransferPlan,
  runPreflight,
  runUniversalTransfer,
  syncTransferPlanMappings,
  updateTransferPlan,
  uploadFile,
} from "../lib/api";
import { defaultPortForType, getConnectorDefaults } from "../lib/connectorTypes";
import {
  buildPreflightMappings,
  confidenceThresholdForMode,
  editableFromPipelineMappings,
  mappingsFromAnalysis,
  type EditableMapping,
} from "../lib/mapping";
import {
  Connector,
  EnhancedAnalysis,
  ParsedUpload,
  PreflightResult,
  TransferPlan,
  TransferResult,
  JobProgress,
} from "../lib/types";

interface TransferPageProps {
  connectors: Connector[];
  onTransferComplete: () => void;
  onOpenSchedules?: () => void;
}

const STEP_SOURCE = 1;
const STEP_DESTINATION = 2;
const STEP_MAP = 3;
const STEP_VALIDATE = 4;
const STEP_RUN = 5;

const STEPS = [
  { n: STEP_SOURCE, label: "Source", shortLabel: "Src", icon: "upload" },
  { n: STEP_DESTINATION, label: "Destination", shortLabel: "Dest", icon: "connectors" },
  { n: STEP_MAP, label: "Map", shortLabel: "Map", icon: "sparkle" },
  { n: STEP_VALIDATE, label: "Validate", shortLabel: "Gate", icon: "gate" },
  { n: STEP_RUN, label: "Run", shortLabel: "Run", icon: "transfer" },
];

const CLOUD_SOURCE_TYPES = new Set(["s3", "gcs", "google_cloud_storage", "azure_blob", "adls"]);

const FALLBACK_DEST_TYPES = [
  "mongodb", "postgresql", "mysql", "snowflake", "bigquery", "redshift",
  "dynamodb", "s3", "gcs", "redis", "elasticsearch", "sqlite",
] as const;
const FALLBACK_EXPORT_FORMATS = ["csv", "json", "jsonl", "tsv", "parquet", "excel", "ndjson"] as const;
const FALLBACK_SOURCE_DBS = [
  "postgresql", "mongodb", "snowflake", "mysql", "bigquery", "redshift",
  "dynamodb", "s3", "gcs", "redis", "elasticsearch", "sqlite",
] as const;

const ACCEPTED_UPLOAD_EXTENSIONS = new Set(["csv", "json", "jsonl", "tsv", "parquet"]);
const MAX_UPLOAD_BYTES = 250 * 1024 * 1024;
const UPLOAD_FORMATS = ["JSON", "CSV", "JSONL", "TSV", "Parquet"] as const;

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

type SyncMode = "full_refresh_overwrite" | "full_refresh_append" | "incremental_append" | "incremental_deduped" | "cdc";
type SchemaPolicy = "manual_review" | "propagate_columns" | "propagate_all" | "pause_on_change";
type ValidationMode = "balanced" | "strict" | "maximum";

const SYNC_MODES: { id: SyncMode; label: string; detail: string }[] = [
  { id: "full_refresh_overwrite", label: "Full overwrite", detail: "Complete snapshot replaces destination rows." },
  { id: "full_refresh_append", label: "Full append", detail: "Complete snapshot appends to destination history." },
  { id: "incremental_append", label: "Incremental append", detail: "Cursor-based new-row sync." },
  { id: "incremental_deduped", label: "Incremental deduped", detail: "Cursor plus key-backed final table." },
  { id: "cdc", label: "CDC", detail: "Change stream with cursor and key contract." },
];

const SCHEMA_POLICIES: { id: SchemaPolicy; label: string; detail: string }[] = [
  { id: "manual_review", label: "Manual approval", detail: "Detect drift, continue on saved contract." },
  { id: "propagate_columns", label: "Column changes", detail: "Auto-apply field additions and removals." },
  { id: "propagate_all", label: "All changes", detail: "Auto-apply streams, fields, and type updates." },
  { id: "pause_on_change", label: "Pause on change", detail: "Stop future runs when drift appears." },
];

const VALIDATION_MODES: { id: ValidationMode; label: string; threshold: string }[] = [
  { id: "strict", label: "Strict", threshold: "0.85" },
  { id: "maximum", label: "Maximum", threshold: "0.95" },
  { id: "balanced", label: "Balanced", threshold: "0.75" },
];

function findColumn(columns: string[], patterns: RegExp[]) {
  return columns.find((col) => patterns.some((pattern) => pattern.test(col))) || "";
}

function fileExtension(name: string) {
  return name.split(".").pop()?.toLowerCase() ?? "";
}

function analysisFromPipeline(
  columns: string[],
  schema: Record<string, string>,
  pipelineColumns: { source: string; target: string; confidence: number; reasoning?: string }[],
): EnhancedAnalysis {
  const bySource = Object.fromEntries(pipelineColumns.map((m) => [m.source, m]));
  return {
    columns: columns.map((column_name) => ({
      column_name,
      inferred_type: schema[column_name] || "string",
      confidence: bySource[column_name]?.confidence ?? 0.7,
      is_pii: /email|phone|ssn|name/i.test(column_name),
      compliance: [],
      reasoning_steps: [bySource[column_name]?.reasoning || "Semantic mapping pipeline"],
      method: "mapping_pipeline",
    })),
    pii_columns: columns.filter((c) => /email|phone|ssn/i.test(c)),
    quality_score: pipelineColumns.length
      ? Math.round(
          (pipelineColumns.reduce((s, m) => s + m.confidence, 0) / pipelineColumns.length) * 100,
        )
      : 70,
    recommendations: ["Review column mappings before executing."],
    method: "mapping_pipeline",
  };
}

export function TransferPage({ connectors, onTransferComplete, onOpenSchedules }: TransferPageProps) {
  const { toast } = useToast();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const autoSelectedConnector = useRef(false);
  const autoSelectedSourceConnector = useRef(false);
  const { setActiveData } = useActiveData();
  const [step, setStep] = useState(STEP_SOURCE);
  const [sourceKind, setSourceKind] = useState<SourceKind>("file");
  const [sourceConnectorId, setSourceConnectorId] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [sourceCollection, setSourceCollection] = useState("");
  const [sourceManualEnabled, setSourceManualEnabled] = useState(false);
  const [sourceManualType, setSourceManualType] = useState("postgresql");
  const [sourceManualHost, setSourceManualHost] = useState("localhost");
  const [sourceManualPort, setSourceManualPort] = useState(5432);
  const [sourceManualUsername, setSourceManualUsername] = useState("");
  const [sourceManualPassword, setSourceManualPassword] = useState("");
  const [sourceManualDatabase, setSourceManualDatabase] = useState("");
  const [sourceManualSchema, setSourceManualSchema] = useState("public");
  const [sourceManualConnectionString, setSourceManualConnectionString] = useState("");
  const [cloudPath, setCloudPath] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [parsed, setParsed] = useState<ParsedUpload | null>(null);
  const [sourceRowEstimate, setSourceRowEstimate] = useState<number | null>(null);
  const [analysis, setAnalysis] = useState<EnhancedAnalysis | null>(null);
  const [preflight, setPreflight] = useState<PreflightResult | null>(null);
  const [analyzing, setAnalyzing] = useState(false);
  const [mappingProgress, setMappingProgress] = useState(0);
  const [mappingPhase, setMappingPhase] = useState("Preparing schema context…");
  const [sourceIntrospecting, setSourceIntrospecting] = useState(false);
  const [preflighting, setPreflighting] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [connectorId, setConnectorId] = useState("");
  const [destType, setDestType] = useState<string>("mongodb");
  const [destKindMode, setDestKindMode] = useState<"database" | "file_export">("database");
  const [exportFormat, setExportFormat] = useState("json");
  const [transferPlan, setTransferPlan] = useState<TransferPlan | null>(null);
  const [persistedPlanId, setPersistedPlanId] = useState<string | null>(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [targetDb, setTargetDb] = useState("dataflow_test");
  const [targetCollection, setTargetCollection] = useState("");
  const [destHost, setDestHost] = useState("localhost");
  const [destPort, setDestPort] = useState(27017);
  const [destSchema, setDestSchema] = useState("public");
  const [destUsername, setDestUsername] = useState("");
  const [destPassword, setDestPassword] = useState("");
  const [destConnectionString, setDestConnectionString] = useState("");
  const [destWarehouse, setDestWarehouse] = useState("");
  const [transferring, setTransferring] = useState(false);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [result, setResult] = useState<TransferResult | null>(null);
  const [syncMode, setSyncMode] = useState<SyncMode>("full_refresh_append");
  const [schemaPolicy, setSchemaPolicy] = useState<SchemaPolicy>("manual_review");
  const [validationMode, setValidationMode] = useState<ValidationMode>("balanced");
  const [backfillNewFields, setBackfillNewFields] = useState(false);
  const [cursorField, setCursorField] = useState("");
  const [primaryKeyField, setPrimaryKeyField] = useState("");
  const [columnMappings, setColumnMappings] = useState<EditableMapping[]>([]);
  const [destColumns, setDestColumns] = useState<string[]>([]);
  const [destSchemaMap, setDestSchemaMap] = useState<Record<string, string>>({});
  const [destSchemaLoading, setDestSchemaLoading] = useState(false);
  const [destTableExists, setDestTableExists] = useState<boolean | null>(null);
  const [liveDestTypes, setLiveDestTypes] = useState<{ id: string; label: string }[]>(
    () => FALLBACK_DEST_TYPES.map((id) => ({ id, label: getConnectorDefaults(id).label })),
  );
  const [liveExportFormats, setLiveExportFormats] = useState<{ id: string; label: string }[]>(
    () => FALLBACK_EXPORT_FORMATS.map((id) => ({ id, label: id.toUpperCase() })),
  );
  const [liveSourceDbs, setLiveSourceDbs] = useState<string[]>([...FALLBACK_SOURCE_DBS]);
  const [liveRouteCount, setLiveRouteCount] = useState<number | null>(null);
  const [transferLaunch, setTransferLaunch] = useState<{ jobId: string; rows: number } | null>(null);
  const [llmMappingUsed, setLlmMappingUsed] = useState(false);

  const confidenceThreshold = confidenceThresholdForMode(validationMode);
  const mappingReviewCount = columnMappings.filter(
    (m) => !m.approved && (m.requiresReview || m.confidence < confidenceThreshold),
  ).length;

  const buildSourceSamples = useCallback((): Record<string, string[]> => {
    const rows = (parsed?.data ?? parsed?.sample_data ?? []) as Record<string, unknown>[];
    const cols =
      parsed?.columns ??
      analysis?.columns.map((c) => c.column_name) ??
      transferPlan?.source_columns ??
      [];
    if (!rows.length || !cols.length) return {};
    const out: Record<string, string[]> = {};
    for (const col of cols) {
      out[col] = rows
        .slice(0, 8)
        .map((r) => String(r[col] ?? ""))
        .filter((v) => v.length > 0);
    }
    return out;
  }, [parsed, analysis, transferPlan?.source_columns]);

  useEffect(() => {
    if (sourceKind === "file") return;
    setAnalysis(null);
    setTransferPlan(null);
    setPreflight(null);
    setPersistedPlanId(null);
  }, [sourceConnectorId, sourceTable, sourceCollection, cloudPath, sourceKind]);

  const buildDestinationEndpoint = () => {
    const isMongo = destType === "mongodb";
    const isDynamo = destType === "dynamodb";
    return {
      kind: "database",
      format: destType,
      connector_id: connectorId || undefined,
      host: destHost,
      port: destPort,
      database: isDynamo ? (targetCollection || targetDb) : targetDb,
      schema: destSchema,
      table: isMongo ? undefined : targetCollection || undefined,
      collection: isMongo ? targetCollection : undefined,
      username: destUsername || undefined,
      password: destPassword || undefined,
      connection_string: destConnectionString || undefined,
      warehouse: destType === "snowflake" ? destWarehouse : undefined,
    };
  };

  useEffect(() => {
    fetchTransferCapabilities()
      .then((caps) => {
        const dbs = (caps.destination_databases as string[] | undefined) ?? [];
        const exports = (caps.destination_file_formats as string[] | undefined) ?? [];
        const sources = (caps.source_databases as string[] | undefined) ?? [];
        if (dbs.length) {
          setLiveDestTypes(dbs.map((id) => ({ id, label: getConnectorDefaults(id).label })));
        }
        if (exports.length) {
          setLiveExportFormats(exports.map((id) => ({ id, label: id.toUpperCase() })));
        }
        if (sources.length) setLiveSourceDbs(sources);
        if (typeof caps.live_route_combinations === "number") {
          setLiveRouteCount(caps.live_route_combinations);
        }
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (liveDestTypes.length && !liveDestTypes.some((d) => d.id === destType)) {
      setDestType(liveDestTypes[0].id);
    }
  }, [liveDestTypes, destType]);

  const destConnectors = connectors.filter((c) => c.type === destType);
  const testedDestConnectors = destConnectors.filter((c) => c.last_test_ok !== false);
  const selectedDestConnector = destConnectors.find((c) => c.id === connectorId);
  const dbSourceConnectors = connectors.filter((c) => liveSourceDbs.includes(c.type));
  const cloudSourceConnectors = connectors.filter((c) => CLOUD_SOURCE_TYPES.has(c.type));
  const sourceConnector =
    sourceKind === "cloud"
      ? cloudSourceConnectors.find((c) => c.id === sourceConnectorId)
      : dbSourceConnectors.find((c) => c.id === sourceConnectorId);
  const isConnectorSource = sourceKind === "database" || sourceKind === "cloud";
  const currentSourceColumns = sourceKind === "file"
    ? parsed?.columns ?? []
    : transferPlan?.source_columns ?? [];
  const currentSourceSchema = sourceKind === "file"
    ? parsed?.schema ?? {}
    : transferPlan?.source_schema ?? {};
  const samplePreviewRows = parsed?.sample_data ?? parsed?.data ?? [];
  const currentSourceColumnsKey = currentSourceColumns.join("|");
  const cursorCandidate = findColumn(currentSourceColumns, [
    /^updated_at$/i,
    /^modified_at$/i,
    /^created_at$/i,
    /timestamp/i,
    /_at$/i,
    /date/i,
  ]);
  const primaryKeyCandidate = findColumn(currentSourceColumns, [
    /^id$/i,
    /_id$/i,
    /uuid/i,
    /primary/i,
    /key/i,
  ]);
  const requiresCursor = syncMode === "incremental_append" || syncMode === "incremental_deduped" || syncMode === "cdc";
  const requiresPrimaryKey = syncMode === "incremental_deduped" || syncMode === "cdc";
  const sourceStreamName = sourceKind === "file"
    ? file?.name.replace(/\.[^/.]+$/, "") || "uploaded_file"
    : sourceKind === "cloud"
      ? cloudPath.split("/").filter(Boolean).pop() || "cloud_object"
      : sourceCollection || sourceTable || "source_stream";
  const streamContracts = [{
    name: sourceStreamName,
    selected: true,
    sync_mode: syncMode,
    cursor_field: requiresCursor ? cursorField : "",
    primary_key: requiresPrimaryKey ? primaryKeyField : "",
    schema_policy: schemaPolicy,
    field_count: currentSourceColumns.length,
    validation_mode: validationMode,
  }];
  const streamNeedsReview =
    currentSourceColumns.length > 0 &&
    ((requiresCursor && !cursorField) || (requiresPrimaryKey && !primaryKeyField));
  const syncModeLabel = SYNC_MODES.find((m) => m.id === syncMode)?.label ?? syncMode;
  const schemaPolicyLabel = SCHEMA_POLICIES.find((p) => p.id === schemaPolicy)?.label ?? schemaPolicy;

  const buildSourceEndpoint = () => {
    if (sourceKind === "file") {
      return {
        kind: "file",
        format: parsed?.file_type ?? file?.name.split(".").pop() ?? "csv",
        filename: file?.name,
      };
    }
    if (sourceManualEnabled && sourceKind === "database") {
      const isMongo = sourceManualType === "mongodb";
      const isDynamo = sourceManualType === "dynamodb";
      const tableOrPath = isMongo ? (sourceCollection || sourceTable) : (isDynamo ? (sourceTable || sourceManualDatabase || "") : sourceTable);
      return {
        kind: "database",
        format: sourceManualType,
        connector_id: "",
        host: sourceManualHost,
        port: sourceManualPort,
        username: sourceManualUsername || undefined,
        password: sourceManualPassword || undefined,
        database: isDynamo ? tableOrPath : sourceManualDatabase,
        schema: sourceManualSchema,
        table: isMongo ? undefined : tableOrPath || undefined,
        collection: isMongo ? tableOrPath : undefined,
        connection_string: sourceManualConnectionString || undefined,
      };
    }
    if (!sourceConnector) return { kind: "database", format: "json" };
    const isMongo = sourceConnector.type === "mongodb";
    const isDynamo = sourceConnector.type === "dynamodb";
    const tableOrPath = sourceKind === "cloud"
      ? cloudPath.trim()
      : (isMongo ? (sourceCollection || sourceTable) : (isDynamo ? (sourceTable || sourceConnector.database || "") : sourceTable));
    return {
      kind: "database",
      format: sourceConnector.type,
      connector_id: sourceConnectorId,
      database: isDynamo ? tableOrPath : sourceConnector.database,
      table: isMongo ? undefined : tableOrPath || undefined,
      collection: isMongo ? tableOrPath : undefined,
    };
  };

  const buildPlanPayload = useCallback(() => ({
    name: file?.name ?? sourceStreamName,
    source: buildSourceEndpoint(),
    destination: destKindMode === "file_export"
      ? { kind: "file_export", format: exportFormat, database: targetDb }
      : buildDestinationEndpoint(),
    source_columns: currentSourceColumns,
    source_schema: currentSourceSchema,
    target_columns: destColumns,
    target_schema: destSchemaMap,
    row_count_estimate: parsed?.row_count ?? sourceRowEstimate ?? 0,
    sample_rows: (parsed?.data ?? parsed?.sample_data)?.slice(0, 100) ?? [],
    policies: {
      sync_mode: syncMode,
      schema_policy: schemaPolicy,
      validation_mode: validationMode,
      backfill_new_fields: backfillNewFields,
      stream_contracts: streamContracts,
    },
  }), [
    file,
    sourceStreamName,
    sourceKind,
    parsed,
    sourceRowEstimate,
    sourceConnector,
    sourceConnectorId,
    sourceCollection,
    sourceTable,
    cloudPath,
    destKindMode,
    exportFormat,
    targetDb,
    currentSourceColumns,
    currentSourceSchema,
    destColumns,
    destSchemaMap,
    syncMode,
    schemaPolicy,
    validationMode,
    backfillNewFields,
    streamContracts,
    connectorId,
    destType,
    destHost,
    destPort,
    destSchema,
    destUsername,
    destPassword,
    destConnectionString,
    destWarehouse,
    targetCollection,
  ]);

  const ensurePersistedPlan = useCallback(async (): Promise<string | null> => {
    if (!currentSourceColumns.length) return null;
    const payload = buildPlanPayload();
    try {
      if (persistedPlanId) {
        await updateTransferPlan(persistedPlanId, payload);
        return persistedPlanId;
      }
      const { plan } = await createTransferPlan(payload);
      setPersistedPlanId(plan.id);
      return plan.id;
    } catch (e) {
      console.error("Transfer plan persistence failed:", e);
      return persistedPlanId;
    }
  }, [buildPlanPayload, currentSourceColumns.length, persistedPlanId]);

  const buildMappingsFromSource = useCallback((
    columns: import("../lib/types").ColumnAnalysis[] | undefined,
    targetCols?: string[],
  ) => {
    const rows = parsed?.data ?? parsed?.sample_data;
    if (columns?.length) {
      const destSet = new Set((targetCols ?? destColumns).map((c) => c.toLowerCase()));
      return mappingsFromAnalysis(columns, rows).map((m) => ({
        ...m,
        existsInDestination: destSet.has(m.target.toLowerCase()),
      }));
    }
    const sourceCols = parsed?.columns ?? transferPlan?.source_columns ?? [];
    if (!sourceCols.length) return [];
    const destSet = new Set((targetCols ?? destColumns).map((c) => c.toLowerCase()));
    return sourceCols.map((col) => ({
      source: col,
      target: col,
      confidence: 0.7,
      inferredType: parsed?.schema?.[col] ?? transferPlan?.source_schema?.[col] ?? "string",
      sample: rows?.find((r) => r[col] != null)?.[col] != null
        ? String(rows!.find((r) => r[col] != null)![col])
        : undefined,
      approved: false,
      existsInDestination: destSet.has(col.toLowerCase()),
      reason: "Identity mapping (pipeline unavailable)",
      transform: "none" as const,
    }));
  }, [parsed, transferPlan, destColumns]);

  const applyPipelineMappings = useCallback(
    async (targetCols?: string[], targetSchema?: Record<string, string>, analysisOverride?: import("../lib/types").EnhancedAnalysis | null) => {
      const sourceCols =
        parsed?.columns ??
        analysisOverride?.columns.map((c) => c.column_name) ??
        analysis?.columns.map((c) => c.column_name) ??
        transferPlan?.source_columns ??
        [];
      if (!sourceCols.length) return;
      const threshold = confidenceThresholdForMode(validationMode);
      const planId = await ensurePersistedPlan();
      const rows = parsed?.data ?? parsed?.sample_data;
      const analysisCols = analysisOverride?.columns ?? analysis?.columns;
      try {
        const result = planId
          ? await mapTransferPlan(planId, {
              validation_mode: validationMode,
              use_llm: true,
              source_samples: buildSourceSamples(),
            })
          : await mapTransferColumns({
              source_columns: sourceCols,
              source_schema: parsed?.schema ?? transferPlan?.source_schema ?? {},
              target_columns: targetCols?.length ? targetCols : undefined,
              target_schema: targetSchema,
              validation_mode: validationMode,
              file_format: parsed?.file_type
                ?? (sourceKind !== "file" ? sourceConnector?.type : undefined)
                ?? file?.name.split(".").pop(),
              use_llm: true,
              source_samples: buildSourceSamples(),
            });
        setColumnMappings(
          editableFromPipelineMappings(
            result.mappings,
            rows,
            targetCols,
            threshold,
          ),
        );
        setLlmMappingUsed(Boolean(result.llm?.llm_used));
      } catch (e) {
        console.error("Mapping pipeline failed:", e);
        const fallback = buildMappingsFromSource(analysisCols, targetCols);
        if (fallback.length) {
          setColumnMappings(fallback);
          toast({
            title: "Using fallback mappings",
            message: "Semantic pipeline unavailable — showing AI-classified column pairs. Review before transfer.",
            tone: "warning",
          });
        }
        setLlmMappingUsed(false);
      }
    },
    [parsed, analysis, transferPlan, validationMode, file, sourceKind, sourceConnector, buildSourceSamples, ensurePersistedPlan, buildMappingsFromSource, toast],
  );

  const remapWithDestination = async (targetCols: string[], targetSchema: Record<string, string>) => {
    await applyPipelineMappings(targetCols, targetSchema);
  };

  const loadDestinationSchema = async () => {
    if (destKindMode !== "database" || !targetCollection.trim()) return;
    setDestSchemaLoading(true);
    try {
      const { destination } = await introspectTransferEndpoints({
        source: buildSourceEndpoint(),
        destination: buildDestinationEndpoint(),
      });
      setDestColumns(destination.columns ?? []);
      setDestSchemaMap(destination.schema ?? {});
      setDestTableExists(destination.table_exists ?? (destination.columns?.length > 0));
      const hasSourceSchema = Boolean(parsed || analysis?.columns.length || currentSourceColumns.length);
      if (hasSourceSchema) {
        await remapWithDestination(destination.columns ?? [], destination.schema ?? {});
      }
    } catch {
      setDestColumns([]);
      setDestSchemaMap({});
      setDestTableExists(null);
    }
    setDestSchemaLoading(false);
  };

  useEffect(() => {
    if (!analysis?.columns.length || step !== STEP_MAP) return;
    void applyPipelineMappings(destColumns.length ? destColumns : undefined, destSchemaMap);
  }, [validationMode, step]);

  useEffect(() => {
    if (step !== STEP_DESTINATION || destKindMode !== "database" || !targetCollection.trim()) return;
    const t = window.setTimeout(() => { void loadDestinationSchema(); }, 400);
    return () => window.clearTimeout(t);
  }, [step, destKindMode, targetCollection, connectorId, destType, targetDb, destHost, destPort]);

  useEffect(() => {
    if (cursorCandidate && (!cursorField || !currentSourceColumns.includes(cursorField))) {
      setCursorField(cursorCandidate);
    } else if (!cursorCandidate && cursorField && !currentSourceColumns.includes(cursorField)) {
      setCursorField("");
    }
    if (primaryKeyCandidate && (!primaryKeyField || !currentSourceColumns.includes(primaryKeyField))) {
      setPrimaryKeyField(primaryKeyCandidate);
    } else if (!primaryKeyCandidate && primaryKeyField && !currentSourceColumns.includes(primaryKeyField)) {
      setPrimaryKeyField("");
    }
  }, [cursorCandidate, cursorField, currentSourceColumns, currentSourceColumnsKey, primaryKeyCandidate, primaryKeyField]);

  const applyConnectorSelection = (id: string) => {
    setConnectorId(id);
    if (!id) return;
    const conn = connectors.find((c) => c.id === id);
    if (!conn) return;
    if (liveDestTypes.some((d) => d.id === conn.type)) {
      setDestType(conn.type);
    }
    if (conn.database) setTargetDb(conn.database);
    if (conn.host) setDestHost(conn.host);
    if (conn.port) setDestPort(conn.port);
  };

  useEffect(() => {
    setDestPort(defaultPortForType(destType));
    autoSelectedConnector.current = false;
  }, [destType]);

  useEffect(() => {
    if (autoSelectedConnector.current || connectorId || destConnectors.length === 0) return;
    const preferred =
      testedDestConnectors.find((c) => c.name.toLowerCase().includes("local")) ??
      testedDestConnectors[0] ??
      destConnectors[0];
    if (preferred) {
      applyConnectorSelection(preferred.id);
      autoSelectedConnector.current = true;
    }
  }, [connectorId, destConnectors, destType, testedDestConnectors]);

  useEffect(() => {
    if (sourceKind !== "database" && sourceKind !== "cloud") return;
    if (autoSelectedSourceConnector.current || sourceConnectorId) return;
    const pool = sourceKind === "cloud" ? cloudSourceConnectors : dbSourceConnectors;
    if (pool.length === 0) return;
    const preferred =
      pool.find((c) => c.last_test_ok !== false && c.name.toLowerCase().includes("local")) ??
      pool.find((c) => c.last_test_ok !== false) ??
      pool[0];
    if (preferred) {
      setSourceConnectorId(preferred.id);
      autoSelectedSourceConnector.current = true;
    }
  }, [sourceKind, sourceConnectorId, dbSourceConnectors, cloudSourceConnectors]);

  useEffect(() => {
    autoSelectedSourceConnector.current = false;
  }, [sourceKind]);

  const routeAnalyzedRef = useRef(false);

  useEffect(() => {
    const content = document.querySelector(".df2-content");
    const inner = document.querySelector(".df2-content-inner");
    content?.classList.add("is-transfer-studio-view");
    inner?.classList.add("is-transfer-studio-view");
    return () => {
      content?.classList.remove("is-transfer-studio-view");
      inner?.classList.remove("is-transfer-studio-view");
    };
  }, []);

  const loadTransferPlan = async () => {
    if (!currentSourceColumns.length && !(sourceKind === "file" && file)) {
      toast({
        title: "No source schema",
        message: "Complete the source step and map columns before analyzing the route.",
        tone: "warning",
      });
      return;
    }
    setPlanLoading(true);
    try {
      const destination = destKindMode === "file_export"
        ? { kind: "file_export", format: exportFormat, database: targetDb }
        : buildDestinationEndpoint();
      const source = buildSourceEndpoint();

      let plan: TransferPlan;
      if (currentSourceColumns.length) {
        plan = await analyzeTransferRoute({
          source,
          destination,
          source_columns: currentSourceColumns,
          source_schema: currentSourceSchema,
        });
      } else if (sourceKind === "file" && file) {
        plan = await analyzeFileTransfer(file, {
          destKind: destKindMode,
          destFormat: destKindMode === "file_export" ? exportFormat : destType,
          destDatabase: targetDb,
          destTable: destType !== "mongodb" && destType !== "dynamodb" ? targetCollection : undefined,
          destCollection: destType === "mongodb" || destType === "dynamodb" ? targetCollection : undefined,
        });
      } else {
        return;
      }
      setTransferPlan(plan);
      toast({
        title: plan.supported ? "Route ready" : "Route needs attention",
        message: plan.message || `${plan.auto_create?.length ?? 0} auto-create steps planned`,
        tone: plan.supported ? "success" : "warning",
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Could not build transfer plan.";
      toast({ title: "Route analysis failed", message: msg, tone: "error" });
      console.error(e);
    } finally {
      setPlanLoading(false);
    }
  };

  useEffect(() => {
    if (step !== STEP_DESTINATION) {
      routeAnalyzedRef.current = false;
      return;
    }
    if (routeAnalyzedRef.current || !currentSourceColumns.length || planLoading) return;
    routeAnalyzedRef.current = true;
    void loadTransferPlan();
  }, [step, currentSourceColumnsKey, planLoading]);

  const runSourceColumnAnalysis = async (data: ParsedUpload) => {
    setAnalyzing(true);
    try {
      const rows = data.data ?? data.sample_data;
      const columnSamples = buildColumnSamples(data.columns, rows);
      const result = await analyzeSchemaEnhanced(columnSamples);
      setAnalysis(result);
      await applyPipelineMappings(
        destColumns.length ? destColumns : undefined,
        destSchemaMap,
        result,
      );
      const destLabel = destKindMode === "file_export"
        ? `${exportFormat.toUpperCase()} export`
        : targetCollection
          ? `${targetDb}.${targetCollection}`
          : destType;
      toast({
        title: "Column analysis complete",
        message: `${result.columns.length} source columns ready to map against ${destLabel}.`,
        tone: result.quality_score >= 85 ? "success" : "warning",
      });
    } catch (e) {
      toast({ title: "AI analysis unavailable", message: "Running semantic mapping pipeline instead.", tone: "warning" });
      console.error("AI analysis failed:", e);
      try {
        const rows = data.data ?? data.sample_data;
        const pipeline = await mapTransferColumns({
          source_columns: data.columns,
          source_schema: data.schema ?? {},
          target_columns: destColumns.length ? destColumns : undefined,
          target_schema: destSchemaMap,
          validation_mode: validationMode,
          file_format: data.file_type ?? file?.name.split(".").pop(),
          use_llm: true,
          source_samples: buildColumnSamples(data.columns, rows),
        });
        const pipelineAnalysis = analysisFromPipeline(data.columns, data.schema ?? {}, pipeline.mappings);
        setAnalysis(pipelineAnalysis);
        setColumnMappings(editableFromPipelineMappings(pipeline.mappings, rows, destColumns.length ? destColumns : undefined));
        setLlmMappingUsed(Boolean(pipeline.llm?.llm_used));
      } catch (pipeErr) {
        console.error("Mapping pipeline failed:", pipeErr);
        const fallback = buildMappingsFromSource(
          analysisFromPipeline(
            data.columns,
            data.schema ?? {},
            data.columns.map((col) => ({ source: col, target: col, confidence: 0.7 })),
          ).columns,
          destColumns,
        );
        if (fallback.length) {
          setColumnMappings(fallback);
          toast({
            title: "Basic mappings created",
            message: "Could not reach mapping API — identity column pairs generated. Review each mapping.",
            tone: "warning",
          });
        } else {
          toast({ title: "Mapping failed", message: "Could not map columns — check API connectivity.", tone: "error" });
          throw pipeErr;
        }
      }
    } finally {
      setAnalyzing(false);
    }
  };

  const processFile = async (selected: File) => {
    const ext = fileExtension(selected.name);
    if (!ACCEPTED_UPLOAD_EXTENSIONS.has(ext)) {
      toast({
        title: "Unsupported file type",
        message: "Use CSV, TSV, JSON, or JSONL for this transfer flow.",
        tone: "warning",
      });
      return;
    }
    if (selected.size > MAX_UPLOAD_BYTES) {
      toast({
        title: "File is too large",
        message: "Use a file under 250 MB or connect the source as a database stream.",
        tone: "error",
      });
      return;
    }
    setUploadError(null);
    setFile(selected);
    setParsed(null);
    setAnalysis(null);
    setPreflight(null);
    setPersistedPlanId(null);
    setLlmMappingUsed(false);
    setUploading(true);
    try {
      const data = await uploadFile(selected);
      if (!data.columns?.length) {
        throw new Error("No columns detected — ensure JSON is an array of objects with field names.");
      }
      setParsed(data);
      const rows = data.data ?? data.sample_data;
      const samples = buildColumnSamples(data.columns, rows);
      setActiveData({
        name: selected.name.replace(/\.[^/.]+$/, ""),
        filename: selected.name,
        columns: data.columns,
        row_count: data.row_count,
        samples,
        schema: data.schema,
      });
      if (!targetCollection) {
        setTargetCollection(selected.name.replace(/\.[^/.]+$/, ""));
      }
      toast({
        title: "Source profiled",
        message: `${data.row_count.toLocaleString()} rows and ${data.columns.length} columns detected.${
          data.validation && !data.validation.ok
            ? ` ${data.validation.issue_count} type issue(s) found — review before transfer.`
            : ""
        }`,
        tone: data.validation && !data.validation.ok ? "warning" : "success",
      });
      setStep(STEP_SOURCE);
    } catch (e) {
      const message = e instanceof Error ? e.message : "Check file format and try again.";
      setUploadError(message);
      setFile(null);
      setParsed(null);
      toast({ title: "Upload failed", message, tone: "error" });
      console.error(e);
    }
    setUploading(false);
  };

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0];
    if (selected) processFile(selected);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const selected = e.dataTransfer.files?.[0];
    if (selected) processFile(selected);
  };

  const explainSourceGap = () => {
    if (sourceKind === "file" && !parsed) {
      toast({ title: "Source file required", message: "Upload a CSV, TSV, JSON, JSONL, or Parquet file to continue.", tone: "warning" });
      setStep(STEP_SOURCE);
      return true;
    }
    if (isConnectorSource && !sourceConnectorId) {
      toast({
        title: "Source connector required",
        message: sourceKind === "cloud"
          ? "Select a saved S3, GCS, or Azure Blob connector."
          : "Select a saved database or warehouse connector.",
        tone: "warning",
      });
      setStep(STEP_SOURCE);
      return true;
    }
    if (sourceKind === "database" && !(sourceTable || sourceCollection)) {
      toast({ title: "Source stream required", message: "Enter the table or collection name to inspect.", tone: "warning" });
      setStep(STEP_SOURCE);
      return true;
    }
    if (sourceKind === "cloud" && !cloudPath.trim()) {
      toast({ title: "Object path required", message: "Enter a bucket/prefix or object key to read.", tone: "warning" });
      setStep(STEP_SOURCE);
      return true;
    }
    return false;
  };

  const explainDestinationGap = () => {
    if (explainSourceGap()) return true;
    if (destKindMode === "database" && !targetDb.trim()) {
      toast({ title: "Destination database required", message: "Enter the target database or project.", tone: "warning" });
      setStep(STEP_DESTINATION);
      return true;
    }
    if (destKindMode === "database" && !targetCollection.trim()) {
      toast({ title: "Destination table required", message: "Enter the target table or collection.", tone: "warning" });
      setStep(STEP_DESTINATION);
      return true;
    }
    if (streamNeedsReview) {
      toast({
        title: "Stream contract needs review",
        message: `${requiresCursor && !cursorField ? "Select a cursor field. " : ""}${requiresPrimaryKey && !primaryKeyField ? "Select a primary key." : ""}`.trim(),
        tone: "warning",
      });
      setStep(STEP_DESTINATION);
      return true;
    }
    return false;
  };

  const introspectConnectorSource = async (): Promise<boolean> => {
    const sourceEndpoint = buildSourceEndpoint();
    if (sourceEndpoint.kind !== "database") return false;
    const { source: intro } = await introspectTransferEndpoints({
      source: sourceEndpoint,
      destination: { kind: "file_export", format: "json" },
    });
    if (!intro.connected || !intro.columns?.length) {
      toast({
        title: "Could not read source schema",
        message: intro.message || "Verify table, collection, or object path and credentials.",
        tone: "error",
      });
      return false;
    }
    if (intro.row_estimate != null && intro.row_estimate > 0) {
      setSourceRowEstimate(intro.row_estimate);
    }
    const columnSamples = Object.fromEntries(
      intro.columns.map((col) => [col, intro.schema?.[col] ? [String(intro.schema[col])] : []]),
    );
    const dbAnalysis = await analyzeSchemaEnhanced(columnSamples);
    setAnalysis(dbAnalysis);
    setTransferPlan((prev) => ({
      supported: prev?.supported ?? true,
      message: intro.message,
      operation: prev?.operation ?? "insert",
      auto_create: prev?.auto_create ?? [],
      type_mappings: prev?.type_mappings ?? [],
      source_columns: intro.columns,
      source_schema: intro.schema ?? {},
    }));
    setActiveData({
      name: sourceTable || sourceCollection || sourceConnector?.name || sourceManualType || "source_stream",
      columns: intro.columns,
      row_count: intro.row_estimate ?? 0,
      samples: columnSamples,
      schema: intro.schema ?? {},
    });
    return true;
  };

  const proceedToDestination = async () => {
    if (explainSourceGap()) return;
    if (isConnectorSource && !analysis?.columns.length && !currentSourceColumns.length) {
      setSourceIntrospecting(true);
      setAnalyzing(true);
      try {
        const ok = await introspectConnectorSource();
        if (!ok) return;
      } catch (e) {
        const message = e instanceof Error ? e.message : "Source introspection failed.";
        toast({ title: "Schema read failed", message, tone: "error" });
        return;
      } finally {
        setSourceIntrospecting(false);
        setAnalyzing(false);
      }
    }
    setStep(STEP_DESTINATION);
  };

  const goToMapping = async () => {
    if (explainDestinationGap()) return;
    setStep(STEP_MAP);
    setAnalyzing(true);
    try {
      if (destKindMode === "database") {
        await loadDestinationSchema();
      }
      await loadTransferPlan();
      if (sourceKind === "file" && parsed) {
        if (!analysis?.columns.length || !columnMappings.length) {
          await runSourceColumnAnalysis(parsed);
        } else {
          await applyPipelineMappings(
            destColumns.length ? destColumns : undefined,
            destSchemaMap,
          );
        }
      } else if (analysis?.columns.length || currentSourceColumns.length) {
        await applyPipelineMappings(
          destColumns.length ? destColumns : undefined,
          destSchemaMap,
        );
      } else {
        toast({
          title: "Source schema required",
          message: "Complete the source step before mapping columns.",
          tone: "warning",
        });
        setStep(STEP_SOURCE);
      }
    } catch (e) {
      const message = e instanceof Error ? e.message : "Could not prepare column mappings.";
      toast({ title: "Mapping setup failed", message, tone: "error" });
      console.error(e);
    } finally {
      setAnalyzing(false);
    }
  };

  const goToPreflight = () => {
    if (explainDestinationGap()) return;
    const threshold = confidenceThreshold;
    const pendingReview = columnMappings.filter(
      (m) => !m.approved && (m.requiresReview || m.confidence < threshold),
    ).length;
    if (columnMappings.length && pendingReview > 0) {
      toast({
        title: "Review column mappings",
        message: `${pendingReview} column(s) need approval before validation.`,
        tone: "warning",
      });
      setStep(STEP_MAP);
      return;
    }
    setStep(STEP_VALIDATE);
    void executePreflight();
  };

  const approveAllMappings = () => {
    setColumnMappings((prev) =>
      prev.map((m) => ({ ...m, approved: true })),
    );
  };

  const approveAllAndPreflight = async () => {
    const approved = columnMappings.map((m) => ({ ...m, approved: true }));
    setColumnMappings(approved);
    setStep(STEP_VALIDATE);
    await executePreflight(approved);
  };

  const executePreflight = async (overrideMappings?: EditableMapping[], validationOverride?: ValidationMode) => {
    const activeMappings = overrideMappings ?? columnMappings;
    const activeValidation = validationOverride ?? validationMode;
    const threshold = confidenceThresholdForMode(activeValidation);
    if (
      sourceKind === "file"
      && parsed?.validation
      && !parsed.validation.ok
      && activeValidation !== "balanced"
    ) {
      toast({
        title: "Source data issues detected",
        message: `${parsed.validation.issue_count} CSV type issue(s) found — fix source data or switch to Balanced validation after review.`,
        tone: "error",
      });
      setStep(STEP_SOURCE);
      return;
    }
    const pendingReview = activeMappings.filter(
      (m) => !m.approved && (m.requiresReview || m.confidence < threshold),
    ).length;
    if (pendingReview > 0) {
      toast({
        title: "Review column mappings",
        message: `${pendingReview} column(s) need approval — edit names or click Approve in the column table.`,
        tone: "warning",
      });
      setStep(STEP_MAP);
      return;
    }
    if (!canRunPreflight || streamNeedsReview) {
      explainDestinationGap();
      return;
    }
    setPreflighting(true);
    setStep(STEP_VALIDATE);
    setPreflight(null);
    try {
      let columns: string[] = [];
      let columnTypes: Record<string, string> = {};
      let mappings: { source: string; target: string; confidence: number; reason?: string }[] = [];
      let sampleRows: Record<string, unknown>[] | undefined;
      let rowCount = 0;
      let estimatedBytes = file?.size ?? 0;

      if (sourceKind === "file") {
        if (!parsed) {
          toast({ title: "Analysis required", message: "Upload and parse a source file before preflight.", tone: "warning" });
          setStep(STEP_SOURCE);
          return;
        }
        if (!analysis && !columnMappings.length) {
          toast({ title: "Mapping required", message: "Map source columns to destination before preflight.", tone: "warning" });
          setStep(STEP_MAP);
          return;
        }
        columns = parsed.columns;
        columnTypes = parsed.schema || {};
        rowCount = parsed.row_count;
        sampleRows = (parsed.data ?? parsed.sample_data)?.slice(0, 100);
        mappings = buildPreflightMappings(
          analysis?.columns ?? [],
          activeMappings.length ? activeMappings : columnMappings,
        );
      } else {
        if (!sourceConnector && !sourceManualEnabled) {
          toast({
            title: "Source required",
            message: sourceKind === "cloud"
              ? "Select a cloud connector and object path."
              : "Select a source connector and table, or enable manual connection.",
            tone: "warning",
          });
          setStep(STEP_SOURCE);
          return;
        }
        const isManual = sourceManualEnabled && sourceKind === "database";
        const routePlan = await analyzeDbTransfer({
          sourceConnectorId: isManual ? "" : sourceConnectorId,
          sourceFormat: isManual ? sourceManualType : sourceConnector?.type ?? "",
          sourceDatabase: isManual ? sourceManualDatabase : sourceConnector?.database,
          sourceTable: sourceKind === "cloud" ? cloudPath || undefined : sourceTable || undefined,
          sourceCollection: sourceKind === "cloud"
            ? cloudPath || undefined
            : sourceCollection || undefined,
          sourceHost: isManual ? sourceManualHost : undefined,
          sourcePort: isManual ? sourceManualPort : undefined,
          sourceUsername: isManual ? sourceManualUsername : undefined,
          sourcePassword: isManual ? sourceManualPassword : undefined,
          sourceSchema: isManual ? sourceManualSchema : undefined,
          sourceConnectionString: isManual ? sourceManualConnectionString : undefined,
          destFormat: destType,
          destDatabase: targetDb,
          destTable: destType !== "mongodb" ? targetCollection : undefined,
          destCollection: destType === "mongodb" ? targetCollection : undefined,
          destConnectorId: connectorId || undefined,
        });
        columns = routePlan.source_columns ?? [];
        columnTypes = routePlan.source_schema ?? {};
        if (!columns.length) {
          toast({
            title: "Schema introspection failed",
            message: routePlan.message || "Could not read columns from source — verify table/collection and credentials.",
            tone: "error",
          });
          return;
        }
        const columnSamples = buildColumnSamples(columns, []);
        const dbAnalysis = await analyzeSchemaEnhanced(columnSamples);
        setAnalysis((prev) => prev ?? dbAnalysis);
        mappings = buildPreflightMappings(
          dbAnalysis.columns,
          activeMappings.length ? activeMappings : columnMappings,
        );
        setTransferPlan(routePlan);
      }

      const planId = await ensurePersistedPlan();
      if (planId) {
        await syncTransferPlanMappings(planId, mappings);
        const pf = await preflightTransferPlan(planId);
        if (pf.passed) {
          await approveTransferPlan(planId);
        }
        setPreflight(pf);
        if (!pf.passed) {
          toast({
            title: "Validation incomplete",
            message: pf.blockers?.[0]?.message ?? `${pf.blockers?.length ?? 0} check(s) failed — use the fix actions below.`,
            tone: "warning",
          });
        } else {
          setStep(STEP_RUN);
          toast({
            title: "Ready to transfer",
            message: `All ${pf.total_gates} checks passed. Plan ${planId.slice(0, 8)} approved — moved to Run step.`,
            tone: "success",
          });
        }
        return;
      }

      const pf = await runPreflight({
        columns,
        column_types: columnTypes,
        row_count: rowCount,
        mappings,
        dest_kind: destKindMode,
        connector_id: destKindMode === "database" && connectorId ? connectorId : undefined,
        source_connector_id: isConnectorSource ? sourceConnectorId || undefined : undefined,
        dest_type: destKindMode === "database" && !connectorId ? destType : undefined,
        dest_host: destKindMode === "database" && !connectorId ? destHost : undefined,
        dest_port: destKindMode === "database" && !connectorId ? destPort : undefined,
        dest_database: destKindMode === "database" && !connectorId ? targetDb : undefined,
        dest_username: destKindMode === "database" && !connectorId ? destUsername || undefined : undefined,
        dest_password: destKindMode === "database" && !connectorId ? destPassword || undefined : undefined,
        dest_connection_string: destKindMode === "database" && !connectorId ? destConnectionString || undefined : undefined,
        dest_schema: destKindMode === "database" && !connectorId && destType === "snowflake" ? destSchema || "PUBLIC" : undefined,
        dest_warehouse: destKindMode === "database" && !connectorId && destType === "snowflake" ? destWarehouse || undefined : undefined,
        sample_rows: sampleRows,
        estimated_bytes: estimatedBytes,
        sync_mode: syncMode,
        schema_policy: schemaPolicy,
        validation_mode: validationOverride ?? validationMode,
        backfill_new_fields: backfillNewFields,
        stream_contracts: streamContracts,
      });
      setPreflight(pf);
      if (!pf.passed) {
        toast({
          title: "Validation incomplete",
          message: pf.blockers[0]?.message ?? `${pf.blockers.length} check(s) failed — use the fix actions below.`,
          tone: "warning",
        });
      } else {
        setStep(STEP_RUN);
        toast({
          title: "Ready to transfer",
          message: `All ${pf.total_gates} checks passed. Moved to Run step — execute when ready.`,
          tone: "success",
        });
      }
    } catch (e) {
      const message = e instanceof Error ? e.message : "Validation could not complete.";
      toast({ title: "Preflight failed", message, tone: "error" });
      console.error(e);
    } finally {
      setPreflighting(false);
    }
  };

  const executeTransfer = async () => {
    const needsDbTarget = destKindMode === "database";
    if (sourceKind === "file" && !file) {
      toast({ title: "Source file required", message: "Upload a file before executing.", tone: "warning" });
      setStep(STEP_SOURCE);
      return;
    }
    if (isConnectorSource && !sourceConnectorId) {
      toast({ title: "Source connector required", message: "Select a source connector before executing.", tone: "warning" });
      setStep(STEP_SOURCE);
      return;
    }
    if (needsDbTarget && (!targetDb || !targetCollection)) {
      toast({ title: "Destination required", message: "Enter the target database and table or collection.", tone: "warning" });
      setStep(STEP_DESTINATION);
      return;
    }
    if (destKindMode === "database" && !preflight?.passed) {
      toast({ title: "Preflight required", message: "Run and pass preflight gates before writing to a database.", tone: "warning" });
      setStep(STEP_VALIDATE);
      return;
    }
    if (
      sourceKind === "file"
      && parsed?.validation
      && !parsed.validation.ok
      && validationMode !== "balanced"
    ) {
      toast({
        title: "Source data issues block transfer",
        message: `${parsed.validation.issue_count} CSV type issue(s) — fix source file before writing to production.`,
        tone: "error",
      });
      setStep(STEP_SOURCE);
      return;
    }

    const enforcePreflight = destKindMode === "database";

    setTransferring(true);
    setActiveJobId(null);
    setResult(null);
    setTransferLaunch(null);
    const transferMappings = columnMappings.length
      ? buildPreflightMappings(analysis?.columns ?? [], columnMappings)
      : analysis
        ? buildPreflightMappings(analysis.columns)
        : undefined;
    try {
      const isManualSource = sourceManualEnabled && sourceKind === "database";
      const data = await runUniversalTransfer({
        file: sourceKind === "file" ? file ?? undefined : undefined,
        sourceKind: sourceKind === "cloud" ? "database" : sourceKind,
        sourceFormat: isManualSource ? sourceManualType : sourceConnector?.type,
        sourceConnectorId: isConnectorSource ? sourceConnectorId || undefined : undefined,
        sourceHost: isManualSource ? sourceManualHost : undefined,
        sourcePort: isManualSource ? sourceManualPort : undefined,
        sourceUsername: isManualSource ? sourceManualUsername : undefined,
        sourcePassword: isManualSource ? sourceManualPassword : undefined,
        sourceDatabase: isManualSource ? sourceManualDatabase : sourceConnector?.database,
        sourceSchema: isManualSource ? sourceManualSchema : undefined,
        sourceTable: sourceKind === "cloud"
          ? cloudPath || undefined
          : isManualSource
            ? (sourceManualType !== "mongodb" ? sourceTable || sourceCollection : undefined)
            : sourceConnector?.type !== "mongodb" ? sourceTable || sourceCollection : undefined,
        sourceCollection: sourceKind === "cloud"
          ? cloudPath || undefined
          : isManualSource
            ? (sourceManualType === "mongodb" ? sourceCollection || sourceTable : undefined)
            : sourceConnector?.type === "mongodb" ? sourceCollection || sourceTable : undefined,
        sourceConnectionString: isManualSource ? sourceManualConnectionString : undefined,
        destKind: destKindMode,
        destFormat: destKindMode === "file_export" ? exportFormat : destType,
        destDatabase: targetDb,
        destSchema: destType === "snowflake" ? "PUBLIC" : destType === "bigquery" ? destSchema : destSchema,
        destTable: destType !== "mongodb" ? targetCollection : undefined,
        destCollection: destType === "mongodb" ? targetCollection : targetCollection,
        destConnectorId: connectorId || undefined,
        destHost: !connectorId ? destHost : undefined,
        destPort: !connectorId ? destPort : undefined,
        destUsername: !connectorId ? destUsername || undefined : undefined,
        destPassword: !connectorId ? destPassword || undefined : undefined,
        destConnectionString: !connectorId ? destConnectionString || undefined : undefined,
        destWarehouse: destType === "snowflake" ? destWarehouse : undefined,
        skipPreflight: !enforcePreflight,
        mappings: transferMappings,
        syncMode,
        schemaPolicy,
        validationMode,
        backfillNewFields,
        streamContracts,
        planId: persistedPlanId ?? undefined,
      });
      if (data.job_id && (data as { async?: boolean }).async) {
        setTransferLaunch({
          jobId: data.job_id,
          rows: parsed?.row_count ?? sourceRowEstimate ?? 0,
        });
        setTransferring(false);
        toast({
          title: "Transfer started",
          message: "Review the summary, then open live progress when ready.",
          tone: "success",
        });
        return;
      }
      setResult(data);
      setStep(STEP_RUN);
      if (data.success) onTransferComplete();
    } catch {
      setResult({ success: false, error: "Transfer failed" });
      toast({ title: "Transfer failed", message: "See details below or check Job Theater.", tone: "error" });
    }
    setTransferring(false);
  };

  const openJobTheater = () => {
    if (!transferLaunch) return;
    setActiveJobId(transferLaunch.jobId);
    setTransferLaunch(null);
    setStep(STEP_RUN);
  };

  const handleJobComplete = (job: JobProgress) => {
    setActiveJobId(null);
    setResult({
      success: job.status === "completed",
      records_transferred: job.records_processed,
      error: job.error,
      destination: {
        database: job.destination_database,
        collection: job.destination_collection,
      },
    });
    if (job.status === "completed") onTransferComplete();
  };

  const handleScheduleRoute = async () => {
    if (destKindMode !== "database" || !connectorId) {
      toast({
        title: "Saved destination required",
        message: "Select a saved destination connector to schedule a recurring pipeline.",
        tone: "info",
      });
      return;
    }
    if (!isConnectorSource || !sourceConnectorId) {
      toast({
        title: "Database source required",
        message: "Scheduling works for database-to-database routes with saved connectors on both ends.",
        tone: "info",
      });
      return;
    }
    const sourceTableName = sourceKind === "cloud" ? cloudPath.trim() : (sourceCollection || sourceTable);
    if (!sourceTableName || !targetCollection.trim()) {
      toast({ title: "Route incomplete", message: "Source and destination table names are required.", tone: "warning" });
      return;
    }
    try {
      await createSchedule({
        name: `${sourceConnector?.name ?? "Source"} → ${targetCollection}`,
        source_connector_id: sourceConnectorId,
        source_table: sourceTableName,
        dest_connector_id: connectorId,
        dest_table: targetCollection,
        interval: "daily",
        enabled: true,
      });
      toast({
        title: "Pipeline created",
        message: "Daily sync enabled. Manage cadence in Pipelines.",
        tone: "success",
      });
      onOpenSchedules?.();
    } catch (e) {
      toast({
        title: "Could not create pipeline",
        message: e instanceof Error ? e.message : "Schedule API failed",
        tone: "error",
      });
    }
  };

  const sourceInputsReady =
    sourceKind === "file"
      ? Boolean(parsed)
      : sourceKind === "cloud"
        ? Boolean(sourceConnectorId && cloudPath.trim())
        : Boolean(
            (sourceManualEnabled
              ? sourceManualType && (sourceManualHost || sourceManualConnectionString) && (sourceManualDatabase || sourceManualConnectionString) && (sourceTable || sourceCollection)
              : sourceConnectorId)
            && (sourceTable || sourceCollection),
          );

  const canConfigureDest =
    sourceKind === "file"
      ? Boolean(parsed)
      : Boolean(
          sourceInputsReady
          && (analysis?.columns.length || currentSourceColumns.length),
        );

  const canRunPreflight =
    canConfigureDest &&
    (destKindMode === "file_export" || Boolean(targetDb && targetCollection));

  const needsDbPreflight = destKindMode === "database";
  const canExecute =
    destKindMode === "file_export" ? canConfigureDest : Boolean(preflight?.passed);

  const destinationLabel = destKindMode === "file_export"
    ? exportFormat.toUpperCase()
    : `${destType}${targetCollection ? ` · ${targetCollection}` : " · not set"}`;
  const sourceLabel = sourceKind === "file"
    ? (file?.name ?? "Choose source")
    : sourceKind === "cloud"
      ? (cloudPath.trim() || sourceConnector?.name || "Cloud source")
      : (sourceConnector?.name ?? "Database source");
  const destLabelShort = canConfigureDest && targetCollection
    ? destinationLabel
    : "Choose destination";

  const mapSourceType = sourceKind === "file"
    ? (parsed?.file_type ?? file?.name.split(".").pop() ?? "file")
    : (sourceConnector?.type ?? "database");
  const mapSourceSubtitle = sourceKind === "file"
    ? `Uploaded file${parsed?.file_type ? ` · ${parsed.file_type.toUpperCase()}` : ""}${parsed?.row_count ? ` · ${parsed.row_count.toLocaleString()} rows` : ""}`
    : sourceKind === "cloud"
      ? `Cloud object${cloudPath ? ` · ${cloudPath}` : ""}`
      : sourceConnector
        ? `${sourceConnector.type}${sourceConnector.database ? ` · ${sourceConnector.database}` : ""}${(sourceCollection || sourceTable) ? ` · ${sourceCollection || sourceTable}` : ""}`
        : "Database source";
  const mapDestRouteLabel = destKindMode === "file_export"
    ? `${exportFormat.toUpperCase()} export`
    : destType === "dynamodb"
      ? (targetCollection || targetDb || destinationLabel)
      : targetCollection
        ? `${targetDb}.${targetCollection}`
        : destinationLabel;
  const mapDestRouteSubtitle = destKindMode === "file_export"
    ? "File export destination"
    : destType === "dynamodb"
      ? destSchemaLoading
        ? `Fetching existing schema from DynamoDB table ${targetCollection || targetDb}`
        : destColumns.length > 0
          ? `Existing DynamoDB table schema — ${destColumns.length} attributes introspected`
          : `Items will be written to DynamoDB table ${targetCollection || targetDb || "table"}`
      : destSchemaLoading
      ? `Fetching existing schema from ${destType} connector`
      : destColumns.length > 0
        ? `Existing ${destType} collection schema — ${destColumns.length} fields introspected`
        : `New schema will be created in ${targetDb}.${targetCollection || "collection"}`;
  const mapSourceColumnCount = columnMappings.length || analysis?.columns.length || currentSourceColumns.length;

  useEffect(() => {
    if (!(step === STEP_MAP && analyzing)) {
      setMappingProgress(0);
      setMappingPhase("Preparing schema context…");
      return;
    }

    const phaseForProgress = (value: number) => {
      if (value < 25) return "Preparing schema context…";
      if (value < 55) return "Profiling semantic intent…";
      if (value < 80) return "Matching source to destination fields…";
      if (value < 95) return "Scoring confidence and policy checks…";
      return "Finalizing mapping editor…";
    };

    setMappingProgress(10);
    setMappingPhase(phaseForProgress(10));

    const timer = window.setInterval(() => {
      setMappingProgress((prev) => {
        const next = Math.min(prev + Math.max(2, Math.round(Math.random() * 8)), 96);
        setMappingPhase(phaseForProgress(next));
        return next;
      });
    }, 260);

    return () => window.clearInterval(timer);
  }, [step, analyzing]);

  const transferInsightTone =
    transferring || activeJobId
      ? "live"
      : preflight && !preflight.passed
        ? "warn"
        : preflight?.passed
          ? "ok"
          : "info";
  const transferInsightPill = transferring ? "Running" : STEPS[step - 1]?.label ?? `Step ${step}`;
  const transferInsightMessage =
    transferring || activeJobId
      ? "Migration in progress — batch throughput and reconciliation stream to Job Theater."
      : step === STEP_SOURCE
        ? "Connect a file, database, or cloud object store as your source."
        : step === STEP_DESTINATION
          ? "Choose destination engine, connector, and sync policy before mapping."
          : step === STEP_MAP
          ? `${columnMappings.length || analysis?.columns.length || 0} columns mapped — review semantic matches against destination schema.`
          : step === STEP_VALIDATE
              ? preflight?.passed
                ? "All preflight gates passed — ready to execute."
                : preflight
                  ? "Preflight reported issues — resolve before running."
                  : "Run eight preflight gates before writing data."
              : canExecute
                ? "Execute the governed transfer with checksum proof."
                : "Complete prior steps to unlock execution.";

  return (
    <PageShell
      wide
      showHeader={false}
      className="df2-page-transfer-studio"
      title="Transfer Studio"
      description="Source → Destination → Map → Validate → Run"
    >
      <PageFrame className={`df2-transfer-studio-shell is-transfer-studio-active${step === STEP_MAP ? " is-map-step-active" : ""}`} showHonesty>
      <PageInsightStrip
        tone={transferInsightTone}
        pill={transferInsightPill}
        message={transferInsightMessage}
      />
      <PageMetricsRow
        compact
        columns={4}
        metrics={[
          { label: "Step", value: `${step}/5`, icon: "transfer" },
          { label: "Columns", value: columnMappings.length || analysis?.columns.length || "—", icon: "sparkle" },
          {
            label: "Preflight",
            value: preflight?.passed ? "Passed" : preflight ? "Issues" : "Pending",
            tone: preflight?.passed ? "green" : preflight ? "red" : undefined,
            icon: "gate",
          },
          { label: "Source rows", value: parsed?.row_count != null ? parsed.row_count.toLocaleString() : "—", icon: "trend" },
        ]}
      />
      <header className="df2-transfer-studio-chrome">
        <div className="df2-transfer-studio-chrome-row">
        <WizardSteps
          variant="studio"
          steps={STEPS}
          current={step}
          onStepClick={setStep}
          canGoTo={(n) =>
            n < step ||
            n === STEP_SOURCE ||
            (n === STEP_DESTINATION && (sourceKind === "file" ? !!parsed : Boolean(currentSourceColumns.length || analysis?.columns.length))) ||
            (n === STEP_MAP && canRunPreflight) ||
            (n === STEP_VALIDATE && canRunPreflight && columnMappings.length > 0) ||
            (n === STEP_RUN && canExecute)
          }
        />
        <TransferRouteBar
          sourceLabel={sourceLabel}
          destLabel={destLabelShort}
          sourceType={sourceKind === "file" ? "file" : sourceConnector?.type ?? sourceKind}
          destType={destKindMode === "file_export" ? exportFormat : destType}
          rowCount={parsed?.row_count}
          live={Boolean(activeJobId) || transferring}
        />
        </div>
      </header>

      <div className={`df2-transfer-studio-body ${step === STEP_MAP ? "is-map-step is-full-width" : ""}${step === STEP_VALIDATE ? " is-validate-step" : " is-full-width"}`}>
      <main className="df2-transfer-main-panel" key={step}>
      {step === STEP_MAP && columnMappings.length > 0 && !analyzing && (
        <TransferMapStep
          columnMappings={columnMappings}
          analysis={analysis}
          destColumns={destColumns}
          destSchemaLoading={destSchemaLoading}
          targetCollection={targetCollection}
          targetDatabase={targetDb}
          destKindMode={destKindMode}
          destType={destType}
          sourceLabel={sourceLabel}
          sourceSubtitle={mapSourceSubtitle}
          sourceType={mapSourceType}
          destRouteLabel={mapDestRouteLabel}
          destRouteSubtitle={mapDestRouteSubtitle}
          mappingReviewCount={mappingReviewCount}
          confidenceThreshold={confidenceThreshold}
          rowCount={parsed?.row_count ?? sourceRowEstimate ?? undefined}
          sourceColumnCount={mapSourceColumnCount}
          llmUsed={llmMappingUsed}
          onChangeMappings={setColumnMappings}
          onBack={() => setStep(STEP_DESTINATION)}
          onContinue={() => void goToPreflight()}
        />
      )}

      {step === STEP_MAP && !analyzing && columnMappings.length === 0 && (
        <div className="df2-transfer-step-panel">
          <EmptyState
            icon="sparkle"
            title="Preparing column mappings"
            description={analysis?.columns.length
              ? "Analysis finished but mappings did not load — retry to map source columns to your destination."
              : "Configure your destination first, then we will fetch the existing schema and map source columns intelligently."}
            action={
              <button
                type="button"
                className="df2-btn df2-btn-primary"
                onClick={() => (analysis?.columns.length ? void goToMapping() : setStep(STEP_DESTINATION))}
              >
                {analysis?.columns.length ? "Retry mapping" : "← Back to destination"}
              </button>
            }
          />
        </div>
      )}

      {step === STEP_MAP && analyzing && (
        <div className="df2-transfer-step-panel">
          <div className="df2-card-body df2-analyzing">
            <Spinner />
            <p className="df2-analyzing-title">Mapping source to destination…</p>
            <div className="df2-mapping-progress" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={mappingProgress}>
              <div className="df2-mapping-progress-meta">
                <strong>{mappingProgress}%</strong>
                <span>{mappingPhase}</span>
              </div>
              <div className="df2-mapping-progress-track">
                <span className="df2-mapping-progress-fill" style={{ width: `${mappingProgress}%` }} />
              </div>
            </div>
          </div>
        </div>
      )}

      {step === STEP_SOURCE && (
      <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-source-step">
        <div className="df2-card-head">
          <div>
            <h3 className="df2-card-title">Source</h3>
            <p className="df2-card-sub">Upload a file or connect a database / cloud bucket.</p>
          </div>
        </div>
        <div className="df2-card-body">
          <div className="df2-transfer-step-split">
            <div className="df2-transfer-step-primary">
              <div className="df2-field">
                <label className="df2-label">Where is your data?</label>
                <SourceKindTiles
                  value={sourceKind}
                  onChange={(kind) => {
                    setSourceKind(kind);
                    setSourceConnectorId("");
                    setTransferPlan(null);
                    setCloudPath("");
                  }}
                />
              </div>

          {sourceKind === "file" ? (
            <>
              <input ref={fileInputRef} type="file" accept=".json,.csv,.jsonl,.tsv,.parquet" onChange={handleFileSelect} hidden />
              {uploadError && (
                <div className="df2-alert df2-alert-error" role="alert">
                  <DtIcon name="x" size={16} />
                  <div>
                    <strong>Upload failed</strong>
                    <p>{uploadError}</p>
                  </div>
                </div>
              )}
              <div
                className={`df2-upload df2-upload-studio ${dragOver ? "drag-over" : ""} ${uploading ? "is-loading" : ""} ${parsed ? "has-file" : ""}`}
                onClick={() => !uploading && fileInputRef.current?.click()}
                onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
                onDragLeave={() => setDragOver(false)}
                onDrop={handleDrop}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fileInputRef.current?.click(); } }}
              >
                <div className="df2-upload-icon">
                  {uploading || analyzing ? <Spinner /> : <DtIcon name="upload" size={22} />}
                </div>
                <p className="df2-upload-title">
                  {uploading ? "Profiling source file…" : parsed ? "Replace source file" : "Drop your data file here"}
                </p>
                <p className="df2-upload-hint">
                  {uploading ? "Parsing schema and sampling rows" : "or click to browse · max 250 MB"}
                </p>
                <div className="df2-upload-formats">
                  {UPLOAD_FORMATS.map((fmt) => (
                    <span key={fmt} className="df2-upload-format-chip">{fmt}</span>
                  ))}
                </div>
              </div>
              {file && parsed && (
                <>
                  <div className="df2-upload-result">
                    <div className="df2-upload-result-main">
                      <span className="df2-badge df2-badge-live"><DtIcon name="check" size={14} /> {file.name}</span>
                      <span className="df2-upload-result-meta">
                        {formatFileSize(file.size)} · {parsed.row_count.toLocaleString()} rows · {parsed.columns.length} columns
                      </span>
                    </div>
                  </div>
                  {parsed.validation && !parsed.validation.ok && (
                    <div className="df2-csv-validation-alert" role="alert">
                      <DtIcon name="alert" size={16} />
                      <div>
                        <strong>{parsed.validation.issue_count} type mismatch{parsed.validation.issue_count === 1 ? "" : "es"} detected</strong>
                        <p>
                          Scanned {parsed.validation.rows_scanned.toLocaleString()} rows
                          {parsed.validation.full_scan === false && " (sample scan for large file)"}.
                          Fix source data or adjust column types in the Map step.
                        </p>
                        <ul className="df2-csv-validation-issues">
                          {parsed.validation.issues.slice(0, 6).map((issue) => (
                            <li key={issue}>{issue}</li>
                          ))}
                          {parsed.validation.issues.length > 6 && (
                            <li>+{parsed.validation.issues.length - 6} more issues</li>
                          )}
                        </ul>
                      </div>
                    </div>
                  )}
                </>
              )}
            </>
          ) : sourceKind === "cloud" ? (
            cloudSourceConnectors.length === 0 ? (
              <EmptyState
                icon="connectors"
                title="No cloud storage connectors"
                description="Add an S3, GCS, or Azure Blob connector first, then return here to pick a path."
                compact
              />
            ) : (
              <>
                <div className="df2-form-row">
                  <ConnectorSelect
                    id="cloud-source-connector"
                    label="Cloud connector"
                    value={sourceConnectorId}
                    onChange={setSourceConnectorId}
                    connectors={cloudSourceConnectors}
                    placeholder="Select S3 / GCS / Azure…"
                  />
                  <div className="df2-field df2-field-flex">
                    <label className="df2-label">Object path / prefix</label>
                    <input
                      className="df2-input"
                      value={cloudPath}
                      onChange={(e) => setCloudPath(e.target.value)}
                      placeholder="s3://bucket/path/orders.jsonl"
                    />
                  </div>
                </div>
                <p className="df2-label-hint" style={{ marginTop: 8 }}>
                  DataFlow will detect format from the object key and profile schema on continue.
                </p>
              </>
            )
          ) : (
            <div className="df2-source-connection">
              {dbSourceConnectors.length > 0 && (
                <div className="df2-form-row" style={{ alignItems: "center", gap: 12 }}>
                  <label className="df2-label">Connection source</label>
                  <button
                    type="button"
                    className="df2-btn df2-btn-ghost df2-btn-sm"
                    onClick={() => {
                      setSourceManualEnabled((v) => !v);
                      setSourceConnectorId("");
                    }}
                  >
                    {sourceManualEnabled ? "Use a saved connector" : "Connect manually"}
                  </button>
                </div>
              )}
              {sourceManualEnabled || dbSourceConnectors.length === 0 ? (
                <div className="df2-form-rows" style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                  <div className="df2-form-row">
                    <div className="df2-field df2-field-sm">
                      <label className="df2-label">Database type</label>
                      <select
                        className="df2-select"
                        value={sourceManualType}
                        onChange={(e) => setSourceManualType(e.target.value)}
                      >
                        {(liveSourceDbs.length ? liveSourceDbs : FALLBACK_SOURCE_DBS).map((t) => (
                          <option key={t} value={t}>
                            {getConnectorDefaults(t).label}
                          </option>
                        ))}
                      </select>
                    </div>
                    <div className="df2-field df2-field-flex">
                      <label className="df2-label">Host / endpoint</label>
                      <input
                        className="df2-input"
                        value={sourceManualHost}
                        onChange={(e) => setSourceManualHost(e.target.value)}
                        placeholder={sourceManualType === "dynamodb" ? "us-east-1" : "localhost"}
                      />
                    </div>
                    <div className="df2-field df2-field-xs">
                      <label className="df2-label">Port</label>
                      <input
                        className="df2-input"
                        type="number"
                        value={sourceManualPort}
                        onChange={(e) => setSourceManualPort(parseInt(e.target.value || "0", 10))}
                        placeholder={String(getConnectorDefaults(sourceManualType).port)}
                      />
                    </div>
                  </div>
                  <div className="df2-form-row">
                    <div className="df2-field df2-field-md">
                      <label className="df2-label">Database</label>
                      <input
                        className="df2-input"
                        value={sourceManualDatabase}
                        onChange={(e) => setSourceManualDatabase(e.target.value)}
                        placeholder={sourceManualType === "dynamodb" ? "table-name" : "dataflow"}
                      />
                    </div>
                    {["postgresql", "snowflake", "redshift"].includes(sourceManualType) && (
                      <div className="df2-field df2-field-md">
                        <label className="df2-label">Schema</label>
                        <input
                          className="df2-input"
                          value={sourceManualSchema}
                          onChange={(e) => setSourceManualSchema(e.target.value)}
                          placeholder="public"
                        />
                      </div>
                    )}
                    <div className="df2-field df2-field-md">
                      <label className="df2-label">Username</label>
                      <input
                        className="df2-input"
                        value={sourceManualUsername}
                        onChange={(e) => setSourceManualUsername(e.target.value)}
                        placeholder="dataflow"
                      />
                    </div>
                    <div className="df2-field df2-field-md">
                      <label className="df2-label">Password</label>
                      <input
                        className="df2-input"
                        type="password"
                        value={sourceManualPassword}
                        onChange={(e) => setSourceManualPassword(e.target.value)}
                        placeholder="••••••••"
                      />
                    </div>
                  </div>
                  <div className="df2-form-row">
                    <div className="df2-field df2-field-flex">
                      <label className="df2-label">Connection string (optional — overrides host fields)</label>
                      <input
                        className="df2-input"
                        value={sourceManualConnectionString}
                        onChange={(e) => setSourceManualConnectionString(e.target.value)}
                        placeholder="postgres://user:pass@host:5432/db or /path/to/db.sqlite"
                      />
                    </div>
                  </div>
                  <div className="df2-form-row">
                    <div className="df2-field df2-field-md">
                      <label className="df2-label">
                        {sourceManualType === "mongodb" ? "Collection" : sourceManualType === "dynamodb" ? "Table" : "Table"}
                      </label>
                      <input
                        className="df2-input"
                        value={sourceManualType === "mongodb" ? sourceCollection : sourceTable}
                        onChange={(e) => {
                          if (sourceManualType === "mongodb") setSourceCollection(e.target.value);
                          else setSourceTable(e.target.value);
                        }}
                        placeholder={
                          sourceManualType === "mongodb"
                            ? "orders"
                            : sourceManualType === "dynamodb"
                              ? sourceManualDatabase || "orders"
                              : "public.orders"
                        }
                      />
                    </div>
                  </div>
                </div>
              ) : (
                <div className="df2-form-row">
                  <ConnectorSelect
                    id="source-connector"
                    label="Source Connector"
                    value={sourceConnectorId}
                    onChange={setSourceConnectorId}
                    connectors={dbSourceConnectors}
                    placeholder="Select connector…"
                  />
                  <div className="df2-field df2-field-md">
                    <label className="df2-label">
                      {sourceConnector?.type === "mongodb"
                        ? "Collection"
                        : sourceConnector?.type === "dynamodb"
                          ? "Table"
                          : "Table"}
                    </label>
                    <input
                      className="df2-input"
                      value={sourceConnector?.type === "mongodb" ? sourceCollection : sourceTable}
                      onChange={(e) => {
                        if (sourceConnector?.type === "mongodb") setSourceCollection(e.target.value);
                        else setSourceTable(e.target.value);
                      }}
                      placeholder={
                        sourceConnector?.type === "mongodb"
                          ? "orders"
                          : sourceConnector?.type === "dynamodb"
                            ? sourceConnector.database || "orders"
                            : "public.orders"
                      }
                    />
                  </div>
                </div>
              )}
            </div>
          )}

            </div>

            <div className="df2-transfer-step-secondary">
              <SourceStepAside
                sourceKind={sourceKind}
                parsed={parsed}
                samplePreviewRows={samplePreviewRows}
                sourceConnector={sourceConnector}
                sourceColumns={currentSourceColumns}
                sourceSchema={currentSourceSchema}
                cloudPath={cloudPath}
                dbConnectors={dbSourceConnectors}
                cloudConnectors={cloudSourceConnectors}
                uploading={uploading}
                sourceManual={sourceManualEnabled}
                sourceManualType={sourceManualType}
              />
            </div>
          </div>
        </div>

        {sourceKind === "file" && parsed && (
          <div className="df2-card-footer df2-wizard-footer">
            <span className="df2-label-hint">Source profiled — choose where data should land next.</span>
            <button
              type="button"
              className="df2-btn df2-btn-primary"
              onClick={() => void proceedToDestination()}
              disabled={uploading}
            >
              Continue to Destination →
            </button>
          </div>
        )}
        {isConnectorSource && (sourceKind === "database" ? dbSourceConnectors.length > 0 : cloudSourceConnectors.length > 0) && (
          <div className="df2-card-footer df2-wizard-footer">
            <span className="df2-label-hint">
              {sourceKind === "cloud" ? "Select connector and path to continue" : "Select connector and table to continue"}
            </span>
            <button
              type="button"
              className="df2-btn df2-btn-primary"
              disabled={!sourceInputsReady || sourceIntrospecting}
              onClick={() => void proceedToDestination()}
            >
              {sourceIntrospecting ? <ButtonLoader label="Reading schema…" /> : "Continue to Destination →"}
            </button>
          </div>
        )}
      </div>
      )}

      {step === STEP_DESTINATION && (
      <div className={`df2-transfer-step-panel df2-transfer-step-viewport df2-dest-step${advancedOpen ? " is-advanced" : ""}`}>
        <div className="df2-card-head">
          <div>
            <h3 className="df2-card-title">Destination</h3>
            <p className="df2-card-sub">Pick a saved connector, then set database & collection — schema loads before mapping.</p>
          </div>
        </div>
        <div className="df2-card-body">
          <div className="df2-field">
            <label className="df2-label">Destination Mode</label>
            <FilterTabs
              ariaLabel="Destination mode"
              className="df2-filter-tabs--field"
              value={destKindMode}
              onChange={(mode) => {
                setDestKindMode(mode);
                setTransferPlan(null);
                if (mode === "file_export") void loadTransferPlan();
              }}
              items={[
                { id: "database", label: "Database / Warehouse" },
                { id: "file_export", label: "File Export" },
              ]}
            />
          </div>

          {destKindMode === "file_export" ? (
            <div className="df2-field">
              <label className="df2-label">Export Format</label>
              <FilterTabs
                ariaLabel="Export format"
                className="df2-filter-tabs--field"
                value={exportFormat}
                onChange={(format) => {
                  setExportFormat(format);
                  setTransferPlan(null);
                }}
                items={liveExportFormats.map((f) => ({ id: f.id, label: f.label }))}
              />
            </div>
          ) : (
            <>
          <DestinationPicker
            connectors={connectors}
            connectorId={connectorId}
            destType={destType}
            liveDestTypes={liveDestTypes}
            onSelectConnector={applyConnectorSelection}
            onSelectManual={() => setConnectorId("")}
            onSelectType={(type) => {
              setDestType(type);
              setConnectorId("");
              setDestPort(defaultPortForType(type));
            }}
          />

          {!connectorId && destType !== "bigquery" && (
          <div className="df2-dest-section df2-dest-manual-fields">
            <label className="df2-label">Connection</label>
            <div className="df2-form-row">
              {destType === "mongodb" ? (
                <div className="df2-field df2-field-flex">
                  <label className="df2-label">Connection String (optional)</label>
                  <input
                    className="df2-input"
                    value={destConnectionString}
                    onChange={(e) => setDestConnectionString(e.target.value)}
                    placeholder="mongodb://localhost:27017/"
                  />
                </div>
              ) : null}
              <div className="df2-field df2-field-md">
                <label className="df2-label">Host</label>
                <input className="df2-input" value={destHost} onChange={(e) => setDestHost(e.target.value)} />
              </div>
              <div className="df2-field df2-field-sm">
                <label className="df2-label">Port</label>
                <input type="number" className="df2-input" value={destPort} onChange={(e) => setDestPort(Number(e.target.value))} />
              </div>
              {destType !== "mongodb" && (
                <>
                  <div className="df2-field df2-field-140">
                    <label className="df2-label">Username</label>
                    <input className="df2-input" value={destUsername} onChange={(e) => setDestUsername(e.target.value)} />
                  </div>
                  <div className="df2-field df2-field-140">
                    <label className="df2-label">Password</label>
                    <input type="password" className="df2-input" value={destPassword} onChange={(e) => setDestPassword(e.target.value)} />
                  </div>
                </>
              )}
              {destType === "snowflake" && (
                <div className="df2-field df2-field-160">
                  <label className="df2-label">Warehouse</label>
                  <input className="df2-input" value={destWarehouse} onChange={(e) => setDestWarehouse(e.target.value)} placeholder="COMPUTE_WH" />
                </div>
              )}
            </div>
          </div>
          )}

          {connectorId && selectedDestConnector && (
            <p className="df2-connector-hint">
              Using <strong>{selectedDestConnector.name}</strong> ({selectedDestConnector.host}:{selectedDestConnector.port})
            </p>
          )}

          <div className="df2-dest-section df2-dest-target-fields">
            <label className="df2-label">Target location</label>
            <div className="df2-form-row">
            <div className="df2-field df2-field-flex">
              <label className="df2-label" htmlFor="dest-db">
                {destType === "bigquery"
                  ? "GCP Project ID"
                  : destType === "dynamodb"
                    ? "AWS region or local endpoint"
                    : "Database"}
              </label>
              <input id="dest-db" className="df2-input" value={targetDb} onChange={(e) => setTargetDb(e.target.value)} placeholder={destType === "bigquery" ? "my-gcp-project" : destType === "dynamodb" ? "us-east-1" : "test_db"} />
            </div>
            {destType === "bigquery" && (
              <div className="df2-field df2-field-flex">
                <label className="df2-label">Dataset</label>
                <input className="df2-input" value={destSchema} onChange={(e) => setDestSchema(e.target.value)} placeholder="dataflow" />
              </div>
            )}
            <div className="df2-field df2-field-flex">
              <label className="df2-label" htmlFor="dest-col">
                {destType === "mongodb" ? "Collection" : destType === "dynamodb" ? "DynamoDB table" : "Table"}
              </label>
              <input id="dest-col" className="df2-input" value={targetCollection} onChange={(e) => setTargetCollection(e.target.value)} placeholder={destType === "mongodb" ? "my_collection" : destType === "dynamodb" ? "orders" : "my_table"} />
            </div>
            {destType === "postgresql" && (
              <div className="df2-field df2-field-120">
                <label className="df2-label">Schema</label>
                <input className="df2-input" value={destSchema} onChange={(e) => setDestSchema(e.target.value)} />
              </div>
            )}
          </div>
          </div>
          {destType === "dynamodb" && (
            <p className="df2-label-hint df2-field-note">
              Set region to <code>us-east-1</code> for AWS, or <code>http://localhost:8000</code> for DynamoDB Local / personal cloud.
              Table name is the DynamoDB table to read or write.
            </p>
          )}
          {destType === "bigquery" && (
            <p className="df2-label-hint df2-field-note">
              Set Database to GCP project ID. Optional: save service account JSON path as connection string in connector settings.
            </p>
          )}
            </>
          )}

          <div className="df2-policy-console">
            <div className="df2-policy-head">
              <div>
                <span className="df2-rail-kicker">Sync contract</span>
                <h4>{advancedOpen ? "Sync and schema policy" : "Defaults applied"}</h4>
              </div>
              <div className="df2-policy-head-actions">
                <button
                  type="button"
                  className="df2-btn df2-btn-ghost df2-btn-sm"
                  onClick={() => setAdvancedOpen((o) => !o)}
                >
                  {advancedOpen ? "Hide advanced" : "Advanced mode"}
                </button>
                <span className={`df2-badge ${streamNeedsReview ? "df2-badge-run" : "df2-badge-live"}`}>
                  {currentSourceColumns.length ? (streamNeedsReview ? "Review required" : "Ready") : "Waiting for schema"}
                </span>
              </div>
            </div>

            {advancedOpen ? (
            <>
            <div className="df2-policy-grid">
              <div className="df2-field">
                <label className="df2-label">Sync Mode</label>
                <div className="df2-policy-options">
                  {SYNC_MODES.map((mode) => (
                    <button
                      key={mode.id}
                      type="button"
                      className={`df2-policy-option ${syncMode === mode.id ? "active" : ""}`}
                      onClick={() => setSyncMode(mode.id)}
                    >
                      <strong>{mode.label}</strong>
                      <span>{mode.detail}</span>
                    </button>
                  ))}
                </div>
              </div>

              <div className="df2-field">
                <label className="df2-label">Schema Change Policy</label>
                <div className="df2-policy-options">
                  {SCHEMA_POLICIES.map((policy) => (
                    <button
                      key={policy.id}
                      type="button"
                      className={`df2-policy-option ${schemaPolicy === policy.id ? "active" : ""}`}
                      onClick={() => setSchemaPolicy(policy.id)}
                    >
                      <strong>{policy.label}</strong>
                      <span>{policy.detail}</span>
                    </button>
                  ))}
                </div>
              </div>
            </div>

            <div className="df2-policy-toolbar">
              <div className="df2-field">
                <label className="df2-label">Validation</label>
                <FilterTabs
                  ariaLabel="Validation mode"
                  className="df2-filter-tabs--field"
                  value={validationMode}
                  onChange={setValidationMode}
                  items={VALIDATION_MODES.map((mode) => ({ id: mode.id, label: mode.label }))}
                />
              </div>
              <label className="df2-policy-toggle">
                <input
                  type="checkbox"
                  checked={backfillNewFields}
                  disabled={!["propagate_columns", "propagate_all"].includes(schemaPolicy)}
                  onChange={(e) => setBackfillNewFields(e.target.checked)}
                />
                <span>
                  <strong>Backfill new fields</strong>
                  <small>
                    {["propagate_columns", "propagate_all"].includes(schemaPolicy)
                      ? "Requires automatic column propagation"
                      : "Enable Column changes or All changes schema policy first"}
                  </small>
                </span>
              </label>
            </div>

            <div className="df2-stream-contract">
              <div className="df2-stream-head">
                <strong>Streams and fields</strong>
                <span>{currentSourceColumns.length} discovered fields</span>
              </div>
              <div className="df2-stream-cards" aria-label="Stream contract">
                <article className="df2-stream-card">
                  <header className="df2-stream-card-head">
                    <strong>{sourceStreamName}</strong>
                    <span className="df2-badge df2-badge-live df2-badge-xs">{syncModeLabel}</span>
                  </header>
                  <dl className="df2-stream-card-meta">
                    <div><dt>Fields</dt><dd>{currentSourceColumns.length || "—"}</dd></div>
                    <div><dt>Policy</dt><dd>{schemaPolicyLabel}</dd></div>
                    <div><dt>Status</dt><dd>{currentSourceColumns.length ? "Ready" : "Pending schema"}</dd></div>
                  </dl>
                  <div className="df2-stream-card-fields">
                    <label className="df2-label">Cursor field</label>
                    <select
                      className="df2-input df2-select df2-stream-select"
                      value={requiresCursor ? cursorField : ""}
                      disabled={!requiresCursor || currentSourceColumns.length === 0}
                      onChange={(e) => setCursorField(e.target.value)}
                    >
                      <option value="">{requiresCursor ? "Select cursor" : "Not required"}</option>
                      {currentSourceColumns.map((col) => (
                        <option key={col} value={col}>
                          {col}{currentSourceSchema[col] ? ` · ${currentSourceSchema[col]}` : ""}
                        </option>
                      ))}
                    </select>
                    <label className="df2-label">Primary key</label>
                    <select
                      className="df2-input df2-select df2-stream-select"
                      value={requiresPrimaryKey ? primaryKeyField : ""}
                      disabled={!requiresPrimaryKey || currentSourceColumns.length === 0}
                      onChange={(e) => setPrimaryKeyField(e.target.value)}
                    >
                      <option value="">{requiresPrimaryKey ? "Select key" : "Not required"}</option>
                      {currentSourceColumns.map((col) => (
                        <option key={col} value={col}>
                          {col}{currentSourceSchema[col] ? ` · ${currentSourceSchema[col]}` : ""}
                        </option>
                      ))}
                    </select>
                  </div>
                </article>
              </div>
              <div className="df2-stream-table-wrap df2-stream-table-desktop">
                <table className="df2-stream-table">
                  <thead>
                    <tr>
                      <th>Stream</th>
                      <th>Mode</th>
                      <th>Cursor</th>
                      <th>Primary key</th>
                      <th>Policy</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>
                        <label className="df2-stream-name">
                          <input type="checkbox" checked readOnly aria-label="Stream selected" />
                          <span>
                            <strong>{sourceStreamName}</strong>
                            <small>{currentSourceColumns.length ? `${currentSourceColumns.length} fields` : "No schema loaded"}</small>
                          </span>
                        </label>
                      </td>
                      <td>{syncModeLabel}</td>
                      <td>
                        <select
                          className="df2-input df2-select df2-stream-select"
                          value={requiresCursor ? cursorField : ""}
                          disabled={!requiresCursor || currentSourceColumns.length === 0}
                          onChange={(e) => setCursorField(e.target.value)}
                        >
                          <option value="">{requiresCursor ? "Select cursor" : "Not required"}</option>
                          {currentSourceColumns.map((col) => (
                            <option key={col} value={col}>
                              {col}{currentSourceSchema[col] ? ` · ${currentSourceSchema[col]}` : ""}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td>
                        <select
                          className="df2-input df2-select df2-stream-select"
                          value={requiresPrimaryKey ? primaryKeyField : ""}
                          disabled={!requiresPrimaryKey || currentSourceColumns.length === 0}
                          onChange={(e) => setPrimaryKeyField(e.target.value)}
                        >
                          <option value="">{requiresPrimaryKey ? "Select key" : "Not required"}</option>
                          {currentSourceColumns.map((col) => (
                            <option key={col} value={col}>
                              {col}{currentSourceSchema[col] ? ` · ${currentSourceSchema[col]}` : ""}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td>{schemaPolicyLabel}</td>
                      <td>
                        <span className={`df2-badge ${streamNeedsReview ? "df2-badge-run" : "df2-badge-live"}`}>
                          {currentSourceColumns.length ? (streamNeedsReview ? "Needs contract" : "Valid") : "Pending"}
                        </span>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
            </>
            ) : (
              <p className="df2-label-hint" style={{ margin: "8px 0 0" }}>
                Defaults: append to existing data · manual schema approval · balanced validation.
                Open Advanced mode for overwrite, CDC, incremental cursors, and drift policies.
              </p>
            )}
          </div>

          {destKindMode === "database" && destColumns.length > 0 && (
            <div className="df2-dest-schema-preview">
              <StructurePreview
                columns={destColumns}
                schema={destSchemaMap}
                title="Existing destination schema"
                subtitle={`${destColumns.length} fields in ${targetDb}.${targetCollection} — mapping will align to these columns`}
              />
            </div>
          )}
          {destKindMode === "database" && destSchemaLoading && (
            <p className="df2-label-hint df2-dest-schema-loading">
              <Spinner size="sm" /> Fetching existing schema from destination…
            </p>
          )}

          {destKindMode === "database" && destTableExists && destColumns.length > 0 && (
            <p className="df2-label-hint df2-append-hint">
              Existing {destType} table detected — new rows will <strong>append</strong> by default. Open Advanced to switch to overwrite or incremental sync.
            </p>
          )}

          {transferPlan && (
            <div className={`df2-plan-callout${transferPlan.supported ? " is-ready" : " is-warn"}`}>
              <p className="df2-plan-callout-title">
                {transferPlan.supported ? "Route ready" : "Route needs attention"} · {transferPlan.operation}
                {!transferPlan.supported && (
                  <span className="df2-badge df2-badge-run">{transferPlan.message}</span>
                )}
              </p>
              {transferPlan.auto_create.length > 0 && (
                <ul className="df2-plan-callout-list">
                  {transferPlan.auto_create.slice(0, 3).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                  {transferPlan.auto_create.length > 3 && (
                    <li className="df2-plan-callout-more">+{transferPlan.auto_create.length - 3} more steps</li>
                  )}
                </ul>
              )}
              {transferPlan.type_mappings.length > 0 && (
                <p className="df2-plan-callout-meta">
                  {transferPlan.type_mappings.length} column type mappings
                </p>
              )}
            </div>
          )}
        </div>
        <div className="df2-card-footer df2-wizard-footer">
          <button type="button" className="df2-btn" onClick={() => setStep(STEP_SOURCE)}>← Back to source</button>
          <div className="df2-btn-row">
          <button
            type="button"
            className="df2-btn"
            onClick={() => void loadTransferPlan()}
            disabled={!canConfigureDest || planLoading}
          >
            {planLoading ? "Analyzing…" : "Analyze Route"}
          </button>
          <button
            type="button"
            className="df2-btn df2-btn-primary"
            onClick={() => void goToMapping()}
            disabled={!canRunPreflight || analyzing}
          >
            {analyzing ? <ButtonLoader label="Preparing mappings…" /> : <><DtIcon name="sparkle" size={18} /> Continue to Map</>}
          </button>
          </div>
        </div>
      </div>
      )}

      {step === STEP_VALIDATE && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-validate-step df2-validate-split">
          <div className="df2-proof-dashboard-wrap">
            <ProofDashboard preflight={preflight} running={preflighting} />
          </div>
          <PreflightTimeline
            result={preflight ?? {
              passed: false,
              passed_count: 0,
              total_gates: 11,
              readiness_score: 0,
              gates: [],
              blockers: [],
            }}
            running={preflighting}
            confidenceThreshold={confidenceThreshold}
            compact
            hideActions
          />
        </div>
      )}

      {step === STEP_RUN && !activeJobId && !result && !transferring && !transferLaunch && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-run-step">
          <div className="df2-card-body df2-run-center">
            <div className="df2-run-readiness" aria-label="Run readiness summary">
              <div className="df2-run-readiness-head">
                <span className="df2-badge df2-badge-live">
                  <DtIcon name="check" size={12} /> Preflight passed
                </span>
                <span className="df2-run-readiness-score">
                  {preflight ? `${preflight.passed_count}/${preflight.total_gates} checks` : "Validated"}
                </span>
              </div>
              <div className="df2-run-readiness-route">
                <strong>{sourceLabel}</strong>
                <DtIcon name="transfer" size={14} />
                <strong>{mapDestRouteLabel}</strong>
              </div>
              <p>
                Execute now to start governed transfer with live theater progress and reconciliation evidence.
              </p>
            </div>
            <EmptyState
              icon="transfer"
              title="Ready to transfer"
              description="Preflight passed — execute the transfer to move your data."
              action={
                <button type="button" className="df2-btn df2-btn-primary df2-btn-lg" onClick={() => void executeTransfer()}>
                  <DtIcon name="transfer" size={18} /> Execute Transfer
                </button>
              }
            />
          </div>
        </div>
      )}

      {step === STEP_RUN && transferring && !activeJobId && !result && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-run-step">
          <div className="df2-card-body df2-run-center df2-analyzing">
            <ButtonLoader label="Starting transfer…" />
          </div>
        </div>
      )}

      {step === STEP_RUN && activeJobId && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-run-step">
          <div className="df2-card-body">
            <JobTheater
              jobId={activeJobId}
              sourceLabel={file?.name || sourceConnector?.name}
              destLabel={`${targetDb}.${targetCollection}`}
              sourceType={sourceKind === "file" ? "file" : sourceConnector?.type || sourceKind}
              destType={destKindMode === "file_export" ? exportFormat : destType}
              onComplete={handleJobComplete}
              onFailed={handleJobComplete}
            />
          </div>
        </div>
      )}

      {step === STEP_RUN && result && !activeJobId && (
        <div className={`df2-transfer-step-panel df2-transfer-step-viewport df2-run-step df2-result-banner df2-transfer-panel ${result.success ? "success" : "error"}`}>
          {result.success ? (
            <div>
              <span className="df2-badge df2-badge-live df2-result-badge"><DtIcon name="check" size={14} /> Transfer Complete</span>
              <p className="df2-result-stat">{result.records_transferred?.toLocaleString()} records transferred</p>
              {result.destination?.path && (
                <p className="df2-result-meta">Exported to {result.destination.path}</p>
              )}
              {result.ddl_executed && result.ddl_executed.length > 0 && (
                <ul className="df2-result-ddl">
                  {result.ddl_executed.map((d) => <li key={d}>{d}</li>)}
                </ul>
              )}
              <div className="df2-result-actions">
                <button type="button" className="df2-btn df2-btn-primary" onClick={() => setStep(STEP_SOURCE)}>
                  <DtIcon name="plus" size={14} /> New transfer
                </button>
                <button
                  type="button"
                  className="df2-btn"
                  onClick={() => void handleScheduleRoute()}
                >
                  <DtIcon name="activity" size={14} /> Schedule this route
                </button>
              </div>
            </div>
          ) : (
            <span className="df2-badge df2-badge-error"><DtIcon name="x" size={14} /> {result.error || "Transfer failed"}</span>
          )}
        </div>
      )}
      </main>
      {step === STEP_VALIDATE && (
        <ValidateActionsRail
          preflight={preflight}
          preflighting={preflighting}
          transferring={transferring}
          mappingReviewCount={mappingReviewCount}
          rowCount={parsed?.row_count ?? sourceRowEstimate ?? undefined}
          transferLaunch={transferLaunch}
          onBack={() => setStep(STEP_MAP)}
          onRunPreflight={() => void executePreflight()}
          onApproveMappings={() => void approveAllAndPreflight()}
          onExecute={() => void executeTransfer()}
          onOpenJobTheater={openJobTheater}
        />
      )}
      </div>
      </PageFrame>
    </PageShell>
  );
}
