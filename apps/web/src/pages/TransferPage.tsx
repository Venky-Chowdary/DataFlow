import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { JobTheater } from "../components/JobTheater";
import { DtIcon } from "../components/DtIcon";
import { EmptyState } from "../components/ui/EmptyState";
import { ConnectorIcon } from "../app/brand-icons";
import { ConnectorSelect } from "../components/ui/ConnectorSelect";
import { SourceKindTiles, type SourceKind } from "../components/ui/SourceKindTiles";
import { StructurePreview } from "../components/ui/StructurePreview";
import { PageFrame } from "../components/ui/PageFrame";
import { FilterTabs } from "../components/ui/FilterTabs";
import { FilterBar } from "../components/ui/FilterBar";
import { PageShell } from "../components/ui/PageShell";
import { WizardSteps } from "../components/ui/WizardSteps";
import { ButtonLoader, LoadingBlock, Spinner } from "../components/LoadingState";
import { useToast } from "../components/Toast";
import { TransferMapStep } from "./transfer/TransferMapStep";
import { DestinationPicker } from "../components/transfer/DestinationPicker";
import { DestinationAdvancedDrawer } from "../components/transfer/DestinationAdvancedDrawer";
import { Button } from "../components/ui/Button";
import { SourceStepAside } from "../components/transfer/SourceStepAside";
import { ValidateActionsRail } from "../components/transfer/ValidateActionsRail";
import { ValidateDashboard } from "../components/transfer/ValidateDashboard";
import { TransferResultDashboard } from "../components/transfer/TransferResultDashboard";
import { TransferRouteBar } from "../components/transfer/TransferRouteBar";
import {
  MappingProofDrawer,
  mergeMappingProof,
} from "../components/MappingProofDrawer";
import { useActiveData } from "../lib/DataContext";
import { useStudioActions, type StudioAction } from "../lib/StudioActionsContext";
import {
  analyzeDbTransfer,
  analyzeFileTransfer,
  analyzeTransferRoute,
  analyzeSchemaEnhanced,
  approveTransferPlan,
  buildColumnSamples,
  createContractFromTransfer,
  createSchedule,
  createTransferPlan,
  fetchTransferCapabilities,
  introspectTransferEndpoints,
  mapTransferColumns,
  mapTransferPlan,
  preflightTransferPlan,
  previewQuarantineCells,
  runPreflight,
  runUniversalTransfer,
  syncTransferPlanMappings,
  updateTransferPlan,
  uploadFile,
  type CellPreviewResult,
} from "../lib/api";
import { defaultPortForType, getConnectorDefaults, getGenericSqlGroup, getGenericSqlPlaceholder, isGenericSql, isTransferLiveType, resolveDriverType } from "../lib/connectorTypes";
import { isJobSuccess } from "../lib/uiUtils";
import {
  parseStreamNames,
  primaryStreamName,
  type StreamSchemaPreview,
} from "../lib/sourceStreams";
import {
  buildPreflightMappings,
  confidenceThresholdForMode,
  editableFromPipelineMappings,
  isEnumToBooleanConflict,
  widenMappingToVarchar,
  mappingsFromAnalysis,
  type EditableMapping,
  type MappingTransform,
} from "../lib/mapping";
import {
  Connector,
  EnhancedAnalysis,
  ParsedUpload,
  PreflightResult,
  TransferPlan,
  TransferResult,
  JobProgress,
  ValidationSuggestedAction,
} from "../lib/types";
import { parseCsvTextForPreview } from "../lib/csvPreview";
import { runLocalFileExport } from "../lib/localFileExport";
import { runLocalPreflight } from "../lib/localPreflight";
import { readJobEventLog } from "../lib/jobEventLog";
import { schemaIntrospectionFailureMessage } from "../lib/preflightMessages";
import {
  buildStreamContracts,
  seedStreamFieldsFromCandidates,
  streamContractsNeedReview,
  type StreamFieldContract,
} from "../lib/streamContracts";

interface TransferPageProps {
  connectors: Connector[];
  /** True while the first connectors fetch has not settled yet. */
  connectorsLoading?: boolean;
  onTransferComplete: () => void;
  onOpenSchedules?: () => void;
  /** Jump to Contracts after Save as contract so the draft is visible immediately. */
  onOpenContracts?: () => void;
  /** Remount studio and clear prior transfer cache (source, map, result). */
  onFreshTransfer?: () => void;
  /** Pre-select a saved connection as the Transfer Studio source (from Connectors drawer). */
  seedSourceConnector?: { connectorId: string; token: number } | null;
}

/** File formats are never listed as database sources. */
const FILE_FORMAT_SOURCE_TYPES = new Set([
  "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet", "avro", "orc", "xml",
]);

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

const RUN_LAUNCH_STAGES = [
  "Submitting governed job request",
  "Locking approved mapping revision",
  "Provisioning destination writer",
  "Opening live telemetry stream",
] as const;

const CLOUD_SOURCE_TYPES = new Set(["s3", "gcs", "google_cloud_storage", "azure_blob", "adls"]);

const FALLBACK_DEST_TYPES = ["mongodb", "postgresql", "mysql", "snowflake", "bigquery"] as const;
const FALLBACK_EXPORT_FORMATS = ["csv", "json", "jsonl"] as const;

const ACCEPTED_UPLOAD_EXTENSIONS = new Set(["csv", "json", "jsonl", "tsv", "parquet"]);
const MAX_UPLOAD_BYTES = 250 * 1024 * 1024;
const UPLOAD_FORMATS = ["JSON", "CSV", "JSONL", "TSV", "Parquet"] as const;

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

type SyncMode =
  | "full_refresh_overwrite"
  | "full_refresh_append"
  | "incremental_append"
  | "incremental_deduped"
  | "cdc"
  | "scd2"
  | "mirror";
type SchemaPolicy = "manual_review" | "propagate_columns" | "propagate_all" | "pause_on_change" | "type_locked";
type ValidationMode = "balanced" | "strict" | "maximum";

const SYNC_MODES: { id: SyncMode; label: string; detail: string }[] = [
  { id: "full_refresh_overwrite", label: "Full overwrite", detail: "Drop/replace destination, then load the full snapshot." },
  { id: "full_refresh_append", label: "Full append", detail: "Keep existing rows; append the full snapshot (100k + 100k → 200k)." },
  { id: "incremental_append", label: "Incremental append", detail: "Cursor-based new rows only — never rewrites history." },
  { id: "incremental_deduped", label: "Incremental deduped", detail: "Cursor + primary key upserts for a final table." },
  { id: "cdc", label: "CDC", detail: "Log-based changes with cursor + key; at-least-once upsert until proven otherwise." },
  { id: "scd2", label: "SCD Type 2", detail: "Versioned history with valid-from / valid-to; requires primary key." },
  { id: "mirror", label: "Mirror", detail: "Keep destination in sync with soft-deletes for missing keys; requires primary key." },
];

const SCHEMA_POLICIES: { id: SchemaPolicy; label: string; detail: string }[] = [
  {
    id: "manual_review",
    label: "Manual approval",
    detail: "Detect drift; keep the approved contract until you review (safest default).",
  },
  {
    id: "propagate_columns",
    label: "Propagate columns",
    detail: "Auto-add new destination columns on transfer (type changes still need review).",
  },
  {
    id: "propagate_all",
    label: "Propagate everything",
    detail: "Auto-add columns like Propagate columns; incompatible type changes still need review.",
  },
  {
    id: "pause_on_change",
    label: "Pause on drift",
    detail: "Stop scheduled runs when schema changes — best for production warehouses.",
  },
  {
    id: "type_locked",
    label: "Type locked",
    detail: "Reject type changes at the destination — fail closed on incompatible casts.",
  },
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

export function TransferPage({
  connectors,
  connectorsLoading = false,
  onTransferComplete,
  onOpenSchedules,
  onOpenContracts,
  onFreshTransfer,
  seedSourceConnector = null,
}: TransferPageProps) {
  const { toast } = useToast();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const autoSelectedConnector = useRef(false);
  const autoSelectedSourceConnector = useRef(false);
  /** Last applied Connectors→Studio seed token (prevents re-seeding on connectors refresh). */
  const appliedSeedTokenRef = useRef<number | null>(null);
  /** Last destination identity we auto-analyzed — empty means not analyzed yet. */
  const routeAnalyzedKeyRef = useRef("");
  const { setActiveData } = useActiveData();
  const { registerStudioHandler } = useStudioActions();
  const [step, setStep] = useState(STEP_SOURCE);
  const [sourceKind, setSourceKind] = useState<SourceKind>("file");
  const [sourceConnectorId, setSourceConnectorId] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [sourceCollection, setSourceCollection] = useState("");
  const [cloudPath, setCloudPath] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [parsed, setParsed] = useState<ParsedUpload | null>(null);
  const [sourceRowEstimate, setSourceRowEstimate] = useState<number | null>(null);
  const [analysis, setAnalysis] = useState<EnhancedAnalysis | null>(null);
  const [preflight, setPreflight] = useState<PreflightResult | null>(null);
  const [cellPreview, setCellPreview] = useState<CellPreviewResult | null>(null);
  const [analyzing, setAnalyzing] = useState(false);
  const [mappingProgress, setMappingProgress] = useState(0);
  const [mappingPhase, setMappingPhase] = useState("Preparing schema context…");
  const [sourceIntrospecting, setSourceIntrospecting] = useState(false);
  const [sourceIntrospectError, setSourceIntrospectError] = useState<string | null>(null);
  /** Per-stream schema previews for comma-separated multi-stream sources. */
  const [streamPreviews, setStreamPreviews] = useState<StreamSchemaPreview[]>([]);
  const [activeStreamTab, setActiveStreamTab] = useState("");
  /** Prevents auto-introspect from looping after timeout/error for the same source. */
  const sourceIntrospectGateRef = useRef<{ key: string; status: "idle" | "running" | "ok" | "error" }>({
    key: "",
    status: "idle",
  });
  const sourceIntrospectGenRef = useRef(0);
  const [preflighting, setPreflighting] = useState(false);
  const [savingContract, setSavingContract] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [connectorId, setConnectorId] = useState("");
  /** Empty until the operator picks a destination — never default to MongoDB. */
  const [destType, setDestType] = useState<string>("");
  const [destKindMode, setDestKindMode] = useState<"database" | "file_export">("database");
  const destDriverType = destType ? resolveDriverType(destType) : "";
  const destSelected = destKindMode === "file_export" || Boolean(destType);
  const [exportFormat, setExportFormat] = useState("json");
  const [transferPlan, setTransferPlan] = useState<TransferPlan | null>(null);
  const [persistedPlanId, setPersistedPlanId] = useState<string | null>(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [targetDb, setTargetDb] = useState("dataflow_test");
  const [targetCollection, setTargetCollection] = useState("");
  const [destHost, setDestHost] = useState("");
  const [destPort, setDestPort] = useState(0);
  const [destSchema, setDestSchema] = useState("public");
  const [destUsername, setDestUsername] = useState("");
  const [destPassword, setDestPassword] = useState("");
  const [destConnectionString, setDestConnectionString] = useState("");
  const [destOutputPath, setDestOutputPath] = useState("");
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
  const [priorityColumn, setPriorityColumn] = useState("");
  const [priorityDirection, setPriorityDirection] = useState<"asc" | "desc">("desc");
  const [rowLimit, setRowLimit] = useState(0);
  /** Per-stream cursor/PK when source lists multiple tables (comma-separated). */
  const [streamFields, setStreamFields] = useState<Record<string, StreamFieldContract>>({});
  const [columnMappings, setColumnMappings] = useState<EditableMapping[]>([]);
  /** Per-stream column mappings when source lists multiple tables. */
  const [streamMappings, setStreamMappings] = useState<Record<string, EditableMapping[]>>({});
  const [mapActiveStream, setMapActiveStream] = useState<string | null>(null);
  const [destColumns, setDestColumns] = useState<string[]>([]);
  const [destSchemaMap, setDestSchemaMap] = useState<Record<string, string>>({});
  const [destSchemaLoading, setDestSchemaLoading] = useState(false);
  const [destTableExists, setDestTableExists] = useState<boolean | null>(null);
  const [liveSourceTypes, setLiveSourceTypes] = useState<string[]>([]);
  const [liveDestTypes, setLiveDestTypes] = useState<{ id: string; label: string }[]>(
    () => FALLBACK_DEST_TYPES.map((id) => ({ id, label: getConnectorDefaults(id).label })),
  );
  const [liveExportFormats, setLiveExportFormats] = useState<{ id: string; label: string }[]>(
    () => FALLBACK_EXPORT_FORMATS.map((id) => ({ id, label: id.toUpperCase() })),
  );
  const [liveRouteCount, setLiveRouteCount] = useState<number | null>(null);
  const [transferLaunch, setTransferLaunch] = useState<{ jobId: string; rows: number } | null>(null);
  const [llmMappingUsed, setLlmMappingUsed] = useState(false);
  const [mappingProof, setMappingProof] = useState<import("../components/MappingProofDrawer").MappingProof | null>(null);
  const [mappingProofOpen, setMappingProofOpen] = useState(false);
  const [runStartupProgress, setRunStartupProgress] = useState(0);
  const [runStartupPhase, setRunStartupPhase] = useState<string>(RUN_LAUNCH_STAGES[0]);

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
    setParsed(null);
    setStreamPreviews([]);
    setActiveStreamTab("");
    setSourceIntrospectError(null);
    sourceIntrospectGateRef.current = { key: "", status: "idle" };
    // Only reset when the connector or source kind changes, not while the user
    // is still typing a table/collection name.  That prevents the preview from
    // flickering blank between keystrokes and keeps the last valid schema
    // visible until the new introspection completes.
  }, [sourceConnectorId, sourceKind]);

  const buildDestinationEndpoint = () => {
    const isMongo = destDriverType === "mongodb";
    const isDynamo = destDriverType === "dynamodb";
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
      warehouse: destDriverType === "snowflake" ? destWarehouse : undefined,
      auth_source: selectedDestConnector?.auth_source || undefined,
      auth_mode: selectedDestConnector?.auth_mode || undefined,
      auth_role: selectedDestConnector?.auth_role || undefined,
      api_key: selectedDestConnector?.api_key || undefined,
      service_account: selectedDestConnector?.service_account || undefined,
    };
  };

  useEffect(() => {
    fetchTransferCapabilities()
      .then((caps) => {
        const sources = (caps.source_databases as string[] | undefined) ?? [];
        const dbs = (caps.destination_databases as string[] | undefined) ?? [];
        const exports = (caps.destination_file_formats as string[] | undefined) ?? [];
        const drivers = (caps.transfer_live_drivers as string[] | undefined) ?? [];
        if (sources.length || drivers.length) {
          setLiveSourceTypes([...new Set([...sources, ...drivers])]);
        }
        if (dbs.length) {
          setLiveDestTypes(dbs.map((id) => ({ id, label: getConnectorDefaults(id).label })));
        }
        if (exports.length) {
          setLiveExportFormats(exports.map((id) => ({ id, label: id.toUpperCase() })));
        }
        if (typeof caps.live_route_combinations === "number") {
          setLiveRouteCount(caps.live_route_combinations);
        }
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    // Never invent a destination type. Only coerce when an already-chosen type
    // disappeared from the live capability list.
    if (!destType || !liveDestTypes.length) return;
    if (!liveDestTypes.some((d) => d.id === destType)) {
      setDestType(liveDestTypes[0].id);
    }
  }, [liveDestTypes, destType]);

  const destConnectors = destType
    ? connectors.filter((c) => getGenericSqlGroup(c.type) === getGenericSqlGroup(destType))
    : [];
  const testedDestConnectors = destConnectors.filter((c) => c.last_test_ok !== false);
  const selectedDestConnector = destConnectors.find((c) => c.id === connectorId);
  // Honesty: only Certified / Source-only types (capabilities). Planned brands stay hidden.
  const isLiveSourceType = (type: string) => {
    if (!liveSourceTypes.length) {
      // Capabilities not loaded yet — allow duplex live drivers from the client mirror.
      return isTransferLiveType(type) || isTransferLiveType(resolveDriverType(type));
    }
    const driver = resolveDriverType(type);
    return (
      liveSourceTypes.includes(type) ||
      liveSourceTypes.includes(driver) ||
      CLOUD_SOURCE_TYPES.has(type)
    );
  };
  const isLiveDestType = (type: string) => {
    if (!liveDestTypes.length) {
      return isTransferLiveType(type) || isTransferLiveType(resolveDriverType(type));
    }
    const driver = resolveDriverType(type);
    return liveDestTypes.some((d) => d.id === type || d.id === driver);
  };
  const dbSourceConnectors = connectors.filter((c) => {
    if (CLOUD_SOURCE_TYPES.has(c.type)) return false;
    const driver = resolveDriverType(c.type);
    if (FILE_FORMAT_SOURCE_TYPES.has(driver)) return false;
    return isLiveSourceType(c.type);
  });
  const cloudSourceConnectors = connectors.filter((c) => CLOUD_SOURCE_TYPES.has(c.type));
  const transferDestConnectors = connectors.filter((c) => isLiveDestType(c.type));
  const sourceConnector =
    sourceKind === "cloud"
      ? cloudSourceConnectors.find((c) => c.id === sourceConnectorId)
      : dbSourceConnectors.find((c) => c.id === sourceConnectorId)
        ?? connectors.find((c) => c.id === sourceConnectorId && !CLOUD_SOURCE_TYPES.has(c.type));
  const isConnectorSource = sourceKind === "database" || sourceKind === "cloud";
  const currentSourceColumns = sourceKind === "file"
    ? parsed?.columns ?? []
    : (transferPlan?.source_columns ?? parsed?.columns ?? []);
  const currentSourceSchema = sourceKind === "file"
    ? parsed?.schema ?? {}
    : (transferPlan?.source_schema ?? parsed?.schema ?? {});
  const samplePreviewRows = parsed?.sample_data ?? parsed?.data ?? [];
  const currentSourceColumnsKey = currentSourceColumns.join("|");

  useEffect(() => {
    if (step !== STEP_VALIDATE) return;
    const headers = currentSourceColumns;
    const rows = samplePreviewRows;
    if (!headers.length || !rows.length || !columnMappings.length) {
      setCellPreview(null);
      return;
    }
    const sample_rows = rows.slice(0, 25).map((row) =>
      headers.map((h) => (row[h] == null ? "" : String(row[h]))),
    );
    let cancelled = false;
    previewQuarantineCells({
      headers,
      sample_rows,
      mappings: columnMappings.map((m) => ({
        source: m.source,
        target: m.target,
        transform: m.transform || undefined,
        target_type: m.destType || undefined,
      })),
      column_types: (currentSourceSchema || {}) as Record<string, string>,
      sample_size: 25,
    })
      .then((res) => {
        if (!cancelled) setCellPreview(res);
      })
      .catch(() => {
        if (!cancelled) setCellPreview(null);
      });
    return () => {
      cancelled = true;
    };
  }, [step, currentSourceColumnsKey, columnMappings, samplePreviewRows, currentSourceSchema, currentSourceColumns]);

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
  const requiresPrimaryKey =
    syncMode === "incremental_deduped"
    || syncMode === "cdc"
    || syncMode === "scd2"
    || syncMode === "mirror";
  const sourceStreamName = sourceKind === "file"
    ? file?.name.replace(/\.[^/.]+$/, "") || "uploaded_file"
    : sourceKind === "cloud"
      ? cloudPath.split("/").filter(Boolean).pop() || "cloud_object"
      : sourceCollection || sourceTable || "source_stream";
  // Comma-separated tables → multi-stream contracts (each gets its own watermark).
  const sourceStreamInputRaw = sourceKind === "database"
    ? (sourceConnector?.type === "mongodb" ? sourceCollection : sourceTable)
    : "";
  const multiStreamNames = parseStreamNames(sourceStreamInputRaw);
  /** First named stream — API table/collection field (never the raw CSV string). */
  const primarySourceStream = primaryStreamName(sourceStreamInputRaw);
  const isMultiStreamSource = multiStreamNames.length > 1;
  const advancedStreamNames = isMultiStreamSource ? multiStreamNames : [sourceStreamName];
  const mapStreamsDiverge = useMemo(() => {
    const ok = streamPreviews.filter((s) => s.status === "ok" && (s.columns?.length ?? 0) > 0);
    if (ok.length < 2) return false;
    const sig = (cols: string[]) => [...cols].map((c) => c.toLowerCase()).sort().join("|");
    const first = sig(ok[0].columns || []);
    return ok.some((s) => sig(s.columns || []) !== first);
  }, [streamPreviews]);
  const streamContracts = buildStreamContracts({
    streamNames: advancedStreamNames,
    syncMode,
    schemaPolicy,
    validationMode,
    fieldCount: currentSourceColumns.length,
    requiresCursor,
    requiresPrimaryKey,
    defaultCursor: cursorField,
    defaultPrimaryKey: primaryKeyField,
    streamFields,
    streamMappings: isMultiStreamSource
      ? {
          ...streamMappings,
          [mapActiveStream || primarySourceStream]: columnMappings,
        }
      : undefined,
  });
  const streamNeedsReview = streamContractsNeedReview({
    streamNames: advancedStreamNames,
    sourceColumns: currentSourceColumns,
    requiresCursor,
    requiresPrimaryKey,
    defaultCursor: cursorField,
    defaultPrimaryKey: primaryKeyField,
    streamFields,
  });
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
    if (!sourceConnector) return { kind: "database", format: "", connector_id: sourceConnectorId };
    const isMongo = sourceConnector.type === "mongodb";
    const isDynamo = sourceConnector.type === "dynamodb";
    // Never send "a, b" as one object name — multi-stream uses stream_contracts.
    const tableOrPath = sourceKind === "cloud"
      ? cloudPath.trim()
      : (isDynamo
        ? (primarySourceStream || sourceConnector.database || "")
        : primarySourceStream);
    return {
      kind: "database",
      format: sourceConnector.type,
      connector_id: sourceConnectorId,
      database: isDynamo ? tableOrPath : sourceConnector.database,
      table: isMongo ? undefined : tableOrPath || undefined,
      collection: isMongo ? tableOrPath : undefined,
      auth_source: sourceConnector.auth_source || undefined,
      auth_mode: sourceConnector.auth_mode || undefined,
      auth_role: sourceConnector.auth_role || undefined,
      api_key: sourceConnector.api_key || undefined,
      service_account: sourceConnector.service_account || undefined,
    };
  };

  const buildPlanPayload = useCallback(() => ({
    name: file?.name ?? sourceStreamName,
    source: buildSourceEndpoint(),
    destination: destKindMode === "file_export"
      ? { kind: "file_export", format: exportFormat, database: targetDb, output_path: destOutputPath }
      : buildDestinationEndpoint(),
    source_columns: currentSourceColumns,
    source_schema: currentSourceSchema,
    target_columns: destColumns,
    target_schema: destSchemaMap,
    row_count_estimate: parsed?.row_count ?? sourceRowEstimate ?? 0,
    // Cap samples — large Mongo/document rows were timing out plan persistence (15s).
    sample_rows: (parsed?.data ?? parsed?.sample_data)?.slice(0, 25) ?? [],
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
    primarySourceStream,
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
    destOutputPath,
    destWarehouse,
    targetCollection,
  ]);

  const ensurePersistedPlan = useCallback(async (
    validationOverride?: ValidationMode,
  ): Promise<string | null> => {
    if (!currentSourceColumns.length) return null;
    const payload = buildPlanPayload();
    // setState for validationMode is async — honor explicit override so plan
    // preflight never runs as stale "strict" after Quarantine → balanced.
    if (validationOverride) {
      payload.policies = { ...payload.policies, validation_mode: validationOverride };
    }
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
              destination_db_type: destKindMode === "file_export" ? exportFormat : destType,
              sync_mode: syncMode,
            });
        setColumnMappings(
          editableFromPipelineMappings(
            result.mappings,
            rows,
            targetCols,
            threshold,
            targetSchema,
          ),
        );
        setLlmMappingUsed(Boolean(result.llm?.llm_used));
        setMappingProof((result as { mapping_proof?: import("../components/MappingProofDrawer").MappingProof }).mapping_proof ?? null);
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
        setMappingProof(null);
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
    setDestTableExists(null);
    try {
      // Destination-only probe: stub file source so we do not re-sample Mongo/SQL
      // (that was hanging the Destination step for minutes on large collections).
      const { destination } = await introspectTransferEndpoints({
        source: { kind: "file", format: "csv" },
        destination: buildDestinationEndpoint(),
      });
      setDestColumns(destination.columns ?? []);
      setDestSchemaMap(destination.schema ?? {});
      const exists = destination.table_exists ?? ((destination.columns?.length ?? 0) > 0);
      setDestTableExists(exists);
      if (!exists) {
        toast({
          title: "New table will be created",
          message: `${targetCollection.trim()} was not found on the destination — DataFlow will CREATE TABLE on first write.`,
          tone: "info",
        });
      }
      // Mapping pipeline runs on the Map step — never block Destination on AI remap.
    } catch (e) {
      // Missing / unreachable schema must not trap the wizard: treat as create-new.
      setDestColumns([]);
      setDestSchemaMap({});
      setDestTableExists(false);
      toast({
        title: "Could not read destination schema",
        message: e instanceof Error ? e.message : "Continuing — table will be created on first write if missing.",
        tone: "warning",
      });
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
    setStreamFields((prev) =>
      seedStreamFieldsFromCandidates(
        advancedStreamNames,
        prev,
        cursorCandidate || cursorField,
        primaryKeyCandidate || primaryKeyField,
        currentSourceColumns,
      ),
    );
  }, [
    advancedStreamNames.join("\0"),
    cursorCandidate,
    cursorField,
    currentSourceColumns,
    currentSourceColumnsKey,
    primaryKeyCandidate,
    primaryKeyField,
  ]);

  const resetRouteForDestinationChange = useCallback(() => {
    setTransferPlan(null);
    setPersistedPlanId(null);
    setPreflight(null);
    setCellPreview(null);
    setDestColumns([]);
    setDestSchemaMap({});
    setDestTableExists(null);
    routeAnalyzedKeyRef.current = "";
  }, []);

  const applyConnectorSelection = (id: string) => {
    setConnectorId(id);
    if (!id) return;
    const conn = connectors.find((c) => c.id === id);
    if (!conn) return;
    resetRouteForDestinationChange();
    const matched = liveDestTypes.find((d) => getGenericSqlGroup(d.id) === getGenericSqlGroup(conn.type));
    if (matched) {
      setDestType(matched.id);
    } else {
      setDestType(conn.type);
    }
    if (conn.database) setTargetDb(conn.database);
    if (conn.schema) setDestSchema(conn.schema);
    setDestHost(conn.host || getConnectorDefaults(conn.type).host);
    setDestPort(conn.port || defaultPortForType(conn.type));
    setTargetCollection("");
  };

  useEffect(() => {
    if (connectorId || !destType) return;
    setDestHost(getConnectorDefaults(destType).host);
    setDestPort(defaultPortForType(destType));
    const group = getGenericSqlGroup(destType);
    if (group === "postgresql+psycopg2") setDestSchema("public");
    if (group === "mssql+pyodbc") setDestSchema("dbo");
    autoSelectedConnector.current = false;
  }, [connectorId, destType]);

  // Do not auto-pick a saved connector — that forced MongoDB onto the route bar
  // before the operator chose a destination.

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

  // Carry selected connection from Connectors drawer into Transfer Studio source step.
  // Apply once per seed token — do not re-run on connectors list refresh (that would
  // yank the operator back to Source mid-wizard).
  useEffect(() => {
    if (!seedSourceConnector?.connectorId) {
      appliedSeedTokenRef.current = null;
      return;
    }
    if (appliedSeedTokenRef.current === seedSourceConnector.token) return;
    const seeded = connectors.find((c) => c.id === seedSourceConnector.connectorId);
    if (!seeded) return; // wait until connectors are loaded

    if (CLOUD_SOURCE_TYPES.has(seeded.type)) {
      setSourceKind("cloud");
    } else if (!FILE_FORMAT_SOURCE_TYPES.has(seeded.type)) {
      setSourceKind("database");
    } else {
      // File-format connector profiles are not valid Studio sources.
      appliedSeedTokenRef.current = seedSourceConnector.token;
      return;
    }

    appliedSeedTokenRef.current = seedSourceConnector.token;
    autoSelectedSourceConnector.current = true;
    setSourceConnectorId(seeded.id);
    setStep(STEP_SOURCE);
  }, [seedSourceConnector, connectors]);

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
    if (destKindMode === "database" && !destType) {
      toast({
        title: "Choose a destination",
        message: "Select a saved connector or engine before analyzing the route.",
        tone: "warning",
      });
      return;
    }
    setPlanLoading(true);
    try {
      const destination = destKindMode === "file_export"
        ? { kind: "file_export", format: exportFormat, database: targetDb, output_path: destOutputPath }
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
          destCollection: destDriverType === "mongodb" || destDriverType === "dynamodb" ? targetCollection : undefined,
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

  // Auto-analyze only after a real destination is chosen (never on bare step entry
  // with a default MongoDB type). Re-run when the destination identity changes.
  useEffect(() => {
    if (step !== STEP_DESTINATION) return;
    if (!currentSourceColumns.length || planLoading) return;
    if (destKindMode === "database" && !destType) return;
    // Wait for table/collection so we don't analyze a half-filled Mongo default.
    if (destKindMode === "database" && !targetCollection.trim()) return;
    const routeKey = [
      destKindMode,
      destType,
      connectorId,
      targetDb,
      targetCollection,
      exportFormat,
    ].join("|");
    if (routeAnalyzedKeyRef.current === routeKey) return;
    routeAnalyzedKeyRef.current = routeKey;
    void loadTransferPlan();
  }, [
    step,
    currentSourceColumnsKey,
    planLoading,
    destKindMode,
    destType,
    connectorId,
    targetDb,
    targetCollection,
    exportFormat,
  ]);

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
          destination_db_type: destKindMode === "file_export" ? exportFormat : destType,
          sync_mode: syncMode,
        });
        const pipelineAnalysis = analysisFromPipeline(data.columns, data.schema ?? {}, pipeline.mappings);
        setAnalysis(pipelineAnalysis);
        setColumnMappings(editableFromPipelineMappings(
          pipeline.mappings,
          rows,
          destColumns.length ? destColumns : undefined,
          confidenceThresholdForMode(validationMode),
          destSchemaMap,
        ));
        setLlmMappingUsed(Boolean(pipeline.llm?.llm_used));
        setMappingProof((pipeline as { mapping_proof?: import("../components/MappingProofDrawer").MappingProof }).mapping_proof ?? null);
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
    setMappingProof(null);
    setUploading(true);
    try {
      let data: ParsedUpload;
      try {
        data = await uploadFile(selected);
      } catch (uploadErr) {
        const ext = fileExtension(selected.name);
        if (ext === "csv" || ext === "tsv") {
          const text = await selected.text();
          data = parseCsvTextForPreview(text);
          toast({
            title: "Profiled locally",
            message: "API unavailable — preview uses browser parsing. Start the API for full preflight and write.",
            tone: "warning",
          });
        } else {
          throw uploadErr;
        }
      }
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

  const loadSampleDataset = async () => {
    if (uploading) return;
    try {
      const res = await fetch("/fixtures/sample-orders.csv");
      if (!res.ok) throw new Error("Sample file not found");
      const blob = await res.blob();
      const sample = new File([blob], "sample-orders.csv", { type: "text/csv" });
      await processFile(sample);
    } catch (e) {
      toast({
        title: "Could not load sample",
        message: e instanceof Error ? e.message : "Try uploading your own CSV instead.",
        tone: "error",
      });
    }
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

  const applyPrimaryStreamSchema = useCallback((
    streamName: string,
    intro: {
      columns: string[];
      schema?: Record<string, string>;
      schema_intelligence?: Record<string, { semantic_role?: string; logical_type?: string; notes?: string[] }>;
      row_estimate?: number;
      data?: Record<string, unknown>[];
      sample_data?: Record<string, unknown>[];
      message?: string;
    },
  ) => {
    if (!sourceConnector) return;
    if (intro.row_estimate != null && intro.row_estimate > 0) {
      setSourceRowEstimate(intro.row_estimate);
    }
    const sampleRows = intro.data ?? intro.sample_data ?? [];
    const columnSamples = Object.fromEntries(
      intro.columns.map((col) => [
        col,
        sampleRows.slice(0, 8).map((row) => String(row[col] ?? "")).filter((v) => v.length > 0),
      ]),
    );
    setTransferPlan((prev) => ({
      supported: prev?.supported ?? true,
      message: intro.message ?? prev?.message ?? "",
      operation: prev?.operation ?? "insert",
      auto_create: prev?.auto_create ?? [],
      type_mappings: prev?.type_mappings ?? [],
      source_columns: intro.columns,
      source_schema: intro.schema ?? {},
    }));
    setActiveData({
      name: streamName || sourceConnector.name,
      columns: intro.columns,
      row_count: intro.row_estimate ?? 0,
      samples: columnSamples,
      schema: intro.schema ?? {},
    });
    setParsed({
      columns: intro.columns,
      schema: intro.schema ?? {},
      row_count: intro.row_estimate ?? 0,
      data: intro.data ?? intro.sample_data ?? [],
      file_type: sourceConnector.type,
    });
    const fallbackAnalysis = analysisFromPipeline(
      intro.columns,
      intro.schema ?? {},
      intro.columns.map((col) => ({
        source: col,
        target: col,
        confidence: 0.75,
        reasoning: "Inferred from live connector schema",
      })),
    );
    setAnalysis(fallbackAnalysis);
    const intel = intro.schema_intelligence || {};
    const seeded = editableFromPipelineMappings(
      intro.columns.map((col) => {
        const role = intel[col]?.semantic_role;
        const logical = intel[col]?.logical_type || intro.schema?.[col] || "VARCHAR";
        return {
          source: col,
          target: col,
          confidence: role === "string_enum" ? 0.7 : 0.9,
          reasoning: role === "string_enum"
            ? "String enum (status/lifecycle) — VARCHAR, not BOOLEAN"
            : "Inferred from live connector schema",
          requires_review: role === "string_enum",
          source_type: logical,
          target_type: logical,
          semantic_role: role,
        };
      }),
      sampleRows,
    );
    setColumnMappings(seeded);
    void analyzeSchemaEnhanced(columnSamples, { timeoutMs: 25_000 })
      .then((dbAnalysis) => setAnalysis(dbAnalysis))
      .catch((aiErr) => {
        console.warn("AI schema enrichment skipped after successful introspect:", aiErr);
      });
  }, [sourceConnector, setActiveData]);

  const introspectOneStream = useCallback(async (streamName: string) => {
    if (!sourceConnector) {
      return { ok: false as const, error: "No source connector selected" };
    }
    const isMongo = sourceConnector.type === "mongodb";
    const sourceEndpoint: Record<string, unknown> = {
      kind: "database",
      format: sourceConnector.type,
      connector_id: sourceConnectorId,
      database: sourceConnector.database,
    };
    if (isMongo) sourceEndpoint.collection = streamName;
    else sourceEndpoint.table = streamName;

    const { source: intro } = await introspectTransferEndpoints({
      source: sourceEndpoint,
      destination: { kind: "file_export", format: "json" },
    });
    if (!intro.connected || !intro.columns?.length) {
      return {
        ok: false as const,
        error: intro.message || `“${streamName}” was not found or could not be read on this connector.`,
      };
    }
    const sampleRows = intro.data ?? intro.sample_data ?? [];
    if (!sampleRows.length) {
      // Schema-only introspect is incomplete for Validate — surface loudly.
      const detail = (intro as { sample_error?: string }).sample_error
        || intro.message
        || "Columns loaded but no sample rows. Check warehouse/role and reload.";
      toast({
        title: "Preview has columns but no sample rows",
        message: detail,
        tone: "warning",
      });
    }
    return { ok: true as const, intro };
  }, [sourceConnector, sourceConnectorId, toast]);

  const introspectConnectorSource = useCallback(async () => {
    if (!sourceConnector) return null;
    const isMongo = sourceConnector.type === "mongodb";

    if (sourceKind === "cloud") {
      const tableOrPath = cloudPath.trim();
      if (!tableOrPath) return null;
      setStreamPreviews([]);
      const result = await introspectOneStream(tableOrPath);
      if (!result.ok) {
        setSourceIntrospectError(result.error);
        toast({ title: "Could not read source schema", message: result.error, tone: "error" });
        return null;
      }
      applyPrimaryStreamSchema(tableOrPath, result.intro);
      setSourceIntrospectError(null);
      return result.intro;
    }

    const raw = isMongo ? (sourceCollection || sourceTable) : sourceTable;
    const names = parseStreamNames(raw);
    if (!names.length) return null;

    // Show tabs immediately while each stream loads independently.
    setStreamPreviews(names.map((name) => ({
      name,
      status: "loading",
      columns: [],
      schema: {},
      rows: [],
    })));
    setActiveStreamTab(names[0]);
    setSourceIntrospectError(null);

    const settled = await Promise.all(
      names.map(async (name) => {
        try {
          const result = await introspectOneStream(name);
          if (!result.ok) {
            return {
              name,
              status: "error" as const,
              columns: [] as string[],
              schema: {} as Record<string, string>,
              rows: [] as Record<string, unknown>[],
              error: result.error,
            };
          }
          return {
            name,
            status: "ok" as const,
            columns: result.intro.columns,
            schema: result.intro.schema ?? {},
            rows: (result.intro.data ?? result.intro.sample_data ?? []) as Record<string, unknown>[],
            rowEstimate: result.intro.row_estimate,
          };
        } catch (e) {
          return {
            name,
            status: "error" as const,
            columns: [] as string[],
            schema: {} as Record<string, string>,
            rows: [] as Record<string, unknown>[],
            error: e instanceof Error ? e.message : `Failed to read “${name}”.`,
          };
        }
      }),
    );

    setStreamPreviews(settled);

    const primaryOk = settled.find((s) => s.name === names[0] && s.status === "ok")
      || settled.find((s) => s.status === "ok");
    const failed = settled.filter((s) => s.status === "error");

    if (!primaryOk) {
      const detail = failed.map((f) => `${f.name}: ${f.error}`).join(" · ");
      const message = names.length > 1
        ? `None of the ${names.length} streams could be read. ${detail}`
        : (failed[0]?.error || "Could not read source schema.");
      setSourceIntrospectError(message);
      toast({ title: "Could not read source schema", message, tone: "error" });
      return null;
    }

    // Drive mapping / continue from the first successful stream (prefer listed order).
    applyPrimaryStreamSchema(primaryOk.name, {
      columns: primaryOk.columns,
      schema: primaryOk.schema,
      row_estimate: primaryOk.rowEstimate,
      data: primaryOk.rows,
      message: failed.length
        ? `${failed.length} of ${names.length} streams failed — using “${primaryOk.name}” for mapping preview.`
        : undefined,
    });

    if (failed.length) {
      const warn = `${failed.length} of ${names.length} streams failed (${failed.map((f) => f.name).join(", ")}). Preview tabs show details; remove or fix those names before run.`;
      setSourceIntrospectError(warn);
      toast({ title: "Partial stream schema", message: warn, tone: "warning" });
    } else {
      setSourceIntrospectError(null);
    }

    setActiveStreamTab(primaryOk.name);
    return {
      connected: true,
      columns: primaryOk.columns,
      schema: primaryOk.schema,
      row_estimate: primaryOk.rowEstimate,
      data: primaryOk.rows,
      message: failed.length ? `${failed.length} stream(s) failed` : "ok",
    };
  }, [
    sourceConnector,
    sourceKind,
    sourceCollection,
    sourceTable,
    cloudPath,
    introspectOneStream,
    applyPrimaryStreamSchema,
    toast,
  ]);

  const introspectConnectorSourceRef = useRef(introspectConnectorSource);
  introspectConnectorSourceRef.current = introspectConnectorSource;

  // Auto-introspect when the user enters a table/collection. Same key is never
  // auto-retried after success or error — change the name or click Retry.
  useEffect(() => {
    if (sourceKind !== "database" && sourceKind !== "cloud") return;
    if (!sourceConnectorId || !sourceConnector) return;
    const rawPath = sourceKind === "cloud"
      ? cloudPath.trim()
      : (sourceConnector.type === "mongodb" ? (sourceCollection || sourceTable) : sourceTable);
    const names = sourceKind === "cloud" ? (rawPath ? [rawPath] : []) : parseStreamNames(rawPath);
    if (!names.length) {
      setStreamPreviews([]);
      return;
    }

    // Gate on the full stream list so adding/removing a name re-reads schemas.
    const key = `${sourceKind}|${sourceConnectorId}|${names.join("|")}`;
    const gate = sourceIntrospectGateRef.current;
    if (gate.key === key && (gate.status === "ok" || gate.status === "error" || gate.status === "running")) {
      return;
    }

    // Wait for typing to settle — 400ms fired on half-names (e.g. "csv" of "csvtestfile").
    const gen = ++sourceIntrospectGenRef.current;
    sourceIntrospectGateRef.current = { key, status: "running" };
    let started = false;
    const t = window.setTimeout(() => {
      started = true;
      setSourceIntrospecting(true);
      setSourceIntrospectError(null);
      setAnalyzing(true);
      void introspectConnectorSourceRef.current()
        .then((res) => {
          if (gen !== sourceIntrospectGenRef.current) return;
          if (res) {
            // Keep any partial-stream warning set inside introspectConnectorSource.
            sourceIntrospectGateRef.current = { key, status: "ok" };
          } else {
            sourceIntrospectGateRef.current = { key, status: "error" };
            setSourceIntrospectError((prev) => prev || (
              "Could not read the source schema. Verify each table/collection name and connector credentials."
            ));
          }
        })
        .catch((e) => {
          if (gen !== sourceIntrospectGenRef.current) return;
          sourceIntrospectGateRef.current = { key, status: "error" };
          setSourceIntrospectError(e instanceof Error ? e.message : "Source introspection failed.");
        })
        .finally(() => {
          if (gen !== sourceIntrospectGenRef.current) return;
          setSourceIntrospecting(false);
          setAnalyzing(false);
        });
    }, 1200);
    return () => {
      window.clearTimeout(t);
      // Only release the gate if the timer never fired — never interrupt an
      // in-flight attempt or we will restart analysis on every parent re-render.
      if (!started && sourceIntrospectGateRef.current.key === key && sourceIntrospectGateRef.current.status === "running") {
        sourceIntrospectGateRef.current = { key: "", status: "idle" };
      }
    };
  }, [
    sourceKind,
    sourceConnectorId,
    sourceConnector?.type,
    sourceCollection,
    sourceTable,
    cloudPath,
  ]);

  const retrySourceIntrospect = useCallback(() => {
    sourceIntrospectGateRef.current = { key: "", status: "idle" };
    sourceIntrospectGenRef.current += 1;
    setSourceIntrospectError(null);
    setSourceIntrospecting(false);
    setAnalyzing(false);
    // Nudge effect by clearing then relying on current collection key — force via
    // a microtask re-entry of the same inputs.
    const rawPath = sourceKind === "cloud"
      ? cloudPath.trim()
      : (sourceConnector?.type === "mongodb" ? (sourceCollection || sourceTable) : sourceTable);
    const names = sourceKind === "cloud" ? (rawPath ? [rawPath] : []) : parseStreamNames(rawPath);
    if (!sourceConnectorId || !names.length) return;
    const key = `${sourceKind}|${sourceConnectorId}|${names.join("|")}`;
    const gen = ++sourceIntrospectGenRef.current;
    sourceIntrospectGateRef.current = { key, status: "running" };
    setSourceIntrospecting(true);
    setAnalyzing(true);
    void introspectConnectorSource()
      .then((res) => {
        if (gen !== sourceIntrospectGenRef.current) return;
        if (res) {
          sourceIntrospectGateRef.current = { key, status: "ok" };
          setSourceIntrospectError(null);
        } else {
          sourceIntrospectGateRef.current = { key, status: "error" };
          setSourceIntrospectError(
            "Could not read the source schema. Verify the table or collection name and connector credentials.",
          );
        }
      })
      .catch((e) => {
        if (gen !== sourceIntrospectGenRef.current) return;
        sourceIntrospectGateRef.current = { key, status: "error" };
        setSourceIntrospectError(e instanceof Error ? e.message : "Source introspection failed.");
      })
      .finally(() => {
        if (gen !== sourceIntrospectGenRef.current) return;
        setSourceIntrospecting(false);
        setAnalyzing(false);
      });
  }, [
    sourceKind,
    sourceConnectorId,
    sourceConnector?.type,
    sourceCollection,
    sourceTable,
    cloudPath,
    introspectConnectorSource,
  ]);

  const proceedToDestination = async () => {
    if (explainSourceGap()) return;
    if (isConnectorSource && !analysis?.columns.length && !currentSourceColumns.length) {
      setSourceIntrospecting(true);
      setAnalyzing(true);
      try {
        const intro = await introspectConnectorSource();
        if (!intro?.columns?.length) return;
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
    const bump = (pct: number, phase: string) => {
      setMappingProgress(pct);
      setMappingPhase(phase);
    };
    try {
      bump(8, "Preparing schema context…");
      if (destKindMode === "database") {
        bump(22, "Loading destination schema…");
        await loadDestinationSchema();
      }
      bump(42, "Building transfer plan…");
      await loadTransferPlan();
      if (sourceKind === "file" && parsed) {
        if (!analysis?.columns.length || !columnMappings.length) {
          bump(58, "Profiling source columns…");
          await runSourceColumnAnalysis(parsed);
        }
        bump(72, "Matching source to destination fields…");
        await applyPipelineMappings(
          destColumns.length ? destColumns : undefined,
          destSchemaMap,
        );
      } else if (analysis?.columns.length || currentSourceColumns.length) {
        bump(65, "Matching source to destination fields…");
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
        return;
      }
      bump(100, "Mapping ready");
    } catch (e) {
      const message = e instanceof Error ? e.message : "Could not prepare column mappings.";
      toast({ title: "Mapping setup failed", message, tone: "error" });
      console.error(e);
    } finally {
      setAnalyzing(false);
      setMappingProgress(0);
      setMappingPhase("Preparing schema context…");
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
      prev.map((m) => {
        if (isEnumToBooleanConflict(m)) {
          return { ...widenMappingToVarchar(m), approved: true, requiresReview: false };
        }
        return { ...m, approved: true };
      }),
    );
  };

  const approveAllAndPreflight = async () => {
    const approved = columnMappings.map((m) => {
      if (isEnumToBooleanConflict(m)) {
        return { ...widenMappingToVarchar(m), approved: true, requiresReview: false };
      }
      return { ...m, approved: true };
    });
    setColumnMappings(approved);
    setStep(STEP_VALIDATE);
    await executePreflight(approved);
  };

  /** Reverse of `buildPreflightMappings`' engine-transform map, back to the Studio's UI transforms. */
  const ENGINE_TO_UI_TRANSFORM: Record<string, MappingTransform> = {
    trim: "trim",
    upper: "upper",
    lower: "lower",
    datetime: "date_iso",
    date_iso: "date_iso",
    hash_pii: "hash_pii",
    decimal: "cast_number",
    cast_number: "cast_number",
    boolean: "cast_boolean",
    cast_boolean: "cast_boolean",
    json: "parse_json",
    parse_json: "parse_json",
    strip_controls: "strip_controls",
    normalize_unicode: "strip_controls",
  };

  const stripControlCharsAndRerun = async (modeOverride?: ValidationMode) => {
    const typed = new Set<MappingTransform>([
      "cast_number",
      "cast_boolean",
      "date_iso",
      "parse_json",
      "hash_pii",
    ]);
    let applied = 0;
    const next = columnMappings.map((m) => {
      if (m.transform && typed.has(m.transform)) {
        return { ...m, approved: true };
      }
      applied += 1;
      return { ...m, transform: "strip_controls" as MappingTransform, approved: true };
    });
    setColumnMappings(next);
    toast({
      title: "Strip controls applied",
      message: `Applied strip_controls to ${applied} text mapping${applied === 1 ? "" : "s"}. Re-running validation…`,
      tone: "success",
    });
    await executePreflight(next, modeOverride ?? validationMode);
  };

  const quarantineAndRerun = async () => {
    // Wrong column maps (status → boolean date flag) cannot be fixed by
    // quarantine/strip — send the operator back to Map with a clear reason.
    const dry = preflight?.gates?.find((g) => /dry_run|integrity/i.test(g.id));
    const dryMsg = `${dry?.message || ""} ${JSON.stringify(dry?.details || {})}`;
    const encodingOnly = /format-control|replacement character|encoding|strip_controls/i.test(dryMsg)
      && !/\([A-Z_]+\)\s*→\s*\w+\s*\([A-Z_]+\)/i.test(dryMsg)
      && !/confidence\s+\d+%\s*</i.test(dryMsg);
    const looksLikeBadMapping =
      /\([A-Z_]+\)\s*→\s*\w+\s*\([A-Z_]+\)/i.test(dryMsg)
      || /confidence\s+\d+%\s*</i.test(dryMsg)
      || /remap|posted_date_estimated|Invalid (date|boolean|decimal)/i.test(dryMsg);

    if (looksLikeBadMapping && !encodingOnly) {
      toast({
        title: "Remap columns — quarantine cannot fix this",
        message:
          "Preflight blocked the transfer (0 rows written). Findings are inspect-only until Map types/targets are fixed "
          + "(for example status enums → VARCHAR, not BOOLEAN). Quarantine-and-continue only helps encoding/control-character rows after Validate passes.",
        tone: "warning",
      });
      setStep(STEP_MAP);
      return;
    }

    setValidationMode("balanced");
    toast({
      title: "Quarantine + strip controls",
      message: "Applying strip_controls and balanced validation so format-control characters are sanitized before run…",
      tone: "info",
    });
    await stripControlCharsAndRerun("balanced");
  };

  /** Map an AI `suggested_action` onto the real Studio controls. */
  const applySuggestedAction = (action: ValidationSuggestedAction) => {
    const matches = (m: EditableMapping) =>
      (action.target && m.target === action.target) || (action.column && m.source === action.column);

    switch (action.kind) {
      case "change_target_type": {
        if (!action.to_type) return;
        let hit = false;
        let existingConflict = false;
        const next = columnMappings.map((m) => {
          if (matches(m)) {
            hit = true;
            if (m.existsInDestination) {
              existingConflict = true;
              // Keep live dest type — mapping Widen is not ALTER TABLE.
              return {
                ...m,
                approved: false,
                requiresReview: true,
                reason: [
                  m.reason,
                  `Destination already typed — remap or ALTER before targeting ${action.to_type}`,
                ]
                  .filter(Boolean)
                  .join(" · "),
              };
            }
            return { ...m, destType: action.to_type, approved: true };
          }
          return m;
        });
        setColumnMappings(next);
        if (existingConflict) {
          toast({
            title: "Remap or ALTER required",
            message:
              "The destination column already exists with a typed DDL. Changing the mapping type alone will not widen BOOLEAN → VARCHAR on the warehouse.",
            tone: "warning",
          });
          setStep(STEP_MAP);
          return;
        }
        toast({
          title: hit ? "Target type updated — re-validating" : "Column not found",
          message: hit
            ? `Changed ${action.column ?? action.target} → type ${action.to_type}. Re-running Validate now so you can see if the block cleared.`
            : `Couldn't find '${action.column ?? action.target}' in the current mappings.`,
          tone: hit ? "success" : "warning",
        });
        if (hit) void executePreflight(next);
        break;
      }
      case "normalize_control_chars": {
        void stripControlCharsAndRerun();
        break;
      }
      case "open_bad_data_fix":
        // ValidateDashboard opens the drawer; keep as no-op fallback.
        break;
      case "add_transform": {
        const uiTransform = action.transform
          ? ENGINE_TO_UI_TRANSFORM[action.transform] || (action.transform as MappingTransform)
          : undefined;
        if (!uiTransform) {
          toast({
            title: "Transform unavailable",
            message: `No Studio transform matches '${action.transform ?? "?"}'. Adjust it in the Map step.`,
            tone: "warning",
          });
          setStep(STEP_MAP);
          return;
        }
        let hit = false;
        const next = columnMappings.map((m) => {
          if (matches(m)) {
            hit = true;
            return { ...m, transform: uiTransform, approved: true };
          }
          return m;
        });
        setColumnMappings(next);
        toast({
          title: hit ? "Transform applied — re-validating" : "Column not found",
          message: hit
            ? `Applied ${uiTransform} to '${action.column ?? action.target}'. Re-running Validate so you can confirm the fix.`
            : `Couldn't find '${action.column ?? action.target}' in the current mappings.`,
          tone: hit ? "success" : "warning",
        });
        if (hit) void executePreflight(next);
        break;
      }
      case "review_mappings":
      case "rerun_mapping":
        setStep(STEP_MAP);
        toast({
          title: "Opened Map step",
          message:
            action.kind === "rerun_mapping"
              ? "Re-run mapping to accept the new schema, then re-run preflight."
              : "Review and approve the flagged mappings, then re-run preflight.",
          tone: "info",
        });
        break;
      case "check_connection":
        setStep(STEP_DESTINATION);
        toast({
          title: "Opened connection settings",
          message: "Check the source/destination connection, then re-run preflight.",
          tone: "info",
        });
        break;
      default:
        break;
    }
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
        if (!sourceConnector) {
          toast({
            title: "Source required",
            message: sourceKind === "cloud"
              ? "Select a cloud connector and object path."
              : "Select a source connector and table.",
            tone: "warning",
          });
          setStep(STEP_SOURCE);
          return;
        }

        // Prefer schema already loaded on Source/Map — re-analyze often returns
        // empty columns while message is the useless success token "supported".
        const cachedColumns =
          currentSourceColumns.length
            ? currentSourceColumns
            : (transferPlan?.source_columns?.length
              ? transferPlan.source_columns
              : (parsed?.columns?.length ? parsed.columns : []));
        const cachedSchema =
          Object.keys(currentSourceSchema).length
            ? currentSourceSchema
            : (transferPlan?.source_schema || parsed?.schema || {});

        if (cachedColumns.length > 0) {
          columns = cachedColumns;
          columnTypes = cachedSchema;
          rowCount = parsed?.row_count ?? sourceRowEstimate ?? 0;
          sampleRows = (parsed?.data ?? parsed?.sample_data)?.slice(0, 100);
          mappings = buildPreflightMappings(
            analysis?.columns ?? cachedColumns.map((c) => ({
              column_name: c,
              inferred_type: cachedSchema[c] || "string",
              semantic_type: "unknown",
              confidence: 1,
              is_pii: false,
              compliance: [],
            })),
            activeMappings.length ? activeMappings : columnMappings,
          );
        } else {
          const routePlan = await analyzeDbTransfer({
            sourceConnectorId: sourceConnectorId,
            sourceFormat: sourceConnector.type,
            sourceDatabase: sourceConnector.database,
            sourceTable: sourceKind === "cloud"
              ? cloudPath || undefined
              : sourceConnector.type !== "mongodb" ? primarySourceStream || undefined : undefined,
            sourceCollection: sourceKind === "cloud"
              ? cloudPath || undefined
              : sourceConnector.type === "mongodb" ? primarySourceStream || undefined : undefined,
            destFormat: destType,
            destDatabase: targetDb,
            destTable: destType !== "mongodb" ? targetCollection : undefined,
            destCollection: destDriverType === "mongodb" ? targetCollection : undefined,
            destConnectorId: connectorId || undefined,
          });
          const nestedSource = (routePlan as { source?: { columns?: string[]; schema?: Record<string, string> } }).source;
          columns = routePlan.source_columns?.length
            ? routePlan.source_columns
            : (nestedSource?.columns ?? []);
          columnTypes = routePlan.source_schema && Object.keys(routePlan.source_schema).length
            ? routePlan.source_schema
            : (nestedSource?.schema ?? {});
          if (!columns.length) {
            toast({
              title: "Schema introspection failed",
              message: schemaIntrospectionFailureMessage(routePlan.message, primarySourceStream),
              tone: "error",
            });
            setStep(STEP_SOURCE);
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
      }

      const planId = await ensurePersistedPlan(activeValidation);
      if (planId) {
        try {
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
        } catch (planErr) {
          if (!(sourceKind === "file" && destKindMode === "file_export" && parsed)) {
            throw planErr;
          }
        }
      }

      let pf: PreflightResult;
      try {
        pf = await runPreflight({
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
          dest_schema: destKindMode === "database" && !connectorId && (destDriverType === "snowflake" || getGenericSqlGroup(destType) === "postgresql+psycopg2" || getGenericSqlGroup(destType) === "mssql+pyodbc") ? destSchema || (getGenericSqlGroup(destType) === "postgresql+psycopg2" ? "public" : "dbo") : undefined,
          dest_warehouse: destKindMode === "database" && !connectorId && destDriverType === "snowflake" ? destWarehouse || undefined : undefined,
          // Live dest schema — required so existing BOOLEAN columns are not invisible to DDL gates.
          dest_table: destKindMode === "database" && destDriverType !== "mongodb" && destDriverType !== "dynamodb"
            ? (targetCollection || undefined)
            : undefined,
          dest_collection: destKindMode === "database" && (destDriverType === "mongodb" || destDriverType === "dynamodb")
            ? (targetCollection || undefined)
            : undefined,
          destination_column_types:
            destKindMode === "database" && Object.keys(destSchemaMap).length
              ? destSchemaMap
              : undefined,
          sample_rows: sampleRows,
          estimated_bytes: estimatedBytes,
          sync_mode: syncMode,
          schema_policy: schemaPolicy,
          validation_mode: validationOverride ?? validationMode,
          backfill_new_fields: backfillNewFields,
          stream_contracts: streamContracts,
        });
      } catch (apiErr) {
        if (sourceKind === "file" && destKindMode === "file_export" && parsed) {
          pf = runLocalPreflight({
            columns,
            rowCount,
            mappings: activeMappings.length ? activeMappings : columnMappings,
            sampleRows,
            confidenceThreshold: threshold,
            destKind: destKindMode,
          });
          toast({
            title: "Validated locally",
            message: "API unavailable — browser preflight passed for file export demo.",
            tone: "warning",
          });
        } else {
          throw apiErr;
        }
      }
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
      if (sourceKind === "file" && destKindMode === "file_export" && parsed) {
        const threshold = confidenceThresholdForMode(validationOverride ?? validationMode);
        const pf = runLocalPreflight({
          columns: parsed.columns,
          rowCount: parsed.row_count,
          mappings: overrideMappings ?? columnMappings,
          sampleRows: (parsed.data ?? parsed.sample_data)?.slice(0, 100),
          confidenceThreshold: threshold,
          destKind: destKindMode,
        });
        setPreflight(pf);
        if (pf.passed) {
          setStep(STEP_RUN);
          toast({
            title: "Validated locally",
            message: "API unavailable — browser preflight passed for file export demo.",
            tone: "warning",
          });
        } else {
          toast({
            title: "Validation incomplete",
            message: pf.blockers[0]?.message ?? "Local validation failed.",
            tone: "warning",
          });
        }
      } else {
        const message = e instanceof Error ? e.message : "Validation could not complete.";
        toast({ title: "Preflight failed", message, tone: "error" });
        console.error(e);
      }
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
    setStep(STEP_RUN);
    setActiveJobId(null);
    setResult(null);
    setTransferLaunch(null);
    setRunStartupProgress(12);
    setRunStartupPhase(RUN_LAUNCH_STAGES[0]);
    const transferMappings = columnMappings.length
      ? buildPreflightMappings(analysis?.columns ?? [], columnMappings)
      : analysis
        ? buildPreflightMappings(analysis.columns)
        : undefined;
    try {
      setRunStartupProgress(24);
      setRunStartupPhase(RUN_LAUNCH_STAGES[1]);
      const data = await runUniversalTransfer({
        file: sourceKind === "file" ? file ?? undefined : undefined,
        sourceKind: sourceKind === "cloud" ? "database" : sourceKind,
        sourceFormat: sourceConnector?.type,
        sourceConnectorId: isConnectorSource ? sourceConnectorId || undefined : undefined,
        sourceDatabase: sourceConnector?.database,
        sourceTable: sourceKind === "cloud"
          ? cloudPath || undefined
          : sourceConnector?.type !== "mongodb" ? primarySourceStream || undefined : undefined,
        sourceCollection: sourceKind === "cloud"
          ? cloudPath || undefined
          : sourceConnector?.type === "mongodb" ? primarySourceStream || undefined : undefined,
        sourceAuthSource: sourceConnector?.auth_source,
        destKind: destKindMode,
        destFormat: destKindMode === "file_export" ? exportFormat : destType,
        destDatabase: targetDb,
        destSchema: destDriverType === "snowflake" ? "PUBLIC" : destDriverType === "bigquery" ? destSchema : destSchema,
        destTable: destType !== "mongodb" ? targetCollection : undefined,
        destCollection: destDriverType === "mongodb" ? targetCollection : targetCollection,
        destConnectorId: connectorId || undefined,
        destHost: !connectorId ? destHost : undefined,
        destPort: !connectorId ? destPort : undefined,
        destUsername: !connectorId ? destUsername || undefined : undefined,
        destPassword: !connectorId ? destPassword || undefined : undefined,
        destConnectionString: !connectorId ? destConnectionString || undefined : undefined,
        destOutputPath: destKindMode === "file_export" ? destOutputPath || undefined : undefined,
        destWarehouse: destDriverType === "snowflake" ? destWarehouse : undefined,
        destAuthSource: selectedDestConnector?.auth_source,
        skipPreflight: !enforcePreflight,
        mappings: transferMappings,
        syncMode,
        schemaPolicy,
        validationMode,
        backfillNewFields,
        streamContracts,
        planId: persistedPlanId ?? undefined,
        priorityColumn: priorityColumn || undefined,
        priorityDirection,
        limit: rowLimit > 0 ? rowLimit : undefined,
      });
      setRunStartupProgress(36);
      setRunStartupPhase(RUN_LAUNCH_STAGES[3]);
      if (data.job_id && (data as { async?: boolean }).async) {
        setRunStartupProgress(40);
        setActiveJobId(data.job_id);
        setTransferring(false);
        toast({
          title: "Transfer started",
          message: "Live theater is now tracking throughput, phases, and reconciliation in real time.",
          tone: "success",
        });
        return;
      }
      setResult(data);
      setRunStartupProgress(100);
      setStep(STEP_RUN);
      if (data.success) onTransferComplete();
    } catch (transferErr) {
      if (
        sourceKind === "file"
        && destKindMode === "file_export"
        && parsed
        && columnMappings.length > 0
      ) {
        const rows = parsed.data ?? parsed.sample_data ?? [];
        const localResult = runLocalFileExport({
          sourceFilename: file?.name ?? "export",
          rows,
          mappings: columnMappings,
          format: exportFormat,
          outputBasename: targetCollection || undefined,
        });
        setResult(localResult);
        setRunStartupProgress(100);
        setStep(STEP_RUN);
        onTransferComplete();
        toast({
          title: "Exported locally",
          message: `${localResult.records_transferred?.toLocaleString() ?? 0} rows saved — start the API for governed Job Theater proof.`,
          tone: "success",
        });
      } else {
        setResult({ success: false, error: transferErr instanceof Error ? transferErr.message : "Transfer failed" });
        toast({ title: "Transfer failed", message: "See details below or check Job Theater.", tone: "error" });
      }
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
    const success = isJobSuccess(job.status);
    const ds = (job.destination_summary ?? {}) as NonNullable<TransferResult["destination_summary"]>;
    const rps = job.records_per_second ?? ds.records_per_second;
    setResult({
      success,
      records_transferred: job.records_processed,
      records_per_second: rps,
      error: job.error,
      job_id: job._id,
      destination: {
        database: job.destination_database,
        collection: job.destination_collection,
      },
      destination_summary: {
        ...ds,
        rejected_rows: job.rejected_rows ?? ds.rejected_rows,
        coerced_null_rows: job.coerced_null_rows ?? ds.coerced_null_rows,
        rejected_details: job.rejected_details ?? ds.rejected_details,
        records_per_second: rps ?? ds.records_per_second,
        load_history_report:
          ds.load_history_report
          ?? job.load_history_report,
      },
      reconciliation: job.reconciliation,
      explanation: job.explanation,
      ddl_executed: job.ddl_executed ?? job.ddl_log,
      event_log: job.event_log?.length ? job.event_log : (job._id ? readJobEventLog(job._id) : undefined),
      notifications: job.notifications,
      error_details: job.load_history_report
        ? { load_history_report: job.load_history_report }
        : undefined,
    });
    if (success) onTransferComplete();
  };

  /** Keep Job Theater mounted on fail/cancel so recovery CTAs remain visible. */
  const leaveTheaterToValidate = useCallback(() => {
    setActiveJobId(null);
    setTransferring(false);
    setResult(null);
    setStep(STEP_VALIDATE);
  }, []);

  const leaveTheaterToMap = useCallback(() => {
    setActiveJobId(null);
    setTransferring(false);
    setResult(null);
    setStep(STEP_MAP);
  }, []);

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
    const sourceTableName = sourceKind === "cloud" ? cloudPath.trim() : primarySourceStream;
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
      : Boolean(
          sourceConnectorId
          && (sourceKind === "cloud" ? cloudPath.trim() : (sourceTable || sourceCollection)),
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
    (destKindMode === "file_export" ||
      (Boolean(destType && targetDb && targetCollection) && !destSchemaLoading));

  const needsDbPreflight = destKindMode === "database";
  const canExecute =
    destKindMode === "file_export" ? canConfigureDest : Boolean(preflight?.passed);

  const destinationLabel = destKindMode === "file_export"
    ? exportFormat.toUpperCase()
    : destType
      ? `${destType}${targetCollection ? ` · ${targetCollection}` : ""}`
      : "Choose destination";
  const sourceLabel = sourceKind === "file"
    ? (file?.name ?? "Choose source")
    : sourceKind === "cloud"
      ? (cloudPath.trim() || sourceConnector?.name || "Cloud source")
      : (sourceConnector?.name ?? "Database source");
  const destLabelShort = destSelected && (destKindMode === "file_export" || Boolean(destType))
    ? (selectedDestConnector
      ? `${selectedDestConnector.name}${targetCollection ? ` · ${targetCollection}` : ""}`
      : destinationLabel)
    : "Choose destination";

  const mapSourceType = sourceKind === "file"
    ? (parsed?.file_type ?? file?.name.split(".").pop() ?? "file")
    : (sourceConnector?.type ?? "database");
  const mapSourceSubtitle = sourceKind === "file"
    ? `Uploaded file${parsed?.file_type ? ` · ${parsed.file_type.toUpperCase()}` : ""}${parsed?.row_count ? ` · ${parsed.row_count.toLocaleString()} rows` : ""}`
    : sourceKind === "cloud"
      ? `Cloud object${cloudPath ? ` · ${cloudPath}` : ""}`
      : sourceConnector
        ? `${sourceConnector.type}${sourceConnector.database ? ` · ${sourceConnector.database}` : ""}${
          isMultiStreamSource
            ? ` · ${multiStreamNames.length} streams`
            : primarySourceStream
              ? ` · ${primarySourceStream}`
              : ""
        }`
        : "Database source";
  const mapDestRouteLabel = destKindMode === "file_export"
    ? `${exportFormat.toUpperCase()} export`
    : destDriverType === "dynamodb"
      ? (targetCollection || targetDb || destinationLabel)
      : targetCollection
        ? `${targetDb}.${targetCollection}`
        : destinationLabel;
  const mapDestRouteSubtitle = destKindMode === "file_export"
    ? "File export destination"
    : destDriverType === "dynamodb"
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

  const effectiveMappingProof = useMemo(
    () =>
      mergeMappingProof(mappingProof, columnMappings, {
        destColumns,
        destType: destKindMode === "file_export" ? exportFormat : destType,
      }),
    [mappingProof, columnMappings, destColumns, destKindMode, exportFormat, destType],
  );
  const mappingProofSummary = useMemo(() => {
    if (!columnMappings.length) return null;
    const rows = effectiveMappingProof.mappings ?? [];
    return {
      destMode: effectiveMappingProof.dest_mode,
      mappedCount: effectiveMappingProof.summary?.mapped_count ?? rows.length,
      exactOverlaps: rows.filter((r) => r.match_quality === "exact_name").length,
      riskCount: effectiveMappingProof.summary?.risk_count ?? 0,
      reviewCount: effectiveMappingProof.summary?.review_count ?? 0,
      avgConfidence: effectiveMappingProof.summary?.avg_confidence,
      maxConfidence: effectiveMappingProof.summary?.max_confidence,
    };
  }, [columnMappings.length, effectiveMappingProof]);

  // Keep Data Pilot fed with the active validation/job IDs for NL triage & remediations.
  useEffect(() => {
    if (!preflight && !activeJobId) return;
    setActiveData((prev) => {
      const base = prev ?? {
        name: sourceLabel || "transfer",
        columns: columnMappings.map((m) => m.source),
        row_count: parsed?.row_count ?? sourceRowEstimate ?? 0,
      };
      return {
        ...base,
        name: base.name || sourceLabel || "transfer",
        columns: base.columns?.length ? base.columns : columnMappings.map((m) => m.source),
        row_count: base.row_count || parsed?.row_count || sourceRowEstimate || 0,
        preflight_run_id: preflight?.run_id || base.preflight_run_id,
        job_id: activeJobId || base.job_id,
        validation_status: preflighting
          ? "running"
          : preflight
            ? preflight.passed
              ? "passed"
              : "blocked"
            : base.validation_status,
        route: `${sourceLabel} → ${mapDestRouteLabel}`,
        blockers: (preflight?.blockers || []).map((b) => b.message).slice(0, 8),
      };
    });
  }, [
    activeJobId,
    columnMappings,
    mapDestRouteLabel,
    parsed?.row_count,
    preflight,
    preflighting,
    setActiveData,
    sourceLabel,
    sourceRowEstimate,
  ]);

  useEffect(() => {
    const handler = async (action: StudioAction) => {
      switch (action.kind) {
        case "normalize_control_chars":
          setStep(STEP_VALIDATE);
          await stripControlCharsAndRerun();
          break;
        case "quarantine_and_rerun":
          setStep(STEP_VALIDATE);
          await quarantineAndRerun();
          break;
        case "open_bad_data_fix":
          setStep(STEP_VALIDATE);
          toast({
            title: "Fix bad data",
            message: action.run_id
              ? `Opened Validate for run ${action.run_id}. Use Fix bad data on the Dry-run gate.`
              : "Opened Validate — use Fix bad data on the blocked Dry-run gate.",
            tone: "info",
          });
          break;
        case "review_mappings":
          setStep(STEP_MAP);
          break;
        case "rerun_preflight":
          setStep(STEP_VALIDATE);
          await executePreflight();
          break;
        default:
          break;
      }
    };
    registerStudioHandler(handler);
    return () => registerStudioHandler(null);
  });

  const handleSaveAsContract = async () => {
    if (!preflight) {
      toast({ title: "Run preflight first", message: "Validate gates before saving a contract.", tone: "warning" });
      return;
    }
    setSavingContract(true);
    try {
      const mappings = columnMappings.map((m) => ({
        source: m.source,
        target: m.target,
        confidence: m.confidence,
        transform: m.transform && m.transform !== "none" ? m.transform : undefined,
        source_type: m.inferredType || currentSourceSchema[m.source],
        target_type: m.destType || m.inferredType || currentSourceSchema[m.source],
      }));
      const name =
        `${sourceLabel || "source"} → ${mapDestRouteLabel || "destination"}`.slice(0, 180)
        || `contract-${Date.now()}`;
      const columnTypes: Record<string, string> = {};
      for (const [key, value] of Object.entries(currentSourceSchema || {})) {
        if (key) columnTypes[key] = String(value || "VARCHAR");
      }
      const contract = await createContractFromTransfer({
        name,
        source: buildSourceEndpoint() as Record<string, unknown>,
        destination: (destKindMode === "file_export"
          ? { kind: "file_export", format: exportFormat, database: targetDb, output_path: destOutputPath }
          : buildDestinationEndpoint()) as Record<string, unknown>,
        mappings,
        column_types: columnTypes,
        preflight_gates: (preflight.gates || []) as unknown as Record<string, unknown>[],
        quality_rules: (preflight.blockers || []).map((b) => ({
          name: b.id,
          expectation: b.message,
          severity: "block",
        })),
        // Draft contracts capture the intended schema even when Validate is still blocked.
        strict: Boolean(preflight.passed),
        metadata: {
          sync_mode: syncMode,
          validation_mode: validationMode,
          schema_policy: schemaPolicy,
          readiness_score: preflight.readiness_score,
          preflight_passed: Boolean(preflight.passed),
        },
      });
      try {
        sessionStorage.setItem("df2.last-saved-contract", JSON.stringify(contract));
      } catch {
        /* ignore */
      }
      try {
        // Broadcast before navigate so keep-alive Contracts can upsert immediately.
        window.dispatchEvent(
          new CustomEvent("df2:contracts-changed", { detail: { id: contract.id, contract } }),
        );
      } catch {
        /* ignore */
      }
      toast({
        title: "Contract saved as draft",
        message: `${contract.name} is now under Contracts. `
          + (preflight.passed
            ? "Preflight passed — you can Sign it there."
            : "Saved while Validate is still blocked — fix mappings, then Sign after gates pass."),
        tone: "success",
      });
      onOpenContracts?.();
    } catch (e) {
      toast({ title: "Could not save contract", message: (e as Error).message, tone: "error" });
    } finally {
      setSavingContract(false);
    }
  };

  useEffect(() => {
    const isLaunching = step === STEP_RUN && transferring && !activeJobId && !result;
    if (!isLaunching) {
      setRunStartupProgress(0);
      setRunStartupPhase(RUN_LAUNCH_STAGES[0]);
    }
  }, [step, transferring, activeJobId, result]);

  const resetTransferStudio = useCallback(() => {
    if (onFreshTransfer) {
      onFreshTransfer();
      return;
    }
    setStep(STEP_SOURCE);
    setSourceKind("file");
    setSourceConnectorId("");
    setSourceTable("");
    setSourceCollection("");
    setCloudPath("");
    setAdvancedOpen(false);
    setFile(null);
    setParsed(null);
    setSourceRowEstimate(null);
    setAnalysis(null);
    setPreflight(null);
    setCellPreview(null);
    setAnalyzing(false);
    setMappingProgress(0);
    setMappingPhase("Preparing schema context…");
    setSourceIntrospecting(false);
    setSourceIntrospectError(null);
    setStreamPreviews([]);
    setActiveStreamTab("");
    sourceIntrospectGateRef.current = { key: "", status: "idle" };
    setPreflighting(false);
    setSavingContract(false);
    setDragOver(false);
    setUploadError(null);
    setUploading(false);
    setConnectorId("");
    setDestType("");
    setDestKindMode("database");
    setExportFormat("json");
    setTransferPlan(null);
    setPersistedPlanId(null);
    setPlanLoading(false);
    setTargetDb("dataflow_test");
    setTargetCollection("");
    setDestHost("");
    setDestPort(0);
    routeAnalyzedKeyRef.current = "";
    setDestSchema("public");
    setDestUsername("");
    setDestPassword("");
    setDestConnectionString("");
    setDestOutputPath("");
    setDestWarehouse("");
    setTransferring(false);
    setActiveJobId(null);
    setResult(null);
    setSyncMode("full_refresh_append");
    setSchemaPolicy("manual_review");
    setValidationMode("balanced");
    setBackfillNewFields(false);
    setCursorField("");
    setPrimaryKeyField("");
    setStreamFields({});
    setColumnMappings([]);
    setDestColumns([]);
    setDestSchemaMap({});
    setDestSchemaLoading(false);
    setDestTableExists(null);
    setTransferLaunch(null);
    setLlmMappingUsed(false);
    setMappingProof(null);
    setRunStartupProgress(0);
    setRunStartupPhase(RUN_LAUNCH_STAGES[0]);
    autoSelectedConnector.current = false;
    autoSelectedSourceConnector.current = false;
    if (fileInputRef.current) fileInputRef.current.value = "";
    setActiveData(null);
  }, [onFreshTransfer, setActiveData]);

  return (
    <PageShell
      wide
      showHeader={false}
      className="df2-page-transfer-studio"
      title="Transfer Studio"
      description="Governed path: source → destination → map → preflight → run → proof"
    >
      <PageFrame className={`df2-transfer-studio-shell is-transfer-studio-active${step === STEP_MAP ? " is-map-step-active" : ""}`} showHonesty>
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
          destType={
            destKindMode === "file_export"
              ? exportFormat
              : destType || ""
          }
          rowCount={parsed?.row_count ?? sourceRowEstimate ?? undefined}
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
          mappingProof={mappingProof}
          proofOpen={mappingProofOpen}
          onProofOpenChange={setMappingProofOpen}
          streamNames={isMultiStreamSource ? multiStreamNames : []}
          activeStream={mapActiveStream || primarySourceStream}
          streamsDiverge={mapStreamsDiverge}
          onActiveStreamChange={(name) => {
            // Persist current tab mappings, then swap to the selected stream.
            const current = mapActiveStream || primarySourceStream;
            setStreamMappings((prev) => ({ ...prev, [current]: columnMappings }));
            const next = streamMappings[name];
            if (next && next.length) {
              setColumnMappings(next);
            }
            setMapActiveStream(name);
          }}
          onChangeMappings={(next) => {
            setColumnMappings(next);
            const current = mapActiveStream || primarySourceStream;
            if (isMultiStreamSource && current) {
              setStreamMappings((prev) => ({ ...prev, [current]: next }));
            }
          }}
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
        <div className="df2-transfer-step-panel df2-transfer-analyzing-panel">
          <div className="df2-card-body df2-analyzing">
            <Spinner size="lg" premium />
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
            <p className="df2-card-sub">
              Upload a file or pick a saved connector, then name the table or collection to read.
              For CDC / incremental multi-stream, comma-separate several names.
            </p>
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
              {!parsed && !uploading && (
                <div className="df2-upload-sample-row">
                  <span className="df2-label-hint">New to DataFlow?</span>
                  <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => void loadSampleDataset()}>
                    <DtIcon name="sparkle" size={14} /> Load sample orders CSV
                  </button>
                </div>
              )}
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
            connectorsLoading && cloudSourceConnectors.length === 0 ? (
              <LoadingBlock
                title="Loading cloud connectors"
                hint="Fetching saved S3, GCS, and Azure connections…"
                size="sm"
              />
            ) : cloudSourceConnectors.length === 0 ? (
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
          ) : connectorsLoading && dbSourceConnectors.length === 0 ? (
            <LoadingBlock
              title="Loading database connectors"
              hint="Fetching your saved MongoDB, PostgreSQL, and warehouse connections…"
              size="sm"
            />
          ) : dbSourceConnectors.length === 0 ? (
            <EmptyState
              icon="connectors"
              title="No database connectors"
              description="Add a PostgreSQL, MySQL, MongoDB, or warehouse connector first."
              compact
            />
          ) : (
            <div className="df2-source-endpoint">
              <div className="df2-source-endpoint-fields">
                <ConnectorSelect
                  id="source-connector"
                  label="Source connector"
                  value={sourceConnectorId}
                  onChange={setSourceConnectorId}
                  connectors={dbSourceConnectors}
                  placeholder="Select connector…"
                  hint="Saved connection (host, database, credentials)."
                />
                <div className="df2-field">
                  <label className="df2-label" htmlFor="source-stream-input">
                    {sourceConnector?.type === "mongodb" ? "Collection(s)" : "Table(s)"}
                  </label>
                  <input
                    id="source-stream-input"
                    className="df2-input"
                    value={sourceConnector?.type === "mongodb" ? sourceCollection : sourceTable}
                    onChange={(e) => {
                      if (sourceConnector?.type === "mongodb") setSourceCollection(e.target.value);
                      else setSourceTable(e.target.value);
                    }}
                    placeholder={
                      sourceConnector?.type === "mongodb"
                        ? "orders — or orders, customers"
                        : sourceConnector?.type === "dynamodb"
                          ? sourceConnector.database || "orders"
                          : "public.orders — or public.orders, public.items"
                    }
                    autoComplete="off"
                    spellCheck={false}
                  />
                  <span className="df2-label-hint">
                    {sourceConnector?.type === "mongodb"
                      ? "One collection, or several separated by commas."
                      : "One table, or several separated by commas."}
                  </span>
                </div>
              </div>

              <div className="df2-source-multistream" role="note">
                <div className="df2-source-multistream-head">
                  <DtIcon name="activity" size={15} />
                  <strong>
                    {isMultiStreamSource
                      ? `${multiStreamNames.length} streams — each read separately`
                      : "Single stream or multi-stream"}
                  </strong>
                </div>
                <p>
                  {isMultiStreamSource
                    ? "Each comma-separated name is a real table/collection. Preview opens one tab per stream. Mapping currently applies the primary stream schema to the shared map — open each stream tab to confirm columns match, or run one stream at a time for distinct schemas. Configure cursor / primary key per stream in Destination → Advanced for CDC."
                    : "For CDC or incremental across multiple tables, enter comma-separated names (example: sessions, users). Preview shows a tab for each; each stream keeps its own watermark. Distinct schemas across streams require separate transfers until per-stream mapping ships."}
                </p>
                {isMultiStreamSource && (
                  <ul className="df2-source-stream-chips" aria-label="Streams to sync">
                    {multiStreamNames.map((name, i) => {
                      const preview = streamPreviews.find((s) => s.name === name);
                      return (
                        <li
                          key={`${name}-${i}`}
                          className={
                            preview?.status === "error" ? "is-error"
                              : preview?.status === "ok" ? "is-ok"
                                : i === 0 ? "is-primary" : undefined
                          }
                        >
                          <span>{name}</span>
                          {preview?.status === "ok" && <em>ready</em>}
                          {preview?.status === "error" && <em>failed</em>}
                          {preview?.status === "loading" && <em>reading…</em>}
                        </li>
                      );
                    })}
                  </ul>
                )}
              </div>
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
                sourceIntrospecting={sourceIntrospecting}
                sourceIntrospectError={sourceIntrospectError}
                onRetrySourceIntrospect={retrySourceIntrospect}
                sourceObjectLabel={
                  sourceKind === "cloud"
                    ? cloudPath.trim()
                    : isMultiStreamSource
                      ? `${primarySourceStream} (+${multiStreamNames.length - 1} more)`
                      : primarySourceStream || (
                        sourceConnector?.type === "mongodb"
                          ? sourceCollection || sourceTable
                          : sourceTable
                      )
                }
                streamNames={sourceKind === "database" ? multiStreamNames : undefined}
                streamPreviews={sourceKind === "database" ? streamPreviews : undefined}
                activeStreamTab={activeStreamTab}
                onActiveStreamTabChange={setActiveStreamTab}
              />
            </div>
          </div>
        </div>

        {(() => {
          const fileReady = sourceKind === "file" && !!parsed;
          const connectorReady =
            isConnectorSource && (sourceKind === "database" ? dbSourceConnectors.length > 0 : cloudSourceConnectors.length > 0);
          if (!fileReady && !connectorReady) return null;
          const hint = fileReady
            ? "Source profiled — choose where data should land next."
            : sourceKind === "cloud"
              ? "Select connector and path to continue"
              : isMultiStreamSource
                ? `${multiStreamNames.length} streams selected — continue to pick a destination`
                : "Select connector and table/collection to continue";
          const disabled = fileReady ? uploading : !canConfigureDest || sourceIntrospecting;
          return (
            <div className="df2-card-footer df2-wizard-footer">
              <span className="df2-label-hint">{hint}</span>
              <button
                type="button"
                className="df2-btn df2-btn-primary"
                disabled={disabled}
                onClick={() => void proceedToDestination()}
              >
                {sourceIntrospecting ? <ButtonLoader label="Reading schema…" /> : "Continue to Destination →"}
              </button>
            </div>
          );
        })()}
      </div>
      )}

      {step === STEP_DESTINATION && (
      <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-dest-step">
        <div className="df2-card-head">
          <div>
            <h3 className="df2-card-title">Destination</h3>
            <p className="df2-card-sub">Pick a saved connector, then set database & table — schema loads before mapping.</p>
          </div>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => setAdvancedOpen(true)}
            leadingIcon={<DtIcon name="settings" size={14} />}
          >
            Advanced settings
          </Button>
        </div>
        <div className="df2-card-body">
          <div className="df2-field">
            <label className="df2-label">Destination Mode</label>
            <FilterBar ariaLabel="Destination mode">
              <FilterTabs
                ariaLabel="Destination mode"
                value={destKindMode}
                onChange={(mode) => {
                  setDestKindMode(mode);
                  resetRouteForDestinationChange();
                  if (mode === "file_export") void loadTransferPlan();
                }}
                items={[
                  { id: "database", label: "Database / Warehouse" },
                  { id: "file_export", label: "File Export" },
                ]}
              />
            </FilterBar>
          </div>

          {destKindMode === "file_export" ? (
            <>
              <div className="df2-field">
                <label className="df2-label">Export Format</label>
                <FilterBar ariaLabel="Export format">
                  <FilterTabs
                    ariaLabel="Export format"
                    value={exportFormat}
                    onChange={(format) => {
                      setExportFormat(format);
                      setTransferPlan(null);
                    }}
                    items={liveExportFormats.map((f) => ({ id: f.id, label: f.label }))}
                  />
                </FilterBar>
              </div>
              <div className="df2-field">
                <label className="df2-label">Output path (optional)</label>
                <input
                  className="df2-input"
                  value={destOutputPath}
                  onChange={(e) => setDestOutputPath(e.target.value)}
                  placeholder="exports/my-export.csv — leave empty for server exports folder"
                />
                <p className="df2-label-hint">
                  Leave empty to generate a downloadable file in the server exports folder.
                </p>
              </div>
            </>
          ) : (
            <>
          <DestinationPicker
            connectors={transferDestConnectors}
            connectorId={connectorId}
            destType={destType}
            liveDestTypes={liveDestTypes}
            onSelectConnector={applyConnectorSelection}
            onSelectManual={() => {
              setConnectorId("");
              resetRouteForDestinationChange();
            }}
            onSelectType={(type) => {
              resetRouteForDestinationChange();
              setDestType(type);
              setConnectorId("");
              setTargetCollection("");
              setDestHost(getConnectorDefaults(type).host);
              setDestPort(defaultPortForType(type));
            }}
          />

          {!connectorId && destType && destType !== "bigquery" && (
          <div className="df2-dest-section df2-dest-manual-fields">
            <label className="df2-label">Connection</label>
            <div className="df2-form-row">
              {destDriverType === "mongodb" || isGenericSql(destType) || ["mysql", "postgresql", "redshift", "sqlite"].includes(destDriverType) ? (
                <div className="df2-field df2-field-flex">
                  <label className="df2-label">Connection String (optional)</label>
                  <input
                    className="df2-input"
                    value={destConnectionString}
                    onChange={(e) => setDestConnectionString(e.target.value)}
                    placeholder={destDriverType === "mongodb" ? "mongodb://localhost:27017/" : getGenericSqlPlaceholder(destType)}
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
              {destDriverType === "snowflake" && (
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

          {!destType && destKindMode === "database" && (
            <p className="df2-label-hint" style={{ marginTop: 8 }}>
              Select a saved connector or Custom connection engine above. The route stays empty until you choose a destination.
            </p>
          )}

          {destType ? (
            <>
          <div className="df2-dest-section df2-dest-target-fields">
            <label className="df2-label">Target location</label>
            <div className="df2-form-row df2-dest-target-row">
            <div className="df2-field df2-field-flex">
              <label className="df2-label" htmlFor="dest-db">
                {destDriverType === "bigquery"
                  ? "GCP Project ID"
                  : destDriverType === "dynamodb"
                    ? "AWS region or local endpoint"
                    : "Database"}
              </label>
              <input id="dest-db" className="df2-input" value={targetDb} onChange={(e) => setTargetDb(e.target.value)} placeholder={destDriverType === "bigquery" ? "my-gcp-project" : destDriverType === "dynamodb" ? "us-east-1" : "test_db"} />
            </div>
            {destDriverType === "bigquery" && (
              <div className="df2-field df2-field-flex">
                <label className="df2-label">Dataset</label>
                <input className="df2-input" value={destSchema} onChange={(e) => setDestSchema(e.target.value)} placeholder="dataflow" />
              </div>
            )}
            <div className="df2-field df2-field-flex">
              <label className="df2-label" htmlFor="dest-col">
                {destDriverType === "mongodb" ? "Collection" : destDriverType === "dynamodb" ? "DynamoDB table" : "Table"}
              </label>
              <input id="dest-col" className="df2-input" value={targetCollection} onChange={(e) => setTargetCollection(e.target.value)} placeholder={destDriverType === "mongodb" ? "my_collection" : destDriverType === "dynamodb" ? "orders" : "my_table"} />
            </div>
            {getGenericSqlGroup(destType) === "postgresql+psycopg2" && (
              <div className="df2-field df2-field-120">
                <label className="df2-label">Schema</label>
                <input className="df2-input" value={destSchema} onChange={(e) => setDestSchema(e.target.value)} />
              </div>
            )}
          </div>
            {/* Status lives below the row so dynamic copy never shifts aligned inputs */}
            {destDriverType !== "mongodb" && destDriverType !== "dynamodb" && (
              <div
                className={`df2-dest-target-status${
                  destTableExists === true ? " is-existing" : destTableExists === false ? " is-create" : ""
                }`}
                aria-live="polite"
              >
                {destTableExists === true ? (
                  <>
                    <DtIcon name="database" size={14} />
                    <p>
                      <strong>Existing table detected.</strong> New rows will <strong>append</strong> by default.
                      Open Advanced settings to switch to overwrite or incremental sync.
                    </p>
                  </>
                ) : destTableExists === false ? (
                  <>
                    <DtIcon name="sparkle" size={14} />
                    <p>
                      <strong>Table not found.</strong> DataFlow will create it automatically on first write.
                    </p>
                  </>
                ) : (
                  <>
                    <DtIcon name="activity" size={14} />
                    <p>Enter a table name. If it does not exist yet, DataFlow creates it on first write.</p>
                  </>
                )}
              </div>
            )}
          </div>
          {destDriverType === "dynamodb" && (
            <p className="df2-label-hint df2-field-note">
              Set region to <code>us-east-1</code> for AWS, or <code>http://localhost:8000</code> for DynamoDB Local / personal cloud.
              Table name is the DynamoDB table to read or write.
            </p>
          )}
          {destDriverType === "bigquery" && (
            <p className="df2-label-hint df2-field-note">
              Set Database to GCP project ID. Optional: save service account JSON path as connection string in connector settings.
            </p>
          )}
            </>
          ) : null}
            </>
          )}

          <div className="df2-dest-sync-summary">
            <div className="df2-dest-sync-summary-main">
              <span className="df2-rail-kicker">Sync defaults</span>
              <p>
                <strong>{syncModeLabel}</strong>
                <span aria-hidden> · </span>
                {schemaPolicyLabel}
                <span aria-hidden> · </span>
                {VALIDATION_MODES.find((m) => m.id === validationMode)?.label ?? validationMode} validation
              </p>
              <p className="df2-label-hint">
                Destination selection stays on this page. Overwrite, CDC, SCD2, mirror, cursors, and drift
                policies open in Advanced settings.
              </p>
            </div>
            <div className="df2-dest-sync-summary-actions">
              <span className={`df2-badge ${streamNeedsReview ? "df2-badge-run" : "df2-badge-live"}`}>
                {currentSourceColumns.length ? (streamNeedsReview ? "Review required" : "Ready") : "Waiting for schema"}
              </span>
              <Button
                size="sm"
                variant="secondary"
                onClick={() => setAdvancedOpen(true)}
                leadingIcon={<DtIcon name="settings" size={14} />}
              >
                Advanced settings
              </Button>
            </div>
            {(syncMode === "scd2" || syncMode === "mirror") && requiresPrimaryKey && !primaryKeyField && (
              <p className="df2-label-hint df2-dest-sync-warning">
                {syncMode === "scd2" ? "SCD Type 2" : "Mirror"} requires a primary key — open Advanced settings to set it.
              </p>
            )}
            {isMultiStreamSource && syncMode !== "cdc" && (
              <p className="df2-label-hint df2-dest-sync-warning" role="status">
                Multi-stream full/incremental modes currently run the <strong>primary stream</strong> only
                ({advancedStreamNames[0] || "first selected"}). Switch to <strong>CDC</strong> for shared-reader multi-table sync, or run streams one at a time.
              </p>
            )}
          </div>

          <DestinationAdvancedDrawer
            open={advancedOpen}
            onClose={() => setAdvancedOpen(false)}
            syncModes={SYNC_MODES}
            schemaPolicies={SCHEMA_POLICIES}
            validationModes={VALIDATION_MODES}
            syncMode={syncMode}
            schemaPolicy={schemaPolicy}
            validationMode={validationMode}
            backfillNewFields={backfillNewFields}
            streamNames={advancedStreamNames}
            streamFields={streamFields}
            defaultCursor={cursorField}
            defaultPrimaryKey={primaryKeyField}
            sourceColumns={currentSourceColumns}
            sourceSchema={currentSourceSchema}
            syncModeLabel={syncModeLabel}
            schemaPolicyLabel={schemaPolicyLabel}
            requiresCursor={requiresCursor}
            requiresPrimaryKey={requiresPrimaryKey}
            streamNeedsReview={streamNeedsReview}
            suggestedCursor={cursorCandidate}
            suggestedPrimaryKey={primaryKeyCandidate}
            priorityColumn={priorityColumn}
            priorityDirection={priorityDirection}
            rowLimit={rowLimit}
            onPriorityColumnChange={setPriorityColumn}
            onPriorityDirectionChange={setPriorityDirection}
            onRowLimitChange={setRowLimit}
            onSyncModeChange={setSyncMode}
            onSchemaPolicyChange={(policy) => {
              setSchemaPolicy(policy);
              if (policy === "propagate_columns" || policy === "propagate_all") {
                setBackfillNewFields(true);
              }
            }}
            onValidationModeChange={setValidationMode}
            onBackfillChange={setBackfillNewFields}
            onStreamCursorChange={(stream, value) => {
              setStreamFields((prev) => ({
                ...prev,
                [stream]: {
                  cursorField: value,
                  primaryKeyField: prev[stream]?.primaryKeyField ?? primaryKeyField,
                },
              }));
              if (!isMultiStreamSource || stream === advancedStreamNames[0]) {
                setCursorField(value);
              }
            }}
            onStreamPrimaryKeyChange={(stream, value) => {
              setStreamFields((prev) => ({
                ...prev,
                [stream]: {
                  cursorField: prev[stream]?.cursorField ?? cursorField,
                  primaryKeyField: value,
                },
              }));
              if (!isMultiStreamSource || stream === advancedStreamNames[0]) {
                setPrimaryKeyField(value);
              }
            }}
          />

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
            disabled={
              !canConfigureDest
              || planLoading
              || (destKindMode === "database" && (!destType || !targetCollection.trim()))
            }
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
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-validate-step df2-validate-split df2-validate-dashboard-host">
          <ValidateDashboard
            preflight={preflight}
            running={preflighting}
            confidenceThreshold={confidenceThreshold}
            destType={destKindMode === "file_export" ? exportFormat : destType}
            validationMode={validationMode}
            onApplyAction={applySuggestedAction}
            onStripControlChars={stripControlCharsAndRerun}
            onQuarantineAndRerun={quarantineAndRerun}
            cellPreview={cellPreview}
            onReviewMappings={() => setStep(STEP_MAP)}
            onOpenMappingProof={() => setMappingProofOpen(true)}
            mappingProofSummary={mappingProofSummary}
            onRunPreflight={() => void executePreflight()}
          />
        </div>
      )}

      {mappingProofOpen && columnMappings.length > 0 && step !== STEP_MAP && (
        <MappingProofDrawer
          open={mappingProofOpen}
          onClose={() => setMappingProofOpen(false)}
          proof={effectiveMappingProof}
          sourceLabel={sourceLabel}
          destLabel={mapDestRouteLabel}
        />
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
          <div className="df2-card-body df2-run-launch">
            <span className="df2-run-launch-kicker">Live control plane</span>
            <h3>Transfer engine is preparing execution</h3>
            <p>{runStartupPhase}</p>

            <div className="df2-run-launch-route" aria-label="Transfer route">
              <strong title={sourceLabel}>{sourceLabel}</strong>
              <DtIcon name="transfer" size={14} />
              <strong title={mapDestRouteLabel}>{mapDestRouteLabel}</strong>
            </div>

            <div className="df2-run-launch-progress" role="status" aria-live="polite">
              <div className="df2-run-launch-progress-meta">
                <span>Initializing transfer job</span>
                <strong>Starting…</strong>
              </div>
              <div className="df2-run-launch-progress-track df2-run-launch-progress-track-indeterminate">
                <span className="df2-run-launch-progress-fill" style={{ width: `${Math.min(runStartupProgress, 40)}%` }} />
              </div>
            </div>

            <div className="df2-run-launch-stages" aria-label="Launch stages">
              {RUN_LAUNCH_STAGES.map((stage, idx) => {
                const state = runStartupProgress >= (idx + 1) * 25 ? "done" : runStartupProgress >= idx * 25 ? "active" : "pending";
                return (
                  <span key={stage} className={`df2-run-launch-stage ${state}`}>
                    {stage}
                  </span>
                );
              })}
            </div>

            <div className="df2-run-launch-foot">
              <Spinner size="sm" />
              <span>Establishing telemetry stream and destination writer...</span>
            </div>
          </div>
        </div>
      )}

      {step === STEP_RUN && activeJobId && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-run-step">
          <div className="df2-card-body df2-run-theater-host">
            <JobTheater
              jobId={activeJobId}
              sourceLabel={file?.name || sourceConnector?.name}
              destLabel={`${targetDb}.${targetCollection}`}
              sourceType={sourceKind === "file" ? "file" : sourceConnector?.type || sourceKind}
              destType={destKindMode === "file_export" ? exportFormat : destType}
              preflight={preflight || undefined}
              onComplete={handleJobComplete}
              onNewTransfer={resetTransferStudio}
              onBackToValidate={leaveTheaterToValidate}
              onBackToMap={leaveTheaterToMap}
              onResumed={(nextId) => {
                setActiveJobId(nextId);
                setTransferring(true);
                setResult(null);
              }}
            />
          </div>
        </div>
      )}

      {step === STEP_RUN && result && !activeJobId && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-run-step df2-result-host">
          <TransferResultDashboard
            result={result}
            sourceLabel={sourceLabel}
            destLabel={mapDestRouteLabel}
            sourceType={sourceKind === "file" ? "file" : sourceConnector?.type || sourceKind}
            destType={destKindMode === "file_export" ? exportFormat : destType}
            onNewTransfer={resetTransferStudio}
            onSchedule={() => void handleScheduleRoute()}
            onOpenValidate={() => setStep(STEP_VALIDATE)}
          />
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
          savingContract={savingContract}
          onBack={() => setStep(STEP_MAP)}
          onRunPreflight={() => void executePreflight()}
          onApproveMappings={() => void approveAllAndPreflight()}
          onExecute={() => void executeTransfer()}
          onOpenJobTheater={openJobTheater}
          onSaveAsContract={() => void handleSaveAsContract()}
        />
      )}
      </div>
      </PageFrame>
    </PageShell>
  );
}
