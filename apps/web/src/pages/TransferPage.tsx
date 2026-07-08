import { useEffect, useRef, useState, type CSSProperties } from "react";
import { JobTheater } from "../components/JobTheater";
import { DtIcon } from "../components/DtIcon";
import { MappingCanvas } from "../components/MappingCanvas";
import { EmptyState } from "../components/EmptyState";
import { PageShell } from "../components/ui/PageShell";
import { WizardSteps } from "../components/ui/WizardSteps";
import { ButtonLoader, Spinner } from "../components/LoadingState";
import { useToast } from "../components/Toast";
import { PreflightTimeline } from "../components/PreflightTimeline";
import { useActiveData } from "../lib/DataContext";
import {
  analyzeDbTransfer,
  analyzeFileTransfer,
  analyzeSchemaEnhanced,
  buildColumnSamples,
  runPreflight,
  runUniversalTransfer,
  transferFile,
  uploadFile,
} from "../lib/api";
import { buildPreflightMappings } from "../lib/mapping";
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
}

const STEPS = [
  { n: 1, label: "Source", icon: "upload" },
  { n: 2, label: "AI Mapping", icon: "sparkle" },
  { n: 3, label: "Destination", icon: "connectors" },
  { n: 4, label: "Preflight", icon: "gate" },
  { n: 5, label: "Execute", icon: "transfer" },
];


const DEST_TYPES = [
  { id: "mongodb", label: "MongoDB", icon: "connectors" },
  { id: "postgresql", label: "PostgreSQL", icon: "connectors" },
  { id: "mysql", label: "MySQL", icon: "connectors" },
  { id: "snowflake", label: "Snowflake", icon: "connectors" },
  { id: "bigquery", label: "BigQuery", icon: "connectors" },
] as const;

const EXPORT_FORMATS = [
  { id: "csv", label: "CSV" },
  { id: "json", label: "JSON" },
  { id: "jsonl", label: "JSONL" },
] as const;

const SOURCE_KINDS = [
  { id: "file", label: "File" },
  { id: "database", label: "Database / Warehouse" },
] as const;

const ACCEPTED_UPLOAD_EXTENSIONS = new Set(["csv", "json", "jsonl", "tsv"]);
const MAX_UPLOAD_BYTES = 250 * 1024 * 1024;

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

export function TransferPage({ connectors, onTransferComplete }: TransferPageProps) {
  const { toast } = useToast();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { setActiveData } = useActiveData();
  const [step, setStep] = useState(1);
  const [sourceKind, setSourceKind] = useState<"file" | "database">("file");
  const [sourceConnectorId, setSourceConnectorId] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [sourceCollection, setSourceCollection] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [parsed, setParsed] = useState<ParsedUpload | null>(null);
  const [analysis, setAnalysis] = useState<EnhancedAnalysis | null>(null);
  const [preflight, setPreflight] = useState<PreflightResult | null>(null);
  const [analyzing, setAnalyzing] = useState(false);
  const [preflighting, setPreflighting] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [connectorId, setConnectorId] = useState("");
  const [destType, setDestType] = useState<string>("mongodb");
  const [destKindMode, setDestKindMode] = useState<"database" | "file_export">("database");
  const [exportFormat, setExportFormat] = useState("json");
  const [transferPlan, setTransferPlan] = useState<TransferPlan | null>(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [targetDb, setTargetDb] = useState("test_db");
  const [targetCollection, setTargetCollection] = useState("");
  const [destHost, setDestHost] = useState("localhost");
  const [destPort, setDestPort] = useState(5432);
  const [destSchema, setDestSchema] = useState("public");
  const [destUsername, setDestUsername] = useState("");
  const [destPassword, setDestPassword] = useState("");
  const [destWarehouse, setDestWarehouse] = useState("");
  const [transferring, setTransferring] = useState(false);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [result, setResult] = useState<TransferResult | null>(null);
  const [syncMode, setSyncMode] = useState<SyncMode>("full_refresh_overwrite");
  const [schemaPolicy, setSchemaPolicy] = useState<SchemaPolicy>("manual_review");
  const [validationMode, setValidationMode] = useState<ValidationMode>("strict");
  const [backfillNewFields, setBackfillNewFields] = useState(false);
  const [cursorField, setCursorField] = useState("");
  const [primaryKeyField, setPrimaryKeyField] = useState("");

  const destConnectors = connectors.filter((c) => c.type === destType);
  const dbSourceConnectors = connectors.filter((c) =>
    ["mongodb", "postgresql", "snowflake", "mysql", "bigquery"].includes(c.type)
  );
  const sourceConnector = dbSourceConnectors.find((c) => c.id === sourceConnectorId);
  const currentSourceColumns = sourceKind === "file"
    ? parsed?.columns ?? []
    : transferPlan?.source_columns ?? [];
  const currentSourceSchema = sourceKind === "file"
    ? parsed?.schema ?? {}
    : transferPlan?.source_schema ?? {};
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

  const loadTransferPlan = async () => {
    if (sourceKind === "file" && file) {
      setPlanLoading(true);
      try {
        const plan = await analyzeFileTransfer(file, {
          destKind: destKindMode,
          destFormat: destKindMode === "file_export" ? exportFormat : destType,
          destDatabase: targetDb,
          destTable: destType !== "mongodb" ? targetCollection : undefined,
          destCollection: destType === "mongodb" ? targetCollection : undefined,
        });
        setTransferPlan(plan);
      } catch (e) {
        toast({ title: "Route analysis failed", message: "Could not build transfer plan.", tone: "error" });
        console.error(e);
      }
      setPlanLoading(false);
    }
  };

  const runAiAnalysis = async (data: ParsedUpload) => {
    setAnalyzing(true);
    try {
      const rows = data.data ?? data.sample_data;
      const columnSamples = buildColumnSamples(data.columns, rows);
      const result = await analyzeSchemaEnhanced(columnSamples);
      setAnalysis(result);
      toast({
        title: "Mapping analysis complete",
        message: `${result.columns.length} columns classified with ${result.quality_score.toFixed(0)}% quality.`,
        tone: result.quality_score >= 85 ? "success" : "warning",
      });
      setStep(2);
    } catch (e) {
      toast({ title: "AI mapping unavailable", message: "Continuing with manual mapping.", tone: "warning" });
      console.error("AI analysis failed:", e);
      setStep(3);
    }
    setAnalyzing(false);
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
    setFile(selected);
    setResult(null);
    setAnalysis(null);
    setPreflight(null);
    setUploading(true);
    try {
      const data = await uploadFile(selected);
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
        message: `${data.row_count.toLocaleString()} rows and ${data.columns.length} columns detected.`,
        tone: "success",
      });
      setStep(1);
      await runAiAnalysis(data);
    } catch (e) {
      toast({ title: "Upload failed", message: "Check file format and try again.", tone: "error" });
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
      toast({ title: "Source file required", message: "Upload a CSV, TSV, JSON, or JSONL file to continue.", tone: "warning" });
      setStep(1);
      return true;
    }
    if (sourceKind === "database" && !sourceConnectorId) {
      toast({ title: "Source connector required", message: "Select a saved database or warehouse connector.", tone: "warning" });
      setStep(1);
      return true;
    }
    if (sourceKind === "database" && !(sourceTable || sourceCollection)) {
      toast({ title: "Source stream required", message: "Enter the table or collection name to inspect.", tone: "warning" });
      setStep(1);
      return true;
    }
    return false;
  };

  const explainDestinationGap = () => {
    if (explainSourceGap()) return true;
    if (destKindMode === "database" && !targetDb.trim()) {
      toast({ title: "Destination database required", message: "Enter the target database or project.", tone: "warning" });
      setStep(3);
      return true;
    }
    if (destKindMode === "database" && !targetCollection.trim()) {
      toast({ title: "Destination table required", message: "Enter the target table or collection.", tone: "warning" });
      setStep(3);
      return true;
    }
    if (streamNeedsReview) {
      toast({
        title: "Stream contract needs review",
        message: `${requiresCursor && !cursorField ? "Select a cursor field. " : ""}${requiresPrimaryKey && !primaryKeyField ? "Select a primary key." : ""}`.trim(),
        tone: "warning",
      });
      setStep(3);
      return true;
    }
    return false;
  };

  const goToDestination = () => {
    if (explainSourceGap()) return;
    setStep(3);
  };

  const goToPreflight = () => {
    if (explainDestinationGap()) return;
    setStep(4);
    void executePreflight();
  };

  const executePreflight = async () => {
    if (!canRunPreflight || streamNeedsReview) {
      explainDestinationGap();
      return;
    }
    setPreflighting(true);
    setStep(4);
    setPreflight(null);
    try {
      let columns: string[] = [];
      let columnTypes: Record<string, string> = {};
      let mappings: { source: string; target: string; confidence: number; reason?: string }[] = [];
      let sampleRows: Record<string, unknown>[] | undefined;
      let rowCount = 0;
      let estimatedBytes = file?.size ?? 0;

      if (sourceKind === "file") {
        if (!parsed || !analysis) {
          toast({ title: "Analysis required", message: "Complete AI mapping before preflight.", tone: "warning" });
          setStep(2);
          return;
        }
        columns = parsed.columns;
        columnTypes = parsed.schema || {};
        rowCount = parsed.row_count;
        sampleRows = (parsed.data ?? parsed.sample_data)?.slice(0, 100);
        mappings = buildPreflightMappings(analysis.columns);
      } else {
        if (!sourceConnector) {
          toast({ title: "Source required", message: "Select a source connector and table.", tone: "warning" });
          setStep(1);
          return;
        }
        const routePlan = await analyzeDbTransfer({
          sourceConnectorId: sourceConnectorId,
          sourceFormat: sourceConnector.type,
          sourceDatabase: sourceConnector.database,
          sourceTable: sourceTable || undefined,
          sourceCollection: sourceCollection || undefined,
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
        mappings = buildPreflightMappings(dbAnalysis.columns);
        setTransferPlan(routePlan);
      }

      const pf = await runPreflight({
        columns,
        column_types: columnTypes,
        row_count: rowCount,
        mappings,
        connector_id: destKindMode === "database" ? connectorId || undefined : undefined,
        source_connector_id: sourceKind === "database" ? sourceConnectorId || undefined : undefined,
        sample_rows: sampleRows,
        estimated_bytes: estimatedBytes,
        sync_mode: syncMode,
        schema_policy: schemaPolicy,
        validation_mode: validationMode,
        backfill_new_fields: backfillNewFields,
        stream_contracts: streamContracts,
      });
      setPreflight(pf);
      if (!pf.passed) {
        toast({
          title: "Preflight blocked",
          message: `${pf.blockers.length} gate(s) failed — fix issues before executing.`,
          tone: "warning",
        });
      }
    } catch (e) {
      toast({ title: "Preflight failed", message: "Validation could not complete.", tone: "error" });
      console.error(e);
    } finally {
      setPreflighting(false);
    }
  };

  const executeTransfer = async () => {
    const needsDbTarget = destKindMode === "database";
    if (sourceKind === "file" && !file) {
      toast({ title: "Source file required", message: "Upload a file before executing.", tone: "warning" });
      setStep(1);
      return;
    }
    if (sourceKind === "database" && !sourceConnectorId) {
      toast({ title: "Source connector required", message: "Select a source connector before executing.", tone: "warning" });
      setStep(1);
      return;
    }
    if (needsDbTarget && (!targetDb || !targetCollection)) {
      toast({ title: "Destination required", message: "Enter the target database and table or collection.", tone: "warning" });
      setStep(3);
      return;
    }
    if (destKindMode === "database" && !preflight?.passed) {
      toast({ title: "Preflight required", message: "Run and pass preflight gates before writing to a database.", tone: "warning" });
      setStep(4);
      return;
    }

    const enforcePreflight = destKindMode === "database";

    setTransferring(true);
    setStep(5);
    setActiveJobId(null);
    setResult(null);
    const transferMappings = analysis ? buildPreflightMappings(analysis.columns) : undefined;
    try {
      const useUniversal = sourceKind === "database" || destKindMode === "file_export";
      const data = useUniversal
        ? await runUniversalTransfer({
            file: sourceKind === "file" ? file ?? undefined : undefined,
            sourceKind,
            sourceFormat: sourceConnector?.type,
            sourceConnectorId: sourceConnectorId || undefined,
            sourceDatabase: sourceConnector?.database,
            sourceTable: sourceConnector?.type !== "mongodb" ? sourceTable || sourceCollection : undefined,
            sourceCollection: sourceConnector?.type === "mongodb" ? sourceCollection || sourceTable : undefined,
            destKind: destKindMode,
            destFormat: destKindMode === "file_export" ? exportFormat : destType,
            destDatabase: targetDb,
            destSchema: destType === "snowflake" ? "PUBLIC" : destType === "bigquery" ? destSchema : destSchema,
            destTable: destType !== "mongodb" ? targetCollection : undefined,
            destCollection: destType === "mongodb" ? targetCollection : targetCollection,
            destConnectorId: connectorId || undefined,
            destHost: destType !== "mongodb" ? destHost : undefined,
            destPort: destType === "postgresql" ? destPort : destType === "mysql" ? 3306 : destType === "snowflake" || destType === "bigquery" ? 443 : undefined,
            destUsername: destUsername || undefined,
            destPassword: destPassword || undefined,
            destWarehouse: destType === "snowflake" ? destWarehouse : undefined,
            skipPreflight: !enforcePreflight,
            mappings: transferMappings,
            syncMode,
            schemaPolicy,
            validationMode,
            backfillNewFields,
            streamContracts,
          })
        : await transferFile(file!, targetDb, targetCollection, {
            connectorId: connectorId || undefined,
            skipPreflight: !enforcePreflight,
            destType,
            destHost: destType !== "mongodb" ? destHost : undefined,
            destPort: destType === "postgresql" ? destPort : destType === "mysql" ? 3306 : destType === "snowflake" || destType === "bigquery" ? 443 : undefined,
            destSchema: destType === "snowflake" ? "PUBLIC" : destType === "bigquery" ? "dataflow" : destSchema,
            destUsername: destUsername || undefined,
            destPassword: destPassword || undefined,
            destWarehouse: destType === "snowflake" ? destWarehouse : undefined,
            syncMode,
            schemaPolicy,
            validationMode,
            backfillNewFields,
            streamContracts,
          });
      if (data.job_id && (data as { async?: boolean }).async) {
        setActiveJobId(data.job_id);
        setTransferring(false);
        return;
      }
      setResult(data);
      if (data.success) onTransferComplete();
    } catch {
      setResult({ success: false, error: "Transfer failed" });
      toast({ title: "Transfer failed", message: "See details below or check Job Theater.", tone: "error" });
    }
    setTransferring(false);
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

  const canConfigureDest =
    sourceKind === "database"
      ? Boolean(sourceConnectorId && (sourceTable || sourceCollection))
      : Boolean(parsed);

  const canRunPreflight =
    canConfigureDest &&
    (destKindMode === "file_export" || Boolean(targetDb && targetCollection));

  const mappedColumns = analysis?.columns.length ?? 0;
  const highConfidenceColumns = analysis?.columns.filter((c) => c.confidence >= 0.9).length ?? 0;
  const reviewColumns = analysis?.columns.filter((c) => c.confidence < 0.85).length ?? 0;
  const piiColumns = analysis?.pii_columns.length ?? 0;
  const readiness = preflight?.readiness_score ?? 0;
  const rejectedRows =
    result?.destination_summary?.rejected_rows ??
    result?.reconciliation?.rejected_rows ??
    0;
  const assuranceStages = [
    {
      label: "Source contract",
      value: parsed || sourceConnectorId ? "Ready" : "Pending",
      tone: parsed || sourceConnectorId ? "ok" : "muted",
    },
    {
      label: "Mapping engine",
      value: mappedColumns ? `${highConfidenceColumns}/${mappedColumns} high` : "Pending",
      tone: reviewColumns ? "warn" : mappedColumns ? "ok" : "muted",
    },
    {
      label: "Preflight gates",
      value: preflight ? `${preflight.passed_count}/${preflight.total_gates}` : "Not run",
      tone: preflight?.passed ? "ok" : preflight?.blockers.length ? "block" : "muted",
    },
    {
      label: "Reconciliation",
      value: result?.reconciliation?.passed ? "Verified" : result ? "Check" : "Pending",
      tone: result?.reconciliation?.passed ? "ok" : result ? "warn" : "muted",
    },
  ];
  const stepProgress = Math.round(((Math.min(step, 5) - 1) / 4) * 100);
  const destinationLabel = destKindMode === "file_export"
    ? exportFormat.toUpperCase()
    : `${destType}${targetCollection ? ` · ${targetCollection}` : ""}`;
  const nextAction = (() => {
    if (transferring || activeJobId) {
      return { title: "Execution is running", body: "Watch batch progress, throughput, and reconciliation from the live theater.", label: "Watching", icon: "activity", disabled: true, run: () => {} };
    }
    if (preflighting) {
      return { title: "Preflight is running", body: "Validation gates are checking schema, mapping, policy, and destination readiness.", label: "Validating", icon: "gate", disabled: true, run: () => {} };
    }
    if (result) {
      return {
        title: result.success ? "Transfer completed" : "Transfer needs attention",
        body: result.success ? "Review final proof or start another route." : result.error || "Inspect the failure and run again after correction.",
        label: result.success ? "New transfer" : "Run preflight",
        icon: result.success ? "plus" : "gate",
        disabled: false,
        run: result.success ? () => setStep(1) : goToPreflight,
      };
    }
    if (!canConfigureDest) {
      return {
        title: "Choose a source",
        body: sourceKind === "file" ? "Upload a structured file so DataFlow can profile schema and samples." : "Select a connector and table or collection.",
        label: sourceKind === "file" ? "Upload file" : "Review source",
        icon: sourceKind === "file" ? "upload" : "database",
        disabled: false,
        run: () => sourceKind === "file" ? fileInputRef.current?.click() : setStep(1),
      };
    }
    if (sourceKind === "file" && analysis && step < 3) {
      return { title: "Review mapping result", body: "Semantic mapping is ready. Continue to destination and schema policy.", label: "Configure destination", icon: "connectors", disabled: false, run: goToDestination };
    }
    if (!preflight?.passed) {
      return { title: "Run preflight before writing", body: "Validate schema contract, cursor/key policy, mapping confidence, and destination readiness.", label: "Run preflight", icon: "gate", disabled: false, run: goToPreflight };
    }
    return { title: "Ready to execute", body: "Preflight passed. Execute the transfer and monitor live reconciliation.", label: "Execute transfer", icon: "transfer", disabled: false, run: () => void executeTransfer() };
  })();

  return (
    <PageShell
      wide
      title="Transfer Studio"
      description="Enterprise transfer cockpit for schema mapping, preflight, execution, and reconciliation."
    >
      <div className="df2-transfer-control-plane">
        <div className="df2-control-metric">
          <span>Route</span>
          <strong>{sourceKind === "file" ? "File" : sourceConnector?.type ?? "Database"} → {destKindMode === "file_export" ? exportFormat.toUpperCase() : destType}</strong>
        </div>
        <div className="df2-control-metric">
          <span>Schema</span>
          <strong>{parsed ? `${parsed.columns.length} columns` : mappedColumns ? `${mappedColumns} mapped` : "Waiting"}</strong>
        </div>
        <div className="df2-control-metric">
          <span>Assurance</span>
          <strong>{preflight ? `${readiness}% ready` : analysis ? `${analysis.quality_score.toFixed(0)}% mapped` : "Not scored"}</strong>
        </div>
        <div className={`df2-control-metric ${rejectedRows ? "warn" : ""}`}>
          <span>Rejects</span>
          <strong>{rejectedRows ? rejectedRows.toLocaleString() : "0"}</strong>
        </div>
      </div>

      <section className="df2-transfer-command" aria-label="Transfer command cockpit">
        <div className="df2-route-visual">
          <div className="df2-route-node">
            <DtIcon name={sourceKind === "file" ? "upload" : "database"} size={20} />
            <span>Source</span>
            <strong>{sourceKind === "file" ? file?.name ?? "File upload" : sourceConnector?.name ?? "Database source"}</strong>
          </div>
          <div className="df2-route-line" style={{ "--route-progress": `${stepProgress}%` } as CSSProperties} aria-hidden>
            <span />
            <i />
          </div>
          <div className="df2-route-node">
            <DtIcon name={destKindMode === "file_export" ? "download" : "database"} size={20} />
            <span>Destination</span>
            <strong>{destinationLabel}</strong>
          </div>
        </div>

        <div className="df2-next-action-card">
          <span className="df2-rail-kicker">Guided action</span>
          <h2>{nextAction.title}</h2>
          <p>{nextAction.body}</p>
          <button
            type="button"
            className="df2-btn df2-btn-primary"
            onClick={nextAction.run}
            disabled={nextAction.disabled}
          >
            <DtIcon name={nextAction.icon} size={16} /> {nextAction.label}
          </button>
        </div>

        <div className="df2-readiness-card">
          <span className="df2-rail-kicker">Readiness checklist</span>
          {assuranceStages.map((stage) => (
            <div key={stage.label} className={`df2-readiness-row ${stage.tone}`}>
              <span>{stage.label}</span>
              <strong>{stage.value}</strong>
            </div>
          ))}
        </div>
      </section>

      <WizardSteps
        steps={STEPS}
        current={step}
        onStepClick={setStep}
        canGoTo={(n) =>
          n < step ||
          n === 1 ||
          (n === 2 && sourceKind === "file" && !!parsed) ||
          (n === 3 && canConfigureDest) ||
          (n === 4 && canRunPreflight) ||
          (n === 5 && !!preflight?.passed)
        }
      />

      {step > 1 && (
        <div className="df2-step-summary">
          <DtIcon name="check" size={14} />
          <span>
            Source: <strong>{sourceKind === "file" ? file?.name ?? "File" : sourceConnector?.name ?? "Database"}</strong>
            {parsed && <> · {parsed.row_count.toLocaleString()} rows</>}
          </span>
          {step > 2 && analysis && (
            <>
              <span>·</span>
              <span>Mapping: <strong>{(analysis.quality_score).toFixed(0)}% quality</strong></span>
            </>
          )}
          {step > 3 && targetCollection && (
            <>
              <span>·</span>
              <span>Dest: <strong>{targetDb}.{targetCollection}</strong></span>
            </>
          )}
        </div>
      )}

      <div className="df2-transfer-grid">
      <main className="df2-stack df2-transfer-main">
      {step === 1 && (
      <div className="df2-card df2-card-elevated df2-transfer-panel">
        <div className="df2-card-head"><h3 className="df2-card-title">1. Select Source</h3></div>
        <div className="df2-card-body">
          <div className="df2-field" style={{ marginBottom: 16 }}>
            <label className="df2-label">Source Type</label>
            <div className="df2-segment">
              {SOURCE_KINDS.map((s) => (
                <button
                  key={s.id}
                  type="button"
                  className={`df2-btn ${sourceKind === s.id ? "df2-btn-primary" : ""}`}
                  onClick={() => { setSourceKind(s.id); setTransferPlan(null); }}
                >
                  {s.label}
                </button>
              ))}
            </div>
          </div>

          {sourceKind === "file" ? (
            <>
              <input ref={fileInputRef} type="file" accept=".json,.csv,.jsonl,.tsv" onChange={handleFileSelect} hidden />
              <div
                className={`df2-upload ${dragOver ? "drag-over" : ""}`}
                onClick={() => fileInputRef.current?.click()}
                onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
                onDragLeave={() => setDragOver(false)}
                onDrop={handleDrop}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fileInputRef.current?.click(); } }}
              >
                <div className="df2-upload-icon">
                  {uploading || analyzing ? <Spinner /> : <DtIcon name="upload" size={24} />}
                </div>
                <p className="df2-upload-title"><strong>Click to upload</strong> or drag and drop</p>
                <p className="df2-upload-hint">JSON, CSV, JSONL, TSV — any structured file</p>
              </div>
              {file && parsed && (
                <div className="df2-inline-meta">
                  <span className="df2-badge df2-badge-live"><DtIcon name="check" size={14} /> {file.name}</span>
                  <span>
                    {parsed.row_count.toLocaleString()} rows · {parsed.columns.length} columns
                  </span>
                </div>
              )}
            </>
          ) : dbSourceConnectors.length === 0 ? (
            <EmptyState
              icon="connectors"
              title="No database connectors"
              description="Add a PostgreSQL, MySQL, MongoDB, or warehouse connector first."
              compact
            />
          ) : (
            <div className="df2-form-row">
              <div className="df2-field df2-field-lg">
                <label className="df2-label">Source Connector</label>
                <select
                  className="df2-input df2-select"
                  value={sourceConnectorId}
                  onChange={(e) => setSourceConnectorId(e.target.value)}
                >
                  <option value="">Select connector…</option>
                  {dbSourceConnectors.map((c) => (
                    <option key={c.id} value={c.id}>{c.name} — {c.type}</option>
                  ))}
                </select>
              </div>
              <div className="df2-field df2-field-md">
                <label className="df2-label">
                  {sourceConnector?.type === "mongodb" ? "Collection" : "Table"}
                </label>
                <input
                  className="df2-input"
                  value={sourceConnector?.type === "mongodb" ? sourceCollection : sourceTable}
                  onChange={(e) => {
                    if (sourceConnector?.type === "mongodb") setSourceCollection(e.target.value);
                    else setSourceTable(e.target.value);
                  }}
                  placeholder={sourceConnector?.type === "mongodb" ? "orders" : "public.orders"}
                />
              </div>
            </div>
          )}
        </div>
        {sourceKind === "database" && dbSourceConnectors.length > 0 && (
          <div className="df2-card-footer df2-wizard-footer">
            <span style={{ fontSize: 13, color: "#64748b" }}>Select connector and table to continue</span>
            <button
              type="button"
              className="df2-btn df2-btn-primary"
              aria-disabled={!canConfigureDest}
              onClick={goToDestination}
            >
              Continue to Destination →
            </button>
          </div>
        )}
      </div>
      )}

      {step === 2 && sourceKind === "file" && (analysis || analyzing) && (
        <div className="df2-transfer-panel">
          {analyzing ? (
            <div className="df2-card df2-card-elevated">
              <div className="df2-card-body df2-analyzing">
                <Spinner />
                <p style={{ fontWeight: 600, margin: 0 }}>AI is analyzing your data…</p>
                <p style={{ fontSize: 13, color: "#64748b", margin: 0 }}>Pattern engine · RAG retrieval · semantic classification</p>
              </div>
            </div>
          ) : analysis ? (
            <>
              <div className="df2-segment" style={{ marginBottom: 16 }}>
                <span className="df2-badge df2-badge-beta">Quality {analysis.quality_score.toFixed(0)}%</span>
                <span className="df2-badge df2-badge-muted">{analysis.method}</span>
                {analysis.pii_columns.length > 0 && (
                  <span className="df2-badge df2-badge-run">{analysis.pii_columns.length} PII columns</span>
                )}
              </div>
              <MappingCanvas
                columns={analysis.columns}
                destinationLabel={destKindMode === "file_export" ? exportFormat.toUpperCase() : destType}
                targetTable={targetCollection || file?.name.replace(/\.[^/.]+$/, "")}
              />
              {step === 2 && (
                <div className="df2-studio-actions">
                  <button type="button" className="df2-btn" onClick={() => setStep(1)}>← Back</button>
                  <button type="button" className="df2-btn df2-btn-primary df2-btn-lg" onClick={goToDestination}>
                    Configure Destination →
                  </button>
                </div>
              )}
            </>
          ) : null}
        </div>
      )}

      {step === 3 && (
      <div className="df2-card df2-card-elevated df2-transfer-panel">
        <div className="df2-card-head">
          <div>
            <h3 className="df2-card-title">3. Configure Destination</h3>
            <p style={{ fontSize: 13, color: "#64748b", margin: 0 }}>Any database or warehouse — tables/collections created automatically</p>
          </div>
        </div>
        <div className="df2-card-body">
          <div className="df2-field">
            <label className="df2-label">Destination Mode</label>
            <div className="df2-segment">
              <button
                type="button"
                className={`df2-btn ${destKindMode === "database" ? "df2-btn-primary" : ""}`}
                onClick={() => { setDestKindMode("database"); setTransferPlan(null); }}
              >
                Database / Warehouse
              </button>
              <button
                type="button"
                className={`df2-btn ${destKindMode === "file_export" ? "df2-btn-primary" : ""}`}
                onClick={() => { setDestKindMode("file_export"); void loadTransferPlan(); }}
              >
                File Export
              </button>
            </div>
          </div>

          {destKindMode === "file_export" ? (
            <div className="df2-field">
              <label className="df2-label">Export Format</label>
              <div className="df2-segment">
                {EXPORT_FORMATS.map((f) => (
                  <button
                    key={f.id}
                    type="button"
                    className={`df2-btn ${exportFormat === f.id ? "df2-btn-primary" : ""}`}
                    onClick={() => { setExportFormat(f.id); setTransferPlan(null); }}
                  >
                    {f.label}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <>
          <div className="df2-field">
            <label className="df2-label">Destination Type</label>
            <div className="df2-segment">
              {DEST_TYPES.map((d) => (
                <button
                  key={d.id}
                  type="button"
                  className={`df2-btn ${destType === d.id ? "df2-btn-primary" : ""}`}
                  onClick={() => { setDestType(d.id); setConnectorId(""); }}
                >
                  {d.label}
                </button>
              ))}
            </div>
          </div>

          {destConnectors.length > 0 && (
            <div className="df2-field">
              <label className="df2-label" htmlFor="connector">Saved Connector</label>
              <select
                id="connector"
                className="df2-input df2-select"
                value={connectorId}
                onChange={(e) => setConnectorId(e.target.value)}
              >
                <option value="">Use connection settings below</option>
                {destConnectors.map((c) => (
                  <option key={c.id} value={c.id}>{c.name} — {c.host}:{c.port}</option>
                ))}
              </select>
            </div>
          )}

          {!connectorId && destType !== "mongodb" && destType !== "bigquery" && (
            <div className="df2-form-row">
              <div className="df2-field df2-field-md">
                <label className="df2-label">Host</label>
                <input className="df2-input" value={destHost} onChange={(e) => setDestHost(e.target.value)} />
              </div>
              <div className="df2-field df2-field-sm">
                <label className="df2-label">Port</label>
                <input type="number" className="df2-input" value={destPort} onChange={(e) => setDestPort(Number(e.target.value))} />
              </div>
              <div className="df2-field df2-field-140">
                <label className="df2-label">Username</label>
                <input className="df2-input" value={destUsername} onChange={(e) => setDestUsername(e.target.value)} />
              </div>
              <div className="df2-field df2-field-140">
                <label className="df2-label">Password</label>
                <input type="password" className="df2-input" value={destPassword} onChange={(e) => setDestPassword(e.target.value)} />
              </div>
              {destType === "snowflake" && (
                <div className="df2-field df2-field-160">
                  <label className="df2-label">Warehouse</label>
                  <input className="df2-input" value={destWarehouse} onChange={(e) => setDestWarehouse(e.target.value)} placeholder="COMPUTE_WH" />
                </div>
              )}
            </div>
          )}

          <div className="df2-form-row">
            <div className="df2-field df2-field-flex">
              <label className="df2-label" htmlFor="dest-db">
                {destType === "bigquery" ? "GCP Project ID" : "Database"}
              </label>
              <input id="dest-db" className="df2-input" value={targetDb} onChange={(e) => setTargetDb(e.target.value)} placeholder={destType === "bigquery" ? "my-gcp-project" : "test_db"} />
            </div>
            {destType === "bigquery" && (
              <div className="df2-field df2-field-flex">
                <label className="df2-label">Dataset</label>
                <input className="df2-input" value={destSchema} onChange={(e) => setDestSchema(e.target.value)} placeholder="dataflow" />
              </div>
            )}
            <div className="df2-field df2-field-flex">
              <label className="df2-label" htmlFor="dest-col">
                {destType === "mongodb" ? "Collection" : "Table"}
              </label>
              <input id="dest-col" className="df2-input" value={targetCollection} onChange={(e) => setTargetCollection(e.target.value)} placeholder={destType === "mongodb" ? "my_collection" : "my_table"} />
            </div>
            {destType === "postgresql" && (
              <div className="df2-field df2-field-120">
                <label className="df2-label">Schema</label>
                <input className="df2-input" value={destSchema} onChange={(e) => setDestSchema(e.target.value)} />
              </div>
            )}
          </div>
          {destType === "bigquery" && (
            <p style={{ fontSize: 13, color: "#64748b", marginTop: 8 }}>
              Set Database to GCP project ID. Optional: save service account JSON path as connection string in connector settings.
            </p>
          )}
            </>
          )}

          <div className="df2-policy-console">
            <div className="df2-policy-head">
              <div>
                <span className="df2-rail-kicker">Connection Setup</span>
                <h4>Sync and schema contract</h4>
              </div>
              <span className={`df2-badge ${streamNeedsReview ? "df2-badge-run" : "df2-badge-live"}`}>
                {currentSourceColumns.length ? (streamNeedsReview ? "Review required" : "Ready") : "Waiting for schema"}
              </span>
            </div>

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
                <div className="df2-segment">
                  {VALIDATION_MODES.map((mode) => (
                    <button
                      key={mode.id}
                      type="button"
                      className={`df2-btn ${validationMode === mode.id ? "df2-btn-primary" : ""}`}
                      onClick={() => setValidationMode(mode.id)}
                      title={`Mapping confidence threshold ${mode.threshold}`}
                    >
                      {mode.label}
                    </button>
                  ))}
                </div>
              </div>
              <label className="df2-policy-toggle">
                <input
                  type="checkbox"
                  checked={backfillNewFields}
                  onChange={(e) => setBackfillNewFields(e.target.checked)}
                />
                <span>
                  <strong>Backfill new fields</strong>
                  <small>Requires automatic column propagation</small>
                </span>
              </label>
            </div>

            <div className="df2-stream-contract">
              <div className="df2-stream-head">
                <strong>Streams and fields</strong>
                <span>{currentSourceColumns.length} discovered fields</span>
              </div>
              <div className="df2-stream-table-wrap">
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
          </div>

          {transferPlan && (
            <div className="df2-plan-callout">
              <p style={{ fontWeight: 500, margin: "0 0 8px" }}>
                Auto-create plan · {transferPlan.operation}
                {!transferPlan.supported && (
                  <span className="df2-badge df2-badge-run" style={{ marginLeft: 8 }}>{transferPlan.message}</span>
                )}
              </p>
              <ul style={{ fontSize: 13, color: "#64748b", margin: 0, paddingLeft: 18 }}>
                {transferPlan.auto_create.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
              {transferPlan.type_mappings.length > 0 && (
                <p style={{ fontSize: 12, color: "#94a3b8", marginTop: 8, marginBottom: 0 }}>
                  {transferPlan.type_mappings.length} column type mappings (string → native DDL)
                </p>
              )}
            </div>
          )}
        </div>
        <div className="df2-card-footer df2-wizard-footer">
          <button type="button" className="df2-btn" onClick={() => setStep(sourceKind === "file" ? 2 : 1)}>← Back</button>
          <div className="df2-segment">
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
            onClick={goToPreflight}
            disabled={preflighting}
          >
            {preflighting ? <ButtonLoader label="Running Preflight…" /> : <><DtIcon name="gate" size={18} /> Run Preflight</>}
          </button>
          </div>
        </div>
      </div>
      )}

      {step >= 4 && (preflight || preflighting) && (
        <div className="df2-transfer-panel">
          <PreflightTimeline
            result={preflight ?? {
              passed: false,
              passed_count: 0,
              total_gates: 8,
              readiness_score: 0,
              gates: [],
              blockers: [],
            }}
            running={preflighting}
          />
          {preflight?.passed && (
            <div className="df2-studio-actions">
              <button type="button" className="df2-btn" onClick={() => setStep(3)}>← Back</button>
              <button
                type="button"
                className="df2-btn df2-btn-primary df2-btn-lg"
                onClick={() => { setStep(5); void executeTransfer(); }}
                disabled={transferring}
              >
                {transferring ? <ButtonLoader label="Transferring…" /> : <><DtIcon name="transfer" size={18} /> Execute Transfer</>}
              </button>
            </div>
          )}
        </div>
      )}

      {step >= 5 && activeJobId && (
        <div className="df2-transfer-panel">
          <JobTheater
            jobId={activeJobId}
            sourceLabel={file?.name || sourceConnector?.name}
            destLabel={`${targetDb}.${targetCollection}`}
            sourceType={sourceKind === "file" ? "file" : sourceConnector?.type || "database"}
            destType={destKindMode === "file_export" ? exportFormat : destType}
            onComplete={handleJobComplete}
            onFailed={handleJobComplete}
          />
        </div>
      )}

      {step >= 5 && result && !activeJobId && (
        <div className={`df2-result-banner df2-transfer-panel ${result.success ? "success" : "error"}`}>
          {result.success ? (
            <div>
              <span className="df2-badge df2-badge-live" style={{ marginBottom: 12, display: "inline-flex" }}><DtIcon name="check" size={14} /> Transfer Complete</span>
              <p style={{ fontWeight: 600, margin: "0 0 4px" }}>{result.records_transferred?.toLocaleString()} records transferred</p>
              {result.destination?.path && (
                <p style={{ fontSize: 13, color: "#64748b", margin: 0 }}>Exported to {result.destination.path}</p>
              )}
              {result.ddl_executed && result.ddl_executed.length > 0 && (
                <ul style={{ fontSize: 13, color: "#64748b", marginTop: 8, marginBottom: 0, paddingLeft: 18 }}>
                  {result.ddl_executed.map((d) => <li key={d}>{d}</li>)}
                </ul>
              )}
            </div>
          ) : (
            <span className="df2-badge df2-badge-error"><DtIcon name="x" size={14} /> {result.error || "Transfer failed"}</span>
          )}
        </div>
      )}
      </main>
      <aside className="df2-assurance-rail" aria-label="Transfer assurance">
        <div className="df2-rail-panel">
          <div className="df2-rail-head">
            <DtIcon name="shield" size={18} />
            <div>
              <h3>Assurance</h3>
              <p>Controls and proof points</p>
            </div>
          </div>
          <div className="df2-rail-stage-list">
            {assuranceStages.map((stage) => (
              <div key={stage.label} className={`df2-rail-stage ${stage.tone}`}>
                <span>{stage.label}</span>
                <strong>{stage.value}</strong>
              </div>
            ))}
          </div>
        </div>

        <div className="df2-rail-panel">
          <div className="df2-rail-kicker">Algorithms</div>
          <div className="df2-algorithm-list">
            <div><strong>Optimal matching</strong><span>global one-to-one assignment</span></div>
            <div><strong>Semantic graph</strong><span>lexicon, patterns, samples</span></div>
            <div><strong>Type pivot</strong><span>lossless native DDL mapping</span></div>
            <div><strong>Validation</strong><span>dry-run, gates, reconciliation</span></div>
          </div>
        </div>

        <div className="df2-rail-panel">
          <div className="df2-rail-kicker">Run Contract</div>
          <div className="df2-rail-split">
            <span>Sync</span>
            <strong>{syncModeLabel}</strong>
          </div>
          <div className="df2-rail-split">
            <span>Schema</span>
            <strong>{schemaPolicyLabel}</strong>
          </div>
          <div className="df2-rail-split">
            <span>Validation</span>
            <strong>{validationMode}</strong>
          </div>
          {streamNeedsReview && (
            <div className="df2-rail-alert">
              <DtIcon name="alert" size={14} />
              <span>{requiresCursor && !cursorField ? "Cursor field required. " : ""}{requiresPrimaryKey && !primaryKeyField ? "Primary key required." : ""}</span>
            </div>
          )}
        </div>

        {analysis && (
          <div className="df2-rail-panel">
            <div className="df2-rail-kicker">Mapping Health</div>
            <div className="df2-rail-meter">
              <div style={{ width: `${Math.min(analysis.quality_score, 100)}%` }} />
            </div>
            <div className="df2-rail-split">
              <span>{highConfidenceColumns} high confidence</span>
              <span>{reviewColumns} review</span>
            </div>
            {piiColumns > 0 && (
              <div className="df2-rail-alert">
                <DtIcon name="shield" size={14} />
                <span>{piiColumns} PII columns governed</span>
              </div>
            )}
          </div>
        )}

        {preflight && (
          <div className="df2-rail-panel">
            <div className="df2-rail-kicker">Preflight</div>
            <div className="df2-rail-split">
              <span>Passed</span>
              <strong>{preflight.passed_count}/{preflight.total_gates}</strong>
            </div>
            {preflight.blockers.slice(0, 2).map((b) => (
              <div className="df2-rail-alert block" key={b.id}>
                <DtIcon name="alert" size={14} />
                <span>{b.message}</span>
              </div>
            ))}
          </div>
        )}

        {result?.success && (
          <div className="df2-rail-panel">
            <div className="df2-rail-kicker">Final Proof</div>
            <div className="df2-rail-split">
              <span>Rows written</span>
              <strong>{result.records_transferred?.toLocaleString() ?? "0"}</strong>
            </div>
            <div className="df2-rail-split">
              <span>Rejected</span>
              <strong>{rejectedRows.toLocaleString()}</strong>
            </div>
            {result.reconciliation?.message && (
              <p className="df2-rail-note">{result.reconciliation.message}</p>
            )}
          </div>
        )}
      </aside>
      </div>
    </PageShell>
  );
}
