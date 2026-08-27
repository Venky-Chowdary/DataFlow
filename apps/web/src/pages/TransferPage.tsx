import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { JobTheater } from "../components/JobTheater";
import { DtIcon } from "../components/DtIcon";
import { EmptyState } from "../components/ui/EmptyState";
import { ConnectorIcon } from "../app/brand-icons";
import { ConnectorSelect } from "../components/ui/ConnectorSelect";
import { FilterTabs } from "../components/ui/FilterTabs";
import { SourceKindTiles, type SourceKind } from "../components/ui/SourceKindTiles";
import { StructurePreview } from "../components/ui/StructurePreview";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { WizardSteps } from "../components/ui/WizardSteps";
import { ButtonLoader, LoadingBlock, Spinner } from "../components/LoadingState";
import { useToast } from "../components/Toast";
import { PERMISSIONS, useWriteGate } from "../lib/PermissionsContext";
import { PermissionNotice } from "../components/PermissionNotice";
import { TransferMapStep } from "./transfer/TransferMapStep";
import { DestinationPicker } from "../components/transfer/DestinationPicker";
import { DestProcedurePanel, type DestWriteMode } from "../components/transfer/DestProcedurePanel";
import { SqlEditor } from "../components/ui/SqlEditor";
import { DestinationAdvancedDrawer } from "../components/transfer/DestinationAdvancedDrawer";
import { ObjectNameCombobox } from "../components/transfer/ObjectNameCombobox";
import { Button } from "../components/ui/Button";
import { HiddenFileInput } from "../components/ui/HiddenFileInput";
import { SourceStepAside } from "../components/transfer/SourceStepAside";
import { ValidateActionsRail } from "../components/transfer/ValidateActionsRail";
import { ContractBindField } from "../components/contracts/ContractBindField";
import { contractBindFromPolicies } from "../lib/contractBind";
import { destExistsPrimaryCta, shapeContractFromPreflight } from "../lib/destExistsShape";
import { launchStageState, stagePercent } from "../lib/progressRing";
import { ValidateDashboard, type RemediationOpResult } from "../components/transfer/ValidateDashboard";
import { TransferResultDashboard } from "../components/transfer/TransferResultDashboard";
import { TransferRouteBar } from "../components/transfer/TransferRouteBar";
import {
  MappingProofDrawer,
  mergeMappingProof,
} from "../components/MappingProofDrawer";
import { useActiveData } from "../lib/DataContext";
import { useStudioActions, type StudioAction } from "../lib/StudioActionsContext";
import { readSession } from "../lib/session";
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
  fetchJob,
  fetchTransferCapabilities,
  introspectTransferEndpoints,
  mapTransferColumns,
  mapTransferPlan,
  preflightTransferPlan,
  isTransportFailure,
  validateTransportMessage,
  previewQuarantineCells,
  resumeJob,
  runPreflight,
  runUniversalTransfer,
  syncTransferPlanMappings,
  updateTransferPlan,
  uploadFile,
  fetchVectorRouting,
  fetchEmbeddingCacheStats,
  clearEmbeddingCache,
  type CellPreviewResult,
  type EmbeddingCacheStats,
  type VectorFieldRouting,
  type VectorRoutingPlan,
} from "../lib/api";
import type { ValidateProgress } from "../lib/engineProgress";
import { CdcRetentionPanel } from "../components/transfer/CdcRetentionPanel";
import {
  defaultSchemaForDriver,
  foldSchemaForDriver,
} from "../lib/dialectDefaults";
import { defaultPortForType, getConnectorDefaults, getGenericSqlGroup, getGenericSqlPlaceholder, isGenericSql, isTransferLiveType, resolveDriverType, setTransferLiveDrivers } from "../lib/connectorTypes";
import {
  bindNamesFromSql,
  callableSourceExtra,
  dialectOffersProcedures,
  dialectOffersQuery,
  dialectOffersSqlExtract,
  destWriteReady,
  isCallableDestMode,
  isCallableSourceMode,
  procedureHint,
  procedureStreamName,
  queryHint,
  sourceExtractReady,
  type SourceReadMode,
} from "../lib/sourceReadMode";
import {
  describeDestRoute,
  destRouteKey,
  runResultDescribesRoute,
} from "../lib/runRouteScope";
import { diagnoseSql } from "../lib/sqlEditorModel";
import {
  availableSyncModes,
  DATE_LOCALES,
  NUMBER_LOCALES,
  PREFLIGHT_SAMPLE_LIMIT,
  SCHEMA_POLICIES,
  SYNC_MODES,
  VALIDATION_MODES,
  type DateLocaleId,
  type NumberLocaleId,
  type SchemaPolicyId,
  type SyncModeId,
  type ValidationModeId,
  multiStreamScd2MirrorBlockCopy,
  MULTI_STREAM_SCD2_MIRROR_BLOCK,
  schemaPolicyHonestyLine,
  syncModeHonestyLine,
} from "../lib/transferConstants";
import {
  jobStudioDataRules,
  namedStudioSchemaPolicy,
  namedStudioValidationMode,
  schemaPolicyBackfills,
  studioSchedulePolicies,
} from "../lib/studioDataRules";
import { buildValidateContractKey as composeValidateContractKey } from "../lib/studioValidateIdentity";
import {
  CDC_DELIVERY_AT_LEAST_ONCE,
  exactlyOnceWiredDest,
  jobStudioDeliveryGuarantee,
  namedCdcDeliveryGuarantee,
  studioDeliveryGuarantee,
  type CdcDeliveryGuarantee,
} from "../lib/cdcExactlyOnce";
import { isJobSuccess } from "../lib/uiUtils";
import {
  parseStreamNames,
  primaryStreamName,
  type StreamSchemaPreview,
} from "../lib/sourceStreams";
import {
  approveMappingsHonestly,
  confirmFalseFriendsBySource,
  buildPreflightMappings,
  confidenceThresholdForMode,
  editableFromPipelineMappings,
  markMappingDestUnread,
  ENGINE_TO_UI_TRANSFORM,
  engineTransformToUi,
  isEnumToBooleanConflict,
  mappingRequiresRiskAck,
  mergeSignedRiskContracts,
  mergeStampedTargetTypes,
  uiTransformToEngine,
  widenMappingToVarchar,
  mappingsFromAnalysis,
  type EditableMapping,
  type MappingTransform,
} from "../lib/mapping";
import {
  carryOperatorDecisions,
  holdOutRowsAndContinue,
} from "../lib/mappingDecisions";
import {
  Connector,
  EnhancedAnalysis,
  ParsedUpload,
  PreflightResult,
  SourceReadOptions,
  TransferPlan,
  TransferResult,
  JobProgress,
  ValidationSuggestedAction,
} from "../lib/types";
import { standingAcknowledgmentReason } from "../lib/acknowledgments";
import {
  DEST_PROBE_FAILURE_COOLDOWN_MS,
  DEST_PROBE_TIMEOUT_MS,
  destProbeSpeedClass,
  shouldSkipAutoDestProbe,
} from "../lib/destProbeTimeout";
import {
  destCatalogExists,
  destProbeSettled,
  sameColumnList,
  sameSchemaMap,
} from "../lib/destSchemaIdentity";
import {
  SourceReadWindowPanel,
  offersReadWindow,
} from "../components/transfer/SourceReadWindowPanel";
import { parseCsvTextForPreview } from "../lib/csvPreview";
import { runLocalFileExport } from "../lib/localFileExport";
import { isApiPreflight, runLocalPreflight } from "../lib/localPreflight";
import { readJobEventLog } from "../lib/jobEventLog";
import { destHeadline } from "../lib/conservationLedger";
import { schemaIntrospectionFailureMessage } from "../lib/preflightMessages";
import {
  buildDisplayBlockers,
  findDuplicateKeyRoot,
  isEncodingIntegritySignal,
  rankAndDedupeSuggestedActions,
} from "../lib/validateIssueGrouping";
import { schemaDriftRequiresRemap } from "../lib/validateHonestyControls";
import {
  promoteBlockedPrimaryFix,
  resolveValidateStudioPrimary,
  type PromotedPrimaryFix,
} from "../lib/validateStudioPrimary";
import { planFkOrphanSuggestedAction, resolvePopulationOrphanScanFlag } from "../lib/fkOrphanCta";
import { createNewFitWidenActions, createNewFitWidenLabel } from "../lib/populationFit";
import { suggestUniqueKeyCandidates, suggestCompositeUniqueKeyCandidates } from "../lib/uniqueKeySuggestions";
import { needsMappingReview } from "../lib/columnWorkbench";
import {
  buildStreamContracts,
  firstStreamContractIssue,
  seedStreamFieldsFromCandidates,
  type StreamFieldContract,
} from "../lib/streamContracts";
import type { TransferPageProps } from "./transfer/TransferPageProps";
import {
  ACCEPTED_UPLOAD_EXTENSIONS,
  CLOUD_SOURCE_TYPES,
  FALLBACK_DEST_TYPES,
  FALLBACK_EXPORT_FORMATS,
  FILE_FORMAT_SOURCE_TYPES,
  MAX_UPLOAD_BYTES,
  RUN_LAUNCH_STAGES,
  STEP_DESTINATION,
  STEP_MAP,
  STEP_RUN,
  STEP_SHAPE,
  STEP_SOURCE,
  STEP_VALIDATE,
  STEPS,
  UPLOAD_FORMATS,
} from "./transfer/studioConstants";
import { TransferTransformStep } from "./transfer/TransferTransformStep";
import { recipePayload, type ShapeStepWire, type TransformImage } from "../lib/shape";
import {
  analysisFromPipeline,
  fileExtension,
  findColumn,
  formatFileSize,
  sealRemediationApproval,
} from "./transfer/studioHelpers";

type SyncMode = SyncModeId;
type SchemaPolicy = SchemaPolicyId;
type ValidationMode = ValidationModeId;

export function TransferPage({
  connectors,
  connectorsLoading = false,
  onTransferComplete,
  onOpenSchedules,
  onOpenContracts,
  onFreshTransfer,
  seedSourceConnector = null,
  seedStudioIntent = null,
}: TransferPageProps) {
  const { toast } = useToast();
  const jobRun = useWriteGate(PERMISSIONS.jobRun);
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
  const [sourceReadMode, setSourceReadMode] = useState<SourceReadMode>("table");
  const [procedureCall, setProcedureCall] = useState("");
  const [procedureParams, setProcedureParams] = useState<Record<string, string>>({});
  const [destWriteMode, setDestWriteMode] = useState<DestWriteMode>("table");
  const [destProcedureCall, setDestProcedureCall] = useState("");
  const [destQuerySql, setDestQuerySql] = useState("");
  const [destProcedureParams, setDestProcedureParams] = useState<Record<string, string>>({});
  const [destProcedureBefore, setDestProcedureBefore] = useState("");
  const [destProcedureAfter, setDestProcedureAfter] = useState("");
  const [destProcedureParamMap, setDestProcedureParamMap] = useState<Record<string, string>>({});
  const [shapeContract, setShapeContract] = useState<{
    shape?: string;
    extra_source_columns?: string[];
    headline?: string;
    primary_action?: string;
    unaccounted_sources?: string[];
  } | null>(null);
  /**
   * The recipe composed on the Transform (pre-load) step, and the identity the engine
   * gave it. The hash is what Execute is held to: if the recipe changes after
   * Validate, the run is refused rather than quietly running a different one.
   */
  const [shapeSteps, setShapeSteps] = useState<ShapeStepWire[]>([]);
  const [shapeIdentity, setShapeIdentity] = useState<TransformImage | null>(null);
  /**
   * Read inside `applyPipelineMappings` without making the transformed image a
   * dependency of it: Map must ask about the transformed image, but re-running
   * every mapping on each preview keystroke is not what the operator asked for.
   */
  const shapeIdentityRef = useRef<TransformImage | null>(null);
  shapeIdentityRef.current = shapeIdentity;
  /** Sample rows from the last connector introspect, for Transform profile/preview. */
  const [connectorSampleRows, setConnectorSampleRows] = useState<Record<string, unknown>[]>([]);
  const [cloudPath, setCloudPath] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [advancedLocaleFocus, setAdvancedLocaleFocus] = useState<"date" | "number" | null>(null);
  /** Shared Fix-bad-data drawer open state (Validate dashboard + rail Fix CTA). */
  const [badDataFixOpen, setBadDataFixOpen] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [parsed, setParsed] = useState<ParsedUpload | null>(null);
  /** Opt-in Tesseract OCR for scanned/image-only PDFs. */
  const [enableOcr, setEnableOcr] = useState(false);
  /** Declared source read window — the same one the run reads and reconciles. */
  const [readOptions, setReadOptions] = useState<SourceReadOptions>({});
  const [ocrStatus, setOcrStatus] = useState<{ available?: boolean; message?: string } | null>(null);
  const [sourceRowEstimate, setSourceRowEstimate] = useState<number | null>(null);
  const [analysis, setAnalysis] = useState<EnhancedAnalysis | null>(null);
  const [preflight, setPreflight] = useState<PreflightResult | null>(null);
  /** Last transport / start failure — Validate must not look "Not run". */
  const [preflightError, setPreflightError] = useState("");
  /** Fingerprint of Map/sync/PK that produced the current preflight result. */
  const [validatedContractKey, setValidatedContractKey] = useState<string | null>(null);
  /** Destination route of the run on screen. */
  const [executedRouteKey, setExecutedRouteKey] = useState<string | null>(null);
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
  const destSchemaGenRef = useRef(0);
  /** Last destination identity whose probe failed — throttles automatic re-probes. */
  const destProbeFailureRef = useRef<{ key: string; at: number } | null>(null);
  /**
   * Probes in flight. "Reading destination…" follows this count, not the newest
   * generation: a superseded probe's cleanup used to be skipped, so overlapping
   * probes could leave the control disabled after every request had settled.
   */
  const destProbeInflightRef = useRef(0);
  /** Re-arms the unreachable-destination re-check timer. */
  const [destProbeRetryTick, setDestProbeRetryTick] = useState(0);
  /** Last table key that successfully owned ``destColumns`` — blocks cross-table stickiness. */
  const destSchemaTableKeyRef = useRef("");
  const lastNewTableToastRef = useRef("");
  const [preflighting, setPreflighting] = useState(false);
  const [validateProgress, setValidateProgress] = useState<ValidateProgress | null>(null);
  const [savingContract, setSavingContract] = useState(false);
  const [boundContractId, setBoundContractId] = useState("");
  const [requireSignedContract, setRequireSignedContract] = useState(false);
  const [contractBlockReason, setContractBlockReason] = useState("");
  const [dragOver, setDragOver] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  /** True when the last error refused a declared read window, not the file. */
  const [readWindowRefused, setReadWindowRefused] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [connectorId, setConnectorId] = useState("");
  /** Empty until the operator picks a destination — never default to MongoDB. */
  const [destType, setDestType] = useState<string>("");
  const [destKindMode, setDestKindMode] = useState<"database" | "file_export">("database");
  const destDriverType = destType ? resolveDriverType(destType) : "";
  // Map must tell the engine where the source types came from. Without this the
  // pipeline treated a warehouse's declared DDL as a guess and let sample
  // profiling re-infer it, so the same Snowflake DATE column projected DATE on
  // one Map and DATETIME(6) on the next depending on how the sample rendered.
  const mapSourceKind = sourceKind === "cloud" ? "object_store" : sourceKind;
  const destSelected = destKindMode === "file_export" || Boolean(destType);
  const [exportFormat, setExportFormat] = useState("json");
  const [transferPlan, setTransferPlan] = useState<TransferPlan | null>(null);
  const [persistedPlanId, setPersistedPlanId] = useState<string | null>(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [targetDb, setTargetDb] = useState("dataflow_test");
  const [targetCollection, setTargetCollection] = useState("");
  const [destHost, setDestHost] = useState("");
  const [destPort, setDestPort] = useState(0);
  const [destSchema, setDestSchema] = useState("");
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
  const [validationMode, setValidationMode] = useState<ValidationMode>("strict");
  const [complianceAcknowledged, setComplianceAcknowledged] = useState(false);
  const [schemaDriftAcknowledged, setSchemaDriftAcknowledged] = useState(false);
  const [fkRiskAcknowledged, setFkRiskAcknowledged] = useState(false);
  /** Module 16 — opt-in population orphan scan (only path to RI proven). */
  const [runPopulationOrphanScan, setRunPopulationOrphanScan] = useState(false);
  const [dateLocale, setDateLocale] = useState<DateLocaleId>("");
  const [numberLocale, setNumberLocale] = useState<NumberLocaleId>("");
  const [backfillNewFields, setBackfillNewFields] = useState(false);
  const [writeViaStaging, setWriteViaStaging] = useState(false);
  const [vectorContentColumn, setVectorContentColumn] = useState("");
  const [vectorEmbeddingColumn, setVectorEmbeddingColumn] = useState("");
  const [vectorMetadataColumns, setVectorMetadataColumns] = useState("");
  const [vectorEmbeddingModel, setVectorEmbeddingModel] = useState("");
  const [vectorChunkSize, setVectorChunkSize] = useState(512);
  const [vectorChunkOverlap, setVectorChunkOverlap] = useState(50);
  const [vectorExcludePiiColumns, setVectorExcludePiiColumns] = useState("");
  const [vectorRoutingFields, setVectorRoutingFields] = useState<VectorFieldRouting[]>([]);
  const [vectorRoutingLoading, setVectorRoutingLoading] = useState(false);
  const [vectorDurableCache, setVectorDurableCache] = useState(true);
  const [embeddingCacheStats, setEmbeddingCacheStats] = useState<EmbeddingCacheStats | null>(null);
  const [embeddingCacheBusy, setEmbeddingCacheBusy] = useState(false);
  const [cursorField, setCursorField] = useState("");
  // What the cursor column means in the source. Only ever set by the operator:
  // a column name cannot establish whether the source moves the value when a
  // row changes, and assuming it silently loses updates and backdated inserts.
  const [cursorSemantics, setCursorSemantics] = useState("");
  const [primaryKeyField, setPrimaryKeyField] = useState("");
  const [priorityColumn, setPriorityColumn] = useState("");
  const [priorityDirection, setPriorityDirection] = useState<"asc" | "desc">("desc");
  const [rowLimit, setRowLimit] = useState(0);
  const [snapshotMode, setSnapshotMode] = useState("initial");
  const [deliveryGuarantee, setDeliveryGuarantee] = useState<CdcDeliveryGuarantee>(
    CDC_DELIVERY_AT_LEAST_ONCE,
  );
  const [allowAppendOnly, setAllowAppendOnly] = useState(false);
  const [multiSubnetFailover, setMultiSubnetFailover] = useState(false);
  /** SQL Server CDC TVF row filter: all | all update old | net. */
  const [cdcRowFilter, setCdcRowFilter] = useState<"all" | "all update old" | "net">("all");
  /** Per-stream cursor/PK when source lists multiple tables (comma-separated). */
  const [streamFields, setStreamFields] = useState<Record<string, StreamFieldContract>>({});
  const [columnMappings, setColumnMappings] = useState<EditableMapping[]>([]);
  // Regeneration (step change, dest schema reload) must not wipe operator
  // approvals — carryOperatorDecisions replays them by decision fingerprint.
  const columnMappingsRef = useRef<EditableMapping[]>([]);
  columnMappingsRef.current = columnMappings;
  /** Per-stream column mappings when source lists multiple tables. */
  const [streamMappings, setStreamMappings] = useState<Record<string, EditableMapping[]>>({});
  const [mapActiveStream, setMapActiveStream] = useState<string | null>(null);
  const [mapStreamBusy, setMapStreamBusy] = useState<string | null>(null);
  const [destColumns, setDestColumns] = useState<string[]>([]);
  const [destSchemaMap, setDestSchemaMap] = useState<Record<string, string>>({});
  const [destSchemaLoading, setDestSchemaLoading] = useState(false);
  /** Table/collection names from the last destination introspect (picker + exists check). */
  const [destObjectNames, setDestObjectNames] = useState<string[]>([]);
  const [destTableExists, setDestTableExists] = useState<boolean | null>(null);
  /**
   * Last existence verdict the destination catalog actually returned.
   *
   * `setDestTableExists` does not flush before the plan payload is rebuilt, so a
   * probe that just proved the table absent was still persisted as "unknown" —
   * Map then mapped against `null`, left every `target_type` empty and printed
   * `T → T loses fidelity` instead of a CREATE.  Written synchronously with the
   * state so both readers see the same fact within one tick.
   */
  const destTableExistsRef = useRef<boolean | null>(null);
  const proveDestTableExists = useCallback((verdict: boolean | null) => {
    destTableExistsRef.current = verdict;
    setDestTableExists(verdict);
  }, []);
  const [destConnected, setDestConnected] = useState<boolean | null>(null);
  const [destConnectionError, setDestConnectionError] = useState<string>("");
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
  const [mapFocusSource, setMapFocusSource] = useState<string | null>(null);
  const [mapIdentityBanner, setMapIdentityBanner] = useState<string | null>(null);
  const [seedRepairProposalId, setSeedRepairProposalId] = useState<string | null>(null);
  const appliedStudioIntentTokenRef = useRef<number | null>(null);
  const [runStartupProgress, setRunStartupProgress] = useState(0);
  const [runStartupPhase, setRunStartupPhase] = useState<string>(RUN_LAUNCH_STAGES[0]);

  const confidenceThreshold = confidenceThresholdForMode(validationMode);
  const mappingReviewCount = columnMappings.filter((m) =>
    needsMappingReview(m, confidenceThreshold),
  ).length;
  const riskAckPendingCount = columnMappings.filter(
    (m) => mappingRequiresRiskAck(m) && !m.riskAcknowledged,
  ).length;
  const duplicateKeyRoot = useMemo(
    () => findDuplicateKeyRoot(preflight, syncMode),
    [preflight, syncMode],
  );

  const buildSourceSamples = useCallback((): Record<string, string[]> => {
    // Sampled values are evidence for the mapping engine, so once a Transform
    // (pre-load) recipe is declared they must be the transformed values: sending
    // the raw ones would let the engine re-infer the carrier the transform just
    // replaced.
    const image = shapeIdentityRef.current;
    if (image && image.columns.length && image.sampleRows.length) {
      const shaped: Record<string, string[]> = {};
      for (const col of image.columns) {
        shaped[col] = image.sampleRows
          .slice(0, 8)
          .map((r) => String(r[col] ?? ""))
          .filter((v) => v.length > 0);
      }
      return shaped;
    }
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
    setPreflightError("");
    setValidatedContractKey(null);
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
    const provenDestTableExists = destTableExists ?? destTableExistsRef.current;
    const isMongo = destDriverType === "mongodb";
    const isDynamo = destDriverType === "dynamodb";
    const isIceberg = destDriverType === "iceberg";
    const isVector =
      destDriverType === "pgvector" ||
      destDriverType === "qdrant" ||
      destDriverType === "weaviate" ||
      destDriverType === "pinecone" ||
      destDriverType === "milvus";
    const metadataCols = vectorMetadataColumns
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    return {
      kind: "database",
      format: destType,
      connector_id: connectorId || undefined,
      host: isIceberg ? "" : destHost,
      port: isIceberg ? 0 : destPort,
      // Iceberg: warehouse path prefers connection_string; schema holds namespace.
      // Do not put namespace into database — `_warehouse_root` would treat it as the lake path.
      database: isDynamo
        ? (targetCollection || targetDb)
        : isIceberg
          ? undefined
          : targetDb,
      schema: isIceberg ? (destSchema || undefined) : destSchema,
      table: isMongo ? undefined : targetCollection || undefined,
      collection: isMongo ? targetCollection : undefined,
      username: isIceberg ? undefined : (destUsername || undefined),
      password: isIceberg ? undefined : (destPassword || undefined),
      connection_string: isIceberg
        ? (destConnectionString || undefined)
        : (destConnectionString || undefined),
      warehouse: destDriverType === "snowflake" ? destWarehouse : undefined,
      auth_source: selectedDestConnector?.auth_source || undefined,
      auth_mode: selectedDestConnector?.auth_mode || undefined,
      auth_role: selectedDestConnector?.auth_role || undefined,
      api_key: selectedDestConnector?.api_key || undefined,
      service_account: selectedDestConnector?.service_account || undefined,
      // Dual-write: nested extra is SSOT for EndpointConfig / writers; flat root
      // keeps transfer-plan Map/preflight readers that still look at dest.table_exists.
      // `?? ref` only fills the *unknown* case, so a just-proven verdict is never
      // downgraded to unknown by an unflushed setState.
      ...(provenDestTableExists === null || provenDestTableExists === undefined
        ? {}
        : { table_exists: provenDestTableExists }),
      ...(provenDestTableExists === true && Object.keys(destSchemaMap).length
        ? { schema_types: destSchemaMap }
        : {}),
      extra: {
        ...(provenDestTableExists === null || provenDestTableExists === undefined
          ? {}
          : { table_exists: provenDestTableExists }),
        ...(provenDestTableExists === true && Object.keys(destSchemaMap).length
          ? { schema_types: destSchemaMap }
          : {}),
        ...(syncMode === "cdc" && allowAppendOnly ? { allow_append_only: true } : {}),
        ...(isVector
          ? {
              ...(vectorContentColumn ? { content_column: vectorContentColumn } : {}),
              ...(vectorEmbeddingColumn ? { embedding_column: vectorEmbeddingColumn } : {}),
              ...(metadataCols.length ? { metadata_columns: metadataCols } : {}),
              ...(vectorExcludePiiColumns
                .split(",")
                .map((s) => s.trim())
                .filter(Boolean).length
                ? {
                    exclude_pii_columns: vectorExcludePiiColumns
                      .split(",")
                      .map((s) => s.trim())
                      .filter(Boolean),
                  }
                : {}),
              ...(vectorEmbeddingModel ? { embedding_model: vectorEmbeddingModel } : {}),
              chunk_size: vectorChunkSize,
              chunk_overlap: vectorChunkOverlap,
              durable_embedding_cache: vectorDurableCache,
            }
          : {}),
        ...(destWriteMode === "procedure"
          ? {
              dest_write_mode: "procedure",
              dest_procedure_call: destProcedureCall.trim(),
              ...(Object.keys(destProcedureParamMap).length
                ? { dest_procedure_param_map: destProcedureParamMap }
                : {}),
              ...(Object.keys(destProcedureParams).length
                ? { dest_procedure_params: destProcedureParams }
                : {}),
            }
          : destWriteMode === "query"
            ? {
                dest_write_mode: "query",
                dest_query_sql: destQuerySql.trim(),
                ...(Object.keys(destProcedureParamMap).length
                  ? { dest_procedure_param_map: destProcedureParamMap }
                  : {}),
                ...(Object.keys(destProcedureParams).length
                  ? { dest_procedure_params: destProcedureParams }
                  : {}),
              }
            : {}),
        ...(destProcedureBefore.trim()
          ? { dest_procedure_before: destProcedureBefore.trim() }
          : {}),
        ...(destProcedureAfter.trim()
          ? { dest_procedure_after: destProcedureAfter.trim() }
          : {}),
      },
    };
  };

  const isVectorDest =
    destDriverType === "pgvector" ||
    destDriverType === "qdrant" ||
    destDriverType === "weaviate" ||
    destDriverType === "pinecone" ||
    destDriverType === "milvus";

  const writeViaStagingSupported = [
    "postgresql", "mysql", "sqlite", "sqlserver", "mssql", "oracle",
    "snowflake", "redshift", "bigquery", "duckdb", "generic_sql",
  ].includes(destDriverType);

  useEffect(() => {
    if (!writeViaStagingSupported && writeViaStaging) {
      setWriteViaStaging(false);
    }
  }, [writeViaStagingSupported, writeViaStaging]);

  useEffect(() => {
    if (
      deliveryGuarantee === "exactly_once"
      && !exactlyOnceWiredDest(destDriverType || destType)
    ) {
      setDeliveryGuarantee("at_least_once");
    }
  }, [deliveryGuarantee, destDriverType, destType]);

  const applyVectorRoutingPlan = (plan: VectorRoutingPlan) => {
    if (plan.content_column) setVectorContentColumn(plan.content_column);
    if (plan.embedding_column) setVectorEmbeddingColumn(plan.embedding_column);
    if (plan.metadata_columns?.length) {
      setVectorMetadataColumns(plan.metadata_columns.join(","));
    }
    setVectorExcludePiiColumns((plan.exclude_pii_columns || []).join(","));
    setVectorRoutingFields(plan.fields || []);
  };

  const runVectorRouting = async (autoApply: boolean) => {
    const cols =
      analysis?.columns?.map((c) => c.column_name).filter(Boolean) ||
      parsed?.columns ||
      [];
    if (!cols.length) {
      toast({ title: "No columns to route", message: "Profile a source first.", tone: "warning" });
      return;
    }
    setVectorRoutingLoading(true);
    try {
      const samples: Record<string, string[]> = {};
      const rows = (parsed?.data ?? parsed?.sample_data ?? []) as Record<string, unknown>[];
      for (const col of cols) {
        const fromAnalysis = analysis?.columns?.find((c) => c.column_name === col);
        // Enhanced analysis may not expose samples; use upload preview rows.
        const vals = rows
          .slice(0, 20)
          .map((r) => String(r?.[col] ?? ""))
          .filter(Boolean);
        if (vals.length) samples[col] = vals;
        void fromAnalysis;
      }
      const plan = await fetchVectorRouting({
        columns: cols,
        samples,
        schema_types: parsed?.schema || analysis?.columns?.reduce<Record<string, string>>((acc, c) => {
          if (c.inferred_type) acc[c.column_name] = c.inferred_type;
          return acc;
        }, {}) || {},
        analysis_columns: (analysis?.columns || []).map((c) => ({
          column_name: c.column_name,
          is_pii: c.is_pii,
          semantic_type: c.semantic_type,
        })),
      });
      setVectorRoutingFields(plan.fields || []);
      if (autoApply) applyVectorRoutingPlan(plan);
      else {
        toast({
          title: "Routing ready",
          message: `Embed=${plan.content_column || "—"} · PII excluded=${(plan.exclude_pii_columns || []).length}`,
          tone: "info",
        });
      }
    } catch (e) {
      toast({
        title: "Vector routing failed",
        message: e instanceof Error ? e.message : "Try again",
        tone: "error",
      });
    } finally {
      setVectorRoutingLoading(false);
    }
  };

  useEffect(() => {
    // Document uploads → default vector embed fields (content + provenance metadata).
    const ft = (parsed?.file_type || "").toLowerCase();
    if (!["pdf", "docx", "html", "htm"].includes(ft)) return;
    if (!vectorContentColumn) setVectorContentColumn("content");
    if (!vectorMetadataColumns) setVectorMetadataColumns("filename,page,heading,element_type,chunk_index");
  }, [parsed?.file_type, vectorContentColumn, vectorMetadataColumns]);

  useEffect(() => {
    // When a vector destination is selected and fields are still empty, auto-route once.
    if (!isVectorDest) return;
    if (vectorContentColumn || vectorRoutingFields.length) return;
    const cols = analysis?.columns?.length ? analysis.columns.map((c) => c.column_name) : parsed?.columns;
    if (!cols?.length) return;
    void runVectorRouting(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- one-shot when dest/analysis ready
  }, [isVectorDest, analysis?.columns, parsed?.columns]);

  useEffect(() => {
    fetchTransferCapabilities()
      .then((caps) => {
        const sources = (caps.source_databases as string[] | undefined) ?? [];
        const dbs = (caps.destination_databases as string[] | undefined) ?? [];
        const exports = (caps.destination_file_formats as string[] | undefined) ?? [];
        const drivers = (caps.transfer_live_drivers as string[] | undefined) ?? [];
        // Reinforce catalog SSOT (app boot also loads; Transfer page refreshes lists).
        setTransferLiveDrivers(drivers.length ? drivers : [...sources, ...dbs]);
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
        const ocr = caps.ocr as { available?: boolean; message?: string } | undefined;
        if (ocr && typeof ocr === "object") {
          setOcrStatus(ocr);
        }
        const emb = caps.embedding_cache as EmbeddingCacheStats | undefined;
        if (emb && typeof emb === "object" && typeof emb.entries === "number") {
          setEmbeddingCacheStats(emb);
          if (typeof emb.durable_default === "boolean") {
            setVectorDurableCache(emb.durable_default);
          }
        }
      })
      .catch(() => {});
  }, []);

  const refreshEmbeddingCacheStats = useCallback(async () => {
    setEmbeddingCacheBusy(true);
    try {
      const stats = await fetchEmbeddingCacheStats();
      setEmbeddingCacheStats(stats);
    } catch (err) {
      toast({
        title: "Embedding cache",
        message: err instanceof Error ? err.message : "Could not load cache stats",
        tone: "warning",
      });
    } finally {
      setEmbeddingCacheBusy(false);
    }
  }, [toast]);

  const handleClearEmbeddingCache = useCallback(async () => {
    setEmbeddingCacheBusy(true);
    try {
      const result = await clearEmbeddingCache();
      toast({
        title: "Embedding cache cleared",
        message: `Deleted ${result.deleted} durable entries` +
          (result.memory_cleared ? ` and ${result.memory_cleared} in-memory` : ""),
        tone: "success",
      });
      const stats = await fetchEmbeddingCacheStats();
      setEmbeddingCacheStats(stats);
    } catch (err) {
      toast({
        title: "Clear failed",
        message: err instanceof Error ? err.message : "Could not clear cache",
        tone: "error",
      });
    } finally {
      setEmbeddingCacheBusy(false);
    }
  }, [toast]);

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
  // Prefer non-empty lists — `??` does not skip `[]`, and route analyze can
  // overwrite transferPlan with an empty source_columns array.
  const currentSourceColumns = (() => {
    if (sourceKind === "file") return parsed?.columns?.length ? parsed.columns : [];
    if (transferPlan?.source_columns?.length) return transferPlan.source_columns;
    if (parsed?.columns?.length) return parsed.columns;
    if (analysis?.columns?.length) return analysis.columns.map((c) => c.column_name);
    return [];
  })();
  const currentSourceSchema = (() => {
    if (sourceKind === "file") return parsed?.schema ?? {};
    if (transferPlan?.source_schema && Object.keys(transferPlan.source_schema).length) {
      return transferPlan.source_schema;
    }
    if (parsed?.schema && Object.keys(parsed.schema).length) return parsed.schema;
    if (analysis?.columns?.length) {
      return Object.fromEntries(
        analysis.columns.map((c) => [c.column_name, c.inferred_type || "VARCHAR"]),
      );
    }
    return {};
  })();
  const samplePreviewRows = parsed?.sample_data ?? parsed?.data ?? [];
  /** Rows Transform profiles and previews against — file preview or connector sample. */
  const shapeSampleRows = (
    sourceKind === "file" ? samplePreviewRows : connectorSampleRows
  ) as Record<string, unknown>[];
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
        transform: uiTransformToEngine(m.transform, m.engineTransform),
        target_type: m.destType || undefined,
        struct_policy: m.structPolicy,
      })),
      column_types: (currentSourceSchema || {}) as Record<string, string>,
      sample_size: 25,
      // Same image as the gates: the recipe runs on the read, so scanning the
      // raw cell reports a finding on a value the writer never binds.
      shape_recipe: recipePayload(shapeSteps),
      date_locale: dateLocale,
      number_locale: numberLocale,
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
  }, [step, currentSourceColumnsKey, columnMappings, samplePreviewRows, currentSourceSchema, currentSourceColumns, shapeSteps, dateLocale, numberLocale]);

  // A name-matched column is a starting point for the operator, never a claim
  // about the column's behaviour — the declaration beside it carries that.
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
    ? (sourceConnector?.type === "mongodb"
      ? sourceCollection
      : isCallableSourceMode(sourceReadMode)
        ? procedureStreamName(procedureCall)
        : sourceTable)
    : "";
  const multiStreamNames = parseStreamNames(sourceStreamInputRaw);
  /** First named stream — API table/collection field (never the raw CSV string). */
  const primarySourceStream = primaryStreamName(sourceStreamInputRaw);
  const isMultiStreamSource = multiStreamNames.length > 1;
  /** SCD2/mirror multi-stream not supported yet — full/incremental/CDC are. */
  const multiStreamUnsupportedMode =
    isMultiStreamSource && (syncMode === "scd2" || syncMode === "mirror");
  const advancedStreamNames = isMultiStreamSource ? multiStreamNames : [sourceStreamName];
  const routeSyncModes = useMemo(
    () =>
      availableSyncModes({
        destDriver: destDriverType || destType || "",
        sourceDriver: resolveDriverType(sourceConnector?.type || "") || "",
        sourceKind,
        isMultiStream: isMultiStreamSource,
        sourceReadMode,
        destWriteMode,
      }),
    [destDriverType, destType, sourceConnector?.type, sourceKind, isMultiStreamSource, sourceReadMode, destWriteMode],
  );
  // Client deploy: never leave an engine-unsupported mode selected after route change.
  useEffect(() => {
    if (!routeSyncModes.some((m) => m.id === syncMode)) {
      const fallback =
        routeSyncModes.find((m) => m.id === "full_refresh_append")?.id
        || routeSyncModes[0]?.id
        || "full_refresh_append";
      setSyncMode(fallback as SyncModeId);
    }
  }, [routeSyncModes, syncMode]);
  const mapStreamsDiverge = useMemo(() => {
    const ok = streamPreviews.filter((s) => s.status === "ok" && (s.columns?.length ?? 0) > 0);
    if (ok.length < 2) return false;
    const sig = (cols: string[]) => [...cols].map((c) => c.toLowerCase()).sort().join("|");
    const first = sig(ok[0].columns || []);
    return ok.some((s) => sig(s.columns || []) !== first);
  }, [streamPreviews]);
  const sourceColumnsByStream = useMemo(() => {
    const out: Record<string, string[]> = {};
    for (const preview of streamPreviews) {
      if (preview.status === "ok" && preview.columns?.length) {
        out[preview.name] = preview.columns;
      }
    }
    return out;
  }, [streamPreviews]);
  const sourceSchemaByStream = useMemo(() => {
    const out: Record<string, Record<string, string>> = {};
    for (const preview of streamPreviews) {
      if (preview.status === "ok" && preview.schema && Object.keys(preview.schema).length) {
        out[preview.name] = preview.schema;
      }
    }
    return out;
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
    defaultCursorSemantics: cursorSemantics,
    streamFields,
    snapshotMode: syncMode === "cdc" ? snapshotMode : undefined,
    streamMappings: isMultiStreamSource
      ? {
          ...streamMappings,
          [mapActiveStream || primarySourceStream]: columnMappings,
        }
      : undefined,
  });
  const streamContractIssue = firstStreamContractIssue({
    streamNames: advancedStreamNames,
    sourceColumns: currentSourceColumns,
    sourceColumnsByStream,
    requiresCursor,
    requiresPrimaryKey,
    defaultCursor: cursorField,
    defaultPrimaryKey: primaryKeyField,
    defaultCursorSemantics: cursorSemantics,
    streamFields,
    syncMode,
    validationMode,
  });
  const streamNeedsReview = streamContractIssue !== null;
  const syncModeLabel =
    routeSyncModes.find((m) => m.id === syncMode)?.label
    ?? SYNC_MODES.find((m) => m.id === syncMode)?.label
    ?? syncMode;
  const schemaPolicyLabel = SCHEMA_POLICIES.find((p) => p.id === schemaPolicy)?.label ?? schemaPolicy;

  const uniqueKeySuggestions = useMemo(
    () =>
      suggestUniqueKeyCandidates(
        samplePreviewRows as Record<string, unknown>[],
        currentSourceColumns,
        { exclude: primaryKeyField ? [primaryKeyField] : [], limit: 5 },
      ),
    [samplePreviewRows, currentSourceColumns, primaryKeyField],
  );
  const compositeKeySuggestions = useMemo(
    () =>
      suggestCompositeUniqueKeyCandidates(
        samplePreviewRows as Record<string, unknown>[],
        currentSourceColumns,
        { exclude: primaryKeyField && !primaryKeyField.includes(",") ? [primaryKeyField] : [], limit: 3 },
      ),
    [samplePreviewRows, currentSourceColumns, primaryKeyField],
  );

  /** Opens Advanced drawer in-place — never navigates away from Map / Validate. */
  const openIdentitySettings = useCallback(() => {
    setAdvancedLocaleFocus(null);
    setAdvancedOpen(true);
    toast({
      title: "Advanced settings",
      message:
        "Primary key, sync mode, cursor, and write policies. Stay on this step — close the drawer when done, then continue or re-run Validate.",
      tone: "info",
    });
  }, [toast]);

  /** Validate Set date/number locale — open Advanced and land on that control. */
  const openLocaleSettings = useCallback(
    (kind: "date" | "number") => {
      setAdvancedLocaleFocus(kind);
      setAdvancedOpen(true);
      toast({
        title: kind === "date" ? "Date locale" : "Number locale",
        message:
          kind === "date"
            ? "Set DMY or MDY. Auto will not guess 01/02/2024. Re-run Validate after you choose."
            : "Set US or EU. Auto will not guess 1,234. Re-run Validate after you choose.",
        tone: "info",
      });
    },
    [toast],
  );

  // Fix-bad-data is Validate-only — close if the operator leaves the step.
  useEffect(() => {
    if (step !== STEP_VALIDATE && badDataFixOpen) setBadDataFixOpen(false);
  }, [step, badDataFixOpen]);

  const applyPrimaryKeySuggestion = useCallback(
    (column: string) => {
      if (!column) return;
      setPrimaryKeyField(column);
      const stream = primarySourceStream || sourceStreamName;
      if (stream) {
        setStreamFields((prev) => ({
          ...prev,
          [stream]: {
            ...(prev[stream] || { cursorField: "", primaryKeyField: "" }),
            primaryKeyField: column,
          },
        }));
      }
      const isComposite = column.includes(",");
      toast({
        title: isComposite ? `Composite PK → ${column.replace(/,/g, " + ")}` : `Primary key → ${column}`,
        message: isComposite
          ? "Composite unique in the Validate sample only — use when a single column is a false PK. Confirm in Advanced, then Re-run Validate."
          : "Unique in the Validate sample only — confirm in Advanced, then Re-run Validate.",
        tone: "success",
      });
    },
    [primarySourceStream, sourceStreamName, toast],
  );

  const buildSourceEndpoint = () => {
    if (sourceKind === "file") {
      return {
        kind: "file",
        format: parsed?.file_type ?? file?.name.split(".").pop() ?? "csv",
        filename: file?.name,
        ...(parsed?.file_id ? { file_id: parsed.file_id } : {}),
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
    const callable = isCallableSourceMode(sourceReadMode) && !isMongo && !isDynamo;
    const procName = callable ? procedureStreamName(procedureCall) : "";
    return {
      kind: "database",
      format: sourceConnector.type,
      connector_id: sourceConnectorId,
      database: isDynamo ? tableOrPath : sourceConnector.database,
      table: isMongo ? undefined : (callable ? procName : tableOrPath) || undefined,
      collection: isMongo ? tableOrPath : undefined,
      auth_source: sourceConnector.auth_source || undefined,
      auth_mode: sourceConnector.auth_mode || undefined,
      auth_role: sourceConnector.auth_role || undefined,
      api_key: sourceConnector.api_key || undefined,
      service_account: sourceConnector.service_account || undefined,
      ...(syncMode === "cdc" && multiSubnetFailover
        ? { multi_subnet_failover: true }
        : {}),
      ...(callable
        ? {
            source_read_mode: sourceReadMode,
            procedure_call: sourceReadMode === "procedure" ? procedureCall.trim() : undefined,
            source_query: sourceReadMode === "query" ? procedureCall.trim() : undefined,
            procedure_params: Object.keys(procedureParams).length ? procedureParams : undefined,
            extra: {
              source_read_mode: sourceReadMode,
              procedure_call: sourceReadMode === "procedure" ? procedureCall.trim() : "",
              source_query: sourceReadMode === "query" ? procedureCall.trim() : "",
              procedure_params: procedureParams,
            },
          }
        : {}),
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
      date_locale: dateLocale,
      number_locale: numberLocale,
      backfill_new_fields: backfillNewFields,
      write_via_staging: writeViaStaging,
      priority_column: priorityColumn || "",
      priority_direction: priorityColumn ? priorityDirection : "desc",
      row_limit: rowLimit > 0 ? rowLimit : 0,
      stream_contracts: streamContracts,
      ...(() => {
        const bind = contractBindFromPolicies({
          contract_id: boundContractId,
          require_signed_contract: requireSignedContract,
        });
        return {
          contract_id: bind.contractId,
          require_signed_contract: bind.requireSigned,
        };
      })(),
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
    sourceReadMode,
    procedureCall,
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
    dateLocale,
    numberLocale,
    backfillNewFields,
    writeViaStaging,
    priorityColumn,
    priorityDirection,
    rowLimit,
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
    destTableExists,
    destWriteMode,
    destProcedureCall,
    destQuerySql,
    destProcedureParams,
    destProcedureBefore,
    destProcedureAfter,
    destProcedureParamMap,
    boundContractId,
    requireSignedContract,
  ]);

  const ensurePersistedPlan = useCallback(async (
    validationOverride?: ValidationMode,
    columnOverrides?: {
      source_columns?: string[];
      source_schema?: Record<string, string>;
      target_columns?: string[];
      target_schema?: Record<string, string>;
    },
  ): Promise<string | null> => {
    const sourceCols = columnOverrides?.source_columns?.length
      ? columnOverrides.source_columns
      : currentSourceColumns;
    if (!sourceCols.length) return null;
    const base = buildPlanPayload();
    const payload = {
      ...base,
      source_columns: sourceCols,
      source_schema: columnOverrides?.source_schema ?? base.source_schema,
      ...(columnOverrides?.target_columns
        ? {
            target_columns: columnOverrides.target_columns,
            target_schema: columnOverrides.target_schema ?? {},
          }
        : {}),
    };
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
      // Do not map against a stale plan — fall through to direct /transfer/map.
      return null;
    }
  }, [buildPlanPayload, currentSourceColumns, persistedPlanId]);

  const buildMappingsFromSource = useCallback((
    columns: import("../lib/types").ColumnAnalysis[] | undefined,
    targetCols?: string[],
  ) => {
    const rows = parsed?.data ?? parsed?.sample_data;
    if (columns?.length) {
      const destCols = targetCols ?? destColumns;
      return mappingsFromAnalysis(columns, rows, destCols);
    }
    const sourceCols = parsed?.columns ?? transferPlan?.source_columns ?? [];
    if (!sourceCols.length) return [];
    const destSet = new Set((targetCols ?? destColumns).map((c) => c.toLowerCase()));
    const pendingDest = destSet.size === 0;
    return sourceCols.map((col) => ({
      source: col,
      target: col,
      confidence: pendingDest ? Math.min(0.7, 0.55) : 0.7,
      inferredType: parsed?.schema?.[col] ?? transferPlan?.source_schema?.[col] ?? "string",
      sample: rows?.find((r) => r[col] != null)?.[col] != null
        ? String(rows!.find((r) => r[col] != null)![col])
        : undefined,
      approved: false,
      existsInDestination: destSet.has(col.toLowerCase()),
      createNew: !pendingDest && !destSet.has(col.toLowerCase()) ? true : undefined,
      assignmentStrategy: pendingDest
        ? "pending_dest_schema" as const
        : (!destSet.has(col.toLowerCase()) ? "create_compatible_new" as const : undefined),
      reason: pendingDest
        ? "Identity mapping — destination schema not loaded yet"
        : "Identity mapping (pipeline unavailable)",
      transform: "none" as const,
      requiresReview: pendingDest || undefined,
    }));
  }, [parsed, transferPlan, destColumns]);

  const mappingGenRef = useRef(0);
  /**
   * Latest `loadDestinationSchema`. Map runs before that function is declared and
   * must not close over a stale render's copy, so it is reached through a ref.
   */
  const loadDestSchemaRef = useRef<
    ((opts?: { force?: boolean }) => Promise<{
      columns: string[];
      schema: Record<string, string>;
      tableExists: boolean | null;
      connected: boolean | null;
      message: string;
    }>) | null
  >(null);

  const applyPipelineMappings = useCallback(
    async (
      targetCols?: string[],
      targetSchema?: Record<string, string>,
      analysisOverride?: import("../lib/types").EnhancedAnalysis | null,
      destinationTableExistsOverride?: boolean | null,
    ) => {
      const sourceCols =
        (parsed?.columns?.length ? parsed.columns : null)
        ?? analysisOverride?.columns.map((c) => c.column_name)
        ?? analysis?.columns.map((c) => c.column_name)
        ?? (transferPlan?.source_columns?.length ? transferPlan.source_columns : null)
        ?? [];
      if (!sourceCols.length) return [];
      const threshold = confidenceThresholdForMode(validationMode);
      const declaredSchema = parsed?.schema ?? transferPlan?.source_schema ?? {};
      // Transform (pre-load) runs on the read, before Map. So the columns and
      // carriers Map decides fidelity from are the ones the recipe produces, not
      // the raw source they replaced — otherwise a column rounded to whole
      // numbers is still explained as a lossy decimal the writer would refuse.
      // The plan keeps declared source truth: the recipe is applied once, by the
      // engine, on the read.
      const image = shapeIdentityRef.current;
      const transformed = Boolean(image && image.columns.length);
      const mapSourceCols = transformed && image ? image.columns : sourceCols;
      const mapSourceSchema = transformed && image
        ? Object.fromEntries(
            mapSourceCols.map((column) => [
              column,
              image.columnTypes[column] ?? declaredSchema[column] ?? "VARCHAR",
            ]),
          )
        : declaredSchema;
      const gen = ++mappingGenRef.current;
      const rows = parsed?.data ?? parsed?.sample_data;
      const analysisCols = analysisOverride?.columns ?? analysis?.columns;
      let existsForMap =
        destinationTableExistsOverride !== undefined
          ? destinationTableExistsOverride
          : (destTableExists ?? destTableExistsRef.current);
      let mapTargetCols = targetCols;
      let mapTargetSchema = targetSchema;
      // Destination existence is a fact the catalog can settle, not an unknown to
      // map against. Mapping with `null` left every target_type empty, so Map
      // showed ten columns of "loses fidelity (T → T)" behind a Risk Contract that
      // no signature could clear. Probe once here: the answer is either real
      // destination types or proof of absence, i.e. a CREATE plan.
      if (
        existsForMap == null
        && destKindMode === "database"
        && Boolean(connectorId)
        && Boolean(targetCollection.trim())
      ) {
        try {
          const probed = await loadDestSchemaRef.current?.();
          if (probed) {
            existsForMap = probed.tableExists;
            if (probed.tableExists === true && probed.columns.length && !targetCols?.length) {
              mapTargetCols = probed.columns;
              mapTargetSchema = probed.schema;
            }
          }
        } catch {
          // Probe failed — existence stays unknown and the engine stays fail-closed.
        }
      }
      const commitMappings = (
        next: EditableMapping[],
        llmUsed: boolean,
        proof: import("../components/MappingProofDrawer").MappingProof | null,
      ) => {
        if (gen !== mappingGenRef.current) return next;
        const prior = columnMappingsRef.current;
        if (!next.length && sourceCols.length) {
          const identity = carryOperatorDecisions(
            buildMappingsFromSource(analysisCols, targetCols),
            prior,
          );
          if (identity.length) {
            setColumnMappings(identity);
            setLlmMappingUsed(false);
            setMappingProof(null);
            return identity;
          }
        }
        const carried = carryOperatorDecisions(next, prior);
        setColumnMappings(carried);
        setLlmMappingUsed(llmUsed);
        setMappingProof(proof);
        return carried;
      };
      try {
        // Prefer direct map when fresh dest schema is in-hand — plan persistence
        // can lag React state and previously wiped create-new proposals.
        // A declared transform is also a reason to map directly: the persisted
        // plan holds declared source truth, not the transformed image, so only
        // the direct call can be asked about the columns the writer will see.
        const useDirect = Boolean(mapTargetCols?.length) || !persistedPlanId || transformed;
        let result: Awaited<ReturnType<typeof mapTransferColumns>>;
        if (!useDirect) {
          // The plan records declared source truth — the engine applies the
          // recipe on the read, so persisting the transformed image here would
          // apply it twice.
          const planId = await ensurePersistedPlan(undefined, {
            source_columns: sourceCols,
            source_schema: declaredSchema,
            target_columns: mapTargetCols,
            target_schema: mapTargetSchema,
          });
          if (planId) {
            result = await mapTransferPlan(planId, {
              validation_mode: validationMode,
              use_llm: true,
              source_samples: buildSourceSamples(),
            });
          } else {
            result = await mapTransferColumns({
              source_columns: mapSourceCols,
              source_schema: mapSourceSchema,
              target_columns: mapTargetCols?.length ? mapTargetCols : undefined,
              target_schema: mapTargetSchema,
              validation_mode: validationMode,
              file_format: parsed?.file_type
                ?? (sourceKind !== "file" ? sourceConnector?.type : undefined)
                ?? file?.name.split(".").pop(),
              use_llm: true,
              source_samples: buildSourceSamples(),
              destination_db_type: destKindMode === "file_export" ? exportFormat : destType,
              sync_mode: syncMode,
              source_kind: mapSourceKind,
              destination_table_exists:
                destKindMode === "database" ? existsForMap : false,
            });
          }
        } else {
          result = await mapTransferColumns({
            source_columns: mapSourceCols,
            source_schema: mapSourceSchema,
            target_columns: mapTargetCols?.length ? mapTargetCols : undefined,
            target_schema: mapTargetSchema,
            validation_mode: validationMode,
            file_format: parsed?.file_type
              ?? (sourceKind !== "file" ? sourceConnector?.type : undefined)
              ?? file?.name.split(".").pop(),
            use_llm: true,
            source_samples: buildSourceSamples(),
            destination_db_type: destKindMode === "file_export" ? exportFormat : destType,
            sync_mode: syncMode,
            source_kind: mapSourceKind,
            destination_table_exists:
              destKindMode === "database" ? existsForMap : false,
          });
          // Do NOT create an empty draft plan here. Fire-and-forget create races
          // Validate (which may sync a good plan) and leaves Execute pointing at a
          // draft with zero revisions → "Plan has no mappings". Plan persistence
          // happens on Validate via ensurePersistedPlan + syncTransferPlanMappings.
        }
        const mapped = editableFromPipelineMappings(
          result.mappings ?? [],
          rows,
          mapTargetCols,
          threshold,
          mapTargetSchema,
        );
        const shape = (result as { shape_contract?: typeof shapeContract }).shape_contract;
        if (shape && typeof shape === "object") {
          const extras = [
            ...((shape as { unaccounted_sources?: string[] }).unaccounted_sources || []),
            ...(((shape as { columns?: Array<{ source?: string; kind?: string }> }).columns || [])
              .filter((c) => c.kind === "add_proposed" || c.kind === "pending" || c.kind === "unaccounted")
              .map((c) => c.source || "")
              .filter(Boolean)),
          ];
          setShapeContract({
            ...shape,
            extra_source_columns: [...new Set(extras)],
          });
        } else {
          setShapeContract(null);
        }
        return commitMappings(
          mapped,
          Boolean(result.llm?.llm_used),
          (result as { mapping_proof?: import("../components/MappingProofDrawer").MappingProof }).mapping_proof ?? null,
        );
      } catch (e) {
        console.error("Mapping pipeline failed:", e);
        let fallback = buildMappingsFromSource(analysisCols, mapTargetCols);
        // Without the pipeline nothing has projected a destination type. For a
        // database destination whose columns were never read, the source type is
        // not an answer — echoing it printed "VARCHAR(16777216) → VARCHAR(16777216)"
        // and let the operator approve a destination that was never inspected.
        const destSchemaUnread =
          destKindMode === "database" && !(mapTargetCols?.length ?? destColumns.length);
        if (destSchemaUnread) {
          fallback = fallback.map(markMappingDestUnread);
        }
        if (fallback.length) {
          toast({
            title: destSchemaUnread ? "Destination schema not read" : "Using fallback mappings",
            message: destSchemaUnread
              ? "The mapping engine could not be reached, so no destination type was projected. "
                + "Reload the destination schema on Map before continuing."
              : "Semantic pipeline unavailable — showing AI-classified column pairs. Review before transfer.",
            tone: "warning",
          });
        }
        return commitMappings(fallback, false, null);
      }
    },
    [
      parsed,
      analysis,
      transferPlan,
      validationMode,
      file,
      sourceKind,
      sourceConnector,
      buildSourceSamples,
      ensurePersistedPlan,
      buildMappingsFromSource,
      toast,
      persistedPlanId,
      destKindMode,
      exportFormat,
      destType,
      syncMode,
      destTableExists,
      destColumns,
      connectorId,
      targetCollection,
    ],
  );

  /** Semantic map for one multi-stream source table (uses that stream's columns). */
  const mapColumnsForStream = useCallback(
    async (streamName: string): Promise<EditableMapping[]> => {
      const preview = streamPreviews.find((s) => s.name === streamName && s.status === "ok");
      const sourceCols = preview?.columns?.length
        ? preview.columns
        : (analysis?.columns.map((c) => c.column_name) ?? []);
      if (!sourceCols.length) return [];
      const sourceSchema = preview?.schema ?? {};
      const rows = preview?.rows?.length ? preview.rows : undefined;
      const threshold = confidenceThresholdForMode(validationMode);
      const targetCols = destColumns.length ? destColumns : undefined;
      const targetSchema = destSchemaMap;
      try {
        const result = await mapTransferColumns({
          source_columns: sourceCols,
          source_schema: sourceSchema,
          target_columns: targetCols,
          target_schema: targetSchema,
          validation_mode: validationMode,
          file_format: sourceConnector?.type,
          use_llm: true,
          source_samples: buildColumnSamples(sourceCols, rows ?? []),
          destination_db_type: destKindMode === "file_export" ? exportFormat : destType,
          sync_mode: syncMode,
          source_kind: mapSourceKind,
          destination_table_exists:
            destKindMode === "database"
              ? (destTableExists ?? destTableExistsRef.current)
              : false,
        });
        return editableFromPipelineMappings(
          result.mappings,
          rows,
          targetCols,
          threshold,
          targetSchema,
        );
      } catch (e) {
        console.error(`Stream mapping failed for ${streamName}:`, e);
        return sourceCols.map((col) => ({
          source: col,
          target: col,
          confidence: 0.5,
          approved: false,
          requiresReview: true,
          existsInDestination: (targetCols || []).some((t) => t.toLowerCase() === col.toLowerCase()),
          reason: "Fallback identity mapping (stream rematch unavailable)",
          transform: "none" as const,
        }));
      }
    },
    [
      streamPreviews,
      analysis,
      validationMode,
      destColumns,
      destSchemaMap,
      sourceConnector,
      destKindMode,
      exportFormat,
      destType,
      syncMode,
      destTableExists,
    ],
  );

  const remapWithDestination = async (targetCols: string[], targetSchema: Record<string, string>) => {
    await applyPipelineMappings(targetCols, targetSchema);
  };

  /**
   * Publish a probe result only when it differs from what Studio already holds.
   * Map depends on these two values by identity, and Map probes the destination,
   * so re-publishing an equal answer was a self-feeding probe loop.
   */
  const commitDestSchema = (columns: string[], schema: Record<string, string>) => {
    if (!sameColumnList(destColumns, columns)) setDestColumns(columns);
    if (!sameSchemaMap(destSchemaMap, schema)) setDestSchemaMap(schema);
  };

  const loadDestinationSchema = async (opts?: { force?: boolean }): Promise<{
    columns: string[];
    schema: Record<string, string>;
    tableExists: boolean | null;
    connected: boolean | null;
    message: string;
  }> => {
    if (destKindMode !== "database" || !connectorId) {
      return {
        columns: destColumns,
        schema: destSchemaMap,
        tableExists: destCatalogExists(destKindMode, destTableExists),
        connected: null,
        message: "",
      };
    }
    const tableKey = `${connectorId}|${destType}|${targetDb}|${destSchema}|${targetCollection.trim()}`;
    if (
      !opts?.force
      && shouldSkipAutoDestProbe(destProbeFailureRef.current, tableKey, Date.now())
    ) {
      return {
        columns: destColumns,
        schema: destSchemaMap,
        tableExists: destTableExists,
        connected: false,
        message: destConnectionError,
      };
    }
    const gen = ++destSchemaGenRef.current;
    destProbeInflightRef.current += 1;
    setDestSchemaLoading(true);
    try {
      // Destination-only probe: stub file source so we do not re-sample Mongo/SQL
      // (that was hanging the Destination step for minutes on large collections).
      const { destination } = await introspectTransferEndpoints(
        {
          source: { kind: "file", format: "csv" },
          destination: buildDestinationEndpoint(),
        },
        // A warehouse can cold-start for minutes; an unreachable OLTP host must not
        // hold "Reading destination…" for three minutes with no way back. Bound the
        // wait so the operator reaches the unknown-destination state and can retry.
        { timeoutMs: DEST_PROBE_TIMEOUT_MS[destProbeSpeedClass(destType)] },
      );
      if (gen !== destSchemaGenRef.current) {
        return {
          columns: destColumns,
          schema: destSchemaMap,
          tableExists: destTableExists,
          connected: null,
          message: "",
        };
      }
      const objectNames = (destination.objects ?? [])
        .map((o) => (o.name || "").trim())
        .filter(Boolean);
      setDestObjectNames(objectNames);
      const connected = destination.connected !== false;
      setDestConnected(connected ? true : false);
      setDestConnectionError((destination.message as string) || "");

      // No table typed yet — only refresh the picker list.
      if (!targetCollection.trim()) {
        commitDestSchema([], {});
        proveDestTableExists(null);
        destSchemaTableKeyRef.current = tableKey;
        return { columns: [], schema: {}, tableExists: null, connected, message: (destination.message as string) || "" };
      }

      const columns = destination.columns ?? [];
      const schema = destination.schema ?? {};
      const want = targetCollection.trim();
      const wantL = want.toLowerCase();
      const wantLeaf = wantL.split(".").pop() || wantL;
      const listed = objectNames.some((raw) => {
        const rawL = raw.toLowerCase();
        const rawLeaf = rawL.split(".").pop() || rawL;
        return (
          rawL === wantL
          || rawLeaf === wantL
          || wantLeaf === rawL
          || rawL.endsWith(`.${wantL}`)
          || wantL.endsWith(`.${rawL}`)
          || rawLeaf === wantLeaf
        );
      });
      // Trust the destination probe. Never sticky-promote "exists" across a
      // table rename — that showed "Existing table detected" for missing names.
      let resolvedExists: boolean | null;
      if (destination.connected === false) {
        resolvedExists = null;
      } else if (destination.table_exists === false) {
        resolvedExists = false;
      } else if (listed || destination.table_exists === true || columns.length > 0) {
        resolvedExists = true;
      } else if (objectNames.length > 0 && want) {
        resolvedExists = false;
      } else {
        resolvedExists = null;
      }
      // Same-table flaky re-probe only: keep prior exists when the new result is
      // uncertain (null), never when the API says missing.
      if (
        resolvedExists == null
        && destSchemaTableKeyRef.current === tableKey
        && destTableExists === true
      ) {
        resolvedExists = true;
      }
      // Confirmed missing → wipe prior columns (do not attribute another table's DDL).
      // Empty probe for the *same* existing table → keep prior schema (avoid create-new flicker).
      const sameTable = destSchemaTableKeyRef.current === tableKey;
      const keepPrior =
        resolvedExists !== false
        && columns.length === 0
        && sameTable
        && (resolvedExists === true || destTableExists === true || destColumns.length > 0);
      const nextColumns = resolvedExists === false ? [] : (keepPrior ? destColumns : columns);
      const nextSchema = resolvedExists === false ? {} : (keepPrior ? destSchemaMap : schema);
      // A probe that cannot settle existence is not a reading Studio can act on,
      // and the automatic probe must back off from it exactly as it does from a
      // thrown error — an unreachable host answers "not connected" through the
      // success path, which is how the re-probe loop survived the first fix.
      destProbeFailureRef.current = destProbeSettled(connected, resolvedExists)
        ? null
        : { key: tableKey, at: Date.now() };
      commitDestSchema(nextColumns, nextSchema);
      proveDestTableExists(resolvedExists);
      destSchemaTableKeyRef.current = tableKey;
      if (keepPrior && resolvedExists === true && destColumns.length === 0) {
        const metaKey = `meta:${tableKey}`;
        if (lastNewTableToastRef.current !== metaKey) {
          lastNewTableToastRef.current = metaKey;
          toast({
            title: "Table exists — columns not loaded",
            message:
              `${targetCollection.trim()} is on the destination, but column metadata failed. Retry Destination/Map; do not treat this as create-new.`,
            tone: "warning",
          });
        }
      }
      if (resolvedExists === false && lastNewTableToastRef.current !== tableKey) {
        lastNewTableToastRef.current = tableKey;
        toast({
          title: "New table will be created",
          message: `${targetCollection.trim()} was not found on the destination — Datawrap will CREATE TABLE on first write.`,
          tone: "info",
        });
      }
      return {
        columns: nextColumns,
        schema: nextSchema,
        tableExists: resolvedExists,
        connected,
        message: (destination.message as string) || "",
      };
    } catch (e) {
      if (gen !== destSchemaGenRef.current) {
        return {
          columns: destColumns,
          schema: destSchemaMap,
          tableExists: destTableExists,
          connected: null,
          message: "",
        };
      }
      // A transient error on the *same* table keeps its last-known schema so the
      // panel does not blank out. A different table has no schema of its own —
      // showing the previous table's DDL for it is a false read.
      destProbeFailureRef.current = { key: tableKey, at: Date.now() };
      const sameTable = destSchemaTableKeyRef.current === tableKey;
      const keptColumns = sameTable ? destColumns : [];
      const keptSchema = sameTable ? destSchemaMap : {};
      // Re-setting an already-empty schema still hands Map a new object identity,
      // which re-ran Map, which re-probed — the loop that pinned the reload
      // control on "Reading destination…" for as long as the host stayed down.
      if (!sameTable) commitDestSchema([], {});
      // Do not force "table missing" — unknown is safer than a false create promise.
      proveDestTableExists(null);
      const rawMsg = e instanceof Error ? e.message : "";
      const timedOut = /timed out/i.test(rawMsg);
      const errMsg = timedOut
        ? `Destination did not answer within `
          + `${Math.round(DEST_PROBE_TIMEOUT_MS[destProbeSpeedClass(destType)] / 1000)}s — existence unknown. `
          + "Check the destination is reachable, then reload the destination schema."
        : rawMsg || "Retry or continue — existence will be rechecked on Validate.";
      setDestConnected(false);
      setDestConnectionError(errMsg);
      toast({
        title: "Could not read destination schema",
        message: errMsg,
        tone: "warning",
      });
      return { columns: keptColumns, schema: keptSchema, tableExists: null, connected: false, message: errMsg };
    } finally {
      destProbeInflightRef.current = Math.max(0, destProbeInflightRef.current - 1);
      if (destProbeInflightRef.current === 0) setDestSchemaLoading(false);
    }
  };
  loadDestSchemaRef.current = loadDestinationSchema;

  useEffect(() => {
    if (!analysis?.columns.length || step !== STEP_MAP || analyzing) return;
    // Never invent create-new identity mappings for a database destination unless
    // the object is confirmed missing OR unknown with no columns (operator-typed new table).
    if (
      destKindMode === "database"
      && !destColumns.length
      && destTableExists === true
    ) {
      return;
    }
    void applyPipelineMappings(destColumns.length ? destColumns : undefined, destSchemaMap);
  }, [validationMode, step, destColumns, destSchemaMap, destTableExists, destKindMode, analyzing]);

  useEffect(() => {
    if (destKindMode !== "database" || !connectorId) return;
    // Load object list as soon as a connector is chosen; refine when table is set.
    if (step !== STEP_DESTINATION && step !== STEP_MAP && step !== STEP_VALIDATE) return;
    const t = window.setTimeout(() => { void loadDestinationSchema(); }, 350);
    return () => window.clearTimeout(t);
  }, [step, destKindMode, targetCollection, connectorId, destType, targetDb, destHost, destPort, destSchema, destWarehouse]);

  // An unreachable destination is re-checked on a fixed cadence, not as fast as
  // Map can re-render: the operator gets automatic recovery when the host comes
  // back, and a host that stays down costs four probes a minute instead of
  // twenty. Reload and Validate still probe on demand.
  useEffect(() => {
    if (destKindMode !== "database" || !connectorId) return;
    if (step !== STEP_DESTINATION && step !== STEP_MAP) return;
    if (destConnected !== false || destSchemaLoading) return;
    const t = window.setTimeout(() => {
      void loadDestinationSchema({ force: true }).finally(() => {
        setDestProbeRetryTick((n) => n + 1);
      });
    }, DEST_PROBE_FAILURE_COOLDOWN_MS);
    return () => window.clearTimeout(t);
  }, [step, destKindMode, connectorId, destConnected, destSchemaLoading, destProbeRetryTick]);

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
    setPreflightError("");
    setValidatedContractKey(null);
    setCellPreview(null);
    setDestColumns([]);
    setDestSchemaMap({});
    proveDestTableExists(null);
    setDestObjectNames([]);
    destSchemaTableKeyRef.current = "";
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
    else if (resolveDriverType(conn.type) === "snowflake") setDestSchema("PUBLIC");
    if (conn.warehouse) setDestWarehouse(conn.warehouse);
    setDestHost(conn.host || getConnectorDefaults(conn.type).host);
    setDestPort(conn.port || defaultPortForType(conn.type));
    if (resolveDriverType(conn.type) === "iceberg") {
      setDestConnectionString(conn.connection_string || "");
      setDestSchema(conn.database || conn.schema || "");
      setTargetDb(conn.database || "");
    } else {
      setDestConnectionString(conn.connection_string || "");
    }
    setTargetCollection("");
  };

  useEffect(() => {
    if (connectorId || !destType) return;
    setDestHost(getConnectorDefaults(destType).host);
    setDestPort(defaultPortForType(destType));
    setDestSchema(defaultSchemaForDriver(destType));
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

  useEffect(() => {
    if (sourceReadMode === "procedure" && !dialectOffersProcedures(sourceConnector?.type)) {
      setSourceReadMode(dialectOffersQuery(sourceConnector?.type) ? "query" : "table");
    }
    if (sourceReadMode === "query" && !dialectOffersQuery(sourceConnector?.type)) {
      setSourceReadMode("table");
    }
  }, [sourceConnector?.type, sourceReadMode]);

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

  // Jobs → Studio: land on Validate/Map, seed mappings, open repair proposal.
  useEffect(() => {
    if (!seedStudioIntent?.token) return;
    if (appliedStudioIntentTokenRef.current === seedStudioIntent.token) return;
    appliedStudioIntentTokenRef.current = seedStudioIntent.token;

    setCellPreview(null);
    setMappingProof(null);
    setResult(null);
    setActiveJobId(seedStudioIntent.jobId || null);

    // Hydrate Validate from the job's captured gates — never leave all rules Pending
    // when opening from Jobs. Clear only when no job preflight is available.
    if (seedStudioIntent.preflight) {
      setPreflight(seedStudioIntent.preflight);
    } else if (seedStudioIntent.jobId) {
      setPreflight(null);
      void fetchJob(seedStudioIntent.jobId)
        .then((job) => {
          if (job?.preflight) setPreflight(job.preflight as typeof preflight);
          const rules = jobStudioDataRules(job as {
            validation_mode?: string;
            schema_policy?: string;
            transfer_request?: { validation_mode?: string; schema_policy?: string };
          });
          if (rules.validationMode) setValidationMode(rules.validationMode);
          if (rules.schemaPolicy) {
            setSchemaPolicy(rules.schemaPolicy);
            setBackfillNewFields(schemaPolicyBackfills(rules.schemaPolicy));
          }
          const delivery = jobStudioDeliveryGuarantee(job as {
            delivery_guarantee?: string;
            transfer_request?: { delivery_guarantee?: string };
          });
          setDeliveryGuarantee(delivery);
        })
        .catch(() => {
          /* keep null — operator can Re-run */
        });
    } else {
      setPreflight(null);
    }

    const seededMode = namedStudioValidationMode(seedStudioIntent.validationMode);
    if (seededMode) setValidationMode(seededMode);
    const seededPolicy = namedStudioSchemaPolicy(seedStudioIntent.schemaPolicy);
    if (seededPolicy) {
      setSchemaPolicy(seededPolicy);
      setBackfillNewFields(schemaPolicyBackfills(seededPolicy));
    }
    if (seedStudioIntent.deliveryGuarantee) {
      setDeliveryGuarantee(namedCdcDeliveryGuarantee(seedStudioIntent.deliveryGuarantee));
    }

    const maps = seedStudioIntent.mappings;
    if (Array.isArray(maps) && maps.length > 0) {
      setColumnMappings(
        maps.map((m) => {
          const xfRaw = m.transform
            || (Array.isArray(m.transforms) && m.transforms[0]?.type)
            || "none";
          const xf = String(xfRaw) as EditableMapping["transform"];
          return {
            source: String(m.source || ""),
            target: String(m.destination || m.source || ""),
            confidence: 1,
            destType: String(m.destination_type || m.target_type || ""),
            approved: false,
            requiresReview: true,
            transform: xf && xf !== "none" ? xf : undefined,
            reason: seedStudioIntent.jobId
              ? `Seeded from job ${seedStudioIntent.jobId.slice(0, 8)}…`
              : "Seeded from Jobs quarantine repair",
          };
        }).filter((m) => m.source),
      );
    }

    if (seedStudioIntent.repairProposalId) {
      setSeedRepairProposalId(seedStudioIntent.repairProposalId);
    } else {
      setSeedRepairProposalId(null);
    }

    if (seedStudioIntent.step === "map") {
      setStep(STEP_MAP);
    } else if (seedStudioIntent.step === "source") {
      setStep(STEP_SOURCE);
    } else {
      setStep(STEP_VALIDATE);
    }

    const jobHint = seedStudioIntent.jobId
      ? ` from job ${seedStudioIntent.jobId.slice(0, 8)}…`
      : "";
    toast({
      title: "Opened Validate in Studio",
      message: seedStudioIntent.repairProposalId
        ? `Repair proposal ready${jobHint}. Review actions, then re-run Validate.`
        : seedStudioIntent.preflight
          ? `Job gates loaded${jobHint}. Review results — Re-run only if you changed mappings.`
          : `Continue remediation${jobHint}. Job gates load when available; otherwise Re-run Validate.`,
      tone: "info",
    });
  }, [seedStudioIntent, toast]);

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
      setTransferPlan((prev) => ({
        ...plan,
        // Route analyze sometimes omits columns — keep introspected schema.
        source_columns: plan.source_columns?.length
          ? plan.source_columns
          : (prev?.source_columns?.length ? prev.source_columns : parsed?.columns ?? []),
        source_schema: plan.source_schema && Object.keys(plan.source_schema).length
          ? plan.source_schema
          : (prev?.source_schema && Object.keys(prev.source_schema).length
            ? prev.source_schema
            : parsed?.schema ?? {}),
      }));
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

  const runSourceColumnAnalysis = async (data: ParsedUpload, opts?: { manageAnalyzing?: boolean }) => {
    const manageAnalyzing = opts?.manageAnalyzing !== false;
    if (manageAnalyzing) setAnalyzing(true);
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
          source_kind: mapSourceKind,
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
      if (manageAnalyzing) setAnalyzing(false);
    }
  };

  const processFile = async (
    selected: File,
    opts: { readOptions?: SourceReadOptions } = {},
  ) => {
    // A window belongs to the file it was declared on, so a newly chosen file
    // starts from the default (active sheet, header on row 1).
    const window = opts.readOptions ?? {};
    const windowDeclared = Object.keys(window).length > 0;
    const ext = fileExtension(selected.name);
    if (!ACCEPTED_UPLOAD_EXTENSIONS.has(ext)) {
      toast({
        title: "Unsupported file type",
        message: "Use CSV, TSV, JSON, JSONL, Excel (.xlsx), or Parquet for this transfer flow.",
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
    // Re-profiling the same file through a new window keeps the panel mounted so
    // the operator can see which declaration is being applied; unmounting it mid
    // request left the controls to disappear under the cursor. Everything derived
    // from the old profile is still dropped, and Source cannot be left while the
    // re-read is in flight.
    const reprofiling = opts.readOptions !== undefined && selected === file;
    setUploadError(null);
    setReadWindowRefused(false);
    setFile(selected);
    if (!reprofiling) setParsed(null);
    setAnalysis(null);
    setPreflight(null);
    setPreflightError("");
    setPersistedPlanId(null);
    setLlmMappingUsed(false);
    setMappingProof(null);
    setUploading(true);
    try {
      let data: ParsedUpload;
      try {
        data = await uploadFile(selected, { enableOcr, readOptions: window });
      } catch (uploadErr) {
        const ext = fileExtension(selected.name);
        // Browser parsing cannot honour a declared window — never profile a
        // different population than the one that was asked for.
        if ((ext === "csv" || ext === "tsv") && !windowDeclared) {
          const text = await selected.text();
          data = parseCsvTextForPreview(text);
          toast({
            title: "Profiled locally",
            message: "Upload API timed out — preview uses browser parsing. Re-try upload for full server preflight and write. Data rules still apply on Validate.",
            tone: "warning",
          });
        } else {
          throw uploadErr;
        }
      }
      if (!data.columns?.length) {
        throw new Error(
          "No columns detected — JSON needs object rows: [{...}], a wrapper like {\"data\":[{...}]} / {\"countries\":[{...}]}, or a single object.",
        );
      }
      setParsed(data);
      setReadOptions(data.read_options ?? window);
      if (data.ocr_status) {
        setOcrStatus(data.ocr_status);
      }
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
          data.ocr_used
            ? ` OCR extracted text from ${data.ocr_page_count ?? 0} page(s).`
            : ""
        }${
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
      setReadWindowRefused(reprofiling);
      // A refused *window* keeps the file and its last good profile: the
      // declaration is wrong, not the upload, and forcing a re-upload to correct
      // a sheet name discards work the operator already did.
      if (!reprofiling) {
        setFile(null);
        setParsed(null);
        setReadOptions({});
      }
      toast({
        title: windowDeclared ? "Read window refused" : "Upload failed",
        message,
        tone: "error",
      });
      console.error(e);
    }
    setUploading(false);
  };

  /** Re-profile the current file through a newly declared window. */
  const applyReadWindow = (next: SourceReadOptions) => {
    if (!file || uploading) return;
    void processFile(file, { readOptions: next });
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
    if (sourceKind === "file" && uploading) {
      toast({
        title: "Source still being read",
        message: "The declared read window is being applied — the columns and row count on screen are the previous read.",
        tone: "warning",
      });
      setStep(STEP_SOURCE);
      return true;
    }
    if (sourceKind === "file" && !parsed) {
      toast({ title: "Source file required", message: "Upload a CSV, TSV, JSON, JSONL, Excel (.xlsx), or Parquet file to continue.", tone: "warning" });
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
    if (sourceKind === "database" && isCallableSourceMode(sourceReadMode) && !procedureCall.trim()) {
      toast({
        title: sourceReadMode === "query" ? "SQL query required" : "Stored procedure required",
        message: sourceReadMode === "query"
          ? "Paste one read-only SELECT/WITH to inspect the result set."
          : "Paste a single CALL / EXEC (or a PostgreSQL SELECT * FROM func()) to inspect the result set.",
        tone: "warning",
      });
      setStep(STEP_SOURCE);
      return true;
    }
    if (sourceKind === "database" && !isCallableSourceMode(sourceReadMode) && !(sourceTable || sourceCollection)) {
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
    if (destKindMode === "database" && isCallableDestMode(destWriteMode)) {
      const sql = destWriteMode === "query" ? destQuerySql : destProcedureCall;
      if (!sql.trim()) {
        toast({
          title: destWriteMode === "query" ? "Destination query required" : "Destination procedure required",
          message: destWriteMode === "query"
            ? "Paste one INSERT / MERGE / UPDATE with :binds."
            : "Paste one CALL / EXEC. Each row is one statement; failed rows quarantine.",
          tone: "warning",
        });
        setStep(STEP_DESTINATION);
        return true;
      }
      const boundForDiag = {
        ...destProcedureParams,
        ...Object.fromEntries(
          Object.entries(destProcedureParamMap)
            .filter(([, col]) => Boolean(col))
            .map(([name]) => [name, "mapped"]),
        ),
      };
      const diagnosis = diagnoseSql(sql, {
        mode: destWriteMode === "query" ? "dest_dml" : "procedure",
        dialect: destDriverType || destType,
        bound: boundForDiag,
      });
      if (!diagnosis.ok) {
        toast({
          title: "Destination SQL needs a fix",
          message: diagnosis.error,
          tone: "warning",
        });
        setStep(STEP_DESTINATION);
        return true;
      }
      if (!targetCollection.trim()) {
        setTargetCollection(procedureStreamName(sql));
      }
    }
    if (destKindMode === "database" && !isCallableDestMode(destWriteMode) && !targetDb.trim()) {
      toast({ title: "Destination database required", message: "Enter the target database or project.", tone: "warning" });
      setStep(STEP_DESTINATION);
      return true;
    }
    if (destKindMode === "database" && !isCallableDestMode(destWriteMode) && !targetCollection.trim()) {
      toast({ title: "Destination table required", message: "Enter the target table or collection.", tone: "warning" });
      setStep(STEP_DESTINATION);
      return true;
    }
    if (streamContractIssue) {
      // One cause, one action, and the stream it belongs to — the engine refuses
      // per stream, so a generic "contract incomplete" would hide which one.
      toast({
        title: "Stream contract needs review",
        message: `${streamContractIssue.reason} ${streamContractIssue.action}`,
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
      row_estimate_uncertain?: boolean;
      data?: Record<string, unknown>[];
      sample_data?: Record<string, unknown>[];
      message?: string;
    },
  ) => {
    if (!sourceConnector) return;
    // Never stamp a 100-row preview as the transfer volume.
    if (
      !intro.row_estimate_uncertain
      && intro.row_estimate != null
      && intro.row_estimate > 0
    ) {
      setSourceRowEstimate(intro.row_estimate);
    }
    const sampleRows = intro.data ?? intro.sample_data ?? [];
    // Kept so Transform can profile and preview a connector source without a second
    // round-trip — the same rows Map was seeded from.
    setConnectorSampleRows(sampleRows);
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
    const population = (
      intro.row_estimate_uncertain
      || intro.row_estimate == null
      || intro.row_estimate <= 0
    ) ? 0 : intro.row_estimate;
    setActiveData({
      name: streamName || sourceConnector.name,
      columns: intro.columns,
      row_count: population,
      samples: columnSamples,
      schema: intro.schema ?? {},
    });
    setParsed({
      columns: intro.columns,
      schema: intro.schema ?? {},
      row_count: population,
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
    // The seed above uses the *source* logical type on both sides so the grid has
    // something to show. That is only honest for a file export or a destination
    // whose columns were actually read — a database destination with no read
    // schema must stay pending until Map stamps a destination type.
    setColumnMappings(
      destKindMode === "database" && !destColumns.length
        ? seeded.map(markMappingDestUnread)
        : seeded,
    );
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
    if (isCallableSourceMode(sourceReadMode) && !isMongo) {
      sourceEndpoint.source_read_mode = sourceReadMode;
      if (sourceReadMode === "procedure") sourceEndpoint.procedure_call = procedureCall.trim();
      if (sourceReadMode === "query") sourceEndpoint.source_query = procedureCall.trim();
      if (Object.keys(procedureParams).length) sourceEndpoint.procedure_params = procedureParams;
      sourceEndpoint.extra = {
        source_read_mode: sourceReadMode,
        procedure_call: sourceReadMode === "procedure" ? procedureCall.trim() : "",
        source_query: sourceReadMode === "query" ? procedureCall.trim() : "",
        procedure_params: procedureParams,
      };
    }

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
  }, [sourceConnector, sourceConnectorId, toast, sourceReadMode, procedureCall]);

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

    if (isCallableSourceMode(sourceReadMode) && !isMongo) {
      const call = procedureCall.trim();
      if (!call) return null;
      const streamName = procedureStreamName(call);
      setStreamPreviews([{
        name: streamName,
        status: "loading",
        columns: [],
        schema: {},
        rows: [],
      }]);
      setActiveStreamTab(streamName);
      setSourceIntrospectError(null);
      const result = await introspectOneStream(streamName);
      if (!result.ok) {
        setSourceIntrospectError(result.error);
        toast({ title: "Could not execute stored procedure", message: result.error, tone: "error" });
        return null;
      }
      applyPrimaryStreamSchema(streamName, result.intro);
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
    sourceReadMode,
    procedureCall,
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
    const callable = sourceKind === "database"
      && isCallableSourceMode(sourceReadMode)
      && sourceConnector.type !== "mongodb";
    const rawPath = sourceKind === "cloud"
      ? cloudPath.trim()
      : callable
        ? procedureCall.trim()
        : (sourceConnector.type === "mongodb" ? (sourceCollection || sourceTable) : sourceTable);
    const names = sourceKind === "cloud"
      ? (rawPath ? [rawPath] : [])
      : callable
        ? (rawPath ? [procedureStreamName(rawPath)] : [])
        : parseStreamNames(rawPath);
    if (!names.length) {
      setStreamPreviews([]);
      return;
    }

    // Gate on the full stream list so adding/removing a name re-reads schemas.
    const key = `${sourceKind}|${sourceConnectorId}|${sourceReadMode}|${callable ? procedureCall.trim() : names.join("|")}`;
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
    sourceReadMode,
    procedureCall,
  ]);

  const retrySourceIntrospect = useCallback(() => {
    sourceIntrospectGateRef.current = { key: "", status: "idle" };
    sourceIntrospectGenRef.current += 1;
    setSourceIntrospectError(null);
    setSourceIntrospecting(false);
    setAnalyzing(false);
    const callable = sourceKind === "database"
      && isCallableSourceMode(sourceReadMode)
      && sourceConnector?.type !== "mongodb";
    const rawPath = sourceKind === "cloud"
      ? cloudPath.trim()
      : callable
        ? procedureCall.trim()
        : (sourceConnector?.type === "mongodb" ? (sourceCollection || sourceTable) : sourceTable);
    const names = sourceKind === "cloud"
      ? (rawPath ? [rawPath] : [])
      : callable
        ? (rawPath ? [procedureStreamName(rawPath)] : [])
        : parseStreamNames(rawPath);
    if (!sourceConnectorId || !names.length) return;
    const key = `${sourceKind}|${sourceConnectorId}|${sourceReadMode}|${callable ? procedureCall.trim() : names.join("|")}`;
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
    sourceReadMode,
    procedureCall,
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
      let mappingUnitsDone = 0;
      const mappingUnits =
        (destKindMode === "database" ? 1 : 0)
        + 1
        + 1
        + (isMultiStreamSource && multiStreamNames.length > 1 ? 1 : 0);
      const mark = (phase: string) => {
        mappingUnitsDone += 1;
        bump(stagePercent(mappingUnitsDone, mappingUnits + 1), phase);
      };
      mark("Preparing schema context…");
      let freshDestCols = destColumns;
      let freshDestSchema = destSchemaMap;
      let loadedTableExists: boolean | null = destTableExists;
      let loadedConnected: boolean | null = null;
      if (destKindMode === "database") {
        mark("Loading destination schema…");
        let loaded = await loadDestinationSchema({ force: true });
        // One retry — SSL / metadata races often succeed on the second probe.
        if (
          loaded.columns.length === 0
          && loaded.tableExists !== false
          && targetCollection.trim()
          && loaded.connected !== false
        ) {
          bump(stagePercent(mappingUnitsDone, mappingUnits + 1), "Retrying destination schema…");
          await new Promise((r) => window.setTimeout(r, 400));
          loaded = await loadDestinationSchema();
        }
        freshDestCols = loaded.columns;
        freshDestSchema = loaded.schema;
        loadedTableExists = loaded.tableExists;
        loadedConnected = loaded.connected;
      }
      mark("Building transfer plan…");
      await loadTransferPlan();
      let mapped: EditableMapping[] = [];
      // Create-new only when the destination object is confirmed missing (or file export).
      // Unknown existence → schema_pending (never invent CREATE / fake 93% identity).
      const canCreateNew =
        destKindMode !== "database"
        || loadedTableExists === false;
      const schemaPending =
        destKindMode === "database"
        && loadedTableExists == null
        && freshDestCols.length === 0
        && Boolean(targetCollection.trim())
        && loadedConnected !== false;
      const mapTargets =
        freshDestCols.length > 0
          ? freshDestCols
          : canCreateNew || schemaPending
            ? undefined
            : null;
      if (mapTargets === null) {
        toast({
          title:
            loadedConnected === false
              ? "Destination unreachable"
              : "Existing table — columns not loaded",
          message:
            loadedConnected === false
              ? "Could not connect to the destination. Fix the connector (Test on Connectors) before Map invents create-new fields."
              : "Destination table is present, but schema metadata failed. Retry Destination/Map before matching columns (do not treat this as create-new).",
          tone: "warning",
        });
        mapped = columnMappings;
      } else if (freshDestCols.length === 0 && (canCreateNew || schemaPending)) {
        if (loadedTableExists === false) {
          toast({
            title: "New table — create on first write",
            message: `${targetCollection.trim()} was not found. Mapping source columns as create-new fields.`,
            tone: "info",
          });
        } else if (schemaPending) {
          toast({
            title: "Destination schema pending",
            message: `${targetCollection.trim()} was not confirmed. Map stays pending — retry Destination schema load or confirm the table is missing before create-new.`,
            tone: "warning",
          });
          // Keep tri-state null — do not invent destTableExists=false.
        }
        const existsForEmpty =
          loadedTableExists === false
            ? false
            : schemaPending
              ? null
              : loadedTableExists;
        if (sourceKind === "file" && parsed) {
          if (!analysis?.columns.length || !columnMappings.length) {
            bump(stagePercent(mappingUnitsDone, mappingUnits + 1), "Profiling source columns…");
            await runSourceColumnAnalysis(parsed, { manageAnalyzing: false });
          }
          mark(schemaPending ? "Holding map until destination schema confirms…" : "Matching source to create-new fields…");
          mapped = await applyPipelineMappings(undefined, {}, undefined, existsForEmpty) ?? [];
        } else if (analysis?.columns.length || currentSourceColumns.length) {
          mark(schemaPending ? "Holding map until destination schema confirms…" : "Matching source to create-new fields…");
          mapped = await applyPipelineMappings(undefined, {}, undefined, existsForEmpty) ?? [];
        } else {
          toast({
            title: "Source schema required",
            message: "Complete the source step before mapping columns.",
            tone: "warning",
          });
          setStep(STEP_SOURCE);
          return;
        }
      } else if (sourceKind === "file" && parsed) {
        if (!analysis?.columns.length || !columnMappings.length) {
            bump(stagePercent(mappingUnitsDone, mappingUnits + 1), "Profiling source columns…");
            await runSourceColumnAnalysis(parsed, { manageAnalyzing: false });
          }
          mark("Matching source to destination fields…");
        mapped = await applyPipelineMappings(mapTargets, freshDestSchema, undefined, loadedTableExists) ?? [];
      } else if (analysis?.columns.length || currentSourceColumns.length) {
          mark("Matching source to destination fields…");
        mapped = await applyPipelineMappings(mapTargets, freshDestSchema, undefined, loadedTableExists) ?? [];
      } else {
        toast({
          title: "Source schema required",
          message: "Complete the source step before mapping columns.",
          tone: "warning",
        });
        setStep(STEP_SOURCE);
        return;
      }

      // Multi-stream: seed per-stream mappings (copy when schemas match; rematch when they diverge).
      if (isMultiStreamSource && multiStreamNames.length > 1) {
        mark("Mapping each source stream…");
        const primary = primarySourceStream;
        setMapActiveStream(primary);
        // Read latest primary mappings from a dedicated rematch of primary stream columns.
        const primaryMaps = await mapColumnsForStream(primary);
        const primaryResolved = primaryMaps.length ? primaryMaps : mapped;
        if (primaryResolved.length) {
          setColumnMappings(primaryResolved);
        }
        const seeded: Record<string, EditableMapping[]> = {
          [primary]: primaryResolved,
        };
        const colSig = (cols: string[]) =>
          [...cols].map((c) => c.toLowerCase()).sort().join("|");
        const primaryPreview = streamPreviews.find((s) => s.name === primary);
        const primarySig = colSig(primaryPreview?.columns || currentSourceColumns);
        for (const name of multiStreamNames) {
          if (name === primary) continue;
          const preview = streamPreviews.find((s) => s.name === name && s.status === "ok");
          const sig = colSig(preview?.columns || []);
          if (sig && sig === primarySig && seeded[primary]?.length) {
            seeded[name] = seeded[primary];
          } else {
            seeded[name] = await mapColumnsForStream(name);
          }
        }
        setStreamMappings(seeded);
        if (seeded[primary]?.length) {
          setColumnMappings(seeded[primary]);
        }
      }

      bump(100, "Mapping ready");
      await new Promise((r) => window.setTimeout(r, 220));
      if (!mapped.length) {
        toast({
          title: "Mappings did not load",
          message: "Retry mapping, or go back and confirm the destination table schema is reachable.",
          tone: "warning",
        });
      }
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
    const pendingRisk = columnMappings.filter(
      (m) => mappingRequiresRiskAck(m) && !m.riskAcknowledged,
    ).length;
    if (pendingRisk > 0) {
      toast({
        title: "Accept risk before Validate",
        message: `${pendingRisk} column(s) still need Accept risk (lossy/cast/create-new). Approve alone is not enough.`,
        tone: "warning",
      });
      setStep(STEP_MAP);
      return;
    }
    const pendingReview = columnMappings.filter((m) =>
      needsMappingReview(m, threshold),
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
    if (isMultiStreamSource && mapStreamsDiverge) {
      const active = mapActiveStream || primarySourceStream;
      const merged: Record<string, EditableMapping[]> = {
        ...streamMappings,
        [active]: columnMappings,
      };
      const missing = multiStreamNames.filter((n) => !(merged[n]?.length));
      if (missing.length) {
        toast({
          title: "Map every stream",
          message: `Schemas differ — open and map: ${missing.join(", ")}.`,
          tone: "warning",
        });
        setStep(STEP_MAP);
        return;
      }
    }
    setStep(STEP_VALIDATE);
    void executePreflight();
  };

  const approveAllMappings = () => {
    setColumnMappings((prev) => approveMappingsHonestly(prev));
  };

  /**
   * Validate's forward door — sign holdout Risk Contracts in place and re-run,
   * instead of bouncing to Map. Failing rows go to the DLQ for replay; nothing
   * is written lossily and nothing is silently dropped.
   */
  const holdOutRowsAndRevalidate = async () => {
    const { mappings: next, signed } = holdOutRowsAndContinue(columnMappings, {
      rowsSampled: parsed?.data?.length ?? parsed?.sample_data?.length ?? 0,
      estimatedRows: parsed?.row_count ?? sourceRowEstimate ?? null,
      planId: persistedPlanId || undefined,
      table: targetCollection || "",
    });
    if (!signed.length) {
      toast({
        title: "Nothing to hold out",
        message: "No column is blocking on unacknowledged fidelity risk.",
        tone: "warning",
      });
      return;
    }
    setColumnMappings(next);
    toast({
      title: "Running with rows held out",
      message: `Signed a quarantine Risk Contract for ${signed.length} column(s): ${signed
        .slice(0, 3)
        .join(", ")}${signed.length > 3 ? "…" : ""}. Failing rows go to quarantine for replay — re-validating…`,
      tone: "success",
    });
    await executePreflight(next);
  };

  /**
   * G15 Validate door — stamp false_friend_confirmed in place and re-run.
   * Approve eligible must not call this. Remap still lives on Map.
   */
  const confirmFalseFriendsAndRevalidate = async (sources?: string[]) => {
    const { mappings: next, confirmed, blocked, unmatched } = confirmFalseFriendsBySource(
      columnMappings,
      sources,
    );
    if (blocked.length && !confirmed.length) {
      toast({
        title: "Cannot confirm this pair yet",
        message: `${blocked.slice(0, 3).join(", ")} still needs Accept risk or a type remap on Map.`,
        tone: "warning",
      });
      if (blocked[0]) setMapFocusSource(blocked[0]);
      setStep(STEP_MAP);
      return;
    }
    if (!confirmed.length) {
      toast({
        title: unmatched.length ? "Column is not a false-friend" : "No false-friend pair to confirm",
        message: unmatched.length
          ? `${unmatched.slice(0, 3).join(", ")} is not waiting on Confirm this pair. Open Map to remap.`
          : "Open Map to remap, or re-run Validate if the pair was already confirmed.",
        tone: "warning",
      });
      if (unmatched[0] || sources?.[0]) setMapFocusSource(unmatched[0] || sources?.[0] || "");
      setStep(STEP_MAP);
      return;
    }
    setColumnMappings(next);
    toast({
      title: "Pair confirmed — re-validating",
      message: `Confirmed ${confirmed.slice(0, 3).join(", ")}${confirmed.length > 3 ? "…" : ""}. Re-running Validate.`,
      tone: "success",
    });
    await executePreflight(next);
  };

  const approveAllAndPreflight = async () => {
    const approved = approveMappingsHonestly(columnMappings);
    setColumnMappings(approved);
    const pendingRisk = approved.filter(
      (m) => mappingRequiresRiskAck(m) && !m.riskAcknowledged,
    ).length;
    const pendingReview = approved.filter((m) =>
      needsMappingReview(m, confidenceThreshold),
    ).length;
    if (pendingRisk > 0 || pendingReview > 0) {
      toast({
        title: pendingRisk > 0 ? "Accept risk before Validate" : "Review column mappings",
        message: pendingRisk > 0
          ? `${pendingRisk} column(s) still need Accept risk — Approve alone cannot clear them.`
          : `${pendingReview} column(s) still need Approve before validation.`,
        tone: "warning",
      });
      setStep(STEP_MAP);
      return;
    }
    setStep(STEP_VALIDATE);
    await executePreflight(approved);
  };

  const stripControlCharsAndRerun = async (modeOverride?: ValidationMode): Promise<RemediationOpResult> => {
    const typed = new Set<MappingTransform>([
      "cast_number",
      "cast_integer",
      "cast_boolean",
      "date_iso",
      "time_iso",
      "parse_json",
      "hash_pii",
      "binary",
      "currency",
      "percentage",
      "identity_specialty",
    ]);
    const changed: string[] = [];
    const next = columnMappings.map((m) => {
      if (m.transform && typed.has(m.transform)) {
        return sealRemediationApproval({ ...m, approved: true });
      }
      const label = m.target ? `${m.source} → ${m.target}` : m.source;
      changed.push(label);
      return sealRemediationApproval({
        ...m,
        transform: "strip_controls" as MappingTransform,
        approved: true,
      });
    });
    const mode = modeOverride ?? validationMode;
    setColumnMappings(next);
    toast({
      title: "Strip controls applied",
      message: `Applied strip_controls to ${changed.length} text mapping${changed.length === 1 ? "" : "s"}. Re-running validation…`,
      tone: "success",
    });
    await executePreflight(next, mode);
    return {
      kind: "strip_controls",
      title: "Strip control characters",
      columnsChanged: changed,
      validationMode: mode,
      steps: [
        `Applied strip_controls to ${changed.length} mapping(s); left typed casts (number/date/boolean/json/hash) unchanged.`,
        changed.length
          ? `Columns: ${changed.slice(0, 20).join(", ")}${changed.length > 20 ? ` (+${changed.length - 20} more)` : ""}.`
          : "No text mappings required strip_controls.",
        `Re-ran Validate in ${mode} mode.`,
        "At Execute, cleaned values are written. Jobs quarantine only lists cells that still fail after Strip.",
      ],
    };
  };

  const quarantineAndRerun = async (): Promise<RemediationOpResult | void> => {
    // Wrong column maps (status → boolean date flag) cannot be fixed by
    // quarantine/strip — send the operator back to Map with a clear reason.
    const dry = preflight?.gates?.find((g) => /dry_run|integrity/i.test(g.id));
    const dryMsg = `${dry?.message || ""} ${JSON.stringify(dry?.details || {})}`;
    const allGateText = (preflight?.gates ?? [])
      .map((g) => `${g.message || ""} ${JSON.stringify(g.details || {})}`)
      .join(" ");
    const blockerText = (preflight?.blockers ?? []).map((b) => b.message || "").join(" ");
    const integrityText = `${dryMsg} ${allGateText} ${blockerText}`;
    // Duplicate identity keys survive Strip/Quarantine/balanced — write-time DQ
    // still fails. Never switch to balanced and falsely enable Execute.
    if (
      duplicateKeyRoot
      || /duplicate (primary )?key|keys repeat|identity-key|source probe/i.test(integrityText)
    ) {
      toast({
        title: "Cannot quarantine duplicate identity keys",
        message:
          "Strip/Quarantine only sanitize encoding. Open Destination → Advanced and set Primary key "
          + "to a column that is unique in the source, or use append without that PK / dedupe upstream — then Re-run Validate.",
        tone: "warning",
      });
      openIdentitySettings();
      return;
    }
    const encodingOnly = isEncodingIntegritySignal(dryMsg)
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
      message:
        "Applying strip_controls and balanced validation so format-control characters are sanitized before run. "
        + "Jobs will only list quarantined rows if cells still fail at write — a clean Strip often means 0 quarantined.",
      tone: "info",
    });
    const stripResult = await stripControlCharsAndRerun("balanced");
    return {
      kind: "quarantine_strip",
      title: "Quarantine + strip controls",
      columnsChanged: stripResult.columnsChanged,
      columnsFlagged: stripResult.columnsFlagged,
      validationMode: "balanced",
      steps: [
        "Switched validation mode to balanced (quarantine-friendly posture).",
        ...stripResult.steps,
        "If Strip clears encoding issues, Jobs correctly shows 0 quarantined after a successful run.",
      ],
    };
  };

  const applyCreateNewTypeWidens = (actions: ValidationSuggestedAction[]) => {
    const widens = actions.filter((a) => (
      a.kind === "change_target_type"
      && a.to_type
      && a.requires_ddl !== true
      && a.mapping_applyable !== false
      && a.apply_proven !== false
    ));
    if (!widens.length) return;
    let hit = 0;
    const next = columnMappings.map((m) => {
      const action = widens.find(
        (a) => (a.target && m.target === a.target) || (a.column && m.source === a.column),
      );
      if (!action?.to_type) return m;
      if (m.existsInDestination === true) return m;
      hit += 1;
      const widenClearsCast =
        /varchar|text|string|char|longtext|double|float|decimal|numeric|number|real/i.test(
          action.to_type || "",
        );
      const nextTransform =
        widenClearsCast
        && (m.transform === "cast_integer"
          || m.transform === "cast_number"
          || m.transform === "cast_boolean"
          || m.transform === "date_iso"
          || m.transform === "time_iso")
          ? "none"
          : m.transform;
      return sealRemediationApproval({
        ...m,
        destType: action.to_type,
        transform: nextTransform,
        approved: true,
        requiresReview: false,
      });
    });
    if (!hit) {
      setStep(STEP_MAP);
      toast({
        title: "Open Map to widen types",
        message: "These columns already exist on the destination — Map type cannot ALTER live DDL.",
        tone: "warning",
      });
      return;
    }
    setColumnMappings(next);
    toast({
      title: "CREATE types updated — re-validating",
      message: `Updated ${hit} Map type(s) so the new table can hold the source. The destination is not written yet.`,
      tone: "success",
    });
    void executePreflight(next);
  };

  /** Map an AI `suggested_action` onto the real Studio controls. */
  const applySuggestedAction = (action: ValidationSuggestedAction) => {
    const matches = (m: EditableMapping) =>
      (action.target && m.target === action.target) || (action.column && m.source === action.column);

    switch (action.kind) {
      case "change_target_type": {
        if (!action.to_type) return;
        let hit = false;
        let remapped = false;
        const usedTargets = new Set(columnMappings.map((m) => m.target.toLowerCase()));
        const isTextType = (t: string) => /varchar|text|string|char|variant/i.test(t || "");
        const next = columnMappings.map((m) => {
          if (!matches(m)) return m;
          hit = true;
          if (m.existsInDestination === false) {
            // Widen create-new type must clear typed casts — cast_integer on
            // LONGTEXT/DOUBLE keeps Invalid integer after Apply (Validate lie).
            const widenClearsCast =
              /varchar|text|string|char|longtext|double|float|decimal|numeric|number|real/i.test(
                action.to_type || "",
              );
            const nextTransform =
              widenClearsCast
              && (m.transform === "cast_integer"
                || m.transform === "cast_number"
                || m.transform === "cast_boolean"
                || m.transform === "date_iso"
                || m.transform === "time_iso")
                ? "none"
                : m.transform;
            return sealRemediationApproval({
              ...m,
              destType: action.to_type,
              transform: nextTransform,
              approved: true,
              requiresReview: false,
            });
          }
          if (m.existsInDestination !== true) {
            // Unknown / pending schema — do not invent create-new type rewrite.
            return m;
          }
          // Existing DDL cannot be widened by mapping alone.
          // A NUMBER/DECIMAL dest-widen must not dump clock/money values into
          // a leftover TEXT sibling — that is a fidelity collapse.
          const isNumericWiden = /^(number|decimal|numeric|bignumeric)\b/i.test(
            (action.to_type || "").trim(),
          );
          const freeText = isNumericWiden
            ? undefined
            : destColumns.find((c) => {
            const lower = c.toLowerCase();
            if (usedTargets.has(lower) && lower !== m.target.toLowerCase()) return false;
            return isTextType(destSchemaMap[c] || "");
          });
          if (freeText && freeText.toLowerCase() !== m.target.toLowerCase()) {
            usedTargets.delete(m.target.toLowerCase());
            usedTargets.add(freeText.toLowerCase());
            remapped = true;
            return sealRemediationApproval({
              ...m,
              target: freeText,
              destType: destSchemaMap[freeText] || action.to_type,
              existsInDestination: true,
              approved: true,
              requiresReview: false,
              reason: [
                m.reason,
                `Remapped off incompatible ${m.destType || "typed"} column → ${freeText} (${action.to_type})`,
              ]
                .filter(Boolean)
                .join(" · "),
            });
          }
          // Prefer original source name for ADD COLUMN (_id stays _id).
          // Stripping underscores first produced id_text beside DECIMAL id and
          // Snowflake failed with invalid identifier when ADD COLUMN was skipped.
          let candidate = m.source;
          const isTaken = (name: string) =>
            usedTargets.has(name.toLowerCase())
            || destColumns.some((c) => c.toLowerCase() === name.toLowerCase());
          if (!candidate || isTaken(candidate)) {
            const base = (m.source || "field").replace(/[^\w]+/g, "_").replace(/^_+|_+$/g, "") || "field";
            candidate = `${m.source.startsWith("_") ? m.source : base}${isNumericWiden ? "_wide" : "_text"}`;
            if (isTaken(candidate)) {
              candidate = `src_${base}`;
            }
          }
          let n = 2;
          const baseName = candidate;
          while (isTaken(candidate)) {
            candidate = `${baseName}_${n}`;
            n += 1;
          }
          usedTargets.delete(m.target.toLowerCase());
          usedTargets.add(candidate.toLowerCase());
          remapped = true;
          return sealRemediationApproval({
            ...m,
            target: candidate,
            destType: action.to_type,
            existsInDestination: false,
            createNew: true,
            assignmentStrategy: "create_compatible_new",
            transform:
              m.transform === "cast_number"
              || m.transform === "cast_boolean"
              || m.transform === "cast_integer"
              || m.transform === "date_iso"
              || m.transform === "time_iso"
                ? "none"
                : m.transform,
            approved: true,
            requiresReview: false,
            reason: [
              m.reason,
              `Destination ${m.target} is typed ${m.destType || "?"} — mapping to new ${candidate} as ${action.to_type}`,
            ]
              .filter(Boolean)
              .join(" · "),
          });
        });
        setColumnMappings(next);
        if (!hit) {
          toast({
            title: "Column not found",
            message: `Couldn't find '${action.column ?? action.target}' in the current mappings.`,
            tone: "warning",
          });
          return;
        }
        toast({
          title: remapped ? "Remapped to compatible type — re-validating" : "Target type updated — re-validating",
          message: remapped
            ? `${action.column ?? action.target} no longer targets an incompatible typed column. Re-running Validate.`
            : `Changed ${action.column ?? action.target} → type ${action.to_type}. Re-running Validate.`,
          tone: "success",
        });
        void executePreflight(next);
        break;
      }
      case "normalize_control_chars":
      case "open_bad_data_fix":
        setStep(STEP_VALIDATE);
        setBadDataFixOpen(true);
        break;
      case "quarantine_and_rerun": {
        if (duplicateKeyRoot) {
          openIdentitySettings();
          break;
        }
        setStep(STEP_VALIDATE);
        setBadDataFixOpen(true);
        break;
      }
      case "add_transform": {
        const uiTransform = action.transform
          ? (ENGINE_TO_UI_TRANSFORM[action.transform] || engineTransformToUi(action.transform) || (action.transform as MappingTransform))
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
            return sealRemediationApproval({
              ...m,
              transform: uiTransform,
              approved: true,
              requiresReview: false,
              riskAcknowledged: false,
            });
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
      case "map_column": {
        const dest = action.target;
        if (!dest) {
          toast({
            title: "Missing target column",
            message: "AI suggested a remap without a destination column. Open Map to pick one.",
            tone: "warning",
          });
          setStep(STEP_MAP);
          return;
        }
        let hit = false;
        const next = columnMappings.map((m) => {
          if (!matches(m)) return m;
          hit = true;
          return sealRemediationApproval({
            ...m,
            target: dest,
            destType: destSchemaMap[dest] || m.destType,
            existsInDestination: destColumns.some((c) => c.toLowerCase() === dest.toLowerCase()),
            approved: true,
            requiresReview: false,
            reason: [m.reason, `Remapped → ${dest}`].filter(Boolean).join(" · "),
          });
        });
        setColumnMappings(next);
        toast({
          title: hit ? "Column remapped — re-validating" : "Column not found",
          message: hit
            ? `Mapped '${action.column ?? "?"}' → '${dest}'. Re-running Validate.`
            : `Couldn't find '${action.column ?? "?"}' in the current mappings.`,
          tone: hit ? "success" : "warning",
        });
        if (hit) void executePreflight(next);
        break;
      }
      case "fix_source_keys":
        setMapIdentityBanner(
          action.column
            ? `Duplicate values on ${action.column} blocked Validate — change primary key or sync mode in Advanced settings.`
            : "Duplicate identity keys blocked Validate — change primary key or sync mode in Advanced settings.",
        );
        openIdentitySettings();
        break;
      case "review_mappings":
      case "rerun_mapping":
        if (action.column) setMapFocusSource(action.column);
        setStep(STEP_MAP);
        toast({
          title: "Opened Map step",
          message:
            action.kind === "rerun_mapping"
              ? "Re-run mapping to accept the new schema, then re-run preflight."
              : "Review mappings, then re-run Validate.",
          tone: "info",
        });
        break;
      case "confirm_or_remap":
        void confirmFalseFriendsAndRevalidate(action.column ? [action.column] : undefined);
        break;
      case "confirm_add":
        if (action.column) setMapFocusSource(action.column);
        setStep(STEP_MAP);
        toast({
          title: "Opened Map step",
          message: "Review ADD COLUMN proposals, then re-run Validate.",
          tone: "info",
        });
        break;
      case "reload_dest_schema":
        void loadDestinationSchema();
        toast({
          title: "Reloading destination schema",
          message: "Re-introspecting dest columns, then return to Validate.",
          tone: "info",
        });
        break;
      case "continue_validate":
        void executePreflight();
        break;
      case "run_population_orphan_scan": {
        const plan = planFkOrphanSuggestedAction({ kind: action.kind, column: action.column });
        if (!plan) break;
        setRunPopulationOrphanScan(true);
        toast({
          title: plan.toastTitle,
          message: plan.toastMessage,
          tone: plan.toastTone,
        });
        void executePreflight(undefined, undefined, { runPopulationOrphanScan: true });
        break;
      }
      case "fix_orphans": {
        const plan = planFkOrphanSuggestedAction({ kind: action.kind, column: action.column });
        if (!plan) break;
        if (plan.focusSource) setMapFocusSource(plan.focusSource);
        if (plan.goToMap) setStep(STEP_MAP);
        toast({
          title: plan.toastTitle,
          message: plan.toastMessage,
          tone: plan.toastTone,
        });
        break;
      }
      case "check_connection":
        window.location.hash = "#/connectors";
        toast({
          title: "Opened Connectors",
          message: "Fix credentials, Test until green, then return to Validate and Re-run.",
          tone: "info",
        });
        break;
      default:
        break;
    }
  };

  const executePreflight = async (
    overrideMappings?: EditableMapping[],
    validationOverride?: ValidationMode,
    opts?: {
      complianceAcknowledged?: boolean;
      schemaDriftAcknowledged?: boolean;
      fkRiskAcknowledged?: boolean;
      acknowledgmentReason?: string;
      runPopulationOrphanScan?: boolean;
    },
  ) => {
    const activeMappings = overrideMappings ?? columnMappings;
    const activeValidation = validationOverride ?? validationMode;
    const ackCompliance = opts?.complianceAcknowledged ?? complianceAcknowledged;
    const ackSchemaDrift = opts?.schemaDriftAcknowledged ?? schemaDriftAcknowledged;
    const ackFkRisk = opts?.fkRiskAcknowledged ?? fkRiskAcknowledged;
    const ackActor = readSession()?.email || readSession()?.name || "";
    const ackReason = (opts?.acknowledgmentReason || "").trim()
      || standingAcknowledgmentReason({
        compliance: ackCompliance,
        schemaDrift: ackSchemaDrift,
        fkRisk: ackFkRisk,
      });
    if (
      (opts?.complianceAcknowledged || opts?.schemaDriftAcknowledged || opts?.fkRiskAcknowledged)
      && (!ackActor || ackActor.length < 2)
    ) {
      toast({
        title: "Sign in required for acknowledgment",
        message: "PII, schema-drift, and FK-risk acknowledgments need a signed-in operator identity.",
        tone: "warning",
      });
      return;
    }
    if (
      (opts?.complianceAcknowledged || opts?.schemaDriftAcknowledged || opts?.fkRiskAcknowledged)
      && ackReason.trim().length < 8
    ) {
      toast({
        title: "Reason required",
        message: "Provide a clear acknowledgment reason before re-validating.",
        tone: "warning",
      });
      return;
    }
    const ackClaimed = Boolean(
      opts?.complianceAcknowledged
      || opts?.schemaDriftAcknowledged
      || opts?.fkRiskAcknowledged,
    );
    // An attestation sticks only when the API says it recorded one. Claiming it
    // optimistically shows "PII approved" over a run that is still blocked.
    const commitAcknowledgments = (pf: PreflightResult | null) => {
      if (opts?.complianceAcknowledged) {
        const recorded = pf?.proof_bundle?.compliance?.acknowledged === true;
        setComplianceAcknowledged(recorded);
        toast(recorded
          ? {
            title: "PII approval recorded",
            message: pf?.proof_bundle?.transfer_decision?.decision === "approve"
              ? "Governance approval accepted — Execute is unlocked."
              : "Governance approval accepted. Remaining blockers still hold Execute.",
            tone: pf?.proof_bundle?.transfer_decision?.decision === "approve" ? "success" : "warning",
          }
          : {
            title: "PII approval not recorded",
            message: "The API did not return an acknowledged compliance record — Execute stays locked.",
            tone: "error",
          });
      }
      if (opts?.schemaDriftAcknowledged) setSchemaDriftAcknowledged(Boolean(pf));
      if (opts?.fkRiskAcknowledged) setFkRiskAcknowledged(Boolean(pf));
    };
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
    const pendingReview = activeMappings.filter((m) =>
      needsMappingReview(m, threshold),
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
    if (writeViaStaging && !writeViaStagingSupported) {
      toast({
        title: "Staging not supported",
        message: "Write via staging requires a SQL table destination. Turn it off in Advanced, or pick a SQL sink.",
        tone: "error",
      });
      return;
    }
    if (multiStreamUnsupportedMode) {
      toast({
        title: "Sync mode not supported",
        message: "SCD2 and Mirror are not available for multi-stream transfers. Switch to full refresh, incremental, or CDC.",
        tone: "error",
      });
      return;
    }
    if (!routeSyncModes.some((m) => m.id === syncMode)) {
      toast({
        title: "Sync mode not supported",
        message: "This sync mode is not available for the current source and destination. Open Advanced and pick a supported mode.",
        tone: "error",
      });
      return;
    }
    setPreflighting(true);
    setValidateProgress(null);
    setStep(STEP_VALIDATE);
    setPreflight(null);
    setPreflightError("");
    setValidatedContractKey(null);
    // Resolved outside try so a transport catch can name the same count
    // the run already computed — `number | null` is the Studio estimate type.
    let rowCount = 0;
    try {
      let columns: string[] = [];
      let columnTypes: Record<string, string> = {};
      let mappings: { source: string; target: string; confidence: number; reason?: string }[] = [];
      let sampleRows: Record<string, unknown>[] | undefined;
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
        sampleRows = (parsed.data ?? parsed.sample_data)?.slice(0, PREFLIGHT_SAMPLE_LIMIT);
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
          sampleRows = (parsed?.data ?? parsed?.sample_data)?.slice(0, PREFLIGHT_SAMPLE_LIMIT);
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
          // The plan transport must carry the attestation — a body-less call
          // cannot clear a PII/drift/FK gate, and Execute would stay locked.
          const pf = await preflightTransferPlan(
            planId,
            {
              compliance_acknowledged: ackCompliance,
              schema_drift_acknowledged: ackSchemaDrift,
              fk_risk_acknowledged: ackFkRisk,
              acknowledgment_actor: ackActor,
              acknowledgment_reason: ackReason,
            },
            recipePayload(shapeSteps),
            setValidateProgress,
          );
          // Never stamp plan approved on review-grade / soft-pass — Execute
          // unlock requires decision===approve (same bar as Validate rail).
          const decision = pf.proof_bundle?.transfer_decision?.decision;
          if (pf.passed && decision === "approve") {
            await approveTransferPlan(planId);
          }
          setPreflight(pf);
          setValidatedContractKey(buildValidateContractKey(activeMappings));
          commitAcknowledgments(pf as PreflightResult);
          if (ackClaimed) return;
          if (!pf.passed) {
            toast({
              title: "Validation incomplete",
              message: pf.blockers?.[0]?.message ?? `${pf.blockers?.length ?? 0} check(s) failed — use the fix actions below.`,
              tone: "warning",
            });
          } else {
            // Stay on Validate — never claim "Ready" on review-grade.
            toast({
              title: decision === "approve" ? "Preflight passed" : "Review-grade preflight",
              message: decision === "approve"
                ? `All ${pf.total_gates} API checks passed. Review gate cards, then Execute when ready.`
                : "Checks completed with review-grade decision — re-run or fix blockers before Execute unlocks.",
              tone: decision === "approve" ? "success" : "warning",
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
          source_table: isConnectorSource
            ? (
              sourceKind === "cloud"
                ? (cloudPath.trim() || undefined)
                : sourceKind === "database" && sourceConnector?.type !== "mongodb"
                  && sourceReadMode === "table"
                  ? (sourceTable || undefined)
                  : undefined
            )
            : undefined,
          source_config: isConnectorSource && (sourceKind === "database" || sourceKind === "cloud")
            ? {
                type: sourceConnector?.type,
                db_type: sourceConnector?.type,
                source_read_mode: sourceReadMode,
                procedure_call: sourceReadMode === "procedure" ? procedureCall.trim() : undefined,
                source_query: sourceReadMode === "query" ? procedureCall.trim() : undefined,
                procedure_params: Object.keys(procedureParams).length ? procedureParams : undefined,
                extra: {
                  source_read_mode: sourceReadMode,
                  procedure_call: sourceReadMode === "procedure" ? procedureCall.trim() : "",
                  source_query: sourceReadMode === "query" ? procedureCall.trim() : "",
                  procedure_params: procedureParams,
                },
              }
            : undefined,
          source_collection: isConnectorSource && sourceKind === "database" && sourceConnector?.type === "mongodb"
            ? (sourceCollection || undefined)
            : undefined,
          // Always send driver type even with a saved connector — Validate must not
          // default db_type to postgresql and invent SQL DDL / fingerprint blocks.
          dest_type: destKindMode === "database"
            ? (destDriverType || destType || selectedDestConnector?.type || undefined)
            : undefined,
          dest_host: destKindMode === "database" && !connectorId ? destHost : undefined,
          dest_port: destKindMode === "database" && !connectorId ? destPort : undefined,
          dest_database: destKindMode === "database" && !connectorId ? targetDb : undefined,
          dest_username: destKindMode === "database" && !connectorId ? destUsername || undefined : undefined,
          dest_password: destKindMode === "database" && !connectorId ? destPassword || undefined : undefined,
          dest_connection_string: destKindMode === "database" && !connectorId ? destConnectionString || undefined : undefined,
          // Always send Studio schema — even with a saved connector. Omitting it
          // made Validate inspect public/default while Map/Execute used railway
          // (false create-new / "Projected CREATE · not dest-proven").
          dest_schema: destKindMode === "database"
            ? (foldSchemaForDriver(destDriverType || destType, destSchema) || undefined)
            : undefined,
          dest_warehouse: destKindMode === "database" && destDriverType === "snowflake" ? destWarehouse || undefined : undefined,
          dest_auth_source: destKindMode === "database"
            ? (selectedDestConnector?.auth_source || undefined)
            : undefined,
          dest_auth_mode: destKindMode === "database"
            ? (selectedDestConnector?.auth_mode || undefined)
            : undefined,
          dest_auth_role: destKindMode === "database"
            ? (selectedDestConnector?.auth_role || undefined)
            : undefined,
          // Live dest schema — required so existing BOOLEAN columns are not invisible to DDL gates.
          dest_table: destKindMode === "database" && destDriverType !== "mongodb" && destDriverType !== "dynamodb"
            ? (targetCollection || undefined)
            : undefined,
          dest_collection: destKindMode === "database" && (destDriverType === "mongodb" || destDriverType === "dynamodb")
            ? (targetCollection || undefined)
            : undefined,
          destination_column_types:
            destKindMode === "database"
            && destTableExists === true
            && Object.keys(destSchemaMap).length
              ? destSchemaMap
              : undefined,
          // Validate must score the rows the write will carry: the run shapes on
          // the read, so a gate judging the raw source refuses values the
          // transform already removed.
          shape_recipe: recipePayload(shapeSteps),
          sample_rows: sampleRows,
          estimated_bytes: estimatedBytes,
          sync_mode: syncMode,
          delivery_guarantee: studioDeliveryGuarantee({
            syncMode,
            deliveryGuarantee,
            allowAppendOnly,
            callableSource: sourceReadMode === "procedure" || sourceReadMode === "query",
          }),
          dest_extra: { allow_append_only: allowAppendOnly },
          schema_policy: schemaPolicy,
          validation_mode: validationOverride ?? validationMode,
          date_locale: dateLocale,
          number_locale: numberLocale,
          backfill_new_fields: backfillNewFields,
          stream_contracts: streamContracts,
          compliance_acknowledged: ackCompliance,
          schema_drift_acknowledged: ackSchemaDrift,
          fk_risk_acknowledged: ackFkRisk,
          run_population_orphan_scan: resolvePopulationOrphanScanFlag(
            opts?.runPopulationOrphanScan,
            runPopulationOrphanScan,
          ),
          acknowledgment_actor: ackActor || undefined,
          acknowledgment_reason: ackReason || undefined,
          write_via_staging: writeViaStaging,
          priority_column: priorityColumn || undefined,
          priority_direction: priorityColumn ? priorityDirection : undefined,
          row_limit: rowLimit > 0 ? rowLimit : undefined,
          source_kind: sourceKind,
          source_type: resolveDriverType(sourceConnector?.type || "") || undefined,
          ...(parsed?.file_id ? { source_file_id: parsed.file_id } : {}),
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
            sourceReadMode,
            destWriteMode,
            syncMode,
            numberLocale,
            dateLocale,
          });
          toast({
            title: "Validated locally",
            message: "Server preflight timed out — browser preview gates passed for file export. Re-run Validate when the API responds. Data rules still apply before write.",
            tone: "warning",
          });
        } else {
          throw apiErr;
        }
      }
      setPreflight(pf);
      commitAcknowledgments(isApiPreflight(pf) ? pf : null);
      // Echo Kernel stamps + signed Risk Contracts from Validate onto Map.
      // Contract key MUST use post-hydrate mappings or Execute stays locked /
      // invalidation clears a green preflight when destType stamps change.
      let hydrateMaps = activeMappings;
      if (
        (Array.isArray(pf.stamped_mappings) && pf.stamped_mappings.length)
        || (Array.isArray(pf.signed_mappings) && pf.signed_mappings.length)
      ) {
        hydrateMaps = mergeSignedRiskContracts(
          mergeStampedTargetTypes(activeMappings, pf.stamped_mappings),
          pf.signed_mappings,
        );
        setColumnMappings(hydrateMaps);
      }
      setValidatedContractKey(buildValidateContractKey(hydrateMaps));
      if (ackClaimed) return;
      if (!pf.passed) {
        toast({
          title: "Validation incomplete",
          message: pf.blockers[0]?.message ?? `${pf.blockers.length} check(s) failed — use the fix actions below.`,
          tone: "warning",
        });
      } else {
        const isLocal = !isApiPreflight(pf);
        const decision = pf.proof_bundle?.transfer_decision?.decision;
        if (isLocal || decision === "review") {
          toast({
            title: isLocal ? "Validated locally" : "Review-grade preflight",
            message: isLocal
              ? "Browser preview gates only — re-run Validate when the API responds. Execute stays locked until API approve."
              : "API returned review-grade — fix blockers or acknowledge policy, then re-run before Execute.",
            tone: "warning",
          });
        } else {
          toast({
            title: "Preflight passed",
            message: `All ${pf.total_gates} API checks passed. Review the gate cards, then Execute when ready.`,
            tone: "success",
          });
        }
      }
    } catch (e) {
      if (sourceKind === "file" && destKindMode === "file_export" && parsed) {
        const threshold = confidenceThresholdForMode(validationOverride ?? validationMode);
        const pf = runLocalPreflight({
          columns: parsed.columns,
          rowCount: parsed.row_count,
          mappings: overrideMappings ?? columnMappings,
          sampleRows: (parsed.data ?? parsed.sample_data)?.slice(0, PREFLIGHT_SAMPLE_LIMIT),
          confidenceThreshold: threshold,
          destKind: destKindMode,
          sourceReadMode,
          destWriteMode,
          syncMode,
          numberLocale,
          dateLocale,
        });
        setPreflight(pf);
        setValidatedContractKey(buildValidateContractKey(activeMappings));
        if (pf.passed) {
          toast({
            title: "Validated locally",
            message: "Server preflight timed out — browser preview gates passed. Re-run Validate when the API responds, then Execute. Data rules still apply before write.",
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
        const raw = e instanceof Error ? e.message : "Validation could not complete.";
        const message = validateTransportMessage(
          raw,
          rowCount > 0 ? rowCount : sourceRowEstimate,
        );
        setPreflightError(message);
        toast({
          title: isTransportFailure(raw) ? "Validate timed out" : "Preflight failed",
          message,
          tone: "error",
        });
        console.error(e);
      }
    } finally {
      setPreflighting(false);
    }
  };

  const primaryFix = useMemo(() => {
    if (duplicateKeyRoot) {
      return {
        onPrimaryFix: openIdentitySettings,
        primaryFixLabel: duplicateKeyRoot.primaryKey
          ? `Fix identity (${duplicateKeyRoot.primaryKey})`
          : "Fix identity / sync mode",
      };
    }
    if (!preflight) return { onPrimaryFix: undefined, primaryFixLabel: undefined };
    const firstBlocker = buildDisplayBlockers(preflight, syncMode)[0];
    const blockerBlob = `${firstBlocker?.message || ""} ${firstBlocker?.impact || ""} ${JSON.stringify(firstBlocker?.source?.details || {})}`;
    // Encoding beats generic gate-rulebook review_mappings so rail matches dashboard.
    // Never match bare "encoding" — column names like encoding_id must stay on Map.
    if (isEncodingIntegritySignal(blockerBlob)) {
      return {
        onPrimaryFix: () => setBadDataFixOpen(true),
        primaryFixLabel: "Fix bad data…",
      };
    }
    const applyPromoted = (promoted: PromotedPrimaryFix) => {
      switch (promoted.destination) {
        case "map":
          return { onPrimaryFix: () => setStep(STEP_MAP), primaryFixLabel: promoted.label };
        case "connectors":
          return {
            onPrimaryFix: () => { window.location.hash = "#/connectors"; },
            primaryFixLabel: promoted.label,
          };
        case "ack_drift":
          return {
            onPrimaryFix: () => {
              toast({
                title: "Submitting schema-drift acknowledgment",
                message: "Re-running Validate — existing mappings kept for this run (exception recorded).",
                tone: "info",
              });
              void executePreflight(undefined, undefined, {
                schemaDriftAcknowledged: true,
                acknowledgmentReason: "Keep existing mappings for this run; ignore new/changed columns",
              });
            },
            primaryFixLabel: promoted.label,
          };
        case "ack_pii":
          return {
            onPrimaryFix: () => {
              toast({
                title: "Submitting PII approval",
                message: "Re-running Validate with governance approval for detected PII fields.",
                tone: "info",
              });
              void executePreflight(undefined, undefined, {
                complianceAcknowledged: true,
                acknowledgmentReason: "Governance policy allows moving detected PII for this transfer",
              });
            },
            primaryFixLabel: promoted.label,
          };
        case "ack_fk":
          return {
            onPrimaryFix: () => {
              toast({
                title: "Submitting FK-risk acknowledgment",
                message: "Re-running Validate — FK mapping risk accepted for this run (RI not proven).",
                tone: "info",
              });
              void executePreflight(undefined, undefined, {
                fkRiskAcknowledged: true,
                acknowledgmentReason: "Accept destination FK mapping risk for this run; population orphans not proven",
              });
            },
            primaryFixLabel: promoted.label,
          };
        default:
          return { onPrimaryFix: undefined, primaryFixLabel: undefined };
      }
    };

    const g15Cta = destExistsPrimaryCta(shapeContractFromPreflight(preflight));
    const ranked = rankAndDedupeSuggestedActions(firstBlocker?.suggested_actions);
    const widens = createNewFitWidenActions(preflight);
    if (widens.length > 0) {
      return {
        onPrimaryFix: () => applyCreateNewTypeWidens(widens),
        primaryFixLabel: createNewFitWidenLabel(widens),
      };
    }
    const action = ranked[0]
      || (g15Cta
        ? { kind: g15Cta.kind, label: g15Cta.label, column: g15Cta.column }
        : undefined);
    if (!action) return applyPromoted(promoteBlockedPrimaryFix(firstBlocker));

    switch (action.kind) {
      case "review_mappings":
      case "change_target_type":
      case "add_transform":
      case "map_column": {
        const details = firstBlocker?.source?.details ?? null;
        const label = schemaDriftRequiresRemap(details)
          ? "Open Map to fix breaking change"
          : action.label;
        return { onPrimaryFix: () => setStep(STEP_MAP), primaryFixLabel: label };
      }
      case "confirm_or_remap":
        return {
          onPrimaryFix: () => {
            void confirmFalseFriendsAndRevalidate(action.column ? [action.column] : undefined);
          },
          primaryFixLabel: action.label || "Confirm this pair",
        };
      case "confirm_add":
        return {
          onPrimaryFix: () => {
            if (action.column) setMapFocusSource(action.column);
            setStep(STEP_MAP);
          },
          primaryFixLabel: action.label,
        };
      case "reload_dest_schema":
        return {
          onPrimaryFix: () => { void loadDestinationSchema(); },
          primaryFixLabel: action.label,
        };
      case "continue_validate":
        return {
          onPrimaryFix: () => { void executePreflight(); },
          primaryFixLabel: action.label,
        };
      case "normalize_control_chars":
      case "open_bad_data_fix":
        return {
          onPrimaryFix: () => setBadDataFixOpen(true),
          primaryFixLabel: "Fix bad data…",
        };
      case "rerun_mapping":
        return {
          onPrimaryFix: () => {
            setStep(STEP_MAP);
            toast({
              title: "Opened Map",
              message: "Re-run mapping to accept schema changes, then return to Validate.",
              tone: "info",
            });
          },
          primaryFixLabel: action.label,
        };
      case "quarantine_and_rerun":
        // Never offer Quarantine as the primary Fix when identity duplicates
        // are in the gate text — Strip/balanced cannot make Execute safe.
        if (
          /duplicate (primary )?key|keys repeat|identity-key|source probe/i.test(blockerBlob)
        ) {
          return {
            onPrimaryFix: openIdentitySettings,
            primaryFixLabel: "Fix identity / sync mode",
          };
        }
        return {
          onPrimaryFix: () => setBadDataFixOpen(true),
          primaryFixLabel: "Fix bad data…",
        };
      case "check_connection":
        return {
          onPrimaryFix: () => {
            window.location.hash = "#/connectors";
          },
          primaryFixLabel: "Fix connector credentials",
        };
      case "fix_source_keys":
        return { onPrimaryFix: openIdentitySettings, primaryFixLabel: action.label };
      case "run_population_orphan_scan": {
        const plan = planFkOrphanSuggestedAction({ kind: action.kind, column: action.column });
        return {
          onPrimaryFix: () => {
            if (!plan) return;
            setRunPopulationOrphanScan(true);
            toast({
              title: plan.toastTitle,
              message: plan.toastMessage,
              tone: plan.toastTone,
            });
            void executePreflight(undefined, undefined, { runPopulationOrphanScan: true });
          },
          primaryFixLabel: action.label,
        };
      }
      case "fix_orphans": {
        const plan = planFkOrphanSuggestedAction({ kind: action.kind, column: action.column });
        return {
          onPrimaryFix: () => {
            if (!plan) return;
            if (plan.focusSource) setMapFocusSource(plan.focusSource);
            if (plan.goToMap) setStep(STEP_MAP);
            toast({
              title: plan.toastTitle,
              message: plan.toastMessage,
              tone: plan.toastTone,
            });
          },
          primaryFixLabel: action.label,
        };
      }
      default:
        return applyPromoted(promoteBlockedPrimaryFix(firstBlocker));
    }
  }, [duplicateKeyRoot, openIdentitySettings, preflight, syncMode, toast, columnMappings]);

  const executeTransfer = async () => {
    if (!jobRun.allowed) {
      toast({ title: "No write permission", message: jobRun.reason, tone: "warning" });
      return;
    }
    if (multiStreamUnsupportedMode) {
      toast({
        title: "Multi-stream not supported for this mode",
        message: multiStreamScd2MirrorBlockCopy("toast"),
        tone: "error",
      });
      setStep(STEP_DESTINATION);
      return;
    }
    const needsDbTarget = destKindMode === "database";
    if (sourceKind === "file" && !file && !parsed?.file_id) {
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
    const decision = preflight?.proof_bundle?.transfer_decision?.decision;
    // No run id is not an API run: an unidentifiable verdict never unlocks Execute.
    const localPf = !isApiPreflight(preflight);
    if (!preflight?.passed || decision !== "approve" || localPf) {
      toast({
        title: localPf || decision === "review" ? "API Validate required" : "Preflight required",
        message: localPf || decision === "review"
          ? "Browser/local or review-grade results cannot unlock Execute. Re-run Validate until API decision is approve."
          : "Run and pass API preflight gates (decision: approve) before writing.",
        tone: "warning",
      });
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

    const enforcePreflight = true;
    const approvedDecisionArtifactHash = String(
      preflight?.proof_bundle?.decision_artifact_hash
        || preflight?.proof_bundle?.decision_artifact?.content_hash
        || "",
    ).trim();
    // Map→DDL fingerprint Validate stamped over these same Map rows. Execute
    // checks the operator contract against it instead of re-deriving its own.
    const approvedDdlIdentityHash = String(
      preflight?.proof_bundle?.ddl_identity?.ddl_identity_hash || "",
    ).trim();
    if (contractBlockReason) {
      toast({
        title: "Signed contract required",
        message: contractBlockReason,
        tone: "warning",
      });
      return;
    }
    if (
      enforcePreflight
      && (!approvedDecisionArtifactHash || approvedDecisionArtifactHash.length !== 64)
    ) {
      toast({
        title: "Re-run Validate",
        message:
          "Execute requires the Decision Artifact hash from Validate. "
          + "Run Validate again before starting the transfer.",
        tone: "warning",
      });
      return;
    }

    setTransferring(true);
    setStep(STEP_RUN);
    setActiveJobId(null);
    setResult(null);
    setTransferLaunch(null);
    setExecutedRouteKey(currentDestRouteKey);
    setRunStartupProgress(stagePercent(1, RUN_LAUNCH_STAGES.length));
    setRunStartupPhase(RUN_LAUNCH_STAGES[0]);
    // Prefer Validate-echoed Kernel stamps + signed contracts over Map drafts.
    const mappingsForExecute = mergeSignedRiskContracts(
      mergeStampedTargetTypes(columnMappings, preflight?.stamped_mappings),
      preflight?.signed_mappings,
    );
    const transferMappings = mappingsForExecute.length
      ? buildPreflightMappings(analysis?.columns ?? [], mappingsForExecute)
      : analysis
        ? buildPreflightMappings(analysis.columns)
        : undefined;
    try {
      // Belt-and-suspenders: sync Studio mappings onto the plan before Execute so
      // an empty draft (Map race) cannot block a green Validate with "no mappings".
      let runPlanId = persistedPlanId ?? undefined;
      if (runPlanId && transferMappings?.length) {
        try {
          await syncTransferPlanMappings(runPlanId, transferMappings);
        } catch {
          // If sync fails, still send mappings_json; backend merge_plan_into_run recovers.
        }
      } else if (!runPlanId && transferMappings?.length) {
        const created = await ensurePersistedPlan();
        if (created) {
          try {
            await syncTransferPlanMappings(created, transferMappings);
            runPlanId = created;
          } catch {
            runPlanId = created;
          }
        }
      }
      setRunStartupProgress(stagePercent(2, RUN_LAUNCH_STAGES.length));
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
        destSchema: destSchema || (destDriverType === "snowflake" ? "PUBLIC" : undefined),
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
        deliveryGuarantee: studioDeliveryGuarantee({
          syncMode,
          deliveryGuarantee,
          allowAppendOnly,
          callableSource: sourceReadMode === "procedure" || sourceReadMode === "query",
        }),
        dateLocale,
        numberLocale,
        backfillNewFields,
        writeViaStaging,
        enableOcr,
        readOptions: sourceKind === "file" ? readOptions : undefined,
        shapeRecipe: recipePayload(shapeSteps),
        approvedShapeRecipeHash: shapeIdentity?.hash || undefined,
        sourceExtra: (() => {
          const extra: Record<string, unknown> = {
            ...(sourceKind === "database"
              ? callableSourceExtra(sourceReadMode, procedureCall, procedureParams) || {}
              : {}),
            ...(parsed?.file_id ? { file_id: parsed.file_id } : {}),
          };
          if (syncMode === "cdc") {
            if (multiSubnetFailover) extra.multi_subnet_failover = true;
            const sqlServerCdc = [
              "sqlserver",
              "mssql",
              "azure_sql_database",
              "microsoft_sql_server",
              "amazon_rds_sql_server",
            ].includes(resolveDriverType(sourceConnector?.type || ""));
            if (sqlServerCdc && cdcRowFilter && cdcRowFilter !== "all") {
              extra.cdc_row_filter = cdcRowFilter;
            }
          }
          return Object.keys(extra).length ? extra : undefined;
        })(),
        destExtra: (() => {
          const extra: Record<string, unknown> = {};
          const provenExists = destTableExists ?? destTableExistsRef.current;
          if (provenExists !== null && provenExists !== undefined) {
            extra.table_exists = provenExists;
          }
          if (provenExists === true && Object.keys(destSchemaMap).length) {
            // Live DDL must ride multipart dest_extra — form fields alone omit it.
            extra.schema_types = destSchemaMap;
          }
          if (syncMode === "cdc" && allowAppendOnly) {
            extra.allow_append_only = true;
          }
          if (destWriteMode === "procedure") {
            extra.dest_write_mode = "procedure";
            extra.dest_procedure_call = destProcedureCall.trim();
            if (Object.keys(destProcedureParamMap).length) {
              extra.dest_procedure_param_map = { ...destProcedureParamMap };
            }
            if (Object.keys(destProcedureParams).length) {
              extra.dest_procedure_params = { ...destProcedureParams };
            }
          }
          if (destWriteMode === "query") {
            extra.dest_write_mode = "query";
            extra.dest_query_sql = destQuerySql.trim();
            if (Object.keys(destProcedureParamMap).length) {
              extra.dest_procedure_param_map = { ...destProcedureParamMap };
            }
            if (Object.keys(destProcedureParams).length) {
              extra.dest_procedure_params = { ...destProcedureParams };
            }
          }
          if (destProcedureBefore.trim()) extra.dest_procedure_before = destProcedureBefore.trim();
          if (destProcedureAfter.trim()) extra.dest_procedure_after = destProcedureAfter.trim();
          const isVectorDestRun =
            destDriverType === "pgvector" ||
            destDriverType === "qdrant" ||
            destDriverType === "weaviate" ||
            destDriverType === "pinecone" ||
            destDriverType === "milvus";
          if (isVectorDestRun) {
            if (vectorContentColumn) extra.content_column = vectorContentColumn;
            if (vectorEmbeddingColumn) extra.embedding_column = vectorEmbeddingColumn;
            const meta = vectorMetadataColumns
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean);
            if (meta.length) extra.metadata_columns = meta;
            const excludePii = vectorExcludePiiColumns
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean);
            if (excludePii.length) extra.exclude_pii_columns = excludePii;
            if (vectorEmbeddingModel) extra.embedding_model = vectorEmbeddingModel;
            extra.chunk_size = vectorChunkSize;
            extra.chunk_overlap = vectorChunkOverlap;
            extra.durable_embedding_cache = vectorDurableCache;
          }
          return Object.keys(extra).length ? extra : undefined;
        })(),
        streamContracts,
        planId: runPlanId,
        priorityColumn: priorityColumn || undefined,
        priorityDirection,
        limit: rowLimit > 0 ? rowLimit : undefined,
        complianceAcknowledged,
        schemaDriftAcknowledged,
        fkRiskAcknowledged,
        acknowledgmentActor: readSession()?.email || readSession()?.name || undefined,
        acknowledgmentReason: standingAcknowledgmentReason({
          compliance: complianceAcknowledged,
          schemaDrift: schemaDriftAcknowledged,
          fkRisk: fkRiskAcknowledged,
        }) || undefined,
        approvedDecisionArtifactHash: approvedDecisionArtifactHash || undefined,
        approvedDdlIdentityHash: approvedDdlIdentityHash || undefined,
        contractId: boundContractId.trim() || undefined,
        requireSignedContract: Boolean(boundContractId.trim() && requireSignedContract),
        decisionArtifact:
          preflight?.proof_bundle?.decision_artifact
          && typeof preflight.proof_bundle.decision_artifact === "object"
            ? (preflight.proof_bundle.decision_artifact as Record<string, unknown>)
            : undefined,
      });
      setRunStartupProgress(stagePercent(3, RUN_LAUNCH_STAGES.length));
      setRunStartupPhase(RUN_LAUNCH_STAGES[3]);
      // A double-click / retry that hit an already-running equivalent transfer.
      // Open the live job instead of starting a second writer against the same table.
      if (
        (data as { duplicate?: boolean }).duplicate
        && (data as { existing_job_id?: string }).existing_job_id
      ) {
        const existingId = String((data as { existing_job_id: string }).existing_job_id);
        const existingStatus = String(
          (data as { existing_status?: string }).existing_status || "in progress",
        );
        setRunStartupProgress(100);
        setActiveJobId(existingId);
        setTransferring(false);
        toast({
          title: "Transfer already running",
          message: `Opened the ${existingStatus} job instead of starting a second writer.`,
          tone: "warning",
        });
        return;
      }
      if (data.job_id && (data as { async?: boolean }).async) {
        setRunStartupProgress(100);
        setActiveJobId(data.job_id);
        setTransferLaunch({
          jobId: data.job_id,
          rows: Number(sourceRowEstimate ?? parsed?.row_count ?? 0),
        });
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
          numberLocale,
        });
        setResult(localResult);
        setRunStartupProgress(100);
        setStep(STEP_RUN);
        // Local export has no server job — skip onTransferComplete (that toast
        // says "View progress in Job Theater" and would double with this one).
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
      row_accounting: job.row_accounting,
      explanation: job.explanation,
      mapping_proof: job.mapping_proof,
      ddl_executed: job.ddl_executed ?? job.ddl_log,
      event_log: job.event_log?.length ? job.event_log : (job._id ? readJobEventLog(job._id) : undefined),
      cdc_lag_seconds: job.cdc_lag_seconds,
      cdc_plugin: job.cdc_plugin,
      cdc_delivery: job.cdc_delivery,
      cdc_row_filter: job.cdc_row_filter,
      cdc_shared_reader: job.cdc_shared_reader,
      snapshot_mode: job.snapshot_mode,
      snapshot_plan: job.snapshot_plan,
      watermark: job.watermark,
      cdc_lease_holder: job.cdc_lease_holder,
      cdc_lease_backend: job.cdc_lease_backend,
      source_ha_role: job.source_ha_role,
      source_ha_topology: job.source_ha_topology,
      source_ha_group: job.source_ha_group,
      source_ha_message: job.source_ha_message,
      cdc_retention_status: job.cdc_retention_status,
      cdc_retention_resume: job.cdc_retention_resume,
      cdc_retention_retained: job.cdc_retention_retained,
      cdc_retention_message: job.cdc_retention_message,
      cdc_retention_dialect: job.cdc_retention_dialect,
      cdc_cursor_gap: job.cdc_cursor_gap,
      cdc_cursor_gap_code: job.cdc_cursor_gap_code,
      cdc_cursor_gap_dialect: job.cdc_cursor_gap_dialect,
      cdc_cursor_gap_resume: job.cdc_cursor_gap_resume,
      cdc_cursor_gap_retained: job.cdc_cursor_gap_retained,
      cdc_lease_cursor_key: job.cdc_lease_cursor_key,
      error_code: job.error_code,
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
    if (multiStreamUnsupportedMode) {
      toast({
        title: "Multi-stream not supported for this mode",
        message: multiStreamScd2MirrorBlockCopy("schedule"),
        tone: "error",
      });
      return;
    }
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
      const mappingsForSchedule = mergeSignedRiskContracts(
        mergeStampedTargetTypes(columnMappings, preflight?.stamped_mappings),
        preflight?.signed_mappings,
      );
      const transferMappings = mappingsForSchedule.length
        ? buildPreflightMappings(analysis?.columns ?? [], mappingsForSchedule)
        : analysis
          ? buildPreflightMappings(analysis.columns)
          : columnMappings.map((m) => ({
              source: m.source,
              target: m.target,
              confidence: m.confidence,
              transform: m.transform,
            }));
      const decisionHash = String(
        (preflight?.proof_bundle?.decision_artifact as { content_hash?: string } | undefined)
          ?.content_hash || "",
      );
      await createSchedule({
        name: `${sourceConnector?.name ?? "Source"} → ${targetCollection}`,
        source_connector_id: sourceConnectorId,
        source_table: sourceTableName,
        dest_connector_id: connectorId,
        dest_table: targetCollection,
        interval: "daily",
        enabled: true,
        sync_mode: syncMode,
        cursor_column: cursorField,
        primary_key: primaryKeyField,
        source_read_mode: sourceReadMode,
        procedure_call: sourceReadMode === "procedure" ? procedureCall.trim() : "",
        source_query: sourceReadMode === "query" ? procedureCall.trim() : "",
        procedure_params: Object.keys(procedureParams).length ? procedureParams : {},
        mappings: transferMappings,
        stream_contracts: streamContracts,
        date_locale: dateLocale,
        number_locale: numberLocale,
        shape_recipe: recipePayload(shapeSteps),
        approved_shape_recipe_hash: shapeIdentity?.hash || "",
        approved_decision_artifact_hash: decisionHash,
        ...studioSchedulePolicies({
          validationMode,
          schemaPolicy,
          backfillNewFields,
          writeViaStaging,
          priorityColumn,
          priorityDirection,
          rowLimit,
        }),
        delivery_guarantee: studioDeliveryGuarantee({
          syncMode,
          deliveryGuarantee,
          allowAppendOnly,
          callableSource: sourceReadMode === "procedure" || sourceReadMode === "query",
        }),
        contract_id: boundContractId.trim(),
        require_signed_contract: Boolean(boundContractId.trim() && requireSignedContract),
      });
      toast({
        title: "Pipeline created",
        message: "Daily sync enabled. Manage cadence in Schedules.",
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

  const sourceInputsReady = sourceExtractReady({
    sourceKind,
    parsed: Boolean(parsed),
    sourceConnectorId,
    cloudPath,
    sourceTable,
    sourceCollection,
    sourceReadMode,
    procedureCall,
  });

  const canConfigureDest =
    sourceKind === "file"
      ? Boolean(parsed)
      : Boolean(
          sourceInputsReady
          && (analysis?.columns.length || currentSourceColumns.length),
        );

  const destSqlReady = destWriteReady({
    destWriteMode,
    destProcedureCall,
    destQuerySql,
  });
  const canRunPreflight =
    canConfigureDest &&
    (destKindMode === "file_export" ||
      (isCallableDestMode(destWriteMode)
        ? Boolean(
            destType
            && destSqlReady
            && (connectorId || targetDb.trim() || destDriverType === "iceberg")
            && !destSchemaLoading,
          )
        : Boolean(destType && targetDb && targetCollection) && !destSchemaLoading));

  const needsDbPreflight = destKindMode === "database";
  const currentDestRouteKey = destRouteKey({
    destKindMode,
    destType,
    targetDb,
    destSchema,
    targetCollection,
    exportFormat,
    destOutputPath,
  });
  /** Map/sync/PK/dest/Advanced edits must invalidate a prior green Validate before Execute. */
  const buildValidateContractKey = useCallback(
    (maps: EditableMapping[]) =>
      composeValidateContractKey({
        syncMode,
        primaryKeyField,
        cursorField,
        validationMode,
        schemaPolicy,
        targetCollection,
        destType,
        targetDb,
        destKindMode,
        destSchema,
        mappings: maps,
        dateLocale,
        numberLocale,
        backfillNewFields,
        writeViaStaging,
        deliveryGuarantee,
        allowAppendOnly,
        snapshotMode,
        priorityColumn,
        priorityDirection,
        rowLimit,
        cursorSemantics,
        streamFields,
        shapeRecipeHash: shapeIdentity?.hash || "",
        shapeSteps,
        multiSubnetFailover,
        cdcRowFilter,
        schemaDriftAcknowledged,
      }),
    [
      syncMode,
      primaryKeyField,
      cursorField,
      validationMode,
      schemaPolicy,
      targetCollection,
      destType,
      targetDb,
      destKindMode,
      destSchema,
      dateLocale,
      numberLocale,
      backfillNewFields,
      writeViaStaging,
      deliveryGuarantee,
      allowAppendOnly,
      snapshotMode,
      priorityColumn,
      priorityDirection,
      rowLimit,
      cursorSemantics,
      streamFields,
      shapeIdentity?.hash,
      shapeSteps,
      multiSubnetFailover,
      cdcRowFilter,
      schemaDriftAcknowledged,
    ],
  );
  const validateContractKey = useMemo(
    () => buildValidateContractKey(columnMappings),
    [buildValidateContractKey, columnMappings],
  );

  useEffect(() => {
    if (!preflight) return;
    if (validatedContractKey != null && validatedContractKey !== validateContractKey) {
      setPreflight(null);
      setPreflightError("");
      setValidatedContractKey(null);
    }
  }, [validateContractKey, validatedContractKey, preflight]);

  /**
   * A finished run reports the route it wrote. Retarget the destination and that
   * panel stops being an answer about the route on screen, so Run offers Execute
   * again instead of claiming a landing in a table it never wrote. The result is
   * kept rather than destroyed: point the plan back and the proof returns.
   * Mapping edits are deliberately not part of this — repairs are applied from
   * the result panel itself.
   */
  const runResultDescribesCurrentPlan = runResultDescribesRoute(
    executedRouteKey,
    currentDestRouteKey,
  );
  /**
   * The launch pointer belongs to the route it was launched for. Kept in state
   * so Job Theater can still open it, but withheld from Validate and Run, whose
   * job is the route on screen — otherwise "Open live progress" takes Execute's
   * place for a plan that was never executed.
   */
  const routeScopedLaunch = runResultDescribesCurrentPlan ? transferLaunch : null;
  const studioPrimary = resolveValidateStudioPrimary({
    preflight,
    preflighting,
    transferring,
    mappingReviewCount,
    riskAckPendingCount,
    transferLaunch: routeScopedLaunch,
    executeBlocked: multiStreamUnsupportedMode || Boolean(contractBlockReason),
    hasPrimaryFix: Boolean(primaryFix.onPrimaryFix && primaryFix.primaryFixLabel),
    primaryFixLabel: primaryFix.primaryFixLabel,
    hasHoldOut: true,
  });
  /**
   * Route the superseded run actually wrote, named from the route it recorded so
   * the sentence matches the label the result dashboard uses for the same run.
   */
  const staleRunDestLabel = describeDestRoute(executedRouteKey)
    || [result?.destination?.database, result?.destination?.collection]
      .filter(Boolean)
      .join(".");

  /** API-approved preflight only — local/review-grade never unlocks Execute. */
  const isGovernedExecuteReady = Boolean(
    preflight?.passed
    && validatedContractKey === validateContractKey
    && preflight.proof_bundle?.transfer_decision?.decision === "approve"
    && isApiPreflight(preflight),
  );
  const canExecute = Boolean(canConfigureDest && isGovernedExecuteReady);

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
          : destTableExists === false
            ? `DynamoDB table ${targetCollection || targetDb || "table"} will be created on first write`
            : destTableExists === true
              ? `Existing DynamoDB table ${targetCollection || targetDb} — attribute metadata pending`
              : `Confirming DynamoDB table ${targetCollection || targetDb || "table"}…`
      : destSchemaLoading
      ? `Fetching existing schema from ${destType} connector`
      : destColumns.length > 0
        ? `Existing ${destType} schema — ${destColumns.length} fields introspected`
        : destTableExists === false
          ? `New schema will be created in ${targetDb}.${targetCollection || "collection"}`
          : destTableExists === true
            ? `Existing ${destType} table ${targetDb}.${targetCollection} — column metadata pending`
            : `Confirming whether ${targetDb}.${targetCollection || "table"} already exists…`;
  const mapSourceColumnCount = columnMappings.length || analysis?.columns.length || currentSourceColumns.length;

  const effectiveMappingProof = useMemo(
    () =>
      mergeMappingProof(mappingProof, columnMappings, {
        destColumns,
        destType: destKindMode === "file_export" ? exportFormat : destType,
        destTableExists: destCatalogExists(destKindMode, destTableExists),
      }),
    [mappingProof, columnMappings, destColumns, destKindMode, exportFormat, destType, destTableExists],
  );
  const mappingProofSummary = useMemo(() => {
    if (!columnMappings.length) return null;
    const rows = effectiveMappingProof.mappings ?? [];
    const classCounts: Record<string, number> = {};
    for (const r of rows) {
      const label = String(r.evidence?.confidence_class_label || r.evidence?.confidence_class || "").trim();
      if (!label) continue;
      classCounts[label] = (classCounts[label] || 0) + 1;
    }
    return {
      destMode: effectiveMappingProof.dest_mode,
      mappedCount: effectiveMappingProof.summary?.mapped_count ?? rows.length,
      exactOverlaps: rows.filter((r) => r.match_quality === "exact_name").length,
      riskCount: effectiveMappingProof.summary?.risk_count ?? 0,
      reviewCount: effectiveMappingProof.summary?.review_count ?? 0,
      avgConfidence: effectiveMappingProof.summary?.avg_confidence,
      maxConfidence: effectiveMappingProof.summary?.max_confidence,
      classCounts,
    };
  }, [columnMappings.length, effectiveMappingProof]);

  // Keep Datawrap Pilot fed with the active validation/job IDs for NL triage & remediations.
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
              ? ((preflight.proof_bundle?.transfer_decision?.decision || "") === "approve"
                ? "passed"
                : "review")
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
        case "open_bad_data_fix":
          setStep(STEP_VALIDATE);
          setBadDataFixOpen(true);
          toast({
            title: "Fix bad data",
            message: action.run_id
              ? `Opened Fix bad data for run ${action.run_id}. Choose Strip or Quarantine, then re-run Validate.`
              : "Opened Fix bad data — choose Strip or Quarantine, then re-run Validate.",
            tone: "info",
          });
          break;
        case "quarantine_and_rerun":
          if (duplicateKeyRoot) {
            openIdentitySettings();
            break;
          }
          setStep(STEP_VALIDATE);
          setBadDataFixOpen(true);
          toast({
            title: "Fix bad data",
            message: "Opened Fix bad data — choose Strip or Quarantine, then re-run Validate.",
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
      const mappings = buildPreflightMappings([], columnMappings);
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
    setAdvancedLocaleFocus(null);
    setFile(null);
    setParsed(null);
    setShapeSteps([]);
    setShapeIdentity(null);
    setConnectorSampleRows([]);
    setSourceRowEstimate(null);
    setAnalysis(null);
    setPreflight(null);
    setPreflightError("");
    setComplianceAcknowledged(false);
    setSchemaDriftAcknowledged(false);
    setValidatedContractKey(null);
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
    setDestSchema("");
    setDestUsername("");
    setDestPassword("");
    setDestConnectionString("");
    setDestOutputPath("");
    setDestWarehouse("");
    setTransferring(false);
    setActiveJobId(null);
    setResult(null);
    setExecutedRouteKey(null);
    setSyncMode("full_refresh_append");
    setSchemaPolicy("manual_review");
    setValidationMode("balanced");
    setBackfillNewFields(false);
    setMultiSubnetFailover(false);
    setCdcRowFilter("all");
    setCursorField("");
    setPrimaryKeyField("");
    setStreamFields({});
    setColumnMappings([]);
    setDestColumns([]);
    setDestSchemaMap({});
    setDestSchemaLoading(false);
    proveDestTableExists(null);
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
      <PermissionNotice
        allowed={jobRun.allowed}
        reason={jobRun.reason}
        what="You can plan a transfer but not run one."
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
            (n === STEP_SHAPE && canRunPreflight) ||
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

      <div className={`df2-transfer-studio-body ${step === STEP_MAP ? "is-map-step is-full-width" : ""}${step === STEP_VALIDATE ? " is-validate-step is-full-width" : " is-full-width"}`}>
      <main className="df2-transfer-main-panel" key={step}>
      {step === STEP_MAP && columnMappings.length > 0 && !analyzing && (
        <TransferMapStep
          columnMappings={columnMappings}
          analysis={analysis}
          destColumns={destColumns}
          destSchemaLoading={destSchemaLoading}
          destTableExists={destCatalogExists(destKindMode, destTableExists)}
          extraSourceColumns={shapeContract?.extra_source_columns ?? []}
          destShapeHeadline={shapeContract?.headline ?? ""}
          onReloadDestSchema={async () => {
            // Re-probe the catalog, then re-map with the probe's own verdict —
            // a proven-absent table becomes create-new, a loaded table binds
            // real destination types. No guessing either way.
            const res = await loadDestinationSchema({ force: true });
            await applyPipelineMappings(
              res.columns,
              res.schema,
              undefined,
              res.tableExists,
            );
          }}
          destConnected={destConnected}
          destConnectionError={destConnectionError}
          targetCollection={targetCollection}
          targetDatabase={targetDb}
          destKindMode={destKindMode}
          destType={destKindMode === "file_export" ? exportFormat : destType}
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
          streamBusy={mapStreamBusy}
          onRematchAllStreams={
            isMultiStreamSource
              ? async () => {
                  setMapStreamBusy("all");
                  try {
                    const active = mapActiveStream || primarySourceStream;
                    const seeded: Record<string, EditableMapping[]> = {
                      ...streamMappings,
                      [active]: columnMappings,
                    };
                    for (const name of multiStreamNames) {
                      seeded[name] = await mapColumnsForStream(name);
                    }
                    setStreamMappings(seeded);
                    setColumnMappings(seeded[active] || []);
                    toast({
                      title: "Streams rematched",
                      message: `Mapped ${multiStreamNames.length} source streams to the destination.`,
                      tone: "success",
                    });
                  } finally {
                    setMapStreamBusy(null);
                  }
                }
              : undefined
          }
          onActiveStreamChange={(name) => {
            void (async () => {
              const current = mapActiveStream || primarySourceStream;
              let cached = streamMappings[name];
              setStreamMappings((prev) => ({ ...prev, [current]: columnMappings }));
              if (!cached?.length) {
                setMapStreamBusy(name);
                try {
                  cached = await mapColumnsForStream(name);
                  setStreamMappings((prev) => ({
                    ...prev,
                    [current]: columnMappings,
                    [name]: cached || [],
                  }));
                } finally {
                  setMapStreamBusy(null);
                }
              }
              setColumnMappings(cached?.length ? cached : []);
              setMapActiveStream(name);
            })();
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
          initialFocusSource={mapFocusSource}
          identityFixBanner={mapIdentityBanner}
          onIdentityFixConsumed={() => {
            setMapFocusSource(null);
            setMapIdentityBanner(null);
          }}
          syncModeLabel={syncModeLabel}
          primaryKeyField={primaryKeyField}
          cursorField={cursorField}
          requiresPrimaryKey={requiresPrimaryKey}
          requiresCursor={requiresCursor}
          onOpenIdentitySettings={openIdentitySettings}
          uniqueKeySuggestions={uniqueKeySuggestions}
          compositeKeySuggestions={compositeKeySuggestions}
          onApplyPrimaryKey={applyPrimaryKeySuggestion}
          sampleRows={(samplePreviewRows as Record<string, unknown>[]) || []}
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
        <div className="df2-card-body">
          <div className="df2-transfer-step-split">
            <div className="df2-transfer-step-primary">
              <div className="df2-field">
                <label className="df2-label">Where is your data?</label>
                <SourceKindTiles
                  value={sourceKind}
                  hideHint={
                    (sourceKind === "file" && Boolean(parsed))
                    || (sourceKind === "database" && Boolean(sourceConnectorId || currentSourceColumns.length))
                    || (sourceKind === "cloud" && Boolean(cloudPath))
                  }
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
              <HiddenFileInput
                id="df2-source-file"
                ref={fileInputRef}
                accept=".json,.csv,.jsonl,.tsv,.parquet,.pdf,.docx,.html,.htm,.xlsx,.xls,.xml"
                disabled={uploading}
                onChange={handleFileSelect}
              />
              {uploadError && (
                <div className="df2-alert df2-alert-error" role="alert">
                  <DtIcon name="x" size={16} />
                  <div>
                    <strong>{readWindowRefused ? "Read window refused" : "Upload failed"}</strong>
                    <p>{uploadError}</p>
                    {readWindowRefused && (
                      <p>The file and its previous read window are unchanged — correct the declaration and apply again.</p>
                    )}
                  </div>
                </div>
              )}
              <label
                className="df2-policy-toggle df2-source-ocr-toggle"
                style={{ marginBottom: 8 }}
                onClick={(e) => e.stopPropagation()}
                title={
                  ocrStatus?.available === false
                    ? `OCR not ready: ${ocrStatus.message || "install tesseract + pypdfium2/Pillow/pytesseract"}`
                    : ocrStatus?.available
                      ? "Tesseract is available — OCR scanned PDFs when no text layer exists"
                      : "OCR scanned PDFs when no text layer exists (requires Tesseract on the API host)"
                }
              >
                <input
                  type="checkbox"
                  checked={enableOcr}
                  onChange={(e) => setEnableOcr(e.target.checked)}
                />
                <span>
                  <strong>OCR scanned PDFs</strong>
                  <small>
                    {ocrStatus?.available === false
                      ? "Not ready on this host — hover for install steps."
                      : ocrStatus?.available
                        ? "Tesseract ready — used when a PDF has no text layer."
                        : "Optional · runs Tesseract when a PDF has no text layer."}
                  </small>
                </span>
              </label>
              {file && parsed && offersReadWindow(parsed.file_type || "") && (
                <SourceReadWindowPanel
                  fileType={parsed.file_type || ""}
                  sheets={parsed.sheets ?? []}
                  applied={readOptions}
                  busy={uploading}
                  onApply={applyReadWindow}
                />
              )}
              {file && parsed ? (
                <div className="df2-upload-result df2-upload-result-compact">
                  <div className="df2-upload-result-main">
                    <span className="df2-badge df2-badge-live"><DtIcon name="check" size={14} /> {file.name}</span>
                    <span className="df2-upload-result-meta">
                      {formatFileSize(file.size)} · {parsed.row_count.toLocaleString()} rows · {parsed.columns.length} columns
                    </span>
                  </div>
                  <label
                    htmlFor="df2-source-file"
                    className="df2-btn df2-btn-sm"
                    aria-disabled={uploading}
                    title="Replace source file"
                  >
                    <DtIcon name="upload" size={14} /> Replace
                  </label>
                </div>
              ) : (
              <label
                htmlFor="df2-source-file"
                className={`df2-upload df2-upload-studio ${dragOver ? "drag-over" : ""} ${uploading ? "is-loading" : ""}`}
                onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
                onDragLeave={() => setDragOver(false)}
                onDrop={handleDrop}
              >
                <div className="df2-upload-icon">
                  {uploading || analyzing ? <Spinner /> : <DtIcon name="upload" size={22} />}
                </div>
                <p className="df2-upload-title">
                  {uploading ? "Profiling source file…" : "Drop your data file here"}
                </p>
                <p className="df2-upload-hint">
                  {uploading ? "Parsing schema and sampling rows" : "or click to browse · max 250 MB"}
                </p>
                <div className="df2-upload-formats">
                  {UPLOAD_FORMATS.map((fmt) => (
                    <span key={fmt} className="df2-upload-format-chip">{fmt}</span>
                  ))}
                </div>
              </label>
              )}
              {!parsed && !uploading && (
                <div className="df2-upload-sample-row">
                  <span className="df2-label-hint">New to Datawrap?</span>
                  <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => void loadSampleDataset()}>
                    <DtIcon name="sparkle" size={14} /> Load sample orders CSV
                  </button>
                </div>
              )}
              {file && parsed && (
                <>
                  {["pdf", "docx", "html", "htm"].includes((parsed.file_type || "").toLowerCase()) && (
                    <p className="df2-label-hint" role="status">
                      Document source: {parsed.row_count.toLocaleString()} text chunk(s) with page/heading
                      provenance
                      {parsed.ocr_used
                        ? ` (OCR on ${parsed.ocr_page_count ?? 0} page(s))`
                        : ""}
                      . Pair with a vector destination (pgvector, Qdrant, Weaviate, Pinecone, or Milvus)
                      in Destination → Advanced for RAG embed.
                    </p>
                  )}
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
                  Datawrap will detect format from the object key and profile schema on continue.
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
                {dialectOffersSqlExtract(sourceConnector?.type) && (
                  <div className="df2-field df2-source-read-mode">
                    <label className="df2-label" htmlFor="source-read-mode">
                      Source extract
                    </label>
                    <FilterTabs<SourceReadMode>
                      ariaLabel="Source extract"
                      value={sourceReadMode}
                      onChange={setSourceReadMode}
                      items={[
                        { id: "table", label: "Table" },
                        ...(dialectOffersQuery(sourceConnector?.type)
                          ? [{ id: "query" as const, label: "SQL query" }]
                          : []),
                        ...(dialectOffersProcedures(sourceConnector?.type)
                          ? [{ id: "procedure" as const, label: "Stored procedure" }]
                          : []),
                      ]}
                    />
                    <span className="df2-label-hint">
                      {sourceReadMode === "procedure"
                        ? "Execute one CALL/EXEC, map the result set, remap extra columns on Map."
                        : sourceReadMode === "query"
                          ? "One read-only SELECT/WITH. Result columns map on the next step. Not CDC."
                          : "Read a table or view. Query and stored procedure are result-set snapshots, not CDC."}
                    </span>
                  </div>
                )}
                {isCallableSourceMode(sourceReadMode) && dialectOffersSqlExtract(sourceConnector?.type) ? (
                <div className="df2-field df2-source-procedure">
                  <SqlEditor
                    id="source-procedure-input"
                    label={sourceReadMode === "query" ? "SQL query" : "Stored procedure"}
                    value={procedureCall}
                    onChange={setProcedureCall}
                    mode={sourceReadMode === "query" ? "query" : "procedure"}
                    dialect={sourceConnector?.type}
                    bound={procedureParams}
                    placeholder={
                      sourceReadMode === "query"
                        ? queryHint(sourceConnector?.type)
                        : procedureHint(sourceConnector?.type)
                    }
                    hint="One statement. :name binds below, or quoted/numeric literals. Extra result columns stay on Map — never silent drop."
                    rows={6}
                  />
                  {bindNamesFromSql(procedureCall).length > 0 && (
                    <div className="df2-source-bind-params">
                      {bindNamesFromSql(procedureCall).map((name) => (
                        <div className="df2-field" key={name}>
                          <label className="df2-label" htmlFor={`bind-${name}`}>
                            :{name}
                          </label>
                          <input
                            id={`bind-${name}`}
                            className="df2-input"
                            value={procedureParams[name] ?? ""}
                            onChange={(e) =>
                              setProcedureParams((prev) => ({ ...prev, [name]: e.target.value }))
                            }
                            placeholder="Bound value"
                            autoComplete="off"
                            spellCheck={false}
                          />
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                ) : (
                <div className="df2-field df2-source-stream-field">
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
                )}
              </div>

              {!isCallableSourceMode(sourceReadMode) && (
              <div className="df2-source-multistream" role="note">
                <div className="df2-source-multistream-head">
                  <DtIcon name="activity" size={15} />
                  <strong>
                    {isMultiStreamSource
                      ? `${multiStreamNames.length} streams`
                      : "Stream selection"}
                  </strong>
                </div>
                <p>
                  {isMultiStreamSource
                    ? "Each name is a separate table/collection with its own watermark. Configure identity in Destination → Advanced."
                    : "Use commas for multi-table sync (example: sessions, users)."}
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
              : isCallableSourceMode(sourceReadMode)
                ? (currentSourceColumns.length || analysis?.columns.length
                    ? "Result set profiled — choose where data should land next."
                    : sourceReadMode === "query"
                      ? "Enter a read-only SELECT. Preview the result set, then continue."
                      : "Enter a CALL/EXEC. Preview the result set, then continue.")
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
        <div className="df2-card-body">
          <div className="df2-dest-step-layout">
            <div className="df2-dest-step-left">
              <div className="df2-dest-toolbar">
                <div className="df2-field df2-dest-mode-field">
                  <label className="df2-label">Mode</label>
                  <div className="df2-dest-mode-row">
                    <div className="df2-dest-mode-toggle" role="tablist" aria-label="Destination mode">
                      <button
                        type="button"
                        role="tab"
                        aria-selected={destKindMode === "database"}
                        className={`df2-dest-mode-btn${destKindMode === "database" ? " active" : ""}`}
                        onClick={() => {
                          setDestKindMode("database");
                          resetRouteForDestinationChange();
                        }}
                      >
                        Database
                      </button>
                      <button
                        type="button"
                        role="tab"
                        aria-selected={destKindMode === "file_export"}
                        className={`df2-dest-mode-btn${destKindMode === "file_export" ? " active" : ""}`}
                        onClick={() => {
                          setDestKindMode("file_export");
                          resetRouteForDestinationChange();
                        }}
                      >
                        File export
                      </button>
                    </div>
                    <Button
                      variant="secondary"
                      className="df2-dest-advanced-btn"
                      onClick={() => setAdvancedOpen(true)}
                      leadingIcon={<DtIcon name="settings" size={16} />}
                      title="Advanced settings — sync mode, primary key, cursor, schema drift, and write policies"
                    >
                      Advanced
                    </Button>
                  </div>
                </div>
              </div>

              {destKindMode === "file_export" ? (
                <div className="df2-field df2-dest-export-format">
                  <label className="df2-label" htmlFor="dest-export-format">Export format</label>
                  <select
                    id="dest-export-format"
                    className="df2-input df2-dest-export-select"
                    value={exportFormat}
                    onChange={(e) => {
                      setExportFormat(e.target.value);
                      setTransferPlan(null);
                    }}
                  >
                    {liveExportFormats.map((f) => (
                      <option key={f.id} value={f.id}>{f.label}</option>
                    ))}
                  </select>
                </div>
              ) : (
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
                    setDestConnectionString("");
                    setDestSchema(defaultSchemaForDriver(type));
                    if (resolveDriverType(type) === "iceberg") {
                      setTargetDb("");
                    }
                  }}
                />
              )}
            </div>

            <div className="df2-dest-step-right">
          {destKindMode === "file_export" ? (
            <div className="df2-field">
              <label className="df2-label">Output path (optional)</label>
              <input
                className="df2-input"
                value={destOutputPath}
                onChange={(e) => setDestOutputPath(e.target.value)}
                placeholder={`exports/my-export.${exportFormat || "json"} — leave empty for server exports folder`}
              />
              <p className="df2-label-hint">
                Leave empty to generate a downloadable {exportFormat.toUpperCase()} file in the server exports folder.
              </p>
            </div>
          ) : (
            <>
          {!connectorId && destType && destType !== "bigquery" && destDriverType === "iceberg" && (
          <div className="df2-dest-section df2-dest-manual-fields df2-dest-iceberg">
            <label className="df2-label">Iceberg warehouse</label>
            <p className="df2-label-hint" style={{ marginTop: 0 }}>
              Filesystem / mounted lakehouse root. Writes Iceberg V2 metadata + Parquet/JSONL data files
              with additive schema evolution and CoW upsert (<code>_df_lsn</code>). REST/Glue catalog
              committers are not required for this writer — do not claim multi-engine catalog yet.
            </p>
            <div className="df2-form-row">
              <div className="df2-field df2-field-flex">
                <label className="df2-label" htmlFor="dest-iceberg-warehouse">Warehouse path</label>
                <input
                  id="dest-iceberg-warehouse"
                  className="df2-input"
                  value={destConnectionString}
                  onChange={(e) => setDestConnectionString(e.target.value)}
                  placeholder="/data/iceberg-warehouse or file:///mnt/lake"
                />
              </div>
            </div>
          </div>
          )}

          {!connectorId && destType && destType !== "bigquery" && destDriverType !== "iceberg" && (
          <div className="df2-dest-section df2-dest-manual-fields">
            <label className="df2-label">Connection</label>
            <div className="df2-dest-manual-grid">
              {(destDriverType === "mongodb" || isGenericSql(destType) || ["mysql", "postgresql", "redshift", "sqlite"].includes(destDriverType)) && (
                <div className="df2-field df2-dest-manual-span">
                  <label className="df2-label">Connection string (optional)</label>
                  <input
                    className="df2-input"
                    value={destConnectionString}
                    onChange={(e) => setDestConnectionString(e.target.value)}
                    placeholder={destDriverType === "mongodb" ? "mongodb://localhost:27017/" : getGenericSqlPlaceholder(destType)}
                  />
                </div>
              )}
              <div className="df2-field">
                <label className="df2-label">
                  {destDriverType === "pinecone" ? "Index host" : "Host"}
                </label>
                <input
                  className="df2-input"
                  value={destHost}
                  onChange={(e) => setDestHost(e.target.value)}
                  placeholder={
                    destDriverType === "pinecone"
                      ? "my-index-xxxx.svc.pinecone.io"
                      : destDriverType === "weaviate" || destDriverType === "milvus"
                        ? "localhost"
                        : undefined
                  }
                />
              </div>
              {destDriverType !== "pinecone" && (
              <div className="df2-field df2-field-sm">
                <label className="df2-label">Port</label>
                <input type="number" className="df2-input" value={destPort} onChange={(e) => setDestPort(Number(e.target.value))} />
              </div>
              )}
              {destDriverType === "snowflake" && (
                <div className="df2-field">
                  <label className="df2-label">Warehouse</label>
                  <input className="df2-input" value={destWarehouse} onChange={(e) => setDestWarehouse(e.target.value)} placeholder="COMPUTE_WH" />
                </div>
              )}
              {destType !== "mongodb" && (
                <div className="df2-dest-manual-creds">
                  {destDriverType !== "pinecone" && destDriverType !== "qdrant" && destDriverType !== "weaviate" && (
                  <div className="df2-field">
                    <label className="df2-label">Username</label>
                    <input className="df2-input" value={destUsername} onChange={(e) => setDestUsername(e.target.value)} placeholder={destDriverType === "milvus" ? "root" : undefined} />
                  </div>
                  )}
                  <div className="df2-field">
                    <label className="df2-label">
                      {["pinecone", "qdrant", "weaviate"].includes(destDriverType) ? "API key" : "Password"}
                    </label>
                    <input type="password" className="df2-input" value={destPassword} onChange={(e) => setDestPassword(e.target.value)} placeholder={destDriverType === "milvus" ? "Milvus" : undefined} />
                  </div>
                </div>
              )}
            </div>
          </div>
          )}

          {connectorId && selectedDestConnector && (
            <p className="df2-connector-hint">
              Using <strong>{selectedDestConnector.name}</strong>
              {resolveDriverType(selectedDestConnector.type) === "iceberg"
                ? ` · warehouse ${selectedDestConnector.connection_string || selectedDestConnector.database || "(unset)"}`
                : ` (${selectedDestConnector.host}:${selectedDestConnector.port})`}
            </p>
          )}

          {destType ? (
            <>
          <div className="df2-dest-section df2-dest-target-fields df2-dest-right-section">
            <div className="df2-dest-target-head">
              <label className="df2-label" id="dest-target-location-label">
                Target location
              </label>
              {destObjectNames.length > 0 && !destSchemaLoading && (
                <span className="df2-dest-target-count" aria-live="polite">
                  {destObjectNames.length} existing{" "}
                  {destDriverType === "mongodb" ? "collection" : "table"}
                  {destObjectNames.length === 1 ? "" : "s"}
                </span>
              )}
            </div>
            <div
              className={`df2-dest-target-grid${
                destDriverType === "bigquery"
                  ? " has-dataset"
                  : destDriverType === "snowflake"
                    || destDriverType.includes("mssql")
                    || getGenericSqlGroup(destType) === "postgresql+psycopg2"
                    ? " has-schema"
                    : ""
              }`}
              role="group"
              aria-labelledby="dest-target-location-label"
            >
              <div className="df2-field">
                <label className="df2-label" htmlFor="dest-db">
                  {destDriverType === "bigquery"
                    ? "GCP Project ID"
                    : destDriverType === "dynamodb"
                      ? "AWS region or local endpoint"
                      : destDriverType === "iceberg"
                        ? "Namespace (optional)"
                        : destDriverType === "pinecone" || destDriverType === "qdrant" || destDriverType === "weaviate" || destDriverType === "milvus"
                          ? "Unused (optional)"
                          : "Database"}
                </label>
                <input
                  id="dest-db"
                  className="df2-input"
                  value={destDriverType === "iceberg" ? destSchema : targetDb}
                  onChange={(e) => {
                    if (destDriverType === "iceberg") setDestSchema(e.target.value);
                    else setTargetDb(e.target.value);
                  }}
                  placeholder={
                    destDriverType === "bigquery"
                      ? "my-gcp-project"
                      : destDriverType === "dynamodb"
                        ? "us-east-1"
                        : destDriverType === "iceberg"
                          ? "analytics"
                          : destDriverType === "milvus"
                            ? "default"
                            : "test_db"
                  }
                  autoComplete="off"
                  spellCheck={false}
                />
              </div>
              {destDriverType === "bigquery" && (
                <div className="df2-field">
                  <label className="df2-label" htmlFor="dest-dataset">Dataset</label>
                  <input
                    id="dest-dataset"
                    className="df2-input"
                    value={destSchema}
                    onChange={(e) => setDestSchema(e.target.value)}
                    placeholder="dataflow"
                    autoComplete="off"
                    spellCheck={false}
                  />
                </div>
              )}
              <ObjectNameCombobox
                id="dest-col"
                label={
                  destDriverType === "mongodb"
                    ? "Collection"
                    : destDriverType === "dynamodb"
                      ? "DynamoDB table"
                      : destDriverType === "iceberg"
                        ? "Iceberg table"
                        : destDriverType === "pinecone"
                          ? "Namespace"
                          : destDriverType === "weaviate"
                            ? "Class name"
                            : destDriverType === "qdrant" || destDriverType === "milvus"
                              ? "Collection"
                              : "Table"
                }
                value={targetCollection}
                options={destObjectNames}
                loading={destSchemaLoading && !targetCollection.trim()}
                objectNoun={destDriverType === "mongodb" ? "collection" : "table"}
                placeholder={
                  destDriverType === "mongodb"
                    ? "Pick collection or type new"
                    : destDriverType === "dynamodb"
                      ? "orders"
                      : destDriverType === "iceberg"
                        ? "orders"
                        : destDriverType === "pinecone"
                          ? "default"
                          : destDriverType === "weaviate"
                            ? "DatawrapChunk"
                            : destDriverType === "qdrant" || destDriverType === "milvus"
                              ? "chunks"
                              : "Pick table or type new name"
                }
                emptyHint={
                  destDriverType === "mongodb"
                    ? "No collections discovered yet — type a name to create."
                    : "No tables discovered yet — type a name to create."
                }
                onChange={(next) => {
                  setTargetCollection(next);
                  // Do not claim existence for a name we have not probed yet.
                  proveDestTableExists(null);
                  setDestColumns([]);
                  setDestSchemaMap({});
                  // The destination side of every row described the old table.
                  // Keeping it showed "city VARCHAR(6) [exists]" for a table that
                  // had never been probed.
                  setColumnMappings((prev) => prev.map(markMappingDestUnread));
                  setPreflight(null);
                  setPreflightError("");
                  setValidatedContractKey(null);
                  setCellPreview(null);
                }}
              />
              {(destDriverType === "snowflake"
                || destDriverType.includes("mssql")
                || getGenericSqlGroup(destType) === "postgresql+psycopg2") && (
                <div className="df2-field df2-dest-target-schema">
                  <label className="df2-label" htmlFor="dest-schema">Schema</label>
                  <input
                    id="dest-schema"
                    className="df2-input"
                    value={destSchema}
                    onChange={(e) => setDestSchema(e.target.value)}
                    placeholder={destDriverType === "snowflake" ? "PUBLIC" : destDriverType.includes("mssql") ? "dbo" : "public"}
                    autoComplete="off"
                    spellCheck={false}
                  />
                </div>
              )}
            </div>
            <DestProcedurePanel
              destType={destDriverType || destType}
              destWriteMode={destWriteMode}
              onDestWriteMode={setDestWriteMode}
              destProcedureCall={destProcedureCall}
              onDestProcedureCall={setDestProcedureCall}
              destQuerySql={destQuerySql}
              onDestQuerySql={setDestQuerySql}
              destProcedureParams={destProcedureParams}
              onDestProcedureParams={setDestProcedureParams}
              destProcedureBefore={destProcedureBefore}
              onDestProcedureBefore={setDestProcedureBefore}
              destProcedureAfter={destProcedureAfter}
              onDestProcedureAfter={setDestProcedureAfter}
              sourceColumns={currentSourceColumns}
              paramMap={destProcedureParamMap}
              onParamMap={setDestProcedureParamMap}
            />
            {/* Status when probing / resolved — including pending unknown existence */}
            {destDriverType !== "dynamodb"
              && (destSchemaLoading
                || destTableExists === true
                || destTableExists === false
                || (Boolean(targetCollection.trim()) && destTableExists == null && !destSchemaLoading)) && (
              <div
                className={`df2-dest-target-status${
                  destSchemaLoading
                    ? " is-loading"
                    : destTableExists === true
                      ? " is-existing"
                      : destTableExists === false
                        ? " is-create"
                        : " is-pending"
                }`}
                aria-live="polite"
                role="status"
              >
                {destSchemaLoading ? (
                  <>
                    <Spinner size="sm" label="Analyzing destination schema" />
                    <p>
                      <strong>Checking destination…</strong> Looking up{" "}
                      <code>{targetDb ? `${targetDb}.` : ""}{targetCollection.trim() || "table"}</code>
                      {" "}(column types only — no row scan).
                      {destDriverType === "snowflake" ? (
                        <>
                          {" "}If the warehouse is suspended, Snowflake is waking it
                          (often 20–60s), then one <code>DESC TABLE</code>.
                          {String(targetDb || "").toUpperCase() === "SNOWFLAKE_SAMPLE_DATA" ? (
                            <>
                              {" "}<code>SNOWFLAKE_SAMPLE_DATA</code> is a shared
                              sample catalog (usually read-only) — destination
                              tables normally live in your own database.
                            </>
                          ) : null}
                        </>
                      ) : null}
                    </p>
                  </>
                ) : destTableExists === true ? (
                  <>
                    <DtIcon name="database" size={14} />
                    <p>
                      <strong>Existing table detected</strong>
                      {targetCollection.trim() ? <> — <code>{targetCollection.trim()}</code></> : null}
                      . This is not create-new. The object already exists
                      (even if a prior run wrote 0 rows). Full append inserts into
                      {" "}<strong>live</strong> column types and does not ALTER them.
                      To start a new table, type a name that does not exist.
                      Open Advanced to switch overwrite or incremental.
                      {destColumns.length > 0 ? (
                        <> · {destColumns.length} live columns loaded.</>
                      ) : (
                        <> · Column metadata pending — load the columns before Map invents create-new.</>
                      )}
                    </p>
                  </>
                ) : destTableExists === false ? (
                  <>
                    <DtIcon name="sparkle" size={14} />
                    <p>
                      <strong>
                        {destDriverType === "mongodb" ? "Collection not found." : "Table not found."}
                      </strong>{" "}
                      Datawrap will create it automatically on first write.
                    </p>
                  </>
                ) : (
                  <>
                    <DtIcon name="database" size={14} />
                    <p>
                      <strong>Destination not confirmed yet.</strong> Map stays schema-pending until the
                      table is found or confirmed missing (no invent create-new).
                      {destConnectionError ? <> · {destConnectionError}</> : null}
                    </p>
                    <Button
                      size="sm"
                      variant="primary"
                      disabled={destSchemaLoading}
                      onClick={() => void loadDestinationSchema({ force: true })}
                    >
                      Reload destination schema
                    </Button>
                  </>
                )}
              </div>
            )}
            {/* Schema preview under status — keep visible when exists but columns pending. */}
            {destKindMode === "database"
              && !destSchemaLoading
              && destTableExists === true
              && (
              <div className="df2-dest-schema-preview">
                {destColumns.length > 0 ? (
                  <StructurePreview
                    columns={destColumns}
                    schema={destSchemaMap}
                    title="Existing destination schema"
                    subtitle={`${destColumns.length} fields in ${targetDb}.${targetCollection}`}
                  />
                ) : (
                  <div className="df2-dest-schema-pending" role="status">
                    <p>
                      <strong>Existing table — column metadata pending.</strong>{" "}
                      Load the columns before Map invents create-new types.
                      {destConnectionError ? <> · {destConnectionError}</> : null}
                    </p>
                    <Button
                      size="sm"
                      variant="primary"
                      disabled={destSchemaLoading}
                      onClick={() => void loadDestinationSchema({ force: true })}
                    >
                      Reload destination schema
                    </Button>
                  </div>
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
          ) : (
            <p className="df2-label-hint df2-dest-right-empty">
              Select a saved connection, or open New connection to pick an engine.
            </p>
          )}
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
                {syncModeHonestyLine(syncMode, destTableExists)}
              </p>
              <p className="df2-label-hint">
                {schemaPolicyHonestyLine(schemaPolicy)}
              </p>
              <p className="df2-label-hint">
                Change overwrite, CDC, schema drift, and identity in Advanced.
              </p>
            </div>
            <div className="df2-dest-sync-summary-actions">
              <span className={`df2-badge ${streamNeedsReview ? "df2-badge-run" : "df2-badge-live"}`}>
                {!currentSourceColumns.length
                  ? "Waiting for schema"
                  : streamNeedsReview
                    ? "Sync contract incomplete"
                    : requiresPrimaryKey || requiresCursor
                      ? "Identity fields set"
                      : "Sync mode ready"}
              </span>
            </div>
            {(syncMode === "scd2" || syncMode === "mirror") && requiresPrimaryKey && !primaryKeyField && (
              <p className="df2-label-hint df2-dest-sync-warning">
                {syncMode === "scd2" ? "SCD Type 2" : "Mirror"} requires a primary key — open Advanced to set it.
              </p>
            )}
            {isMultiStreamSource && syncMode === "cdc" && (
              <p className="df2-label-hint" role="status">
                Multi-stream CDC uses a shared log reader when the source supports it
                (Postgres / MySQL / SQL Server / Oracle); otherwise streams run sequentially.
              </p>
            )}
            {isMultiStreamSource && syncMode !== "cdc" && !multiStreamUnsupportedMode && (
              <p className="df2-label-hint" role="status">
                Multi-stream full/incremental runs each table/collection{" "}
                <strong>sequentially</strong> with its own watermark and mappings
                ({multiStreamNames.length} streams). Prefer CDC when you need one shared log consumer.
              </p>
            )}
            {multiStreamUnsupportedMode && (
              <p className="df2-label-hint df2-dest-sync-warning" role="alert">
                {MULTI_STREAM_SCD2_MIRROR_BLOCK}
              </p>
            )}
          </div>

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
          </div>
        </div>
        <div className="df2-card-footer df2-wizard-footer">
          <button type="button" className="df2-btn" onClick={() => setStep(STEP_SOURCE)}>← Back to source</button>
          <div className="df2-btn-row">
          <button
            type="button"
            className="df2-btn df2-btn-ghost"
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
            onClick={() => setStep(STEP_SHAPE)}
            disabled={!canRunPreflight || analyzing}
          >
            {analyzing ? <ButtonLoader label="Preparing mappings…" /> : <><DtIcon name="layers" size={18} /> Continue to Transform</>}
          </button>
          </div>
        </div>
      </div>
      )}

      {step === STEP_SHAPE && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-xform-step">
        <TransferTransformStep
          sampleRows={shapeSampleRows}
          sourceColumns={currentSourceColumns}
          sourceSchema={currentSourceSchema}
          targetSchema={destSchemaMap}
          sourceLabel={sourceLabel}
          destRouteLabel={mapDestRouteLabel}
          rowCount={parsed?.row_count ?? sourceRowEstimate ?? undefined}
          steps={shapeSteps}
          onChangeSteps={setShapeSteps}
          onIdentity={setShapeIdentity}
          onBack={() => setStep(STEP_DESTINATION)}
          onContinue={() => void goToMapping()}
        />
        </div>
      )}

      {step === STEP_VALIDATE && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-validate-step df2-validate-dashboard-host">
          <div className="df2-card-body df2-validate-body">
          <ValidateDashboard
            preflight={preflight}
            preflightError={preflightError}
            running={preflighting}
            progress={validateProgress}
            confidenceThreshold={confidenceThreshold}
            destType={destKindMode === "file_export" ? exportFormat : destType}
            validationMode={validationMode}
            syncMode={syncMode}
            writeViaStaging={writeViaStaging}
            onApplyAction={applySuggestedAction}
            onApplyCreateNewWidens={() => applyCreateNewTypeWidens(createNewFitWidenActions(preflight))}
            onStripControlChars={stripControlCharsAndRerun}
            stripControlsApplied={columnMappings.some(
              (m) => m.transform === "strip_controls",
            )}
            onQuarantineAndRerun={quarantineAndRerun}
            badDataFixOpen={badDataFixOpen}
            onBadDataFixOpenChange={setBadDataFixOpen}
            cellPreview={cellPreview}
            onReviewMappings={(opts) => {
              if (opts?.focusSource) {
                setMapFocusSource(opts.focusSource);
                setMapIdentityBanner(
                  `Validate focused ${opts.focusSource} — column mapping is evidence only. Use identity settings to change the primary key.`,
                );
              }
              setStep(STEP_MAP);
            }}
            onReloadDestSchema={() => { void loadDestinationSchema({ force: true }); }}
            onOpenIdentitySettings={openIdentitySettings}
            onOpenLocaleSettings={openLocaleSettings}
            uniqueKeySuggestions={uniqueKeySuggestions}
            compositeKeySuggestions={compositeKeySuggestions}
            onApplyPrimaryKey={(column) => {
              applyPrimaryKeySuggestion(column);
              openIdentitySettings();
            }}
            onOpenMappingProof={() => setMappingProofOpen(true)}
            mappingProofSummary={mappingProofSummary}
            onRunPreflight={() => void executePreflight()}
            onAcknowledgeCompliance={() => {
              toast({
                title: "Submitting PII approval",
                message: "Re-running Validate with governance approval for detected PII fields.",
                tone: "info",
              });
              void executePreflight(undefined, undefined, {
                complianceAcknowledged: true,
                acknowledgmentReason: "Governance policy allows moving detected PII for this transfer",
              });
            }}
            onAcknowledgeSchemaDrift={() => {
              toast({
                title: "Submitting schema-drift acknowledgment",
                message: "Re-running Validate — existing mappings kept for this run (exception recorded).",
                tone: "info",
              });
              void executePreflight(undefined, undefined, {
                schemaDriftAcknowledged: true,
                acknowledgmentReason: "Keep existing mappings for this run; ignore new/changed columns",
              });
            }}
            onAcknowledgeFkRisk={() => {
              toast({
                title: "Submitting FK-risk acknowledgment",
                message: "Re-running Validate — FK mapping risk accepted for this run (RI not proven).",
                tone: "info",
              });
              void executePreflight(undefined, undefined, {
                fkRiskAcknowledged: true,
                acknowledgmentReason: "Accept destination FK mapping risk for this run; population orphans not proven",
              });
            }}
            runPopulationOrphanScan={runPopulationOrphanScan}
            onRunPopulationOrphanScanChange={setRunPopulationOrphanScan}
            repairJobId={activeJobId || seedStudioIntent?.jobId || persistedPlanId || ""}
            seedRepairProposalId={seedRepairProposalId}
            studioPrimary={studioPrimary}
            onSeedRepairConsumed={() => setSeedRepairProposalId(null)}
            repairMappings={columnMappings.map((m) => ({
              source: m.source,
              destination: m.target,
              destination_type: m.destType,
              target_type: m.destType,
              transform: m.transform || undefined,
              transforms: m.transform ? [{ type: m.transform }] : [],
            }))}
            onRepairMappingsApplied={(updated) => {
              setColumnMappings((prev) => {
                const bySource = new Map(prev.map((m) => [m.source, m]));
                for (const hit of updated) {
                  const src = String(hit.source || "");
                  if (!src) continue;
                  const existing = bySource.get(src);
                  const xf =
                    hit.transform
                    || (Array.isArray(hit.transforms) && hit.transforms[0]?.type)
                    || existing?.transform;
                  const nextType = hit.destination_type || hit.target_type || existing?.destType || "";
                  const typeChanged = Boolean(nextType) && nextType !== (existing?.destType || "");
                  bySource.set(
                    src,
                    sealRemediationApproval({
                      source: src,
                      target: String(hit.destination || existing?.target || src),
                      confidence: existing?.confidence ?? 1,
                      destType: String(nextType),
                      // Operator approved the repair proposal — still fail-closed on lossy.
                      approved: true,
                      requiresReview: false,
                      transform: (xf as EditableMapping["transform"]) || existing?.transform,
                      reason: [
                        existing?.reason,
                        `Repair applied (${hit.destination_type || hit.transform || "update"})`,
                      ].filter(Boolean).join(" · "),
                      sample: existing?.sample,
                      inferredType: existing?.inferredType,
                      isPii: existing?.isPii,
                      existsInDestination: existing?.existsInDestination,
                      createNew: existing?.createNew,
                      assignmentStrategy: existing?.assignmentStrategy,
                      // Dest-type repair must not reuse stale preserve / Accept risk.
                      fidelity: typeChanged ? undefined : existing?.fidelity,
                      fidelityReason: typeChanged ? undefined : existing?.fidelityReason,
                      typeNarrowing: typeChanged ? undefined : existing?.typeNarrowing,
                      riskAcknowledged: typeChanged ? false : existing?.riskAcknowledged,
                    }),
                  );
                }
                const next = [...bySource.values()];
                queueMicrotask(() => void executePreflight(next));
                return next;
              });
              toast({
                title: "Repair applied — re-validating",
                message: "Approved mapping fixes are in place. Re-running Validate to confirm gates pass.",
                tone: "success",
              });
            }}
          />
          </div>
          <ValidateActionsRail
            preflight={preflight}
            preflightError={preflightError}
            preflighting={preflighting}
            transferring={transferring}
            mappingReviewCount={mappingReviewCount}
            riskAckPendingCount={riskAckPendingCount}
            rowCount={parsed?.row_count ?? sourceRowEstimate ?? undefined}
            transferLaunch={routeScopedLaunch}
            supersededRunLabel={
              result && !runResultDescribesCurrentPlan
                ? (staleRunDestLabel || "another destination")
                : undefined
            }
            savingContract={savingContract}
            executeBlocked={multiStreamUnsupportedMode || Boolean(contractBlockReason)}
            executeBlockedReason={
              multiStreamUnsupportedMode
                ? MULTI_STREAM_SCD2_MIRROR_BLOCK
                : contractBlockReason || undefined
            }
            contractSlot={(
              <ContractBindField
                idPrefix="studio"
                contractId={boundContractId}
                requireSigned={requireSignedContract}
                onContractIdChange={setBoundContractId}
                onRequireSignedChange={setRequireSignedContract}
                onBlockReasonChange={setContractBlockReason}
                compact
              />
            )}
            cdcRetentionSlot={
              syncMode === "cdc"
              && sourceConnector
              && ["sqlserver", "mssql", "oracle", "azure_sql_database", "microsoft_sql_server", "amazon_rds_sql_server"].includes(
                resolveDriverType(sourceConnector.type),
              )
                ? (
                  <CdcRetentionPanel
                    probeRequest={{
                      type: sourceConnector.type,
                      host: sourceConnector.host,
                      port: sourceConnector.port,
                      database: sourceConnector.database,
                      username: sourceConnector.username,
                      password: sourceConnector.password,
                      schema: sourceConnector.schema,
                      connection_string: sourceConnector.connection_string,
                      table: primarySourceStream || "",
                      multi_subnet_failover: multiSubnetFailover || undefined,
                    }}
                  />
                )
                : undefined
            }
            onBack={() => setStep(STEP_MAP)}
            onRunPreflight={() => void executePreflight()}
            onApproveMappings={() => void approveAllAndPreflight()}
            onOpenMapForRisk={() => setStep(STEP_MAP)}
            onHoldOutRows={() => void holdOutRowsAndRevalidate()}
            holdingOutRows={preflighting}
            onExecute={() => void executeTransfer()}
            onOpenJobTheater={openJobTheater}
            onSaveAsContract={() => void handleSaveAsContract()}
            onPrimaryFix={primaryFix.onPrimaryFix}
            primaryFixLabel={primaryFix.primaryFixLabel}
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

      {step === STEP_RUN
        && !activeJobId
        && !transferring
        && !routeScopedLaunch
        && (!result || !runResultDescribesCurrentPlan) && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-run-step">
          <div className="df2-card-body df2-run-center">
            <div className="df2-run-readiness" aria-label="Run readiness summary">
              <div className="df2-run-readiness-head">
                {(() => {
                  if (isGovernedExecuteReady) {
                    return (
                      <span className="df2-badge df2-badge-live">
                        <DtIcon name="check" size={12} /> Preflight approved
                      </span>
                    );
                  }
                  const decision = preflight?.proof_bundle?.transfer_decision?.decision;
                  if (preflight?.passed && decision === "review") {
                    return (
                      <span className="df2-badge df2-badge-warn">
                        <DtIcon name="alert" size={12} /> Review-grade preflight
                      </span>
                    );
                  }
                  if (preflight) {
                    return (
                      <span className="df2-badge df2-badge-warn">
                        <DtIcon name="alert" size={12} /> Preflight incomplete
                      </span>
                    );
                  }
                  return (
                    <span className="df2-badge df2-badge-warn">
                      <DtIcon name="alert" size={12} /> Preflight not proven
                    </span>
                  );
                })()}
                <span className="df2-run-readiness-score">
                  {preflight
                    ? `${preflight.passed_count}/${preflight.total_gates} checks`
                    : "No API preflight"}
                </span>
              </div>
              <div className="df2-run-readiness-route">
                <strong>{sourceLabel}</strong>
                <DtIcon name="transfer" size={14} />
                <strong>{mapDestRouteLabel}</strong>
              </div>
              <p>
                {isGovernedExecuteReady
                  ? "Execute now to start governed transfer with live theater progress and reconciliation evidence."
                  : "Re-open Validate to confirm API preflight (decision approve) before treating this run as cleared."}
                {writeViaStaging
                  ? " Staging is on — rows land in {table}_df_staging first; only clean rows promote to primary."
                  : ""}
              </p>
            </div>
            <EmptyState
              icon="transfer"
              title={isGovernedExecuteReady ? "Execute-ready · not migration proven" : "Confirm Validate before write"}
              description={
                (result && !runResultDescribesCurrentPlan
                  ? `The plan changed since the last run${staleRunDestLabel ? ` (it wrote ${staleRunDestLabel})` : ""}; nothing has been written for the route above. `
                  : "")
                + (isGovernedExecuteReady
                  ? "API preflight approved on Validate. Execute starts the write; Gate-8 post-write proof is still required for migration_proven."
                  : "Execute stays locked until API Validate returns decision approve (local/review-grade cannot unlock).")
              }
            />
          </div>
          <div className="df2-card-footer df2-wizard-footer df2-run-footer">
            <button
              type="button"
              className="df2-btn"
              onClick={() => setStep(STEP_VALIDATE)}
            >
              ← Back
            </button>
            <div className="df2-run-footer-status" aria-live="polite">
              <span>
                <strong>Route</strong> {sourceLabel} → {mapDestRouteLabel}
              </span>
              <span>
                <strong>Preflight</strong>{" "}
                {isGovernedExecuteReady
                  ? "API approved"
                  : preflight
                    ? `${preflight.passed_count}/${preflight.total_gates} · not approved`
                    : "not proven"}
              </span>
            </div>
            <div className="df2-run-footer-actions">
              <button
                type="button"
                className="df2-btn df2-btn-primary"
                onClick={() => void executeTransfer()}
                disabled={!canExecute || multiStreamUnsupportedMode}
                title={
                  multiStreamUnsupportedMode
                    ? MULTI_STREAM_SCD2_MIRROR_BLOCK
                    : !canExecute
                      ? "Requires API Validate with decision approve"
                      : undefined
                }
              >
                <DtIcon name="transfer" size={16} /> Execute Transfer
              </button>
            </div>
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
              <div className="df2-run-launch-progress-track">
                <span className="df2-run-launch-progress-fill" style={{ width: `${runStartupProgress}%` }} />
              </div>
            </div>

            <div className="df2-run-launch-stages" aria-label="Launch stages">
              {RUN_LAUNCH_STAGES.map((stage, idx) => {
                const state = launchStageState(runStartupProgress, idx, RUN_LAUNCH_STAGES.length);
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

      {step === STEP_RUN && result && !activeJobId && runResultDescribesCurrentPlan && (
        <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-run-step df2-result-host">
          <div className="df2-card-body df2-result-body">
            <TransferResultDashboard
              result={result}
              sourceLabel={sourceLabel}
              destLabel={mapDestRouteLabel}
              sourceType={sourceKind === "file" ? "file" : sourceConnector?.type || sourceKind}
              destType={destKindMode === "file_export" ? exportFormat : destType}
              mappingProof={mappingProof}
              hideActions
              repairMappings={columnMappings.map((m) => ({
                source: m.source,
                destination: m.target || m.source,
                destination_type: m.destType,
                target_type: m.destType,
                transform: m.transform || undefined,
              }))}
              onRepairMappingsApplied={(updated) => {
                setColumnMappings((prev) => {
                  const bySource = new Map(prev.map((m) => [m.source, m]));
                  for (const hit of updated) {
                    const src = String(hit.source || "");
                    if (!src) continue;
                    const existing = bySource.get(src);
                    const xf =
                      hit.transform
                      || (Array.isArray(hit.transforms) && hit.transforms[0]?.type)
                      || existing?.transform;
                    const nextType = hit.destination_type || hit.target_type || existing?.destType || "";
                    const typeChanged = Boolean(nextType) && nextType !== (existing?.destType || "");
                    bySource.set(
                      src,
                      sealRemediationApproval({
                        source: src,
                        target: String(hit.destination || existing?.target || src),
                        confidence: existing?.confidence ?? 1,
                        destType: String(nextType),
                        approved: true,
                        requiresReview: false,
                        transform: (xf as EditableMapping["transform"]) || existing?.transform,
                        reason: [
                          existing?.reason,
                          `Repair applied (${hit.destination_type || hit.transform || "update"})`,
                        ].filter(Boolean).join(" · "),
                        sample: existing?.sample,
                        inferredType: existing?.inferredType,
                        isPii: existing?.isPii,
                        existsInDestination: existing?.existsInDestination,
                        createNew: existing?.createNew,
                        assignmentStrategy: existing?.assignmentStrategy,
                        fidelity: typeChanged ? undefined : existing?.fidelity,
                        fidelityReason: typeChanged ? undefined : existing?.fidelityReason,
                        typeNarrowing: typeChanged ? undefined : existing?.typeNarrowing,
                        riskAcknowledged: typeChanged ? false : existing?.riskAcknowledged,
                      }),
                    );
                  }
                  const next = [...bySource.values()];
                  queueMicrotask(() => void executePreflight(next));
                  return next;
                });
              }}
              onNewTransfer={resetTransferStudio}
              onSchedule={() => void handleScheduleRoute()}
              onOpenValidate={() => setStep(STEP_VALIDATE)}
              onOpenChildJob={(childId) => {
                setActiveJobId(childId);
                setTransferring(true);
                setResult(null);
              }}
              onResume={
                result.job_id && !result.success
                  ? () => {
                      void (async () => {
                        try {
                          await resumeJob(result.job_id!);
                          toast({
                            title: "Resume started",
                            message: "Continuing from the last durable checkpoint.",
                            tone: "success",
                          });
                          setActiveJobId(result.job_id!);
                          setTransferring(true);
                          setResult(null);
                        } catch (e) {
                          toast({
                            title: "Resume failed",
                            message: e instanceof Error ? e.message : "Resume failed",
                            tone: "error",
                          });
                        }
                      })();
                    }
                  : undefined
              }
            />
          </div>
          <div className="df2-card-footer df2-wizard-footer df2-run-footer">
            <button
              type="button"
              className="df2-btn"
              onClick={() => setStep(STEP_VALIDATE)}
            >
              ← Back
            </button>
            <div className="df2-run-footer-status" aria-live="polite">
              <span>
                <strong>Result</strong> {result.success ? "Completed" : "Needs attention"}
              </span>
              {(() => {
                const dest = destHeadline(result);
                return (
                  <span title={dest.title}>
                    <strong>{dest.label}</strong> {dest.value}
                  </span>
                );
              })()}
            </div>
            <div className="df2-run-footer-actions">
              {(!result.success || Boolean(result.destination_summary?.rejected_rows)) && (
                <button type="button" className="df2-btn" onClick={() => setStep(STEP_VALIDATE)}>
                  <DtIcon name="gate" size={14} /> Open Validate
                </button>
              )}
              {result.job_id && !result.success && (
                <button
                  type="button"
                  className="df2-btn"
                  onClick={() => {
                    void (async () => {
                      try {
                        await resumeJob(result.job_id!);
                        toast({
                          title: "Resume started",
                          message: "Continuing from the last durable checkpoint.",
                          tone: "success",
                        });
                        setActiveJobId(result.job_id!);
                        setTransferring(true);
                        setResult(null);
                      } catch (e) {
                        toast({
                          title: "Resume failed",
                          message: e instanceof Error ? e.message : "Resume failed",
                          tone: "error",
                        });
                      }
                    })();
                  }}
                >
                  Resume
                </button>
              )}
              <button type="button" className="df2-btn" onClick={() => void handleScheduleRoute()}>
                <DtIcon name="activity" size={14} /> Schedule
              </button>
              <button type="button" className="df2-btn df2-btn-primary" onClick={resetTransferStudio}>
                New transfer
              </button>
            </div>
          </div>
        </div>
      )}
      </main>
      </div>

      {/* Shared Advanced drawer — Dest / Map / Validate open it in-place (no step change). */}
      <DestinationAdvancedDrawer
        open={advancedOpen}
        onClose={() => {
          setAdvancedOpen(false);
          setAdvancedLocaleFocus(null);
        }}
        localeFocus={advancedLocaleFocus}
        syncModes={routeSyncModes}
        schemaPolicies={SCHEMA_POLICIES}
        validationModes={VALIDATION_MODES}
        dateLocales={DATE_LOCALES}
        numberLocales={NUMBER_LOCALES}
        syncMode={syncMode}
        syncHonestyLine={syncModeHonestyLine(syncMode, destTableExists)}
        schemaHonestyLine={schemaPolicyHonestyLine(schemaPolicy)}
        schemaPolicy={schemaPolicy}
        validationMode={validationMode}
        dateLocale={dateLocale}
        numberLocale={numberLocale}
        backfillNewFields={backfillNewFields}
        streamNames={advancedStreamNames}
        streamFields={streamFields}
        defaultCursor={cursorField}
        defaultPrimaryKey={primaryKeyField}
        defaultCursorSemantics={cursorSemantics}
        sourceColumns={currentSourceColumns}
        sourceSchema={currentSourceSchema}
        sourceColumnsByStream={sourceColumnsByStream}
        sourceSchemaByStream={sourceSchemaByStream}
        syncModeLabel={syncModeLabel}
        schemaPolicyLabel={schemaPolicyLabel}
        requiresCursor={requiresCursor}
        requiresPrimaryKey={requiresPrimaryKey}
        streamNeedsReview={streamNeedsReview}
        suggestedCursor={cursorCandidate}
        suggestedPrimaryKey={primaryKeyCandidate}
        uniqueKeySuggestions={uniqueKeySuggestions}
        compositeKeySuggestions={compositeKeySuggestions}
        snapshotMode={snapshotMode}
        onSnapshotModeChange={setSnapshotMode}
        deliveryGuarantee={deliveryGuarantee}
        onDeliveryGuaranteeChange={setDeliveryGuarantee}
        exactlyOnceWired={exactlyOnceWiredDest(destDriverType || destType)}
        allowAppendOnly={allowAppendOnly}
        onAllowAppendOnlyChange={setAllowAppendOnly}
        multiSubnetFailover={multiSubnetFailover}
        onMultiSubnetFailoverChange={setMultiSubnetFailover}
        showMultiSubnetFailover={
          syncMode === "cdc"
          && ["sqlserver", "mssql", "azure_sql_database", "microsoft_sql_server", "amazon_rds_sql_server"].includes(
            resolveDriverType(sourceConnector?.type || ""),
          )
        }
        cdcRowFilter={cdcRowFilter}
        onCdcRowFilterChange={setCdcRowFilter}
        showCdcRowFilter={
          syncMode === "cdc"
          && ["sqlserver", "mssql", "azure_sql_database", "microsoft_sql_server", "amazon_rds_sql_server"].includes(
            resolveDriverType(sourceConnector?.type || ""),
          )
        }
        writeViaStaging={writeViaStaging}
        onWriteViaStagingChange={setWriteViaStaging}
        writeViaStagingSupported={writeViaStagingSupported}
        showVectorOptions={
          destDriverType === "pgvector" ||
          destDriverType === "qdrant" ||
          destDriverType === "weaviate" ||
          destDriverType === "pinecone" ||
          destDriverType === "milvus"
        }
        vectorContentColumn={vectorContentColumn}
        vectorEmbeddingColumn={vectorEmbeddingColumn}
        vectorMetadataColumns={vectorMetadataColumns}
        vectorEmbeddingModel={vectorEmbeddingModel}
        vectorChunkSize={vectorChunkSize}
        vectorChunkOverlap={vectorChunkOverlap}
        onVectorContentColumnChange={setVectorContentColumn}
        onVectorEmbeddingColumnChange={setVectorEmbeddingColumn}
        onVectorMetadataColumnsChange={setVectorMetadataColumns}
        onVectorEmbeddingModelChange={setVectorEmbeddingModel}
        onVectorChunkSizeChange={setVectorChunkSize}
        onVectorChunkOverlapChange={setVectorChunkOverlap}
        vectorRoutingFields={vectorRoutingFields}
        vectorRoutingLoading={vectorRoutingLoading}
        vectorExcludePiiColumns={vectorExcludePiiColumns}
        onApplyVectorRouting={() => void runVectorRouting(true)}
        vectorDurableCache={vectorDurableCache}
        onVectorDurableCacheChange={setVectorDurableCache}
        embeddingCacheStats={embeddingCacheStats}
        embeddingCacheBusy={embeddingCacheBusy}
        onRefreshEmbeddingCache={() => void refreshEmbeddingCacheStats()}
        onClearEmbeddingCache={() => void handleClearEmbeddingCache()}
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
          } else {
            setBackfillNewFields(false);
          }
        }}
        onValidationModeChange={setValidationMode}
        onDateLocaleChange={setDateLocale}
        onNumberLocaleChange={setNumberLocale}
        onBackfillChange={setBackfillNewFields}
        onStreamCursorChange={(stream, value) => {
          setStreamFields((prev) => ({
            ...prev,
            [stream]: {
              cursorField: value,
              primaryKeyField: prev[stream]?.primaryKeyField ?? primaryKeyField,
              // A new column is a new question: the previous column's declared
              // meaning says nothing about this one.
              cursorSemantics: "",
            },
          }));
          if (!isMultiStreamSource || stream === advancedStreamNames[0]) {
            setCursorField(value);
            setCursorSemantics("");
          }
        }}
        onStreamCursorSemanticsChange={(stream, value) => {
          setStreamFields((prev) => ({
            ...prev,
            [stream]: {
              cursorField: prev[stream]?.cursorField ?? cursorField,
              primaryKeyField: prev[stream]?.primaryKeyField ?? primaryKeyField,
              cursorSemantics: value,
            },
          }));
          if (!isMultiStreamSource || stream === advancedStreamNames[0]) {
            setCursorSemantics(value);
          }
        }}
        onStreamPrimaryKeyChange={(stream, value) => {
          setStreamFields((prev) => ({
            ...prev,
            [stream]: {
              cursorField: prev[stream]?.cursorField ?? cursorField,
              primaryKeyField: value,
              cursorSemantics: prev[stream]?.cursorSemantics ?? cursorSemantics,
            },
          }));
          if (!isMultiStreamSource || stream === advancedStreamNames[0]) {
            setPrimaryKeyField(value);
          }
        }}
      />
      </PageFrame>
    </PageShell>
  );
}
