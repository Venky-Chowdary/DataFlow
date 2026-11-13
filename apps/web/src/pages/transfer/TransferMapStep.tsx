import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import { ColumnReviewPanel, type ColumnRemapControls } from "../../components/ColumnReviewPanel";
import { AutoMapDialog } from "../../components/mapping/AutoMapDialog";
import { ComputedColumnDialog } from "../../components/mapping/ComputedColumnDialog";
import { TemplatesDialog } from "../../components/mapping/TemplatesDialog";
import { HiddenFileInput } from "../../components/ui/HiddenFileInput";
import { applyImportedMappings, documentRows, templateUnboundNote } from "../../lib/mappingCode";
import type { ShapeStepWire } from "../../lib/shape";
import { DestinationColumnsPanel, DND_MIME } from "../../components/mapping/DestinationColumnsPanel";
import { MappingToolbar } from "../../components/mapping/MappingToolbar";
import { useTouchDrag } from "../../components/mapping/useTouchDrag";
import {
  useDestinationCatalog,
  useMappingStudio,
  useTypeGrades,
} from "../../components/mapping/useMappingStudio";
import { mapTargetIssues, normalizeColumnName } from "../../lib/destColumnCatalog";
import { liveCarrierIsDoomedOnThisRun } from "../../lib/destSchemaRecreate";
import { isIntentionalOmit, type EditableMapping as MapRow } from "../../lib/mapping";
import {
  applyMappingTemplate,
  createMappingTemplate,
  exportMappingDocument,
  fetchAutoMap,
  importMappingDocument,
  recordMappingMemory,
  type AutoMapSuggestion,
} from "../../lib/mappingStudioApi";
import {
  MappingProofDrawer,
  mergeMappingProof,
  type MappingProof,
} from "../../components/MappingProofDrawer";
import { Dialog } from "../../components/ui/Dialog";
import { Button } from "../../components/ui/Button";
import { ProgressRing } from "../../components/ui/ProgressRing";
import { DtIcon } from "../../components/DtIcon";
import type { ColumnFilter } from "../../lib/columnWorkbench";
import { countByFilter, needsMappingReview } from "../../lib/columnWorkbench";
import { BusinessRuleLedger } from "../../components/transfer/BusinessRuleLedger";
import type { RuleCompileReport } from "../../lib/businessRules";
import { ruleCensus, ruleMixSummary } from "../../lib/businessRules";
import type { EditableMapping } from "../../lib/mapping";
import { mappingHealthSummary } from "../../lib/mapping";
import { destCatalogExists } from "../../lib/destSchemaIdentity";
import { mapApprovalPendingCount, mapBlockerSummary, mapCanContinueToValidate } from "../../lib/mapBlockers";
import type { UniqueKeySuggestion } from "../../lib/uniqueKeySuggestions";

interface TransferMapStepProps {
  columnMappings: EditableMapping[];
  analysis: import("../../lib/types").EnhancedAnalysis | null;
  destColumns: string[];
  destSchemaLoading: boolean;
  /** null = unknown, true = confirmed on destination, false = will CREATE. */
  destTableExists?: boolean | null;
  destConnected?: boolean | null;
  destConnectionError?: string;
  targetCollection: string;
  targetDatabase: string;
  destKindMode: string;
  destType: string;
  sourceLabel: string;
  sourceSubtitle: string;
  sourceType: string;
  destRouteLabel: string;
  destRouteSubtitle: string;
  mappingReviewCount: number;
  confidenceThreshold: number;
  rowCount?: number;
  sampleRows?: Record<string, unknown>[];
  sourceColumnCount?: number;
  llmUsed?: boolean;
  /** Structured proof from mapping pipeline (preferred). */
  mappingProof?: MappingProof | null;
  /** Controlled proof drawer (shared with Validate). */
  proofOpen?: boolean;
  onProofOpenChange?: (open: boolean) => void;
  /** Multi-stream map: stream names for tab strip (comma-separated sources). */
  streamNames?: string[];
  activeStream?: string | null;
  onActiveStreamChange?: (name: string) => void;
  /** True when stream schemas differ — operator must review each tab. */
  streamsDiverge?: boolean;
  /** Stream name currently being rematched (or "all"). */
  streamBusy?: string | null;
  /** Rematch every stream against the destination schema. */
  onRematchAllStreams?: () => void | Promise<void>;
  onChangeMappings: (mappings: EditableMapping[]) => void;
  onBack: () => void;
  onContinue: () => void;
  /** Deep-link from Validate: focus a source column in the mapping table. */
  initialFocusSource?: string | null;
  /** Shown when Validate sent the operator here for identity/duplicate-key work. */
  identityFixBanner?: string | null;
  onIdentityFixConsumed?: () => void;
  /** Identity / sync contract (Destination → Advanced) — always visible on Map. */
  syncModeLabel?: string;
  primaryKeyField?: string;
  cursorField?: string;
  requiresPrimaryKey?: boolean;
  requiresCursor?: boolean;
  onOpenIdentitySettings?: () => void;
  uniqueKeySuggestions?: UniqueKeySuggestion[];
  compositeKeySuggestions?: Array<{ columns: string[]; uniqueCount: number; sampleRows: number }>;
  onApplyPrimaryKey?: (column: string) => void;
  /** Clears the operator-chosen identity so create-new declares no key on it. */
  onClearPrimaryKey?: () => void;
  /** Dest-exists extras the operator must remap or omit — never silent drop. */
  extraSourceColumns?: string[];
  destShapeHeadline?: string;
  /** Re-probe the destination catalog and re-map — the only exit from an unread dest schema. */
  onReloadDestSchema?: () => void | Promise<void>;
  /** Compiled workbook — shown as evidence on each mapped column. */
  ruleReport?: RuleCompileReport | null;
  onAcceptRuleDirect?: (index: number) => void;
  /** Review already owns the action bar. */
  hideFooter?: boolean;
  /**
   * The destination endpoint Studio probes (same body as /transfer/introspect).
   * With it, Map reads the destination's column catalog for the picker/panel.
   */
  destEndpoint?: Record<string, unknown> | null;
  /** Identity of ``destEndpoint`` (connector/host/db/schema/table). */
  destProbeKey?: string;
  /** Destination column types Studio already read (picker fallback). */
  destSchemaTypes?: Record<string, string>;
  schemaPolicy?: string;
  /** Raw sync mode id (G14 is moot when an overwrite recreates the table). */
  syncMode?: string;
  /** Source table/collection name — auto-map reads ``<entity>_id`` against it. */
  sourceObjectName?: string;
  /** The Transform recipe: its column-producing steps travel with exports/templates. */
  transformSteps?: ShapeStepWire[];
  /**
   * Add computed-column steps to Transform (the one expression engine) and
   * take the operator there to preview them. Absent, computed columns are off.
   */
  onAddComputedSteps?: (steps: ShapeStepWire[]) => void;
}


const MAP_STEP_SCROLL_CLASS = "is-map-step-view";

export function TransferMapStep({
  columnMappings,
  analysis,
  destColumns,
  destSchemaLoading,
  destTableExists = null,
  destConnected = null,
  destConnectionError = "",
  targetCollection,
  targetDatabase,
  destKindMode,
  destType,
  sourceLabel,
  sourceSubtitle: _sourceSubtitle,
  sourceType: _sourceType,
  destRouteLabel,
  destRouteSubtitle: _destRouteSubtitle,
  mappingReviewCount,
  confidenceThreshold,
  rowCount,
  sampleRows,
  llmUsed,
  mappingProof,
  proofOpen: proofOpenProp,
  onProofOpenChange,
  streamNames = [],
  activeStream = null,
  onActiveStreamChange,
  streamsDiverge = false,
  streamBusy = null,
  onRematchAllStreams,
  onChangeMappings,
  onBack,
  onContinue,
  initialFocusSource = null,
  identityFixBanner = null,
  onIdentityFixConsumed,
  syncModeLabel = "",
  primaryKeyField = "",
  cursorField = "",
  requiresPrimaryKey = false,
  requiresCursor = false,
  onOpenIdentitySettings,
  uniqueKeySuggestions = [],
  compositeKeySuggestions = [],
  onApplyPrimaryKey,
  onClearPrimaryKey,
  extraSourceColumns = [],
  destShapeHeadline = "",
  onReloadDestSchema,
  ruleReport = null,
  onAcceptRuleDirect,
  hideFooter = false,
  destEndpoint = null,
  destProbeKey = "",
  destSchemaTypes,
  schemaPolicy = "",
  syncMode = "",
  sourceObjectName = "",
  transformSteps = [],
  onAddComputedSteps,
}: TransferMapStepProps) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<ColumnFilter>("review");
  const [userPickedFilter, setUserPickedFilter] = useState(false);
  const [focusSource, setFocusSource] = useState<string | null>(null);
  const [mapDialogOpen, setMapDialogOpen] = useState(false);
  // Collapsed by default: the headline plus one primary control is the whole
  // decision. Ten expanded reasons buried the mapping grid below the fold.
  const [blockerDetailOpen, setBlockerDetailOpen] = useState(false);
  const [proofOpenLocal, setProofOpenLocal] = useState(false);
  const proofOpen = proofOpenProp ?? proofOpenLocal;
  const setProofOpen = onProofOpenChange ?? setProofOpenLocal;

  // ---- Remapping onto the destination's own columns -------------------------
  const remapEnabled = destKindMode === "database";
  const panelId = useId();
  const gripHelpId = useId();
  // Beside the grid on a desktop; on a narrow screen it would halve the grid, so
  // it starts closed there and opens on demand (or when a drag starts).
  const [panelOpen, setPanelOpen] = useState(
    () => typeof window === "undefined" || typeof window.matchMedia !== "function"
      || window.matchMedia("(min-width: 1024px)").matches,
  );
  const [issuesOpen, setIssuesOpen] = useState(false);
  const emptyTypes = useMemo(() => ({}), []);
  const catalogState = useDestinationCatalog({
    endpoint: destEndpoint,
    probeKey: destProbeKey,
    schemaPolicy,
    names: destColumns,
    schema: destSchemaTypes ?? emptyTypes,
    tableExists: destTableExists,
    destType,
    enabled: remapEnabled && Boolean(destEndpoint),
  });
  const catalog = remapEnabled ? catalogState.catalog : null;
  // Once the operator edits, the rows they are working on must not jump out
  // of view because the issues-first filter re-evaluated behind them.
  const operatorEdited = useRef(false);
  const onOperatorEdit = useCallback(
    (next: MapRow[]) => {
      operatorEdited.current = true;
      onChangeMappings(next);
    },
    [onChangeMappings],
  );
  const [visibleTargets, setVisibleTargets] = useState<string[] | null>(null);
  const studio = useMappingStudio({
    mappings: columnMappings,
    onChange: onOperatorEdit,
    catalog,
    visibleTargets: panelOpen ? visibleTargets : null,
  });
  const grades = useTypeGrades(remapEnabled ? columnMappings : [], destType, destTableExists);
  const tableRecreated = liveCarrierIsDoomedOnThisRun({ syncMode, destDbType: destType, destTableExists });
  const targetIssues = useMemo(
    () => (remapEnabled ? mapTargetIssues(studio.summary, columnMappings, { tableRecreated }) : []),
    [remapEnabled, studio.summary, columnMappings, tableRecreated],
  );
  const openSources = columnMappings.filter((m) => !isIntentionalOmit(m) && !m.target.trim()).length;

  const [autoMap, setAutoMap] = useState<{
    open: boolean;
    mode: "all" | "remaining" | "template";
    loading: boolean;
    error: string;
    suggestions: AutoMapSuggestion[];
    templateName?: string;
    notes?: string[];
  }>({ open: false, mode: "all", loading: false, error: "", suggestions: [] });
  const [computedOpen, setComputedOpen] = useState(false);
  const [templatesOpen, setTemplatesOpen] = useState(false);
  const [codeStatus, setCodeStatus] = useState("");
  const [pendingComputed, setPendingComputed] = useState<ShapeStepWire[]>([]);
  const importId = useId();
  const computedSteps = useMemo(
    () => transformSteps.filter((s) => ["derive_column", "constant_column", "concat_columns", "run_timestamp"].includes(s.op)),
    [transformSteps],
  );
  const documentBody = () => ({
    mappings: documentRows(columnMappings),
    computed: computedSteps.map((s) => ({ op: s.op, options: s.options ?? {} })),
    name: [sourceObjectName, targetCollection].filter(Boolean).join(" → "),
    source: { table: sourceObjectName },
    destination: { table: targetCollection, type: destType },
  });

  const exportAs = async (format: "yaml" | "json") => {
    try {
      const { text, filename } = await exportMappingDocument(documentBody(), format);
      const url = typeof URL.createObjectURL === "function"
        ? URL.createObjectURL(new Blob([text], { type: format === "yaml" ? "application/yaml" : "application/json" }))
        : "";
      if (url) {
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      }
      setCodeStatus(`Exported ${filename} (${columnMappings.length} mappings, checksum included).`);
    } catch (e) {
      setCodeStatus(e instanceof Error ? e.message : "Export failed");
    }
  };

  const importFile = async (file: File | undefined) => {
    if (!file) return;
    try {
      const doc = await importMappingDocument(await file.text());
      const out = applyImportedMappings(columnMappings, doc.mappings, catalog);
      studio.change(out.rows);
      setPendingComputed(doc.computed);
      const parts = [
        `Imported ${out.applied} mapping${out.applied === 1 ? "" : "s"} from ${file.name}`,
        doc.checksum_verified ? "unchanged since export" : "no checksum: treated as hand-edited",
        out.unmatched.length ? `not in this source: ${out.unmatched.join(", ")}` : "",
        doc.computed.length ? `${doc.computed.length} computed column(s) to add to Transform` : "",
      ].filter(Boolean);
      setCodeStatus(`${parts.join(" · ")}. Undo reverts the import.`);
      studio.announce(parts[0]);
    } catch (e) {
      setCodeStatus(e instanceof Error ? e.message : "Import failed");
    }
  };

  const applyTemplate = async (t: { id: string; name: string }) => {
    setTemplatesOpen(false);
    setAutoMap({ open: true, mode: "template", templateName: t.name, loading: true, error: "", suggestions: [] });
    try {
      const out = await applyMappingTemplate(
        t.id,
        columnMappings.filter((m) => !isIntentionalOmit(m)).map((m) => m.source),
        (catalog?.columns ?? []).map((c) => c.name),
        (catalog?.columns ?? []).filter((c) => c.writable === false).map((c) => c.name),
      );
      setPendingComputed(out.computed ?? []);
      setAutoMap({
        open: true, mode: "template", templateName: t.name, loading: false, error: "",
        suggestions: out.suggestions,
        notes: [
          ...out.unbound.map(templateUnboundNote),
          ...(out.computed?.length ? [`The template also adds ${out.computed.length} computed column(s): use “Add to Transform” after applying.`] : []),
        ],
      });
    } catch (e) {
      setAutoMap({ open: true, mode: "template", templateName: t.name, loading: false, error: e instanceof Error ? e.message : "Template failed", suggestions: [] });
    }
  };

  const runAutoMap = async (mode: "all" | "remaining") => {
    setAutoMap({ open: true, mode, loading: true, error: "", suggestions: [] });
    try {
      const suggestions = await fetchAutoMap({
        sourceColumns: columnMappings
          .filter((m) => !isIntentionalOmit(m))
          .map((m) => ({ name: m.source, type: m.inferredType })),
        destinationColumns: (catalog?.columns ?? []).map((c) => ({ name: c.name, type: c.type, writable: c.writable })),
        mapped: columnMappings
          .filter((m) => !isIntentionalOmit(m) && m.target)
          .map((m) => ({ source: m.source, target: m.target })),
        mode,
        sourceTable: sourceObjectName,
        destTable: targetCollection,
        destType,
        destination: destEndpoint,
      });
      setAutoMap({ open: true, mode, loading: false, error: "", suggestions });
    } catch (e) {
      setAutoMap({
        open: true,
        mode,
        loading: false,
        error: e instanceof Error ? e.message : "Auto-map failed",
        suggestions: [],
      });
    }
  };

  const currentTargetOf = useCallback(
    (source: string) => columnMappings.find((m) => m.source === source)?.target ?? "",
    [columnMappings],
  );

  const focusSourceRow = (source: string) => {
    setSearch("");
    setFilter("all");
    setFocusSource(source);
  };

  const touch = useTouchDrag(studio, () => setPanelOpen(true));

  const renderGrip = (m: MapRow) => {
    const active = studio.dnd.mode === "dragging" && studio.dnd.source === m.source;
    return (
      <button
        type="button"
        className={`df2-ms-grip${active ? " is-grabbed" : ""}`}
        draggable
        {...touch.handlers(m.source)}
        aria-roledescription="draggable source column"
        aria-label={`Move ${m.source} to a destination column`}
        aria-describedby={gripHelpId}
        aria-pressed={active}
        onKeyDown={(e) => {
          if ((e.key === " " || e.key === "Enter") && !active) setPanelOpen(true);
          studio.onGripKeyDown(m.source, e);
        }}
        onBlur={() => {
          if (active && studio.dnd.mode === "dragging" && studio.dnd.via === "keyboard") {
            studio.dispatchDnd({ type: "cancel" });
          }
        }}
        onDragStart={(e) => {
          e.dataTransfer.setData(DND_MIME, m.source);
          e.dataTransfer.setData("text/plain", m.source);
          e.dataTransfer.effectAllowed = "link";
          setPanelOpen(true);
          studio.dispatchDnd({ type: "pickUp", source: m.source, via: "pointer" });
        }}
        onDragEnd={() => {
          if (studio.dnd.mode === "dragging") studio.dispatchDnd({ type: "cancel" });
        }}
      >
        <span aria-hidden="true" className="df2-ms-grip-dots" />
      </button>
    );
  };

  const remapControls: ColumnRemapControls | undefined = remapEnabled && catalog
    ? {
        catalog,
        claimedBy: studio.claimedBy,
        conflicts: studio.conflicts,
        onRemap: studio.remap,
        gradeFor: grades.gradeFor,
        gradesPending: grades.pending,
        renderGrip,
      }
    : undefined;

  const continueWithMemory = () => {
    // Confirmed, renamed pairs teach this workspace's learned auto-map tier.
    const learned = columnMappings
      .filter((m) => m.approved && !isIntentionalOmit(m) && m.target)
      .filter((m) => normalizeColumnName(m.source) !== normalizeColumnName(m.target))
      .map((m) => ({ source: m.source, target: m.target }));
    if (remapEnabled && learned.length && destEndpoint) {
      void recordMappingMemory(learned, destEndpoint).catch(() => {
        /* Memory is a suggestion source; Validate never depends on it. */
      });
    }
    onContinue();
  };


  const handleFilterChange = (next: ColumnFilter) => {
    setUserPickedFilter(true);
    setFilter(next);
  };

  useEffect(() => {
    // Skip while an identity/focus dialog is open so we don't fight setFilter("all").
    if (userPickedFilter || focusSource || operatorEdited.current) return;
    const needs = columnMappings.filter((m) => needsMappingReview(m, confidenceThreshold)).length;
    setFilter(needs > 0 ? "review" : "all");
  }, [columnMappings, confidenceThreshold, userPickedFilter, focusSource]);

  useEffect(() => {
    const content = document.querySelector(".df2-content");
    const inner = document.querySelector(".df2-content-inner");
    content?.classList.add(MAP_STEP_SCROLL_CLASS);
    inner?.classList.add(MAP_STEP_SCROLL_CLASS);
    return () => {
      content?.classList.remove(MAP_STEP_SCROLL_CLASS);
      inner?.classList.remove(MAP_STEP_SCROLL_CLASS);
    };
  }, []);

  useEffect(() => {
    if (!initialFocusSource) return;
    setFocusSource(initialFocusSource);
    setSearch(initialFocusSource);
    // Do not set userPickedFilter — issues-first resumes when focusSource clears.
    setFilter("all");
    setMapDialogOpen(true);
    onIdentityFixConsumed?.();
  }, [initialFocusSource]);

  const destDisplayType = destType || (destKindMode === "database" ? "" : "file");

  const filterCounts = useMemo(
    () => countByFilter(columnMappings, confidenceThreshold),
    [columnMappings, confidenceThreshold],
  );

  const approvedCount = filterCounts.ready;

  const health = useMemo(
    () => mappingHealthSummary(columnMappings, confidenceThreshold),
    [columnMappings, confidenceThreshold],
  );

  // Same predicate as the Continue gate — the reason shown is the reason enforced.
  const blockerSummary = useMemo(
    () => mapBlockerSummary(columnMappings, confidenceThreshold),
    [columnMappings, confidenceThreshold],
  );

  /** Prefer API proof; refresh pair list from live edits so operators see current transforms. */
  const effectiveProof = useMemo(
    () => mergeMappingProof(mappingProof, columnMappings, {
      destColumns,
      destType: destDisplayType,
      destTableExists: destCatalogExists(destKindMode, destTableExists),
    }),
    [mappingProof, columnMappings, destColumns, destDisplayType, destKindMode, destTableExists],
  );

  const identityFooterLabel = requiresPrimaryKey && !primaryKeyField
    ? "PK required"
    : requiresPrimaryKey && primaryKeyField
      ? `PK ${primaryKeyField}`
      : `${syncModeLabel || "sync"}`;

  const approvalPendingCount = mapApprovalPendingCount(columnMappings);
  const canContinueToValidate =
    mapCanContinueToValidate(columnMappings, confidenceThreshold) && targetIssues.length === 0;
  const mappingFooterLabel = health.falseFriendCount > 0
    ? health.headline
    : mappingReviewCount > 0
      ? `${mappingReviewCount} need review`
      : approvalPendingCount > 0
        ? `${approvalPendingCount} need approval`
      : `${approvedCount} ready`;
  const approvedPct = columnMappings.length
    ? Math.round((approvedCount / columnMappings.length) * 100)
    : 0;
  const showPkSuggest = Boolean(
    requiresPrimaryKey
    && !primaryKeyField
    && (uniqueKeySuggestions.length > 0 || compositeKeySuggestions.length > 0),
  );

  // Census stays on the closed summary so review/conflict counts are visible
  // without pushing the mapping table below the fold.
  const ruleBannerLine = useMemo(() => {
    if (!ruleReport) return "";
    const census = ruleCensus(ruleReport);
    return `${ruleMixSummary(ruleReport)} · ${census.executable} executable · ${census.review} review · ${census.conflict} conflict`;
  }, [ruleReport]);

  const continueToValidate = (
    <button
      type="button"
      className="df2-btn df2-btn-primary"
      onClick={continueWithMemory}
      disabled={!canContinueToValidate}
      title={
        targetIssues.length > 0
          ? `${targetIssues.length} destination column issue(s)\n${targetIssues.slice(0, 4).map((t) => t.title).join("\n")}`
          : blockerSummary.blockers.length > 0
          ? `${blockerSummary.headline}\n${blockerSummary.detail}`
          : approvalPendingCount > 0
            ? `${approvalPendingCount} column(s) need Approve before Validate.`
          : "Continue to Validate"
      }
    >
      Continue to Validate →
    </button>
  );

  return (
    <div className="df2-transfer-step-panel df2-transfer-step-viewport df2-map-step-panel">
      {hideFooter && (
        <div className="df2-wizard-back">
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={onBack}>
            Back to Connect
          </button>
        </div>
      )}
      <div
        className="df2-card-head df2-map-step-head"
        title={`${columnMappings.length} mappings · ${approvedCount} ready${health.falseFriendCount > 0 ? ` · ${health.headline}` : mappingReviewCount > 0 ? ` · ${mappingReviewCount} need review` : approvalPendingCount > 0 ? ` · ${approvalPendingCount} need approval` : ""}`}
      >
        <div className="df2-map-step-head-copy">
          <h3 className="df2-card-title">Map columns</h3>
          <p className="df2-card-sub">
            Align source fields to destination types
            {destDisplayType ? ` · ${destDisplayType}` : ""}
            {llmUsed ? " · semantic engine" : ""}
            {streamNames.length > 1 ? ` · ${streamNames.length} streams` : ""}
            {targetDatabase && targetCollection ? ` · ${targetDatabase}.${targetCollection}` : ""}
          </p>
        </div>
        <div className="df2-map-step-head-actions">
          {(effectiveProof.summary?.cdc_detected || (effectiveProof.sync_mode || "").toLowerCase().includes("cdc")) && (
            <span className="df2-badge df2-badge-info df2-badge-xs" title="Change-stream / CDC route — at-least-once upsert by default">
              CDC
            </span>
          )}
          <button
            type="button"
            className="df2-btn df2-btn-sm"
            onClick={() => setProofOpen(true)}
            title="Inspect how this map works — confidence evidence, transforms, fidelity risks"
          >
            <DtIcon name="sparkle" size={14} />
            <span className="df2-map-btn-label">Proof</span>
          </button>
          <button
            type="button"
            className="df2-btn df2-btn-sm df2-btn-ghost"
            onClick={() => setMapDialogOpen(true)}
            title="Open full mapping table in a dialog"
          >
            <DtIcon name="expand" size={14} />
            <span className="df2-map-btn-label">Expand</span>
          </button>
        </div>
      </div>

      <div className="df2-card-body df2-map-step-body">
        {(llmUsed || (!primaryKeyField && uniqueKeySuggestions[0] && onApplyPrimaryKey)) && (
          <p className="df2-map-inline-suggest" data-testid="map-inline-suggest" role="status">
            <span>Suggested</span>
            {!primaryKeyField && uniqueKeySuggestions[0] && onApplyPrimaryKey ? (
              <button
                type="button"
                className="df2-btn df2-btn-sm"
                onClick={() => onApplyPrimaryKey(uniqueKeySuggestions[0].column)}
              >
                Use {uniqueKeySuggestions[0].column} as the key
              </button>
            ) : (
              <span>Semantic mapping is on this route. Approve the rows that match.</span>
            )}
          </p>
        )}
        {destTableExists !== false && extraSourceColumns.length > 0 && (
          <details className="df2-map-stream-diverge is-compact" role="status">
            <summary>
              <DtIcon name="layers" size={16} />
              <strong>{destShapeHeadline || "Extra source columns — remap or omit"}</strong>
              <span> · {extraSourceColumns.length}</span>
            </summary>
            <p>
              {extraSourceColumns.join(", ")} — dest-exists write is name-addressed.
              These columns are not dropped. Use Remap dest or mark omit.
            </p>
          </details>
        )}

        {streamNames.length > 1 && (
          <div className="df2-map-stream-bar" role="tablist" aria-label="Map per source stream">
            {streamNames.map((name) => (
              <button
                key={name}
                type="button"
                role="tab"
                aria-selected={activeStream === name}
                className={`df2-map-stream-tab${activeStream === name ? " is-active" : ""}${streamBusy === name ? " is-busy" : ""}`}
                onClick={() => onActiveStreamChange?.(name)}
                disabled={Boolean(streamBusy)}
              >
                {name}
                {streamBusy === name ? "…" : ""}
              </button>
            ))}
          </div>
        )}

        {streamsDiverge && streamNames.length > 1 && (
          <div className="df2-map-stream-diverge" role="alert">
            <DtIcon name="alert" size={16} />
            <div>
              <strong>Stream schemas differ</strong>
              <p>
                Each tab has its own column mapping (sent as per-stream write contracts).
                Review every stream before Validate — incompatible shared destinations still
                need separate routes.
              </p>
              {onRematchAllStreams && (
                <button
                  type="button"
                  className="df2-btn df2-btn-sm"
                  disabled={Boolean(streamBusy)}
                  onClick={() => void onRematchAllStreams()}
                >
                  {streamBusy === "all" ? "Rematching…" : "Rematch all streams"}
                </button>
              )}
            </div>
          </div>
        )}

        {ruleReport && (
          <details className="df2-rule-map-banner">
            <summary title={ruleBannerLine}>
              <span className="df2-rule-map-chevron" aria-hidden="true" />
              <DtIcon name="book" size={16} />
              <strong>Business rules</strong>
              <span className="df2-rule-map-census">{ruleBannerLine}</span>
            </summary>
            <div className="df2-rule-map-body">
              <p className="df2-rule-map-kicker">
                Destination names, write transforms, and lookups land here.
                Transform kept the pre-load image. Validate contracts never write
                a column — row-level ones quarantine failing rows at Run, and
                population ones (unique) are checked after load.
                Click a rule to see where it came from: workbook row → compiled rule → destination column.
              </p>
              {ruleReport.honesty ? (
                <details className="df2-rule-honesty">
                  <summary>Compiler contract</summary>
                  <p>{ruleReport.honesty}</p>
                </details>
              ) : null}
              <BusinessRuleLedger
                report={ruleReport}
                embedded
                onAcceptDirect={onAcceptRuleDirect}
              />
            </div>
          </details>
        )}
        {blockerSummary.blockers.length > 0 && (
          <div className="df2-map-blocker-bar" role="status">
            <div className="df2-map-blocker-bar-head">
              <DtIcon name="alert" size={16} />
              <strong>{blockerSummary.headline}</strong>
              {blockerSummary.destSchemaUnloadedOnly && onReloadDestSchema && (
                <Button
                  size="sm"
                  variant="primary"
                  disabled={destSchemaLoading}
                  onClick={() => void onReloadDestSchema()}
                >
                  {destSchemaLoading ? "Reading destination…" : "Reload destination schema"}
                </Button>
              )}
              <button
                type="button"
                className="df2-btn df2-btn-sm df2-btn-ghost"
                aria-expanded={blockerDetailOpen}
                onClick={() => setBlockerDetailOpen((v) => !v)}
              >
                {blockerDetailOpen ? "Hide detail" : `Why (${blockerSummary.groups.length})`}
              </button>
            </div>
            {blockerDetailOpen && (
              <ul className="df2-map-blocker-list">
                {blockerSummary.groups.map((g) => (
                  <li key={g.code}>
                    <strong>{g.title}</strong>
                    <span> — {g.action}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
        {targetIssues.length > 0 && (
          <div className="df2-map-blocker-bar df2-ms-target-issues" role="status" data-testid="map-target-issues">
            <div className="df2-map-blocker-bar-head">
              <DtIcon name="alert" size={16} />
              <strong>
                {targetIssues.length} destination column issue{targetIssues.length === 1 ? "" : "s"} block Validate
              </strong>
              <button
                type="button"
                className="df2-btn df2-btn-sm df2-btn-ghost"
                aria-expanded={issuesOpen}
                onClick={() => setIssuesOpen((v) => !v)}
              >
                {issuesOpen ? "Hide detail" : `Why (${targetIssues.length})`}
              </button>
            </div>
            <span className="df2-sr-only">{targetIssues.map((t) => t.title).join(". ")}.</span>
            {issuesOpen && <ul className="df2-map-blocker-list">
              {targetIssues.slice(0, 8).map((t) => (
                <li key={`${t.code}:${t.title}`}>
                  <strong>{t.title}</strong>
                  <span> — {t.action}</span>
                </li>
              ))}
              {targetIssues.length > 8 ? <li>…and {targetIssues.length - 8} more.</li> : null}
            </ul>}
          </div>
        )}
        {identityFixBanner && (
          <div className="df2-map-identity-banner is-compact" role="status">
            <DtIcon name="alert" size={16} />
            <div className="df2-map-identity-banner-body">
              <strong>Identity fix required</strong>
              <p>{identityFixBanner}</p>
            </div>
            <div className="df2-map-identity-banner-actions">
              {onOpenIdentitySettings && (
                <Button
                  size="sm"
                  variant="primary"
                  leadingIcon={<DtIcon name="settings" size={14} />}
                  onClick={onOpenIdentitySettings}
                >
                  Settings
                </Button>
              )}
              <Button size="sm" variant="ghost" onClick={() => onIdentityFixConsumed?.()}>
                Dismiss
              </Button>
            </div>
          </div>
        )}
        {/* Toolbar and split scroll inside the Map body: the classic view is a fixed-height card. */}
        <div className="df2-ms-scroll">
        {remapControls && (
          <MappingToolbar
            canUndo={studio.canUndo}
            canRedo={studio.canRedo}
            canAutoMap={Boolean(catalog && catalog.columns.length > 0)}
            panelOpen={panelOpen}
            panelId={panelId}
            openSources={openSources}
            onAutoMap={() => void runAutoMap("all")}
            onMapRemaining={() => void runAutoMap("remaining")}
            onClearAll={studio.clearAll}
            onUndo={studio.undo}
            onRedo={studio.redo}
            onTogglePanel={() => setPanelOpen((v) => !v)}
            extra={
              <>
                {onAddComputedSteps && (
                  <button type="button" className="df2-btn df2-btn-sm" onClick={() => setComputedOpen(true)}>
                    Computed column…
                  </button>
                )}
                <button type="button" className="df2-btn df2-btn-sm" onClick={() => setTemplatesOpen(true)}>
                  Templates…
                </button>
                <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => void exportAs("yaml")}>
                  Export YAML
                </button>
                <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => void exportAs("json")}>
                  Export JSON
                </button>
                <HiddenFileInput
                  id={importId}
                  accept=".yaml,.yml,.json,application/json,application/yaml,text/yaml"
                  onChange={(e) => {
                    void importFile(e.target.files?.[0]);
                    e.target.value = "";
                  }}
                />
                <label htmlFor={importId} className="df2-btn df2-btn-sm df2-btn-ghost df2-ms-file-label">
                  Import…
                </label>
              </>
            }
          />
        )}
        {(codeStatus || (pendingComputed.length > 0 && onAddComputedSteps)) && (
          <p className="df2-ms-code-status" role="status" data-testid="map-code-status">
            {codeStatus}
            {pendingComputed.length > 0 && onAddComputedSteps ? (
              <button
                type="button"
                className="df2-btn df2-btn-sm"
                onClick={() => {
                  onAddComputedSteps(pendingComputed);
                  setPendingComputed([]);
                }}
              >
                Add {pendingComputed.length} computed column{pendingComputed.length === 1 ? "" : "s"} to Transform
              </button>
            ) : null}
          </p>
        )}
        {studio.dnd.mode === "dragging" && (studio.dnd.via === "keyboard" || touch.touching) && (
          <p className="df2-ms-drag-status" aria-hidden="true">
            Moving <strong>{studio.dnd.source}</strong> → <strong>{studio.dragOver ?? "…"}</strong>.
            {touch.touching ? "Lift your finger on a destination column to drop it there." : "Arrow keys choose, Enter drops, Escape cancels."}
          </p>
        )}
        <div className={`df2-ms-split${remapControls && panelOpen ? " has-panel" : ""}`}>
        <div className="df2-map-step-workspace is-full-editor">
          <div className="df2-map-editor-pane">
            <div className="df2-map-editor-scroll-host">
              <ColumnReviewPanel
                mappings={columnMappings}
                rowCount={rowCount}
                sampleRows={sampleRows}
                confidenceThreshold={confidenceThreshold}
                onChange={studio.change}
                remap={remapControls}
                destinationFields={destColumns}
                destinationLabel={destRouteLabel}
                destType={destDisplayType}
                destSchemaLoading={destSchemaLoading}
                destTableExists={destTableExists}
                destConnected={destConnected}
                destConnectionError={destConnectionError}
                tableName={targetCollection}
                compact
                hideTitle
                search={search}
                onSearchChange={setSearch}
                filter={filter}
                onFilterChange={handleFilterChange}
                focusSource={focusSource}
                onFocusHandled={() => setFocusSource(null)}
              />
            </div>
          </div>
        </div>
          {remapControls && panelOpen && (
            <DestinationColumnsPanel
              id={panelId}
              tableLabel={targetDatabase && targetCollection ? `${targetDatabase}.${targetCollection}` : targetCollection || destRouteLabel}
              catalog={catalog}
              states={studio.states}
              summary={studio.summary}
              loading={catalogState.loading || destSchemaLoading}
              error={catalogState.error}
              onReload={() => {
                catalogState.reload();
                if (onReloadDestSchema) void onReloadDestSchema();
              }}
              onUnmap={studio.unmapDestination}
              onFocusSource={focusSourceRow}
              dragOver={studio.dragOver}
              dragging={studio.dnd.mode === "dragging"}
              onPointerDrop={studio.pointerDrop}
              onPointerOver={studio.pointerOver}
              onVisibleChange={setVisibleTargets}
            />
          )}
        </div>
        </div>
        <span id={gripHelpId} className="df2-sr-only">
          Press Space or Enter to pick up, arrow keys to move over destination columns, Enter to drop, Escape to cancel.
        </span>
        <div className="df2-sr-only" role="status" aria-live="polite" aria-atomic="true" data-testid="map-live-region">
          {studio.announcement}
        </div>
      </div>

      {!hideFooter && <div className="df2-card-footer df2-wizard-footer df2-map-footer">
        <button type="button" className="df2-btn" onClick={onBack}>← Back</button>
        <div className="df2-map-footer-status" aria-live="polite">
          <ProgressRing
            value={approvedPct}
            size={36}
            tone={!canContinueToValidate ? "warn" : "ok"}
            className="df2-map-footer-ring"
          >
            <strong>{approvedPct}</strong>
          </ProgressRing>
          <span className={!canContinueToValidate ? "is-warn" : "is-ok"} title={health.weak ? health.detail : undefined}>
            <strong>Mapping</strong> {mappingFooterLabel}
          </span>
          <span
            className={requiresPrimaryKey && !primaryKeyField ? "is-warn" : undefined}
            title={
              requiresCursor
                ? `Cursor ${cursorField || "required"} · Sync ${syncModeLabel || "—"}`
                : `Sync ${syncModeLabel || "—"}`
            }
          >
            <strong>Identity</strong> {identityFooterLabel}
          </span>
          {showPkSuggest && (
            <details className="df2-map-pk-details">
              <summary>Try PK</summary>
              <div className="df2-map-pk-details-body">
                {uniqueKeySuggestions.slice(0, 2).map((s) => (
                  <button
                    key={s.column}
                    type="button"
                    className="df2-map-footer-pk-suggest"
                    title={`Unique in ${s.sampleRows}-row sample — not full-table proof`}
                    onClick={() => {
                      onApplyPrimaryKey?.(s.column);
                      onOpenIdentitySettings?.();
                    }}
                  >
                    {s.column}
                  </button>
                ))}
                {compositeKeySuggestions.slice(0, 2).map((s) => (
                  <button
                    key={s.columns.join(",")}
                    type="button"
                    className="df2-map-footer-pk-suggest"
                    title={`Composite unique in ${s.sampleRows}-row sample`}
                    onClick={() => onApplyPrimaryKey?.(s.columns.join(","))}
                  >
                    {s.columns.join(" + ")}
                  </button>
                ))}
              </div>
            </details>
          )}
        </div>
        <div className="df2-map-footer-actions">
          {/* Not inside .df2-map-footer-status: compact widths hide that strip. */}
          {primaryKeyField && onClearPrimaryKey ? (
            <button
              type="button"
              className="df2-btn df2-btn-ghost"
              data-testid="map-clear-primary-key"
              title={`Stop using ${primaryKeyField} as the key — a new table is created without it`}
              onClick={onClearPrimaryKey}
            >
              Clear key
            </button>
          ) : null}
          {onOpenIdentitySettings && (
            <button
              type="button"
              className="df2-btn df2-btn-ghost"
              onClick={onOpenIdentitySettings}
              title="Open Advanced settings — primary key, sync mode, cursor (same drawer as Destination)"
            >
              <DtIcon name="settings" size={14} /> Advanced
            </button>
          )}
          {continueToValidate}
        </div>
      </div>}

      <Dialog
        open={mapDialogOpen}
        onClose={() => setMapDialogOpen(false)}
        size="full"
        title="Edit column mappings"
        subtitle={
          destColumns.length > 0
            ? `${columnMappings.length} columns · match existing destination fields — wrong types fail preflight, not silently.`
            : destTableExists === true
              ? `${columnMappings.length} columns · existing destination table (reload columns to match DDL).`
              : destTableExists === false
                ? `${columnMappings.length} columns · create-new destination — fields CREATE on first write (no existing table required).`
                : `${columnMappings.length} columns · destination schema not confirmed yet — retry Destination/Map before inventing create-new fields.`
        }
        ariaLabel="Full mapping table"
        className="df2-map-dialog"
        footer={
          <button type="button" className="df2-btn df2-btn-primary" onClick={() => setMapDialogOpen(false)}>
            Done
          </button>
        }
      >
        {destColumns.length === 0 && !destSchemaLoading && destTableExists === false && (
          <div className="df2-map-dialog-banner" role="status">
            <DtIcon name="sparkle" size={16} />
            <span>
              <strong>Create-new {destDisplayType || "destination"}</strong>
              {" — "}
              Every source column appears below as a destination field. No existing MongoDB collection or SQL table is required.
            </span>
          </div>
        )}
        {destColumns.length === 0 && !destSchemaLoading && destTableExists === true && (
          <div className="df2-map-dialog-banner" role="status">
            <DtIcon name="alert" size={16} />
            <span>
              <strong>Existing table detected</strong>
              {" — column metadata is missing. Go back to Destination and re-select the table, then return to Map."}
            </span>
          </div>
        )}
        {destColumns.length === 0 && !destSchemaLoading && destTableExists == null && (
          <div className="df2-map-dialog-banner" role="status">
            <DtIcon name="alert" size={16} />
            <span>
              {destConnected === false ? (
                <>
                  <strong>Destination connection failed</strong>
                  {` — ${destConnectionError || "Could not reach the destination."} `}
                  <button type="button" className="df2-btn df2-btn-sm" onClick={onBack}>
                    Fix destination connection
                  </button>
                </>
              ) : destConnectionError ? (
                <>
                  <strong>Destination schema could not be loaded</strong>
                  {` — ${destConnectionError} `}
                  <button type="button" className="df2-btn df2-btn-sm" onClick={onBack}>
                    Retry Destination/Map
                  </button>
                </>
              ) : (
                <>
                  <strong>Destination schema unknown</strong>
                  {" — could not confirm whether the table exists. Retry Destination/Map; do not treat this as create-new."}
                </>
              )}
            </span>
          </div>
        )}
        <ColumnReviewPanel
          mappings={columnMappings}
          rowCount={rowCount}
          confidenceThreshold={confidenceThreshold}
          onChange={studio.change}
          remap={remapControls}
          destinationFields={destColumns}
          destinationLabel={destRouteLabel}
          destType={destDisplayType}
          destSchemaLoading={destSchemaLoading}
          destTableExists={destTableExists}
          destConnected={destConnected}
          destConnectionError={destConnectionError}
          tableName={targetCollection}
          presentation="dialog"
          search={search}
          onSearchChange={setSearch}
          filter={filter}
          onFilterChange={handleFilterChange}
          focusSource={focusSource}
          onFocusHandled={() => setFocusSource(null)}
        />
      </Dialog>

      <ComputedColumnDialog
        open={computedOpen}
        catalog={catalog}
        sourceColumns={columnMappings.map((m) => m.source)}
        onClose={() => setComputedOpen(false)}
        onAdd={(step) => {
          setComputedOpen(false);
          onAddComputedSteps?.([step]);
        }}
      />
      <TemplatesDialog
        open={templatesOpen}
        onClose={() => setTemplatesOpen(false)}
        onSave={async (name, description) => {
          await createMappingTemplate({ ...documentBody(), name, description });
        }}
        onApply={(t) => void applyTemplate(t)}
      />
      <AutoMapDialog
        open={autoMap.open}
        mode={autoMap.mode}
        templateName={autoMap.templateName}
        notes={autoMap.notes}
        loading={autoMap.loading}
        error={autoMap.error}
        suggestions={autoMap.suggestions}
        currentTarget={currentTargetOf}
        onApply={(picked) => {
          studio.applySuggestions(picked);
          setAutoMap((a) => ({ ...a, open: false }));
        }}
        onClose={() => setAutoMap((a) => ({ ...a, open: false }))}
      />

      <MappingProofDrawer
        open={proofOpen}
        onClose={() => setProofOpen(false)}
        proof={effectiveProof}
        sourceLabel={sourceLabel}
        destLabel={destRouteLabel}
      />
    </div>
  );
}
