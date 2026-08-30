import { DtIcon } from "./DtIcon";
import { FilterTabs } from "./ui/FilterTabs";
import { StructurePreview } from "./ui/StructurePreview";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  ARRAY_POLICIES,
  MAPPING_TRANSFORMS,
  STRUCT_POLICIES,
  applyDestTypeChange,
  applyOperatorRemapDest,
  applyStructPolicyChange,
  applyDeclaredSourceZone,
  assumeTimezoneAwaitingZone,
  declaredSourceZone,
  suggestedSourceZones,
  acknowledgeMappingRisk,
  applyTransformChange,
  approveMappingHonestly,
  approveMappingsHonestly,
  canWidenMapping,
  classifyMappingReview,
  confirmFalseFriendMapping,
  countApproveEligible,
  isFalseFriendReview,
  mappingHealthSummary,
  mappingReviewKindMeta,
  EXECUTION_POLICY_OPTIONS,
  flagExistingEnumBooleanConflict,
  isArrayLogicalType,
  isEnumToBooleanConflict,
  isExistingEnumBooleanConflict,
  isIntentionalOmit,
  isSpecialtyLogicalType,
  isStructLogicalType,
  createNewRiskDetail,
  engineStampedRiskChip,
  formatColumnProfileStrip,
  hasCreateNewTypeRisk,
  isDestSchemaPending,
  mappingAckDoneLabel,
  mappingAckLabel,
  mappingAckTier,
  mappingRequiresRiskAck,
  mappingRiskChipState,
  clearExistingDestTypeOverride,
  isExistingDestTypeOverride,
  pipelineTransformChip,
  widenMappingToVarchar,
  type EditableMapping,
  type ExecutionPolicy,
  type MappingTransform,
  type StructPolicy,
} from "../lib/mapping";
import {
  COLUMN_PAGE_SIZES,
  type ColumnFilter,
  type ColumnPageSize,
  type ColumnSort,
  countByFilter,
  filterMappings,
  isMappingReady,
  needsMappingReview,
  paginateMappings,
  totalPages,
} from "../lib/columnWorkbench";
import {
  mapBandLabel,
  partitionMapBands,
  shouldCollapseSafeBand,
} from "../lib/mapSafeBand";
import { destTypeSelectOptions, normalizeDestTypeValue, typeBadgeClass } from "../lib/typeDisplay";

interface ColumnReviewPanelProps {
  mappings: EditableMapping[];
  rowCount?: number;
  sampleRows?: Record<string, unknown>[];
  onChange: (mappings: EditableMapping[]) => void;
  confidenceThreshold?: number;
  compact?: boolean;
  destinationFields?: string[];
  destinationLabel?: string;
  /** Destination connector/db type — drives dest-aware DDL labels (e.g. Snowflake NUMBER). */
  destType?: string;
  /** True while destination schema introspection is in flight. */
  destSchemaLoading?: boolean;
  /** null = unknown; true = table confirmed; false = will create. */
  destTableExists?: boolean | null;
  destConnected?: boolean | null;
  destConnectionError?: string;
  showTransforms?: boolean;
  hideTitle?: boolean;
  /** Expand-dialog layout: table-first, no nested preview, fixed scroll height. */
  presentation?: "default" | "dialog";
  focusSource?: string | null;
  onFocusHandled?: () => void;
  search?: string;
  onSearchChange?: (value: string) => void;
  filter?: ColumnFilter;
  onFilterChange?: (value: ColumnFilter) => void;
  /** Plan / migration id stamped onto Risk Contracts for audit. */
  migrationId?: string;
  /** Destination table stamped onto Risk Contracts. */
  tableName?: string;
}

function confidenceClass(
  c: number,
  threshold: number,
  approved: boolean,
  riskOpen = false,
): string {
  // Confidence is evidence, not clearance — never paint green until Approve.
  if (riskOpen) return "block";
  if (approved) return "ok";
  if (c >= threshold) return "warn";
  if (c >= threshold - 0.1) return "warn";
  return "block";
}

const FILTER_TABS: { id: ColumnFilter; label: string }[] = [
  { id: "all", label: "All" },
  { id: "review", label: "Review" },
  { id: "block", label: "Critical" },
  { id: "warn", label: "Low" },
  { id: "pii", label: "PII" },
  { id: "new", label: "New" },
  { id: "ready", label: "Ready" },
];

export function ColumnReviewPanel({
  mappings,
  rowCount,
  sampleRows,
  onChange,
  confidenceThreshold = 0.85,
  compact = false,
  destinationFields = [],
  destinationLabel,
  destType,
  destSchemaLoading = false,
  destTableExists = null,
  destConnected = null,
  destConnectionError = "",
  showTransforms = true,
  hideTitle = false,
  presentation = "default",
  focusSource = null,
  onFocusHandled,
  search: searchProp,
  onSearchChange,
  filter: filterProp,
  onFilterChange,
  migrationId = "",
  tableName = "",
}: ColumnReviewPanelProps) {
  const [internalSearch, setInternalSearch] = useState("");
  const [internalFilter, setInternalFilter] = useState<ColumnFilter>("review");
  const [sort, setSort] = useState<ColumnSort>("confidence-asc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState<ColumnPageSize>(50);
  const [previewPage, setPreviewPage] = useState(1);
  const [previewPageSize, setPreviewPageSize] = useState(12);
  const [safeBandExpanded, setSafeBandExpanded] = useState(false);
  const [readyBandExpanded, setReadyBandExpanded] = useState(false);
  const rowRefs = useRef<Map<string, HTMLTableRowElement>>(new Map());
  const destInputRefs = useRef<Map<string, HTMLInputElement>>(new Map());

  const search = searchProp ?? internalSearch;
  const setSearch = onSearchChange ?? setInternalSearch;
  const filter = filterProp ?? internalFilter;
  const setFilter = onFilterChange ?? setInternalFilter;

  const destColumnSet = useMemo(
    () => new Set(destinationFields.map((c) => c.toLowerCase())),
    [destinationFields],
  );

  const filterCounts = useMemo(
    () => countByFilter(mappings, confidenceThreshold),
    [mappings, confidenceThreshold],
  );

  const zoneSuggestions = useMemo(
    () =>
      mappings.some((m) => m.transform === "assume_timezone") ? suggestedSourceZones() : [],
    [mappings],
  );

  const filtered = useMemo(
    () => filterMappings(mappings, { search, filter, sort, threshold: confidenceThreshold }),
    [mappings, search, filter, sort, confidenceThreshold],
  );

  // Safe-band accordion only on unfiltered "all" — Issues filter stays flat.
  const useSafeBands = filter === "all" && !search.trim();
  const mapBands = useMemo(() => partitionMapBands(filtered), [filtered]);
  const collapseSafe = useSafeBands && shouldCollapseSafeBand(mapBands) && !safeBandExpanded;
  const collapseReady = useSafeBands && mapBands.ready.length >= 2 && !readyBandExpanded;

  const displayItems = useMemo(() => {
    if (!useSafeBands) return filtered;
    return [
      ...mapBands.attention,
      ...(collapseSafe ? [] : mapBands.safe),
      ...(collapseReady ? [] : mapBands.ready),
      ...mapBands.omitted,
    ];
  }, [useSafeBands, filtered, mapBands, collapseSafe, collapseReady]);

  const pages = totalPages(displayItems.length, pageSize);
  const pageItems = useMemo(
    () => paginateMappings(displayItems, page, pageSize),
    [displayItems, page, pageSize],
  );

  const needsReview = mappings.filter((m) => needsMappingReview(m, confidenceThreshold));
  const approvedCount = mappings.filter((m) => isMappingReady(m, confidenceThreshold)).length;
  const avgConfidence = mappings.length
    ? mappings.reduce((s, m) => s + m.confidence, 0) / mappings.length
    : 0;


  // Issues-first default when the panel owns its filter (uncontrolled).
  useEffect(() => {
    if (filterProp !== undefined) return;
    const needs = mappings.filter((m) => needsMappingReview(m, confidenceThreshold)).length;
    setInternalFilter(needs > 0 ? "review" : "all");
  }, [mappings, confidenceThreshold, filterProp]);

  useEffect(() => {
    setPage(1);
  }, [search, filter, sort, pageSize, mappings.length, safeBandExpanded, readyBandExpanded]);

  useEffect(() => {
    // New Map payload — re-collapse safe band (Issues-first default).
    setSafeBandExpanded(false);
    setReadyBandExpanded(false);
  }, [mappings.length]);

  useEffect(() => {
    setPreviewPage(1);
  }, [sampleRows, previewPageSize]);

  useEffect(() => {
    if (page > pages) setPage(pages);
  }, [page, pages]);

  useEffect(() => {
    if (!focusSource) return;
    const matchIndex = filtered.findIndex(({ mapping }) => mapping.source === focusSource);
    if (matchIndex >= 0) {
      const targetPage = Math.floor(matchIndex / pageSize) + 1;
      if (targetPage !== page) setPage(targetPage);
    }
  }, [focusSource, filtered, pageSize, page]);

  useEffect(() => {
    if (!focusSource) return;
    const row = rowRefs.current.get(focusSource);
    if (row) {
      row.scrollIntoView({ block: "nearest", behavior: "smooth" });
      row.classList.add("is-focused");
      const t = window.setTimeout(() => row.classList.remove("is-focused"), 2200);
      onFocusHandled?.();
      return () => window.clearTimeout(t);
    }
    if (filtered.some(({ mapping }) => mapping.source === focusSource)) {
      return undefined;
    }
    onFocusHandled?.();
    return undefined;
  }, [focusSource, pageItems, filtered, onFocusHandled]);

  /** Per-row execution policy choice — no hidden CAST_AND_CONTINUE default. */
  const [policyBySource, setPolicyBySource] = useState<Record<string, ExecutionPolicy | "">>({});

  const updateMapping = (index: number, patch: Partial<EditableMapping>) => {
    const next = mappings.map((m, i) => (i === index ? { ...m, ...patch } : m));
    onChange(next);
  };

  const approveAll = () => {
    onChange(approveMappingsHonestly(mappings));
  };

  const approveAllSafe = () => {
    const safeIndexes = new Set(mapBands.safe.map((item) => item.index));
    if (safeIndexes.size === 0) return;
    onChange(
      mappings.map((m, i) => (safeIndexes.has(i) ? approveMappingHonestly(m) : m)),
    );
    setSafeBandExpanded(false);
  };

  const approveOne = (index: number) => {
    const m = mappings[index];
    if (!m) return;
    if (isFalseFriendReview(m) && !m.falseFriendConfirmed) {
      updateMapping(index, confirmFalseFriendMapping(m));
      return;
    }
    // Fidelity / STRUCT / specialty need explicit risk ack — bare Approve must not clear G4.
    if (mappingRequiresRiskAck(m)) {
      const chosen = policyBySource[m.source];
      if (!chosen) {
        // Fail closed: refuse to invent CAST_AND_CONTINUE.
        updateMapping(index, {
          ...m,
          approved: false,
          requiresReview: true,
          reason: [m.reason, "Choose an execution policy before signing Risk Contract"]
            .filter(Boolean)
            .join(" · "),
        });
        return;
      }
      updateMapping(
        index,
        acknowledgeMappingRisk(m, {
          executionPolicy: chosen,
          migrationId: migrationId || undefined,
          table: tableName || undefined,
          planId: migrationId || undefined,
          estimatedRows: rowCount ?? null,
        }),
      );
      return;
    }
    updateMapping(index, approveMappingHonestly(m));
  };

  const eligibleApproveCount = countApproveEligible(mappings);
  const health = useMemo(
    () => mappingHealthSummary(mappings, confidenceThreshold),
    [mappings, confidenceThreshold],
  );
  const focusDestInput = (source: string) => {
    const input = destInputRefs.current.get(source);
    if (input) {
      input.focus();
      input.select();
      return;
    }
    const row = rowRefs.current.get(source);
    row?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  };

  const focusIssues = () => {
    setFilter("review");
    setSort("confidence-asc");
    setSearch("");
    setPage(1);
  };

  // Map hotkeys: A approve eligible, R review filter, X accept risk on first open row.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const t = e.target as HTMLElement | null;
      if (!t) return;
      const tag = (t.tagName || "").toLowerCase();
      if (tag === "input" || tag === "textarea" || tag === "select" || t.isContentEditable) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const key = e.key.toLowerCase();
      if (key === "a") {
        e.preventDefault();
        approveAll();
        return;
      }
      if (key === "r") {
        e.preventDefault();
        focusIssues();
        return;
      }
      if (key === "x") {
        const idx = mappings.findIndex(
          (m) => mappingRequiresRiskAck(m) && !m.riskAcknowledged && !isIntentionalOmit(m),
        );
        if (idx < 0) return;
        e.preventDefault();
        approveOne(idx);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [mappings, onChange]);

  const pageStart = displayItems.length === 0 ? 0 : (page - 1) * pageSize + 1;
  const pageEnd = Math.min(page * pageSize, displayItems.length);

  const previewRows = useMemo(() => {
    if (!sampleRows || sampleRows.length === 0) return [];
    const start = (previewPage - 1) * previewPageSize;
    return sampleRows.slice(start, start + previewPageSize);
  }, [sampleRows, previewPage, previewPageSize]);
  const previewTotal = sampleRows?.length || 0;
  const previewPages = Math.max(1, Math.ceil(previewTotal / previewPageSize));
  const previewStart = previewTotal === 0 ? 0 : (previewPage - 1) * previewPageSize + 1;
  const previewEnd = Math.min(previewPage * previewPageSize, previewTotal);
  const previewSubtitle = previewTotal
    ? `Rows ${previewStart.toLocaleString()}–${previewEnd.toLocaleString()} of ${previewTotal.toLocaleString()} sample rows`
    : "Source data preview";

  const tableControls = (
    <div className="df2-column-workbench-table-controls">
      <label className="df2-column-workbench-sort-label">
        Sort
        <select
          className="df2-input df2-select df2-column-workbench-sort"
          value={sort}
          onChange={(e) => setSort(e.target.value as ColumnSort)}
          aria-label="Sort columns"
        >
          <option value="confidence-asc">Issues first</option>
          <option value="confidence-desc">Highest confidence</option>
          <option value="name-asc">Name A–Z</option>
          <option value="name-desc">Name Z–A</option>
        </select>
      </label>
      <label className="df2-column-workbench-sort-label">
        Page size
        <select
          className="df2-input df2-select df2-column-workbench-pagesize"
          value={pageSize}
          onChange={(e) => setPageSize(Number(e.target.value) as ColumnPageSize)}
          aria-label="Rows per page"
        >
          {COLUMN_PAGE_SIZES.map((size) => (
            <option key={size} value={size}>{size}</option>
          ))}
        </select>
      </label>
    </div>
  );

  if (!mappings.length) {
    return (
      <div className="df2-column-review df2-column-review-empty">
        <p>No columns detected yet. Upload a file or select a source table.</p>
      </div>
    );
  }

  const isDialog = presentation === "dialog";
  const showHead = !hideTitle && !isDialog;
  const showPreview = !isDialog && !compact && Boolean(sampleRows && sampleRows.length > 0);

  const filterTabItems = FILTER_TABS.map((tab) => ({
    ...tab,
    count: compact && !isDialog ? undefined : filterCounts[tab.id],
  }));

  return (
    <div
      className={[
        "df2-column-review",
        compact ? "is-compact is-editor" : "",
        isDialog ? "is-dialog" : "",
      ].filter(Boolean).join(" ")}
    >
      {zoneSuggestions.length > 0 && (
        <datalist id="df2-iana-zones">
          {zoneSuggestions.map((z) => (
            <option key={z} value={z} />
          ))}
        </datalist>
      )}
      {showHead && (
        <div className="df2-column-review-head">
          <div>
            <h3 className="df2-column-review-title">Edit mappings</h3>
            <p className="df2-column-review-sub">
              {destinationLabel
                ? <>Map source columns into <strong>{destinationLabel}</strong> — </>
                : null}
              tweak names, transforms, then approve.
              {" · "}
              {approvedCount}/{mappings.length} ready
              {rowCount != null && ` · ${rowCount.toLocaleString()} rows`}
            </p>
          </div>
          {eligibleApproveCount > 0 && compact && (
            <button
              type="button"
              className="df2-btn df2-btn-primary df2-btn-sm"
              onClick={approveAll}
              title="Approves eligible rows only — specialty identity, STRUCT flatten, and existing DDL conflicts stay for review"
            >
              <DtIcon name="check" size={14} /> Approve eligible ({eligibleApproveCount})
            </button>
          )}
        </div>
      )}

      {showPreview && (
        <div className="df2-column-review-data-preview">
          <StructurePreview
            columns={mappings.map((m) => m.source)}
            schema={Object.fromEntries(mappings.map((m) => [m.source, m.inferredType || "string"]))}
            rows={previewRows}
            rowCount={rowCount}
            title="Source data preview"
            subtitle={previewSubtitle}
            showFieldStrip={false}
            showBadge={false}
            maxRows={previewPageSize}
            maxCols={mappings.length}
          />
          <div className="df2-column-review-preview-controls">
            <span className="df2-column-review-preview-pager">
              <button
                type="button"
                className="df2-btn df2-btn-sm"
                disabled={previewPage <= 1}
                onClick={() => setPreviewPage((p) => Math.max(1, p - 1))}
                aria-label="Previous preview rows"
              >
                ← Prev
              </button>
              <span>
                Page {previewPage.toLocaleString()} of {previewPages.toLocaleString()}
              </span>
              <button
                type="button"
                className="df2-btn df2-btn-sm"
                disabled={previewPage >= previewPages}
                onClick={() => setPreviewPage((p) => Math.min(previewPages, p + 1))}
                aria-label="Next preview rows"
              >
                Next →
              </button>
            </span>
            <label className="df2-column-workbench-sort-label">
              Rows per page
              <select
                className="df2-input df2-select df2-column-workbench-pagesize"
                value={previewPageSize}
                onChange={(e) => setPreviewPageSize(Number(e.target.value))}
                aria-label="Preview rows per page"
              >
                {[12, 25, 50, 100].map((size) => (
                  <option key={size} value={size}>{size}</option>
                ))}
              </select>
            </label>
          </div>
        </div>
      )}

      <div className="df2-column-review-editor">
      <div className="df2-column-review-chrome">
        {(isDialog || !compact) && (
          <div className="df2-column-workbench-stats" aria-label="Mapping summary">
            <div className="df2-column-workbench-stat">
              <span>Total columns</span>
              <strong>{mappings.length.toLocaleString()}</strong>
            </div>
            <div className="df2-column-workbench-stat df2-column-workbench-stat-ok">
              <span>Ready</span>
              <strong>{approvedCount.toLocaleString()}</strong>
            </div>
            <div className="df2-column-workbench-stat df2-column-workbench-stat-warn">
              <span>Needs review</span>
              <strong>{filterCounts.review.toLocaleString()}</strong>
            </div>
            <div className="df2-column-workbench-stat df2-column-workbench-stat-block">
              <span>Critical</span>
              <strong>{filterCounts.block.toLocaleString()}</strong>
            </div>
            <div className="df2-column-workbench-stat">
              <span>PII</span>
              <strong>{filterCounts.pii.toLocaleString()}</strong>
            </div>
            <div className="df2-column-workbench-stat">
              <span>Avg confidence</span>
              <strong>{(avgConfidence * 100).toFixed(0)}%</strong>
            </div>
          </div>
        )}

        <div className="df2-column-workbench-toolbar">
          <FilterTabs
            items={filterTabItems}
            value={filter}
            onChange={setFilter}
            className="df2-column-workbench-filters"
            ariaLabel="Filter columns"
          />
          <div className="df2-column-workbench-search-wrap">
            <DtIcon name="search" size={16} />
            <input
              type="search"
              className="df2-input df2-column-workbench-search"
              placeholder="Search columns…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              aria-label="Search columns"
              autoComplete="off"
              spellCheck={false}
            />
            {search && (
              <button
                type="button"
                className="df2-close-btn df2-close-btn-sm df2-column-workbench-clear"
                onClick={() => setSearch("")}
                aria-label="Clear search"
              >
                <DtIcon name="x" size={14} />
              </button>
            )}
          </div>
          <div className="df2-column-workbench-actions">
            {tableControls}
            {filterCounts.review > 0 && !compact && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={focusIssues}>
                <DtIcon name="alert" size={14} /> Issues ({filterCounts.review})
              </button>
            )}
            {eligibleApproveCount > 0 && (
              <button
                type="button"
                className="df2-btn df2-btn-primary df2-btn-sm"
                onClick={approveAll}
                title="Approves eligible rows only — specialty / flatten / existing DDL conflicts stay for review"
              >
                <DtIcon name="check" size={14} /> Approve eligible ({eligibleApproveCount})
              </button>
            )}
          </div>
        </div>

        {health.falseFriendCount > 0 && (
          <div className="df2-column-review-alert" role="status">
            <DtIcon name="alert" size={16} />
            <span>
              <strong>{health.headline}</strong>
              {" — "}
              {health.detail}
            </span>
          </div>
        )}
        {needsReview.length > 0 && filter === "review" && !compact && health.falseFriendCount === 0 && (
          <div className="df2-column-review-alert" role="status">
            <DtIcon name="alert" size={16} />
            <span>
              <strong>{needsReview.length} column(s)</strong> need review before transfer.
              {mappings.some((m) => mappingRequiresRiskAck(m) && !m.riskAcknowledged && !isIntentionalOmit(m)) && (
                <>
                  {" "}
                  Use <strong>Review</strong> for reversible casts and <strong>Accept risk</strong> for lossy / irreversible changes — Approve alone will not unlock Validate on those rows.
                </>
              )}
            </span>
          </div>
        )}

        {!isDialog && !compact && !destSchemaLoading && destColumnSet.size === 0 && destTableExists === false && (
          <div className="df2-column-review-alert df2-column-review-alert-info" role="status">
            <DtIcon name="sparkle" size={16} />
            <span>
              <strong>New destination table</strong>
              {" — create-new fields; types will CREATE on first write"}
              {destType ? ` with ${destType}-native DDL` : ""}.
              {mappings.filter(
                (m) =>
                  m.createNew
                  && (m.fidelity || "").toLowerCase() === "preserve"
                  && !mappingRequiresRiskAck(m)
                  && !isIntentionalOmit(m),
              ).length >= 3 && (
                <>
                  {" "}
                  <strong>
                    {mappings.filter(
                      (m) =>
                        m.createNew
                        && (m.fidelity || "").toLowerCase() === "preserve"
                        && !mappingRequiresRiskAck(m)
                        && !isIntentionalOmit(m),
                    ).length}{" "}
                    equivalent
                  </strong>
                  {" mappings (lossless type path) — use "}
                  <strong>Approve eligible</strong>
                  {" once; no Risk Contract required."}
                </>
              )}
              {mappings.some((m) => hasCreateNewTypeRisk(m)) && (
                <>
                  {" "}
                  Precision / width / timezone risks are stamped on rows — review amber chips before Validate.
                </>
              )}
            </span>
          </div>
        )}
        {!isDialog && !compact && !destSchemaLoading && destColumnSet.size > 0 && destTableExists === true && (
          <div className="df2-column-review-alert df2-column-review-alert-info" role="status">
            <DtIcon name="check" size={16} />
            <span>
              <strong>Existing destination table</strong>
              {" — matching "}
              {destColumnSet.size}
              {" columns. Full append adds rows; it does not replace the table."}
            </span>
          </div>
        )}
        {!isDialog && !compact && !destSchemaLoading && destColumnSet.size === 0 && destTableExists === true && (
          <div className="df2-column-review-alert df2-column-review-alert-warn" role="status">
            <DtIcon name="alert" size={16} />
            <span>
              <strong>Existing destination table</strong>
              {" — confirmed on the server, but column metadata did not load. Retry Destination/Map before treating this as create-new."}
            </span>
          </div>
        )}
        {!isDialog && !compact && !destSchemaLoading && destColumnSet.size === 0 && destTableExists == null && (
          <div className="df2-column-review-alert df2-column-review-alert-warn" role="status">
            <DtIcon name="alert" size={16} />
            <span>
              {destConnected === false ? (
                <>
                  <strong>Destination connection failed</strong>
                  {` — ${destConnectionError || "Could not reach the destination."} Open Destination and test the connector.`}
                </>
              ) : destConnectionError ? (
                <>
                  <strong>Destination schema could not be loaded</strong>
                  {` — ${destConnectionError} Retry Destination/Map or choose a different table/schema.`}
                </>
              ) : (
                <>
                  <strong>Destination schema unknown</strong>
                  {" — existence not confirmed. Retry Destination/Map; Datawrap will not invent create-new fields yet."}
                </>
              )}
            </span>
          </div>
        )}


      </div>

      {useSafeBands && (collapseSafe || collapseReady || safeBandExpanded || readyBandExpanded) && (
        <div className="df2-map-band-bar" role="region" aria-label="Map safe-band groups">
          {mapBands.attention.length > 0 && (
            <span className="df2-map-band-chip is-attention">
              {mapBandLabel("attention", mapBands.attention.length)}
            </span>
          )}
          {mapBands.safe.length > 0 && (
            <div className="df2-map-band-actions">
              <button
                type="button"
                className={`df2-map-band-chip is-safe${collapseSafe ? " is-collapsed" : ""}`}
                onClick={() => setSafeBandExpanded((v) => !v)}
                aria-expanded={!collapseSafe}
                title="Preserve / safe-normalize rows — collapse so Issues stay primary"
              >
                <DtIcon name={collapseSafe ? "chevron-right" : "chevron-down"} size={14} />
                {mapBandLabel("safe", mapBands.safe.length)}
                {collapseSafe ? " · Expand" : " · Collapse"}
              </button>
              {mapBands.safe.length > 0 && (
                <button
                  type="button"
                  className="df2-btn df2-btn-sm df2-btn-primary"
                  onClick={approveAllSafe}
                  title="Approve only preserve / safe-normalize rows — never risk or specialty"
                >
                  Approve safe ({mapBands.safe.length})
                </button>
              )}
            </div>
          )}
          {mapBands.ready.length >= 2 && (
            <button
              type="button"
              className={`df2-map-band-chip is-ready${collapseReady ? " is-collapsed" : ""}`}
              onClick={() => setReadyBandExpanded((v) => !v)}
              aria-expanded={!collapseReady}
            >
              <DtIcon name={collapseReady ? "chevron-right" : "chevron-down"} size={14} />
              {mapBandLabel("ready", mapBands.ready.length)}
              {collapseReady ? " · Expand" : " · Collapse"}
            </button>
          )}
        </div>
      )}

      <div className="df2-column-review-table-wrap df2-column-review-scroll">
        <table className="df2-column-review-table df2-column-review-table-sticky">
          <thead>
            <tr>
              <th className="df2-column-th-source" style={{ width: "14%" }}>Source</th>
              <th className="df2-column-th-sample" style={{ width: showTransforms ? "11%" : "12%" }}>Sample</th>
              <th className="df2-column-th-type" style={{ width: "8%" }}>Type</th>
              <th className="df2-column-th-arrow" aria-hidden style={{ width: "4%" }}>→</th>
              <th className="df2-column-th-destination" style={{ width: showTransforms ? "15%" : "18%" }}>Destination</th>
              {showTransforms && <th className="df2-column-th-transform" style={{ width: "11%" }}>Transform</th>}
              <th className="df2-column-th-reason" style={{ width: showTransforms ? "20%" : "23%" }}>Why</th>
              <th className="df2-column-th-confidence" style={{ width: showTransforms ? "8%" : "10%" }}>Confidence</th>
              <th className="df2-column-th-status" style={{ width: showTransforms ? "9%" : "11%" }}>Status</th>
            </tr>
          </thead>
          <tbody>
            {pageItems.map(({ mapping: m, index }) => {
              const omitted = isIntentionalOmit(m);
              const riskOpen = mappingRequiresRiskAck(m) && !m.riskAcknowledged && !omitted;
              const tier = confidenceClass(m.confidence, confidenceThreshold, m.approved, riskOpen);
              const ready = isMappingReady(m, confidenceThreshold);
              return (
                <tr
                  key={`${m.source}-${index}`}
                  className={`df2-column-row ${tier}${omitted ? " is-omitted" : ""}`}
                  ref={(el) => {
                    if (el) rowRefs.current.set(m.source, el);
                    else rowRefs.current.delete(m.source);
                  }}
                  data-source={m.source}
                >
                  <td className="df2-column-source-cell">
                    <div className="df2-column-cell-content">
                      <span className="df2-column-source">{m.source}</span>
                      {omitted && (
                        <span className="df2-badge df2-badge-muted df2-badge-xs" title="Excluded from write — intentional Map policy">
                          omit
                        </span>
                      )}
                      {m.isPii && <span className="df2-badge df2-badge-run df2-badge-xs">PII</span>}
                      {(() => {
                        const engineRisk = engineStampedRiskChip(m);
                        if (!engineRisk || omitted) return null;
                        const riskState = mappingRiskChipState(m);
                        if (riskState === "open") {
                          return (
                            <span
                              className="df2-badge df2-badge-run df2-badge-xs"
                              title={engineRisk.detail}
                            >
                              {engineRisk.label}
                            </span>
                          );
                        }
                        if (riskState === "fail_closed") {
                          const policy = m.riskContract?.execution_policy || "fail-closed";
                          return (
                            <span
                              className="df2-badge df2-badge-run df2-badge-xs"
                              title={`Contract signed with ${policy} — that policy stops the write, so Validate stays blocked. Re-sign with a continue policy to proceed. ${engineRisk.detail}`}
                            >
                              contract · {policy} · blocked
                            </span>
                          );
                        }
                        return (
                          <span
                            className="df2-badge df2-badge-warn df2-badge-xs"
                            title={engineRisk.detail}
                          >
                            risk accepted · {engineRisk.label}
                          </span>
                        );
                      })()}
                      {(() => {
                        if (omitted || m.approved || mappingRequiresRiskAck(m)) return null;
                        const kind = classifyMappingReview(m);
                        if (!kind) return null;
                        const meta = mappingReviewKindMeta(kind);
                        return (
                          <span
                            className="df2-badge df2-badge-run df2-badge-xs"
                            title={meta.detail}
                          >
                            {meta.chip}
                          </span>
                        );
                      })()}
                      {(m.semanticRole === "string_enum" || isEnumToBooleanConflict(m)) && !omitted && (
                        <span className="df2-badge df2-badge-warn df2-badge-xs" title="Status/lifecycle labels — not true/false">
                          string enum
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="df2-column-sample">
                    <div className="df2-column-sample-stack">
                      <span title={m.sample}>
                        {m.sample ? (m.sample.length > 40 ? `${m.sample.slice(0, 40)}…` : m.sample) : "—"}
                      </span>
                      {(() => {
                        const strip = formatColumnProfileStrip(m.columnProfile);
                        if (!strip) return null;
                        return (
                          <span
                            className="df2-column-profile-strip"
                            title="Sample profile from Map engine (null rate · range · observed precision/scale)"
                          >
                            {strip}
                          </span>
                        );
                      })()}
                    </div>
                  </td>
                  <td className={`df2-column-type ${typeBadgeClass(m.inferredType)}`}>
                    <span className="df2-type-badge">{m.inferredType ?? "string"}</span>
                  </td>
                  <td className="df2-column-arrow" aria-hidden>→</td>
                  <td className="df2-column-destination-cell">
                    <div className="df2-column-cell-content">
                      {omitted ? (
                        <span className="df2-column-omit-target" title="Not written to destination">
                          — not transferred —
                        </span>
                      ) : (
                        <>
                      <input
                        className="df2-input df2-column-target-input"
                        value={m.target}
                        ref={(el) => {
                          if (el) destInputRefs.current.set(m.source, el);
                          else destInputRefs.current.delete(m.source);
                        }}
                        onChange={(e) => updateMapping(index, applyOperatorRemapDest(m, e.target.value))}
                        aria-label={`Destination name for ${m.source}`}
                      />
                      <select
                        className="df2-input df2-select df2-column-dest-type-select"
                        value={
                          // An unread destination type must not display the source
                          // type as if it were the destination's.
                          isDestSchemaPending(m) && !m.destType
                            ? ""
                            : normalizeDestTypeValue(m.destType || m.inferredType || "VARCHAR", destType)
                        }
                        onChange={(e) =>
                          updateMapping(index, applyDestTypeChange(m, e.target.value))
                        }
                        aria-label={`Destination type for ${m.source}`}
                        title={
                          m.existsInDestination
                            ? "Existing physical column — changing type here flags ALTER/remap (does not rewrite DDL)"
                            : "Destination logical type"
                        }
                      >
                        {isDestSchemaPending(m) && !m.destType && (
                          <option value="">— destination type not loaded —</option>
                        )}
                        {destTypeSelectOptions(
                          // No destination type was read, so nothing is "current".
                          // Passing the source type here labelled it as the
                          // destination's current type.
                          isDestSchemaPending(m) && !m.destType ? undefined : (m.destType || m.inferredType),
                          destType,
                        ).map((opt) => (
                          <option key={opt.value} value={opt.value}>
                            {opt.label}
                          </option>
                        ))}
                      </select>
                        </>
                      )}
                      {!omitted && (isStructLogicalType(m.inferredType) || isStructLogicalType(m.destType) || (m.structPolicy && !isArrayLogicalType(m.inferredType) && !isArrayLogicalType(m.destType))) && !m.structDerived && (
                        <select
                          className="df2-input df2-select df2-column-struct-policy"
                          value={m.structPolicy ?? "store_as_json"}
                          onChange={(e) =>
                            onChange(applyStructPolicyChange(mappings, index, e.target.value as StructPolicy))
                          }
                          aria-label={`STRUCT policy for ${m.source}`}
                          title={STRUCT_POLICIES.find((p) => p.id === (m.structPolicy ?? "store_as_json"))?.detail}
                        >
                          {STRUCT_POLICIES.map((p) => (
                            <option key={p.id} value={p.id}>{p.label}</option>
                          ))}
                        </select>
                      )}
                      {!omitted && (isArrayLogicalType(m.inferredType) || isArrayLogicalType(m.destType) || m.structPolicy === "explode_rows" || m.structPolicy === "normalize_child_table" || m.structPolicy === "hybrid_json_and_child") && !m.structDerived && (
                        <>
                        <select
                          className="df2-input df2-select df2-column-struct-policy"
                          value={
                            m.structPolicy === "explode_rows"
                            || m.structPolicy === "normalize_child_table"
                            || m.structPolicy === "hybrid_json_and_child"
                              ? m.structPolicy
                              : "store_as_json"
                          }
                          onChange={(e) =>
                            onChange(applyStructPolicyChange(mappings, index, e.target.value as StructPolicy))
                          }
                          aria-label={`ARRAY policy for ${m.source}`}
                          title={
                            ARRAY_POLICIES.find((p) => p.id === (m.structPolicy ?? "store_as_json"))?.detail
                            || (m.structuralClass ? `Detected ${m.structuralClass}` : undefined)
                          }
                        >
                          {ARRAY_POLICIES.map((p) => (
                            <option key={p.id} value={p.id}>{p.label}</option>
                          ))}
                        </select>
                        {m.structuralClass && (
                          <span className="df2-col-badge-struct" title="Sample-aware array shape">
                            {m.structuralClass.replace(/_/g, " ")}
                          </span>
                        )}
                        {(m.structPolicy === "normalize_child_table" || m.structPolicy === "hybrid_json_and_child") && m.childTableSpec && (
                          <span
                            className="df2-col-badge-struct"
                            title={`Child ${m.childTableSpec.child_table}: ${(m.childTableSpec.columns || []).map((c) => c.name).join(", ")}`}
                          >
                            → {m.childTableSpec.child_table}
                          </span>
                        )}
                        </>
                      )}
                      {!omitted && (
                      <div className="df2-column-dest-badges">
                        {m.existsInDestination === true && (
                          <span className="df2-col-badge-exists">exists</span>
                        )}
                        {m.existsInDestination === false && destColumnSet.size > 0 && (
                          <span className="df2-col-badge-new">new</span>
                        )}
                        {(isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType)) && (
                          <span
                            className="df2-col-badge-specialty"
                            title="VECTOR / INTERVAL / GEOGRAPHY travel as identity — dimensions/SRID are not rewritten"
                          >
                            identity
                          </span>
                        )}
                        {(m.structPolicy === "flatten_top_level_keys" || m.structPolicy === "flatten_deep") && !m.structDerived && (
                          <span className="df2-col-badge-struct" title={m.structPolicy === "flatten_deep" ? "Deep flatten (depth≤2)" : "Top-level keys promoted; nested objects stay on parent blob"}>
                            {m.structPolicy === "flatten_deep" ? "deep flatten" : "flatten"}
                          </span>
                        )}
                        {m.structPolicy === "explode_rows" && !m.structDerived && (
                          <span className="df2-col-badge-struct" title="Array row explode (capped)">
                            explode
                          </span>
                        )}
                        {m.structPolicy === "hybrid_json_and_child" && !m.structDerived && (
                          <span className="df2-col-badge-struct" title="Parent JSON + child table">
                            hybrid
                          </span>
                        )}
                        {m.structPolicy === "normalize_child_table" && !m.structDerived && (
                          <span className="df2-col-badge-struct" title="Normalized child table">
                            normalize
                          </span>
                        )}
                        {m.structDerived && m.structParent && (
                          <span className="df2-col-badge-struct" title={`Promoted from ${m.structParent} flatten`}>
                            from {m.structParent}
                          </span>
                        )}
                      </div>
                      )}
                      {!omitted && m.flattenCollisions && m.flattenCollisions.length > 0 && (
                        <div className="df2-flatten-collision" role="note">
                          <p className="df2-label-hint">Flatten collisions (kept as JSON path owners)</p>
                          <table className="df2-flatten-collision-table">
                            <thead>
                              <tr><th>Flat name</th><th>Paths</th></tr>
                            </thead>
                            <tbody>
                              {m.flattenCollisions.map((c) => (
                                <tr key={c.flat}>
                                  <td className="df2-mono">{c.flat}</td>
                                  <td className="df2-mono">{c.paths.map((p) => p.join(".")).join(" · ")}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                          <button
                            type="button"
                            className="df2-btn df2-btn-sm"
                            onClick={() =>
                              onChange(applyStructPolicyChange(mappings, index, "store_as_json"))
                            }
                          >
                            Keep as JSON blob
                          </button>
                        </div>
                      )}
                      {!omitted && isFalseFriendReview(m) && !m.approved && (
                        <div className="df2-column-false-friend" role="note">
                          <p className="df2-label-hint">
                            {mappingReviewKindMeta(classifyMappingReview(m) || "generic").detail}
                          </p>
                          <button
                            type="button"
                            className="df2-btn df2-btn-primary df2-btn-sm"
                            onClick={() => focusDestInput(m.source)}
                          >
                            {mappingReviewKindMeta(classifyMappingReview(m) || "generic").primaryLabel}
                          </button>
                        </div>
                      )}
                      {!omitted && isExistingEnumBooleanConflict(m) && (
                        <button
                          type="button"
                          className="df2-btn df2-btn-sm df2-btn-ghost"
                          title="Existing column is BOOLEAN — remap to a VARCHAR field or ALTER the destination; mapping Widen cannot change DDL"
                          onClick={() => updateMapping(index, flagExistingEnumBooleanConflict(m))}
                        >
                          Remap / ALTER required
                        </button>
                      )}
                      {!omitted && !isExistingEnumBooleanConflict(m) && isExistingDestTypeOverride(m) && (
                        <button
                          type="button"
                          className="df2-btn df2-btn-sm df2-btn-ghost"
                          title={`Withdraw the ALTER request and keep the physical type ${m.destType || "as-is"} — remaining fidelity loss still needs a Risk Contract`}
                          onClick={() => updateMapping(index, clearExistingDestTypeOverride(m))}
                        >
                          Keep {m.destType || "physical type"}
                        </button>
                      )}
                      {!omitted && isEnumToBooleanConflict(m) && canWidenMapping(m) && (
                        <button
                          type="button"
                          className="df2-btn df2-btn-sm df2-btn-ghost"
                          title="Use VARCHAR on the new destination column instead of BOOLEAN"
                          onClick={() =>
                            updateMapping(index, {
                              ...widenMappingToVarchar(m),
                              approved: false,
                            })
                          }
                        >
                          Widen → VARCHAR
                        </button>
                      )}
                    </div>
                  </td>
                  {showTransforms && (
                    <td className="df2-column-transform-cell">
                      <div className="df2-column-cell-content">
                        <select
                          className="df2-input df2-select df2-column-transform"
                          value={m.transform ?? "none"}
                          onChange={(e) =>
                            updateMapping(index, applyTransformChange(m, e.target.value as MappingTransform))
                          }
                          aria-label={`Transform for ${m.source}`}
                          title={MAPPING_TRANSFORMS.find((t) => t.id === (m.transform ?? "none"))?.detail}
                        >
                          {MAPPING_TRANSFORMS.map((t) => (
                            <option key={t.id} value={t.id}>{t.label}</option>
                          ))}
                        </select>
                        {m.transform === "assume_timezone" && !omitted && (
                          <input
                            className="df2-input df2-input-sm df2-column-zone"
                            value={declaredSourceZone(m)}
                            placeholder="IANA zone, e.g. Europe/Berlin"
                            list="df2-iana-zones"
                            aria-label={`Source time zone for ${m.source}`}
                            title="The zone this zoneless column was recorded in — the destination instant is asserted from it, never guessed"
                            onChange={(e) =>
                              updateMapping(index, applyDeclaredSourceZone(m, e.target.value))
                            }
                          />
                        )}
                        {assumeTimezoneAwaitingZone(m) && !omitted && (
                          <span className="df2-col-badge-warn" title="No zone named yet — Validate stays blocked">
                            zone required
                          </span>
                        )}
                        {pipelineTransformChip(m.engineTransform) && !omitted && (
                          <span
                            className="df2-col-badge-pipeline"
                            title={`Pipeline semantic transform '${pipelineTransformChip(m.engineTransform)}' — preserved on Validate/Execute unless you change the transform select`}
                          >
                            pipeline: {pipelineTransformChip(m.engineTransform)}
                          </span>
                        )}
                      </div>
                    </td>
                  )}
                  <td className="df2-column-reason" title={m.reason}>
                    {m.reason || "Semantic match"}
                  </td>
                  <td className="df2-column-confidence">
                    <span className={`df2-column-conf ${tier}`}>{omitted ? "—" : `${(m.confidence * 100).toFixed(0)}%`}</span>
                  </td>
                  <td className="df2-column-status">
                    {omitted ? (
                      <span className="df2-badge df2-badge-muted df2-badge-xs">Omitted</span>
                    ) : ready ? (
                      <div className="df2-column-contract-done">
                        <span className="df2-badge df2-badge-live df2-badge-xs">
                          {mappingAckDoneLabel(m)}
                        </span>
                        {m.riskContract?.risk_id && (
                          <span
                            className="df2-column-risk-id"
                            title={[
                              m.riskContract.signature
                                ? `sig ${String(m.riskContract.signature).slice(0, 24)}…`
                                : "unsigned draft — Validate will sign",
                              m.riskContract.loss_classification
                                ? `loss=${m.riskContract.loss_classification}`
                                : "",
                              m.riskContract.approved_by
                                ? `by ${m.riskContract.approved_by}`
                                : "",
                            ].filter(Boolean).join(" · ")}
                          >
                            {m.riskContract.risk_id}
                          </span>
                        )}
                      </div>
                    ) : isDestSchemaPending(m) ? (
                      // No row-level action exists: the destination type was never
                      // read, so Approve would claim a comparison nobody made.
                      <div className="df2-column-risk-actions">
                        <span
                          className="df2-badge df2-badge-warn df2-badge-xs"
                          title={
                            "Destination column type has not been read from the destination. "
                            + "Use Reload destination schema above — if the table does not exist, "
                            + "the probe proves it absent and this column becomes a CREATE."
                          }
                        >
                          dest type not loaded
                        </span>
                      </div>
                    ) : (
                      <div className="df2-column-risk-actions">
                        {mappingRequiresRiskAck(m) && (
                          <select
                            className="df2-input df2-select df2-select-xs"
                            value={policyBySource[m.source] || ""}
                            onChange={(e) => {
                              const v = e.target.value as ExecutionPolicy | "";
                              setPolicyBySource((prev) => ({ ...prev, [m.source]: v }));
                            }}
                            aria-label={`Execution policy for ${m.source}`}
                            title="Required — no hidden default"
                          >
                            <option value="">Policy…</option>
                            {EXECUTION_POLICY_OPTIONS.map((p) => (
                              <option key={p.id} value={p.id}>
                                {p.continueUnlock ? p.label : `${p.label} — keeps Validate blocked`}
                              </option>
                            ))}
                          </select>
                        )}
                        <button
                          type="button"
                          className={`df2-btn df2-btn-sm${mappingAckTier(m) === "accept_risk" ? " df2-btn-danger" : ""}`}
                          onClick={() => approveOne(index)}
                          disabled={
                            mappingRequiresRiskAck(m) && !policyBySource[m.source]
                          }
                          title={
                            isFalseFriendReview(m)
                              ? mappingReviewKindMeta(classifyMappingReview(m) || "generic").detail
                              : mappingRequiresRiskAck(m)
                              ? (!policyBySource[m.source]
                                ? "Choose an execution policy first — no hidden defaults"
                                : createNewRiskDetail(m)
                                  || m.fidelityReason
                                  || (mappingAckTier(m) === "review"
                                    ? "Review this conversion before Execute"
                                    : "Sign Migration Risk Contract for this column"))
                              : undefined
                          }
                        >
                          {isFalseFriendReview(m)
                            ? mappingReviewKindMeta(classifyMappingReview(m) || "generic").confirmLabel
                            : mappingAckLabel(m)}
                        </button>
                      </div>
                    )}
                  </td>
                </tr>
              );
            })}
            {pageItems.length === 0 && (
              <tr>
                <td colSpan={showTransforms ? 9 : 8} className="df2-column-review-empty-row">
                  {useSafeBands && collapseSafe && filtered.length > 0
                    ? "Safe mappings are collapsed — expand the safe band above, or use Issues to focus risk rows."
                    : "No columns match your search or filter. Try clearing filters or broadening your search."}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {(compact || pages > 1) && (
        <div className="df2-column-review-footer df2-column-workbench-pagination">
          {compact && (
            <span>
              {displayItems.length === 0
                ? (useSafeBands && collapseSafe && filtered.length > 0
                  ? "Safe band collapsed"
                  : "No matching columns")
                : `Rows ${pageStart.toLocaleString()}–${pageEnd.toLocaleString()} of ${displayItems.length.toLocaleString()}${
                  useSafeBands && (collapseSafe || collapseReady)
                    ? ` (${filtered.length.toLocaleString()} total)`
                    : ""
                }`}
            </span>
          )}
          {pages > 1 && (
            <div className="df2-column-workbench-pagination df2-column-review-pager" role="navigation" aria-label="Mapping column pages">
              <button
                type="button"
                className="df2-btn df2-btn-sm"
                disabled={page <= 1}
                onClick={() => setPage((p) => Math.max(1, p - 1))}
              >
                ← Previous
              </button>
              <span className="df2-column-workbench-page-label">
                Page {page} of {pages}
              </span>
              <button
                type="button"
                className="df2-btn df2-btn-sm"
                disabled={page >= pages}
                onClick={() => setPage((p) => Math.min(pages, p + 1))}
              >
                Next →
              </button>
            </div>
          )}
        </div>
      )}
      </div>
    </div>
  );
}
