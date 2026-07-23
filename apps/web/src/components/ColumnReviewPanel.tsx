import { DtIcon } from "./DtIcon";
import { FilterTabs } from "./ui/FilterTabs";
import { StructurePreview } from "./ui/StructurePreview";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  ARRAY_POLICIES,
  MAPPING_TRANSFORMS,
  STRUCT_POLICIES,
  applyDestTypeChange,
  applyStructPolicyChange,
  applyTransformChange,
  approveMappingHonestly,
  approveMappingsHonestly,
  canWidenMapping,
  countApproveEligible,
  flagExistingEnumBooleanConflict,
  isArrayLogicalType,
  isEnumToBooleanConflict,
  isExistingEnumBooleanConflict,
  isSpecialtyLogicalType,
  isStructLogicalType,
  pipelineTransformChip,
  widenMappingToVarchar,
  type EditableMapping,
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
}

function confidenceClass(c: number, threshold: number, approved: boolean): string {
  if (approved) return "ok";
  if (c >= threshold) return "ok";
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
  showTransforms = true,
  hideTitle = false,
  presentation = "default",
  focusSource = null,
  onFocusHandled,
  search: searchProp,
  onSearchChange,
  filter: filterProp,
  onFilterChange,
}: ColumnReviewPanelProps) {
  const [internalSearch, setInternalSearch] = useState("");
  const [internalFilter, setInternalFilter] = useState<ColumnFilter>("all");
  const [sort, setSort] = useState<ColumnSort>("confidence-asc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState<ColumnPageSize>(50);
  const [previewPage, setPreviewPage] = useState(1);
  const [previewPageSize, setPreviewPageSize] = useState(12);
  const rowRefs = useRef<Map<string, HTMLTableRowElement>>(new Map());

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

  const filtered = useMemo(
    () => filterMappings(mappings, { search, filter, sort, threshold: confidenceThreshold }),
    [mappings, search, filter, sort, confidenceThreshold],
  );

  const pages = totalPages(filtered.length, pageSize);
  const pageItems = useMemo(
    () => paginateMappings(filtered, page, pageSize),
    [filtered, page, pageSize],
  );

  const needsReview = mappings.filter((m) => needsMappingReview(m, confidenceThreshold));
  const approvedCount = mappings.filter((m) => isMappingReady(m, confidenceThreshold)).length;
  const avgConfidence = mappings.length
    ? mappings.reduce((s, m) => s + m.confidence, 0) / mappings.length
    : 0;

  useEffect(() => {
    setPage(1);
  }, [search, filter, sort, pageSize, mappings.length]);

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

  const updateMapping = (index: number, patch: Partial<EditableMapping>) => {
    const next = mappings.map((m, i) => (i === index ? { ...m, ...patch } : m));
    onChange(next);
  };

  const approveAll = () => {
    onChange(approveMappingsHonestly(mappings));
  };

  const approveOne = (index: number) => {
    const m = mappings[index];
    if (!m) return;
    updateMapping(index, approveMappingHonestly(m));
  };

  const eligibleApproveCount = countApproveEligible(mappings);

  const focusIssues = () => {
    setFilter("review");
    setSort("confidence-asc");
    setSearch("");
    setPage(1);
  };

  const pageStart = filtered.length === 0 ? 0 : (page - 1) * pageSize + 1;
  const pageEnd = Math.min(page * pageSize, filtered.length);

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
        compact && sampleRows && sampleRows.length > 0 ? "is-split" : "",
        isDialog ? "is-dialog" : "",
      ].filter(Boolean).join(" ")}
    >
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
                className="df2-column-workbench-clear"
                onClick={() => setSearch("")}
                aria-label="Clear search"
              >
                <DtIcon name="x" size={14} />
              </button>
            )}
          </div>
          <div className="df2-column-workbench-actions">
            {tableControls}
            {filterCounts.review > 0 && (
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

        {needsReview.length > 0 && filter === "review" && !compact && (
          <div className="df2-column-review-alert" role="status">
            <DtIcon name="alert" size={16} />
            <span>
              <strong>{needsReview.length} column(s)</strong> need review before transfer.
            </span>
          </div>
        )}

        {!isDialog && !destSchemaLoading && destColumnSet.size === 0 && destTableExists === false && (
          <div className="df2-column-review-alert df2-column-review-alert-info" role="status">
            <DtIcon name="sparkle" size={16} />
            <span>
              <strong>New destination table</strong>
              {" — create-new fields; types will CREATE on first write"}
              {destType ? ` with ${destType}-native DDL` : ""}.
            </span>
          </div>
        )}
        {!isDialog && !destSchemaLoading && destColumnSet.size === 0 && destTableExists === true && (
          <div className="df2-column-review-alert df2-column-review-alert-warn" role="status">
            <DtIcon name="alert" size={16} />
            <span>
              <strong>Existing destination table</strong>
              {" — confirmed on the server, but column metadata did not load. Retry Destination/Map before treating this as create-new."}
            </span>
          </div>
        )}
        {!isDialog && !destSchemaLoading && destColumnSet.size === 0 && destTableExists == null && (
          <div className="df2-column-review-alert df2-column-review-alert-warn" role="status">
            <DtIcon name="alert" size={16} />
            <span>
              <strong>Destination schema unknown</strong>
              {" — existence not confirmed. Retry Destination/Map; DataFlow will not invent create-new fields yet."}
            </span>
          </div>
        )}


      </div>

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
              const tier = confidenceClass(m.confidence, confidenceThreshold, m.approved);
              const ready = isMappingReady(m, confidenceThreshold);
              return (
                <tr
                  key={`${m.source}-${index}`}
                  className={`df2-column-row ${tier}`}
                  ref={(el) => {
                    if (el) rowRefs.current.set(m.source, el);
                    else rowRefs.current.delete(m.source);
                  }}
                  data-source={m.source}
                >
                  <td className="df2-column-source-cell">
                    <div className="df2-column-cell-content">
                      <span className="df2-column-source">{m.source}</span>
                      {m.isPii && <span className="df2-badge df2-badge-run df2-badge-xs">PII</span>}
                      {m.requiresReview && !m.approved && (
                        <span className="df2-badge df2-badge-run df2-badge-xs">ambiguous</span>
                      )}
                      {(m.semanticRole === "string_enum" || isEnumToBooleanConflict(m)) && (
                        <span className="df2-badge df2-badge-warn df2-badge-xs" title="Status/lifecycle labels — not true/false">
                          string enum
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="df2-column-sample" title={m.sample}>
                    {m.sample ? (m.sample.length > 40 ? `${m.sample.slice(0, 40)}…` : m.sample) : "—"}
                  </td>
                  <td className={`df2-column-type ${typeBadgeClass(m.inferredType)}`}>
                    <span className="df2-type-badge">{m.inferredType ?? "string"}</span>
                  </td>
                  <td className="df2-column-arrow" aria-hidden>→</td>
                  <td className="df2-column-destination-cell">
                    <div className="df2-column-cell-content">
                      <input
                        className="df2-input df2-column-target-input"
                        value={m.target}
                        onChange={(e) => updateMapping(index, { target: e.target.value, approved: false })}
                        aria-label={`Destination name for ${m.source}`}
                      />
                      <select
                        className="df2-input df2-select df2-column-dest-type-select"
                        value={normalizeDestTypeValue(m.destType || m.inferredType || "VARCHAR", destType)}
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
                        {destTypeSelectOptions(m.destType || m.inferredType, destType).map((opt) => (
                          <option key={opt.value} value={opt.value}>
                            {opt.label}
                          </option>
                        ))}
                      </select>
                      {(isStructLogicalType(m.inferredType) || isStructLogicalType(m.destType) || (m.structPolicy && !isArrayLogicalType(m.inferredType) && !isArrayLogicalType(m.destType))) && !m.structDerived && (
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
                      {(isArrayLogicalType(m.inferredType) || isArrayLogicalType(m.destType) || m.structPolicy === "explode_rows") && !m.structDerived && (
                        <select
                          className="df2-input df2-select df2-column-struct-policy"
                          value={m.structPolicy === "explode_rows" ? "explode_rows" : "store_as_json"}
                          onChange={(e) =>
                            onChange(applyStructPolicyChange(mappings, index, e.target.value as StructPolicy))
                          }
                          aria-label={`ARRAY policy for ${m.source}`}
                          title={ARRAY_POLICIES.find((p) => p.id === (m.structPolicy === "explode_rows" ? "explode_rows" : "store_as_json"))?.detail}
                        >
                          {ARRAY_POLICIES.map((p) => (
                            <option key={p.id} value={p.id}>{p.label}</option>
                          ))}
                        </select>
                      )}
                      <div className="df2-column-dest-badges">
                        {m.existsInDestination && (
                          <span className="df2-col-badge-exists">exists</span>
                        )}
                        {!m.existsInDestination && destColumnSet.size > 0 && (
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
                        {m.structDerived && m.structParent && (
                          <span className="df2-col-badge-struct" title={`Promoted from ${m.structParent} flatten`}>
                            from {m.structParent}
                          </span>
                        )}
                      </div>
                      {isExistingEnumBooleanConflict(m) && (
                        <button
                          type="button"
                          className="df2-btn df2-btn-sm df2-btn-ghost"
                          title="Existing column is BOOLEAN — remap to a VARCHAR field or ALTER the destination; mapping Widen cannot change DDL"
                          onClick={() => updateMapping(index, flagExistingEnumBooleanConflict(m))}
                        >
                          Remap / ALTER required
                        </button>
                      )}
                      {isEnumToBooleanConflict(m) && canWidenMapping(m) && (
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
                        {pipelineTransformChip(m.engineTransform) && (
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
                    <span className={`df2-column-conf ${tier}`}>{(m.confidence * 100).toFixed(0)}%</span>
                  </td>
                  <td className="df2-column-status">
                    {ready ? (
                      <span className="df2-badge df2-badge-live df2-badge-xs">Ready</span>
                    ) : (
                      <button
                        type="button"
                        className="df2-btn df2-btn-sm"
                        onClick={() => approveOne(index)}
                      >
                        Approve
                      </button>
                    )}
                  </td>
                </tr>
              );
            })}
            {pageItems.length === 0 && (
              <tr>
                <td colSpan={showTransforms ? 9 : 8} className="df2-column-review-empty-row">
                  No columns match your search or filter. Try clearing filters or broadening your search.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {(compact || filtered.length > pageSize) && (
        <div className="df2-column-review-footer df2-column-workbench-pagination">
          {compact && (
            <span>
              {filtered.length === 0
                ? "No matching columns"
                : `Rows ${pageStart.toLocaleString()}–${pageEnd.toLocaleString()} of ${filtered.length.toLocaleString()}`}
            </span>
          )}
          {filtered.length > pageSize && (
            <div className="df2-column-workbench-pagination">
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
