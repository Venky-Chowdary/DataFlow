import { useEffect, useMemo, useState } from "react";
import { ColumnReviewPanel } from "../../components/ColumnReviewPanel";
import { MappingIntelligencePanel } from "../../components/MappingIntelligencePanel";
import {
  MappingProofDrawer,
  mergeMappingProof,
  type MappingProof,
} from "../../components/MappingProofDrawer";
import { Dialog } from "../../components/ui/Dialog";
import { DtIcon } from "../../components/DtIcon";
import type { ColumnFilter } from "../../lib/columnWorkbench";
import { countByFilter, filterMappings } from "../../lib/columnWorkbench";
import type { EditableMapping } from "../../lib/mapping";
import { mappingHealthSummary } from "../../lib/mapping";

interface TransferMapStepProps {
  columnMappings: EditableMapping[];
  analysis: import("../../lib/types").EnhancedAnalysis | null;
  destColumns: string[];
  destSchemaLoading: boolean;
  /** null = unknown, true = confirmed on destination, false = will CREATE. */
  destTableExists?: boolean | null;
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
}

const INTELLIGENCE_PAIR_LIMIT = 500;

const MAP_STEP_SCROLL_CLASS = "is-map-step-view";

export function TransferMapStep({
  columnMappings,
  analysis,
  destColumns,
  destSchemaLoading,
  destTableExists = null,
  targetCollection,
  targetDatabase,
  destKindMode,
  destType,
  sourceLabel,
  sourceSubtitle,
  sourceType,
  destRouteLabel,
  destRouteSubtitle,
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
}: TransferMapStepProps) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<ColumnFilter>("all");
  const [focusSource, setFocusSource] = useState<string | null>(null);
  const [mapDialogOpen, setMapDialogOpen] = useState(false);
  const [proofOpenLocal, setProofOpenLocal] = useState(false);
  const proofOpen = proofOpenProp ?? proofOpenLocal;
  const setProofOpen = onProofOpenChange ?? setProofOpenLocal;

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
    setFilter("all");
    setMapDialogOpen(true);
    onIdentityFixConsumed?.();
  }, [initialFocusSource]);

  const destDisplayType = destKindMode === "database" ? destType : "file";
  const destPaneSubtitle = destKindMode === "database"
    ? destSchemaLoading
      ? "Loading existing schema from connector…"
      : destColumns.length > 0
        ? `${destColumns.length} existing fields in ${targetDatabase}.${targetCollection}`
        : destTableExists === true
          ? `Existing table ${targetDatabase}.${targetCollection} — column metadata pending`
          : `New fields in ${targetDatabase}.${targetCollection}`
    : destRouteSubtitle;

  const filterCounts = useMemo(
    () => countByFilter(columnMappings, confidenceThreshold),
    [columnMappings, confidenceThreshold],
  );

  const approvedCount = filterCounts.ready;

  const health = useMemo(
    () => mappingHealthSummary(columnMappings, confidenceThreshold),
    [columnMappings, confidenceThreshold],
  );

  const filteredForVisual = useMemo(
    () => filterMappings(columnMappings, {
      search,
      filter,
      sort: "confidence-asc",
      threshold: confidenceThreshold,
    }),
    [columnMappings, search, filter, confidenceThreshold],
  );

  const visualItems = filteredForVisual.slice(0, INTELLIGENCE_PAIR_LIMIT);

  /** Prefer API proof; refresh pair list from live edits so operators see current transforms. */
  const effectiveProof = useMemo(
    () => mergeMappingProof(mappingProof, columnMappings, {
      destColumns,
      destType: destDisplayType,
    }),
    [mappingProof, columnMappings, destColumns, destDisplayType],
  );

  const jumpToSource = (source: string) => {
    setSearch(source);
    setFilter("all");
    setFocusSource(source);
  };

  const filterAttention = (kind: "review" | "block" | "pii" | "warn") => {
    const map: Record<string, ColumnFilter> = {
      review: "review",
      block: "block",
      pii: "pii",
      warn: "warn",
    };
    setFilter(map[kind]);
    setSearch("");
    setFocusSource(null);
  };

  return (
    <div className="df2-transfer-step-panel df2-map-step-panel">
      <div className="df2-card-head df2-map-step-head">
        <div>
          <h3 className="df2-card-title">Map columns</h3>
          <p className="df2-card-sub">
            {columnMappings.length} mappings · {approvedCount} ready
            {mappingReviewCount > 0 ? ` · ${mappingReviewCount} need review` : ""}
            {llmUsed ? " · semantic engine" : ""}
            {destColumns.length === 0 && !destSchemaLoading && destTableExists !== true
              ? " · create-new table"
              : destColumns.length === 0 && destTableExists === true
                ? " · existing table (columns pending)"
                : destColumns.length > 0
                  ? ` · ${destColumns.length} dest columns`
                  : ""}
            {streamNames.length > 1 ? ` · ${streamNames.length} streams` : ""}
          </p>
        </div>
        <div className="df2-map-step-head-actions">
          {(effectiveProof.summary?.cdc_detected || (effectiveProof.sync_mode || "").toLowerCase().includes("cdc")) && (
            <span className="df2-badge df2-badge-info df2-badge-xs" title="Change-stream / CDC route — at-least-once upsert by default">
              CDC · at-least-once
            </span>
          )}
          {destDisplayType && (
            <span className="df2-badge df2-badge-muted df2-badge-xs" title="Destination DDL family used for type pickers and native types">
              {destDisplayType}
            </span>
          )}
          <button
            type="button"
            className="df2-btn df2-btn-sm"
            onClick={() => setProofOpen(true)}
            title="Inspect how this map works — confidence evidence, transforms, fidelity risks"
          >
            <DtIcon name="sparkle" size={14} /> Mapping proof
          </button>
          <button
            type="button"
            className="df2-btn df2-btn-sm df2-btn-ghost"
            onClick={() => setMapDialogOpen(true)}
            title="Open full mapping table in a dialog"
          >
            <DtIcon name="expand" size={14} /> Expand mapping table
          </button>
        </div>
      </div>

      {health.weak && (
        <div
          className={`df2-map-health-banner${health.total === 0 || health.unmappedTarget > 0 || health.existingTypeConflict > 0 ? " is-critical" : " is-warn"}`}
          role="status"
        >
          <DtIcon name="alert" size={16} />
          <div>
            <strong>{health.headline}</strong>
            <p>{health.detail}</p>
            {health.specialtyIdentity > 0 && health.existingTypeConflict === 0 && health.unmappedTarget === 0 && (
              <p className="df2-map-health-note">
                Specialty types use identity transforms — Validate will still fail-closed on VECTOR dim mismatch.
              </p>
            )}
          </div>
        </div>
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

      <div className="df2-card-body df2-map-step-body">
        {identityFixBanner && (
          <div className="df2-map-identity-banner" role="status">
            <DtIcon name="alert" size={16} />
            <div>
              <strong>Identity fix from Validate</strong>
              <p>{identityFixBanner}</p>
              <p className="df2-map-identity-banner-hint">
                Column mapping alone cannot remove duplicate source rows. Change the primary key or
                sync mode in Destination → Advanced settings, or dedupe the source, then re-validate.
              </p>
            </div>
          </div>
        )}
        <div className="df2-map-step-workspace">
          <ColumnReviewPanel
            mappings={columnMappings}
            rowCount={rowCount}
            sampleRows={sampleRows}
            confidenceThreshold={confidenceThreshold}
            onChange={onChangeMappings}
            destinationFields={destColumns}
            destinationLabel={destRouteLabel}
            destType={destDisplayType}
            destSchemaLoading={destSchemaLoading}
            destTableExists={destTableExists}
            compact
            hideTitle
            search={search}
            onSearchChange={setSearch}
            filter={filter}
            onFilterChange={setFilter}
            focusSource={focusSource}
            onFocusHandled={() => setFocusSource(null)}
          />

          <MappingIntelligencePanel
            allMappings={columnMappings}
            items={visualItems}
            sourceLabel={sourceLabel}
            sourceSubtitle={sourceSubtitle}
            sourceType={sourceType}
            destLabel={destRouteLabel}
            destSubtitle={destPaneSubtitle}
            destType={destDisplayType}
            confidenceThreshold={confidenceThreshold}
            totalCount={filteredForVisual.length}
            mappedCount={columnMappings.length}
            llmUsed={llmUsed}
            destSchemaLoading={destSchemaLoading}
            onSelectSource={jumpToSource}
            onFilterAttention={filterAttention}
          />
        </div>
      </div>

      <div className="df2-wizard-footer">
        <button type="button" className="df2-btn" onClick={onBack}>← Back</button>
        <button type="button" className="df2-btn df2-btn-primary" onClick={onContinue}>
          Continue to Validate →
        </button>
      </div>

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
              : `${columnMappings.length} columns · create-new destination — fields CREATE on first write (no existing table required).`
        }
        ariaLabel="Full mapping table"
        className="df2-map-dialog"
        footer={
          <button type="button" className="df2-btn df2-btn-primary" onClick={() => setMapDialogOpen(false)}>
            Done
          </button>
        }
      >
        {destColumns.length === 0 && !destSchemaLoading && destTableExists !== true && (
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
        <ColumnReviewPanel
          mappings={columnMappings}
          rowCount={rowCount}
          confidenceThreshold={confidenceThreshold}
          onChange={onChangeMappings}
          destinationFields={destColumns}
          destinationLabel={destRouteLabel}
          destType={destDisplayType}
          destSchemaLoading={destSchemaLoading}
          destTableExists={destTableExists}
          presentation="dialog"
          search={search}
          onSearchChange={setSearch}
          filter={filter}
          onFilterChange={setFilter}
          focusSource={focusSource}
          onFocusHandled={() => setFocusSource(null)}
        />
      </Dialog>

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
