import { useEffect, useMemo, useState } from "react";
import { ColumnReviewPanel } from "../../components/ColumnReviewPanel";
import {
  MappingProofDrawer,
  mergeMappingProof,
  type MappingProof,
} from "../../components/MappingProofDrawer";
import { Dialog } from "../../components/ui/Dialog";
import { Button } from "../../components/ui/Button";
import { DtIcon } from "../../components/DtIcon";
import type { ColumnFilter } from "../../lib/columnWorkbench";
import { countByFilter, needsMappingReview } from "../../lib/columnWorkbench";
import type { EditableMapping } from "../../lib/mapping";
import { mappingHealthSummary } from "../../lib/mapping";
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
}: TransferMapStepProps) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<ColumnFilter>("review");
  const [userPickedFilter, setUserPickedFilter] = useState(false);
  const [focusSource, setFocusSource] = useState<string | null>(null);
  const [mapDialogOpen, setMapDialogOpen] = useState(false);
  const [proofOpenLocal, setProofOpenLocal] = useState(false);
  const proofOpen = proofOpenProp ?? proofOpenLocal;
  const setProofOpen = onProofOpenChange ?? setProofOpenLocal;


  const handleFilterChange = (next: ColumnFilter) => {
    setUserPickedFilter(true);
    setFilter(next);
  };

  useEffect(() => {
    // Skip while an identity/focus dialog is open so we don't fight setFilter("all").
    if (userPickedFilter || focusSource) return;
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

  const destDisplayType = destKindMode === "database" ? destType : "file";

  const filterCounts = useMemo(
    () => countByFilter(columnMappings, confidenceThreshold),
    [columnMappings, confidenceThreshold],
  );

  const approvedCount = filterCounts.ready;

  const health = useMemo(
    () => mappingHealthSummary(columnMappings, confidenceThreshold),
    [columnMappings, confidenceThreshold],
  );

  /** Prefer API proof; refresh pair list from live edits so operators see current transforms. */
  const effectiveProof = useMemo(
    () => mergeMappingProof(mappingProof, columnMappings, {
      destColumns,
      destType: destDisplayType,
      destTableExists: destKindMode === "database" ? destTableExists : false,
    }),
    [mappingProof, columnMappings, destColumns, destDisplayType, destKindMode, destTableExists],
  );

  const identityFooterLabel = requiresPrimaryKey && !primaryKeyField
    ? "PK required"
    : requiresPrimaryKey && primaryKeyField
      ? `PK ${primaryKeyField}`
      : `${syncModeLabel || "sync"}`;

  const mappingFooterLabel = mappingReviewCount > 0
    ? `${mappingReviewCount} need review`
    : `${approvedCount} ready`;

  return (
    <div className="df2-transfer-step-panel df2-map-step-panel">
      <div
        className="df2-card-head df2-map-step-head"
        title={`${columnMappings.length} mappings · ${approvedCount} ready${mappingReviewCount > 0 ? ` · ${mappingReviewCount} need review` : ""}`}
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
        <div className="df2-map-step-workspace is-full-editor">
          <div className="df2-map-editor-pane">
            <div className="df2-map-editor-scroll-host">
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
      </div>

      <div className="df2-card-footer df2-wizard-footer df2-map-footer">
        <button type="button" className="df2-btn" onClick={onBack}>← Back</button>
        <div className="df2-map-footer-status" aria-live="polite">
          <span className={mappingReviewCount > 0 ? "is-warn" : "is-ok"} title={health.weak ? health.detail : undefined}>
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
            {uniqueKeySuggestions.length > 0 && requiresPrimaryKey && !primaryKeyField && (
              <>
                {" · Try "}
                {uniqueKeySuggestions.slice(0, 2).map((s, i) => (
                  <button
                    key={s.column}
                    type="button"
                    className="df2-map-footer-pk-suggest"
                    title={`Unique in ${s.sampleRows}-row sample`}
                    onClick={() => {
                      onApplyPrimaryKey?.(s.column);
                      onOpenIdentitySettings?.();
                    }}
                  >
                    {s.column}{i === 0 && uniqueKeySuggestions.length > 1 ? "," : ""}
                  </button>
                ))}
              </>
            )}

            {compositeKeySuggestions.length > 0 && requiresPrimaryKey && !primaryKeyField && (
              <span className="df2-map-pk-suggest">
                Composite sample keys:{" "}
                {compositeKeySuggestions.slice(0, 2).map((s, i) => (
                  <button
                    key={s.columns.join(",")}
                    type="button"
                    className="df2-linkish"
                    onClick={() => onApplyPrimaryKey?.(s.columns.join(","))}
                    title={`Unique in ${s.sampleRows}-row sample`}
                  >
                    {s.columns.join(" + ")}{i === 0 && compositeKeySuggestions.length > 1 ? "," : ""}
                  </button>
                ))}
              </span>
            )}

          </span>
        </div>
        <div className="df2-map-footer-actions">
          {onOpenIdentitySettings && (
            <button
              type="button"
              className={`df2-btn${requiresPrimaryKey && !primaryKeyField ? " df2-btn-secondary" : ""}`}
              onClick={onOpenIdentitySettings}
              title="Open Advanced settings — primary key, sync mode, cursor (same drawer as Destination)"
            >
              <DtIcon name="settings" size={14} /> Advanced
            </button>
          )}
          <button
            type="button"
            className="df2-btn df2-btn-primary"
            onClick={onContinue}
            disabled={mappingReviewCount > 0}
            title={
              mappingReviewCount > 0
                ? `${mappingReviewCount} column(s) need Approve or Accept risk before Validate`
                : "Continue to Validate"
            }
          >
            Continue to Validate →
          </button>
        </div>
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
          onChange={onChangeMappings}
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
