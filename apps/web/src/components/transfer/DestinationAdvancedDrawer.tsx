import { useEffect } from "react";
import { Drawer } from "../ui/Drawer";
import {
  type AdvancedLocaleKind,
  scrollAdvancedLocaleIntoView,
} from "../../lib/validateHonestyControls";
import { Button } from "../ui/Button";
import { FilterBar } from "../ui/FilterBar";
import { FilterTabs } from "../ui/FilterTabs";
import { DtIcon } from "../DtIcon";
import type { StreamFieldContract } from "../../lib/streamContracts";
import { resolveStreamFields } from "../../lib/streamContracts";
import {
  CURSOR_SEMANTICS,
  CURSOR_SEMANTICS_LABELS,
  evaluateCursorSemantics,
} from "../../lib/cursorSemantics";

export type DestSyncMode =
  | "full_refresh_overwrite"
  | "full_refresh_append"
  | "incremental_append"
  | "incremental_deduped"
  | "cdc"
  | "scd2"
  | "mirror";

export type DestSchemaPolicy =
  | "manual_review"
  | "propagate_columns"
  | "propagate_all"
  | "pause_on_change"
  | "type_locked";

export type DestValidationMode =
  | "balanced"
  | "strict"
  | "maximum"
  | "migration"
  | "discovery"
  | "audit";
export type DestDateLocale = "" | "DMY" | "MDY";
export type DestNumberLocale = "" | "US" | "EU";

export interface SyncModeOption {
  id: DestSyncMode;
  label: string;
  detail: string;
}

export interface SchemaPolicyOption {
  id: DestSchemaPolicy;
  label: string;
  detail: string;
}

export interface ValidationModeOption {
  id: DestValidationMode;
  label: string;
  threshold?: string;
  detail?: string;
}

export interface DateLocaleOption {
  id: DestDateLocale;
  label: string;
  detail: string;
}

export interface NumberLocaleOption {
  id: DestNumberLocale;
  label: string;
  detail: string;
}

interface DestinationAdvancedDrawerProps {
  open: boolean;
  onClose: () => void;
  syncModes: SyncModeOption[];
  schemaPolicies: SchemaPolicyOption[];
  validationModes: ValidationModeOption[];
  dateLocales: DateLocaleOption[];
  numberLocales: NumberLocaleOption[];
  syncMode: DestSyncMode;
  schemaPolicy: DestSchemaPolicy;
  validationMode: DestValidationMode;
  dateLocale: DestDateLocale;
  numberLocale: DestNumberLocale;
  backfillNewFields: boolean;
  /** Stream names (one row each when multi-stream). */
  streamNames: string[];
  /** Per-stream cursor / PK overrides. */
  streamFields: Record<string, StreamFieldContract>;
  /** Shared fallbacks when a stream has no override yet. */
  defaultCursor: string;
  defaultPrimaryKey: string;
  /** Declared cursor meaning shared by streams without an override. */
  defaultCursorSemantics?: string;
  sourceColumns: string[];
  sourceSchema: Record<string, string>;
  /** Per-stream columns when multi-stream schemas diverge. */
  sourceColumnsByStream?: Record<string, string[]>;
  sourceSchemaByStream?: Record<string, Record<string, string>>;
  syncModeLabel: string;
  schemaPolicyLabel: string;
  requiresCursor: boolean;
  requiresPrimaryKey: boolean;
  streamNeedsReview: boolean;
  onSyncModeChange: (mode: DestSyncMode) => void;
  onSchemaPolicyChange: (policy: DestSchemaPolicy) => void;
  onValidationModeChange: (mode: DestValidationMode) => void;
  onDateLocaleChange: (locale: DestDateLocale) => void;
  onNumberLocaleChange: (locale: DestNumberLocale) => void;
  onBackfillChange: (value: boolean) => void;
  onStreamCursorChange: (stream: string, value: string) => void;
  onStreamCursorSemanticsChange: (stream: string, value: string) => void;
  onStreamPrimaryKeyChange: (stream: string, value: string) => void;
  /** Heuristic suggestions for empty cursor / PK selects. */
  suggestedCursor?: string;
  suggestedPrimaryKey?: string;
  /** Sample-unique PK candidates (honest: preview sample only). */
  uniqueKeySuggestions?: Array<{ column: string; sampleRows: number; uniqueCount: number }>;
  /** Sample-unique 2-column composites (comma-joined into primary key field). */
  compositeKeySuggestions?: Array<{ columns: string[]; sampleRows: number; uniqueCount: number }>;
  /** Debezium-compatible snapshot mode (CDC). */
  snapshotMode?: string;
  onSnapshotModeChange?: (mode: string) => void;
  /** Priority-first sync: sort source by this column before write. */
  priorityColumn?: string;
  priorityDirection?: "asc" | "desc";
  /** Soft row cap (0 = no limit). */
  rowLimit?: number;
  onPriorityColumnChange?: (value: string) => void;
  onPriorityDirectionChange?: (value: "asc" | "desc") => void;
  onRowLimitChange?: (value: number) => void;
  /** CDC dest-owned watermark EOS (opt-in). Default at_least_once. */
  deliveryGuarantee?: "at_least_once" | "exactly_once";
  onDeliveryGuaranteeChange?: (value: "at_least_once" | "exactly_once") => void;
  /** True when the destination writer can share one dest transaction. */
  exactlyOnceWired?: boolean;
  /** CDC → append-only dest opt-in (duplicates on redelivery). */
  allowAppendOnly?: boolean;
  onAllowAppendOnlyChange?: (value: boolean) => void;
  /** SQL Server Always On listener: ODBC MultiSubnetFailover=Yes. */
  multiSubnetFailover?: boolean;
  onMultiSubnetFailoverChange?: (value: boolean) => void;
  showMultiSubnetFailover?: boolean;
  /** SQL Server CDC TVF row_filter_option (all / all update old / net). */
  cdcRowFilter?: "all" | "all update old" | "net";
  onCdcRowFilterChange?: (value: "all" | "all update old" | "net") => void;
  showCdcRowFilter?: boolean;
  /** Stage into `{table}_df_staging`, promote only clean rows to primary. */
  writeViaStaging?: boolean;
  onWriteViaStagingChange?: (value: boolean) => void;
  /** False for Mongo/files/SaaS — hide/disable staging toggle so Execute cannot fail after Validate. */
  writeViaStagingSupported?: boolean;
  /** Show vector destination embedding controls (pgvector / Qdrant / Weaviate / Pinecone / Milvus). */
  showVectorOptions?: boolean;
  vectorContentColumn?: string;
  vectorEmbeddingColumn?: string;
  vectorMetadataColumns?: string;
  vectorEmbeddingModel?: string;
  vectorChunkSize?: number;
  vectorChunkOverlap?: number;
  onVectorContentColumnChange?: (value: string) => void;
  onVectorEmbeddingColumnChange?: (value: string) => void;
  onVectorMetadataColumnsChange?: (value: string) => void;
  onVectorEmbeddingModelChange?: (value: string) => void;
  onVectorChunkSizeChange?: (value: number) => void;
  onVectorChunkOverlapChange?: (value: number) => void;
  /** Semantic routing plan (embed / metadata / exclude_pii / skip). */
  vectorRoutingFields?: Array<{
    column: string;
    action: string;
    confidence: number;
    reason: string;
    is_pii?: boolean;
  }>;
  vectorRoutingLoading?: boolean;
  vectorExcludePiiColumns?: string;
  onApplyVectorRouting?: () => void;
  /** Persist embeddings to SQLite across restarts (default on). */
  vectorDurableCache?: boolean;
  onVectorDurableCacheChange?: (value: boolean) => void;
  embeddingCacheStats?: {
    entries?: number;
    models?: number;
    approx_bytes?: number;
    session_hits?: number;
    session_misses?: number;
    hit_rate?: number | null;
    path?: string;
  } | null;
  embeddingCacheBusy?: boolean;
  onRefreshEmbeddingCache?: () => void;
  onClearEmbeddingCache?: () => void;
  /** Validate Set date/number locale — scroll that control into view after open. */
  localeFocus?: AdvancedLocaleKind | null;
}

/**
 * Right-side drawer for sync / schema / stream contract controls.
 * Keeps the Destination step focused on picking a clear destination.
 */
export function DestinationAdvancedDrawer({
  open,
  onClose,
  syncModes,
  schemaPolicies,
  validationModes,
  dateLocales,
  numberLocales,
  syncMode,
  schemaPolicy,
  validationMode,
  dateLocale,
  numberLocale,
  backfillNewFields,
  streamNames,
  streamFields,
  defaultCursor,
  defaultPrimaryKey,
  defaultCursorSemantics = "",
  sourceColumns,
  sourceSchema,
  sourceColumnsByStream = {},
  sourceSchemaByStream = {},
  syncModeLabel,
  schemaPolicyLabel,
  requiresCursor,
  requiresPrimaryKey,
  streamNeedsReview,
  onSyncModeChange,
  onSchemaPolicyChange,
  onValidationModeChange,
  onDateLocaleChange,
  onNumberLocaleChange,
  onBackfillChange,
  onStreamCursorChange,
  onStreamCursorSemanticsChange,
  onStreamPrimaryKeyChange,
  suggestedCursor = "",
  suggestedPrimaryKey = "",
  uniqueKeySuggestions = [],
  compositeKeySuggestions = [],
  snapshotMode = "initial",
  onSnapshotModeChange,
  priorityColumn = "",
  priorityDirection = "desc",
  rowLimit = 0,
  onPriorityColumnChange,
  onPriorityDirectionChange,
  onRowLimitChange,
  deliveryGuarantee = "at_least_once",
  onDeliveryGuaranteeChange,
  exactlyOnceWired = false,
  allowAppendOnly = false,
  onAllowAppendOnlyChange,
  multiSubnetFailover = false,
  onMultiSubnetFailoverChange,
  showMultiSubnetFailover = false,
  cdcRowFilter = "all",
  onCdcRowFilterChange,
  showCdcRowFilter = false,
  writeViaStaging = false,
  onWriteViaStagingChange,
  writeViaStagingSupported = true,
  showVectorOptions = false,
  vectorContentColumn = "",
  vectorEmbeddingColumn = "",
  vectorMetadataColumns = "",
  vectorEmbeddingModel = "",
  vectorChunkSize = 512,
  vectorChunkOverlap = 50,
  onVectorContentColumnChange,
  onVectorEmbeddingColumnChange,
  onVectorMetadataColumnsChange,
  onVectorEmbeddingModelChange,
  onVectorChunkSizeChange,
  onVectorChunkOverlapChange,
  vectorRoutingFields = [],
  vectorRoutingLoading = false,
  vectorExcludePiiColumns = "",
  onApplyVectorRouting,
  vectorDurableCache = true,
  onVectorDurableCacheChange,
  embeddingCacheStats = null,
  embeddingCacheBusy = false,
  onRefreshEmbeddingCache,
  onClearEmbeddingCache,
  localeFocus = null,
}: DestinationAdvancedDrawerProps) {
  const names = streamNames.length > 0 ? streamNames : ["source_stream"];
  const activeMode = syncModes.find((m) => m.id === syncMode);

  useEffect(() => {
    if (!open || !localeFocus) return;
    const timer = window.setTimeout(() => {
      scrollAdvancedLocaleIntoView(localeFocus);
    }, 80);
    return () => window.clearTimeout(timer);
  }, [open, localeFocus]);

  return (
    <Drawer
      open={open}
      onClose={onClose}
      size="lg"
      side="right"
      ariaLabel="Advanced sync and schema settings"
      icon={<DtIcon name="settings" size={20} />}
      title="Advanced settings"
      subtitle="Sync mode, schema drift policy, validation, and per-stream contracts"
      headerExtra={
        <span className={`df2-badge ${streamNeedsReview ? "df2-badge-run" : "df2-badge-live"}`}>
          {!sourceColumns.length
            ? "Waiting for schema"
            : streamNeedsReview
              ? "Sync contract incomplete"
              : requiresPrimaryKey || requiresCursor
                ? "Identity fields set"
                : "Sync mode ready"}
        </span>
      }
      footer={
        <div className="df2-drawer-actions">
          <Button size="sm" variant="primary" onClick={onClose}>
            Done
          </Button>
        </div>
      }
    >
      <div className="df2-dest-advanced-drawer">
        {activeMode && (
          <aside className="df2-adv-behavior-callout" aria-label="Sync behavior">
            <strong>{activeMode.label}</strong>
            <p>{activeMode.detail}</p>
            {syncMode === "full_refresh_overwrite" && (
              <p className="is-warn">Replaces destination rows — existing data is dropped before load. Do not use when you need to keep rows already in the table.</p>
            )}
            {syncMode === "full_refresh_append" && (
              <p>
                <strong>Load more into an existing table:</strong> keeps every destination row and
                inserts the full source snapshot again (100k existing + 100k file → 200k). Duplicate
                primary keys will fail or quarantine — use Incremental deduped / Upsert when keys
                may collide.
              </p>
            )}
            {syncMode === "incremental_append" && (
              <p>
                Requires a <strong>cursor column</strong> (e.g. updated_at / id). Only rows newer than
                the last watermark are inserted. Without a cursor, Validate blocks — pick Full append
                to reload the whole file into an existing table.
              </p>
            )}
            {syncMode === "incremental_deduped" && (
              <p>
                Cursor + <strong>primary key</strong> upserts: new keys insert, existing keys update.
                Best when the destination table already has data and the source re-sends changed rows.
              </p>
            )}
            {syncMode === "cdc" && (
              <p>Change delivery is <strong>at-least-once upsert</strong>. Exactly-once and at-most-once are not claimed.</p>
            )}
            {syncMode === "mirror" && (
              <p>
                Missing source keys are <strong>soft-deleted</strong> on the destination
                (<code>_deleted</code>), not hard-deleted. Physical <code>COUNT(*)</code> stays;
                the conservation identity is dest-engine <code>COUNT(*) WHERE NOT _deleted</code>.
                Writer acknowledgement is not active population.
              </p>
            )}
          </aside>
        )}

        <div className="df2-policy-grid">
          <div className="df2-field">
            <label className="df2-label">Sync mode</label>
            <div className="df2-policy-options">
              {syncModes.map((mode) => (
                <button
                  key={mode.id}
                  type="button"
                  className={`df2-policy-option ${syncMode === mode.id ? "active" : ""}`}
                  onClick={() => onSyncModeChange(mode.id)}
                >
                  <strong>{mode.label}</strong>
                  <span>{mode.detail}</span>
                </button>
              ))}
            </div>
          </div>

          <div className="df2-field">
            <label className="df2-label">Schema change policy</label>
            <div className="df2-policy-options">
              {schemaPolicies.map((policy) => (
                <button
                  key={policy.id}
                  type="button"
                  className={`df2-policy-option ${schemaPolicy === policy.id ? "active" : ""}`}
                  onClick={() => onSchemaPolicyChange(policy.id)}
                >
                  <strong>{policy.label}</strong>
                  <span>{policy.detail}</span>
                </button>
              ))}
            </div>
          </div>
        </div>

        {((requiresCursor || requiresPrimaryKey) && (suggestedCursor || suggestedPrimaryKey || uniqueKeySuggestions.length > 0 || (compositeKeySuggestions?.length ?? 0) > 0)) && (
          <div className="df2-adv-suggest-row">
            {requiresCursor && suggestedCursor && !defaultCursor && (
              <button
                type="button"
                className="df2-adv-suggest-chip"
                onClick={() => onStreamCursorChange(names[0], suggestedCursor)}
              >
                Use suggested cursor · <strong>{suggestedCursor}</strong>
              </button>
            )}
            {requiresPrimaryKey && suggestedPrimaryKey && !defaultPrimaryKey && (
              <button
                type="button"
                className="df2-adv-suggest-chip"
                onClick={() => onStreamPrimaryKeyChange(names[0], suggestedPrimaryKey)}
              >
                Use name heuristic · <strong>{suggestedPrimaryKey}</strong>
              </button>
            )}
            {requiresPrimaryKey &&
              uniqueKeySuggestions
                .filter((s) => s.column !== defaultPrimaryKey)
                .slice(0, 3)
                .map((s) => (
                  <button
                    key={s.column}
                    type="button"
                    className="df2-adv-suggest-chip"
                    title={`Unique in ${s.sampleRows}-row sample (${s.uniqueCount} values) — not full-table proof`}
                    onClick={() => onStreamPrimaryKeyChange(names[0], s.column)}
                  >
                    Unique in sample · <strong>{s.column}</strong>
                  </button>
                ))}
            {requiresPrimaryKey &&
              (compositeKeySuggestions || [])
                .slice(0, 2)
                .map((s) => {
                  const joined = s.columns.join(",");
                  return (
                    <button
                      key={joined}
                      type="button"
                      className="df2-adv-suggest-chip"
                      title={`Composite unique in ${s.sampleRows}-row sample — not full-table proof`}
                      onClick={() => onStreamPrimaryKeyChange(names[0], joined)}
                    >
                      Composite in sample · <strong>{s.columns.join(" + ")}</strong>
                    </button>
                  );
                })}
          </div>
        )}

        {syncMode === "cdc" && (
          <div className="df2-field df2-adv-snapshot-field">
            <label className="df2-label" htmlFor="df2-adv-snapshot-mode">CDC snapshot mode</label>
            <select
              id="df2-adv-snapshot-mode"
              className="df2-input df2-select"
              value={snapshotMode}
              onChange={(e) => onSnapshotModeChange?.(e.target.value)}
              disabled={!onSnapshotModeChange}
            >
              <option value="initial">initial — snapshot if no watermark (Debezium default)</option>
              <option value="always">always — snapshot every run, then stream</option>
              <option value="never">never — stream only (requires existing watermark)</option>
              <option value="initial_only">initial_only — snapshot then stop</option>
              <option value="when_needed">when_needed — snapshot if resume missing/broken</option>
            </select>
            <label className="df2-label" htmlFor="df2-adv-delivery" style={{ marginTop: "0.75rem" }}>
              CDC delivery guarantee
            </label>
            <select
              id="df2-adv-delivery"
              className="df2-input df2-select"
              value={deliveryGuarantee}
              onChange={(e) => {
                const next = e.target.value === "exactly_once" ? "exactly_once" : "at_least_once";
                onDeliveryGuaranteeChange?.(next);
                if (next === "exactly_once" && allowAppendOnly) {
                  onAllowAppendOnlyChange?.(false);
                }
              }}
              disabled={!onDeliveryGuaranteeChange}
            >
              <option value="at_least_once">at_least_once — default upsert (PK + _df_lsn)</option>
              <option value="exactly_once">
                exactly_once — dest-owned watermark + shared-log bundle (opt-in)
              </option>
            </select>
            <small className="df2-label-hint">
              Default stays <strong>at_least_once</strong>. Exactly-once commits apply and a dest
              watermark in one transaction
              {exactlyOnceWired
                ? " on this destination. Dest LSN is source of truth on resume; a stolen-lease writer cannot commit."
                : " — this destination is not transactional and fails closed"}
              {" "}At-most-once is not offered. Append-only below is incompatible with exactly-once.
            </small>
            {onAllowAppendOnlyChange && (
              <label className="df2-policy-toggle" style={{ marginTop: "0.75rem" }}>
                <input
                  type="checkbox"
                  checked={allowAppendOnly}
                  disabled={deliveryGuarantee === "exactly_once"}
                  onChange={(e) => onAllowAppendOnlyChange(e.target.checked)}
                />
                <span>
                  <strong>Allow append-only CDC</strong>
                  <small className="df2-label-hint">
                    Opt in when the destination cannot upsert. Redelivery will duplicate rows —
                    not idempotent. Prefer a PK upsert sink.
                  </small>
                </span>
              </label>
            )}
            {showMultiSubnetFailover && onMultiSubnetFailoverChange && (
              <label className="df2-policy-toggle" style={{ marginTop: "0.75rem" }}>
                <input
                  type="checkbox"
                  checked={multiSubnetFailover}
                  onChange={(e) => onMultiSubnetFailoverChange(e.target.checked)}
                />
                <span>
                  <strong>SQL Server MultiSubnetFailover</strong>
                  <small className="df2-label-hint">
                    Set ODBC <code>MultiSubnetFailover=Yes</code> when the source host is an Always On
                    AG listener. Speeds failover reconnect — does not invent continuous CDC across a
                    retention gap (still reset watermark + re-snapshot).
                  </small>
                </span>
              </label>
            )}
            {showCdcRowFilter && onCdcRowFilterChange && (
              <div className="df2-field" style={{ marginTop: "0.75rem" }}>
                <label className="df2-label" htmlFor="cdc-row-filter">
                  SQL Server CDC row filter
                </label>
                <select
                  id="cdc-row-filter"
                  className="df2-input"
                  value={cdcRowFilter}
                  onChange={(e) =>
                    onCdcRowFilterChange(e.target.value as "all" | "all update old" | "net")
                  }
                >
                  <option value="all">all — every change row (insert / update-before+after / delete)</option>
                  <option value="all update old">all update old — pair before-image on updates</option>
                  <option value="net">net — net changes TVF (requires @supports_net_changes=1)</option>
                </select>
                <small className="df2-label-hint">
                  Maps to Microsoft <code>row_filter_option</code> on{" "}
                  <code>cdc.fn_cdc_get_all_changes_*</code> /{" "}
                  <code>cdc.fn_cdc_get_net_changes_*</code>. Default <code>all</code> is safest;
                  <code>net</code> collapses multiple updates to the latest row per PK in the LSN
                  window.
                </small>
              </div>
            )}
          </div>
        )}

        <div className="df2-policy-toolbar">
          <div className="df2-field">
            <label className="df2-label" htmlFor="df2-adv-date-locale">Date locale</label>
            <select
              id="df2-adv-date-locale"
              className="df2-select"
              value={dateLocale}
              onChange={(e) => onDateLocaleChange(e.target.value as DestDateLocale)}
              title="How to interpret ambiguous day/month dates like 5/8/1967"
            >
              {dateLocales.map((loc) => (
                <option key={loc.id} value={loc.id} title={loc.detail}>
                  {loc.label}
                </option>
              ))}
            </select>
            <small className="df2-label-hint">Auto infers from unambiguous rows. Set DMY or MDY for all-ambiguous samples.</small>
          </div>
          <div className="df2-field">
            <label className="df2-label" htmlFor="df2-adv-number-locale">Number locale</label>
            <select
              id="df2-adv-number-locale"
              className="df2-select"
              value={numberLocale}
              onChange={(e) => onNumberLocaleChange(e.target.value as DestNumberLocale)}
              title="How to interpret 1,234 versus 1.234"
            >
              {numberLocales.map((loc) => (
                <option key={loc.id || "auto"} value={loc.id} title={loc.detail}>
                  {loc.label}
                </option>
              ))}
            </select>
            <small className="df2-label-hint">Auto will not guess a lone 1,234. Set US or EU, or use $ / € on the cell.</small>
          </div>
          <div className="df2-field">
            <label className="df2-label">Validation</label>
            <FilterBar ariaLabel="Validation mode">
              <FilterTabs
                ariaLabel="Validation mode"
                value={validationMode}
                onChange={(id) => onValidationModeChange(id as DestValidationMode)}
                items={validationModes.map((mode) => ({ id: mode.id, label: mode.label }))}
              />
            </FilterBar>
          </div>
          <label className="df2-policy-toggle">
            <input
              type="checkbox"
              checked={backfillNewFields || ["propagate_columns", "propagate_all"].includes(schemaPolicy)}
              disabled={!["propagate_columns", "propagate_all"].includes(schemaPolicy)}
              onChange={(e) => onBackfillChange(e.target.checked)}
            />
            <span>
              <strong>Backfill new fields</strong>
              <small>
                {["propagate_columns", "propagate_all"].includes(schemaPolicy)
                  ? "Propagate policies auto-enable additive destination columns"
                  : "Enable Propagate columns / Propagate everything first"}
              </small>
            </span>
          </label>
          {onWriteViaStagingChange && (
            <label className={`df2-policy-toggle${writeViaStagingSupported === false ? " is-disabled" : ""}`}>
              <input
                type="checkbox"
                checked={Boolean(writeViaStaging) && writeViaStagingSupported !== false}
                disabled={writeViaStagingSupported === false}
                onChange={(e) => onWriteViaStagingChange(e.target.checked)}
              />
              <span>
                <strong>Write via staging</strong>
                <small>
                  {writeViaStagingSupported === false
                    ? "Unavailable for this destination — SQL engines only (PostgreSQL, MySQL, Snowflake, BigQuery, …)."
                    : "Load into {table}_df_staging first, then promote only clean rows. Bad rows stay off primary (DLQ + staging). Strict validation blocks promote entirely."}
                </small>
              </span>
            </label>
          )}
        </div>

        {showVectorOptions && (
          <div className="df2-stream-contract" style={{ marginTop: "1rem" }} aria-label="Vector destination options">
            <div className="df2-stream-head">
              <strong>Vector / embedding</strong>
              <span>Chunk → embed → upsert (at-least-once)</span>
            </div>
            <p className="df2-label-hint" style={{ margin: "0 0 10px" }}>
              Requires <code>sentence-transformers</code> locally or an OpenAI model +{" "}
              <code>OPENAI_API_KEY</code>. Precomputed vectors skip re-embedding when an embedding
              column is set. PDF/DOCX/HTML uploads arrive as pre-chunked rows with{" "}
              <code>page</code>/<code>heading</code> provenance — content column should be{" "}
              <code>content</code>. Semantic routing excludes PII from embed content and metadata.
            </p>
            {onApplyVectorRouting && (
              <div className="df2-policy-toolbar" style={{ marginBottom: 10, alignItems: "center", gap: 8 }}>
                <Button
                  size="sm"
                  variant="secondary"
                  disabled={vectorRoutingLoading || !sourceColumns.length}
                  onClick={onApplyVectorRouting}
                >
                  {vectorRoutingLoading ? "Routing…" : "Apply semantic routing"}
                </Button>
                {vectorExcludePiiColumns ? (
                  <span className="df2-badge df2-badge-run" title="Excluded from vector metadata">
                    PII excluded: {vectorExcludePiiColumns}
                  </span>
                ) : null}
              </div>
            )}
            {vectorRoutingFields.length > 0 && (
              <div className="df2-stream-table-wrap" style={{ marginBottom: 12, maxHeight: 180, overflow: "auto" }}>
                <table className="df2-stream-table" aria-label="Semantic vector field routing">
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th>Action</th>
                      <th>Reason</th>
                    </tr>
                  </thead>
                  <tbody>
                    {vectorRoutingFields.map((row) => (
                      <tr key={row.column}>
                        <td>{row.column}</td>
                        <td>
                          <span className={`df2-badge ${row.action === "exclude_pii" ? "df2-badge-run" : row.action === "embed" ? "df2-badge-live" : ""}`}>
                            {row.action}
                          </span>
                        </td>
                        <td>
                          <small>{row.reason}</small>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-vector-content">Content column</label>
              <select
                id="df2-vector-content"
                className="df2-input"
                value={vectorContentColumn}
                onChange={(e) => onVectorContentColumnChange?.(e.target.value)}
              >
                <option value="">Auto (first long text column)</option>
                {sourceColumns.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-vector-embed-col">Precomputed embedding column</label>
              <select
                id="df2-vector-embed-col"
                className="df2-input"
                value={vectorEmbeddingColumn}
                onChange={(e) => onVectorEmbeddingColumnChange?.(e.target.value)}
              >
                <option value="">None — embed at write time</option>
                {sourceColumns.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-vector-meta">Metadata columns</label>
              <input
                id="df2-vector-meta"
                className="df2-input"
                placeholder="Comma-separated (e.g. id, category)"
                value={vectorMetadataColumns}
                onChange={(e) => onVectorMetadataColumnsChange?.(e.target.value)}
              />
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-vector-model">Embedding model</label>
              <input
                id="df2-vector-model"
                className="df2-input"
                placeholder="sentence-transformers/all-MiniLM-L6-v2 or openai/text-embedding-3-small"
                value={vectorEmbeddingModel}
                onChange={(e) => onVectorEmbeddingModelChange?.(e.target.value)}
              />
            </div>
            <div className="df2-policy-toolbar">
              <div className="df2-field">
                <label className="df2-label" htmlFor="df2-vector-chunk">Chunk size</label>
                <input
                  id="df2-vector-chunk"
                  className="df2-input"
                  type="number"
                  min={64}
                  max={4096}
                  value={vectorChunkSize}
                  onChange={(e) => onVectorChunkSizeChange?.(Number(e.target.value) || 512)}
                />
              </div>
              <div className="df2-field">
                <label className="df2-label" htmlFor="df2-vector-overlap">Chunk overlap</label>
                <input
                  id="df2-vector-overlap"
                  className="df2-input"
                  type="number"
                  min={0}
                  max={1024}
                  value={vectorChunkOverlap}
                  onChange={(e) => onVectorChunkOverlapChange?.(Number(e.target.value) || 0)}
                />
              </div>
            </div>
            {onVectorDurableCacheChange && (
              <label className="df2-policy-toggle" style={{ marginTop: 12 }}>
                <input
                  type="checkbox"
                  checked={vectorDurableCache}
                  onChange={(e) => onVectorDurableCacheChange(e.target.checked)}
                />
                <span>
                  <strong>Durable embedding cache</strong>
                  <small>
                    Persist model outputs in SQLite under the data directory so restarts reuse
                    vectors (process L1 + disk L2). Disable only for one-off experiments. Not a
                    shared multi-node cache unless nodes share the same volume.
                  </small>
                </span>
              </label>
            )}
            {(onRefreshEmbeddingCache || onClearEmbeddingCache) && (
              <div className="df2-policy-toolbar" style={{ marginTop: 10, alignItems: "center", gap: 8 }}>
                {embeddingCacheStats ? (
                  <span className="df2-badge df2-badge-live" title={embeddingCacheStats.path || ""}>
                    Cache: {embeddingCacheStats.entries ?? 0} entries
                    {typeof embeddingCacheStats.hit_rate === "number"
                      ? ` · ${(embeddingCacheStats.hit_rate * 100).toFixed(0)}% session hits`
                      : ""}
                  </span>
                ) : (
                  <span className="df2-badge">Cache stats unavailable</span>
                )}
                {onRefreshEmbeddingCache && (
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={embeddingCacheBusy}
                    onClick={onRefreshEmbeddingCache}
                  >
                    Refresh
                  </Button>
                )}
                {onClearEmbeddingCache && (
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={embeddingCacheBusy}
                    onClick={onClearEmbeddingCache}
                  >
                    Clear cache
                  </Button>
                )}
              </div>
            )}
          </div>
        )}
        <div className="df2-stream-contract">
          <div className="df2-stream-head">
            <strong>Streams and fields</strong>
            <span>
              {names.length > 1 ? `${names.length} streams` : "1 stream"}
              {" · "}
              {sourceColumns.length} discovered fields
            </span>
          </div>
          <p className="df2-adv-identity-note">
            {requiresPrimaryKey
              ? "Primary key is required for this sync mode (upsert / CDC / mirror / SCD2)."
              : "Full refresh · Append / Overwrite does not require a unique primary key. "
                + "You can still set one for documentation; duplicate id values will not block Validate."}
            {requiresCursor
              ? " Cursor is required for incremental / CDC, and what it means in the "
                + "source decides what this sync can capture \u2014 a column name cannot "
                + "establish that, so declare it."
              : ""}
          </p>
          {names.length > 1 && (
            <p className="df2-label-hint" style={{ margin: "0 0 10px" }}>
              Each stream keeps its own cursor and primary key. Sync mode and schema policy apply to all streams.
            </p>
          )}
          <div className="df2-stream-table-wrap">
            <table className="df2-stream-table">
              <thead>
                <tr>
                  <th>Stream</th>
                  <th>Mode</th>
                  <th>Cursor</th>
                  <th>Cursor means</th>
                  <th>Primary key</th>
                  <th>Policy</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {names.map((streamName) => {
                  const fields = resolveStreamFields(
                    streamName,
                    streamFields,
                    defaultCursor,
                    defaultPrimaryKey,
                    defaultCursorSemantics,
                  );
                  const streamCols = sourceColumnsByStream[streamName]?.length
                    ? sourceColumnsByStream[streamName]
                    : sourceColumns;
                  const streamSchema = sourceSchemaByStream[streamName] && Object.keys(sourceSchemaByStream[streamName]).length
                    ? sourceSchemaByStream[streamName]
                    : sourceSchema;
                  const semantics = evaluateCursorSemantics({
                    syncMode,
                    cursorField: fields.cursorField,
                    declared: fields.cursorSemantics || "",
                    validationMode,
                  });
                  const rowNeeds =
                    streamCols.length > 0
                    && ((requiresCursor && (!fields.cursorField || !streamCols.includes(fields.cursorField)))
                      || (requiresPrimaryKey && (!fields.primaryKeyField || !streamCols.includes(fields.primaryKeyField)))
                      || semantics.status === "block");
                  return (
                    <tr key={streamName}>
                      <td>
                        <label className="df2-stream-name">
                          <input type="checkbox" checked readOnly aria-label={`${streamName} selected`} />
                          <span>
                            <strong>{streamName}</strong>
                            <small>
                              {streamCols.length ? `${streamCols.length} fields` : "No schema loaded"}
                            </small>
                          </span>
                        </label>
                      </td>
                      <td>{syncModeLabel}</td>
                      <td>
                        <select
                          className="df2-input df2-select df2-stream-select"
                          value={requiresCursor && fields.cursorField && streamCols.includes(fields.cursorField)
                            ? fields.cursorField
                            : ""}
                          disabled={!requiresCursor || streamCols.length === 0}
                          onChange={(e) => onStreamCursorChange(streamName, e.target.value)}
                          aria-label={`Cursor field for ${streamName}`}
                        >
                          <option value="">{requiresCursor ? "Select cursor" : "Not required"}</option>
                          {streamCols.map((col) => (
                            <option key={col} value={col}>
                              {col}{streamSchema[col] ? ` · ${streamSchema[col]}` : ""}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td>
                        <select
                          className="df2-input df2-select df2-stream-select"
                          value={fields.cursorSemantics || ""}
                          disabled={!requiresCursor || !fields.cursorField}
                          onChange={(e) =>
                            onStreamCursorSemanticsChange(streamName, e.target.value)}
                          aria-label={`Cursor semantics for ${streamName}`}
                        >
                          <option value="">
                            {requiresCursor ? "Declare meaning" : "Not required"}
                          </option>
                          {CURSOR_SEMANTICS.map((value) => (
                            <option key={value} value={value}>
                              {CURSOR_SEMANTICS_LABELS[value]}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td>
                        <select
                          className="df2-input df2-select df2-stream-select"
                          value={fields.primaryKeyField && streamCols.includes(fields.primaryKeyField)
                            ? fields.primaryKeyField
                            : ""}
                          disabled={streamCols.length === 0}
                          onChange={(e) => onStreamPrimaryKeyChange(streamName, e.target.value)}
                          aria-label={`Primary key for ${streamName}`}
                        >
                          <option value="">
                            {requiresPrimaryKey ? "Select key" : "Optional (append)"}
                          </option>
                          {streamCols.map((col) => (
                            <option key={col} value={col}>
                              {col}{streamSchema[col] ? ` · ${streamSchema[col]}` : ""}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td>{schemaPolicyLabel}</td>
                      <td>
                        <span className={`df2-badge ${rowNeeds ? "df2-badge-run" : "df2-badge-live"}`}>
                          {streamCols.length ? (rowNeeds ? "Needs contract" : "Valid") : "Pending"}
                        </span>
                        {semantics.reason && (
                          <small className="df2-label-hint df2-stream-cursor-note">
                            {semantics.reason}
                            {semantics.primaryAction ? ` \u2192 ${semantics.primaryAction}.` : ""}
                          </small>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        {(syncMode === "scd2" || syncMode === "mirror") && (
          <p className="df2-label-hint" style={{ margin: "12px 0 0" }}>
            {syncMode === "scd2"
              ? "SCD Type 2 requires a primary key on each stream to version rows (valid-from / valid-to)."
              : "Mirror sync requires a primary key on each stream. Dest keys missing from the source are flagged _deleted; physical COUNT(*) does not drop."}
            {streamNeedsReview ? " Select a primary key above before running." : ""}
          </p>
        )}

        <div className="df2-adv-load-controls">
          <h4 className="df2-adv-section-title">Load controls</h4>
          <p className="df2-label-hint">
            Priority-first ordering and optional row caps — useful for smoke tests and high-value-first migrations.
          </p>
          <div className="df2-adv-load-grid">
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-adv-priority-col">Priority column</label>
              <select
                id="df2-adv-priority-col"
                className="df2-input df2-select"
                value={priorityColumn}
                onChange={(e) => onPriorityColumnChange?.(e.target.value)}
                disabled={!onPriorityColumnChange || sourceColumns.length === 0}
              >
                <option value="">None (source order)</option>
                {sourceColumns.map((col) => (
                  <option key={col} value={col}>
                    {col}{sourceSchema[col] ? ` · ${sourceSchema[col]}` : ""}
                  </option>
                ))}
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-adv-priority-dir">Direction</label>
              <select
                id="df2-adv-priority-dir"
                className="df2-input df2-select"
                value={priorityDirection}
                disabled={!priorityColumn || !onPriorityDirectionChange}
                onChange={(e) => onPriorityDirectionChange?.(e.target.value as "asc" | "desc")}
              >
                <option value="desc">Highest first</option>
                <option value="asc">Lowest first</option>
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-adv-row-limit">Row limit</label>
              <input
                id="df2-adv-row-limit"
                className="df2-input"
                type="number"
                min={0}
                step={1000}
                value={rowLimit || ""}
                placeholder="0 = no limit"
                onChange={(e) => onRowLimitChange?.(Math.max(0, Number(e.target.value) || 0))}
                disabled={!onRowLimitChange}
              />
              <small className="df2-label-hint">0 means transfer all rows.</small>
            </div>
          </div>
        </div>
      </div>
    </Drawer>
  );
}
