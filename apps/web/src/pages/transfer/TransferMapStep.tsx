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

interface TransferMapStepProps {
  columnMappings: EditableMapping[];
  analysis: import("../../lib/types").EnhancedAnalysis | null;
  destColumns: string[];
  destSchemaLoading: boolean;
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
  onChangeMappings: (mappings: EditableMapping[]) => void;
  onBack: () => void;
  onContinue: () => void;
}

const INTELLIGENCE_PAIR_LIMIT = 500;

const MAP_STEP_SCROLL_CLASS = "is-map-step-view";

export function TransferMapStep({
  columnMappings,
  analysis,
  destColumns,
  destSchemaLoading,
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
  onChangeMappings,
  onBack,
  onContinue,
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

  const destDisplayType = destKindMode === "database" ? destType : "file";
  const destPaneSubtitle = destKindMode === "database"
    ? destSchemaLoading
      ? "Loading existing schema from connector…"
      : destColumns.length > 0
        ? `${destColumns.length} existing fields in ${targetDatabase}.${targetCollection}`
        : `New fields in ${targetDatabase}.${targetCollection}`
    : destRouteSubtitle;

  const filterCounts = useMemo(
    () => countByFilter(columnMappings, confidenceThreshold),
    [columnMappings, confidenceThreshold],
  );

  const approvedCount = filterCounts.ready;

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
            {destColumns.length === 0 && !destSchemaLoading ? " · create-new table" : ""}
          </p>
        </div>
        <div className="df2-map-step-head-actions">
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

      <div className="df2-card-body df2-map-step-body">
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
        size="xl"
        title="Edit column mappings"
        subtitle={`${columnMappings.length} columns · choose destination names and logical types carefully — wrong types fail preflight, not silently.`}
        ariaLabel="Full mapping table"
        className="df2-map-dialog"
        footer={
          <button type="button" className="df2-btn df2-btn-primary" onClick={() => setMapDialogOpen(false)}>
            Done
          </button>
        }
      >
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
