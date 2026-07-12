import { useEffect, useMemo, useState } from "react";
import { ColumnReviewPanel } from "../../components/ColumnReviewPanel";
import { MappingIntelligencePanel } from "../../components/MappingIntelligencePanel";
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
  onChangeMappings,
  onBack,
  onContinue,
}: TransferMapStepProps) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<ColumnFilter>("all");
  const [focusSource, setFocusSource] = useState<string | null>(null);

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
        <h3 className="df2-card-title">Map columns</h3>
      </div>

      <div className="df2-card-body df2-map-step-body">
        <div className="df2-map-step-workspace">
          <section className="df2-map-editor-pane" aria-label="Column mappings editor">
            <header className="df2-map-pane-label">
              <DtIcon name="database" size={14} />
              Column mappings
            </header>
            <div className="df2-map-editor-scroll-host">
              <ColumnReviewPanel
                mappings={columnMappings}
                rowCount={rowCount}
                sampleRows={sampleRows}
                confidenceThreshold={confidenceThreshold}
                onChange={onChangeMappings}
                destinationFields={destColumns}
                destinationLabel={destRouteLabel}
                compact
                hideTitle
                search={search}
                onSearchChange={setSearch}
                filter={filter}
                onFilterChange={setFilter}
                focusSource={focusSource}
                onFocusHandled={() => setFocusSource(null)}
              />
            </div>
          </section>

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
    </div>
  );
}
