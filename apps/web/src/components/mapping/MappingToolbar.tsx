import { DtIcon } from "../DtIcon";
import { JobsMenu, type JobsMenuItem } from "../jobs/JobsMenu";

interface MappingToolbarProps {
  /** Auto-map, Map remaining, Undo/Redo and the panel need the destination catalog. */
  remapEnabled: boolean;
  canUndo: boolean;
  canRedo: boolean;
  canAutoMap: boolean;
  panelOpen: boolean;
  panelId: string;
  openSources: number;
  /** Rows that need a decision before Validate (the Review action's count). */
  reviewCount: number;
  onReview: () => void;
  onAutoMap: () => void;
  onMapRemaining: () => void;
  onClearAll: () => void;
  onUndo: () => void;
  onRedo: () => void;
  onTogglePanel: () => void;
  /** Everything else (computed column, templates, import/export, proof, full table). */
  moreItems?: JobsMenuItem[];
}

const IS_MAC = typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform || "");
const MOD = IS_MAC ? "⌘" : "Ctrl";

/**
 * The Map step's compact toolbar: two primary actions (Auto-map, Review),
 * Undo/Redo as icon buttons with their shortcuts, the destination panel
 * toggle, and one "More" menu for every other action. Every bulk action is a
 * single undo step.
 */
export function MappingToolbar({
  remapEnabled,
  canUndo,
  canRedo,
  canAutoMap,
  panelOpen,
  panelId,
  openSources,
  reviewCount,
  onReview,
  onAutoMap,
  onMapRemaining,
  onClearAll,
  onUndo,
  onRedo,
  onTogglePanel,
  moreItems = [],
}: MappingToolbarProps) {
  const items: JobsMenuItem[] = [
    ...(remapEnabled
      ? [
          {
            id: "map-remaining",
            label: `Map remaining${openSources ? ` (${openSources})` : ""}`,
            onSelect: onMapRemaining,
            disabled: !canAutoMap || openSources === 0,
            title: openSources ? `${openSources} source column(s) have no destination` : "Every source column has a destination",
            section: "Mapping",
          },
          { id: "clear-all", label: "Clear all mappings", onSelect: onClearAll, section: "Mapping" },
        ]
      : []),
    ...moreItems,
  ];
  return (
    <div className="df2-ms-toolbar" role="toolbar" aria-label="Mapping actions">
      {remapEnabled && (
        <button type="button" className="df2-btn df2-btn-sm" onClick={onAutoMap} disabled={!canAutoMap}>
          <DtIcon name="sparkle" size={14} />Auto-map…
        </button>
      )}
      <button
        type="button"
        className="df2-btn df2-btn-sm"
        onClick={onReview}
        disabled={reviewCount === 0}
        aria-keyshortcuts="R"
        title={reviewCount ? `Show the ${reviewCount} row(s) that need a decision (R)` : "Nothing needs review"}
      >
        Review{reviewCount ? <span className="df2-ms-count">{reviewCount}</span> : null}
      </button>
      {remapEnabled && (
        <span className="df2-ms-toolbar-icons">
          <button
            type="button"
            className="df2-ms-icon-btn"
            onClick={onUndo}
            disabled={!canUndo}
            aria-label="Undo"
            aria-keyshortcuts={IS_MAC ? "Meta+Z" : "Control+Z"}
            title={`Undo (${MOD}+Z)`}
          >
            <DtIcon name="undo" size={15} />
          </button>
          <button
            type="button"
            className="df2-ms-icon-btn"
            onClick={onRedo}
            disabled={!canRedo}
            aria-label="Redo"
            aria-keyshortcuts={IS_MAC ? "Meta+Shift+Z" : "Control+Shift+Z"}
            title={`Redo (Shift+${MOD}+Z)`}
          >
            <DtIcon name="redo" size={15} />
          </button>
          <button
            type="button"
            className={`df2-ms-icon-btn${panelOpen ? " is-on" : ""}`}
            aria-pressed={panelOpen}
            aria-controls={panelId}
            aria-label={panelOpen ? "Hide destination columns" : "Show destination columns"}
            title={panelOpen ? "Hide destination columns" : "Show destination columns"}
            onClick={onTogglePanel}
          >
            <DtIcon name="panel-right" size={15} />
          </button>
        </span>
      )}
      {items.length > 0 && (
        <JobsMenu
          label="More mapping actions"
          text="More"
          icon={<DtIcon name="more" size={15} />}
          items={items}
          triggerClassName="df2-btn df2-btn-sm df2-btn-ghost df2-ms-more"
        />
      )}
    </div>
  );
}
