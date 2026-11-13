interface MappingToolbarProps {
  canUndo: boolean;
  canRedo: boolean;
  canAutoMap: boolean;
  panelOpen: boolean;
  panelId: string;
  openSources: number;
  onAutoMap: () => void;
  onMapRemaining: () => void;
  onClearAll: () => void;
  onUndo: () => void;
  onRedo: () => void;
  onTogglePanel: () => void;
}

const IS_MAC = typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform || "");
const MOD = IS_MAC ? "⌘" : "Ctrl";

/** Bulk mapping actions. Every one of them is a single undo step. */
export function MappingToolbar({
  canUndo,
  canRedo,
  canAutoMap,
  panelOpen,
  panelId,
  openSources,
  onAutoMap,
  onMapRemaining,
  onClearAll,
  onUndo,
  onRedo,
  onTogglePanel,
}: MappingToolbarProps) {
  return (
    <div className="df2-ms-toolbar" role="toolbar" aria-label="Mapping actions">
      <div className="df2-ms-toolbar-group">
        <button type="button" className="df2-btn df2-btn-sm" onClick={onAutoMap} disabled={!canAutoMap}>
          Auto-map…
        </button>
        <button
          type="button"
          className="df2-btn df2-btn-sm"
          onClick={onMapRemaining}
          disabled={!canAutoMap || openSources === 0}
          title={openSources ? `${openSources} source column(s) have no destination` : "Every source column has a destination"}
        >
          Map remaining{openSources ? ` (${openSources})` : ""}
        </button>
        <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={onClearAll}>
          Clear all
        </button>
      </div>
      <div className="df2-ms-toolbar-group">
        <button
          type="button"
          className="df2-btn df2-btn-sm df2-btn-ghost"
          onClick={onUndo}
          disabled={!canUndo}
          aria-keyshortcuts={IS_MAC ? "Meta+Z" : "Control+Z"}
          title={`Undo (${MOD}+Z)`}
        >
          Undo
        </button>
        <button
          type="button"
          className="df2-btn df2-btn-sm df2-btn-ghost"
          onClick={onRedo}
          disabled={!canRedo}
          aria-keyshortcuts={IS_MAC ? "Meta+Shift+Z" : "Control+Shift+Z"}
          title={`Redo (Shift+${MOD}+Z)`}
        >
          Redo
        </button>
        <button
          type="button"
          className="df2-btn df2-btn-sm df2-btn-ghost"
          aria-expanded={panelOpen}
          aria-controls={panelId}
          onClick={onTogglePanel}
        >
          {panelOpen ? "Hide destination columns" : "Show destination columns"}
        </button>
      </div>
    </div>
  );
}
