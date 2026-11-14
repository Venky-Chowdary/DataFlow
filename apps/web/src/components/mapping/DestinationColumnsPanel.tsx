import { useEffect, useMemo, useState, type DragEvent } from "react";
import {
  destColumnFacts,
  destPanelSummaryLine,
  filterDestColumns,
  type DestColumnCatalog,
  type DestColumnState,
  type MapTargetSummary,
} from "../../lib/destColumnCatalog";
import { useVirtualWindow } from "./useVirtualWindow";

export const PANEL_ROW_PX = 76;
export const DND_MIME = "application/x-datawrap-source-column";

type PanelFilter = "all" | "unmapped" | "required" | "conflicts";

const STATUS_LABEL: Record<DestColumnState["status"], string> = {
  mapped: "Mapped",
  conflict: "Conflict",
  generated_target: "Generated",
  required_unmapped: "Required",
  default_unmapped: "Default",
  engine_unmapped: "Generated",
  null_unmapped: "NULL",
  unmeasured_unmapped: "Unmapped",
};

interface DestinationColumnsPanelProps {
  id: string;
  tableLabel: string;
  catalog: DestColumnCatalog | null;
  states: DestColumnState[];
  summary: MapTargetSummary;
  loading: boolean;
  error: string;
  onReload?: () => void;
  onUnmap: (target: string) => void;
  onFocusSource: (source: string) => void;
  /** Column under the keyboard/pointer drag, by name (null = none). */
  dragOver: string | null;
  /** Scroll to the drag cursor (keyboard drags); off for pointer drags. */
  followDrag?: boolean;
  dragging: boolean;
  onPointerDrop: (source: string, target: string) => void;
  onPointerOver: (target: string | null) => void;
  /** The names this panel shows after its search and filter (keyboard drag walks them). */
  onVisibleChange?: (names: string[]) => void;
}

/**
 * Every column of the destination table, with what the write does to it.
 *
 * Mapped columns name their source. Required columns (NOT NULL, no default —
 * the G14 predicate) that nothing writes are flagged as Validate blockers.
 * Columns left unmapped say whether they get NULL, their default, or an
 * engine-generated value. Each row is a drop target for a dragged source.
 */
export function DestinationColumnsPanel({
  id,
  tableLabel,
  catalog,
  states,
  summary,
  loading,
  error,
  onReload,
  onUnmap,
  onFocusSource,
  dragOver,
  followDrag = true,
  dragging,
  onPointerDrop,
  onPointerOver,
  onVisibleChange,
}: DestinationColumnsPanelProps) {
  const [filter, setFilter] = useState<PanelFilter>("all");
  const [query, setQuery] = useState("");


  const visible = useMemo(() => {
    const byName = new Map(states.map((s) => [s.column.name, s]));
    const matched = filterDestColumns(states.map((s) => s.column), query)
      .map((c) => byName.get(c.name))
      .filter((s): s is DestColumnState => Boolean(s));
    return matched.filter((s) => {
      if (filter === "unmapped") return s.sources.length === 0;
      if (filter === "required") return s.column.required === true;
      if (filter === "conflicts") return s.status === "conflict";
      return true;
    });
  }, [states, query, filter]);

  const visibleKey = visible.map((s) => s.column.name).join("\u0000");
  useEffect(() => {
    onVisibleChange?.(visible.map((s) => s.column.name));
  }, [visibleKey]);

  const win = useVirtualWindow(visible.length, PANEL_ROW_PX, 480);
  const overIndex = dragOver ? visible.findIndex((s) => s.column.name === dragOver) : -1;
  useEffect(() => {
    // Follow the keyboard's drag cursor. A finger is already on its row:
    // scrolling the list would move the row out from under it.
    if (overIndex >= 0 && followDrag) win.scrollToIndex(overIndex);
  }, [overIndex]);

  const blocking = summary.requiredUnmapped.length + summary.conflicts.size + summary.refusedNewTargets.length;
  const counts: Record<PanelFilter, number> = {
    all: states.length,
    unmapped: summary.unmapped,
    required: states.filter((s) => s.column.required === true).length,
    conflicts: summary.conflicts.size,
  };

  const readSource = (e: DragEvent): string => e.dataTransfer.getData(DND_MIME) || e.dataTransfer.getData("text/plain");

  return (
    <aside id={id} className="df2-ms-panel" aria-label={`Destination columns of ${tableLabel}`}>
      <div className="df2-ms-panel-head">
        <div>
          <h4 className="df2-ms-panel-title">Destination columns</h4>
          <p className="df2-ms-panel-sub" title={tableLabel}>
            {tableLabel}
            {catalog?.origin === "api" ? "" : loading ? " · reading catalog…" : ""}
          </p>
        </div>
        {onReload ? (
          <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={onReload} disabled={loading}>
            {loading ? "Reading…" : "Reload"}
          </button>
        ) : null}
      </div>

      <p className="df2-ms-panel-counts" role="status" data-testid="dest-panel-summary">
        {destPanelSummaryLine(summary, catalog)}
      </p>

      {error ? (
        <p className="df2-ms-panel-note is-block" role="alert">{error}</p>
      ) : null}

      {blocking > 0 ? (
        <p className="df2-ms-panel-blockers" role="status">
          <strong>Validate will block: </strong>
          {[
            summary.requiredUnmapped.length
              ? `${summary.requiredUnmapped.join(", ")} ${summary.requiredUnmapped.length === 1 ? "needs" : "need"} a source (NOT NULL, no default)`
              : "",
            ...Array.from(summary.conflicts.entries()).map(([t, srcs]) => `${t} is written by ${srcs.join(" and ")}`),
            summary.refusedNewTargets.length
              ? `${summary.refusedNewTargets.join(", ")} cannot be created here`
              : "",
          ].filter(Boolean).join("; ")}.
        </p>
      ) : null}

      <div className="df2-ms-panel-tools">
        <input
          aria-label="Find a destination column"
          className="df2-input df2-ms-panel-search"
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Find a destination column"
          aria-controls={`${id}-list`}
        />
        <div className="df2-ms-panel-filters" role="group" aria-label="Show destination columns">
          {(["all", "unmapped", "required", "conflicts"] as PanelFilter[]).map((f) => (
            <button
              key={f}
              type="button"
              className={`df2-ms-chip${filter === f ? " is-active" : ""}`}
              aria-pressed={filter === f}
              onClick={() => setFilter(f)}
            >
              {f === "all" ? "All" : f === "unmapped" ? "Unmapped" : f === "required" ? "Required" : "Conflicts"} {counts[f]}
            </button>
          ))}
        </div>
      </div>

      <ul
        ref={win.ref}
        id={`${id}-list`}
        className={`df2-ms-panel-list${dragging ? " is-drop-armed" : ""}`}
        aria-label="Destination columns"
        onDragLeave={(e) => {
          if (!e.currentTarget.contains(e.relatedTarget as Node | null)) onPointerOver(null);
        }}
      >
        {visible.length === 0 ? (
          <li className="df2-ms-panel-empty">
            {loading ? "Reading the destination's columns…" : catalog?.columns.length ? "No columns match." : "The destination reported no columns."}
          </li>
        ) : (
          <>
            <li aria-hidden="true" style={{ height: win.offsetTop }} />
            {visible.slice(win.start, win.end).map((s, k) => {
              const i = win.start + k;
              const over = i === overIndex;
              return (
                <li
                  key={s.column.name}
                  className={`df2-ms-dest-row is-${s.status}${over ? " is-drop-over" : ""}`}
                  data-dest-column={s.column.name}
                  aria-current={over ? "location" : undefined}
                  onDragOver={(e) => {
                    if (!dragging && !Array.from(e.dataTransfer.types).includes(DND_MIME)) return;
                    e.preventDefault();
                    e.dataTransfer.dropEffect = "link";
                    if (!over) onPointerOver(s.column.name);
                  }}
                  onDrop={(e) => {
                    const source = readSource(e);
                    if (!source) return;
                    e.preventDefault();
                    onPointerDrop(source, s.column.name);
                  }}
                >
                  <div className="df2-ms-dest-line">
                    <span className="df2-ms-dest-name" title={s.column.name}>
                      {s.column.primaryKey ? <span className="df2-ms-key" aria-label="primary key">PK</span> : null}
                      {s.column.name}
                    </span>
                    <span className={`df2-ms-status is-${s.status}`}>{STATUS_LABEL[s.status]}</span>
                  </div>
                  <div className="df2-ms-dest-line is-sub">
                    <span className="df2-ms-dest-facts">{destColumnFacts(s.column).join(" · ") || "type not read"}</span>
                    {s.sources.length ? (
                      <span className="df2-ms-dest-sources">
                        ←{" "}
                        {s.sources.map((src) => (
                          <button
                            key={src}
                            type="button"
                            className="df2-ms-link"
                            onClick={() => onFocusSource(src)}
                            title={`Show the ${src} row`}
                          >
                            {src}
                          </button>
                        ))}
                        <button
                          type="button"
                          className="df2-ms-link is-unmap"
                          onClick={() => onUnmap(s.column.name)}
                          aria-label={`Unmap ${s.column.name}`}
                          title={`Leave ${s.column.name} unmapped`}
                        >
                          Unmap
                        </button>
                      </span>
                    ) : null}
                  </div>
                  <span className="df2-sr-only">{s.explanation}</span>
                  <span className="df2-ms-dest-why" aria-hidden="true" title={s.explanation}>{s.explanation}</span>
                </li>
              );
            })}
            <li aria-hidden="true" style={{ height: win.tailHeight }} />
          </>
        )}
      </ul>

      <p className="df2-ms-panel-note">
        Unmapped columns that are not required are written as NULL or their default.
        {catalog ? ` New target names: ${catalog.addColumnsReason}` : ""}
      </p>
    </aside>
  );
}
