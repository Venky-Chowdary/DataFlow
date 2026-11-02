import { useMemo, useRef, useState, type KeyboardEvent } from "react";
import { typeTone } from "../../lib/queryResults";
import { columnCaption, filterSchemaObjects } from "../../lib/querySchema";
import { nextTreeIndex, SCHEMA_TRUNCATION_COPY } from "../../lib/schemaTree";
import type { SchemaObject } from "../../lib/sqlIntel";
import { uniqueListKey } from "../../lib/uiUtils";
import { shouldVirtualize, virtualRange } from "../../lib/virtualWindow";
import { DtIcon } from "../DtIcon";
import { EmptyState } from "../ui/EmptyState";

/**
 * Schema sidebar — objects, then columns on expand.
 *
 * Two-phase on purpose: the API lists objects without columns and expands one
 * object at a time. Query and Transfer share this tree. Transfer passes
 * `density="studio"` and `onUseObject`; Query keeps the historical metrics.
 */

export interface SchemaBrowserProps {
  objects: SchemaObject[];
  loading?: boolean;
  error?: string;
  /** Object names currently fetching columns. */
  pending?: string[];
  connected?: boolean;
  /** Provenance of the reported types — inference vs catalog varies by engine. */
  typeSource?: string;
  warnings?: string[];
  /** The catalog capped the object list. The browser must say so. */
  objectsTruncated?: boolean;
  onExpand: (objectName: string) => void;
  onRefresh: () => void;
  /** Click-to-insert into the editor at the caret. */
  onInsert: (text: string) => void;
  /** Generate and load a starter SELECT for an object. */
  onPreview: (object: SchemaObject) => void;
  /** Transfer uses this to set the source table. Query leaves it unset. */
  onUseObject?: (objectName: string) => void;
  density?: "query" | "studio";
}

const ROW_HEIGHT = 36;

export function SchemaBrowser({
  objects,
  loading,
  error,
  pending = [],
  connected,
  typeSource,
  warnings = [],
  objectsTruncated = false,
  onExpand,
  onRefresh,
  onInsert,
  onPreview,
  onUseObject,
  density = "query",
}: SchemaBrowserProps) {
  const [filter, setFilter] = useState("");
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [scrollTop, setScrollTop] = useState(0);
  const treeRef = useRef<HTMLDivElement>(null);
  const studio = density === "studio";
  const icon = studio ? 16 : 13;

  const shown = useMemo(() => filterSchemaObjects(objects, filter), [objects, filter]);
  const expanded = shown.some((object) => open[object.name]);
  const virtual = shouldVirtualize(shown.length, expanded);
  const range = virtual
    ? virtualRange(shown.length, scrollTop, 420, ROW_HEIGHT)
    : { start: 0, end: shown.length, offsetTop: 0, totalHeight: 0 };
  const visible = virtual ? shown.slice(range.start, range.end) : shown;

  const toggle = (object: SchemaObject) => {
    const next = !open[object.name];
    setOpen((prev) => ({ ...prev, [object.name]: next }));
    if (next && !(object.columns?.length ?? 0)) onExpand(object.name);
  };

  const onTreeKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const items = treeRef.current?.querySelectorAll<HTMLButtonElement>("[data-tree-item]");
    if (!items || items.length === 0) return;
    const list = Array.from(items);
    const current = list.findIndex((element) => element === document.activeElement);
    const next = nextTreeIndex(current, event.key, list.length);
    if (next == null) return;
    event.preventDefault();
    list[next]?.focus();
  };

  return (
    <aside className="df2-qw-schema" aria-label="Schema browser" data-density={density}>
      <div className="df2-qw-schema-head">
        <span className="df2-qw-schema-title">
          <DtIcon name="database" size={icon} chrome={studio ? "product" : "nav"} /> Schema
        </span>
        <button
          type="button"
          className="df2-qw-icon-btn"
          onClick={onRefresh}
          disabled={loading}
          title="Reload objects"
          aria-label="Reload objects"
        >
          <DtIcon name={loading ? "spinner" : "refresh"} size={icon} chrome={studio ? "product" : "nav"} />
        </button>
      </div>

      <input
        className="df2-input df2-input-sm df2-qw-schema-filter"
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        placeholder="Filter objects and columns…"
        aria-label="Filter schema"
      />

      {error && (
        <p className="df2-qw-schema-error" role="alert">
          {error}
        </p>
      )}

      {objectsTruncated && (
        <p className="df2-qw-schema-warn" role="status">
          {SCHEMA_TRUNCATION_COPY}
        </p>
      )}

      {warnings.map((w, i) => (
        <p key={uniqueListKey(w, i, "warn")} className="df2-qw-schema-warn">
          <DtIcon name="warning" size={studio ? 16 : 12} chrome={studio ? "product" : "nav"} /> {w}
        </p>
      ))}

      <div
        className="df2-qw-schema-tree"
        ref={treeRef}
        onKeyDown={onTreeKeyDown}
        onScroll={virtual ? (event) => setScrollTop(event.currentTarget.scrollTop) : undefined}
      >
        {loading && objects.length === 0 && (
          studio ? (
            <EmptyState compact icon="database" title="Loading objects" description="Reading the catalog for this connector." />
          ) : (
            <p className="df2-qw-schema-empty">Loading objects…</p>
          )
        )}
        {!loading && objects.length === 0 && !error && (
          studio ? (
            <EmptyState
              compact
              icon="database"
              title={connected === false ? "Not connected" : "No objects yet"}
              description={connected === false
                ? "The connector did not open. Test it, then reload."
                : "Select a connector to browse its objects."}
            />
          ) : (
            <p className="df2-qw-schema-empty">
              {connected === false
                ? "Not connected."
                : "Select a connector to browse its objects."}
            </p>
          )
        )}
        {!loading && objects.length > 0 && shown.length === 0 && (
          <p className="df2-qw-schema-empty">No object matches that filter.</p>
        )}
        <div style={virtual ? { height: range.totalHeight, position: "relative" } : undefined}>
          <div style={virtual ? { position: "absolute", top: range.offsetTop, left: 0, right: 0 } : undefined}>
            {visible.map((object) => {
              const isOpen = Boolean(open[object.name]);
              const isPending = pending.includes(object.name);
              const cols = object.columns ?? [];
              return (
                <div key={`${object.schema ?? ""}.${object.name}`} className="df2-qw-node">
                  <div className="df2-qw-node-row">
                    <button
                      type="button"
                      className="df2-qw-node-toggle"
                      data-tree-item=""
                      onClick={() => toggle(object)}
                      aria-expanded={isOpen}
                      title={isOpen ? "Collapse" : "Show columns"}
                    >
                      <DtIcon
                        name={isOpen ? "chevron-down" : "chevron-right"}
                        size={studio ? 16 : 12}
                        chrome={studio ? "product" : "nav"}
                      />
                      <span className="df2-qw-node-name" title={object.name}>
                        {object.name}
                      </span>
                    </button>
                    <span className="df2-qw-node-kind">{object.type || "table"}</span>
                    {onUseObject && (
                      <button
                        type="button"
                        className="df2-qw-icon-btn"
                        data-tree-item=""
                        onClick={() => onUseObject(object.name)}
                        title={`Use ${object.name}`}
                        aria-label={`Use ${object.name}`}
                      >
                        <DtIcon name="check" size={icon} chrome={studio ? "product" : "nav"} />
                      </button>
                    )}
                    <button
                      type="button"
                      className="df2-qw-icon-btn"
                      data-tree-item=""
                      onClick={() => onPreview(object)}
                      title="Preview rows"
                      aria-label={`Preview rows from ${object.name}`}
                    >
                      <DtIcon name="play" size={studio ? 16 : 11} chrome={studio ? "product" : "nav"} />
                    </button>
                  </div>

                  {isOpen && (
                    <div className="df2-qw-cols">
                      {isPending && <p className="df2-qw-schema-empty">Loading columns…</p>}
                      {!isPending && cols.length === 0 && (
                        <p className="df2-qw-schema-empty">No columns reported.</p>
                      )}
                      {cols.map((column) => (
                        <button
                          key={column.name}
                          type="button"
                          className="df2-qw-col"
                          data-tree-item=""
                          onClick={() => onInsert(column.name)}
                          title={`Insert ${column.name} · ${columnCaption(column)}`}
                        >
                          <span className="df2-qw-col-name">
                            {column.primaryKey && (
                              <DtIcon name="key" size={studio ? 16 : 10} chrome={studio ? "product" : "nav"} />
                            )}
                            {column.name}
                          </span>
                          <span className="df2-qw-col-type" data-tone={typeTone(column.type)}>
                            {column.type || "—"}
                          </span>
                          {column.nullable === false && (
                            <span className="df2-qw-col-flag">NOT NULL</span>
                          )}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {objects.length > 0 && (
        <p className="df2-qw-schema-foot">
          {objects.length} object{objects.length === 1 ? "" : "s"}
          {objectsTruncated ? " · list capped" : ""}
          {typeSource === "connector_introspection" && (
            <>
              {" · "}
              <span title="Catalog-backed engines report declared types; dynamically typed sources (SQLite, Mongo, CSV) report types inferred from sampled values.">
                types from connector introspection
              </span>
            </>
          )}
        </p>
      )}
    </aside>
  );
}
