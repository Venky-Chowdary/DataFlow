import { useMemo, useState } from "react";
import { typeTone } from "../../lib/queryResults";
import type { SchemaObject } from "../../lib/sqlIntel";
import { DtIcon } from "../DtIcon";

/**
 * Schema sidebar for the query workspace — objects, then columns on expand.
 *
 * Two-phase on purpose: the API lists objects without columns and expands one
 * object at a time, because loading every column on a large estate is what
 * makes schema browsers unusable. Column loading is delegated to the caller.
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
  onExpand: (objectName: string) => void;
  onRefresh: () => void;
  /** Click-to-insert into the editor at the caret. */
  onInsert: (text: string) => void;
  /** Generate and load a starter SELECT for an object. */
  onPreview: (object: SchemaObject) => void;
}

export function SchemaBrowser({
  objects,
  loading,
  error,
  pending = [],
  connected,
  typeSource,
  warnings = [],
  onExpand,
  onRefresh,
  onInsert,
  onPreview,
}: SchemaBrowserProps) {
  const [filter, setFilter] = useState("");
  const [open, setOpen] = useState<Record<string, boolean>>({});

  const shown = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return objects;
    // Match the object, or keep it when one of its loaded columns matches —
    // "which table has customer_ref?" is the question this answers.
    return objects.filter(
      (o) =>
        o.name.toLowerCase().includes(q) ||
        (o.columns ?? []).some((c) => c.name.toLowerCase().includes(q)),
    );
  }, [objects, filter]);

  const toggle = (o: SchemaObject) => {
    const next = !open[o.name];
    setOpen((prev) => ({ ...prev, [o.name]: next }));
    if (next && !(o.columns?.length ?? 0)) onExpand(o.name);
  };

  return (
    <aside className="df2-qw-schema" aria-label="Schema browser">
      <div className="df2-qw-schema-head">
        <span className="df2-qw-schema-title">
          <DtIcon name="database" size={13} /> Schema
        </span>
        <button
          type="button"
          className="df2-qw-icon-btn"
          onClick={onRefresh}
          disabled={loading}
          title="Reload objects"
          aria-label="Reload objects"
        >
          <DtIcon name={loading ? "spinner" : "refresh"} size={13} />
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

      {warnings.map((w) => (
        <p key={w} className="df2-qw-schema-warn">
          <DtIcon name="warning" size={12} /> {w}
        </p>
      ))}

      <div className="df2-qw-schema-tree">
        {loading && objects.length === 0 && (
          <p className="df2-qw-schema-empty">Loading objects…</p>
        )}
        {!loading && objects.length === 0 && !error && (
          <p className="df2-qw-schema-empty">
            {connected === false
              ? "Not connected."
              : "Select a connector to browse its objects."}
          </p>
        )}
        {shown.map((o) => {
          const isOpen = Boolean(open[o.name]);
          const isPending = pending.includes(o.name);
          const cols = o.columns ?? [];
          return (
            <div key={`${o.schema ?? ""}.${o.name}`} className="df2-qw-node">
              <div className="df2-qw-node-row">
                <button
                  type="button"
                  className="df2-qw-node-toggle"
                  onClick={() => toggle(o)}
                  aria-expanded={isOpen}
                  title={isOpen ? "Collapse" : "Show columns"}
                >
                  <DtIcon name={isOpen ? "chevron-down" : "chevron-right"} size={12} />
                  <span className="df2-qw-node-name" title={o.name}>
                    {o.name}
                  </span>
                </button>
                <span className="df2-qw-node-kind">{o.type || "table"}</span>
                <button
                  type="button"
                  className="df2-qw-icon-btn"
                  onClick={() => onPreview(o)}
                  title="Preview rows"
                  aria-label={`Preview rows from ${o.name}`}
                >
                  <DtIcon name="play" size={11} />
                </button>
              </div>

              {isOpen && (
                <div className="df2-qw-cols">
                  {isPending && <p className="df2-qw-schema-empty">Loading columns…</p>}
                  {!isPending && cols.length === 0 && (
                    <p className="df2-qw-schema-empty">No columns reported.</p>
                  )}
                  {cols.map((c) => (
                    <button
                      key={c.name}
                      type="button"
                      className="df2-qw-col"
                      onClick={() => onInsert(c.name)}
                      title={`Insert ${c.name}${c.type ? ` · ${c.type}` : ""}`}
                    >
                      <span className="df2-qw-col-name">
                        {c.primaryKey && (
                          <DtIcon name="key" size={10} aria-label="primary key" />
                        )}
                        {c.name}
                      </span>
                      <span className="df2-qw-col-type" data-tone={typeTone(c.type)}>
                        {c.type || "—"}
                      </span>
                      {/* Only assert NOT NULL when the catalog said so;
                          undefined nullability stays silent. */}
                      {c.nullable === false && (
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

      {objects.length > 0 && (
        <p className="df2-qw-schema-foot">
          {objects.length} object{objects.length === 1 ? "" : "s"}
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
