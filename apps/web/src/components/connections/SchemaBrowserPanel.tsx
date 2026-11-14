import { Fragment, useCallback, useEffect, useId, useRef, useState } from "react";

import { fetchQuerySchema, type QuerySchemaColumnInfo } from "../../lib/api";
import { fetchSchemaNames, listingCountLabel } from "../../lib/schemaBrowse";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { EmptyState } from "../ui/EmptyState";

type Listed = { name: string; type: string };
type ColumnsState = { loading: boolean; error: string; columns: QuerySchemaColumnInfo[] };

const SEARCH_DEBOUNCE_MS = 300;

/**
 * Connection workbench schema browser (UX-9): a schema selector, a search
 * that runs against the whole schema on the server, an honest "N of M", and
 * columns loaded only for the table a person opens.
 */
export function SchemaBrowserPanel({ connector }: { connector: { id: string; name: string; type: string } }) {
  const noun = connector.type === "mongodb" ? "collection" : "table";
  const [schemas, setSchemas] = useState<string[]>([]);
  const [schema, setSchema] = useState("");
  const [schemasReady, setSchemasReady] = useState(false);
  const [search, setSearch] = useState("");
  const [query, setQuery] = useState("");
  const [objects, setObjects] = useState<Listed[]>([]);
  const [total, setTotal] = useState<number | null>(null);
  const [truncated, setTruncated] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [columns, setColumns] = useState<Record<string, ColumnsState>>({});
  const generation = useRef(0);
  const schemaId = useId();
  const searchId = useId();

  useEffect(() => {
    let cancelled = false;
    setSchemasReady(false);
    setSchemas([]);
    setSchema("");
    fetchSchemaNames(connector.id)
      .then(({ schemas: names, current }) => {
        if (cancelled) return;
        setSchemas(names);
        setSchema(current && names.includes(current) ? current : names[0] ?? current);
      })
      .catch(() => {
        // The listing still works on the connector's configured schema.
      })
      .finally(() => {
        if (!cancelled) setSchemasReady(true);
      });
    return () => {
      cancelled = true;
    };
  }, [connector.id]);

  useEffect(() => {
    const t = window.setTimeout(() => setQuery(search.trim()), SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(t);
  }, [search]);

  const load = useCallback(async () => {
    const gen = ++generation.current;
    setLoading(true);
    setError("");
    setColumns({});
    try {
      const res = await fetchQuerySchema({
        connector_id: connector.id,
        schema_name: schema || undefined,
        search: query || undefined,
      });
      if (gen !== generation.current) return;
      setObjects(res.objects.map((o) => ({ name: o.name, type: o.type || noun })));
      setTotal(typeof res.objects_total === "number" ? res.objects_total : null);
      setTruncated(Boolean(res.objects_truncated));
      if (!res.objects.length && !query && res.message && !res.connected) setError(res.message);
    } catch (e) {
      if (gen !== generation.current) return;
      setObjects([]);
      setTotal(null);
      setTruncated(false);
      setError(e instanceof Error ? e.message : "The schema could not be read.");
    } finally {
      if (gen === generation.current) setLoading(false);
    }
  }, [connector.id, schema, query, noun]);

  useEffect(() => {
    if (schemasReady) void load();
  }, [schemasReady, load]);

  const toggleColumns = async (name: string) => {
    if (columns[name]) {
      setColumns((prev) => {
        const next = { ...prev };
        delete next[name];
        return next;
      });
      return;
    }
    setColumns((prev) => ({ ...prev, [name]: { loading: true, error: "", columns: [] } }));
    try {
      const res = await fetchQuerySchema({ connector_id: connector.id, schema_name: schema || undefined, object_name: name });
      const hit = res.objects.find((o) => o.columns.length) ?? res.objects.find((o) => o.name === name);
      setColumns((prev) => ({ ...prev, [name]: { loading: false, error: "", columns: hit?.columns ?? [] } }));
    } catch (e) {
      setColumns((prev) => ({
        ...prev,
        [name]: { loading: false, error: e instanceof Error ? e.message : "Columns could not be read.", columns: [] },
      }));
    }
  };

  const countLabel = listingCountLabel(objects.length, total, noun);

  return (
    <div className="df2-schema-live" data-testid="schema-browser">
      <div className="df2-schema-live-toolbar">
        <div>
          <span className="df2-rail-kicker">Live introspection</span>
          <h4>{connector.name}</h4>
        </div>
        <Button variant="secondary" size="sm" onClick={() => void load()} disabled={loading}>
          {loading ? "Refreshing…" : "Refresh schema"}
        </Button>
      </div>

      <div className="df2-schema-browse-controls">
        {schemas.length > 0 && (
          <div className="df2-field">
            <label className="df2-label" htmlFor={schemaId}>
              Schema
            </label>
            <select id={schemaId} className="df2-input" value={schema} onChange={(e) => setSchema(e.target.value)}>
              {schemas.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
        )}
        <div className="df2-field df2-schema-browse-search">
          <label className="df2-label" htmlFor={searchId}>
            Search {noun}s
          </label>
          <input
            id={searchId}
            className="df2-input"
            type="search"
            value={search}
            placeholder={`Name contains…`}
            onChange={(e) => setSearch(e.target.value)}
            autoComplete="off"
            spellCheck={false}
          />
        </div>
      </div>

      <p className="df2-schema-browse-count" role="status" aria-live="polite" data-testid="schema-browser-count">
        {loading
          ? `Searching ${schema || "the schema"}…`
          : error
            ? ""
            : `${countLabel}${query ? ` matching “${query}”` : ""}${schema ? ` in ${schema}` : ""}.${
                truncated ? " Search to narrow the list." : ""
              }`}
      </p>

      {error && !objects.length ? (
        <EmptyState compact icon="alert" title="Schema unavailable" description={error} />
      ) : !loading && objects.length === 0 ? (
        <EmptyState
          compact
          icon="database"
          title={query ? `No ${noun}s match “${query}”` : `No ${noun}s in this schema`}
          description={query ? "Search matches any part of the name, in the selected schema." : "Choose another schema, or check the connection's database name."}
        />
      ) : (
        <div className="df2-table-wrap">
          <table className="df2-table" aria-label={`${connector.name} ${noun}s`}>
            <thead>
              <tr>
                <th scope="col">Name</th>
                <th scope="col">Kind</th>
                <th scope="col">Columns</th>
              </tr>
            </thead>
            <tbody>
              {objects.map((obj) => {
                const state = columns[obj.name];
                const panelId = `schema-cols-${obj.name.replace(/[^A-Za-z0-9_-]/g, "_")}`;
                return (
                  <Fragment key={obj.name}>
                    <tr>
                      <td>
                        <strong>{obj.name}</strong>
                      </td>
                      <td>{obj.type}</td>
                      <td>
                        <button
                          type="button"
                          className="df2-btn df2-btn-ghost df2-btn-sm"
                          aria-expanded={Boolean(state)}
                          aria-controls={state ? panelId : undefined}
                          onClick={() => void toggleColumns(obj.name)}
                        >
                          <DtIcon name={state ? "chevron-up" : "chevron-down"} size={12} />
                          {state ? "Hide columns" : "Show columns"}
                        </button>
                      </td>
                    </tr>
                    {state && (
                      <tr id={panelId} className="df2-schema-browse-columns">
                        <td colSpan={3}>
                          {state.loading ? (
                            <span className="df2-muted">Reading columns…</span>
                          ) : state.error ? (
                            <span className="df2-field-error-text">{state.error}</span>
                          ) : state.columns.length === 0 ? (
                            <span className="df2-muted">No columns reported.</span>
                          ) : (
                            <ul aria-label={`Columns of ${obj.name}`}>
                              {state.columns.map((c) => (
                                <li key={c.name}>
                                  <code>{c.name}</code> <span className="df2-muted">{c.type || "type not reported"}</span>
                                  {c.primary_key ? <span className="df2-badge df2-badge-muted">key</span> : null}
                                </li>
                              ))}
                            </ul>
                          )}
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
