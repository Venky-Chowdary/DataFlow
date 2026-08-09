import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { Button } from "../components/ui/Button";
import { ConnectorSelect } from "../components/ui/ConnectorSelect";
import { EmptyState } from "../components/ui/EmptyState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { useToast } from "../components/Toast";
import {
  executeQuery,
  exportQuery,
  fetchQuerySchema,
  type QueryExportResult,
  type QueryResult,
} from "../lib/api";
import {
  firstExpanded,
  mergeExpandedObject,
  toSchemaObject,
} from "../lib/querySchema";
import { Connector } from "../lib/types";
import { QueryEditor, type QueryEditorHandle } from "../components/query/QueryEditor";
import { ResultGrid } from "../components/query/ResultGrid";
import { SchemaBrowser } from "../components/query/SchemaBrowser";
import {
  dialectForConnector,
  explainPrefix,
  limitSyntax,
  supportsExplain,
  type SchemaObject,
} from "../lib/sqlIntel";
import {
  closeTab as closeTabIn,
  createTab,
  duplicateTab as duplicateTabIn,
  filterHistory,
  formatDuration,
  formatRelativeTime,
  loadHistory,
  loadLayout,
  loadTabs,
  pushHistory,
  retitleTab,
  saveHistory,
  saveLayout,
  saveTabs,
  type QueryHistoryEntry,
  type QueryLayout,
  type QueryTab,
} from "../lib/queryWorkspace";

/**
 * Unified query workspace — one console for every connector family the engine
 * transfers, with schema-aware completion, multi-tab sessions, bound
 * parameters and type-honest results.
 *
 * Read-only by construction: the server refuses anything but SELECT-class
 * statements, and production movement stays in Transfer Studio where
 * Map → Validate → Execute produces the proof artifacts.
 */

interface QueryPageProps {
  connectors: Connector[];
}

const FORMATS = ["csv", "json", "jsonl", "tsv", "excel", "parquet"];
const LIMITS = [100, 500, 1000, 5000, 10000];

export function QueryPage({ connectors }: QueryPageProps) {
  const { toast } = useToast();

  const [tabs, setTabs] = useState<QueryTab[]>([]);
  const [activeId, setActiveId] = useState("");
  const [layout, setLayout] = useState<QueryLayout>(() => ({
    schemaOpen: true,
    historyOpen: false,
    editorHeight: 260,
  }));
  const [history, setHistory] = useState<QueryHistoryEntry[]>([]);
  const [historyTerm, setHistoryTerm] = useState("");
  const [renamingId, setRenamingId] = useState("");

  const [schemaObjects, setSchemaObjects] = useState<SchemaObject[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(false);
  const [schemaError, setSchemaError] = useState("");
  const [schemaPending, setSchemaPending] = useState<string[]>([]);
  const [schemaConnected, setSchemaConnected] = useState<boolean | undefined>(undefined);
  const [schemaTypeSource, setSchemaTypeSource] = useState("");
  const [schemaWarnings, setSchemaWarnings] = useState<string[]>([]);

  const [detectedParams, setDetectedParams] = useState<string[]>([]);
  const [queryLoading, setQueryLoading] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [lastRunText, setLastRunText] = useState("");
  const [exportResult, setExportResult] = useState<QueryExportResult | null>(null);
  const [exportOpen, setExportOpen] = useState(false);
  const [exportFormat, setExportFormat] = useState("csv");
  const [outputPath, setOutputPath] = useState("");
  const [destConnectorId, setDestConnectorId] = useState("");
  const [destTarget, setDestTarget] = useState("");
  const [destSyncMode, setDestSyncMode] = useState("append");

  // Hydrate from localStorage on mount only — reading during render would make
  // the first paint depend on storage and break SSR-style hydration.
  useEffect(() => {
    const loaded = loadTabs();
    setTabs(loaded.tabs);
    setActiveId(loaded.activeId);
    setHistory(loadHistory());
    setLayout(loadLayout());
  }, []);

  const active = useMemo(
    () => tabs.find((t) => t.id === activeId) ?? tabs[0],
    [tabs, activeId],
  );

  useEffect(() => {
    if (tabs.length) saveTabs(tabs, activeId);
  }, [tabs, activeId]);
  useEffect(() => {
    saveLayout(layout);
  }, [layout]);

  const patchActive = useCallback(
    (patch: Partial<QueryTab>) => {
      setTabs((prev) =>
        prev.map((t) =>
          t.id === activeId ? retitleTab({ ...t, ...patch, updatedAt: Date.now() }) : t,
        ),
      );
    },
    [activeId],
  );

  const selected = useMemo(
    () => connectors.find((c) => c.id === active?.connectorId),
    [connectors, active?.connectorId],
  );
  const isMongo = selected?.type === "mongodb";
  const dialect = useMemo(() => dialectForConnector(selected?.type), [selected?.type]);
  const queryPlaceholder = isMongo
    ? '{"status": "active"}  or  [{ "$match": { ... } }]'
    : "SELECT * FROM users LIMIT 100";

  // ---------------------------------------------------------------- schema ---

  const schemaSeq = useRef(0);

  const loadObjects = useCallback(
    async (connectorId: string, database: string) => {
      // Introspection latency varies per engine, so a slow earlier response can
      // land after a newer one. Only the newest request may write state.
      const seq = ++schemaSeq.current;
      const isCurrent = () => seq === schemaSeq.current;
      if (!connectorId) {
        setSchemaObjects([]);
        setSchemaConnected(undefined);
        setSchemaError("");
        return;
      }
      setSchemaLoading(true);
      setSchemaError("");
      try {
        const data = await fetchQuerySchema({ connector_id: connectorId, database });
        if (!isCurrent()) return;
        setSchemaObjects(data.objects.map(toSchemaObject));
        setSchemaConnected(data.connected);
        setSchemaTypeSource(data.type_source ?? "");
        setSchemaWarnings(data.warnings ?? []);
      } catch (e) {
        if (!isCurrent()) return;
        setSchemaObjects([]);
        setSchemaError((e as Error).message || "Schema introspection failed");
        setSchemaConnected(false);
      } finally {
        if (isCurrent()) setSchemaLoading(false);
      }
    },
    [],
  );

  const activeConnectorId = active?.connectorId ?? "";
  const activeDatabase = active?.database ?? "";
  useEffect(() => {
    // Debounced: the database field is typed into, and introspection opens a
    // real connection — one per keystroke would hammer the source.
    const t = window.setTimeout(() => {
      void loadObjects(activeConnectorId, activeDatabase);
    }, 350);
    return () => window.clearTimeout(t);
  }, [activeConnectorId, activeDatabase, loadObjects]);

  const expandObject = useCallback(
    async (objectName: string) => {
      if (!active?.connectorId) return;
      setSchemaPending((p) => [...p, objectName]);
      try {
        const data = await fetchQuerySchema({
          connector_id: active.connectorId,
          database: active.database,
          object_name: objectName,
        });
        setSchemaObjects((prev) =>
          mergeExpandedObject(prev, objectName, firstExpanded(data.objects)),
        );
        if (data.warnings?.length) setSchemaWarnings(data.warnings);
      } catch (e) {
        toast({
          title: "Could not load columns",
          message: (e as Error).message,
          tone: "error",
        });
      } finally {
        setSchemaPending((p) => p.filter((n) => n !== objectName));
      }
    },
    [active?.connectorId, active?.database, toast],
  );

  // ------------------------------------------------------------------- run ---

  const runSeq = useRef(0);

  const paramsForRequest = useMemo(() => {
    const out: Record<string, string> = {};
    for (const name of detectedParams) out[name] = active?.params?.[name] ?? "";
    return out;
  }, [detectedParams, active?.params]);

  const missingParams = useMemo(
    () => detectedParams.filter((p) => (active?.params?.[p] ?? "") === ""),
    [detectedParams, active?.params],
  );

  const runText = useCallback(
    async (text: string, scope: "selection" | "statement" | "all") => {
      if (!active?.connectorId) {
        toast({ title: "Select a connector", tone: "warning" });
        return;
      }
      if (!text.trim()) {
        toast({ title: "Nothing to run", tone: "warning" });
        return;
      }
      // Same ordering hazard as introspection: a slow first result must not
      // overwrite a newer one and mislabel which statement produced it.
      const seq = ++runSeq.current;
      const isCurrent = () => seq === runSeq.current;
      setQueryLoading(true);
      setResult(null);
      setExportResult(null);
      const startedAt = Date.now();
      try {
        const data = await executeQuery({
          connector_id: active.connectorId,
          query: text.trim(),
          database: active.database,
          collection: active.collection,
          limit: active.limit,
          // Values stay bound server-side; the console never builds SQL text
          // out of operator input.
          params: paramsForRequest,
        });
        if (isCurrent()) {
          setResult(data);
          setLastRunText(text.trim());
        }
        setHistory((prev) => {
          const next = pushHistory(prev, {
            query: text.trim(),
            connectorId: active.connectorId,
            connectorLabel: selected?.name,
            database: active.database,
            durationMs: data.duration_ms,
            rowCount: data.row_count,
            truncated: data.truncated,
            ok: true,
          });
          saveHistory(next);
          return next;
        });
        if (scope !== "all" && isCurrent()) {
          toast({
            title: `Ran ${scope}`,
            message: `${data.row_count.toLocaleString()} rows in ${formatDuration(data.duration_ms)}`,
            tone: "success",
          });
        }
      } catch (e) {
        const message = (e as Error).message || "Query failed";
        setHistory((prev) => {
          const next = pushHistory(prev, {
            query: text.trim(),
            connectorId: active.connectorId,
            connectorLabel: selected?.name,
            database: active.database,
            durationMs: Date.now() - startedAt,
            ok: false,
            error: message,
          });
          saveHistory(next);
          return next;
        });
        const dialectHint = /dialect|pymysql|psycopg|driver/i.test(message);
        toast({
          title: "Query failed",
          message: dialectHint
            ? `${message} Check that the ${selected?.type || "SQL"} connector driver is installed in the API environment.`
            : message,
          tone: "error",
        });
      } finally {
        if (isCurrent()) setQueryLoading(false);
      }
    },
    [active, paramsForRequest, selected?.name, selected?.type, toast],
  );

  const runAll = useCallback(() => {
    void runText(active?.query ?? "", "all");
  }, [runText, active?.query]);

  const runExplain = useCallback(() => {
    const q = (active?.query ?? "").trim();
    if (!q) return;
    if (!supportsExplain(dialect)) {
      toast({
        title: "Plans not available here",
        message: `${selected?.type ?? "This engine"} does not expose a plan through a read-only statement.`,
        tone: "warning",
      });
      return;
    }
    void runText(`${explainPrefix(dialect)}${q}`, "all");
  }, [active?.query, dialect, runText, selected?.type, toast]);

  const runExport = async () => {
    const text = (lastRunText || active?.query || "").trim();
    if (!active?.connectorId || !text) {
      toast({ title: "Run a query first", tone: "warning" });
      return;
    }
    setExportLoading(true);
    try {
      const data = await exportQuery({
        connector_id: active.connectorId,
        query: text,
        database: active.database,
        collection: active.collection,
        limit: active.limit,
        format: exportFormat,
        output_path: outputPath,
        destination_connector_id: destConnectorId || undefined,
        destination: destTarget || undefined,
        sync_mode: destSyncMode,
      });
      setExportResult(data);
      if (data.success) {
        toast({
          title: "Export ready",
          message: `${data.row_count?.toLocaleString() ?? 0} rows exported`,
          tone: "success",
        });
      } else {
        toast({ title: "Export failed", message: data.error || "Unknown error", tone: "error" });
      }
    } catch (e) {
      toast({ title: "Export failed", message: (e as Error).message, tone: "error" });
    } finally {
      setExportLoading(false);
    }
  };

  // ------------------------------------------------------------------ tabs ---

  const addTab = () => {
    const fresh = createTab({
      connectorId: active?.connectorId ?? "",
      database: active?.database ?? "",
      limit: active?.limit ?? 1000,
    });
    setTabs((prev) => [...prev, fresh]);
    setActiveId(fresh.id);
    setResult(null);
  };

  const closeTab = (id: string) => {
    const next = closeTabIn(tabs, activeId, id);
    setTabs(next.tabs);
    setActiveId(next.activeId);
    if (id === activeId) setResult(null);
  };

  const duplicate = () => {
    if (!active) return;
    const next = duplicateTabIn(tabs, active.id);
    setTabs(next.tabs);
    setActiveId(next.activeId);
  };

  const commitRename = (id: string, name: string) => {
    setRenamingId("");
    const trimmed = name.trim();
    if (!trimmed) return;
    setTabs((prev) =>
      prev.map((t) => (t.id === id ? { ...t, title: trimmed, titlePinned: true } : t)),
    );
  };

  // -------------------------------------------------------------- insertion ---

  const editorRef = useRef<QueryEditorHandle>(null);
  const insertIntoQuery = useCallback((text: string) => {
    editorRef.current?.insertAtCaret(text);
  }, []);

  const previewObject = useCallback(
    (obj: SchemaObject) => {
      const q = isMongo
        ? "{}"
        : `SELECT *\nFROM ${obj.name}\n${limitSyntax(dialect, Math.min(active?.limit ?? 100, 100))}`;
      patchActive(isMongo ? { query: q, collection: obj.name } : { query: q });
      void runText(q, "all");
    },
    [isMongo, dialect, active?.limit, patchActive, runText],
  );

  const shownHistory = useMemo(
    () => filterHistory(history, historyTerm),
    [history, historyTerm],
  );

  if (connectors.length === 0) {
    return (
      <PageShell
        wide
        className="df2-page-query"
        title="Query"
        kicker="Operations"
        description="Unified read-only console across every connector family — schema-aware, type-honest, multi-tab."
      >
        <PageFrame className="df2-query-page">
          <EmptyState
            page
            icon="search"
            title="Add a connector to run queries"
            description="Save a PostgreSQL, MySQL, MongoDB, or warehouse connection first — then run read-only SQL or aggregation pipelines and export results."
            action={<p className="df2-label-hint">Open Connectors from the sidebar to browse the catalog.</p>}
          />
        </PageFrame>
      </PageShell>
    );
  }

  return (
    <PageShell
      wide
      className="df2-page-query"
      title="Query"
      kicker="Operations"
      description="Unified read-only console across every connector family — schema-aware, type-honest, multi-tab."
    >
      <PageFrame className="df2-query-page df2-qw">
        {/* ---------------------------------------------------- tab strip --- */}
        <div className="df2-qw-tabs" role="tablist" aria-label="Query tabs">
          {tabs.map((t) => (
            <div
              key={t.id}
              className="df2-qw-tab"
              data-active={t.id === activeId}
              role="tab"
              aria-selected={t.id === activeId}
            >
              {renamingId === t.id ? (
                <input
                  className="df2-qw-tab-rename"
                  defaultValue={t.title}
                  autoFocus
                  aria-label="Tab name"
                  onBlur={(e) => commitRename(t.id, e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") commitRename(t.id, e.currentTarget.value);
                    if (e.key === "Escape") setRenamingId("");
                  }}
                />
              ) : (
                <button
                  type="button"
                  className="df2-qw-tab-btn"
                  onClick={() => {
                    setActiveId(t.id);
                    setResult(null);
                  }}
                  onDoubleClick={() => setRenamingId(t.id)}
                  title={t.query.slice(0, 200) || "Empty query"}
                >
                  {t.title}
                </button>
              )}
              <button
                type="button"
                className="df2-qw-tab-close"
                onClick={() => closeTab(t.id)}
                aria-label={`Close ${t.title}`}
              >
                <DtIcon name="x" size={11} />
              </button>
            </div>
          ))}
          <button type="button" className="df2-qw-tab-add" onClick={addTab} title="New tab">
            <DtIcon name="plus" size={13} />
          </button>
          <div className="df2-qw-tabs-right">
            <button type="button" className="df2-qw-chip" onClick={duplicate} title="Duplicate tab">
              Duplicate
            </button>
            <button
              type="button"
              className="df2-qw-chip"
              data-active={layout.schemaOpen}
              onClick={() => setLayout((l) => ({ ...l, schemaOpen: !l.schemaOpen }))}
            >
              <DtIcon name="panel-left" size={11} /> Schema
            </button>
            <button
              type="button"
              className="df2-qw-chip"
              data-active={layout.historyOpen}
              onClick={() => setLayout((l) => ({ ...l, historyOpen: !l.historyOpen }))}
            >
              <DtIcon name="clock" size={11} /> History
            </button>
          </div>
        </div>

        <div className="df2-qw-body" data-schema={layout.schemaOpen} data-history={layout.historyOpen}>
          {layout.schemaOpen && (
            <SchemaBrowser
              objects={schemaObjects}
              loading={schemaLoading}
              error={schemaError}
              pending={schemaPending}
              connected={schemaConnected}
              typeSource={schemaTypeSource}
              warnings={schemaWarnings}
              onExpand={(name) => void expandObject(name)}
              onRefresh={() =>
                void loadObjects(active?.connectorId ?? "", active?.database ?? "")
              }
              onInsert={insertIntoQuery}
              onPreview={previewObject}
            />
          )}

          <div className="df2-qw-main">
            {/* ------------------------------------------------ connector --- */}
            <div className="df2-qw-meta">
              <div className="df2-field-flex">
                <ConnectorSelect
                  id="query-connector"
                  label="Connector"
                  value={active?.connectorId ?? ""}
                  onChange={(id) => patchActive({ connectorId: id })}
                  connectors={connectors}
                  placeholder="Select a saved connector…"
                />
              </div>
              <div className="df2-field df2-field-md">
                <label className="df2-label">{isMongo ? "Database" : "Database / Schema"}</label>
                <input
                  className="df2-input"
                  value={active?.database ?? ""}
                  onChange={(e) => patchActive({ database: e.target.value })}
                  placeholder={isMongo ? "mydb" : "public"}
                />
              </div>
              <div className="df2-field df2-field-md">
                <label className="df2-label">{isMongo ? "Collection" : "Table (optional)"}</label>
                <input
                  className="df2-input"
                  value={active?.collection ?? ""}
                  onChange={(e) => patchActive({ collection: e.target.value })}
                  placeholder="users"
                />
              </div>
              <div className="df2-field df2-field-sm">
                <label className="df2-label">Row limit</label>
                <select
                  className="df2-input"
                  value={active?.limit ?? 1000}
                  onChange={(e) => patchActive({ limit: Number(e.target.value) })}
                >
                  {LIMITS.map((l) => (
                    <option key={l} value={l}>
                      {l.toLocaleString()}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {/* --------------------------------------------------- editor --- */}
            <QueryEditor
              ref={editorRef}
              value={active?.query ?? ""}
              onChange={(q) => patchActive({ query: q })}
              connectorType={selected?.type}
              placeholder={queryPlaceholder}
              disabled={queryLoading || exportLoading}
              height={`${layout.editorHeight}px`}
              schemaObjects={schemaObjects}
              onRun={(text, scope) => void runText(text, scope)}
              onRunAll={runAll}
              onExplain={runExplain}
              onParamsChange={setDetectedParams}
              busy={queryLoading}
            />

            {/* ----------------------------------------------- parameters --- */}
            {detectedParams.length > 0 && (
              <div className="df2-qw-params">
                <span className="df2-qw-params-label">
                  <DtIcon name="key" size={12} /> Bind parameters
                </span>
                {detectedParams.map((name) => (
                  <label key={name} className="df2-qw-param">
                    <span>:{name}</span>
                    <input
                      className="df2-input df2-input-sm"
                      value={active?.params?.[name] ?? ""}
                      onChange={(e) =>
                        patchActive({
                          params: { ...(active?.params ?? {}), [name]: e.target.value },
                        })
                      }
                      placeholder="value"
                    />
                  </label>
                ))}
                <span className="df2-qw-params-note">
                  Sent as bound values — never spliced into the statement text.
                </span>
              </div>
            )}

            <div className="df2-qw-runbar">
              <Button
                variant="primary"
                loading={queryLoading}
                loadingLabel="Running…"
                disabled={exportLoading || !(active?.query ?? "").trim()}
                onClick={runAll}
                leadingIcon={<DtIcon name="play" size={14} />}
              >
                Run all
              </Button>
              {missingParams.length > 0 && (
                <span className="df2-qw-warn">
                  <DtIcon name="warning" size={12} /> {missingParams.map((p) => `:${p}`).join(", ")}{" "}
                  {missingParams.length === 1 ? "has" : "have"} no value yet — sent as empty.
                </span>
              )}
              {result && (
                <span className="df2-qw-runstat">
                  {result.row_count.toLocaleString()} rows · {formatDuration(result.duration_ms)}
                  {result.truncated && " · truncated"}
                </span>
              )}
              <div className="df2-qw-runbar-right">
                <button
                  type="button"
                  className="df2-qw-chip"
                  data-active={exportOpen}
                  onClick={() => setExportOpen((v) => !v)}
                  disabled={!result}
                  title={result ? "Export these results" : "Run a query first"}
                >
                  <DtIcon name="download" size={11} /> Export
                </button>
              </div>
            </div>

            {/* --------------------------------------------------- export --- */}
            {exportOpen && (
              <div className="df2-qw-export">
                <div className="df2-qw-export-note">
                  <DtIcon name="info" size={14} />
                  <span>
                    Export re-runs this query server-side and writes the result. For production
                    movement use Transfer Studio — Map → Validate → Execute is what produces
                    reconciliation proof.
                  </span>
                </div>
                <div className="df2-qw-export-row">
                  <ConnectorSelect
                    id="query-destination-connector"
                    label="Destination (optional)"
                    value={destConnectorId}
                    onChange={setDestConnectorId}
                    connectors={connectors}
                    placeholder="File export"
                  />
                  {destConnectorId ? (
                    <>
                      <input
                        className="df2-input df2-input-sm"
                        value={destTarget}
                        onChange={(e) => setDestTarget(e.target.value)}
                        placeholder="Table / collection / object name"
                      />
                      <select
                        className="df2-input df2-input-sm"
                        value={destSyncMode}
                        onChange={(e) => setDestSyncMode(e.target.value)}
                      >
                        <option value="append">Append</option>
                        <option value="upsert">Upsert</option>
                        <option value="overwrite">Overwrite</option>
                      </select>
                    </>
                  ) : (
                    <>
                      <select
                        className="df2-input df2-input-sm"
                        value={exportFormat}
                        onChange={(e) => setExportFormat(e.target.value)}
                      >
                        {FORMATS.map((f) => (
                          <option key={f} value={f}>
                            {f.toUpperCase()}
                          </option>
                        ))}
                      </select>
                      <input
                        className="df2-input df2-input-sm"
                        value={outputPath}
                        onChange={(e) => setOutputPath(e.target.value)}
                        placeholder="Output path (optional)"
                      />
                    </>
                  )}
                  <Button
                    size="sm"
                    variant="secondary"
                    loading={exportLoading}
                    loadingLabel="Exporting…"
                    disabled={queryLoading}
                    onClick={() => void runExport()}
                    leadingIcon={<DtIcon name="download" size={14} />}
                  >
                    {destConnectorId ? "Write to connector" : "Export file"}
                  </Button>
                </div>
                {exportResult?.success && (
                  <div className="df2-alert df2-alert-success" role="alert">
                    <DtIcon name="check" size={16} />
                    <div>
                      <strong>
                        {exportResult.download_url ? "Export ready" : "Export complete"}
                      </strong>
                      <p>
                        {(exportResult.row_count ?? 0).toLocaleString()} rows{" "}
                        {exportResult.download_url
                          ? `exported as ${exportResult.format?.toUpperCase()}`
                          : `written to ${exportResult.format}${exportResult.filename ? ` · ${exportResult.filename}` : ""}`}
                        .
                      </p>
                      {exportResult.download_url && (
                        <a
                          className="df2-btn df2-btn-primary df2-btn-sm"
                          href={exportResult.download_url}
                          download={exportResult.filename}
                        >
                          <DtIcon name="download" size={14} /> Download {exportResult.filename}
                        </a>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* -------------------------------------------------- results --- */}
            {result ? (
              <ResultGrid
                columns={result.columns}
                rows={result.rows}
                columnSchema={result.column_schema}
                typeSource={result.column_type_source}
                truncated={result.truncated}
                durationMs={result.duration_ms}
                onCopied={(what) => toast({ title: "Copied", message: what, tone: "success" })}
              />
            ) : (
              <div className="df2-qw-placeholder">
                <DtIcon name="play" size={16} />
                <span>
                  Run a query to see results. Ctrl/Cmd+Enter runs the statement under the caret,
                  Ctrl/Cmd+Space completes against the live schema.
                </span>
              </div>
            )}
          </div>

          {/* -------------------------------------------------- history --- */}
          {layout.historyOpen && (
            <aside className="df2-qw-history" aria-label="Query history">
              <div className="df2-qw-schema-head">
                <span className="df2-qw-schema-title">
                  <DtIcon name="clock" size={13} /> History
                </span>
                <button
                  type="button"
                  className="df2-qw-icon-btn"
                  onClick={() => {
                    setHistory([]);
                    saveHistory([]);
                  }}
                  title="Clear history"
                  aria-label="Clear history"
                >
                  <DtIcon name="trash" size={13} />
                </button>
              </div>
              <input
                className="df2-input df2-input-sm df2-qw-schema-filter"
                value={historyTerm}
                onChange={(e) => setHistoryTerm(e.target.value)}
                placeholder="Search history…"
                aria-label="Search history"
              />
              <div className="df2-qw-history-list">
                {shownHistory.length === 0 && (
                  <p className="df2-qw-schema-empty">
                    {history.length === 0 ? "No runs yet." : "No matches."}
                  </p>
                )}
                {shownHistory.map((h) => (
                  <button
                    key={h.id}
                    type="button"
                    className="df2-qw-history-item"
                    data-ok={h.ok}
                    onClick={() => patchActive({ query: h.query })}
                    title={h.error || h.query}
                  >
                    <code>{h.query.replace(/\s+/g, " ").slice(0, 90)}</code>
                    <span className="df2-qw-history-meta">
                      {h.ok
                        ? `${(h.rowCount ?? 0).toLocaleString()} rows · ${formatDuration(h.durationMs)}`
                        : "failed"}
                      {" · "}
                      {formatRelativeTime(h.at)}
                    </span>
                  </button>
                ))}
              </div>
              <p className="df2-qw-schema-foot">
                Stored in this browser only — not an audit trail. Credential-looking parameter
                values are never persisted.
              </p>
            </aside>
          )}
        </div>
      </PageFrame>
    </PageShell>
  );
}
