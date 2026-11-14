import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent as ReactKeyboardEvent, type PointerEvent as ReactPointerEvent } from "react";
import { DtIcon } from "../components/DtIcon";
import { Button } from "../components/ui/Button";
import { ConnectorSelect } from "../components/ui/ConnectorSelect";
import { EmptyState } from "../components/ui/EmptyState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { useToast } from "../components/Toast";
import {
  executeQuery,
  downloadExportFile,
  exportQuery,
  fetchQuerySchema,
  type QueryExportResult,
  type QueryResult,
} from "../lib/api";
import { formulaGuardApplies } from "../lib/csvSafety";
import { QUERY_EXPORT_FORMULA_PROTECTION_HELP } from "../lib/fileExportDestination";
import { useQueryExportOptions } from "../lib/exportOptions";
import { FormulaProtectionToggle } from "../components/export/FormulaProtectionToggle";
import { TsvNullMarkerField } from "../components/transfer/TsvNullMarkerField";
import {
  firstExpanded,
  mergeExpandedObject,
  toSchemaObject,
} from "../lib/querySchema";
import { Connector } from "../lib/types";
import { QueryEditor, type QueryEditorHandle } from "../components/query/QueryEditor";
import { ResultGrid } from "../components/query/ResultGrid";
import { SchemaBrowser } from "../components/query/SchemaBrowser";
import { useStickyHeader } from "../components/query/useStickyHeader";
import { useFillHeight } from "../components/query/useFillHeight";
import { QueryErrorPanel } from "../components/query/QueryErrorPanel";
import {
  dialectForConnector,
  explainPrefix,
  limitSyntax,
  supportsExplain,
  type SchemaObject,
} from "../lib/sqlIntel";
import {
  clampEditorSplit,
  closeTab as closeTabIn,
  editedSinceRun,
  MIN_EDITOR_PANE,
  queryFailureCopy,
  connectorMissingFromWorkspace,
  createTab,
  duplicateTab as duplicateTabIn,
  filterHistory,
  formatDuration,
  formatRelativeTime,
  loadHistory,
  loadLayout,
  loadTabs,
  playgroundQueryBody,
  pushHistory,
  querySchemaErrorCopy,
  schemaNotConnectedReason,
  retitleTab,
  saveHistory,
  saveLayout,
  saveTabs,
  type QueryFailureCopy,
  type QueryHistoryEntry,
  type QueryLayout,
  type QueryTab,
} from "../lib/queryWorkspace";
import { useActiveWorkspaceId } from "../lib/workspace";

/**
 * Unified query workspace — one console for the queryable connector families the engine
 * transfers, with schema-aware completion, multi-tab sessions, bound
 * parameters and type-honest results.
 *
 * Read-only by construction: the server refuses anything but SELECT-class
 * statements, and production movement stays in Transfer Studio where
 * Map → Validate → Execute produces the proof artifacts.
 */

interface QueryPageProps {
  connectors: Connector[];
  connectorsLoading?: boolean;
}

const FORMATS = ["csv", "json", "jsonl", "tsv", "excel", "parquet"];
const LIMITS = [100, 500, 1000, 5000, 10000];

const MOD_KEY = typeof navigator !== "undefined" && /Mac|iP(hone|ad)/.test(navigator.platform) ? "⌘" : "Ctrl";
/** The three-zone workspace (schema | editor over results) needs this width. */
const WIDE_QUERY = "(min-width: 1024px)";

function useWideLayout(): boolean {
  const [wide, setWide] = useState(() =>
    typeof window === "undefined" || typeof window.matchMedia !== "function" ? true : window.matchMedia(WIDE_QUERY).matches,
  );
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return;
    const mq = window.matchMedia(WIDE_QUERY);
    const on = () => setWide(mq.matches);
    on();
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, []);
  return wide;
}

/** Elapsed time of the run in flight; ticks on its own so the page does not re-render. */
function RunElapsed({ startedAt }: { startedAt: number }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 100);
    return () => window.clearInterval(id);
  }, []);
  const seconds = Math.max(0, now - startedAt) / 1000;
  return <span className="df2-qw-elapsed">{seconds < 60 ? `${seconds.toFixed(1)} s` : `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`}</span>;
}

export function QueryPage({ connectors, connectorsLoading = false }: QueryPageProps) {
  const { toast } = useToast();
  const workspaceId = useActiveWorkspaceId();

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
  const [schemaTruncated, setSchemaTruncated] = useState(false);

  const [detectedParams, setDetectedParams] = useState<string[]>([]);
  const [queryLoading, setQueryLoading] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [resultLimit, setResultLimit] = useState(0);
  const [runError, setRunError] = useState<QueryFailureCopy | null>(null);
  const [runStartedAt, setRunStartedAt] = useState(0);
  const [liveMessage, setLiveMessage] = useState("");
  const [runNotice, setRunNotice] = useState("");
  const [schemaDrawerOpen, setSchemaDrawerOpen] = useState(false);
  const [lastRunText, setLastRunText] = useState("");
  const [exportResult, setExportResult] = useState<QueryExportResult | null>(null);
  const [downloading, setDownloading] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const [exportFormat, setExportFormat] = useState("csv");
  // G7-F2: "Protect against spreadsheet formulas" — on by default, as the API.
  // G7-F2 / G7-F3: "Protect against spreadsheet formulas" (on by default; it also
  // governs Copy as CSV) and the TSV NULL marker.
  const exportOptions = useQueryExportOptions();
  const [outputPath, setOutputPath] = useState("");
  const [destConnectorId, setDestConnectorId] = useState("");
  const [destTarget, setDestTarget] = useState("");
  const [destSyncMode, setDestSyncMode] = useState("append");

  const skipTabPersist = useRef(true);
  const sticky = useStickyHeader<HTMLDivElement>();
  const fillRef = useFillHeight<HTMLDivElement>();
  const wide = useWideLayout();
  const runAbort = useRef<AbortController | null>(null);

  // Hydrate tabs/history per workspace. Do not persist the previous workspace's
  // tabs into the next workspace's storage key on the same tick.
  useEffect(() => {
    skipTabPersist.current = true;
    const loaded = loadTabs(workspaceId);
    setTabs(loaded.tabs);
    setActiveId(loaded.activeId);
    setHistory(loadHistory(workspaceId));
    setLayout(loadLayout());
    setSchemaObjects([]);
    setSchemaTruncated(false);
    setSchemaError("");
    setResult(null);
    setRunError(null);
  }, [workspaceId]);

  const active = useMemo(
    () => tabs.find((t) => t.id === activeId) ?? tabs[0],
    [tabs, activeId],
  );

  useEffect(() => {
    if (skipTabPersist.current) {
      skipTabPersist.current = false;
      return;
    }
    if (tabs.length) saveTabs(tabs, activeId, workspaceId);
  }, [tabs, activeId, workspaceId]);
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
  useEffect(() => {
    if (connectorsLoading) return;
    if (!active?.connectorId) return;
    if (connectorMissingFromWorkspace(active.connectorId, connectors)) {
      patchActive({ connectorId: "" });
      setSchemaError("");
      setSchemaObjects([]);
    }
  }, [connectorsLoading, connectors, active?.connectorId, patchActive]);
  const isMongo = selected?.type === "mongodb";
  const dialect = useMemo(() => dialectForConnector(selected?.type), [selected?.type]);
  const queryPlaceholder = isMongo
    ? '{"status": "active"}  or  [{ "$match": { ... } }]'
    : "-- Write a read-only query, e.g. SELECT * FROM users LIMIT 100";

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
        setSchemaTruncated(false);
        setSchemaConnected(undefined);
        setSchemaError("");
        return;
      }
      if (connectorMissingFromWorkspace(connectorId, connectors)) {
        setSchemaObjects([]);
        setSchemaConnected(false);
        setSchemaError("This connector is not in the current workspace. Choose a saved connection above.");
        setSchemaLoading(false);
        return;
      }
      setSchemaLoading(true);
      setSchemaError("");
      try {
        const data = await fetchQuerySchema({ connector_id: connectorId, database });
        if (!isCurrent()) return;
        setSchemaObjects(data.objects.map(toSchemaObject));
        setSchemaTruncated(data.objects_truncated === true);
        setSchemaConnected(data.connected);
        setSchemaError(schemaNotConnectedReason(data));
        setSchemaTypeSource(data.type_source ?? "");
        setSchemaWarnings(data.warnings ?? []);
      } catch (e) {
        if (!isCurrent()) return;
        setSchemaObjects([]);
        setSchemaTruncated(false);
        setSchemaError(querySchemaErrorCopy((e as Error).message || "Schema introspection failed"));
        setSchemaConnected(false);
      } finally {
        if (isCurrent()) setSchemaLoading(false);
      }
    },
    [connectors],
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
        toast({ title: "Select a connector", message: "Choose a saved connection in the bar above, then run.", tone: "warning" });
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
      runAbort.current?.abort();
      const controller = new AbortController();
      runAbort.current = controller;
      const startedAt = Date.now();
      setQueryLoading(true);
      setRunStartedAt(startedAt);
      setRunError(null);
      setRunNotice("");
      setLiveMessage(`Running ${scope === "all" ? "the query" : `the ${scope}`} on ${selected?.name ?? "the connector"}.`);
      // UX-37: a failed run keeps the previous result on screen; a success replaces it.
      setExportResult(null);
      // The tab's "edited since last run" marker clears on every run.
      patchActive({ lastRunQuery: active.query });
      try {
        const data = await executeQuery(
          playgroundQueryBody({
            connectorId: active.connectorId,
            query: text.trim(),
            database: active.database,
            collection: active.collection,
            limit: active.limit,
            params: paramsForRequest,
          }),
          { signal: controller.signal },
        );
        if (!isCurrent()) return;
        setResult(data);
        setResultLimit(active.limit);
        setLastRunText(text.trim());
        setLiveMessage(
          `Query finished: ${data.row_count.toLocaleString()} row${data.row_count === 1 ? "" : "s"} in ${formatDuration(data.duration_ms)}${
            data.truncated ? `, limited to ${active.limit.toLocaleString()} rows` : ""
          }.`,
        );
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
          saveHistory(next, workspaceId);
          return next;
        });
        // No toast for a plain successful run: the results status line already
        // says "500 rows · 42 ms", and a toast covered the grid.
      } catch (e) {
        // A run the operator stopped, or one a newer run replaced, says nothing.
        if (!isCurrent()) return;
        const failure = queryFailureCopy(e);
        const message = (e as Error).message || "Query failed";
        setRunError(failure);
        setLiveMessage(`Query failed: ${failure.title}.`);
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
          saveHistory(next, workspaceId);
          return next;
        });
        // The API names the cause and the one next action (error.code); the
        // results pane shows them with the support reference — no toast on top.
      } finally {
        if (isCurrent()) {
          setQueryLoading(false);
          runAbort.current = null;
        }
      }
    },
    [active, paramsForRequest, patchActive, selected?.name, toast, workspaceId],
  );

  /** Stop waiting for the run in flight; its answer, if it comes, is dropped. */
  const stopRun = useCallback(() => {
    if (!queryLoading) return;
    runSeq.current += 1;
    runAbort.current?.abort();
    runAbort.current = null;
    setQueryLoading(false);
    const notice = "Run stopped. Datawrap stopped waiting; the database may still finish the statement on its side.";
    setRunNotice(notice);
    setLiveMessage(notice);
  }, [queryLoading]);

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

  const downloadExport = async () => {
    if (!exportResult?.download_url) return;
    setDownloading(true);
    try {
      await downloadExportFile(exportResult.download_url, exportResult.filename || `query-export.${exportResult.format || "csv"}`);
    } catch (err) {
      toast({
        title: "Download failed",
        message: err instanceof Error ? err.message : "Could not download the export.",
        tone: "error",
      });
    } finally {
      setDownloading(false);
    }
  };

  const runExport = async () => {
    const text = (lastRunText || active?.query || "").trim();
    if (!active?.connectorId || !text) {
      toast({ title: "Run a query first", tone: "warning" });
      return;
    }
    setExportLoading(true);
    try {
      const data = await exportQuery({
        ...playgroundQueryBody({
          connectorId: active.connectorId,
          query: text,
          database: active.database,
          collection: active.collection,
          limit: active.limit,
          params: paramsForRequest,
        }),
        // File export streams every row. The grid limit stays on Run.
        // A connector write still uses that limit and the API says so.
        full_population: !destConnectorId,
        format: exportFormat,
        ...exportOptions.payload(exportFormat),
        output_path: outputPath,
        destination_connector_id: destConnectorId || undefined,
        destination: destTarget || undefined,
        sync_mode: destSyncMode,
      });
      setExportResult(data);
      if (data.success) {
        toast({
          title: "Export ready",
          message: data.population_note
            ? `${data.row_count?.toLocaleString() ?? 0} rows. ${data.population_note}`
            : `${data.row_count?.toLocaleString() ?? 0} rows exported`,
          tone: data.truncated ? "warning" : "success",
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

  /** Results, errors and notices belong to the tab that ran them. */
  const clearRunView = () => {
    setResult(null);
    setRunError(null);
    setRunNotice("");
  };

  const selectTab = (id: string) => {
    if (id === activeId) return;
    setActiveId(id);
    clearRunView();
  };

  const addTab = () => {
    const fresh = createTab({
      connectorId: active?.connectorId ?? "",
      database: active?.database ?? "",
      limit: active?.limit ?? 1000,
    });
    setTabs((prev) => [...prev, fresh]);
    setActiveId(fresh.id);
    clearRunView();
  };

  const closeTab = (id: string) => {
    const next = closeTabIn(tabs, activeId, id);
    setTabs(next.tabs);
    setActiveId(next.activeId);
    if (id === activeId) clearRunView();
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

  // ---------------------------------------------------------------- layout ---

  // Editor / results split (>= 1024px): the editor pane takes layout.editorHeight
  // px of the height both panes share, clamped so neither pane collapses.
  const mainRef = useRef<HTMLDivElement>(null);
  const [mainHeight, setMainHeight] = useState(0);
  useEffect(() => {
    const el = mainRef.current;
    if (!el) return;
    const measure = () => setMainHeight(el.clientHeight);
    measure();
    if (typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [wide]);
  const SPLITTER = 8;
  const editorPaneHeight = clampEditorSplit(layout.editorHeight, mainHeight - SPLITTER);
  const maxEditorPane = Math.max(MIN_EDITOR_PANE, clampEditorSplit(Number.MAX_SAFE_INTEGER, mainHeight - SPLITTER));
  const setEditorHeight = useCallback(
    (px: number) => setLayout((l) => ({ ...l, editorHeight: clampEditorSplit(px, mainHeight - SPLITTER) })),
    [mainHeight],
  );
  const dragRef = useRef<{ startY: number; startH: number } | null>(null);
  const onSplitterPointerDown = (e: ReactPointerEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.currentTarget.setPointerCapture(e.pointerId);
    dragRef.current = { startY: e.clientY, startH: editorPaneHeight };
  };
  const onSplitterPointerMove = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (!dragRef.current) return;
    setEditorHeight(dragRef.current.startH + (e.clientY - dragRef.current.startY));
  };
  const onSplitterPointerUp = () => {
    dragRef.current = null;
  };
  const onSplitterKeyDown = (e: ReactKeyboardEvent<HTMLDivElement>) => {
    const step = e.shiftKey ? 64 : 16;
    const next =
      e.key === "ArrowUp" ? editorPaneHeight - step
        : e.key === "ArrowDown" ? editorPaneHeight + step
          : e.key === "Home" ? MIN_EDITOR_PANE
            : e.key === "End" ? maxEditorPane
              : null;
    if (next === null) return;
    e.preventDefault();
    setEditorHeight(next);
  };

  // Schema browser: a column at >= 1024px, a drawer on phones and tablets.
  const schemaToggleRef = useRef<HTMLButtonElement>(null);
  const drawerCloseRef = useRef<HTMLButtonElement>(null);
  const toggleSchema = () => {
    if (wide) setLayout((l) => ({ ...l, schemaOpen: !l.schemaOpen }));
    else setSchemaDrawerOpen((v) => !v);
  };
  const closeDrawer = useCallback(() => {
    setSchemaDrawerOpen(false);
    schemaToggleRef.current?.focus();
  }, []);
  useEffect(() => {
    if (wide) setSchemaDrawerOpen(false);
  }, [wide]);
  useEffect(() => {
    if (!schemaDrawerOpen) return;
    drawerCloseRef.current?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeDrawer();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [schemaDrawerOpen, closeDrawer]);

  // Export and History open as panels over the workspace; Escape closes them.
  const exportButtonRef = useRef<HTMLButtonElement>(null);
  const historyButtonRef = useRef<HTMLButtonElement>(null);
  const closeExport = useCallback(() => {
    setExportOpen(false);
    exportButtonRef.current?.focus();
  }, []);
  const closeHistory = useCallback(() => {
    setLayout((l) => ({ ...l, historyOpen: false }));
    historyButtonRef.current?.focus();
  }, []);

  // Keep the active tab in view when the strip overflows.
  const tabsRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const strip = tabsRef.current;
    const tab = strip?.querySelector<HTMLElement>('[data-active="true"]');
    if (!strip || !tab) return;
    const left = tab.offsetLeft - strip.offsetLeft;
    if (left < strip.scrollLeft) strip.scrollLeft = left - 8;
    else if (left + tab.offsetWidth > strip.scrollLeft + strip.clientWidth) {
      strip.scrollLeft = left + tab.offsetWidth - strip.clientWidth + 8;
    }
  }, [activeId, tabs.length]);

  const schemaBrowser = (
    <SchemaBrowser
      objects={schemaObjects}
      loading={schemaLoading}
      error={schemaError}
      pending={schemaPending}
      connected={schemaConnected}
      typeSource={schemaTypeSource}
      warnings={schemaWarnings}
      objectsTruncated={schemaTruncated}
      onExpand={(name) => void expandObject(name)}
      onRefresh={() =>
        void loadObjects(active?.connectorId ?? "", active?.database ?? "")
      }
      onInsert={insertIntoQuery}
      onPreview={(obj) => {
        if (!wide) setSchemaDrawerOpen(false);
        previewObject(obj);
      }}
    />
  );

  if (!connectorsLoading && connectors.length === 0) {
    return (
      <PageShell
        wide
        className="df2-page-query"
        title="Query"
        kicker="Operations"
        description="Read-only console for databases, warehouses, and files — schema-aware, type-honest, multi-tab. Kafka topics and vector stores are not queried here."
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

  const queryText = active?.query ?? "";
  const hasQuery = queryText.trim() !== "";
  const runBlocked = !active?.connectorId
    ? "Choose a connector first"
    : !hasQuery
      ? "Write a query first"
      : "";
  const explainable = Boolean(active?.connectorId) && hasQuery && !isMongo;
  const schemaShown = wide ? layout.schemaOpen : schemaDrawerOpen;

  return (
    <PageShell
      wide
      className="df2-page-query"
      title="Query"
      kicker="Operations"
      description="Read-only console for databases, warehouses, and files — schema-aware, type-honest, multi-tab. Kafka topics and vector stores are not queried here."
    >
      <PageFrame className="df2-query-page df2-qw">
        <div ref={fillRef} className="df2-qw-shell" data-testid="query-workspace" data-wide={wide ? "true" : "false"}>
        <div ref={sticky.sentinelRef} className="df2-qw-sticky-sentinel" aria-hidden="true" />
        {/* Tabs and the action bar stay pinned; at >= 1024px only the panes scroll. */}
        <div
          ref={sticky.headerRef}
          className="df2-qw-sticky"
          data-stuck={sticky.stuck}
          data-testid="query-sticky-header"
        >
          {/* ---------------------------------------------------- tab strip --- */}
          <div className="df2-qw-tabsrow">
            <button
              ref={schemaToggleRef}
              type="button"
              className="df2-qw-icon-btn df2-qw-schema-toggle"
              data-active={schemaShown}
              aria-pressed={wide ? layout.schemaOpen : undefined}
              aria-expanded={wide ? undefined : schemaDrawerOpen}
              aria-label={wide ? (layout.schemaOpen ? "Hide schema browser" : "Show schema browser") : "Open schema browser"}
              title={wide ? (layout.schemaOpen ? "Hide schema browser" : "Show schema browser") : "Schema browser"}
              onClick={toggleSchema}
              data-testid="query-schema-toggle"
            >
              <DtIcon name="panel-left" size={16} />
            </button>
            {/* UX-16: editor tabs with their own close buttons are a labelled group
                of buttons, not an ARIA tablist (which may hold only tabs). */}
            <div className="df2-qw-tabs" role="group" aria-label="Query tabs" ref={tabsRef}>
              {tabs.map((t) => {
                const edited = editedSinceRun(t);
                return (
                  <div
                    key={t.id}
                    className="df2-qw-tab"
                    data-active={t.id === activeId}
                    data-edited={edited ? "true" : undefined}
                    data-testid="query-tab"
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
                        aria-current={t.id === activeId ? "true" : undefined}
                        onClick={() => selectTab(t.id)}
                        onDoubleClick={() => setRenamingId(t.id)}
                        onKeyDown={(e) => {
                          if (e.key === "F2") {
                            e.preventDefault();
                            setRenamingId(t.id);
                          }
                        }}
                        title={`${t.query.slice(0, 200) || "Empty query"}\n\nDouble-click or F2 to rename.`}
                      >
                        <span className="df2-qw-tab-title">{t.title}</span>
                        {edited && (
                          <>
                            <span className="df2-qw-tab-dot" aria-hidden />
                            <span className="df2-sr-only"> (edited since last run)</span>
                          </>
                        )}
                      </button>
                    )}
                    <button
                      type="button"
                      className="df2-qw-tab-close"
                      onClick={() => closeTab(t.id)}
                      aria-label={`Close ${t.title}`}
                      title={`Close ${t.title}`}
                    >
                      <DtIcon name="x" size={12} />
                    </button>
                  </div>
                );
              })}
            </div>
            <button type="button" className="df2-qw-tab-add" onClick={addTab} title="New tab" aria-label="New query tab">
              <DtIcon name="plus" size={14} />
            </button>
            <div className="df2-qw-tabs-right">
              <button type="button" className="df2-qw-chip" onClick={duplicate} title="Duplicate this tab">
                Duplicate
              </button>
              <button
                ref={historyButtonRef}
                type="button"
                className="df2-qw-chip"
                data-active={layout.historyOpen}
                aria-expanded={layout.historyOpen}
                aria-controls="query-history-panel"
                onClick={() => setLayout((l) => ({ ...l, historyOpen: !l.historyOpen }))}
              >
                <DtIcon name="clock" size={12} /> History
              </button>
            </div>
          </div>

          {/* --------------------------------------------------- action bar --- */}
          <div className="df2-qw-actionbar" data-testid="query-action-bar">
            <div className="df2-qw-context">
              <div className="df2-qw-field df2-qw-field--connector">
                <ConnectorSelect
                  id="query-connector"
                  label="Connector"
                  value={active?.connectorId ?? ""}
                  onChange={(id) => patchActive({ connectorId: id })}
                  connectors={connectors}
                  placeholder="Select a saved connector…"
                />
              </div>
              <div className="df2-field df2-qw-field">
                <label className="df2-label" htmlFor="query-database">{isMongo ? "Database" : "Database / Schema"}</label>
                <input
                  id="query-database"
                  className="df2-input"
                  value={active?.database ?? ""}
                  onChange={(e) => patchActive({ database: e.target.value })}
                  placeholder={isMongo ? "mydb" : "Default"}
                />
              </div>
              <div className="df2-field df2-qw-field">
                <label className="df2-label" htmlFor="query-collection">{isMongo ? "Collection" : "Table (optional)"}</label>
                <input
                  id="query-collection"
                  className="df2-input"
                  value={active?.collection ?? ""}
                  onChange={(e) => patchActive({ collection: e.target.value })}
                  placeholder={isMongo ? "users" : "Any"}
                />
              </div>
              <div className="df2-field df2-qw-field df2-qw-field--limit">
                <label className="df2-label" htmlFor="query-row-limit">Row limit</label>
                <select
                  id="query-row-limit"
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
            <div className="df2-qw-actions">
              <button
                type="button"
                className="df2-qw-tool"
                onClick={() => editorRef.current?.format()}
                disabled={!hasQuery || queryLoading}
                title={`Format (${MOD_KEY}+Shift+F)`}
                aria-keyshortcuts="Control+Shift+F Meta+Shift+F"
              >
                <DtIcon name="code" size={14} /> <span className="df2-qw-tool-label">Format</span>
              </button>
              {!isMongo && (
                <button
                  type="button"
                  className="df2-qw-tool"
                  onClick={runExplain}
                  disabled={!explainable || queryLoading || exportLoading}
                  title={`Explain plan (${MOD_KEY}+E)`}
                  aria-keyshortcuts="Control+E Meta+E"
                >
                  <DtIcon name="scan" size={14} /> <span className="df2-qw-tool-label">Explain</span>
                </button>
              )}
              <button
                ref={exportButtonRef}
                type="button"
                className="df2-qw-tool"
                data-active={exportOpen}
                aria-expanded={exportOpen}
                aria-controls="query-export-panel"
                onClick={() => setExportOpen((v) => !v)}
                disabled={!result}
                title={result ? "Export these results" : "Run a query first"}
              >
                <DtIcon name="download" size={14} /> <span className="df2-qw-tool-label">Export</span>
              </button>
              <span className="df2-qw-actions-sep" aria-hidden />
              <Button
                variant="secondary"
                disabled={exportLoading || queryLoading || Boolean(runBlocked)}
                onClick={runAll}
                title={runBlocked || `Run everything in the editor (${MOD_KEY}+Shift+Enter)`}
                aria-keyshortcuts="Control+Shift+Enter Meta+Shift+Enter"
                data-testid="query-run-all"
              >
                Run all
              </Button>
              {queryLoading ? (
                <Button
                  variant="danger"
                  onClick={stopRun}
                  leadingIcon={<DtIcon name="x" size={14} />}
                  data-testid="query-stop"
                  title="Stop waiting for this run"
                >
                  Stop
                </Button>
              ) : (
                <Button
                  variant="primary"
                  className="df2-qw-run"
                  disabled={exportLoading || Boolean(runBlocked)}
                  onClick={() => editorRef.current?.run()}
                  leadingIcon={<DtIcon name="play" size={14} />}
                  title={runBlocked || `Run the selection, or the statement under the caret (${MOD_KEY}+Enter)`}
                  aria-keyshortcuts="Control+Enter Meta+Enter"
                  data-testid="query-run"
                >
                  Run <kbd className="df2-qw-kbd" aria-hidden>{MOD_KEY}+↵</kbd>
                </Button>
              )}
            </div>

            {/* ------------------------------------------------- export --- */}
            {exportOpen && (
              <div
                className="df2-qw-export"
                id="query-export-panel"
                role="region"
                aria-label="Export results"
                onKeyDown={(e) => {
                  if (e.key === "Escape") closeExport();
                }}
              >
                <div className="df2-qw-export-head">
                  <strong>Export results</strong>
                  <button type="button" className="df2-close-btn df2-close-btn-sm" onClick={closeExport} aria-label="Close export">
                    <DtIcon name="x" size={14} />
                  </button>
                </div>
                <div className="df2-qw-export-note">
                  <DtIcon name="info" size={14} />
                  <span>
                    File export re-runs this query on the server and writes every row. The
                    grid limit does not apply. A LIMIT already in the SQL is kept. Writing
                    to a connector uses the preview limit and says so. For production
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
                        aria-label="Destination table, collection, or object name"
                        className="df2-input df2-input-sm"
                        value={destTarget}
                        onChange={(e) => setDestTarget(e.target.value)}
                        placeholder="Table / collection / object name"
                      />
                      <select
                        aria-label="Write mode"
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
                        aria-label="File format"
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
                        aria-label="File name in exports/ (optional)"
                        className="df2-input df2-input-sm"
                        value={outputPath}
                        onChange={(e) => setOutputPath(e.target.value)}
                        placeholder="File name in exports/ (optional)"
                      />
                      {formulaGuardApplies(exportFormat) && (
                        <FormulaProtectionToggle
                          id="query-export-formula"
                          className="df2-qw-export-formula"
                          checked={exportOptions.formulaProtection}
                          onChange={exportOptions.setFormulaProtection}
                          help={QUERY_EXPORT_FORMULA_PROTECTION_HELP}
                        />
                      )}
                      {exportFormat === "tsv" && (
                        <TsvNullMarkerField
                          id="query-export-tsv-null"
                          value={exportOptions.tsvNull}
                          onChange={exportOptions.setTsvNull}
                          className="df2-field df2-qw-export-tsv-null"
                          compact
                        />
                      )}
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
                        // UX-2: fetched with the session, never a bare link (401).
                        <Button
                          size="sm"
                          variant="primary"
                          loading={downloading}
                          loadingLabel="Downloading…"
                          onClick={() => void downloadExport()}
                          leadingIcon={<DtIcon name="download" size={14} />}
                          data-testid="query-export-download"
                        >
                          Download {exportResult.filename}
                        </Button>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        <div
          className="df2-qw-body"
          data-schema={wide && layout.schemaOpen}
          data-history={layout.historyOpen}
        >
          {wide && layout.schemaOpen && (
            <div className="df2-qw-pane df2-qw-pane--schema" data-testid="query-pane-schema">
              {schemaBrowser}
            </div>
          )}

          <div
            className="df2-qw-main"
            ref={mainRef}
            style={wide ? ({ "--df2-qw-editor-h": `${editorPaneHeight}px` } as React.CSSProperties) : undefined}
          >
            {/* --------------------------------------------------- editor --- */}
            <div className="df2-qw-pane df2-qw-pane--editor" id="query-editor-pane" data-testid="query-pane-editor">
              <QueryEditor
                ref={editorRef}
                value={queryText}
                onChange={(q) => patchActive({ query: q })}
                connectorType={selected?.type}
                placeholder={queryPlaceholder}
                disabled={exportLoading}
                readOnly={queryLoading}
                variant="workspace"
                fill
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
                        aria-label={`Value for :${name}`}
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
                  {missingParams.length > 0 ? (
                    <span className="df2-qw-warn">
                      <DtIcon name="warning" size={12} /> {missingParams.map((p) => `:${p}`).join(", ")}{" "}
                      {missingParams.length === 1 ? "has" : "have"} no value yet — sent as empty.
                    </span>
                  ) : (
                    <span className="df2-qw-params-note">
                      Sent as bound values — never spliced into the statement text.
                    </span>
                  )}
                </div>
              )}
            </div>

            {wide && (
              <div
                className="df2-qw-splitter"
                role="separator"
                aria-orientation="horizontal"
                aria-label="Resize editor and results"
                aria-controls="query-editor-pane"
                aria-valuemin={MIN_EDITOR_PANE}
                aria-valuemax={maxEditorPane}
                aria-valuenow={editorPaneHeight}
                tabIndex={0}
                onPointerDown={onSplitterPointerDown}
                onPointerMove={onSplitterPointerMove}
                onPointerUp={onSplitterPointerUp}
                onPointerCancel={onSplitterPointerUp}
                onKeyDown={onSplitterKeyDown}
                onDoubleClick={() => setEditorHeight(260)}
                data-testid="query-splitter"
              />
            )}

            {/* -------------------------------------------------- results --- */}
            <section className="df2-qw-pane df2-qw-pane--results" aria-label="Results" data-testid="query-pane-results">
              {runError && (
                <QueryErrorPanel
                  failure={runError}
                  connectorLabel={selected?.name}
                  keptResults={Boolean(result) && !queryLoading}
                  onDismiss={() => setRunError(null)}
                />
              )}
              {runNotice && !queryLoading && (
                <p className="df2-qw-notice" role="status">
                  <DtIcon name="info" size={14} /> {runNotice}
                </p>
              )}
              {queryLoading ? (
                <div className="df2-qw-running" data-testid="query-run-status">
                  <span className="df2-qw-running-spinner" aria-hidden>
                    <DtIcon name="spinner" size={18} />
                  </span>
                  <div>
                    <p className="df2-qw-running-title">
                      Running · <RunElapsed startedAt={runStartedAt} /> elapsed
                    </p>
                    <p className="df2-qw-running-text">
                      On {selected?.name ?? "the connector"}
                      {active?.database ? ` · ${active.database}` : ""} · row limit{" "}
                      {(active?.limit ?? 1000).toLocaleString()}. Stop cancels the wait; results from
                      this run are then not shown.
                    </p>
                  </div>
                </div>
              ) : result ? (
                <ResultGrid
                  columns={result.columns}
                  rows={result.rows}
                  columnSchema={result.column_schema}
                  typeSource={result.column_type_source}
                  truncated={result.truncated}
                  durationMs={result.duration_ms}
                  limit={resultLimit || undefined}
                  onCopied={(what) => toast({ title: "Copied", message: what, tone: "success" })}
                  formulaProtection={exportOptions.formulaProtection}
                  fill={wide}
                />
              ) : runError || runNotice ? null : (
                <div className="df2-qw-start" data-testid="query-empty">
                  {active?.connectorId ? (
                    <>
                      <p className="df2-qw-start-title">Run a query to see results</p>
                      <ul className="df2-qw-start-list">
                        <li>
                          <kbd className="df2-qw-kbd">{MOD_KEY}+↵</kbd> runs the statement under the caret;
                          {" "}<kbd className="df2-qw-kbd">{MOD_KEY}+Shift+↵</kbd> runs everything.
                        </li>
                        <li>
                          <kbd className="df2-qw-kbd">{MOD_KEY}+Space</kbd> completes table and column names from the
                          live schema.
                        </li>
                        <li>Select ▶ next to a table in the schema browser to preview its first rows.</li>
                      </ul>
                    </>
                  ) : (
                    <>
                      <p className="df2-qw-start-title">Start with a connection</p>
                      <ol className="df2-qw-start-list">
                        <li>Choose a saved connection in <strong>Connector</strong> above.</li>
                        <li>Browse its tables in the schema browser, or write a read-only query.</li>
                        <li>
                          Select <strong>Run</strong> or press <kbd className="df2-qw-kbd">{MOD_KEY}+↵</kbd>.
                        </li>
                      </ol>
                    </>
                  )}
                </div>
              )}
            </section>
          </div>

          {/* -------------------------------------------------- history --- */}
          {layout.historyOpen && (
            <aside
              className="df2-qw-history"
              id="query-history-panel"
              aria-label="Query history"
              onKeyDown={(e) => {
                if (e.key === "Escape") closeHistory();
              }}
            >
              <div className="df2-qw-schema-head">
                <span className="df2-qw-schema-title">
                  <DtIcon name="clock" size={13} /> History
                </span>
                <span className="df2-qw-history-tools">
                  <button
                    type="button"
                    className="df2-qw-icon-btn"
                    onClick={() => {
                      setHistory([]);
                      saveHistory([], workspaceId);
                    }}
                    title="Clear history"
                    aria-label="Clear history"
                  >
                    <DtIcon name="trash" size={14} />
                  </button>
                  <button type="button" className="df2-close-btn df2-close-btn-sm" onClick={closeHistory} aria-label="Close history">
                    <DtIcon name="x" size={14} />
                  </button>
                </span>
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

        {/* Phones and tablets: the schema browser is a drawer. */}
        {!wide && schemaDrawerOpen && (
          <>
            <div className="df2-qw-drawer-backdrop" role="presentation" onClick={closeDrawer} />
            <div
              className="df2-qw-drawer"
              role="dialog"
              aria-modal="true"
              aria-label="Schema browser"
              data-testid="query-schema-drawer"
              onKeyDown={(e) => {
                // Modal on phones: Tab cycles inside the drawer until it closes.
                if (e.key !== "Tab") return;
                const items = Array.from(
                  e.currentTarget.querySelectorAll<HTMLElement>("button:not(:disabled), input:not(:disabled), [tabindex='0']"),
                );
                if (items.length === 0) return;
                const first = items[0];
                const last = items[items.length - 1];
                if (e.shiftKey && document.activeElement === first) {
                  e.preventDefault();
                  last.focus();
                } else if (!e.shiftKey && document.activeElement === last) {
                  e.preventDefault();
                  first.focus();
                }
              }}
            >
              <button
                ref={drawerCloseRef}
                type="button"
                className="df2-close-btn df2-close-btn-sm df2-qw-drawer-close"
                onClick={closeDrawer}
                aria-label="Close schema browser"
              >
                <DtIcon name="x" size={16} />
              </button>
              {schemaBrowser}
            </div>
          </>
        )}

        <div className="df2-sr-only" role="status" aria-live="polite" data-testid="query-live">
          {liveMessage}
        </div>
        </div>
      </PageFrame>
    </PageShell>
  );
}
