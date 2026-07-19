import { useMemo, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { Button } from "../components/ui/Button";
import { ConnectorSelect } from "../components/ui/ConnectorSelect";
import { EmptyState } from "../components/ui/EmptyState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { PageContextBar } from "../components/ui/PageContextBar";
import { useToast } from "../components/Toast";
import { executeQuery, exportQuery, type QueryResult, type QueryExportResult } from "../lib/api";
import { Connector } from "../lib/types";
import { QueryEditor } from "../components/query/QueryEditor";

interface QueryPageProps {
  connectors: Connector[];
}

const FORMATS = ["csv", "json", "jsonl", "tsv", "excel", "parquet"];
const LIMITS = [100, 500, 1000, 5000, 10000];

export function QueryPage({ connectors }: QueryPageProps) {
  const { toast } = useToast();
  const [connectorId, setConnectorId] = useState("");
  const [database, setDatabase] = useState("");
  const [collection, setCollection] = useState("");
  const [query, setQuery] = useState("");
  const [limit, setLimit] = useState(1000);
  const [exportFormat, setExportFormat] = useState("csv");
  const [outputPath, setOutputPath] = useState("");
  const [destConnectorId, setDestConnectorId] = useState("");
  const [destTarget, setDestTarget] = useState("");
  const [destSyncMode, setDestSyncMode] = useState("append");
  const [queryLoading, setQueryLoading] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [exportResult, setExportResult] = useState<QueryExportResult | null>(null);

  const selected = useMemo(() => connectors.find((c) => c.id === connectorId), [connectors, connectorId]);
  const isMongo = selected?.type === "mongodb";
  const queryPlaceholder = isMongo
    ? '{"status": "active"}  or  [{ "$match": { ... } }]'
    : "SELECT * FROM users LIMIT 100";

  const runQuery = async () => {
    if (!connectorId) {
      toast({ title: "Select a connector", tone: "warning" });
      return;
    }
    if (!query.trim()) {
      toast({ title: "Enter a query", tone: "warning" });
      return;
    }
    setQueryLoading(true);
    setResult(null);
    setExportResult(null);
    try {
      const data = await executeQuery({
        connector_id: connectorId,
        query: query.trim(),
        database,
        collection,
        limit,
      });
      setResult(data);
    } catch (e) {
      toast({ title: "Query failed", message: (e as Error).message, tone: "error" });
    } finally {
      setQueryLoading(false);
    }
  };

  const runExport = async () => {
    if (!connectorId || !query.trim()) {
      toast({ title: "Run a query first", tone: "warning" });
      return;
    }
    setExportLoading(true);
    try {
      const data = await exportQuery({
        connector_id: connectorId,
        query: query.trim(),
        database,
        collection,
        limit,
        format: exportFormat,
        output_path: outputPath,
        destination_connector_id: destConnectorId || undefined,
        destination: destTarget || undefined,
        sync_mode: destSyncMode,
      });
      setExportResult(data);
      if (data.success) {
        toast({ title: "Export ready", message: `${data.row_count?.toLocaleString() ?? 0} rows exported`, tone: "success" });
      } else {
        toast({ title: "Export failed", message: data.error || "Unknown error", tone: "error" });
      }
    } catch (e) {
      toast({ title: "Export failed", message: (e as Error).message, tone: "error" });
    } finally {
      setExportLoading(false);
    }
  };

  return (
    <PageShell
      wide
      className="df2-page-query"
      title="Query"
      kicker="Operations"
      description="Run ad-hoc queries against saved connectors and export results."
    >
      <PageFrame className="df2-query-page">
        {connectors.length === 0 ? (
          <EmptyState
            page
            icon="search"
            title="Add a connector to run queries"
            description="Save a PostgreSQL, MySQL, MongoDB, or warehouse connection first — then run read-only SQL or aggregation pipelines and export results."
            action={
              <p className="df2-label-hint">Open Connectors from the sidebar to browse the catalog.</p>
            }
          />
        ) : (
        <>
        <PageContextBar
          ariaLabel="Query workspace summary"
          stats={[
            { label: "Connectors", value: connectors.length, icon: "connectors", title: "Saved connectors available to query" },
            { label: "Access", value: "Read-only", icon: "lock", tone: "ok", title: "Queries run in a safe, read-only sandbox" },
            { label: "Export formats", value: FORMATS.length, icon: "download", tone: "muted", title: FORMATS.map((f) => f.toUpperCase()).join(" · ") },
            {
              label: "Last result",
              value: result ? `${result.row_count.toLocaleString()} rows` : "—",
              icon: "layers",
              tone: "muted",
            },
          ]}
        />
        <div className="df2-query-form df2-card">
          <div className="df2-card-head">
            <div>
              <h2 className="df2-card-title">Run a query</h2>
              <p className="df2-card-sub">Safe, read-only SQL or MongoDB — export results when ready.</p>
            </div>
          </div>
          <div className="df2-card-body">
          <div className="df2-form-row df2-query-meta">
            <div className="df2-field-flex">
              <ConnectorSelect
                id="query-connector"
                label="Connector"
                value={connectorId}
                onChange={setConnectorId}
                connectors={connectors}
                placeholder="Select a saved connector…"
              />
            </div>
            <div className="df2-field df2-field-md">
              <label className="df2-label">{isMongo ? "Database" : "Database / Schema"}</label>
              <input className="df2-input" value={database} onChange={(e) => setDatabase(e.target.value)} placeholder={isMongo ? "mydb" : "public"} />
            </div>
            <div className="df2-field df2-field-md">
              <label className="df2-label">{isMongo ? "Collection" : "Table (optional)"}</label>
              <input className="df2-input" value={collection} onChange={(e) => setCollection(e.target.value)} placeholder={isMongo ? "users" : "users"} />
            </div>
            <div className="df2-field df2-field-sm">
              <label className="df2-label">Row limit</label>
              <select className="df2-input" value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
                {LIMITS.map((l) => <option key={l} value={l}>{l.toLocaleString()}</option>)}
              </select>
            </div>
          </div>

          <div className="df2-field">
            <label className="df2-label">Query</label>
            <QueryEditor
              value={query}
              onChange={setQuery}
              connectorType={selected?.type}
              placeholder={queryPlaceholder}
              disabled={queryLoading || exportLoading}
              height="22rem"
            />
            <p className="df2-label-hint">SQL mode supports SELECT, CTEs (WITH), EXPLAIN, SHOW, and subqueries. MongoDB mode accepts a JSON filter or an aggregate pipeline array.</p>
          </div>

          <div className="df2-query-actions">
            <Button
              variant="secondary"
              disabled={queryLoading || exportLoading || !query.trim()}
              onClick={() => setQuery("")}
            >
              Clear query
            </Button>
            <Button
              variant="primary"
              loading={queryLoading}
              loadingLabel="Running…"
              disabled={exportLoading}
              onClick={() => void runQuery()}
              leadingIcon={<DtIcon name="play" size={14} />}
            >
              Run query
            </Button>
          </div>
          </div>
        </div>

        {result && (
          <div className="df2-query-results df2-card">
            <div className="df2-card-head">
              <div>
                <h3 className="df2-card-title">Results</h3>
                <p className="df2-card-sub">{result.row_count.toLocaleString()} rows · {result.columns.length} columns {result.truncated && "· truncated"}</p>
              </div>
              <div className="df2-query-export-bar">
                <div className="df2-query-export-destination">
                  <ConnectorSelect
                    id="query-destination-connector"
                    label="Destination (optional)"
                    value={destConnectorId}
                    onChange={setDestConnectorId}
                    connectors={connectors}
                    placeholder="File export"
                  />
                  {destConnectorId && (
                    <>
                      <input
                        className="df2-input df2-input-sm"
                        value={destTarget}
                        onChange={(e) => setDestTarget(e.target.value)}
                        placeholder="Table / collection / object name"
                      />
                      <select className="df2-input df2-input-sm" value={destSyncMode} onChange={(e) => setDestSyncMode(e.target.value)}>
                        <option value="append">Append</option>
                        <option value="upsert">Upsert</option>
                        <option value="overwrite">Overwrite</option>
                      </select>
                    </>
                  )}
                </div>
                {!destConnectorId && (
                  <>
                    <select className="df2-input df2-input-sm" value={exportFormat} onChange={(e) => setExportFormat(e.target.value)}>
                      {FORMATS.map((f) => <option key={f} value={f}>{f.toUpperCase()}</option>)}
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
            </div>

            <div className="df2-query-table-wrap">
              {result.rows.length === 0 ? (
                <EmptyState icon="search" title="No rows" description="The query returned zero rows." compact />
              ) : (
                <table className="df2-query-table">
                  <thead>
                    <tr>
                      {result.columns.map((c) => <th key={c}>{c}</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, i) => (
                      <tr key={i}>
                        {result.columns.map((c) => (
                          <td key={c}>{formatCell(row[c])}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}

        {exportResult?.success && (
          <div className="df2-query-export-notice df2-alert df2-alert-success" role="alert">
            <DtIcon name="check" size={16} />
            <div>
              <strong>{exportResult.download_url ? "Export ready" : "Export complete"}</strong>
              <p>{(exportResult.row_count ?? 0).toLocaleString()} rows {exportResult.download_url ? `exported as ${exportResult.format?.toUpperCase()}` : `written to ${exportResult.format}${exportResult.filename ? ` · ${exportResult.filename}` : ""}`}.</p>
              {exportResult.download_url && (
                <a className="df2-btn df2-btn-primary df2-btn-sm" href={exportResult.download_url} download={exportResult.filename}>
                  <DtIcon name="download" size={14} /> Download {exportResult.filename}
                </a>
              )}
            </div>
          </div>
        )}
        </>
        )}
      </PageFrame>
    </PageShell>
  );
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
