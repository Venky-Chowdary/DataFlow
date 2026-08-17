import { useEffect, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { EmptyState } from "../components/ui/EmptyState";
import { SectionLoader } from "../components/LoadingState";
import { Button } from "../components/ui/Button";
import { FilterTabs } from "../components/ui/FilterTabs";
import { FilterBar } from "../components/ui/FilterBar";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { useToast } from "../components/Toast";
import { API_BASE } from "../lib/types";
import { fetchMcpLogs, fetchMcpStatus } from "../lib/api";

const INTEGRATIONS = [
  {
    id: "cursor",
    label: "Cursor",
    icon: "sparkle",
    desc: "Add MCP server in Cursor Settings → MCP",
    snippet: `{
  "mcpServers": {
    "dataflow": {
      "url": "${API_BASE.replace(/\/api\/v1$/, "")}/api/v1/mcp"
    }
  }
}`,
  },
  {
    id: "claude",
    label: "Claude Desktop",
    icon: "zap",
    desc: "Paste into claude_desktop_config.json",
    snippet: `{
  "mcpServers": {
    "dataflow": {
      "command": "npx",
      "args": ["-y", "@dataflow/mcp-bridge"],
      "env": { "DATAFLOW_API": "${API_BASE}" }
    }
  }
}`,
  },
  {
    id: "vscode",
    label: "VS Code",
    icon: "connectors",
    desc: "MCP extension with HTTP transport",
    snippet: `// .vscode/mcp.json
{
  "servers": {
    "dataflow": { "type": "http", "url": "${API_BASE}/mcp" }
  }
}`,
  },
  {
    id: "chatgpt",
    label: "Custom GPT",
    icon: "activity",
    desc: "OpenAPI action pointing at the MCP endpoint",
    snippet: `POST ${API_BASE.replace(/\/api\/v1$/, "")}/api/v1/mcp/tools/call
Authorization: Bearer <api-key>`,
  },
];

type McpLog = {
  id: string;
  time: string;
  ts: number;
  tool: string;
  client: string;
  status: "ok" | "error";
  ms: number;
};

export function McpPage() {
  const { toast } = useToast();
  const [status, setStatus] = useState<Record<string, unknown> | null>(null);
  const [logs, setLogs] = useState<McpLog[]>([]);
  const [loading, setLoading] = useState(true);
  const [copied, setCopied] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>("cursor");
  const [logFilter, setLogFilter] = useState<"all" | "ok" | "error">("all");

  const mcpBase = `${API_BASE.replace(/\/api\/v1$/, "")}/api/v1/mcp`;

  useEffect(() => {
    setLoading(true);
    Promise.all([
      fetchMcpStatus().then(setStatus).catch(() => setStatus({ status: "offline" })),
      fetchMcpLogs(50)
        .then((rows) =>
          setLogs(
            rows.map((r) => ({
              id: r.id,
              time: new Date(r.time).toLocaleTimeString(),
              ts: new Date(r.time).getTime(),
              tool: r.tool,
              client: r.client,
              status: r.status === "ok" ? "ok" : "error",
              ms: r.ms,
            })),
          ),
        )
        .catch(() => setLogs([])),
    ]).finally(() => setLoading(false));
  }, []);

  const copyText = (text: string, label: string) => {
    navigator.clipboard.writeText(text);
    setCopied(label);
    toast({ title: "Copied to clipboard", message: label, tone: "success" });
    setTimeout(() => setCopied(null), 2000);
  };

  const online = status?.status === "online";
  const filteredLogs = logFilter === "all" ? logs : logs.filter((l) => l.status === logFilter);
  const okCount = logs.filter((l) => l.status === "ok").length;
  const errCount = logs.filter((l) => l.status === "error").length;

  return (
    <PageShell
      wide
      className="df2-page-mcp"
      title="MCP Server"
      description="Connect Cursor, Claude, or VS Code — agents use the same preflight and proof path as the UI."
    >
      {loading ? (
        <PageFrame className="df2-mcp-workspace">
          <SectionLoader title="Loading MCP server" hint="Checking endpoint status…" />
        </PageFrame>
      ) : (
        <PageFrame className="df2-mcp-workspace df2-stack">
          <section className="df2-mcp-endpoint-card" aria-label="MCP endpoint">
            <div className="df2-mcp-endpoint-copy">
              <div className="df2-mcp-endpoint-head">
                <span className="df2-mcp-endpoint-label">MCP server URL</span>
                <span className={`df2-mcp-status-pill ${online ? "is-online" : "is-offline"}`}>
                  {online ? "Online" : "Offline"}
                </span>
              </div>
              <code className="df2-mcp-endpoint-url" title={mcpBase}>
                {mcpBase}
              </code>
              <span className="df2-mcp-endpoint-meta">
                {online
                  ? "Paste this URL into your agent client — tools load automatically from the server."
                  : "Endpoint not responding. Start the API, then retry setup."}
              </span>
            </div>
            <div className="df2-mcp-endpoint-actions">
              <Button
                variant="primary"
                onClick={() => copyText(mcpBase, "MCP server URL")}
                leadingIcon={<DtIcon name="check" size={14} />}
              >
                {copied === "MCP server URL" ? "Copied MCP URL" : "Copy MCP URL"}
              </Button>
            </div>
          </section>

          <div className="df2-mcp-layout">
            <div className="df2-mcp-panel df2-mcp-panel--integrations">
              <div className="df2-mcp-panel-head">
                <h2>Client setup</h2>
              </div>
              <div className="df2-mcp-panel-body">
                <div className="df2-mcp-integration-list">
                  {INTEGRATIONS.map((item) => (
                    <div key={item.id} className="df2-mcp-integration-row">
                      <div className="df2-cell-main">
                        <div className="df2-cell-icon">
                          <DtIcon name={item.icon} size={20} />
                        </div>
                        <div>
                          <div className="df2-cell-title">{item.label}</div>
                          <div className="df2-cell-meta">{item.desc}</div>
                        </div>
                      </div>
                      <button
                        type="button"
                        className="df2-btn df2-btn-sm"
                        onClick={() => setExpandedId(expandedId === item.id ? null : item.id)}
                      >
                        {expandedId === item.id ? "Hide" : "Setup"}
                      </button>
                    </div>
                  ))}
                </div>
                {expandedId && (
                  <div className="df2-mcp-snippet">
                    {INTEGRATIONS.find((i) => i.id === expandedId)?.snippet}
                    <div className="df2-mcp-snippet-actions">
                      <button
                        type="button"
                        className="df2-btn df2-btn-sm df2-btn-primary"
                        onClick={() =>
                          copyText(
                            INTEGRATIONS.find((i) => i.id === expandedId)!.snippet,
                            "Setup snippet",
                          )
                        }
                      >
                        {copied === "Setup snippet" ? "Copied" : "Copy snippet"}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </div>

            <div className="df2-mcp-panel df2-mcp-panel--logs">
              <div className="df2-mcp-panel-head">
                <h2>Recent activity</h2>
              </div>
              {logs.length > 0 && (
                <div className="df2-mcp-panel-filters">
                  <FilterBar ariaLabel="Filter MCP activity">
                    <FilterTabs
                      ariaLabel="Filter MCP activity"
                      value={logFilter}
                      onChange={setLogFilter}
                      items={[
                        { id: "all", label: "All", count: logs.length },
                        { id: "ok", label: "Success", count: okCount },
                        { id: "error", label: "Errors", count: errCount },
                      ]}
                    />
                  </FilterBar>
                </div>
              )}
              <div className="df2-mcp-panel-body df2-mcp-panel-body--flush">
                <div className="df2-mcp-logs-table-wrap">
                  <table className="df2-mcp-logs-table">
                    <thead>
                      <tr>
                        <th>Time</th>
                        <th>Tool</th>
                        <th>Client</th>
                        <th>Status</th>
                        <th>Latency</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredLogs.length === 0 ? (
                        <tr>
                          <td colSpan={5}>
                            <EmptyState
                              compact
                              icon="activity"
                              title={
                                logs.length === 0
                                  ? "No agent calls yet"
                                  : "No matching requests"
                              }
                              description={
                                logs.length === 0
                                  ? "After you connect Cursor or Claude, invocations show here."
                                  : "Try another filter."
                              }
                            />
                          </td>
                        </tr>
                      ) : (
                        filteredLogs.map((log) => (
                          <tr key={log.id}>
                            <td>{log.time}</td>
                            <td>
                              <code>{log.tool}</code>
                            </td>
                            <td>{log.client}</td>
                            <td>
                              <span
                                className={`df2-mcp-log-status df2-mcp-log-status--${
                                  log.status === "ok" ? "ok" : "err"
                                }`}
                              >
                                {log.status === "ok" ? "200" : "500"}
                              </span>
                            </td>
                            <td>{log.ms} ms</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </PageFrame>
      )}
    </PageShell>
  );
}
