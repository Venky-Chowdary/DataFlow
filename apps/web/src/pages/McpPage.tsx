import { useEffect, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { LoadingBlock } from "../components/LoadingState";
import { PageShell } from "../components/ui/PageShell";
import { useToast } from "../components/Toast";
import { API_BASE } from "../lib/types";
import { fetchMcpManifest, fetchMcpStatus } from "../lib/api";

const INTEGRATIONS = [
  { id: "cursor", label: "Cursor", icon: "sparkle", desc: "One-click MCP in Cursor Settings" },
  { id: "claude", label: "Claude Desktop", icon: "zap", desc: "Add to claude_desktop_config.json" },
  { id: "vscode", label: "VS Code", icon: "connectors", desc: "MCP extension + server URL" },
  { id: "chatgpt", label: "ChatGPT", icon: "activity", desc: "Custom GPT with tool endpoint" },
];

export function McpPage() {
  const { toast } = useToast();
  const [manifest, setManifest] = useState<Record<string, unknown> | null>(null);
  const [status, setStatus] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(true);
  const [copied, setCopied] = useState(false);

  const mcpUrl = `${API_BASE.replace(/\/api\/v1$/, "")}/api/v1/mcp`;

  useEffect(() => {
    setLoading(true);
    Promise.all([
      fetchMcpManifest().then(setManifest).catch(() => setManifest(null)),
      fetchMcpStatus().then(setStatus).catch(() => setStatus({ status: "offline" })),
    ]).finally(() => setLoading(false));
  }, []);

  const copyUrl = () => {
    navigator.clipboard.writeText(`${mcpUrl}/tools/call`);
    setCopied(true);
    toast({ title: "Copied to clipboard", message: "MCP tool endpoint URL.", tone: "success" });
    setTimeout(() => setCopied(false), 2000);
  };

  const tools = (manifest?.tools as unknown[]) ?? [];
  const online = status?.status === "online";

  return (
    <PageShell
      wide
      title="MCP Server"
      description="Connect Cursor, Claude, VS Code, and ChatGPT — same tools as Data Pilot."
      actions={
        <span className={`df2-badge ${online ? "df2-badge-live" : status?.status === "offline" ? "df2-badge-error" : "df2-badge-muted"}`}>
          {online ? "Online" : status?.status === "offline" ? "Offline" : "Checking…"}
        </span>
      }
    >
      {loading ? (
        <LoadingBlock title="Loading MCP server" hint="Fetching manifest and status…" />
      ) : (
        <div className="df2-stack">
          <div className="df2-grid-2" style={{ gridTemplateColumns: "1fr 1fr" }}>
            <div className="df2-card">
              <div className="df2-card-head"><h2 className="df2-card-title">Integrations</h2></div>
              <div className="df2-card-body df2-stack" style={{ gap: 12 }}>
                {INTEGRATIONS.map((item) => (
                  <div key={item.id} className="df2-cell-main">
                    <div className="df2-cell-icon"><DtIcon name={item.icon} size={20} /></div>
                    <div>
                      <div className="df2-cell-title">{item.label}</div>
                      <div style={{ fontSize: 13, color: "#64748b" }}>{item.desc}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="df2-card">
              <div className="df2-card-head"><h2 className="df2-card-title">Server endpoint</h2></div>
              <div className="df2-card-body df2-stack">
                <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                  <code className="df2-code-block" style={{ flex: 1, minWidth: 200 }}>{mcpUrl}/tools/call</code>
                  <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={copyUrl}>
                    {copied ? "Copied" : "Copy URL"}
                  </button>
                </div>
                {status && (
                  <div className="df2-job-stats">
                    <div className="df2-job-stat">
                      <span className="df2-job-stat-val">{String(status.agent_mode ?? "—")}</span>
                      <span className="df2-job-stat-lbl">Agent mode</span>
                    </div>
                    <div className="df2-job-stat">
                      <span className="df2-job-stat-val">{String(status.datasets_indexed ?? 0)}</span>
                      <span className="df2-job-stat-lbl">Datasets</span>
                    </div>
                    <div className="df2-job-stat">
                      <span className="df2-job-stat-val">{String(status.connectors ?? 0)}</span>
                      <span className="df2-job-stat-lbl">Connectors</span>
                    </div>
                    <div className="df2-job-stat">
                      <span className="df2-job-stat-val">{String(status.jobs ?? 0)}</span>
                      <span className="df2-job-stat-lbl">Jobs</span>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

          <div>
            <h2 className="df2-card-title" style={{ marginBottom: 16 }}>Available tools ({tools.length})</h2>
            {tools.length === 0 ? (
              <div className="df2-empty">
                <DtIcon name="zap" size={28} />
                <h3 className="df2-empty-title">No tools registered</h3>
                <p className="df2-empty-desc">Start the API with MCP enabled to expose agent tools.</p>
              </div>
            ) : (
              <div className="df2-mcp-grid">
                {tools.map((t: unknown) => {
                  const tool = t as { name: string; description: string };
                  return (
                    <div key={tool.name} className="df2-mcp-tile">
                      <code style={{ fontSize: 13, fontWeight: 600, color: "#0f766e" }}>{tool.name}</code>
                      <p style={{ margin: "8px 0 0", fontSize: 13, color: "#64748b" }}>{tool.description}</p>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      )}
    </PageShell>
  );
}
