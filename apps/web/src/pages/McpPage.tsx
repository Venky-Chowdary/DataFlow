import { useEffect, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { fetchMcpManifest, fetchMcpStatus } from "../lib/api";

const INTEGRATIONS = [
  { id: "cursor", label: "Cursor", desc: "One-click MCP in Cursor Settings" },
  { id: "claude", label: "Claude Desktop", desc: "Add to claude_desktop_config.json" },
  { id: "vscode", label: "VS Code", desc: "MCP extension + server URL" },
  { id: "chatgpt", label: "ChatGPT", desc: "Custom GPT with tool endpoint" },
];

export function McpPage() {
  const [manifest, setManifest] = useState<Record<string, unknown> | null>(null);
  const [status, setStatus] = useState<Record<string, unknown> | null>(null);
  const [copied, setCopied] = useState(false);

  const mcpUrl = "http://localhost:8001/api/v1/mcp";

  useEffect(() => {
    fetchMcpManifest().then(setManifest).catch(() => {});
    fetchMcpStatus().then(setStatus).catch(() => {});
  }, []);

  const copyUrl = () => {
    navigator.clipboard.writeText(`${mcpUrl}/tools/call`);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const tools = (manifest?.tools as unknown[]) ?? [];

  return (
    <div className="dt-content">
      <div className="dt-page-header">
        <div>
          <h1 className="dt-page-title">MCP Server</h1>
          <p className="dt-page-subtitle">
            Connect Cursor, Claude, VS Code, and ChatGPT to your data platform — same tools as Data Pilot.
          </p>
        </div>
        <span className={`dt-badge ${status?.status === "online" ? "dt-badge-success" : "dt-badge-default"}`}>
          {status?.status === "online" ? "Online" : "Checking…"}
        </span>
      </div>

      <div className="dt-mcp-grid">
        <div className="dt-card">
          <div className="dt-card-header">
            <h3 className="dt-card-title">Add to your AI tools</h3>
          </div>
          <div className="dt-card-body">
            {INTEGRATIONS.map((item) => (
              <div key={item.id} className="dt-mcp-integration">
                <div className="dt-mcp-integration-icon">
                  <DtIcon name="sparkle" size={18} />
                </div>
                <div>
                  <div className="dt-font-semibold">{item.label}</div>
                  <div className="dt-text-sm dt-text-muted">{item.desc}</div>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="dt-card">
          <div className="dt-card-header">
            <h3 className="dt-card-title">Server endpoint</h3>
          </div>
          <div className="dt-card-body">
            <div className="dt-mcp-url-box">
              <code>{mcpUrl}/tools/call</code>
              <button type="button" className="dt-btn dt-btn-sm" onClick={copyUrl}>
                {copied ? "Copied" : "Copy URL"}
              </button>
            </div>
            {status && (
              <ul className="dt-mcp-stats">
                <li>Agent mode: <strong>{String(status.agent_mode)}</strong></li>
                <li>Datasets: <strong>{String(status.datasets_indexed)}</strong></li>
                <li>Connectors: <strong>{String(status.connectors)}</strong></li>
                <li>Jobs: <strong>{String(status.jobs)}</strong></li>
              </ul>
            )}
          </div>
        </div>
      </div>

      <p className="dt-section-title">Available tools ({tools.length})</p>
      <div className="dt-mcp-tools">
        {tools.map((t: unknown) => {
          const tool = t as { name: string; description: string };
          return (
            <div key={tool.name} className="dt-mcp-tool-card">
              <code className="dt-mcp-tool-name">{tool.name}</code>
              <p className="dt-text-sm dt-text-muted">{tool.description}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
}
