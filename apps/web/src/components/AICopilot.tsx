import { useState, useEffect, useRef } from "react";
import { DtIcon } from "./DtIcon";
import {
  copilotChat,
  fetchCopilotPrompts,
  fetchPilotTools,
  CopilotAction,
  CopilotChatMessage,
  PilotToolRegistry,
} from "../lib/api";
import { useActiveData } from "../lib/DataContext";
import { Screen } from "../lib/types";

interface Message {
  role: "user" | "assistant";
  text: string;
  method?: string;
  actions?: CopilotAction[];
  dataInsight?: {
    dataset: string;
    columns: number;
    rows: number;
    pii_count: number;
    quality_score: number;
  };
  toolsUsed?: { name: string; success: boolean; summary: string }[];
}

interface AICopilotProps {
  onNavigate?: (screen: Screen) => void;
  variant?: "fab" | "rail";
  onClose?: () => void;
}

function renderMarkdown(text: string) {
  return text
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/`(.+?)`/g, "<code>$1</code>")
    .replace(/\n/g, "<br/>");
}

const SCREEN_LABELS: Record<string, string> = {
  dashboard: "Overview",
  pilot: "Data Pilot",
  transfer: "Transfer Studio",
  connectors: "Connectors",
  jobs: "Jobs",
  settings: "Settings",
};

const FALLBACK_TOOL_REGISTRY: PilotToolRegistry = {
  tool_count: 15,
  generated_action_count: 1740,
  total_routable_actions: 1755,
  families: [
    { id: "discover", label: "Discover", tools: ["list_datasets", "search_data", "search_connectors"], tool_count: 3, generated_actions: 620 },
    { id: "profile", label: "Profile", tools: ["analyze_dataset", "compare_datasets", "profile_quality_rules"], tool_count: 3, generated_actions: 180 },
    { id: "move", label: "Move", tools: ["plan_transfer_route", "get_transfer_capabilities", "recommend_sync_mode"], tool_count: 3, generated_actions: 720 },
    { id: "govern", label: "Govern", tools: ["explain_mapping_assurance", "inspect_schema_policy"], tool_count: 2, generated_actions: 140 },
    { id: "operate", label: "Operate", tools: ["list_jobs", "navigate"], tool_count: 2, generated_actions: 80 },
  ],
  tools: [],
};

export function AICopilot({ onNavigate, variant = "fab", onClose }: AICopilotProps) {
  const { activeData } = useActiveData();
  const [open, setOpen] = useState(variant === "rail");
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [prompts, setPrompts] = useState<string[]>([]);
  const [history, setHistory] = useState<CopilotChatMessage[]>([]);
  const [toolRegistry, setToolRegistry] = useState<PilotToolRegistry | null>(null);
  const [messages, setMessages] = useState<Message[]>([
    {
      role: "assistant",
      text: "I'm **Data Pilot** — I can plan routes, inspect schema risk, explain mappings, and take you to the right workspace.",
    },
  ]);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if ((open || variant === "rail") && prompts.length === 0) {
      fetchCopilotPrompts().then(setPrompts).catch(() => {});
    }
    if ((open || variant === "rail") && !toolRegistry) {
      fetchPilotTools().then(setToolRegistry).catch(() => setToolRegistry(FALLBACK_TOOL_REGISTRY));
    }
  }, [open, variant, prompts.length, toolRegistry]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  const applyActions = (actions?: CopilotAction[]) => {
    if (!actions?.length || !onNavigate) return;
    for (const action of actions) {
      if (action.type === "navigate" && action.screen) {
        onNavigate(action.screen as Screen);
      } else if (action.route) {
        onNavigate(action.route as Screen);
      }
    }
  };

  const send = async (text?: string) => {
    const q = (text ?? input).trim();
    if (!q || loading) return;
    setInput("");
    setMessages((m) => [...m, { role: "user", text: q }]);
    setLoading(true);

    try {
      const res = await copilotChat(q, history, activeData);
      const newHistory: CopilotChatMessage[] = [
        ...history,
        { role: "user", content: q },
        { role: "assistant", content: res.answer },
      ];
      setHistory(newHistory.slice(-20));
      setMessages((m) => [
        ...m,
        {
          role: "assistant",
          text: res.answer,
          method: res.method,
          actions: res.suggested_actions,
          dataInsight: res.data_insight,
          toolsUsed: res.tools_used,
        },
      ]);
      applyActions(res.suggested_actions);
      if (res.suggested_prompts?.length) {
        setPrompts(res.suggested_prompts);
      }
    } catch {
      setMessages((m) => [
        ...m,
        { role: "assistant", text: "Could not reach Data Pilot. Ensure the API is running on port 8001." },
      ]);
    }
    setLoading(false);
  };

  const panel = (
    <div className="df2-copilot">
      <div className="df2-copilot-head">
        <div>
          <div style={{ fontWeight: 600, fontSize: 14 }}>Data Pilot</div>
          <div style={{ fontSize: 12, color: "#64748b" }}>
            {activeData
              ? `Context: ${activeData.filename || activeData.name}`
              : "Any data question · app actions"}
          </div>
        </div>
        <button
          type="button"
          className="df2-btn df2-btn-ghost df2-btn-sm"
          onClick={() => (variant === "rail" ? onClose?.() : setOpen(false))}
          aria-label="Close"
        >
          <DtIcon name="x" size={16} />
        </button>
      </div>

      {activeData && (
        <div className="df2-copilot-context">
          <DtIcon name="zap" size={12} />
          {activeData.columns.length} cols · {activeData.row_count.toLocaleString()} rows in context
        </div>
      )}

      {toolRegistry && (
        <div className="df2-copilot-console">
          <div>
            <span>Tools</span>
            <strong>{toolRegistry.tool_count}</strong>
          </div>
          <div>
            <span>Actions</span>
            <strong>{toolRegistry.total_routable_actions}</strong>
          </div>
          <div>
            <span>Families</span>
            <strong>{toolRegistry.families.length}</strong>
          </div>
        </div>
      )}

      {prompts.length > 0 && messages.length <= 2 && (
        <div className="df2-copilot-suggestions">
          {prompts.slice(0, 3).map((p) => (
            <button key={p} type="button" onClick={() => send(p)}>{p}</button>
          ))}
        </div>
      )}

      <div className="df2-copilot-msgs">
        {messages.map((msg, i) => (
          <div key={i} className={`df2-copilot-msg ${msg.role}`}>
            <div dangerouslySetInnerHTML={{ __html: renderMarkdown(msg.text) }} />
            {(msg.method || msg.toolsUsed?.length) && (
              <div className="df2-copilot-evidence">
                {msg.method && <span>{msg.method}</span>}
                {msg.toolsUsed?.slice(0, 3).map((tool) => (
                  <span key={tool.name} className={tool.success ? "ok" : "err"}>{tool.name}</span>
                ))}
              </div>
            )}
            {msg.actions && msg.actions.length > 0 && (
              <div className="df2-copilot-actions">
                {msg.actions.map((action, j) => {
                  const screen = action.screen || action.route;
                  const label = action.label || (screen ? `Open ${SCREEN_LABELS[screen] || screen}` : "Action");
                  return (
                    <button
                      key={j}
                      type="button"
                      className="df2-btn df2-btn-sm"
                      onClick={() => screen && onNavigate?.(screen as Screen)}
                    >
                      {label}
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        ))}
        {loading && (
          <div className="df2-copilot-msg assistant df2-copilot-thinking">
            <span className="df2-loader-bars" aria-label="Data Pilot is working"><i /><i /><i /></span>
            <span>Routing tools</span>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      <div className="df2-copilot-input-row">
        <input
          placeholder={activeData ? `Ask about ${activeData.name}…` : "Move data, analyze, or navigate…"}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && send()}
        />
        <button type="button" className="df2-btn df2-btn-primary" onClick={() => send()} disabled={loading}>
          <DtIcon name="transfer" size={16} />
          Send
        </button>
      </div>
    </div>
  );

  if (variant === "rail") return panel;

  return (
    <>
      <button
        type="button"
        className="df2-btn df2-btn-primary"
        style={{ position: "fixed", bottom: 24, right: 24, width: 48, height: 48, borderRadius: "50%", padding: 0 }}
        onClick={() => setOpen(!open)}
        aria-label="Data Pilot"
      >
        <DtIcon name="sparkle" size={22} />
      </button>
      {open && (
        <div style={{ position: "fixed", bottom: 88, right: 24, width: 380, maxHeight: 560, zIndex: 50, boxShadow: "0 12px 32px rgba(15,23,42,0.15)", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0" }}>
          {panel}
        </div>
      )}
    </>
  );
}
