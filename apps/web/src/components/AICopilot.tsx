import { useState, useEffect, useRef } from "react";
import { DtIcon } from "./DtIcon";
import { copilotChat, fetchCopilotPrompts, CopilotAction, CopilotChatMessage } from "../lib/api";
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
}

interface AICopilotProps {
  onNavigate?: (screen: Screen) => void;
}

function renderMarkdown(text: string) {
  return text
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/`(.+?)`/g, "<code>$1</code>")
    .replace(/\n/g, "<br/>");
}

const SCREEN_LABELS: Record<string, string> = {
  dashboard: "Dashboard",
  pilot: "Data Pilot",
  transfer: "New Transfer",
  connectors: "Connectors",
  jobs: "Jobs",
  settings: "Settings",
};

export function AICopilot({ onNavigate }: AICopilotProps) {
  const { activeData } = useActiveData();
  const [open, setOpen] = useState(false);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [prompts, setPrompts] = useState<string[]>([]);
  const [history, setHistory] = useState<CopilotChatMessage[]>([]);
  const [messages, setMessages] = useState<Message[]>([
    {
      role: "assistant",
      text: "I'm **Data Pilot** — your AI agent. Ask me anything about your data, or tell me what to do: analyze datasets, show jobs, start transfers, open connectors.",
    },
  ]);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open && prompts.length === 0) {
      fetchCopilotPrompts().then(setPrompts).catch(() => {});
    }
  }, [open, prompts.length]);

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

  const methodLabel = (method?: string) => {
    if (!method) return null;
    if (method.includes("anthropic")) return "Claude agent";
    if (method.includes("openai")) return "GPT agent";
    if (method === "pilot_local_agent") return "Data Pilot";
    if (method === "data_analysis") return "Data analysis";
    return method;
  };

  return (
    <>
      <button type="button" className="dt-copilot-fab" onClick={() => setOpen(!open)} aria-label="Data Pilot">
        <DtIcon name="sparkle" size={22} />
      </button>

      {open && (
        <div className="dt-copilot-panel">
          <div className="dt-copilot-header">
            <div>
              <div className="dt-font-semibold">Data Pilot</div>
              <div className="dt-text-sm dt-text-muted">
                {activeData
                  ? `Context: ${activeData.filename || activeData.name}`
                  : "Agent · any data question · app actions"}
              </div>
            </div>
            <button type="button" className="dt-btn dt-btn-ghost dt-btn-icon" onClick={() => setOpen(false)}>
              <DtIcon name="x" size={16} />
            </button>
          </div>

          {activeData && (
            <div style={{ padding: "8px 16px", borderBottom: "1px solid var(--dt-border)", fontSize: 11, color: "var(--dt-text-muted)" }}>
              {activeData.columns.length} columns · {activeData.row_count.toLocaleString()} rows in context
            </div>
          )}

          {prompts.length > 0 && messages.length <= 2 && (
            <div className="dt-copilot-suggestions">
              {prompts.slice(0, 4).map((p) => (
                <button key={p} type="button" className="dt-copilot-chip" onClick={() => send(p)}>
                  {p}
                </button>
              ))}
            </div>
          )}

          <div className="dt-copilot-messages">
            {messages.map((msg, i) => (
              <div key={i} className={`dt-copilot-msg ${msg.role}`}>
                <div dangerouslySetInnerHTML={{ __html: renderMarkdown(msg.text) }} />
                {msg.actions && msg.actions.length > 0 && (
                  <div className="dt-flex dt-gap-2 dt-mt-2" style={{ flexWrap: "wrap" }}>
                    {msg.actions.map((action, j) => {
                      const screen = action.screen || action.route;
                      const label = action.label || (screen ? `Open ${SCREEN_LABELS[screen] || screen}` : "Action");
                      return (
                        <button
                          key={j}
                          type="button"
                          className="dt-btn dt-btn-sm"
                          onClick={() => screen && onNavigate?.(screen as Screen)}
                        >
                          {label}
                        </button>
                      );
                    })}
                  </div>
                )}
                {(msg.dataInsight || msg.method) && (
                  <div className="dt-copilot-data-badge">
                    <DtIcon name="zap" size={10} />
                    {msg.dataInsight
                      ? `${msg.dataInsight.dataset} · ${msg.dataInsight.columns} cols · Q${msg.dataInsight.quality_score?.toFixed(0)}%`
                      : methodLabel(msg.method)}
                  </div>
                )}
              </div>
            ))}
            {loading && (
              <div className="dt-copilot-msg assistant">
                <span className="dt-spinner" style={{ width: 16, height: 16 }} />
                <span className="dt-text-sm dt-text-muted dt-ml-2">Thinking…</span>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>

          <div className="dt-copilot-input">
            <input
              className="dt-input"
              placeholder={activeData ? `Ask or command about ${activeData.name}…` : "Ask anything · \"show my jobs\" · \"analyze HR data\"…"}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && send()}
            />
            <button type="button" className="dt-btn dt-btn-primary" onClick={() => send()} disabled={loading}>
              Send
            </button>
          </div>
        </div>
      )}
    </>
  );
}
