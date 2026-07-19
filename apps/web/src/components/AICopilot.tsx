import { useState, useEffect, useRef } from "react";
import { DtIcon } from "./DtIcon";
import {
  copilotChat,
  fetchCopilotPrompts,
  formatPilotReachError,
  CopilotAction,
  CopilotChatMessage,
} from "../lib/api";
import { useActiveData } from "../lib/DataContext";
import { API_BASE, Screen } from "../lib/types";
import { renderSafeMarkdown } from "../lib/safeMarkdown";
import { loadRailChat, PilotMessage, saveRailChat } from "../lib/pilotChatStore";

interface Message extends PilotMessage {
  dataInsight?: {
    dataset: string;
    columns: number;
    rows: number;
    pii_count: number;
    quality_score: number;
  };
}

const DEFAULT_GREETING: Message = {
  role: "assistant",
  text: "I'm **Data Pilot** — I can plan routes, inspect schema risk, explain mappings, and take you to the right workspace. Paste a `pf_…` validation run ID or a job ID to triage failures.",
};

interface AICopilotProps {
  onNavigate?: (screen: Screen) => void;
  variant?: "fab" | "rail";
  onClose?: () => void;
}

const SCREEN_LABELS: Record<string, string> = {
  dashboard: "Overview",
  pilot: "Data Pilot",
  transfer: "Transfer Studio",
  connectors: "Connectors",
  jobs: "Jobs",
  settings: "Settings",
};

export function AICopilot({ onNavigate, variant = "fab", onClose }: AICopilotProps) {
  const { activeData } = useActiveData();
  const [open, setOpen] = useState(variant === "rail");
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [prompts, setPrompts] = useState<string[]>([]);
  const restored = useRef(loadRailChat());
  const [history, setHistory] = useState<CopilotChatMessage[]>(() => restored.current?.history ?? []);
  const [messages, setMessages] = useState<Message[]>(() => {
    const saved = restored.current?.messages;
    return saved && saved.length > 0 ? saved : [DEFAULT_GREETING];
  });
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if ((open || variant === "rail") && prompts.length === 0) {
      fetchCopilotPrompts().then(setPrompts).catch(() => {});
    }
  }, [open, variant, prompts.length]);

  useEffect(() => {
    // Persist rail chat so close/refresh does not wipe the conversation.
    if (messages.length > 1 || history.length > 0) {
      saveRailChat({ messages, history });
    }
  }, [messages, history]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  const applyActions = (actions?: CopilotAction[]) => {
    if (!actions?.length) return;
    for (const action of actions) {
      if (action.risk === "mutate" || action.type === "studio") continue;
      if (action.type === "navigate" && action.screen && onNavigate) {
        onNavigate(action.screen as Screen);
      } else if (action.route && onNavigate) {
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
          actions: res.suggested_actions,
          dataInsight: res.data_insight,
        },
      ]);
      applyActions(res.suggested_actions);
      if (res.suggested_prompts?.length) {
        setPrompts(res.suggested_prompts);
      }
    } catch (error) {
      const detail = formatPilotReachError(error, API_BASE);
      setMessages((m) => [
        ...m,
        { role: "assistant", text: detail },
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
            {activeData?.preflight_run_id
              ? `Run ${activeData.preflight_run_id}`
              : activeData
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
          {(activeData.columns?.length ?? 0).toLocaleString()} cols
          {activeData.row_count != null ? ` · ${activeData.row_count.toLocaleString()} rows` : ""}
          {activeData.preflight_run_id ? ` · ${activeData.preflight_run_id}` : ""}
          {activeData.job_id ? ` · job ${activeData.job_id}` : ""}
          {activeData.validation_status ? ` · ${activeData.validation_status}` : ""}
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
            <div dangerouslySetInnerHTML={{ __html: renderSafeMarkdown(msg.text) }} />
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
            <span>Looking that up…</span>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      <div className="df2-copilot-input-row">
        <input
          placeholder={
            activeData?.preflight_run_id
              ? `Ask about ${activeData.preflight_run_id} or say “strip controls”…`
              : activeData
                ? `Ask about ${activeData.name}…`
                : "Ask about jobs, pf_ run IDs, or say strip controls…"
          }
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              send();
            }
          }}
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
