import { useEffect, useRef, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import {
  copilotChat,
  CopilotAction,
  CopilotChatMessage,
  fetchCopilotPrompts,
  fetchCopilotStatus,
} from "../lib/api";
import { AUTOMATION_CATEGORIES, AUTOMATION_IDEAS } from "../lib/automationIdeas";
import { useActiveData } from "../lib/DataContext";
import { Screen } from "../lib/types";

interface PilotPageProps {
  onNavigate: (screen: Screen) => void;
}

interface Session {
  id: string;
  title: string;
  messages: Message[];
  history: CopilotChatMessage[];
  toolLog: ToolLogEntry[];
}

interface Message {
  role: "user" | "assistant";
  text: string;
  method?: string;
  actions?: CopilotAction[];
  tools_used?: { name: string; success: boolean; summary: string }[];
}

interface ToolLogEntry {
  name: string;
  success: boolean;
  summary: string;
  at: string;
}

function renderMarkdown(text: string) {
  return text
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/`(.+?)`/g, "<code>$1</code>")
    .replace(/\n/g, "<br/>");
}

function newSession(): Session {
  return {
    id: crypto.randomUUID(),
    title: "New conversation",
    messages: [],
    history: [],
    toolLog: [],
  };
}

export function PilotPage({ onNavigate }: PilotPageProps) {
  const { activeData } = useActiveData();
  const [sessions, setSessions] = useState<Session[]>([newSession()]);
  const [activeId, setActiveId] = useState(sessions[0].id);
  const [input, setInput] = useState("");
  const [category, setCategory] = useState("all");
  const [loading, setLoading] = useState(false);
  const [prompts, setPrompts] = useState<string[]>([]);
  const [trainingInfo, setTrainingInfo] = useState<{ docs: number; ready: boolean } | null>(null);
  const endRef = useRef<HTMLDivElement>(null);

  const session = sessions.find((s) => s.id === activeId) ?? sessions[0];
  const started = session.messages.length > 0;

  useEffect(() => {
    fetchCopilotPrompts().then(setPrompts).catch(() => {});
    fetchCopilotStatus().then((s) => {
      const rag = s.rag as { document_count?: number } | undefined;
      const agent = s.training_agent as {
        last_run?: { metrics?: { copilot_evaluation?: { ready?: boolean } } };
      } | undefined;
      setTrainingInfo({
        docs: rag?.document_count ?? 0,
        ready: agent?.last_run?.metrics?.copilot_evaluation?.ready ?? false,
      });
    }).catch(() => {});
  }, []);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [session.messages, loading]);

  const ideas = category === "all"
    ? AUTOMATION_IDEAS
    : AUTOMATION_IDEAS.filter((i) => i.category === category);

  const applyActions = (actions?: CopilotAction[]) => {
    actions?.forEach((a) => {
      const screen = a.screen || a.route;
      if (a.type === "navigate" && screen) onNavigate(screen as Screen);
    });
  };

  const updateSession = (id: string, patch: Partial<Session>) => {
    setSessions((prev) => prev.map((s) => (s.id === id ? { ...s, ...patch } : s)));
  };

  const send = async (text?: string) => {
    const q = (text ?? input).trim();
    if (!q || loading) return;
    setInput("");
    setLoading(true);

    const userMsg: Message = { role: "user", text: q };
    const nextMessages = [...session.messages, userMsg];
    const title = session.title === "New conversation" ? q.slice(0, 48) : session.title;
    updateSession(activeId, { messages: nextMessages, title });

    try {
      const res = await copilotChat(q, session.history, activeData);
      const newHistory: CopilotChatMessage[] = [
        ...session.history,
        { role: "user" as const, content: q },
        { role: "assistant" as const, content: res.answer },
      ].slice(-20);

      const toolEntries: ToolLogEntry[] = (res.tools_used || []).map((t) => ({
        ...t,
        at: new Date().toLocaleTimeString(),
      }));

      updateSession(activeId, {
        history: newHistory,
        messages: [
          ...nextMessages,
          {
            role: "assistant",
            text: res.answer,
            method: res.method,
            actions: res.suggested_actions,
            tools_used: res.tools_used,
          },
        ],
        toolLog: [...toolEntries, ...session.toolLog].slice(0, 30),
      });

      applyActions(res.suggested_actions);
      if (res.suggested_prompts?.length) setPrompts(res.suggested_prompts);
    } catch {
      updateSession(activeId, {
        messages: [...nextMessages, { role: "assistant", text: "Data Pilot unavailable — check API on port 8001." }],
      });
    }
    setLoading(false);
  };

  const startNewChat = () => {
    const s = newSession();
    setSessions((prev) => [s, ...prev]);
    setActiveId(s.id);
    setInput("");
  };

  return (
    <div className="dt-pilot-layout">
      <aside className="dt-pilot-sidebar">
        <button type="button" className="dt-btn dt-btn-primary dt-w-full dt-mb-4" onClick={startNewChat}>
          <DtIcon name="plus" size={16} /> New Chat
        </button>

        <div className="dt-pilot-sidebar-scroll">
          <div className="dt-pilot-sidebar-section">
            <div className="dt-pilot-sidebar-label">Sessions</div>
            {sessions.map((s) => (
              <button
                key={s.id}
                type="button"
                className={`dt-pilot-session ${s.id === activeId ? "active" : ""}`}
                onClick={() => setActiveId(s.id)}
              >
                {s.title}
              </button>
            ))}
          </div>

          <div className="dt-pilot-sidebar-section">
            <div className="dt-pilot-sidebar-label">Tool Calls</div>
            {session.toolLog.length === 0 ? (
              <p className="dt-text-xs dt-text-muted">Tools run live as Data Pilot works — like Cursor agents.</p>
            ) : (
              session.toolLog.map((t, i) => (
                <div key={i} className={`dt-pilot-tool ${t.success ? "ok" : "err"}`}>
                  <code>{t.name}</code>
                  <span>{t.summary}</span>
                  <time>{t.at}</time>
                </div>
              ))
            )}
          </div>
        </div>

        {trainingInfo && (
          <div className="dt-pilot-training-badge">
            <DtIcon name="sparkle" size={14} />
            <span>{trainingInfo.docs.toLocaleString()} trained docs</span>
            {trainingInfo.ready && <span className="dt-badge dt-badge-success dt-badge-sm">Ready</span>}
          </div>
        )}
      </aside>

      <div className={`dt-pilot-main ${started ? "dt-pilot-main-chat" : ""}`}>
        {!started ? (
          <div className="dt-pilot-main-inner">
            <div className="dt-pilot-hero">
              <div className="dt-pilot-hero-icon"><DtIcon name="sparkle" size={28} /></div>
              <h1 className="dt-pilot-title">What should we move, analyze, or automate?</h1>
              <span className="dt-pilot-badge">Universal Data Freedom · Not just ELT</span>
              <p className="dt-pilot-subtitle">
                More powerful than pipeline-only tools — semantic AI, PII gates, any→any transfer, trained on your data.
              </p>
            </div>

            <div className="dt-pilot-composer">
              <textarea
                className="dt-pilot-input"
                rows={3}
                placeholder="Set up Postgres source, move Shopify orders to Snowflake, scan HR for PII…"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } }}
              />
              <div className="dt-pilot-composer-footer">
                <button type="button" className="dt-btn dt-btn-primary dt-pilot-go" onClick={() => send()} disabled={!input.trim()}>
                  Let's go →
                </button>
              </div>
            </div>

            {prompts.length > 0 && (
              <>
                <p className="dt-section-title dt-text-center">Suggested prompts</p>
                <div className="dt-pilot-quick">
                  {prompts.slice(0, 4).map((p) => (
                    <button key={p} type="button" onClick={() => send(p)}>{p}</button>
                  ))}
                </div>
              </>
            )}

            <div className="dt-pilot-categories">
              {AUTOMATION_CATEGORIES.map((c) => (
                <button key={c.id} type="button" className={`dt-pilot-chip ${category === c.id ? "active" : ""}`} onClick={() => setCategory(c.id)}>
                  {c.label}
                </button>
              ))}
            </div>

            <p className="dt-section-title">Or start from an idea</p>
            <div className="dt-pilot-ideas">
              {ideas.slice(0, 6).map((idea) => (
                <button key={idea.id} type="button" className="dt-pilot-idea-card" onClick={() => send(idea.prompt)}>
                  <span className="dt-pilot-idea-cat">{idea.category.replace("_", " ")}</span>
                  <span className="dt-pilot-idea-title">{idea.title}</span>
                  <span className="dt-pilot-idea-desc">{idea.description}</span>
                </button>
              ))}
            </div>
          </div>
        ) : (
          <>
            <div className="dt-pilot-thread">
              {session.messages.map((msg, i) => (
                <div key={i} className={`dt-pilot-msg ${msg.role}`}>
                  <div dangerouslySetInnerHTML={{ __html: renderMarkdown(msg.text) }} />
                  {msg.tools_used && msg.tools_used.length > 0 && (
                    <div className="dt-pilot-msg-tools">
                      {msg.tools_used.map((t) => (
                        <span key={t.name} className={`dt-pilot-tool-chip ${t.success ? "ok" : "err"}`}>
                          {t.name}: {t.summary}
                        </span>
                      ))}
                    </div>
                  )}
                  {msg.actions?.map((a, j) => {
                    const screen = a.screen || a.route;
                    return screen ? (
                      <button key={j} type="button" className="dt-btn dt-btn-sm dt-mt-2" onClick={() => onNavigate(screen as Screen)}>
                        Open {screen}
                      </button>
                    ) : null;
                  })}
                </div>
              ))}
              {loading && (
                <div className="dt-pilot-msg assistant">
                  <span className="dt-spinner" /> Running tools on your data…
                </div>
              )}
              <div ref={endRef} />
            </div>

            <div className="dt-pilot-composer dt-pilot-composer-sticky">
              <textarea
                className="dt-pilot-input"
                rows={2}
                placeholder="Follow up…"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } }}
              />
              <button type="button" className="dt-btn dt-btn-primary" onClick={() => send()} disabled={loading || !input.trim()}>
                Send
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
