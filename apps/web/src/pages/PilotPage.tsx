import { useEffect, useRef, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import {
  copilotChat,
  CopilotAction,
  CopilotChatMessage,
  fetchCopilotPrompts,
  fetchCopilotStatus,
  fetchModelCapabilities,
  fetchPilotTools,
  ModelCapabilities,
  PilotToolRegistry,
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

export function PilotPage({ onNavigate }: PilotPageProps) {
  const { activeData } = useActiveData();
  const [sessions, setSessions] = useState<Session[]>([newSession()]);
  const [activeId, setActiveId] = useState(sessions[0].id);
  const [input, setInput] = useState("");
  const [category, setCategory] = useState("all");
  const [loading, setLoading] = useState(false);
  const [prompts, setPrompts] = useState<string[]>([]);
  const [trainingInfo, setTrainingInfo] = useState<{ docs: number; ready: boolean } | null>(null);
  const [toolRegistry, setToolRegistry] = useState<PilotToolRegistry>(FALLBACK_TOOL_REGISTRY);
  const [modelCapabilities, setModelCapabilities] = useState<ModelCapabilities | null>(null);
  const endRef = useRef<HTMLDivElement>(null);

  const session = sessions.find((s) => s.id === activeId) ?? sessions[0];
  const started = session.messages.length > 0;

  useEffect(() => {
    fetchCopilotPrompts().then(setPrompts).catch(() => {});
    fetchPilotTools().then(setToolRegistry).catch(() => {});
    fetchModelCapabilities().then(setModelCapabilities).catch(() => {});
    fetchCopilotStatus().then((s) => {
      const rag = s.rag as { document_count?: number } | undefined;
      const agent = s.training_agent as {
        last_run?: { metrics?: { copilot_evaluation?: { ready?: boolean } } };
      } | undefined;
      const registry = s.tool_registry as PilotToolRegistry | undefined;
      const models = s.model_capabilities as ModelCapabilities | undefined;
      if (registry?.tool_count) setToolRegistry(registry);
      if (models?.active_provider) setModelCapabilities(models);
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
    <div className="df2-pilot">
      <aside className="df2-pilot-aside">
        <button type="button" className="df2-btn df2-btn-primary" style={{ width: "100%" }} onClick={startNewChat}>
          <DtIcon name="plus" size={16} /> New chat
        </button>

        <div className="df2-pilot-aside-scroll">
          <div className="df2-pilot-section-label">Sessions</div>
          {sessions.map((s) => (
            <button
              key={s.id}
              type="button"
              className={`df2-pilot-session ${s.id === activeId ? "active" : ""}`}
              onClick={() => setActiveId(s.id)}
            >
              {s.title}
            </button>
          ))}

          <div className="df2-pilot-section-label">Tool calls</div>
          {session.toolLog.length === 0 ? (
            <p style={{ margin: 0, fontSize: 12, color: "#94a3b8" }}>Tools run live as Data Pilot works.</p>
          ) : (
            session.toolLog.map((t, i) => (
              <div key={i} className={`df2-pilot-tool-log ${t.success ? "ok" : "err"}`}>
                <code>{t.name}</code>
                <span>{t.summary}</span>
                <time style={{ fontSize: 10, color: "#94a3b8" }}>{t.at}</time>
              </div>
            ))
          )}

          <div className="df2-pilot-section-label">Tool registry</div>
          <div className="df2-pilot-tool-families">
            {toolRegistry.families.map((family) => (
              <div key={family.id} className="df2-pilot-tool-family">
                <div>
                  <strong>{family.label}</strong>
                  <span>{family.tool_count} tools · {family.generated_actions.toLocaleString()} actions</span>
                </div>
                <span className="df2-pilot-family-count">{family.tools.length}</span>
              </div>
            ))}
          </div>
        </div>

        {trainingInfo && (
          <div className="df2-pilot-training">
            <DtIcon name="sparkle" size={14} />
            <span>{trainingInfo.docs.toLocaleString()} trained docs</span>
            {trainingInfo.ready && <span className="df2-badge df2-badge-live">Ready</span>}
          </div>
        )}
      </aside>

      <div className="df2-pilot-main">
        {!started ? (
          <div className="df2-pilot-main-inner">
            <div className="df2-pilot-console-strip" aria-label="Data Pilot readiness">
              <div>
                <span>Active model</span>
                <strong>{modelCapabilities?.active_provider ?? "local"}</strong>
              </div>
              <div>
                <span>Tool families</span>
                <strong>{toolRegistry.families.length}</strong>
              </div>
              <div>
                <span>Deterministic tools</span>
                <strong>{toolRegistry.tool_count}</strong>
              </div>
              <div>
                <span>Routable actions</span>
                <strong>{toolRegistry.total_routable_actions.toLocaleString()}</strong>
              </div>
              <div>
                <span>Knowledge docs</span>
                <strong>{trainingInfo?.docs.toLocaleString() ?? "—"}</strong>
              </div>
            </div>

            <div className="df2-model-mini-strip" aria-label="Model provider routing">
              {(modelCapabilities?.providers ?? [
                { provider: "anthropic", label: "Anthropic Claude", available: false, status: "configure", default_model: "claude-sonnet-4-20250514" },
                { provider: "openai", label: "OpenAI", available: false, status: "configure", default_model: "gpt-4o-mini" },
                { provider: "ollama", label: "Ollama", available: false, status: "offline", default_model: "llama3.2" },
                { provider: "local", label: "Local deterministic", available: true, status: "ready", default_model: "local_knowledge" },
              ]).map((provider) => (
                <div key={provider.provider} className={provider.available ? "ready" : ""}>
                  <span>{provider.label}</span>
                  <strong>{provider.default_model}</strong>
                  <small>{provider.available ? "ready" : provider.status}</small>
                </div>
              ))}
            </div>

            <div className="df2-pilot-hero">
              <div className="df2-pilot-hero-icon"><DtIcon name="sparkle" size={28} /></div>
              <h1 className="df2-pilot-title">Ask Data Pilot to move, inspect, govern, or repair data.</h1>
              <p className="df2-pilot-subtitle">
                Tool-backed agent execution with schema evidence, connector actions, transfer planning, quality gates, and mapping assurance.
              </p>
            </div>

            <div className="df2-pilot-composer">
              <textarea
                rows={3}
                placeholder="Set up Postgres source, move Shopify orders to Snowflake, scan HR for PII…"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } }}
              />
              <div className="df2-pilot-composer-foot">
                <button type="button" className="df2-btn df2-btn-primary" onClick={() => send()} disabled={!input.trim()}>
                  Let's go →
                </button>
              </div>
            </div>

            {prompts.length > 0 && (
              <>
                <p className="df2-section-label">Suggested prompts</p>
                <div className="df2-pilot-quick">
                  {prompts.slice(0, 4).map((p) => (
                    <button key={p} type="button" onClick={() => send(p)}>{p}</button>
                  ))}
                </div>
              </>
            )}

            <div className="df2-pilot-capability-grid">
              {toolRegistry.families.map((family) => (
                <button
                  key={family.id}
                  type="button"
                  className="df2-pilot-capability"
                  onClick={() => send(`Use ${family.label} tools for my current data and explain what you can do.`)}
                >
                  <span>{family.label}</span>
                  <strong>{family.generated_actions.toLocaleString()} actions</strong>
                  <small>{family.tools.slice(0, 3).join(" · ")}</small>
                </button>
              ))}
            </div>

            <div className="df2-chips" style={{ justifyContent: "center", marginBottom: 24 }}>
              {AUTOMATION_CATEGORIES.map((c) => (
                <button key={c.id} type="button" className={`df2-chip ${category === c.id ? "active" : ""}`} onClick={() => setCategory(c.id)}>
                  {c.label}
                </button>
              ))}
            </div>

            <p className="df2-section-label">Or start from an idea</p>
            <div className="df2-pilot-ideas">
              {ideas.slice(0, 6).map((idea) => (
                <button key={idea.id} type="button" className="df2-pilot-idea" onClick={() => send(idea.prompt)}>
                  <span className="df2-pilot-idea-cat">{idea.category.replace("_", " ")}</span>
                  <span className="df2-pilot-idea-title">{idea.title}</span>
                  <span className="df2-pilot-idea-desc">{idea.description}</span>
                </button>
              ))}
            </div>
          </div>
        ) : (
          <>
            <div className="df2-pilot-thread">
              {session.messages.map((msg, i) => (
                <div key={i} className={`df2-pilot-msg ${msg.role}`}>
                  <div dangerouslySetInnerHTML={{ __html: renderMarkdown(msg.text) }} />
                  {msg.tools_used && msg.tools_used.length > 0 && (
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginTop: 8 }}>
                      {msg.tools_used.map((t) => (
                        <span key={t.name} className={`df2-badge ${t.success ? "df2-badge-live" : "df2-badge-error"}`}>
                          {t.name}: {t.summary}
                        </span>
                      ))}
                    </div>
                  )}
                  {msg.actions?.map((a, j) => {
                    const screen = a.screen || a.route;
                    return screen ? (
                      <button key={j} type="button" className="df2-btn df2-btn-sm" style={{ marginTop: 8 }} onClick={() => onNavigate(screen as Screen)}>
                        Open {screen}
                      </button>
                    ) : null;
                  })}
                </div>
              ))}
              {loading && (
                <div className="df2-pilot-msg assistant df2-pilot-thinking">
                  <span className="df2-loader-bars" aria-hidden><i /><i /><i /></span>
                  Running tools on your data…
                </div>
              )}
              <div ref={endRef} />
            </div>

            <div className="df2-pilot-composer-sticky">
              <textarea
                rows={2}
                placeholder="Follow up…"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } }}
              />
              <button type="button" className="df2-btn df2-btn-primary" onClick={() => send()} disabled={loading || !input.trim()}>
                Send
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
