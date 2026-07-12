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
import { useToast } from "../components/Toast";
import { renderSafeMarkdown } from "../lib/safeMarkdown";
import { PageFrame } from "../components/ui/PageFrame";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageShell } from "../components/ui/PageShell";
import { EmptyState } from "../components/EmptyState";

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
  const { toast } = useToast();
  const { activeData } = useActiveData();
  const [sessions, setSessions] = useState<Session[]>([newSession()]);
  const [activeId, setActiveId] = useState(sessions[0].id);
  const [input, setInput] = useState("");
  const [category, setCategory] = useState("all");
  const [loading, setLoading] = useState(false);
  const [pilotOnline, setPilotOnline] = useState<boolean | null>(null);
  const [prompts, setPrompts] = useState<string[]>([]);
  const [trainingInfo, setTrainingInfo] = useState<{ docs: number; ready: boolean }>({ docs: 0, ready: false });
  const [toolRegistry, setToolRegistry] = useState<PilotToolRegistry | null>(null);
  const [toolsLoading, setToolsLoading] = useState(true);
  const [modelsLoading, setModelsLoading] = useState(true);
  const [modelCapabilities, setModelCapabilities] = useState<ModelCapabilities | null>(null);
  const endRef = useRef<HTMLDivElement>(null);
  const threadRef = useRef<HTMLDivElement>(null);
  const metaReady = !toolsLoading && !modelsLoading;

  const session = sessions.find((s) => s.id === activeId) ?? sessions[0];
  const started = session.messages.length > 0;
  const cloudProviders = (modelCapabilities?.providers ?? []).filter((p) => p.tier === "cloud");
  const anyCloudReady = cloudProviders.some((p) => p.available);
  const activeProvider = modelCapabilities?.active_provider ?? "local";
  const emptyRegistry: PilotToolRegistry = {
    tool_count: 0,
    generated_action_count: 0,
    total_routable_actions: 0,
    families: [],
    tools: [],
  };
  const displayRegistry = toolRegistry ?? emptyRegistry;

  useEffect(() => {
    setToolsLoading(true);
    fetchCopilotPrompts().then(setPrompts).catch(() => {});
    fetchPilotTools()
      .then(setToolRegistry)
      .catch(() => setToolRegistry(null))
      .finally(() => setToolsLoading(false));
    fetchModelCapabilities()
      .then(setModelCapabilities)
      .catch(() => setModelCapabilities(null))
      .finally(() => setModelsLoading(false));
    fetchCopilotStatus().then((s) => {
      setPilotOnline(true);
      const rag = s.rag as { document_count?: number } | undefined;
      const agent = s.training_agent as {
        last_run?: { metrics?: { copilot_evaluation?: { ready?: boolean } } };
      } | undefined;
      // Only fill metadata from /copilot/status if the dedicated endpoints
      // have not already returned. Use updater functions so the latest state
      // is checked, avoiding a stale-closure override when /copilot/status
      // arrives after tools/models.
      const registry = s.tool_registry as PilotToolRegistry | undefined;
      const models = s.model_capabilities as ModelCapabilities | undefined;
      if (registry?.tool_count) setToolRegistry((current) => current ?? registry);
      if (models?.active_provider) setModelCapabilities((current) => current ?? models);
      setTrainingInfo({
        docs: rag?.document_count ?? 0,
        ready: agent?.last_run?.metrics?.copilot_evaluation?.ready ?? false,
      });
    }).catch(() => {
      setPilotOnline(false);
    });
  }, []);

  useEffect(() => {
    const el = threadRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 120;
    if (nearBottom) {
      endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
    }
  }, [session.messages.length, loading]);

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
      setPilotOnline(false);
      toast({ title: "Data Pilot unavailable", message: "Start the API on port 8001 and retry.", tone: "error" });
      updateSession(activeId, {
        messages: [...nextMessages, { role: "assistant", text: "Data Pilot unavailable — check that the API is running on port 8001 (`cd apps/api && python3 -m uvicorn src.main:app --reload --port 8001`)." }],
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

  const pilotInsightPill =
    pilotOnline === false ? "Offline" : pilotOnline && modelCapabilities && !anyCloudReady ? "Local engine" : pilotOnline ? "Online" : "Connecting…";
  const pilotInsightMessage =
    pilotOnline === false
      ? "Start the API server to enable tool-backed chat and agent actions."
      : pilotOnline && modelCapabilities && !anyCloudReady
        ? "Cloud models are not configured — Pilot uses local tools and RAG. Add API keys in Settings → AI Models for richer responses."
        : "Natural language data ops — schema introspection, mapping, and job triage.";

  return (
    <PageShell
      title="Data Pilot"
      wide
      fit
      showHeader={false}
      className="df2-page-pilot"
    >
      <PageFrame className="df2-pilot-workspace">
        <div className="df2-pilot-status-bar" role="status">
          <div>
            <strong>Data Pilot</strong>
            <span> · {pilotInsightPill}</span>
            <span className="df2-pilot-status-message"> — {pilotInsightMessage}</span>
          </div>
          <div className="df2-pilot-status-metrics">
            {!metaReady ? (
              <>
                <span className="df2-pilot-skeleton df2-pilot-skeleton--short" aria-hidden>model</span>
                <span className="df2-pilot-skeleton df2-pilot-skeleton--short" aria-hidden>tools</span>
                <span className="df2-pilot-skeleton df2-pilot-skeleton--short" aria-hidden>actions</span>
                <span className="df2-pilot-skeleton df2-pilot-skeleton--short" aria-hidden>docs</span>
              </>
            ) : (
              <>
                <span><strong>{modelCapabilities?.active_provider ?? "local"}</strong> model</span>
                <span><strong>{displayRegistry.tool_count}</strong> tools</span>
                <span><strong>{displayRegistry.total_routable_actions.toLocaleString()}</strong> actions</span>
                <span><strong>{trainingInfo.docs.toLocaleString()}</strong> docs</span>
              </>
            )}
          </div>
          <div className="df2-page-actions-group">
            {pilotOnline && modelCapabilities && !anyCloudReady && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={() => onNavigate("settings")}>
                Configure models
              </button>
            )}
            <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={startNewChat}>
              <DtIcon name="plus" size={14} /> New chat
            </button>
          </div>
        </div>
      <div className="df2-pilot-body">
      <aside className="df2-pilot-aside">
        <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-btn-block" onClick={startNewChat}>
          <DtIcon name="plus" size={14} /> New session
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
            <EmptyState compact icon="zap" title="No tool calls yet" description="Tools run live as Data Pilot works." />
          ) : (
            session.toolLog.map((t, i) => (
              <div key={i} className={`df2-pilot-tool-log ${t.success ? "ok" : "err"}`}>
                <code>{t.name}</code>
                <span>{t.summary}</span>
                <time className="df2-pilot-muted">{t.at}</time>
              </div>
            ))
          )}

          <details className="df2-pilot-tool-registry-details">
            <summary>Tool registry ({displayRegistry.families.length})</summary>
            <div className="df2-pilot-tool-families">
              {toolsLoading ? (
                <p className="df2-cell-meta" aria-live="polite">Loading tool registry…</p>
              ) : displayRegistry.families.length === 0 ? (
                <EmptyState compact icon="zap" title="No tools loaded" description="Start the API on port 8001." />
              ) : (
                displayRegistry.families.map((family) => (
                  <div key={family.id} className="df2-pilot-tool-family">
                    <div>
                      <strong>{family.label}</strong>
                      <span>{family.tool_count} tools · {family.generated_actions.toLocaleString()} actions</span>
                    </div>
                    <span className="df2-pilot-family-count">{family.tools.length}</span>
                  </div>
                ))
              )}
            </div>
          </details>
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
            <div className="df2-model-mini-strip" aria-label="Model provider routing">
              {modelsLoading ? (
                <>
                  {[1, 2, 3].map((i) => (
                    <div key={i} className="df2-pilot-skeleton-card">
                      <span className="df2-pilot-skeleton df2-pilot-skeleton--wide" style={{ display: "block", marginBottom: 8 }} />
                      <span className="df2-pilot-skeleton df2-pilot-skeleton--wide" style={{ display: "block", maxWidth: 100 }} />
                    </div>
                  ))}
                </>
              ) : (modelCapabilities?.providers ?? []).length === 0 ? (
                <EmptyState compact icon="sparkle" title="Local engine only" description="No cloud providers configured — add API keys in Settings." />
              ) : (
                modelCapabilities!.providers.map((provider) => (
                  <div key={provider.provider} className={provider.available ? "ready" : ""}>
                    <span>{provider.label}</span>
                    <strong>{provider.default_model}</strong>
                    <small>{provider.available ? "ready" : provider.status}</small>
                  </div>
                ))
              )}
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
              {!metaReady ? (
                <>
                  {Array.from({ length: 4 }).map((_, i) => (
                    <div key={i} className="df2-pilot-capability-skeleton" aria-hidden>
                      <span className="df2-pilot-skeleton df2-pilot-skeleton--wide" />
                      <span className="df2-pilot-skeleton df2-pilot-skeleton--medium" />
                      <span className="df2-pilot-skeleton df2-pilot-skeleton--wide" />
                    </div>
                  ))}
                </>
              ) : displayRegistry.families.length === 0 ? (
                <EmptyState compact icon="zap" title="No tool families" description="Tool families appear when the API is online." />
              ) : (
                displayRegistry.families.slice(0, 4).map((family) => (
                  <button
                    key={family.id}
                    type="button"
                    className="df2-pilot-capability"
                    onClick={() => send(`Use ${family.label} tools for my current data and explain what you can do.`)}
                    disabled={pilotOnline === false}
                  >
                    <span>{family.label}</span>
                    <strong>{family.generated_actions.toLocaleString()} actions</strong>
                    <small>{family.tools.slice(0, 3).join(" · ")}</small>
                  </button>
                ))
              )}
            </div>

            <FilterTabs
              ariaLabel="Automation ideas by category"
              className="df2-filter-tabs--center"
              value={category}
              onChange={setCategory}
              items={[
                { id: "all", label: "All" },
                ...AUTOMATION_CATEGORIES.filter((c) => c.id !== "all").map((c) => ({ id: c.id, label: c.label })),
              ]}
            />

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
            <div className="df2-pilot-thread" ref={threadRef}>
              {session.messages.map((msg, i) => (
                <div key={i} className={`df2-pilot-msg ${msg.role}`}>
                  <div dangerouslySetInnerHTML={{ __html: renderSafeMarkdown(msg.text) }} />
                  {msg.tools_used && msg.tools_used.length > 0 && (
                    <div className="df2-pilot-tool-badges">
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
                      <button key={j} type="button" className="df2-btn df2-btn-sm df2-mt-sm" onClick={() => onNavigate(screen as Screen)}>
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
      </PageFrame>
    </PageShell>
  );
}
