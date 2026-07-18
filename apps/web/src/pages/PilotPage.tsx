import { useEffect, useRef, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import {
  copilotChat,
  CopilotAction,
  CopilotChatMessage,
  fetchCopilotPrompts,
  fetchCopilotStatus,
  fetchModelCapabilities,
  ModelCapabilities,
} from "../lib/api";
import { AUTOMATION_CATEGORIES, AUTOMATION_IDEAS } from "../lib/automationIdeas";
import { useActiveData } from "../lib/DataContext";
import { useStudioActions } from "../lib/StudioActionsContext";
import { Screen } from "../lib/types";
import { useToast } from "../components/Toast";
import { renderSafeMarkdown } from "../lib/safeMarkdown";
import { CopyIdChip } from "../components/ui/CopyIdChip";
import { PageFrame } from "../components/ui/PageFrame";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageShell } from "../components/ui/PageShell";

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
  const { dispatchStudioAction } = useStudioActions();
  const [sessions, setSessions] = useState<Session[]>([newSession()]);
  const [activeId, setActiveId] = useState(sessions[0].id);
  const [input, setInput] = useState("");
  const [category, setCategory] = useState("all");
  const [loading, setLoading] = useState(false);
  const [pilotOnline, setPilotOnline] = useState<boolean | null>(null);
  const [prompts, setPrompts] = useState<string[]>([]);
  const [modelCapabilities, setModelCapabilities] = useState<ModelCapabilities | null>(null);
  const endRef = useRef<HTMLDivElement>(null);
  const threadRef = useRef<HTMLDivElement>(null);

  const session = sessions.find((s) => s.id === activeId) ?? sessions[0];
  const started = session.messages.length > 0;
  const cloudProviders = (modelCapabilities?.providers ?? []).filter((p) => p.tier === "cloud");
  const anyCloudReady = cloudProviders.some((p) => p.available);

  useEffect(() => {
    fetchCopilotPrompts().then(setPrompts).catch(() => {});
    fetchModelCapabilities()
      .then(setModelCapabilities)
      .catch(() => setModelCapabilities(null));
    fetchCopilotStatus().then((s) => {
      setPilotOnline(true);
      const models = s.model_capabilities as ModelCapabilities | undefined;
      if (models?.active_provider) setModelCapabilities((current) => current ?? models);
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
      if (a.type === "studio" && a.kind) {
        dispatchStudioAction({ kind: a.kind, label: a.label, run_id: a.run_id });
      }
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
      toast({ title: "Data Pilot unavailable", message: "Check the API URL (VITE_API_BASE / DATAFLOW_API_BASE) or sign in and retry.", tone: "error" });
      updateSession(activeId, {
        messages: [...nextMessages, { role: "assistant", text: "Data Pilot unavailable — the DataFlow API could not be reached. Check the API URL or sign in and retry." }],
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

  const pilotStatusClass =
    pilotOnline === false ? "is-offline" : pilotOnline && modelCapabilities && !anyCloudReady ? "is-local" : "";

  return (
    <PageShell
      title="Data Pilot"
      description="Natural-language triage on the same governed transfer engine."
      wide
      fit
      showHeader={false}
      className="df2-page-pilot"
    >
      <PageFrame className="df2-pilot-workspace df2-pilot-v2">
        <div className="df2-pilot-status-bar" role="status">
          <div className="df2-pilot-status-brand">
            <span className={`df2-pilot-status-pill ${pilotStatusClass}`.trim()}>
              <span className="df2-pilot-status-dot" aria-hidden />
              {pilotInsightPill}
            </span>
            {(activeData?.job_id || activeData?.preflight_run_id || activeData?.route) && (
              <div className="df2-pilot-tracking">
                {activeData.job_id && <CopyIdChip id={activeData.job_id} label="Job" compact />}
                {activeData.preflight_run_id && (
                  <CopyIdChip id={activeData.preflight_run_id} label="Run" compact />
                )}
                {activeData.route && (
                  <span className="df2-pilot-tracking-route" title={activeData.route}>
                    {activeData.route}
                  </span>
                )}
                {activeData.validation_status && (
                  <span className="df2-pilot-tracking-status">{activeData.validation_status}</span>
                )}
              </div>
            )}
          </div>
          {pilotOnline && modelCapabilities && !anyCloudReady && (
            <div className="df2-page-actions-group">
              <button
                type="button"
                className="df2-btn df2-btn-ghost df2-btn-sm"
                onClick={() => onNavigate("settings")}
                title="Add cloud model API keys"
              >
                <DtIcon name="settings" size={14} />
                Models
              </button>
            </div>
          )}
        </div>
      <div className="df2-pilot-body">
      <aside className="df2-pilot-aside">
        <button type="button" className="df2-btn df2-btn-primary df2-btn-sm df2-btn-block" onClick={startNewChat}>
          <DtIcon name="plus" size={14} /> New chat
        </button>

        <div className="df2-pilot-aside-scroll">
          <div className="df2-pilot-section-label">Categories</div>
          <FilterTabs
            ariaLabel="Automation ideas by category"
            className="df2-pilot-categories"
            value={category}
            onChange={setCategory}
            items={[
              { id: "all", label: "All" },
              ...AUTOMATION_CATEGORIES.filter((c) => c.id !== "all").map((c) => ({ id: c.id, label: c.label })),
            ]}
          />

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

          {session.toolLog.length > 0 && (
            <>
              <div className="df2-pilot-section-label">Recent tools</div>
              {session.toolLog.slice(0, 8).map((t, i) => (
                <div key={i} className={`df2-pilot-tool-log ${t.success ? "ok" : "err"}`}>
                  <code>{t.name}</code>
                  <span>{t.summary}</span>
                </div>
              ))}
            </>
          )}
        </div>
      </aside>

      <div className="df2-pilot-main">
        <div className="df2-pilot-main-scroll">
          {!started ? (
            <div className="df2-pilot-main-inner">
              <div className="df2-pilot-hero">
                <div className="df2-pilot-hero-icon"><DtIcon name="sparkle" size={28} /></div>
                <h1 className="df2-pilot-title">Ask Data Pilot to move, inspect, or govern data.</h1>
                <p className="df2-pilot-subtitle">
                  Natural-language data ops — schema, mappings, connectors, and jobs with the same governed engine as Transfer Studio.
                </p>
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

              <div className="df2-pilot-ideas">
                {ideas.slice(0, 4).map((idea) => (
                  <button key={idea.id} type="button" className="df2-pilot-idea" onClick={() => send(idea.prompt)}>
                    <span className="df2-pilot-idea-cat">{idea.category.replace("_", " ")}</span>
                    <span className="df2-pilot-idea-title">{idea.title}</span>
                    <span className="df2-pilot-idea-desc">{idea.description}</span>
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <div className="df2-pilot-thread" ref={threadRef}>
              {session.messages.map((msg, i) => (
                <div key={i} className={`df2-pilot-msg ${msg.role}`}>
                  <div dangerouslySetInnerHTML={{ __html: renderSafeMarkdown(msg.text) }} />
                  {msg.tools_used && msg.tools_used.length > 0 && (
                    <div className="df2-pilot-tool-badges">
                      {msg.tools_used.map((t) => (
                        <span key={t.name} className={`df2-badge ${t.success ? "df2-badge-live" : "df2-badge-error"}`}>
                          {t.name}
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
          )}
        </div>

        <div className="df2-pilot-composer-sticky">
          <div className="df2-pilot-composer-bar">
            <textarea
              rows={started ? 2 : 3}
              placeholder={started ? "Follow up…" : "Set up Postgres source, move Shopify orders to Snowflake, scan HR for PII…"}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } }}
            />
            <button
              type="button"
              className="df2-pilot-send"
              onClick={() => send()}
              disabled={loading || !input.trim()}
              aria-label="Send"
            >
              <DtIcon name="send" size={18} />
            </button>
          </div>
        </div>
      </div>
      </div>
      </PageFrame>
    </PageShell>
  );
}
