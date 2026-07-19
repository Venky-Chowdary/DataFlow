import { useEffect, useRef, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import {
  copilotChat,
  CopilotAction,
  CopilotChatMessage,
  CopilotPendingAction,
  fetchCopilotPrompts,
  fetchCopilotStatus,
  fetchModelCapabilities,
  formatPilotReachError,
  ModelCapabilities,
  runScheduleNow,
} from "../lib/api";
import { AUTOMATION_CATEGORIES, AUTOMATION_IDEAS } from "../lib/automationIdeas";
import { useActiveData } from "../lib/DataContext";
import { useStudioActions } from "../lib/StudioActionsContext";
import { API_BASE, Screen } from "../lib/types";
import { useToast } from "../components/Toast";
import { renderSafeMarkdown } from "../lib/safeMarkdown";
import { CopyIdChip } from "../components/ui/CopyIdChip";
import { PageFrame } from "../components/ui/PageFrame";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageShell } from "../components/ui/PageShell";
import {
  createEmptySession,
  loadAsideOpen,
  loadPilotWorkspace,
  PilotSession,
  saveAsideOpen,
  savePilotWorkspace,
} from "../lib/pilotChatStore";

interface PilotPageProps {
  onNavigate: (screen: Screen) => void;
}

const SCREEN_LABELS: Record<string, string> = {
  dashboard: "Overview",
  pilot: "Data Pilot",
  transfer: "Transfer Studio",
  connectors: "Connectors",
  jobs: "Jobs",
  settings: "Settings",
  schedules: "Pipelines",
  contracts: "Contracts",
  query: "Query",
  mcp: "MCP",
  docs: "Docs",
  benchmarks: "Proofs",
};

export function PilotPage({ onNavigate }: PilotPageProps) {
  const { toast } = useToast();
  const { activeData } = useActiveData();
  const { dispatchStudioAction } = useStudioActions();
  const boot = useRef(loadPilotWorkspace());
  const [sessions, setSessions] = useState<PilotSession[]>(boot.current.sessions);
  const [activeId, setActiveId] = useState(boot.current.activeId);
  const [asideOpen, setAsideOpen] = useState(() => loadAsideOpen(true));
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

  // Persist chats + active session across refresh.
  useEffect(() => {
    savePilotWorkspace(sessions, activeId);
  }, [sessions, activeId]);

  useEffect(() => {
    saveAsideOpen(asideOpen);
  }, [asideOpen]);

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

  const applySafeActions = (actions?: CopilotAction[]) => {
    actions?.forEach((a) => {
      if (a.risk === "mutate" || a.type === "studio") return;
      const screen = a.screen || a.route;
      if ((a.type === "navigate" || !a.type) && screen) onNavigate(screen as Screen);
    });
  };

  const confirmPending = async (msgIndex: number, action: CopilotPendingAction) => {
    try {
      if (action.type === "studio" || action.kind) {
        onNavigate("transfer");
        dispatchStudioAction({
          kind: (action.kind || String(action.payload?.kind || "")) as string,
          label: action.label,
          run_id: action.run_id || (action.payload?.run_id as string | undefined),
        });
        toast({ title: "Action confirmed", message: action.label || "Applied in Transfer Studio", tone: "success" });
      } else if (action.type === "run_schedule") {
        const sid = String(action.payload?.schedule_id || "");
        if (!sid) throw new Error("Missing schedule id");
        onNavigate("schedules");
        const res = await runScheduleNow(sid);
        toast({
          title: "Pipeline started",
          message: `Job ${res.job_id || "queued"}`,
          tone: "success",
        });
      } else {
        toast({ title: "Unknown action", message: action.type, tone: "error" });
        return;
      }
      updateSession(activeId, {
        messages: session.messages.map((m, i) =>
          i === msgIndex
            ? { ...m, pending_actions: (m.pending_actions || []).filter((p) => p.id !== action.id) }
            : m,
        ),
      });
    } catch (error) {
      toast({
        title: "Action failed",
        message: error instanceof Error ? error.message : String(error),
        tone: "error",
      });
    }
  };

  const dismissPending = (msgIndex: number, actionId: string) => {
    updateSession(activeId, {
      messages: session.messages.map((m, i) =>
        i === msgIndex
          ? { ...m, pending_actions: (m.pending_actions || []).filter((p) => p.id !== actionId) }
          : m,
      ),
    });
  };

  const updateSession = (id: string, patch: Partial<PilotSession>) => {
    setSessions((prev) =>
      prev.map((s) => (s.id === id ? { ...s, ...patch, updatedAt: Date.now() } : s)),
    );
  };

  const send = async (text?: string) => {
    const q = (text ?? input).trim();
    if (!q || loading) return;
    setInput("");
    setLoading(true);

    const userMsg = { role: "user" as const, text: q };
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

      updateSession(activeId, {
        history: newHistory,
        messages: [
          ...nextMessages,
          {
            role: "assistant",
            text: res.answer,
            actions: res.suggested_actions,
            pending_actions: res.pending_actions,
            suggested_prompts: res.suggested_prompts,
          },
        ],
      });

      applySafeActions(res.suggested_actions);
      if (res.suggested_prompts?.length) setPrompts(res.suggested_prompts);
    } catch (error) {
      setPilotOnline(false);
      const detail = formatPilotReachError(error, API_BASE);
      toast({ title: "Data Pilot unavailable", message: detail, tone: "error" });
      updateSession(activeId, {
        messages: [...nextMessages, { role: "assistant", text: detail }],
      });
    }
    setLoading(false);
  };

  const startNewChat = () => {
    const s = createEmptySession();
    setSessions((prev) => [s, ...prev]);
    setActiveId(s.id);
    setInput("");
    setAsideOpen(true);
  };

  const deleteSession = (id: string) => {
    const target = sessions.find((s) => s.id === id);
    if (!target) return;
    if (target.messages.length > 0) {
      const ok = window.confirm(`Delete chat “${target.title}”? This cannot be undone.`);
      if (!ok) return;
    }
    setSessions((prev) => {
      const next = prev.filter((s) => s.id !== id);
      if (!next.length) {
        const empty = createEmptySession();
        setActiveId(empty.id);
        return [empty];
      }
      if (id === activeId) setActiveId(next[0].id);
      return next;
    });
  };

  const recentChats = sessions.filter((s) => s.messages.length > 0 || s.id === activeId);
  const canDeleteActive = Boolean(session?.messages.length);

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
      <PageFrame className={`df2-pilot-workspace df2-pilot-v2 ${asideOpen ? "" : "is-aside-collapsed"}`.trim()}>
        <div className="df2-pilot-status-bar" role="status">
          <div className="df2-pilot-status-brand">
            {!asideOpen && (
              <button
                type="button"
                className="df2-btn df2-btn-ghost df2-btn-sm df2-pilot-aside-reopen"
                onClick={() => setAsideOpen(true)}
                aria-label="Open recent chats"
                title="Open recent chats"
              >
                <DtIcon name="menu" size={14} />
                Chats
              </button>
            )}
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
          <div className="df2-page-actions-group">
            {pilotOnline && modelCapabilities && !anyCloudReady && (
              <button
                type="button"
                className="df2-btn df2-btn-ghost df2-btn-sm"
                onClick={() => onNavigate("settings")}
                title="Add cloud model API keys"
              >
                <DtIcon name="settings" size={14} />
                Models
              </button>
            )}
            {canDeleteActive && (
              <button
                type="button"
                className="df2-btn df2-btn-ghost df2-btn-sm"
                onClick={() => deleteSession(activeId)}
                title="Delete this chat"
              >
                <DtIcon name="trash" size={14} />
                Delete chat
              </button>
            )}
            <button
              type="button"
              className="df2-btn df2-btn-ghost df2-btn-sm"
              onClick={startNewChat}
              title="Start a new chat"
            >
              <DtIcon name="plus" size={14} />
              New chat
            </button>
          </div>
        </div>
      <div className="df2-pilot-body">
      <aside
        className={`df2-pilot-aside${asideOpen ? "" : " is-collapsed"}`}
        aria-label="Recent chats"
        aria-hidden={!asideOpen}
      >
        {asideOpen ? (
          <>
            <div className="df2-pilot-aside-toolbar">
              <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={startNewChat}>
                <DtIcon name="plus" size={14} /> New chat
              </button>
              <button
                type="button"
                className="df2-btn df2-btn-ghost df2-btn-sm df2-pilot-aside-close"
                onClick={() => setAsideOpen(false)}
                aria-label="Close recent chats"
                title="Close recent chats"
              >
                <DtIcon name="x" size={14} />
              </button>
            </div>

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

              <div className="df2-pilot-section-label">
                Recent chats
                <span className="df2-pilot-session-count">
                  {sessions.filter((s) => s.messages.length > 0).length}
                </span>
              </div>
              <div className="df2-pilot-session-list">
                {recentChats.length === 0 ? (
                  <p className="df2-pilot-recent-empty">No recent chats yet — send a message to save one here.</p>
                ) : (
                  recentChats.map((s) => {
                    const showDelete = s.messages.length > 0 || recentChats.length > 1;
                    return (
                      <div key={s.id} className={`df2-pilot-session-row ${s.id === activeId ? "active" : ""}`}>
                        <button
                          type="button"
                          className="df2-pilot-session"
                          onClick={() => setActiveId(s.id)}
                          title={s.title}
                        >
                          {s.title}
                        </button>
                        {showDelete && (
                          <button
                            type="button"
                            className="df2-pilot-session-delete"
                            aria-label={`Delete ${s.title}`}
                            title="Delete chat"
                            onClick={(e) => {
                              e.stopPropagation();
                              deleteSession(s.id);
                            }}
                          >
                            <DtIcon name="trash" size={12} />
                          </button>
                        )}
                      </div>
                    );
                  })
                )}
              </div>

            </div>
          </>
        ) : (
          <div className="df2-pilot-aside-rail" aria-label="Collapsed recent chats">
            <button
              type="button"
              className="df2-pilot-aside-icon-btn"
              onClick={() => setAsideOpen(true)}
              aria-label="Expand recent chats"
              title="Open recent chats"
            >
              <DtIcon name="menu" size={16} />
            </button>
            <button
              type="button"
              className="df2-pilot-aside-icon-btn"
              onClick={startNewChat}
              aria-label="New chat"
              title="New chat"
            >
              <DtIcon name="plus" size={16} />
            </button>
          </div>
        )}
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
                  Chats are saved in this browser so a refresh does not wipe your thread.
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
                  {msg.pending_actions && msg.pending_actions.length > 0 && (
                    <div className="df2-pilot-pending">
                      {msg.pending_actions.map((pa) => (
                        <div key={pa.id} className="df2-pilot-pending-row">
                          <span className="df2-pilot-pending-label">{pa.label || pa.type}</span>
                          <button
                            type="button"
                            className="df2-btn df2-btn-primary df2-btn-sm"
                            onClick={() => confirmPending(i, pa)}
                          >
                            Confirm
                          </button>
                          <button
                            type="button"
                            className="df2-btn df2-btn-ghost df2-btn-sm"
                            onClick={() => dismissPending(i, pa.id)}
                          >
                            Cancel
                          </button>
                        </div>
                      ))}
                    </div>
                  )}
                  {msg.actions?.map((a, j) => {
                    const screen = a.screen || a.route;
                    return screen ? (
                      <button key={j} type="button" className="df2-btn df2-btn-sm df2-mt-sm" onClick={() => onNavigate(screen as Screen)}>
                        {a.label || `Open ${SCREEN_LABELS[screen] || screen}`}
                      </button>
                    ) : null;
                  })}
                  {msg.suggested_prompts && msg.suggested_prompts.length > 0 && (
                    <div className="df2-pilot-followups">
                      {msg.suggested_prompts.map((p) => (
                        <button key={p} type="button" className="df2-pilot-followup" onClick={() => send(p)} disabled={loading}>
                          {p}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              ))}
              {loading && (
                <div className="df2-pilot-msg assistant df2-pilot-thinking">
                  <span className="df2-loader-bars" aria-hidden><i /><i /><i /></span>
                  Looking that up…
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
