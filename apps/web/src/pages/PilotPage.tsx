import { useEffect, useRef, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { PilotConfirmCard } from "../components/pilot/PilotConfirmCard";
import { PilotSources } from "../components/pilot/PilotSources";
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
} from "../lib/api";
import { AUTOMATION_IDEAS } from "../lib/automationIdeas";
import { useActiveData } from "../lib/DataContext";
import {
  applyPilotSafeActions,
  buildPilotDataContext,
  isNavigableScreen,
  nextPilotResultId,
  pilotActionChipLabel,
  runPilotConfirm,
} from "../lib/pilotChat";
import { useStudioActions } from "../lib/StudioActionsContext";
import { API_BASE, Screen } from "../lib/types";
import { useToast } from "../components/Toast";
import { useConfirm } from "../components/ui/ConfirmDialog";
import { renderSafeMarkdown } from "../lib/safeMarkdown";
import { CopyIdChip } from "../components/ui/CopyIdChip";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import {
  createEmptySession,
  loadAsideOpen,
  loadPilotWorkspace,
  loadRailChat,
  PilotSession,
  promoteRailChatToPilotSession,
  saveAsideOpen,
  savePilotWorkspace,
  saveRailChat,
} from "../lib/pilotChatStore";

interface PilotPageProps {
  onNavigate: (screen: Screen) => void;
}

export function PilotPage({ onNavigate }: PilotPageProps) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const { activeData } = useActiveData();
  const { dispatchStudioAction } = useStudioActions();
  const boot = useRef((() => {
    // Bring FAB/rail continuity into the full Pilot page on first mount.
    return promoteRailChatToPilotSession() || loadPilotWorkspace();
  })());
  const [sessions, setSessions] = useState<PilotSession[]>(boot.current.sessions);
  const [activeId, setActiveId] = useState(boot.current.activeId);
  const [asideOpen, setAsideOpen] = useState(() => loadAsideOpen(true));
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  /** Which pending action is currently being confirmed — drives the Confirm button spinner. */
  const [confirmingId, setConfirmingId] = useState<string | null>(null);
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
    // Keep FAB/rail in sync when it shares this session id (wave 35 handoff).
    const active = sessions.find((s) => s.id === activeId);
    const rail = loadRailChat();
    if (active && rail && rail.sessionId === activeId) {
      saveRailChat({
        messages: active.messages,
        history: active.history,
        sessionId: active.id,
        lastResultId: active.lastResultId,
      });
    }
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

  const ideas = AUTOMATION_IDEAS;

  const applySafeActions = (
    actions?: CopilotAction[],
    toolsUsed?: { name: string; success: boolean }[],
  ) => {
    applyPilotSafeActions(actions, onNavigate, toolsUsed);
  };

  const clearPending = (msgIndex: number, actionId: string) => {
    updateSession(activeId, {
      messages: session.messages.map((m, i) =>
        i === msgIndex
          ? { ...m, pending_actions: (m.pending_actions || []).filter((p) => p.id !== actionId) }
          : m,
      ),
    });
  };

  const confirmPending = async (msgIndex: number, action: CopilotPendingAction) => {
    if (confirmingId) return;
    setConfirmingId(action.id);
    try {
      const outcome = await runPilotConfirm(action, {
        onNavigate,
        toast,
        confirm,
        dispatchStudioAction,
      });
      if (outcome === "cleared") clearPending(msgIndex, action.id);
    } catch (error) {
      toast({
        title: "Action failed",
        message: error instanceof Error ? error.message : String(error),
        tone: "error",
      });
    } finally {
      setConfirmingId(null);
    }
  };

  const dismissPending = (msgIndex: number, actionId: string) => {
    clearPending(msgIndex, actionId);
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
      const pilotContext = buildPilotDataContext(activeData, {
        sessionId: activeId,
        lastResultId: session.lastResultId,
        nameFallback: "pilot",
      });
      const res = await copilotChat(q, session.history, pilotContext);
      const newHistory: CopilotChatMessage[] = [
        ...session.history,
        { role: "user" as const, content: q },
        { role: "assistant" as const, content: res.answer },
      ].slice(-20);

      const nextResultId = nextPilotResultId(
        res.tools_used,
        res.data_insight?.last_result_id,
        session.lastResultId,
      );

      updateSession(activeId, {
        history: newHistory,
        lastResultId: nextResultId,
        messages: [
          ...nextMessages,
          {
            role: "assistant",
            text: res.answer,
            actions: res.suggested_actions,
            pending_actions: res.pending_actions,
            suggested_prompts: res.suggested_prompts,
            tools_used: res.tools_used,
            sources: res.sources,
          },
        ],
      });

      // Never auto-navigate away while a Confirm card is waiting — that is the
      // exact bug that threw operators to Connectors/Studio and away from the
      // approval they were supposed to press for create_connector / start_transfer.
      if (!(res.pending_actions && res.pending_actions.length > 0)) {
        applySafeActions(res.suggested_actions, res.tools_used);
      }
      if (res.suggested_prompts?.length) setPrompts(res.suggested_prompts);
    } catch (error) {
      setPilotOnline(false);
      const detail = formatPilotReachError(error, API_BASE);
      toast({ title: "Datawrap Pilot unavailable", message: detail, tone: "error" });
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

  const deleteSession = async (id: string) => {
    const target = sessions.find((s) => s.id === id);
    if (!target) return;
    if (target.messages.length > 0) {
      const ok = await confirm({
        title: `Delete chat “${target.title}”?`,
        message: "This cannot be undone. Message history for this chat will be removed.",
        confirmLabel: "Delete chat",
        cancelLabel: "Keep chat",
        tone: "danger",
      });
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

  const cloudBroken = cloudProviders.some((p) => p.configured && !p.available);
  const pilotInsightPill =
    pilotOnline === false
      ? "Offline"
      : pilotOnline == null
        ? "Connecting…"
        : anyCloudReady
          ? `LLM · ${modelCapabilities?.active_provider || "cloud"}`
          : cloudBroken
            ? "Local · fix API key"
            : "Local engine";

  const pilotStatusClass =
    pilotOnline === false
      ? "is-offline"
      : anyCloudReady
        ? ""
        : "is-local";

  return (
    <PageShell
      title="Datawrap Pilot"
      description="Natural-language triage on the same governed transfer engine."
      wide
      fit
      showHeader={false}
      className="df2-page-pilot"
    >
      <PageFrame className={`df2-pilot-workspace df2-pilot-v2 ${asideOpen ? "" : "is-aside-collapsed"}`.trim()}>
        <div className="df2-pilot-status-bar" role="status">
          <div className="df2-pilot-status-brand">
            {/* Opening the rail is the rail's own control (its menu icon);
                the status bar used to repeat it. */}
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
            {/* New chat lives in the chat rail — expanded as a button, collapsed
                as the rail's plus icon — so the header never repeats it. */}
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
                className="df2-btn df2-btn-ghost df2-btn-sm df2-pilot-aside-collapse"
                onClick={() => setAsideOpen(false)}
                aria-label="Collapse recent chats"
                title="Collapse"
              >
                <DtIcon name="chevron-left" size={14} />
              </button>
            </div>

            <div className="df2-pilot-aside-scroll">
              <div className="df2-pilot-section-label">
                Recent chats
                {/* Count what the list shows — an unsent draft is listed, so
                    counting only sent chats read as "Recent chats 0" above a
                    visible row. */}
                <span className="df2-pilot-session-count">{recentChats.length}</span>
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
                {/* PageShell already emits the page's only h1; this is the
                    hero line under it. */}
                <h2 className="df2-pilot-title">Ask Datawrap Pilot to move, inspect, or govern data.</h2>
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
                  <PilotSources sources={msg.sources} />
                  {msg.pending_actions && msg.pending_actions.length > 0 && (
                    <div className="df2-pilot-pending">
                      {msg.pending_actions.map((pa) => (
                        <PilotConfirmCard
                          key={pa.id}
                          action={pa}
                          busy={confirmingId === pa.id}
                          onConfirm={() => confirmPending(i, pa)}
                          onCancel={() => dismissPending(i, pa.id)}
                        />
                      ))}
                    </div>
                  )}
                  {msg.actions?.map((a, j) => {
                    const screen = a.screen || a.route;
                    return isNavigableScreen(screen) ? (
                      <button key={j} type="button" className="df2-btn df2-btn-sm df2-mt-sm" onClick={() => onNavigate(screen)}>
                        {pilotActionChipLabel(a)}
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
