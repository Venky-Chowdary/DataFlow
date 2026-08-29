/**
 * Datawrap — Universal Data Platform
 */

import { Suspense, useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { DtIcon } from "./components/DtIcon";
import { BrandWordmark } from "./components/BrandWordmark";
import { PageErrorBoundary } from "./components/PageErrorBoundary";
import { ToastProvider, useToast } from "./components/Toast";
import { ConfirmProvider, useConfirm } from "./components/ui/ConfirmDialog";
import { Button } from "./components/ui/Button";
import { WorkspaceSearch, type SearchNavigateTarget } from "./components/ui/WorkspaceSearch";
import { StatusPopover } from "./components/StatusPopover";
import { DataProvider } from "./lib/DataContext";
import { ForcePasswordChange } from "./components/ForcePasswordChange";
import { PERMISSIONS, PermissionsProvider, useWriteGate } from "./lib/PermissionsContext";
import { StudioActionsProvider } from "./lib/StudioActionsContext";
import { AUTH_REQUIRED_EVENT, deleteConnector, fetchConnectors, fetchJobs, fetchSchedules, fetchTransferCapabilities, noteApiSuccess, probeApiHealth, shouldMarkApiOffline } from "./lib/api";
import { EMPTY_JOB_HISTORY, type JobHistory } from "./lib/jobHistory";
import { clearSession, readSession, writeSession } from "./lib/session";
import { clearActiveWorkspaceId } from "./lib/workspace";
import { loadSidebarNavCompact, saveSidebarNavCompact } from "./lib/pilotChatStore";
import { loadTransferLiveCatalog, resolveCatalogIdToType } from "./lib/connectorTypes";
import { Connector, PipelineSchedule, Screen, TransferJob } from "./lib/types";
import { LoginPage } from "./pages/LoginPage";
import { MarketingSite } from "./pages/marketing/MarketingSite";
import { AICopilot } from "./components/AICopilot";
import { ConnectorModal } from "./components/ConnectorModal";
import { LoadingBlock } from "./components/LoadingState";
import { focusFromHash, readAppHash, writeAppHash } from "./lib/appNavigation";
import {
  PUBLIC_PAGE_META,
  publicRouteFromHash,
  type PublicRoute,
  writePublicHash,
} from "./lib/publicNavigation"; // help article routes
import { apiOfflineMessage } from "./lib/runtimeEnv";
import { usePageMeta } from "./lib/usePageMeta";
import { metaForLogin, metaForScreen } from "./lib/seo";
import type { JobsStudioIntent } from "./pages/JobsPage";
import { DashboardPage } from "./pages/DashboardPage";
import { lazyNamed } from "./lib/lazyPage";

/** Overview is the signed-in home screen — never a separate hashed chunk.
 * Other routes stay lazy; stale Vite hashes reload once (see lazyPage). */
const PilotPage = lazyNamed(() => import("./pages/PilotPage"), "PilotPage");
const TransferPage = lazyNamed(() => import("./pages/TransferPage"), "TransferPage");
const ConnectorsPage = lazyNamed(() => import("./pages/ConnectorsPage"), "ConnectorsPage");
const SchedulesPage = lazyNamed(() => import("./pages/SchedulesPage"), "SchedulesPage");
const TransformsPage = lazyNamed(() => import("./pages/TransformsPage"), "TransformsPage");
const JobsPage = lazyNamed(() => import("./pages/JobsPage"), "JobsPage");
const ContractsPage = lazyNamed(() => import("./pages/ContractsPage"), "ContractsPage");
const McpPage = lazyNamed(() => import("./pages/McpPage"), "McpPage");
const QueryPage = lazyNamed(() => import("./pages/QueryPage"), "QueryPage");
const SettingsPage = lazyNamed(() => import("./pages/SettingsPage"), "SettingsPage");
const DocsPage = lazyNamed(() => import("./pages/DocsPage"), "DocsPage");
const BenchmarksPage = lazyNamed(() => import("./pages/BenchmarksPage"), "BenchmarksPage");

function LazyScreen({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Suspense
      fallback={
        <div className="df2-page df2-route-loading" aria-busy="true">
          <LoadingBlock title={`Loading ${label}…`} />
        </div>
      }
    >
      {children}
    </Suspense>
  );
}

const NAV: { id: Screen; label: string; icon: string; desc: string; group: "platform" | "ops" | "system" }[] = [
  { id: "dashboard", label: "Overview", icon: "dashboard", desc: "Health, throughput, and recent jobs", group: "platform" },
  { id: "transfer", label: "Transfer", icon: "transfer", desc: "Move data with preflight gates", group: "platform" },
  { id: "connectors", label: "Connectors", icon: "connectors", desc: "Saved sources & destinations", group: "platform" },
  { id: "contracts", label: "Contracts", icon: "shield", desc: "Schema agreements and breakers", group: "platform" },
  { id: "jobs", label: "Jobs", icon: "jobs", desc: "Live progress and history", group: "ops" },
  { id: "schedules", label: "Schedules", icon: "activity", desc: "Recurring syncs", group: "ops" },
  { id: "transforms", label: "Transforms", icon: "layers", desc: "Post-load SQL models", group: "ops" },
  { id: "query", label: "Query", icon: "search", desc: "Ad-hoc SQL and export", group: "ops" },
  { id: "pilot", label: "Pilot", icon: "sparkle", desc: "Natural-language assistant", group: "ops" },
  { id: "settings", label: "Settings", icon: "settings", desc: "Security, team, SSO", group: "system" },
  { id: "mcp", label: "MCP", icon: "zap", desc: "IDE tool integrations", group: "system" },
  { id: "docs", label: "Help", icon: "book", desc: "How Datawrap works", group: "system" },
  { id: "benchmarks", label: "Proofs", icon: "speed", desc: "Scale and fidelity benchmarks", group: "system" },
];

const PLATFORM_NAV = NAV.filter((item) => item.group === "platform");
const OPS_NAV = NAV.filter((item) => item.group === "ops");
const SYSTEM_NAV = NAV.filter((item) => item.group === "system");
const DEVELOPER_NAV = SYSTEM_NAV;
function readStoredUser() {
  return readSession()?.email ?? "";
}

function AppShell({
  initialScreen = "dashboard",
  userEmail,
  onSignOut,
}: {
  initialScreen?: Screen;
  userEmail: string;
  onSignOut: () => void;
}) {
  const { toast } = useToast();
  const connectorWrite = useWriteGate(PERMISSIONS.connectorWrite);
  const { confirm } = useConfirm();
  const [screen, setScreenState] = useState<Screen>(() => {
    const fromHash = readAppHash();
    if (fromHash) return fromHash;
    return initialScreen === "landing" ? "dashboard" : initialScreen;
  });
  const [connectors, setConnectors] = useState<Connector[]>([]);
  const [jobHistory, setJobHistory] = useState<JobHistory>(EMPTY_JOB_HISTORY);
  const jobs = jobHistory.jobs;
  const [schedules, setSchedules] = useState<PipelineSchedule[]>([]);
  const [bootLoading, setBootLoading] = useState(true);
  /** False until the first connectors fetch settles — prevents false “no connectors” empty states. */
  const [connectorsReady, setConnectorsReady] = useState(false);
  const [apiOnline, setApiOnline] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [modalType, setModalType] = useState("");
  const [editingConnector, setEditingConnector] = useState<Connector | null>(null);
  const [copilotOpen, setCopilotOpen] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [sidebarNavCompact, setSidebarNavCompact] = useState(() => loadSidebarNavCompact());
  const [searchQuery, setSearchQuery] = useState("");
  const [searchFocus, setSearchFocus] = useState<SearchNavigateTarget | null>(null);
  const [connectorsViewToken, setConnectorsViewToken] = useState(0);
  const [firstScreenPaint, setFirstScreenPaint] = useState(true);
  /** Bump to remount Transfer Studio and clear prior job/source/map cache. */
  const [transferStudioKey, setTransferStudioKey] = useState(0);
  /** Seed Transfer Studio source from Connectors drawer (id + token so re-clicks re-apply). */
  const [transferSeedSource, setTransferSeedSource] = useState<{ connectorId: string; token: number } | null>(null);
  /** Jobs → Studio deep-link (Validate / repair proposal / seeded mappings). */
  const [transferStudioIntent, setTransferStudioIntent] = useState<(JobsStudioIntent & { token: number }) | null>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  /** Keep heavy workspaces mounted after first visit so wizard/query/pilot state is not wiped on nav. */
  const [mountedScreens, setMountedScreens] = useState<Set<Screen>>(() => new Set([screen]));

  const setScreen = useCallback((next: Screen) => {
    // Mount keep-alive screens synchronously so the first paint after navigate
    // is not an empty content hole (useEffect mount races Save → Contracts).
    setMountedScreens((prev) => {
      if (prev.has(next)) return prev;
      const nextSet = new Set(prev);
      nextSet.add(next);
      return nextSet;
    });
    setScreenState(next);
    writeAppHash(next);
  }, []);

  const openFreshTransfer = useCallback(() => {
    setTransferSeedSource(null);
    setTransferStudioIntent(null);
    setTransferStudioKey((k) => k + 1);
    setScreen("transfer");
  }, [setScreen]);

  useEffect(() => {
    const onHash = () => {
      const focus = focusFromHash(window.location.hash);
      if (!focus) return;
      setScreen(focus.screen);
      if (focus.jobId || focus.panel) {
        setSearchFocus({
          screen: focus.screen,
          jobId: focus.jobId,
          panel: focus.panel,
        });
      }
    };
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, [setScreen]);

  const showScreen = (id: Screen) => (mountedScreens.has(id) ? (screen === id ? "is-active" : "is-kept") : "");

  usePageMeta(metaForScreen(screen));

  useEffect(() => {
    if (!bootLoading) {
      const t = window.setTimeout(() => setFirstScreenPaint(false), 400);
      return () => window.clearTimeout(t);
    }
    return undefined;
  }, [bootLoading]);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        searchRef.current?.focus();
      } else if (e.key === "Escape" && document.activeElement === searchRef.current) {
        searchRef.current?.blur();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  const patchConnector = useCallback((id: string, patch: Partial<Connector>) => {
    setConnectors((prev) =>
      prev.map((c) => (c.id === id ? { ...c, ...patch } : c)),
    );
  }, []);

  const loadConnectors = useCallback(async (notifyOnError = true) => {
    try {
      setConnectors(await fetchConnectors());
      noteApiSuccess();
      setApiOnline(true);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "";
      const authOnly = /authentication required|sign in|401/i.test(msg);
      const timedOut = /timed out|abort/i.test(msg);
      // 401/session is not an outage — Railway /health can still be green.
      const up = await probeApiHealth();
      if (up || authOnly) {
        noteApiSuccess();
        setApiOnline(true);
        if (notifyOnError && authOnly) {
          toast({
            title: "Could not load connectors",
            message: "Sign in again or check connector permissions.",
            tone: "warning",
          });
        } else if (notifyOnError && timedOut && up) {
          // Slow control-plane response, not offline — don't scare the user.
          toast({
            title: "Connectors took longer than usual",
            message: "The API is healthy; retrying in the background.",
            tone: "warning",
          });
        }
      } else if (shouldMarkApiOffline(false)) {
        setApiOnline(false);
        if (notifyOnError) {
          toast({
            title: "Control plane offline",
            message: "Check the API URL (VITE_API_BASE / DATAFLOW_API_BASE) and deployment health.",
            tone: "error",
          });
        }
      }
      // Below threshold: keep previous online state (no flicker).
    } finally {
      setConnectorsReady(true);
    }
  }, [toast]);

  const loadJobs = useCallback(async (notifyOnError = true) => {
    try {
      setJobHistory(await fetchJobs());
      noteApiSuccess();
      setApiOnline(true);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "";
      const timedOut = /timed out|abort/i.test(msg);
      if (notifyOnError) {
        const up = await probeApiHealth();
        if (up || timedOut) {
          toast({
            title: timedOut ? "Jobs list took longer than usual" : "Could not refresh jobs",
            message: up
              ? "API is healthy — retrying in the background. Open a job for full detail."
              : "Job history may be temporarily unavailable.",
            tone: "warning",
          });
        } else if (shouldMarkApiOffline(false)) {
          setApiOnline(false);
        }
      }
    }
  }, [toast]);

  const loadSchedules = useCallback(async () => {
    try {
      setSchedules(await fetchSchedules());
      noteApiSuccess();
      setApiOnline(true);
    } catch {
      setSchedules([]);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setBootLoading(true);
      await Promise.allSettled([
        loadConnectors(false),
        loadJobs(false),
        loadSchedules(),
        // Catalog SSOT for transfer-ready drivers — before Transfer Studio paints.
        loadTransferLiveCatalog(fetchTransferCapabilities),
      ]);
      if (!cancelled) setBootLoading(false);
    })();
    return () => { cancelled = true; };
  }, [loadConnectors, loadJobs, loadSchedules]);

  useEffect(() => {
    if (screen === "jobs" || screen === "dashboard") {
      loadJobs(false);
    }
  }, [screen, loadJobs]);

  useEffect(() => {
    const onResize = () => {
      if (window.innerWidth >= 1024) setMobileNavOpen(false); // matches --df-bp-lg / CSS 1023 overlay shell
      if (window.innerWidth < 1280) setCopilotOpen(false);
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  // Soft poll — avoid hammering the API during Transfer Studio introspect/map.
  useEffect(() => {
    const poll = window.setInterval(() => {
      if (document.hidden) return;
      if (screen === "transfer" || screen === "pilot") return;
      void loadConnectors(false);
    }, 60_000);
    return () => window.clearInterval(poll);
  }, [loadConnectors, screen]);

  // Dedicated health pulse — recovers the banner when Railway is green again,
  // and only flips offline after repeated health failures (not one slow request).
  useEffect(() => {
    const pulse = window.setInterval(async () => {
      if (document.hidden) return;
      const up = await probeApiHealth();
      if (up) {
        noteApiSuccess();
        setApiOnline(true);
      } else if (shouldMarkApiOffline(false)) {
        setApiOnline(false);
      }
    }, 30_000);
    return () => window.clearInterval(pulse);
  }, []);

  useEffect(() => {
    const onConnectorsChanged = () => {
      void loadConnectors(false);
    };
    window.addEventListener("df2:connectors-changed", onConnectorsChanged);
    return () => window.removeEventListener("df2:connectors-changed", onConnectorsChanged);
  }, [loadConnectors]);

  useEffect(() => {
    const onAuthRequired = () => {
      toast({
        title: "Session expired",
        message: "Sign in again to load connectors, jobs, and transfers.",
        tone: "warning",
      });
      onSignOut();
    };
    window.addEventListener(AUTH_REQUIRED_EVENT, onAuthRequired);
    return () => window.removeEventListener(AUTH_REQUIRED_EVENT, onAuthRequired);
  }, [onSignOut, toast]);

  const navigateFromSearch = (target: SearchNavigateTarget) => {
    setScreen(target.screen);
    setSearchFocus(target);
    if (target.screen === "connectors") setConnectorsViewToken((n) => n + 1);
    setSearchQuery("");
    searchRef.current?.blur();
  };

  useEffect(() => {
    if (!searchFocus) return;
    const timer = window.setTimeout(() => setSearchFocus(null), 800);
    return () => window.clearTimeout(timer);
  }, [searchFocus]);

  const userInitial = userEmail ? userEmail.charAt(0).toUpperCase() : "U";
  const userShort = (() => {
    const raw = userEmail ? userEmail.split("@")[0] : "User";
    if (!raw) return "User";
    return raw.charAt(0).toUpperCase() + raw.slice(1);
  })();

  /** Refuse in words: a viewer must never see a silent no-op. */
  const refuseConnectorWrite = () => {
    toast({ title: "No write permission", message: connectorWrite.reason, tone: "warning" });
  };

  const openModal = (type?: string) => {
    if (!connectorWrite.allowed) {
      refuseConnectorWrite();
      return;
    }
    setEditingConnector(null);
    setModalType(type ? resolveCatalogIdToType(type) : "");
    setShowModal(true);
  };

  const openEditModal = (connector: Connector) => {
    if (!connectorWrite.allowed) {
      refuseConnectorWrite();
      return;
    }
    setEditingConnector(connector);
    setModalType(connector.type);
    setShowModal(true);
  };

  const contentInnerClass =
    screen === "pilot"
      ? "df2-content-flush"
      : screen === "transfer"
        ? "df2-content-studio"
        : screen === "jobs"
          ? "df2-content-viewport"
          : "df2-content-document";

  /** Document pages own the scroll on the host; immersive/viewport pages lock it
      and scroll internally. Deterministic class beats the legacy :has() toggles. */
  const contentScrolls = contentInnerClass === "df2-content-document";
  const contentModeClass = contentScrolls ? "df2-content-scroll" : "df2-content-fixed";

  useEffect(() => {
    const scrollHost = document.querySelector<HTMLElement>(".df2-content");
    if (!scrollHost) return;

    // Reset to top on every route / keep-alive swap. Overflow is governed purely
    // by the .df2-content-scroll / .df2-content-fixed class (no inline mutation
    // that could get stuck fighting !important rules).
    scrollHost.scrollTop = 0;
    const raf = window.requestAnimationFrame(() => {
      void scrollHost.offsetHeight; // force reflow so scrollHeight is recomputed
      scrollHost.scrollTop = 0;
    });
    return () => window.cancelAnimationFrame(raf);
  }, [screen, bootLoading]);

  const showCopilotRail = screen !== "pilot" && copilotOpen;
  const showCopilotEdge = screen !== "pilot" && !copilotOpen;
  const currentNav = NAV.find((n) => n.id === screen);
  const offlineCopy = apiOfflineMessage();
  const runningJobsCount = jobs.filter((j) => j.status === "running" || j.status === "pending").length;
  const failedJobsCount = jobs.filter((j) => j.status === "failed").length;
  const unhealthyConnectorsCount = connectors.filter((c) => c.last_test_ok === false).length;
  return (
    <div
      className={`df2-app ${showCopilotRail ? "df2-app-with-rail" : ""} ${
        showCopilotEdge ? "df2-app-with-edge" : ""
      } ${sidebarNavCompact ? "df2-sidebar-nav-compact" : ""}`}
    >
      {mobileNavOpen && (
        <div className="df2-overlay" onClick={() => setMobileNavOpen(false)} role="presentation" />
      )}

      <aside className={`df2-sidebar ${mobileNavOpen ? "open" : ""}`} aria-label="Main navigation">
        <div className="df2-sidebar-brand">
          <BrandWordmark
            markSize={sidebarNavCompact ? 32 : 34}
            word={!sidebarNavCompact}
            title=""
          />
          <button
            type="button"
            className="df2-sidebar-collapse-btn"
            onClick={() => {
              setSidebarNavCompact((c) => {
                const next = !c;
                saveSidebarNavCompact(next);
                return next;
              });
            }}
            aria-label={sidebarNavCompact ? "Expand navigation" : "Collapse navigation"}
            title={sidebarNavCompact ? "Expand navigation" : "Collapse navigation"}
            aria-pressed={!sidebarNavCompact}
          >
            <DtIcon name="panel-left" size={16} />
          </button>
        </div>

        <nav className="df2-nav">
          <div className="df2-nav-group-label">Platform</div>
          {PLATFORM_NAV.map((item) => (
            <button
              key={item.id}
              type="button"
              className={`df2-nav-item ${screen === item.id ? "active" : ""}`}
              onClick={() => { setScreen(item.id); setMobileNavOpen(false); }}
              title={item.desc}
            >
              <span className="dt-nav-icon" aria-hidden>
                <DtIcon name={item.icon} size={18} />
              </span>
              <span>{item.label}</span>
              {item.id === "connectors" && connectors.length > 0 && (
                <span className="df2-nav-badge" aria-hidden="true"> {connectors.length}</span>
              )}
            </button>
          ))}

          <div className="df2-nav-group-label">Operations</div>
          {OPS_NAV.map((item) => (
            <button
              key={item.id}
              type="button"
              className={`df2-nav-item ${screen === item.id ? "active" : ""}`}
              onClick={() => { setScreen(item.id); setMobileNavOpen(false); }}
              title={item.desc}
            >
              <span className="dt-nav-icon" aria-hidden>
                <DtIcon name={item.icon} size={18} />
              </span>
              <span>{item.label}</span>
              {item.id === "jobs" && jobHistory.total > 0 && (
                <span className="df2-nav-badge" aria-hidden="true"> {jobHistory.total}</span>
              )}
            </button>
          ))}

          <div className="df2-nav-group-label">System</div>
          {SYSTEM_NAV.map((item) => (
            <button
              key={item.id}
              type="button"
              className={`df2-nav-item ${screen === item.id ? "active" : ""}`}
              onClick={() => { setScreen(item.id); setMobileNavOpen(false); }}
              title={item.desc}
            >
              <span className="dt-nav-icon" aria-hidden>
                <DtIcon name={item.icon} size={18} />
              </span>
              <span>{item.label}</span>
            </button>
          ))}
        </nav>

        <div className="df2-sidebar-foot">
          {/* New transfer lives in the topbar only — one CTA, one place. */}
          <div className="df2-sidebar-user">
            <button
              type="button"
              className="df2-user-row"
              onClick={() => setScreen("settings")}
              title={userEmail || "Account settings"}
            >
              <span className="df2-user-avatar" aria-hidden>{userInitial}</span>
              <span className="df2-user-meta">
                <strong>{userShort}</strong>
                <small>{userEmail || "Workspace"}</small>
              </span>
            </button>
            <div className="df2-user-actions">
              <button type="button" onClick={() => setScreen("settings")} title="Settings">
                <DtIcon name="settings" size={14} />
                <span className="df2-sidebar-collapse-label">Settings</span>
              </button>
              <button type="button" onClick={onSignOut} title="Sign out">
                <DtIcon name="gate" size={14} />
                <span className="df2-sidebar-collapse-label">Sign out</span>
              </button>
            </div>
          </div>
        </div>
      </aside>

      <div className="df2-main">
        <header className="df2-topbar">
          <div className="df2-topbar-left">
            <button
              type="button"
              className="df2-mobile-menu"
              onClick={() => setMobileNavOpen((o) => !o)}
              aria-label="Open navigation"
            >
              <DtIcon name="menu" size={20} />
            </button>
            <div className="df2-breadcrumb">
              <span>Workspace</span>
              <strong> {currentNav?.label ?? "Datawrap"}</strong>
            </div>
            <WorkspaceSearch
              query={searchQuery}
              onQueryChange={setSearchQuery}
              onNavigate={navigateFromSearch}
              navItems={NAV}
              connectors={connectors}
              jobs={jobs}
              schedules={schedules}
              inputRef={searchRef}
            />
          </div>
          <div className="df2-topbar-actions">
            {sidebarNavCompact && (
              <button
                type="button"
                className="df2-sidebar-expand-topbar"
                onClick={() => {
                  setSidebarNavCompact(false);
                  saveSidebarNavCompact(false);
                }}
                aria-label="Expand navigation"
                title="Expand navigation"
              >
                <DtIcon name="menu" size={18} />
              </button>
            )}
            <StatusPopover
              apiOnline={apiOnline}
              failedJobsCount={failedJobsCount}
              runningJobsCount={runningJobsCount}
              unhealthyConnectorsCount={unhealthyConnectorsCount}
              onNavigate={setScreen}
            />
            {screen !== "pilot" && (
              <Button
                variant="ghost"
                className={copilotOpen ? "active" : ""}
                onClick={() => setCopilotOpen((o) => !o)}
                aria-label="Toggle Datawrap Pilot"
                leadingIcon={<DtIcon name="sparkle" size={16} />}
              >
                <span className="df2-topbar-btn-text">Pilot</span>
              </Button>
            )}
            {screen !== "pilot" && (
              <Button
                variant="primary"
                onClick={openFreshTransfer}
              >
                <span className="df2-topbar-btn-text">New transfer</span>
              </Button>
            )}
          </div>
        </header>

        {!apiOnline && (
          <div className="df2-api-offline-banner df2-alert df2-alert-error" role="alert">
            <DtIcon name="alert" size={18} />
            <div>
              <strong>{offlineCopy.title}</strong>
              <p>{offlineCopy.body}</p>
            </div>
          </div>
        )}

        <div className={`df2-content ${contentModeClass}`}>
        {bootLoading && (
          <div className="df2-boot-progress" role="progressbar" aria-label="Loading workspace">
            <div className="df2-boot-progress-fill" />
          </div>
        )}
        <div
          className={`df2-content-inner ${contentInnerClass} ${bootLoading ? "is-booting" : ""} ${firstScreenPaint ? "is-first-screen" : ""}`}
        >
          <div className="df2-screen-panel">
            <LazyScreen label={NAV.find((n) => n.id === screen)?.label || "workspace"}>
            {mountedScreens.has("dashboard") && (
                <div className={`df2-screen-keep ${showScreen("dashboard")}`} hidden={screen !== "dashboard"} aria-hidden={screen !== "dashboard"}>
                <PageErrorBoundary label="Overview">
                  <DashboardPage
                    connectors={connectors}
                    jobs={jobs}
                    schedules={schedules}
                    onOpenConnectors={() => setScreen("connectors")}
                    onOpenJobs={() => setScreen("jobs")}
                    onOpenJob={(jobId) => navigateFromSearch({ screen: "jobs", jobId })}
                    onOpenPipeline={(scheduleId) =>
                      navigateFromSearch({ screen: "schedules", scheduleId })
                    }
                    onOpenSchedules={() => setScreen("schedules")}
                  />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("pilot") && (
                <div className={`df2-screen-keep ${showScreen("pilot")}`} hidden={screen !== "pilot"} aria-hidden={screen !== "pilot"}>
                <PageErrorBoundary label="Datawrap Pilot">
                  <PilotPage onNavigate={setScreen} />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("transfer") && (
                <div className={`df2-screen-keep ${showScreen("transfer")}`} hidden={screen !== "transfer"} aria-hidden={screen !== "transfer"}>
                <PageErrorBoundary label="Transfer Studio">
                  <TransferPage
                    key={transferStudioKey}
                    connectors={connectors}
                    connectorsLoading={!connectorsReady}
                    seedSourceConnector={transferSeedSource}
                    seedStudioIntent={transferStudioIntent}
                    onOpenSchedules={() => setScreen("schedules")}
                    onOpenContracts={() => setScreen("contracts")}
                    onFreshTransfer={openFreshTransfer}
                    onTransferComplete={() => {
                      loadJobs();
                      void loadSchedules();
                      toast({ title: "Transfer complete", message: "View progress in Job Theater.", tone: "success" });
                    }}
                  />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("query") && (
                <div className={`df2-screen-keep ${showScreen("query")}`} hidden={screen !== "query"} aria-hidden={screen !== "query"}>
                <PageErrorBoundary label="Query Playground">
                  <QueryPage connectors={connectors} />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("connectors") && (
                <div className={`df2-screen-keep ${showScreen("connectors")}`} hidden={screen !== "connectors"} aria-hidden={screen !== "connectors"}>
                <PageErrorBoundary label="Connectors">
                  <ConnectorsPage
                    connectors={connectors}
                    connectorsLoading={!connectorsReady}
                    jobs={jobs}
                    schedules={schedules}
                    onAdd={openModal}
                    onEdit={openEditModal}
                    onDelete={handleDeleteConnector}
                    onRefresh={loadConnectors}
                    onConnectorPatch={patchConnector}
                    connectorEditorOpen={showModal && Boolean(editingConnector)}
                    onOpenTransfer={(connectorId) => {
                      if (connectorId) {
                        setTransferSeedSource({ connectorId, token: Date.now() });
                      }
                      setScreen("transfer");
                    }}
                    onOpenJob={(jobId) => navigateFromSearch({ screen: "jobs", jobId })}
                    showConnectionsTab={connectorsViewToken}
                    highlightConnectorId={
                      searchFocus?.screen === "connectors" ? searchFocus.connectorId : undefined
                    }
                  />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("schedules") && (
                <div className={`df2-screen-keep ${showScreen("schedules")}`} hidden={screen !== "schedules"} aria-hidden={screen !== "schedules"}>
                <PageErrorBoundary label="Schedules">
                  <SchedulesPage
                    connectors={connectors}
                    onViewJobs={() => setScreen("jobs")}
                    onOpenJob={(jobId) => navigateFromSearch({ screen: "jobs", jobId })}
                    onSchedulesChange={loadSchedules}
                    highlightScheduleId={
                      searchFocus?.screen === "schedules" ? searchFocus.scheduleId : undefined
                    }
                    onStartTransfer={(intent) => {
                      if (intent && (intent.sourceConnectorId || intent.destConnectorId || intent.step)) {
                        setTransferStudioIntent({ ...intent, token: Date.now() });
                      } else {
                        setTransferStudioIntent(null);
                      }
                      setScreen("transfer");
                    }}
                  />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("transforms") && (
                <div className={`df2-screen-keep ${showScreen("transforms")}`} hidden={screen !== "transforms"} aria-hidden={screen !== "transforms"}>
                <PageErrorBoundary label="Transformations">
                  <TransformsPage connectors={connectors} onNavigate={setScreen} />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("jobs") && (
                <div className={`df2-screen-keep ${showScreen("jobs")}`} hidden={screen !== "jobs"} aria-hidden={screen !== "jobs"}>
                <PageErrorBoundary label="Job Theater">
                  <JobsPage
                    jobs={jobs}
                    history={jobHistory}
                    onRefresh={loadJobs}
                    onStartTransfer={(intent) => {
                      if (intent && (intent.step || intent.repairProposalId || intent.jobId || intent.mappings?.length)) {
                        setTransferStudioIntent({ ...intent, token: Date.now() });
                      } else {
                        setTransferStudioIntent(null);
                      }
                      setScreen("transfer");
                    }}
                    initialJobId={searchFocus?.screen === "jobs" ? searchFocus.jobId : undefined}
                    initialPanel={searchFocus?.screen === "jobs" ? searchFocus.panel : undefined}
                  />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("contracts") && (
                <div className={`df2-screen-keep ${showScreen("contracts")}`} hidden={screen !== "contracts"} aria-hidden={screen !== "contracts"}>
                <PageErrorBoundary label="Contracts">
                  <ContractsPage active={screen === "contracts"} />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("mcp") && (
                <div className={`df2-screen-keep ${showScreen("mcp")}`} hidden={screen !== "mcp"} aria-hidden={screen !== "mcp"}>
                <PageErrorBoundary label="MCP Server">
                  <McpPage />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("docs") && (
                <div className={`df2-screen-keep ${showScreen("docs")}`} hidden={screen !== "docs"} aria-hidden={screen !== "docs"}>
                <PageErrorBoundary label="Docs">
                  <DocsPage />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("benchmarks") && (
                <div className={`df2-screen-keep ${showScreen("benchmarks")}`} hidden={screen !== "benchmarks"} aria-hidden={screen !== "benchmarks"}>
                <PageErrorBoundary label="Benchmarks">
                  <BenchmarksPage />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("settings") && (
                <div className={`df2-screen-keep ${showScreen("settings")}`} hidden={screen !== "settings"} aria-hidden={screen !== "settings"}>
                <PageErrorBoundary label="Settings">
                  <SettingsPage onOpenConnectors={() => setScreen("connectors")} />
                </PageErrorBoundary>
                </div>
              )}
            </LazyScreen>
          </div>
        </div>
        </div>
      </div>

      {showCopilotRail && (
        <aside className="df2-copilot-rail" aria-label="Datawrap Pilot">
          <AICopilot variant="rail" onNavigate={setScreen} onClose={() => setCopilotOpen(false)} />
        </aside>
      )}

      {showModal && (
        <ConnectorModal
          initialType={modalType}
          editing={editingConnector}
          onClose={() => { setShowModal(false); setEditingConnector(null); }}
          onSaved={async () => {
            await loadConnectors();
            setConnectorsViewToken((n) => n + 1);
            setScreen("connectors");
            // Toast owned by ConnectorModal (includes connector name) — do not double-fire.
          }}
        />
      )}

      {/* Mid-right edge tab only — no bottom-corner FAB (duplicates the rail Pilot). */}
      {showCopilotEdge && (
        <button
          type="button"
          className="df2-copilot-edge-open"
          onClick={() => setCopilotOpen(true)}
          aria-label="Expand Datawrap Pilot"
          title="Expand Datawrap Pilot"
        >
          <DtIcon name="sparkle" size={14} />
          <span>Pilot</span>
        </button>
      )}
    </div>
  );

  async function handleDeleteConnector(id: string) {
    if (!connectorWrite.allowed) {
      refuseConnectorWrite();
      return;
    }
    const target = connectors.find((c) => c.id === id);
    const confirmed = await confirm({
      title: `Delete ${target?.name ?? "this connector"}?`,
      message: "This removes saved credentials and route references for this connection. Schedules that used it will need a new connection.",
      confirmLabel: "Delete connection",
      cancelLabel: "Keep connection",
      tone: "danger",
    });
    if (!confirmed) return;

    try {
      await deleteConnector(id);
      await loadConnectors();
      toast({ title: "Connector removed", tone: "success" });
    } catch {
      toast({ title: "Delete failed", message: "Could not remove this connector.", tone: "error" });
    }
  }
}

export function DataTransferApp() {
  return (
    <ToastProvider>
      <ConfirmProvider>
        <DataTransferAppInner />
      </ConfirmProvider>
    </ToastProvider>
  );
}

function DataTransferAppInner() {
  const [stage, setStage] = useState<"landing" | "login" | "app">(() => {
    if (readStoredUser()) return "app";
    return "landing";
  });
  const [publicRoute, setPublicRoute] = useState<PublicRoute>(() => publicRouteFromHash(window.location.hash) ?? "home");
  const [entryScreen, setEntryScreen] = useState<Screen>(() => readAppHash() ?? "dashboard");
  const [userEmail, setUserEmail] = useState(readStoredUser);

  useEffect(() => {
    const syncFromHash = () => {
      const hash = window.location.hash;
      const screen = readAppHash();
      const pub = publicRouteFromHash(hash);
      const session = readStoredUser();

      if (session && screen) {
        setEntryScreen(screen);
        setStage("app");
        return;
      }
      if (pub) {
        setPublicRoute(pub);
        setStage("landing");
        return;
      }
      if (screen && !session) {
        setEntryScreen(screen);
        setStage("login");
        return;
      }
      setPublicRoute("home");
      setStage(session ? "app" : "landing");
    };

    syncFromHash();
    window.addEventListener("hashchange", syncFromHash);
    return () => window.removeEventListener("hashchange", syncFromHash);
  }, []);

  useEffect(() => {
    // Tokens now come in the URL fragment (not the query string) so they are
    // never sent to the server or logged by proxies.
    const fragment = window.location.hash.replace(/^#/, "");
    const params = new URLSearchParams(fragment || window.location.search);
    const token = params.get("sso_token");
    const expiresRaw = params.get("expires_at");
    const email = params.get("sso_email");
    if (!token || !expiresRaw || !email) return;

    const expires_at = Number(expiresRaw);
    writeSession(
      {
        email,
        name: email.split("@")[0] || email,
        role: "member",
        token,
        expires_at,
        signed_in_at: Date.now(),
      },
      true,
    );
    window.history.replaceState({}, "", window.location.pathname + window.location.search);
    setUserEmail(email);
    setStage("app");
  }, []);

  const requestApp = (target: Screen) => {
    setEntryScreen(target);
    setStage(userEmail ? "app" : "login");
  };

  const navigatePublic = (route: PublicRoute) => {
    setPublicRoute(route);
    setStage("landing");
    writePublicHash(route);
  };

  const handleAuthenticated = (email: string) => {
    setUserEmail(email);
    writeAppHash(entryScreen, true);
    setStage("app");
  };

  const signOut = () => {
    clearSession();
    clearActiveWorkspaceId();
    setUserEmail("");
    setEntryScreen("dashboard");
    setPublicRoute("home");
    writePublicHash("home", true);
    setStage("landing");
  };

  const marketingMeta = PUBLIC_PAGE_META[publicRoute];
  const publicMeta =
    stage === "landing"
      ? {
          title: marketingMeta.title,
          description: marketingMeta.description,
          keywords:
            marketingMeta.keywords
            || "Datawrap, data transfer, migration, ETL, Transfer Studio, semantic mapping, preflight",
          canonicalPath: marketingMeta.canonicalPath || "#/",
          ogType: "website" as const,
        }
      : stage === "login"
        ? metaForLogin()
        : metaForScreen(entryScreen);
  usePageMeta(publicMeta);

  useEffect(() => {
    if (stage === "app") {
      writeAppHash(entryScreen, true);
    }
  }, [stage, entryScreen]);

  return (
    <>
      {stage === "landing" && (
        <MarketingSite
          route={publicRoute}
          onNavigate={navigatePublic}
          onLogin={() => requestApp("dashboard")}
          onGetStarted={() => requestApp("transfer")}
        />
      )}

      {stage === "login" && (
        <LoginPage
          target={entryScreen}
          onAuthenticated={handleAuthenticated}
          onBack={() => {
            setPublicRoute("home");
            writePublicHash("home", true);
            setStage("landing");
          }}
          onLegal={(route) => {
            setPublicRoute(route);
            writePublicHash(route);
            setStage("landing");
          }}
        />
      )}

      {stage === "app" && (
        <DataProvider>
          <StudioActionsProvider>
            <PermissionsProvider signedIn={Boolean(userEmail)}>
              <ForcePasswordChange>
                <AppShell initialScreen={entryScreen} userEmail={userEmail} onSignOut={signOut} />
              </ForcePasswordChange>
            </PermissionsProvider>
          </StudioActionsProvider>
        </DataProvider>
      )}
    </>
  );
}
