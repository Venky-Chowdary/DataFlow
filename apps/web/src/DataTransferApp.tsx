/**
 * DataFlow — Universal Data Platform
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { DtIcon } from "./components/DtIcon";
import { DtLogo } from "./components/DtLogo";
import { PageErrorBoundary } from "./components/PageErrorBoundary";
import { ToastProvider, useToast } from "./components/Toast";
import { Button } from "./components/ui/Button";
import { WorkspaceSearch, type SearchNavigateTarget } from "./components/ui/WorkspaceSearch";
import { StatusPopover } from "./components/StatusPopover";
import { DataProvider } from "./lib/DataContext";
import { StudioActionsProvider } from "./lib/StudioActionsContext";
import { AUTH_REQUIRED_EVENT, deleteConnector, fetchConnectors, fetchJobs, fetchSchedules } from "./lib/api";
import { clearSession, readSession, writeSession } from "./lib/session";
import { loadSidebarNavCompact, saveSidebarNavCompact } from "./lib/pilotChatStore";
import { resolveCatalogIdToType } from "./lib/connectorTypes";
import { Connector, PipelineSchedule, Screen, TransferJob } from "./lib/types";
import { LoginPage } from "./pages/LoginPage";
import { MarketingSite } from "./pages/marketing/MarketingSite";
import { DashboardPage } from "./pages/DashboardPage";
import { PilotPage } from "./pages/PilotPage";
import { TransferPage } from "./pages/TransferPage";
import { ConnectorsPage } from "./pages/ConnectorsPage";
import { SchedulesPage } from "./pages/SchedulesPage";
import { JobsPage } from "./pages/JobsPage";
import { ContractsPage } from "./pages/ContractsPage";
import { McpPage } from "./pages/McpPage";
import { QueryPage } from "./pages/QueryPage";
import { SettingsPage } from "./pages/SettingsPage";
import { DocsPage } from "./pages/DocsPage";
import { BenchmarksPage } from "./pages/BenchmarksPage";
import { AICopilot } from "./components/AICopilot";
import { ConnectorModal } from "./components/ConnectorModal";
import { readAppHash, writeAppHash } from "./lib/appNavigation";
import {
  PUBLIC_PAGE_META,
  publicRouteFromHash,
  type PublicRoute,
  writePublicHash,
} from "./lib/publicNavigation";
import { apiEnvLabel, apiOfflineMessage } from "./lib/runtimeEnv";
import { usePageMeta } from "./lib/usePageMeta";
import { metaForLogin, metaForScreen } from "./lib/seo";

const NAV: { id: Screen; label: string; icon: string; desc: string; group: "platform" | "ops" | "system" }[] = [
  { id: "dashboard", label: "Overview", icon: "dashboard", desc: "Health, throughput, and recent jobs", group: "platform" },
  { id: "transfer", label: "Transfer", icon: "transfer", desc: "Move data with preflight gates", group: "platform" },
  { id: "connectors", label: "Connectors", icon: "connectors", desc: "Saved sources & destinations", group: "platform" },
  { id: "contracts", label: "Contracts", icon: "shield", desc: "Schema agreements and breakers", group: "platform" },
  { id: "jobs", label: "Jobs", icon: "jobs", desc: "Live progress and history", group: "ops" },
  { id: "schedules", label: "Pipelines", icon: "activity", desc: "Recurring syncs", group: "ops" },
  { id: "query", label: "Query", icon: "search", desc: "Ad-hoc SQL and export", group: "ops" },
  { id: "pilot", label: "Pilot", icon: "sparkle", desc: "Natural-language assistant", group: "ops" },
  { id: "settings", label: "Settings", icon: "settings", desc: "Security, team, SSO", group: "system" },
  { id: "mcp", label: "MCP", icon: "zap", desc: "IDE tool integrations", group: "system" },
  { id: "docs", label: "Help", icon: "book", desc: "How DataFlow works", group: "system" },
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
  const [screen, setScreenState] = useState<Screen>(() => {
    const fromHash = readAppHash();
    if (fromHash) return fromHash;
    return initialScreen === "landing" ? "dashboard" : initialScreen;
  });
  const [connectors, setConnectors] = useState<Connector[]>([]);
  const [jobs, setJobs] = useState<TransferJob[]>([]);
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

  useEffect(() => {
    const onHash = () => {
      const fromHash = readAppHash();
      if (fromHash) setScreen(fromHash);
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

  const loadConnectors = useCallback(async (notifyOnError = true) => {
    try {
      setConnectors(await fetchConnectors());
      setApiOnline(true);
    } catch {
      setApiOnline(false);
      if (notifyOnError) {
        toast({ title: "Could not load connectors", message: "Check the API URL (VITE_API_BASE / DATAFLOW_API_BASE) or sign in.", tone: "error" });
      }
    } finally {
      setConnectorsReady(true);
    }
  }, [toast]);

  const loadJobs = useCallback(async (notifyOnError = true) => {
    try {
      setJobs(await fetchJobs());
    } catch {
      if (notifyOnError) {
        toast({ title: "Could not load jobs", message: "Job history may be unavailable.", tone: "warning" });
      }
    }
  }, [toast]);

  const loadSchedules = useCallback(async () => {
    try {
      setSchedules(await fetchSchedules());
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
      if (window.innerWidth >= 1024) setMobileNavOpen(false);
      if (window.innerWidth < 1280) setCopilotOpen(false);
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  useEffect(() => {
    const poll = window.setInterval(() => {
      void loadConnectors(false);
    }, 30000);
    return () => window.clearInterval(poll);
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
  const userShort = userEmail ? userEmail.split("@")[0] : "User";

  const openModal = (type?: string) => {
    setEditingConnector(null);
    setModalType(type ? resolveCatalogIdToType(type) : "");
    setShowModal(true);
  };

  const openEditModal = (connector: Connector) => {
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
  const currentNav = NAV.find((n) => n.id === screen);
  const envLabel = apiEnvLabel(apiOnline);
  const offlineCopy = apiOfflineMessage();
  const runningJobsCount = jobs.filter((j) => j.status === "running" || j.status === "pending").length;
  const failedJobsCount = jobs.filter((j) => j.status === "failed").length;
  const unhealthyConnectorsCount = connectors.filter((c) => c.status === "error" || c.last_test_ok === false).length;
  return (
    <div className={`df2-app ${showCopilotRail ? "df2-app-with-rail" : ""} ${sidebarNavCompact ? "df2-sidebar-nav-compact" : ""}`}>
      {mobileNavOpen && (
        <div className="df2-overlay" onClick={() => setMobileNavOpen(false)} role="presentation" />
      )}

      <aside className={`df2-sidebar ${mobileNavOpen ? "open" : ""}`} aria-label="Main navigation">
        <div className="df2-sidebar-brand">
          <DtLogo size={40} />
          <div className="df2-sidebar-brand-copy">
            <div className="df2-brand-name">DataFlow</div>
            <div className="df2-brand-tag">Universal data platform</div>
          </div>
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
          >
            <DtIcon name={sidebarNavCompact ? "chevron-right" : "chevron-left"} size={16} />
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
              {item.id === "jobs" && jobs.length > 0 && (
                <span className="df2-nav-badge" aria-hidden="true"> {jobs.length}</span>
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
          <button type="button" className="df2-sidebar-cta" onClick={() => setScreen("transfer")}>
            <DtIcon name="transfer" size={16} />
            <span className="df2-sidebar-collapse-label">New transfer</span>
          </button>
          <div className={`df2-sidebar-env ${apiOnline ? "" : "offline"}`} title="Control plane health">
            <span className="df2-system-dot" />
            <strong>{apiOnline ? "API connected" : "API offline"}</strong>
            <small>{apiOnline ? envLabel : "Check API service"}</small>
          </div>
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
              <strong> {currentNav?.label ?? "DataFlow"}</strong>
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
            <div className={`df2-system-pill ${apiOnline ? "" : "degraded"}`} title="Control plane status">
              <span className="df2-system-dot" />
              <span className="df2-topbar-pill-text">{apiOnline ? "Online" : "Offline"}</span>
            </div>
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
                aria-label="Toggle Data Pilot"
                leadingIcon={<DtIcon name="sparkle" size={16} />}
              >
                <span className="df2-topbar-btn-text">Pilot</span>
              </Button>
            )}
            {screen !== "transfer" && (
              <Button
                variant="primary"
                onClick={() => setScreen("transfer")}
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
            {mountedScreens.has("dashboard") && (
                <div className={`df2-screen-keep ${showScreen("dashboard")}`} hidden={screen !== "dashboard"} aria-hidden={screen !== "dashboard"}>
                <PageErrorBoundary label="Overview">
                  <DashboardPage
                    connectors={connectors}
                    jobs={jobs}
                    schedules={schedules}
                    onOpenConnectors={() => setScreen("connectors")}
                    onOpenJobs={() => setScreen("jobs")}
                  />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("pilot") && (
                <div className={`df2-screen-keep ${showScreen("pilot")}`} hidden={screen !== "pilot"} aria-hidden={screen !== "pilot"}>
                <PageErrorBoundary label="Data Pilot">
                  <PilotPage onNavigate={setScreen} />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("transfer") && (
                <div className={`df2-screen-keep ${showScreen("transfer")}`} hidden={screen !== "transfer"} aria-hidden={screen !== "transfer"}>
                <PageErrorBoundary label="Transfer Studio">
                  <TransferPage
                    connectors={connectors}
                    connectorsLoading={!connectorsReady}
                    onOpenSchedules={() => setScreen("schedules")}
                    onOpenContracts={() => setScreen("contracts")}
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
                    onOpenTransfer={() => setScreen("transfer")}
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
                <PageErrorBoundary label="Pipelines">
                  <SchedulesPage
                    connectors={connectors}
                    onViewJobs={() => setScreen("jobs")}
                    onOpenJob={(jobId) => navigateFromSearch({ screen: "jobs", jobId })}
                    onSchedulesChange={loadSchedules}
                    highlightScheduleId={
                      searchFocus?.screen === "schedules" ? searchFocus.scheduleId : undefined
                    }
                  />
                </PageErrorBoundary>
                </div>
              )}
              {mountedScreens.has("jobs") && (
                <div className={`df2-screen-keep ${showScreen("jobs")}`} hidden={screen !== "jobs"} aria-hidden={screen !== "jobs"}>
                <PageErrorBoundary label="Job Theater">
                  <JobsPage
                    jobs={jobs}
                    onRefresh={loadJobs}
                    onStartTransfer={() => setScreen("transfer")}
                    initialJobId={searchFocus?.screen === "jobs" ? searchFocus.jobId : undefined}
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
                  <SettingsPage />
                </PageErrorBoundary>
                </div>
              )}
          </div>
        </div>
        </div>
      </div>

      {showCopilotRail && (
        <aside className="df2-copilot-rail" aria-label="Data Pilot">
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
            toast({ title: "Connection saved", message: "Visible in My connections.", tone: "success" });
          }}
        />
      )}

      {/* Mid-right edge tab only — no bottom-corner FAB (duplicates the rail Pilot). */}
      {screen !== "pilot" && !copilotOpen && (
        <button
          type="button"
          className="df2-copilot-edge-open"
          onClick={() => setCopilotOpen(true)}
          aria-label="Expand Data Pilot"
          title="Expand Data Pilot"
        >
          <DtIcon name="chevron-left" size={14} />
          <span>Pilot</span>
        </button>
      )}
    </div>
  );

  async function handleDeleteConnector(id: string) {
    const target = connectors.find((c) => c.id === id);
    const confirmed = window.confirm(
      `Delete ${target?.name ?? "this connector"}? This removes saved credentials and route references for this connection.`,
    );
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
      <DataTransferAppInner />
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
    const params = new URLSearchParams(window.location.search);
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
    window.history.replaceState({}, "", window.location.pathname + window.location.hash);
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
          keywords: "DataFlow, data transfer, migration, ETL, Transfer Studio",
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
        />
      )}

      {stage === "app" && (
        <DataProvider>
          <StudioActionsProvider>
            <AppShell initialScreen={entryScreen} userEmail={userEmail} onSignOut={signOut} />
          </StudioActionsProvider>
        </DataProvider>
      )}
    </>
  );
}
