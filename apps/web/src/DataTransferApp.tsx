/**
 * DataFlow — Universal Data Platform
 */

import { lazy, Suspense, useCallback, useEffect, useRef, useState } from "react";
import { DtIcon } from "./components/DtIcon";
import { DtLogo } from "./components/DtLogo";
import { PageErrorBoundary } from "./components/PageErrorBoundary";
import { SectionLoader } from "./components/LoadingState";
import { useToast } from "./components/Toast";
import { WorkspaceSearch, type SearchNavigateTarget } from "./components/ui/WorkspaceSearch";
import { StatusPopover } from "./components/StatusPopover";
import { DataProvider } from "./lib/DataContext";
import { deleteConnector, fetchConnectors, fetchJobs, fetchSchedules } from "./lib/api";
import { clearSession, readSession, writeSession } from "./lib/session";
import { resolveCatalogIdToType } from "./lib/connectorTypes";
import { Connector, PipelineSchedule, Screen, TransferJob } from "./lib/types";
import { LoginPage } from "./pages/LoginPage";
import { LandingPage } from "./pages/LandingPage";
import { metaForLogin, metaForScreen } from "./lib/seo";
import { usePageMeta } from "./lib/usePageMeta";
import { readAppHash, writeAppHash } from "./lib/appNavigation";
import { apiEnvLabel, apiOfflineMessage } from "./lib/runtimeEnv";


const DashboardPage = lazy(() => import("./pages/DashboardPage").then((m) => ({ default: m.DashboardPage })));
const PilotPage = lazy(() => import("./pages/PilotPage").then((m) => ({ default: m.PilotPage })));
const TransferPage = lazy(() => import("./pages/TransferPage").then((m) => ({ default: m.TransferPage })));
const ConnectorsPage = lazy(() => import("./pages/ConnectorsPage").then((m) => ({ default: m.ConnectorsPage })));
const SchedulesPage = lazy(() => import("./pages/SchedulesPage").then((m) => ({ default: m.SchedulesPage })));
const JobsPage = lazy(() => import("./pages/JobsPage").then((m) => ({ default: m.JobsPage })));
const McpPage = lazy(() => import("./pages/McpPage").then((m) => ({ default: m.McpPage })));
const SettingsPage = lazy(() => import("./pages/SettingsPage").then((m) => ({ default: m.SettingsPage })));
const DocsPage = lazy(() => import("./pages/DocsPage").then((m) => ({ default: m.DocsPage })));
const AICopilot = lazy(() => import("./components/AICopilot").then((m) => ({ default: m.AICopilot })));
const ConnectorModal = lazy(() => import("./components/ConnectorModal").then((m) => ({ default: m.ConnectorModal })));

const NAV: { id: Screen; label: string; icon: string; desc: string }[] = [
  { id: "dashboard", label: "Overview", icon: "dashboard", desc: "Platform overview & live topology" },
  { id: "transfer", label: "Transfer Studio", icon: "transfer", desc: "Move any data anywhere" },
  { id: "pilot", label: "Data Pilot", icon: "sparkle", desc: "AI agent · natural language" },
  { id: "connectors", label: "Connectors", icon: "connectors", desc: "Sources & destinations" },
  { id: "schedules", label: "Pipelines", icon: "activity", desc: "Recurring scheduled syncs" },
  { id: "jobs", label: "Job Theater", icon: "jobs", desc: "Live transfer progress" },
  { id: "mcp", label: "MCP Server", icon: "zap", desc: "Cursor · Claude · VS Code" },
  { id: "docs", label: "Docs", icon: "book", desc: "How DataFlow works" },
  { id: "settings", label: "Settings", icon: "settings", desc: "Security & team" },
];

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

  const setScreen = useCallback((next: Screen) => {
    setScreenState(next);
    writeAppHash(next);
  }, []);

  useEffect(() => {
    const onHash = () => {
      const fromHash = readAppHash();
      if (fromHash) setScreenState(fromHash);
    };
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  const [connectors, setConnectors] = useState<Connector[]>([]);
  const [jobs, setJobs] = useState<TransferJob[]>([]);
  const [schedules, setSchedules] = useState<PipelineSchedule[]>([]);
  const [bootLoading, setBootLoading] = useState(true);
  const [apiOnline, setApiOnline] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [modalType, setModalType] = useState("");
  const [editingConnector, setEditingConnector] = useState<Connector | null>(null);
  const [copilotOpen, setCopilotOpen] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [sidebarNavCompact, setSidebarNavCompact] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchFocus, setSearchFocus] = useState<SearchNavigateTarget | null>(null);
  const [connectorsViewToken, setConnectorsViewToken] = useState(0);
  const [firstScreenPaint, setFirstScreenPaint] = useState(true);
  const searchRef = useRef<HTMLInputElement>(null);

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
      const timeout = window.setTimeout(() => {
        if (!cancelled) setBootLoading(false);
      }, 2500);
      await Promise.allSettled([
        loadConnectors(false),
        loadJobs(false),
        loadSchedules(),
      ]);
      if (!cancelled) {
        window.clearTimeout(timeout);
        setBootLoading(false);
      }
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
          <DtLogo size={36} />
          <div>
            <div className="df2-brand-name">DataFlow</div>
            <div className="df2-brand-tag">Universal data platform</div>
          </div>
          <button
            type="button"
            className="df2-sidebar-collapse-btn"
            onClick={() => setSidebarNavCompact((c) => !c)}
            aria-label={sidebarNavCompact ? "Expand menu labels" : "Compact menu icons"}
            title={sidebarNavCompact ? "Expand menu labels" : "Compact menu icons"}
          >
            <DtIcon name={sidebarNavCompact ? "chevron-right" : "chevron-left"} size={16} />
          </button>
        </div>

        <nav className="df2-nav">
          <div className="df2-nav-group-label">Platform</div>
          {NAV.slice(0, 6).map((item) => (
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
              {item.id === "jobs" && jobs.length > 0 && (
                <span className="df2-nav-badge" aria-hidden="true"> {jobs.length}</span>
              )}
            </button>
          ))}

          <div className="df2-nav-group-label">Developers</div>
          {NAV.slice(6).map((item) => (
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
              <button
                type="button"
                className={`df2-btn df2-btn-ghost ${copilotOpen ? "active" : ""}`}
                onClick={() => setCopilotOpen((o) => !o)}
                aria-label="Toggle Data Pilot"
              >
                <DtIcon name="sparkle" size={16} />
                <span className="df2-topbar-btn-text">Pilot</span>
              </button>
            )}
            {screen !== "transfer" && (
              <button type="button" className="df2-btn df2-btn-primary" onClick={() => setScreen("transfer")}>
                <DtIcon name="plus" size={16} />
                <span className="df2-topbar-btn-text">New transfer</span>
              </button>
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

        <div className="df2-content">
        {bootLoading && (
          <div className="df2-boot-progress" role="progressbar" aria-label="Loading workspace">
            <div className="df2-boot-progress-fill" />
          </div>
        )}
        <div
          className={`df2-content-inner ${
            screen === "pilot"
              ? "df2-content-flush"
              : screen === "transfer"
                ? "df2-content-studio"
                : "df2-content-fit"
          } ${bootLoading ? "is-booting" : ""} ${firstScreenPaint ? "is-first-screen" : ""}`}
        >
          <div className="df2-screen-panel">
            <Suspense
              fallback={(
                <div className="df2-route-suspense">
                  <SectionLoader title="Loading workspace…" hint="Preparing this view." />
                </div>
              )}
            >
              {screen === "dashboard" && (
                <PageErrorBoundary label="Overview">
                  <DashboardPage
                    connectors={connectors}
                    jobs={jobs}
                    schedules={schedules}
                    onNewTransfer={() => setScreen("transfer")}
                    onOpenPilot={() => setScreen("pilot")}
                    onOpenConnectors={() => setScreen("connectors")}
                    onOpenJobs={() => setScreen("jobs")}
                  />
                </PageErrorBoundary>
              )}
              {screen === "pilot" && (
                <PageErrorBoundary label="Data Pilot">
                  <PilotPage onNavigate={setScreen} />
                </PageErrorBoundary>
              )}
              {screen === "transfer" && (
                <PageErrorBoundary label="Transfer Studio">
                  <TransferPage
                    connectors={connectors}
                    onOpenSchedules={() => setScreen("schedules")}
                    onTransferComplete={() => {
                      loadJobs();
                      void loadSchedules();
                      toast({ title: "Transfer complete", message: "View progress in Job Theater.", tone: "success" });
                    }}
                  />
                </PageErrorBoundary>
              )}
              {screen === "connectors" && (
                <PageErrorBoundary label="Connectors">
                  <ConnectorsPage
                    connectors={connectors}
                    jobs={jobs}
                    schedules={schedules}
                    onAdd={openModal}
                    onEdit={openEditModal}
                    onDelete={handleDeleteConnector}
                    onRefresh={loadConnectors}
                    showConnectionsTab={connectorsViewToken}
                    highlightConnectorId={
                      searchFocus?.screen === "connectors" ? searchFocus.connectorId : undefined
                    }
                  />
                </PageErrorBoundary>
              )}
              {screen === "schedules" && (
                <PageErrorBoundary label="Pipelines">
                  <SchedulesPage
                    connectors={connectors}
                    onViewJobs={() => setScreen("jobs")}
                    onSchedulesChange={loadSchedules}
                    highlightScheduleId={
                      searchFocus?.screen === "schedules" ? searchFocus.scheduleId : undefined
                    }
                  />
                </PageErrorBoundary>
              )}
              {screen === "jobs" && (
                <PageErrorBoundary label="Job Theater">
                  <JobsPage
                    jobs={jobs}
                    onRefresh={loadJobs}
                    onStartTransfer={() => setScreen("transfer")}
                    initialJobId={searchFocus?.screen === "jobs" ? searchFocus.jobId : undefined}
                  />
                </PageErrorBoundary>
              )}
              {screen === "mcp" && (
                <PageErrorBoundary label="MCP Server">
                  <McpPage />
                </PageErrorBoundary>
              )}
              {screen === "docs" && (
                <PageErrorBoundary label="Docs">
                  <DocsPage />
                </PageErrorBoundary>
              )}
              {screen === "settings" && (
                <PageErrorBoundary label="Settings">
                  <SettingsPage />
                </PageErrorBoundary>
              )}
            </Suspense>
          </div>
        </div>
        </div>
      </div>

      {showCopilotRail && (
        <aside className="df2-copilot-rail" aria-label="Data Pilot">
          <Suspense fallback={<SectionLoader title="Loading Pilot…" size="md" />}>
            <AICopilot variant="rail" onNavigate={setScreen} onClose={() => setCopilotOpen(false)} />
          </Suspense>
        </aside>
      )}

      {showModal && (
        <Suspense fallback={null}>
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
        </Suspense>
      )}

      {screen !== "pilot" && !copilotOpen && (
        <button
          type="button"
          className="df2-copilot-fab"
          onClick={() => setCopilotOpen(true)}
          aria-label="Open Data Pilot"
          title="Data Pilot"
        >
          <DtIcon name="sparkle" size={22} />
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
  const [stage, setStage] = useState<"landing" | "login" | "app">(() => readStoredUser() ? "app" : "landing");
  const [entryScreen, setEntryScreen] = useState<Screen>("dashboard");
  const [userEmail, setUserEmail] = useState(readStoredUser);

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

  const handleAuthenticated = (email: string) => {
    setUserEmail(email);
    writeAppHash(entryScreen, true);
    setStage("app");
  };

  const signOut = () => {
    clearSession();
    setUserEmail("");
    setEntryScreen("dashboard");
    setStage("login");
  };

  const publicMeta =
    stage === "landing" ? metaForScreen("landing") : stage === "login" ? metaForLogin() : metaForScreen("dashboard");
  usePageMeta(publicMeta);

  useEffect(() => {
    if (stage === "app") {
      writeAppHash(entryScreen, true);
    }
  }, [stage, entryScreen]);

  return (
    <>
      {stage === "landing" && (
      <LandingPage
          onEnterApp={() => requestApp("dashboard")}
          onStartTransfer={() => requestApp("transfer")}
          onOpenPilot={() => requestApp("pilot")}
          onOpenMcp={() => requestApp("mcp")}
      />
      )}

      {stage === "login" && (
        <LoginPage
          target={entryScreen}
          onAuthenticated={handleAuthenticated}
          onBack={() => setStage("landing")}
        />
      )}

      {stage === "app" && (
        <DataProvider>
          <AppShell initialScreen={entryScreen} userEmail={userEmail} onSignOut={signOut} />
        </DataProvider>
      )}
    </>
  );
}
