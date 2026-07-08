/**
 * DataFlow — Universal Data Platform
 */

import { useCallback, useEffect, useState } from "react";
import { AICopilot } from "./components/AICopilot";
import { ConnectorModal } from "./components/ConnectorModal";
import { DtIcon } from "./components/DtIcon";
import { DtLogo } from "./components/DtLogo";
import { PageLoader } from "./components/LoadingState";
import { ToastProvider, useToast } from "./components/Toast";
import { DataProvider } from "./lib/DataContext";
import { deleteConnector, fetchConnectors, fetchJobs } from "./lib/api";
import { resolveCatalogIdToType } from "./lib/connectorTypes";
import { Connector, Screen, TransferJob } from "./lib/types";
import { ConnectorsPage } from "./pages/ConnectorsPage";
import { DashboardPage } from "./pages/DashboardPage";
import { JobsPage } from "./pages/JobsPage";
import { LandingPage } from "./pages/LandingPage";
import { LoginPage } from "./pages/LoginPage";
import { McpPage } from "./pages/McpPage";
import { PilotPage } from "./pages/PilotPage";
import { SchedulesPage } from "./pages/SchedulesPage";
import { SettingsPage } from "./pages/SettingsPage";
import { TransferPage } from "./pages/TransferPage";

const NAV: { id: Screen; label: string; icon: string; desc: string }[] = [
  { id: "dashboard", label: "Overview", icon: "dashboard", desc: "Platform overview & live topology" },
  { id: "transfer", label: "Transfer Studio", icon: "transfer", desc: "Move any data anywhere" },
  { id: "pilot", label: "Data Pilot", icon: "sparkle", desc: "AI agent · natural language" },
  { id: "connectors", label: "Connectors", icon: "connectors", desc: "Sources & destinations" },
  { id: "schedules", label: "Pipelines", icon: "activity", desc: "Recurring scheduled syncs" },
  { id: "jobs", label: "Job Theater", icon: "jobs", desc: "Live transfer progress" },
  { id: "mcp", label: "MCP Server", icon: "zap", desc: "Cursor · Claude · VS Code" },
  { id: "settings", label: "Settings", icon: "settings", desc: "Security & team" },
];

function readStoredUser() {
  try {
    const raw = localStorage.getItem("df2.session") || sessionStorage.getItem("df2.session");
    if (!raw) return "";
    const data = JSON.parse(raw) as { email?: string };
    return data.email || "";
  } catch {
    return "";
  }
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
  const [screen, setScreen] = useState<Screen>(initialScreen === "landing" ? "dashboard" : initialScreen);
  const [connectors, setConnectors] = useState<Connector[]>([]);
  const [jobs, setJobs] = useState<TransferJob[]>([]);
  const [bootLoading, setBootLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [modalType, setModalType] = useState("");
  const [editingConnector, setEditingConnector] = useState<Connector | null>(null);
  const [copilotOpen, setCopilotOpen] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  const loadConnectors = useCallback(async (notifyOnError = true) => {
    try {
      setConnectors(await fetchConnectors());
    } catch {
      if (notifyOnError) {
        toast({ title: "Could not load connectors", message: "Check that the API is running.", tone: "error" });
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

  useEffect(() => {
    (async () => {
      setBootLoading(true);
      await Promise.all([loadConnectors(false), loadJobs(false)]);
      setBootLoading(false);
    })();
  }, [loadConnectors, loadJobs]);

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

  return (
    <div className={`df2-app ${showCopilotRail ? "df2-app-with-rail" : ""}`}>
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
              <DtIcon name={item.icon} size={18} />
              <span>{item.label}</span>
              {item.id === "connectors" && connectors.length > 0 && (
                <span className="df2-nav-badge">{connectors.length}</span>
              )}
              {item.id === "jobs" && jobs.length > 0 && (
                <span className="df2-nav-badge">{jobs.length}</span>
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
              <DtIcon name={item.icon} size={18} />
              <span>{item.label}</span>
            </button>
          ))}
        </nav>

        <div className="df2-sidebar-foot">
          <button type="button" className="df2-sidebar-cta" onClick={() => setScreen("transfer")}>
            <DtIcon name="transfer" size={16} /> New transfer
          </button>
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
              <strong>{currentNav?.label ?? "DataFlow"}</strong>
            </div>
            <label className="df2-command-search" aria-label="Search workspace">
              <DtIcon name="search" size={15} />
              <input type="search" placeholder="Search connections, jobs, datasets..." />
            </label>
          </div>
          <div className="df2-topbar-actions">
            <div className="df2-system-pill" title="Control plane status">
              <span className="df2-system-dot" />
              <span>Control plane</span>
            </div>
            <button
              type="button"
              className="df2-account-pill"
              onClick={() => toast({ title: "Signed in", message: userEmail || "Workspace session active.", tone: "info" })}
              title={userEmail || "Workspace session"}
            >
              <DtIcon name="users" size={15} />
              <span>{userEmail ? userEmail.split("@")[0] : "Workspace"}</span>
            </button>
            {screen !== "pilot" && (
              <button
                type="button"
                className={`df2-btn df2-btn-ghost ${copilotOpen ? "active" : ""}`}
                onClick={() => setCopilotOpen((o) => !o)}
                aria-label="Toggle Data Pilot"
              >
                <DtIcon name="sparkle" size={16} /> Pilot
              </button>
            )}
            <button type="button" className="df2-btn df2-btn-primary" onClick={() => setScreen("transfer")}>
              <DtIcon name="plus" size={16} /> Transfer
            </button>
            <button type="button" className="df2-btn df2-btn-ghost" onClick={onSignOut}>
              Sign out
            </button>
          </div>
        </header>

        <div className={`df2-content ${screen === "pilot" ? "df2-content-flush" : ""}`}>
          {bootLoading ? (
            <PageLoader />
          ) : (
            <div key={screen}>
              {screen === "dashboard" && (
                <DashboardPage
                  connectors={connectors}
                  jobs={jobs}
                  onNewTransfer={() => setScreen("transfer")}
                  onOpenPilot={() => setScreen("pilot")}
                  onOpenConnectors={() => setScreen("connectors")}
                  onOpenJobs={() => setScreen("jobs")}
                />
              )}
              {screen === "pilot" && <PilotPage onNavigate={setScreen} />}
              {screen === "transfer" && (
                <TransferPage
                  connectors={connectors}
                  onTransferComplete={() => {
                    loadJobs();
                    setScreen("jobs");
                    toast({ title: "Transfer complete", message: "View progress in Job Theater.", tone: "success" });
                  }}
                />
              )}
              {screen === "connectors" && (
                <ConnectorsPage
                  connectors={connectors}
                  onAdd={openModal}
                  onEdit={openEditModal}
                  onDelete={handleDeleteConnector}
                />
              )}
              {screen === "schedules" && (
                <SchedulesPage connectors={connectors} onViewJobs={() => setScreen("jobs")} />
              )}
              {screen === "jobs" && (
                <JobsPage jobs={jobs} onRefresh={loadJobs} onStartTransfer={() => setScreen("transfer")} />
              )}
              {screen === "mcp" && <McpPage />}
              {screen === "settings" && <SettingsPage />}
            </div>
          )}
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
          onSaved={loadConnectors}
        />
      )}
    </div>
  );

  async function handleDeleteConnector(id: string) {
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

  const requestApp = (target: Screen) => {
    setEntryScreen(target);
    setStage(userEmail ? "app" : "login");
  };

  const handleAuthenticated = (email: string) => {
    setUserEmail(email);
    setStage("app");
  };

  const signOut = () => {
    try {
      localStorage.removeItem("df2.session");
      sessionStorage.removeItem("df2.session");
    } catch {
      /* storage unavailable */
    }
    setUserEmail("");
    setEntryScreen("dashboard");
    setStage("login");
  };

  return (
    <ToastProvider>
      {stage === "landing" && (
      <LandingPage
          onEnterApp={() => requestApp("dashboard")}
          onStartTransfer={() => requestApp("transfer")}
          onOpenPilot={() => requestApp("pilot")}
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
    </ToastProvider>
  );
}
