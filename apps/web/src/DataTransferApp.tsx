/**
 * DataTransfer.space — Enterprise Application Shell
 */

import { useCallback, useEffect, useState } from "react";
import { AICopilot } from "./components/AICopilot";
import { ConnectorModal } from "./components/ConnectorModal";
import { DtIcon } from "./components/DtIcon";
import { DtLogo } from "./components/DtLogo";
import { DataProvider } from "./lib/DataContext";
import { deleteConnector, fetchConnectors, fetchJobs } from "./lib/api";
import { Connector, Screen, TransferJob } from "./lib/types";
import { ConnectorsPage } from "./pages/ConnectorsPage";
import { DashboardPage } from "./pages/DashboardPage";
import { JobsPage } from "./pages/JobsPage";
import { McpPage } from "./pages/McpPage";
import { PilotPage } from "./pages/PilotPage";
import { SettingsPage } from "./pages/SettingsPage";
import { TransferPage } from "./pages/TransferPage";

const NAV: { id: Screen; label: string; icon: string; desc: string }[] = [
  { id: "dashboard", label: "Dashboard", icon: "dashboard", desc: "Platform overview" },
  { id: "pilot", label: "Data Pilot", icon: "sparkle", desc: "AI agent · automations" },
  { id: "transfer", label: "New Transfer", icon: "transfer", desc: "Move any data" },
  { id: "connectors", label: "Connectors", icon: "connectors", desc: "600+ integrations" },
  { id: "jobs", label: "Jobs", icon: "jobs", desc: "Transfer history" },
  { id: "mcp", label: "MCP Server", icon: "zap", desc: "Cursor · Claude · VS Code" },
  { id: "settings", label: "Settings", icon: "settings", desc: "Security & team" },
];

function AppShell() {
  const [screen, setScreen] = useState<Screen>("pilot");
  const [connectors, setConnectors] = useState<Connector[]>([]);
  const [jobs, setJobs] = useState<TransferJob[]>([]);
  const [showModal, setShowModal] = useState(false);
  const [modalType, setModalType] = useState("mongodb");

  const loadConnectors = useCallback(async () => {
    try { setConnectors(await fetchConnectors()); } catch (e) { console.error(e); }
  }, []);

  const loadJobs = useCallback(async () => {
    try { setJobs(await fetchJobs()); } catch (e) { console.error(e); }
  }, []);

  useEffect(() => {
    loadConnectors();
    loadJobs();
  }, [loadConnectors, loadJobs]);

  useEffect(() => {
    if (screen === "jobs" || screen === "dashboard") {
      loadJobs();
    }
  }, [screen, loadJobs]);

  const openModal = (type = "mongodb") => {
    setModalType(type);
    setShowModal(true);
  };

  const currentNav = NAV.find((n) => n.id === screen);

  return (
    <div className="dt-app">
      <nav className="dt-sidebar" aria-label="Main navigation">
        <div className="dt-sidebar-header">
          <div className="dt-brand">
            <DtLogo size={40} />
            <div className="dt-brand-text">
              <span className="dt-brand-name">DataTransfer</span>
              <span className="dt-brand-tagline">Meridian Platform</span>
            </div>
          </div>
        </div>

        <div className="dt-sidebar-nav">
          <div className="dt-nav-section">
            <div className="dt-nav-label">Platform</div>
            <ul className="dt-nav-list">
              {NAV.slice(0, 5).map((item) => (
                <li key={item.id}>
                  <button
                    type="button"
                    className={`dt-nav-item ${screen === item.id ? "active" : ""}`}
                    onClick={() => setScreen(item.id)}
                    title={item.desc}
                  >
                    <DtIcon name={item.icon} />
                    <span>{item.label}</span>
                    {item.id === "connectors" && connectors.length > 0 && (
                      <span className="dt-nav-badge">{connectors.length}</span>
                    )}
                    {item.id === "jobs" && jobs.length > 0 && (
                      <span className="dt-nav-badge">{jobs.length}</span>
                    )}
                  </button>
                </li>
              ))}
            </ul>
          </div>
          <div className="dt-nav-section">
            <div className="dt-nav-label">Developers</div>
            <ul className="dt-nav-list">
              {NAV.slice(5).map((item) => (
                <li key={item.id}>
                  <button
                    type="button"
                    className={`dt-nav-item ${screen === item.id ? "active" : ""}`}
                    onClick={() => setScreen(item.id)}
                    title={item.desc}
                  >
                    <DtIcon name={item.icon} />
                    <span>{item.label}</span>
                  </button>
                </li>
              ))}
            </ul>
          </div>
        </div>

        <div className="dt-sidebar-footer">
          <div className="dt-user-card">
            <div className="dt-user-avatar">DT</div>
            <div>
              <div className="dt-user-name">Enterprise</div>
              <div className="dt-user-role">datatransfer.space</div>
            </div>
          </div>
        </div>
      </nav>

      <main className="dt-main">
        <header className="dt-header">
          <h1 className="dt-header-title">{currentNav?.label}</h1>
          <div className="dt-header-actions">
            <button type="button" className="dt-btn dt-btn-ghost dt-btn-icon" aria-label="Notifications">
              <DtIcon name="bell" size={18} />
            </button>
            <button type="button" className="dt-btn dt-btn-ghost dt-btn-icon" onClick={() => setScreen("settings")} aria-label="Settings">
              <DtIcon name="settings" size={18} />
            </button>
          </div>
        </header>

        <div className={`dt-main-body ${screen === "pilot" ? "dt-main-body-flush" : ""}`}>
          {screen === "dashboard" && (
            <DashboardPage connectors={connectors} jobs={jobs} onNewTransfer={() => setScreen("transfer")} />
          )}
          {screen === "pilot" && <PilotPage onNavigate={setScreen} />}
          {screen === "transfer" && (
            <TransferPage connectors={connectors} onTransferComplete={() => { loadJobs(); setScreen("jobs"); }} />
          )}
          {screen === "connectors" && (
            <ConnectorsPage connectors={connectors} onAdd={openModal} onDelete={handleDeleteConnector} />
          )}
          {screen === "jobs" && <JobsPage jobs={jobs} />}
          {screen === "mcp" && <McpPage />}
          {screen === "settings" && <SettingsPage />}
        </div>
      </main>

      {showModal && (
        <ConnectorModal initialType={modalType} onClose={() => setShowModal(false)} onSaved={loadConnectors} />
      )}

      {screen !== "pilot" && <AICopilot onNavigate={setScreen} />}
    </div>
  );

  async function handleDeleteConnector(id: string) {
    try {
      await deleteConnector(id);
      loadConnectors();
    } catch (e) {
      console.error(e);
    }
  }
}

export function DataTransferApp() {
  return (
    <DataProvider>
      <AppShell />
    </DataProvider>
  );
}
