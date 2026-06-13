import type { ReactNode } from "react";
import { DatabaseIcon, TransferIcon } from "./DatabaseIcons";
import { Button } from "./Button";

interface MetricCardProps {
  label: string;
  value: string | number;
  change?: string;
  trend?: "up" | "down" | "neutral";
  icon?: ReactNode;
  accent?: "orange" | "mint" | "neutral";
}

export function MetricCard({
  label,
  value,
  change,
  trend = "neutral",
  icon,
  accent = "neutral",
}: MetricCardProps) {
  return (
    <div className={`df-metric-card-v2 df-metric-card-v2--${accent}`}>
      <div className="df-metric-card-v2-header">
        <span className="df-metric-card-v2-label">{label}</span>
        {icon && <span className="df-metric-card-v2-icon">{icon}</span>}
      </div>
      <div className="df-metric-card-v2-value">{typeof value === "number" ? value.toLocaleString() : value}</div>
      {change && (
        <div className={`df-metric-card-v2-change df-metric-card-v2-change--${trend}`}>
          {trend === "up" && "↑"}
          {trend === "down" && "↓"}
          {change}
        </div>
      )}
    </div>
  );
}

interface QuickActionProps {
  icon: ReactNode;
  title: string;
  description: string;
  onClick?: () => void;
  accent?: "orange" | "mint" | "blue" | "purple";
}

export function QuickActionCard({ icon, title, description, onClick, accent = "orange" }: QuickActionProps) {
  return (
    <button type="button" className={`df-quick-action df-quick-action--${accent}`} onClick={onClick}>
      <span className="df-quick-action-icon">{icon}</span>
      <span className="df-quick-action-content">
        <span className="df-quick-action-title">{title}</span>
        <span className="df-quick-action-desc">{description}</span>
      </span>
      <span className="df-quick-action-arrow">→</span>
    </button>
  );
}

interface ActivityItemProps {
  icon: ReactNode;
  title: string;
  description: string;
  timestamp: string;
  status?: "success" | "running" | "failed" | "pending";
}

function ActivityItem({ icon, title, description, timestamp, status = "success" }: ActivityItemProps) {
  return (
    <div className="df-activity-item">
      <span className={`df-activity-item-icon df-activity-item-icon--${status}`}>{icon}</span>
      <div className="df-activity-item-content">
        <span className="df-activity-item-title">{title}</span>
        <span className="df-activity-item-desc">{description}</span>
      </div>
      <span className="df-activity-item-time">{timestamp}</span>
    </div>
  );
}

interface DashboardHeroProps {
  onNewTransfer?: () => void;
}

export function DashboardHero({ onNewTransfer }: DashboardHeroProps) {
  return (
    <div className="df-dashboard-hero">
      <div className="df-dashboard-hero-content">
        <div className="df-dashboard-hero-badge">Universal Data Platform</div>
        <h1 className="df-dashboard-hero-title">
          Transfer any data,
          <br />
          <span className="df-dashboard-hero-title--gradient">anywhere.</span>
        </h1>
        <p className="df-dashboard-hero-desc">
          One-click transfers from any source to any destination with AI-powered semantic mapping
          and enterprise-grade reliability.
        </p>
        <div className="df-dashboard-hero-actions">
          <Button variant="primary" onClick={onNewTransfer}>
            <TransferIcon size={18} />
            New transfer
          </Button>
          <Button variant="ghost">View documentation</Button>
        </div>
      </div>
      <div className="df-dashboard-hero-visual">
        <div className="df-dashboard-hero-flow">
          <div className="df-dashboard-hero-node df-dashboard-hero-node--source">
            <DatabaseIcon type="file" size={32} />
            <span>Source</span>
          </div>
          <div className="df-dashboard-hero-connector">
            <div className="df-dashboard-hero-line" />
            <div className="df-dashboard-hero-gate">
              <span>8 Gates</span>
            </div>
            <div className="df-dashboard-hero-line" />
          </div>
          <div className="df-dashboard-hero-node df-dashboard-hero-node--dest">
            <DatabaseIcon type="snowflake" size={32} />
            <span>Destination</span>
          </div>
        </div>
      </div>
    </div>
  );
}

interface DashboardProps {
  metrics: {
    totalTransfers: number;
    rowsTransferred: number;
    activeJobs: number;
    connectors: number;
  };
  recentActivity: Array<{
    id: string;
    type: string;
    source: string;
    destination: string;
    rows: number;
    status: "success" | "running" | "failed" | "pending";
    timestamp: string;
  }>;
  onNewTransfer?: () => void;
  onViewJobs?: () => void;
  onViewConnectors?: () => void;
}

export function Dashboard({
  metrics,
  recentActivity,
  onNewTransfer,
  onViewJobs,
  onViewConnectors,
}: DashboardProps) {
  return (
    <div className="df-dashboard">
      <DashboardHero onNewTransfer={onNewTransfer} />

      <section className="df-dashboard-metrics">
        <MetricCard
          label="Total transfers"
          value={metrics.totalTransfers}
          change="+12% this week"
          trend="up"
          accent="orange"
        />
        <MetricCard
          label="Rows transferred"
          value={metrics.rowsTransferred}
          change="+2.4M today"
          trend="up"
          accent="mint"
        />
        <MetricCard
          label="Active jobs"
          value={metrics.activeJobs}
          accent="neutral"
        />
        <MetricCard
          label="Connectors"
          value={`${metrics.connectors}+`}
          accent="neutral"
        />
      </section>

      <div className="df-dashboard-grid">
        <section className="df-dashboard-section">
          <div className="df-dashboard-section-header">
            <h2 className="df-dashboard-section-title">Quick actions</h2>
          </div>
          <div className="df-quick-actions">
            <QuickActionCard
              icon={<DatabaseIcon type="file" size={24} />}
              title="File → Database"
              description="Upload CSV, Excel, or JSON to any database"
              onClick={onNewTransfer}
              accent="orange"
            />
            <QuickActionCard
              icon={<DatabaseIcon type="postgresql" size={24} />}
              title="Database migration"
              description="Migrate data between any databases"
              onClick={onNewTransfer}
              accent="mint"
            />
            <QuickActionCard
              icon={<DatabaseIcon type="api" size={24} />}
              title="API → Database"
              description="Import REST API data to your warehouse"
              onClick={onNewTransfer}
              accent="purple"
            />
          </div>
        </section>

        <section className="df-dashboard-section">
          <div className="df-dashboard-section-header">
            <h2 className="df-dashboard-section-title">Recent activity</h2>
            <Button variant="ghost" onClick={onViewJobs}>
              View all
            </Button>
          </div>
          <div className="df-activity-list">
            {recentActivity.length === 0 ? (
              <div className="df-activity-empty">
                <TransferIcon size={40} />
                <span>No recent transfers</span>
                <p>Start your first transfer to see activity here</p>
              </div>
            ) : (
              recentActivity.slice(0, 5).map((item) => (
                <ActivityItem
                  key={item.id}
                  icon={<DatabaseIcon type={item.type} size={20} />}
                  title={`${item.source} → ${item.destination}`}
                  description={`${item.rows.toLocaleString()} rows`}
                  timestamp={item.timestamp}
                  status={item.status}
                />
              ))
            )}
          </div>
        </section>
      </div>

      <section className="df-dashboard-connectors">
        <div className="df-dashboard-section-header">
          <h2 className="df-dashboard-section-title">Supported connectors</h2>
          <Button variant="ghost" onClick={onViewConnectors}>
            Browse catalog
          </Button>
        </div>
        <div className="df-connector-showcase">
          {["postgresql", "snowflake", "mongodb", "mysql", "bigquery", "databricks", "redis", "oracle"].map(
            (db) => (
              <div key={db} className="df-connector-showcase-item">
                <DatabaseIcon type={db} size={32} />
                <span>{db}</span>
              </div>
            )
          )}
          <div className="df-connector-showcase-more">+610 more</div>
        </div>
      </section>
    </div>
  );
}
