import { useEffect, useState } from "react";

interface MigrationJob {
  id: string;
  status: "running" | "completed" | "failed" | "paused";
  source: string;
  destination: string;
  totalRows: number;
  processedRows: number;
  startTime: Date;
  throughput: number;
  errorCount: number;
  currentChunk: number;
  totalChunks: number;
}

interface MigrationControlProps {
  job: MigrationJob;
  onPause?: () => void;
  onResume?: () => void;
  onCancel?: () => void;
}

function AnimatedDataFlow({ active }: { active: boolean }) {
  return (
    <div className="dt-data-flow">
      <svg viewBox="0 0 400 60" preserveAspectRatio="xMidYMid meet">
        <defs>
          <linearGradient id="flow-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#00D4FF" />
            <stop offset="50%" stopColor="#7B61FF" />
            <stop offset="100%" stopColor="#00FF9D" />
          </linearGradient>
          <filter id="flow-glow">
            <feGaussianBlur stdDeviation="4" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {/* Source Node */}
        <g transform="translate(30, 30)">
          <circle r="24" fill="#0C1220" stroke="#FFB800" strokeWidth="2" />
          <circle r="20" fill="#1A2332" />
          <text y="5" textAnchor="middle" fill="#FFB800" fontSize="8" fontWeight="700">SRC</text>
        </g>

        {/* Flow Lines */}
        <g filter="url(#flow-glow)">
          <path
            d="M 54 30 L 346 30"
            fill="none"
            stroke="url(#flow-gradient)"
            strokeWidth="4"
            strokeLinecap="round"
            className={active ? "dt-flow-line-animated" : ""}
          />
        </g>

        {/* Data Packets */}
        {active && (
          <>
            <circle className="dt-data-packet" r="6" fill="#00D4FF">
              <animateMotion dur="2s" repeatCount="indefinite" path="M 54 30 L 346 30" />
            </circle>
            <circle className="dt-data-packet" r="6" fill="#7B61FF" style={{ animationDelay: "0.5s" }}>
              <animateMotion dur="2s" repeatCount="indefinite" path="M 54 30 L 346 30" begin="0.5s" />
            </circle>
            <circle className="dt-data-packet" r="6" fill="#00FF9D" style={{ animationDelay: "1s" }}>
              <animateMotion dur="2s" repeatCount="indefinite" path="M 54 30 L 346 30" begin="1s" />
            </circle>
          </>
        )}

        {/* AI Gate */}
        <g transform="translate(200, 30)">
          <rect x="-20" y="-16" width="40" height="32" rx="8" fill="#1A2332" stroke="#7B61FF" strokeWidth="2" />
          <text y="4" textAnchor="middle" fill="#7B61FF" fontSize="9" fontWeight="700">AI</text>
        </g>

        {/* Destination Node */}
        <g transform="translate(370, 30)">
          <circle r="24" fill="#0C1220" stroke="#00FF9D" strokeWidth="2" />
          <circle r="20" fill="#1A2332" />
          <text y="5" textAnchor="middle" fill="#00FF9D" fontSize="8" fontWeight="700">DST</text>
        </g>
      </svg>
    </div>
  );
}

function LiveCounter({ value, label, color }: { value: number; label: string; color: string }) {
  const [displayValue, setDisplayValue] = useState(0);

  useEffect(() => {
    const duration = 1000;
    const steps = 30;
    const increment = (value - displayValue) / steps;
    let current = displayValue;
    let step = 0;

    const timer = setInterval(() => {
      step++;
      current += increment;
      setDisplayValue(Math.round(current));
      if (step >= steps) {
        setDisplayValue(value);
        clearInterval(timer);
      }
    }, duration / steps);

    return () => clearInterval(timer);
  }, [value]);

  return (
    <div className="dt-live-counter">
      <span className="dt-live-counter-value" style={{ color }}>
        {displayValue.toLocaleString()}
      </span>
      <span className="dt-live-counter-label">{label}</span>
    </div>
  );
}

export function MigrationControl({ job, onPause, onResume, onCancel }: MigrationControlProps) {
  const progress = (job.processedRows / job.totalRows) * 100;
  const elapsed = Date.now() - job.startTime.getTime();
  const eta = job.throughput > 0 ? ((job.totalRows - job.processedRows) / job.throughput) * 1000 : 0;

  const formatTime = (ms: number) => {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    if (hours > 0) return `${hours}h ${minutes % 60}m`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
  };

  return (
    <div className="dt-migration-control">
      <div className="dt-migration-control-header">
        <div className="dt-migration-control-title-row">
          <h2 className="dt-migration-control-title">
            <span className="dt-migration-control-pulse" />
            Live Migration Center
          </h2>
          <div className={`dt-badge dt-badge--${job.status === "running" ? "success" : job.status === "failed" ? "danger" : "info"}`}>
            <span className="dt-badge-dot" />
            {job.status}
          </div>
        </div>
        <p className="dt-migration-control-subtitle">
          {job.source} → {job.destination}
        </p>
      </div>

      <div className="dt-migration-hero">
        <AnimatedDataFlow active={job.status === "running"} />
        
        <div className="dt-migration-progress-section">
          <div className="dt-migration-progress-header">
            <span className="dt-migration-progress-label">Migration Progress</span>
            <span className="dt-migration-progress-pct">{progress.toFixed(1)}%</span>
          </div>
          <div className="dt-migration-progress-track">
            <div
              className="dt-migration-progress-fill"
              style={{ width: `${progress}%` }}
            />
            {job.status === "running" && <div className="dt-migration-progress-glow" style={{ left: `${progress}%` }} />}
          </div>
          <div className="dt-migration-progress-stats">
            <span>{job.processedRows.toLocaleString()} / {job.totalRows.toLocaleString()} rows</span>
            <span>Chunk {job.currentChunk} of {job.totalChunks}</span>
          </div>
        </div>
      </div>

      <div className="dt-migration-metrics">
        <div className="dt-migration-metric dt-migration-metric--electric">
          <LiveCounter value={job.processedRows} label="Records Migrated" color="var(--dt-electric)" />
          <div className="dt-migration-metric-icon">📊</div>
        </div>
        <div className="dt-migration-metric dt-migration-metric--emerald">
          <LiveCounter value={job.throughput} label="Records/Second" color="var(--dt-emerald)" />
          <div className="dt-migration-metric-icon">⚡</div>
        </div>
        <div className="dt-migration-metric dt-migration-metric--purple">
          <div className="dt-live-counter">
            <span className="dt-live-counter-value" style={{ color: "var(--dt-purple)" }}>
              {formatTime(elapsed)}
            </span>
            <span className="dt-live-counter-label">Elapsed Time</span>
          </div>
          <div className="dt-migration-metric-icon">⏱️</div>
        </div>
        <div className="dt-migration-metric dt-migration-metric--amber">
          <div className="dt-live-counter">
            <span className="dt-live-counter-value" style={{ color: "var(--dt-amber)" }}>
              {eta > 0 ? formatTime(eta) : "Calculating..."}
            </span>
            <span className="dt-live-counter-label">Est. Remaining</span>
          </div>
          <div className="dt-migration-metric-icon">🎯</div>
        </div>
      </div>

      <div className="dt-migration-health">
        <div className="dt-migration-health-header">
          <h3 className="dt-migration-health-title">System Health</h3>
        </div>
        <div className="dt-migration-health-grid">
          <div className="dt-health-indicator dt-health-indicator--good">
            <span className="dt-health-indicator-icon">🟢</span>
            <span className="dt-health-indicator-label">Source Connection</span>
            <span className="dt-health-indicator-value">Healthy</span>
          </div>
          <div className="dt-health-indicator dt-health-indicator--good">
            <span className="dt-health-indicator-icon">🟢</span>
            <span className="dt-health-indicator-label">Destination Connection</span>
            <span className="dt-health-indicator-value">Healthy</span>
          </div>
          <div className={`dt-health-indicator ${job.errorCount > 0 ? "dt-health-indicator--warning" : "dt-health-indicator--good"}`}>
            <span className="dt-health-indicator-icon">{job.errorCount > 0 ? "🟡" : "🟢"}</span>
            <span className="dt-health-indicator-label">Error Count</span>
            <span className="dt-health-indicator-value">{job.errorCount}</span>
          </div>
          <div className="dt-health-indicator dt-health-indicator--good">
            <span className="dt-health-indicator-icon">🟢</span>
            <span className="dt-health-indicator-label">Data Quality</span>
            <span className="dt-health-indicator-value">99.9%</span>
          </div>
        </div>
      </div>

      <div className="dt-migration-actions">
        {job.status === "running" ? (
          <>
            <button className="dt-btn dt-btn-secondary" onClick={onPause}>
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <rect x="4" y="3" width="3" height="10" rx="1" fill="currentColor"/>
                <rect x="9" y="3" width="3" height="10" rx="1" fill="currentColor"/>
              </svg>
              Pause
            </button>
            <button className="dt-btn dt-btn-ghost" onClick={onCancel}>
              Cancel
            </button>
          </>
        ) : job.status === "paused" ? (
          <button className="dt-btn dt-btn-primary" onClick={onResume}>
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <path d="M5 3L13 8L5 13V3Z" fill="currentColor"/>
            </svg>
            Resume
          </button>
        ) : null}
      </div>
    </div>
  );
}

export function MigrationControlStyles() {
  return (
    <style>{`
      .dt-migration-control {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-6);
        padding: var(--dt-space-8);
        background: var(--dt-surface);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-2xl);
      }

      .dt-migration-control-header {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-2);
      }

      .dt-migration-control-title-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
      }

      .dt-migration-control-title {
        display: flex;
        align-items: center;
        gap: var(--dt-space-3);
        font-size: var(--dt-text-xl);
        font-weight: 700;
        color: var(--dt-text);
      }

      .dt-migration-control-pulse {
        width: 12px;
        height: 12px;
        background: var(--dt-emerald);
        border-radius: 50%;
        animation: dt-pulse 2s ease-in-out infinite;
        box-shadow: 0 0 20px var(--dt-emerald-glow);
      }

      .dt-migration-control-subtitle {
        font-size: var(--dt-text-sm);
        color: var(--dt-text-tertiary);
      }

      .dt-migration-hero {
        padding: var(--dt-space-8);
        background: linear-gradient(135deg, rgba(0, 212, 255, 0.05), rgba(123, 97, 255, 0.05));
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-data-flow {
        margin-bottom: var(--dt-space-8);
      }

      .dt-data-flow svg {
        width: 100%;
        height: 60px;
      }

      .dt-flow-line-animated {
        stroke-dasharray: 20 10;
        animation: dt-flow-dash 1s linear infinite;
      }

      @keyframes dt-flow-dash {
        to { stroke-dashoffset: -30; }
      }

      .dt-data-packet {
        filter: drop-shadow(0 0 8px currentColor);
      }

      .dt-migration-progress-section {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-3);
      }

      .dt-migration-progress-header {
        display: flex;
        justify-content: space-between;
        align-items: baseline;
      }

      .dt-migration-progress-label {
        font-size: var(--dt-text-sm);
        font-weight: 600;
        color: var(--dt-text);
      }

      .dt-migration-progress-pct {
        font-size: var(--dt-text-2xl);
        font-weight: 700;
        color: var(--dt-electric);
        font-variant-numeric: tabular-nums;
      }

      .dt-migration-progress-track {
        position: relative;
        height: 12px;
        background: rgba(255, 255, 255, 0.1);
        border-radius: var(--dt-radius-full);
        overflow: visible;
      }

      .dt-migration-progress-fill {
        height: 100%;
        background: linear-gradient(90deg, var(--dt-electric), var(--dt-purple), var(--dt-emerald));
        border-radius: var(--dt-radius-full);
        transition: width 0.3s var(--dt-ease);
        box-shadow: 0 0 20px var(--dt-electric-glow);
      }

      .dt-migration-progress-glow {
        position: absolute;
        top: 50%;
        transform: translate(-50%, -50%);
        width: 24px;
        height: 24px;
        background: var(--dt-electric);
        border-radius: 50%;
        filter: blur(8px);
        opacity: 0.8;
        animation: dt-glow 1s ease-in-out infinite;
      }

      .dt-migration-progress-stats {
        display: flex;
        justify-content: space-between;
        font-size: var(--dt-text-xs);
        color: var(--dt-text-tertiary);
      }

      .dt-migration-metrics {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: var(--dt-space-4);
      }

      .dt-migration-metric {
        position: relative;
        padding: var(--dt-space-5);
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
        overflow: hidden;
      }

      .dt-migration-metric::before {
        content: "";
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 2px;
      }

      .dt-migration-metric--electric::before { background: var(--dt-electric); }
      .dt-migration-metric--emerald::before { background: var(--dt-emerald); }
      .dt-migration-metric--purple::before { background: var(--dt-purple); }
      .dt-migration-metric--amber::before { background: var(--dt-amber); }

      .dt-migration-metric-icon {
        position: absolute;
        top: var(--dt-space-3);
        right: var(--dt-space-3);
        font-size: 20px;
        opacity: 0.5;
      }

      .dt-live-counter {
        display: flex;
        flex-direction: column;
        gap: 4px;
      }

      .dt-live-counter-value {
        font-size: var(--dt-text-2xl);
        font-weight: 700;
        font-variant-numeric: tabular-nums;
        line-height: 1;
      }

      .dt-live-counter-label {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-tertiary);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .dt-migration-health {
        padding: var(--dt-space-5);
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-migration-health-header {
        margin-bottom: var(--dt-space-4);
      }

      .dt-migration-health-title {
        font-size: var(--dt-text-sm);
        font-weight: 600;
        color: var(--dt-text);
      }

      .dt-migration-health-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: var(--dt-space-4);
      }

      .dt-health-indicator {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-1);
        padding: var(--dt-space-3);
        background: rgba(255, 255, 255, 0.02);
        border-radius: var(--dt-radius-md);
      }

      .dt-health-indicator-icon {
        font-size: 14px;
      }

      .dt-health-indicator-label {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-muted);
      }

      .dt-health-indicator-value {
        font-size: var(--dt-text-sm);
        font-weight: 600;
        color: var(--dt-text);
      }

      .dt-health-indicator--good .dt-health-indicator-value {
        color: var(--dt-emerald);
      }

      .dt-health-indicator--warning .dt-health-indicator-value {
        color: var(--dt-amber);
      }

      .dt-migration-actions {
        display: flex;
        justify-content: flex-end;
        gap: var(--dt-space-3);
        padding-top: var(--dt-space-4);
        border-top: 1px solid var(--dt-border);
      }

      @media (max-width: 1000px) {
        .dt-migration-metrics,
        .dt-migration-health-grid {
          grid-template-columns: repeat(2, 1fr);
        }
      }
    `}</style>
  );
}
