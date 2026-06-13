import { useEffect, useRef, useState } from "react";

const CONNECTORS = [
  { id: "mongodb", name: "MongoDB", color: "#13AA52", x: 15, y: 20 },
  { id: "postgresql", name: "PostgreSQL", color: "#336791", x: 85, y: 15 },
  { id: "snowflake", name: "Snowflake", color: "#29B5E8", x: 75, y: 35 },
  { id: "oracle", name: "Oracle", color: "#F80000", x: 10, y: 55 },
  { id: "sqlserver", name: "SQL Server", color: "#CC2927", x: 90, y: 55 },
  { id: "mysql", name: "MySQL", color: "#00758F", x: 20, y: 80 },
  { id: "dynamodb", name: "DynamoDB", color: "#4053D6", x: 80, y: 80 },
  { id: "elasticsearch", name: "Elasticsearch", color: "#FEC514", x: 35, y: 10 },
  { id: "kafka", name: "Kafka", color: "#231F20", x: 65, y: 10 },
  { id: "salesforce", name: "Salesforce", color: "#00A1E0", x: 25, y: 40 },
  { id: "sap", name: "SAP", color: "#0FAAFF", x: 75, y: 60 },
  { id: "s3", name: "S3", color: "#FF9900", x: 40, y: 70 },
  { id: "bigquery", name: "BigQuery", color: "#4285F4", x: 60, y: 70 },
  { id: "redis", name: "Redis", color: "#DC382D", x: 50, y: 25 },
  { id: "csv", name: "CSV", color: "#4CAF50", x: 30, y: 60 },
  { id: "json", name: "JSON", color: "#F7DF1E", x: 70, y: 45 },
];

const CONNECTIONS = [
  { from: 0, to: 2 }, { from: 1, to: 4 }, { from: 2, to: 6 },
  { from: 3, to: 5 }, { from: 4, to: 6 }, { from: 5, to: 11 },
  { from: 7, to: 13 }, { from: 8, to: 13 }, { from: 9, to: 14 },
  { from: 10, to: 15 }, { from: 11, to: 12 }, { from: 12, to: 15 },
  { from: 13, to: 2 }, { from: 14, to: 0 }, { from: 3, to: 9 },
];

function ConnectorNode({ connector, index }: { connector: typeof CONNECTORS[0]; index: number }) {
  return (
    <g
      className="dt-connector-node"
      style={{
        transform: `translate(${connector.x}%, ${connector.y}%)`,
        animationDelay: `${index * 100}ms`,
      }}
    >
      <circle
        r="28"
        fill={`${connector.color}20`}
        stroke={connector.color}
        strokeWidth="2"
        className="dt-node-pulse"
      />
      <circle r="18" fill="#0C1220" stroke={connector.color} strokeWidth="1.5" />
      <text
        y="5"
        textAnchor="middle"
        fill="#fff"
        fontSize="8"
        fontWeight="600"
        fontFamily="Inter, sans-serif"
      >
        {connector.name.slice(0, 3).toUpperCase()}
      </text>
    </g>
  );
}

function DataConnection({ from, to, index }: { from: typeof CONNECTORS[0]; to: typeof CONNECTORS[0]; index: number }) {
  return (
    <line
      x1={`${from.x}%`}
      y1={`${from.y}%`}
      x2={`${to.x}%`}
      y2={`${to.y}%`}
      stroke="url(#neon-gradient)"
      strokeWidth="1.5"
      strokeDasharray="8 4"
      className="dt-data-line"
      style={{ animationDelay: `${index * 200}ms` }}
    />
  );
}

export function DataUniverseVisualization() {
  return (
    <div className="dt-data-universe">
      <svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice">
        <defs>
          <linearGradient id="neon-gradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#00D4FF" stopOpacity="0.8" />
            <stop offset="50%" stopColor="#7B61FF" stopOpacity="0.6" />
            <stop offset="100%" stopColor="#00FF9D" stopOpacity="0.4" />
          </linearGradient>
          <filter id="glow">
            <feGaussianBlur stdDeviation="2" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        <g filter="url(#glow)">
          {CONNECTIONS.map((conn, i) => (
            <DataConnection
              key={i}
              from={CONNECTORS[conn.from]}
              to={CONNECTORS[conn.to]}
              index={i}
            />
          ))}
        </g>

        {CONNECTORS.map((connector, i) => (
          <ConnectorNode key={connector.id} connector={connector} index={i} />
        ))}
      </svg>

      <div className="dt-data-universe-overlay" />
    </div>
  );
}

interface HeroLandingProps {
  onStartTransfer?: () => void;
  onViewDemo?: () => void;
  onAIStudio?: () => void;
}

export function HeroLanding({ onStartTransfer, onViewDemo, onAIStudio }: HeroLandingProps) {
  const [particles, setParticles] = useState<Array<{ id: number; x: number; y: number; delay: number }>>([]);

  useEffect(() => {
    const newParticles = Array.from({ length: 50 }, (_, i) => ({
      id: i,
      x: Math.random() * 100,
      y: Math.random() * 100,
      delay: Math.random() * 5,
    }));
    setParticles(newParticles);
  }, []);

  return (
    <div className="dt-hero">
      {/* Animated background */}
      <div className="dt-hero-bg">
        {particles.map((p) => (
          <div
            key={p.id}
            className="dt-hero-particle"
            style={{
              left: `${p.x}%`,
              top: `${p.y}%`,
              animationDelay: `${p.delay}s`,
            }}
          />
        ))}
        <div className="dt-hero-gradient" />
      </div>

      <div className="dt-hero-content">
        <div className="dt-hero-text">
          <div className="dt-hero-badge">
            <span className="dt-hero-badge-dot" />
            Universal Data Platform
          </div>

          <h1 className="dt-hero-title">
            Move Any Data.
            <br />
            <span className="dt-hero-title-gradient">Anywhere.</span>
          </h1>

          <p className="dt-hero-desc">
            Transfer, transform, validate, and synchronize data across databases, files, 
            cloud platforms, APIs, and enterprise systems using AI-powered semantic mapping.
          </p>

          <div className="dt-hero-stats">
            <div className="dt-hero-stat">
              <span className="dt-hero-stat-value">600+</span>
              <span className="dt-hero-stat-label">Connectors</span>
            </div>
            <div className="dt-hero-stat-divider" />
            <div className="dt-hero-stat">
              <span className="dt-hero-stat-value">99.9%</span>
              <span className="dt-hero-stat-label">Uptime</span>
            </div>
            <div className="dt-hero-stat-divider" />
            <div className="dt-hero-stat">
              <span className="dt-hero-stat-value">50B+</span>
              <span className="dt-hero-stat-label">Rows/Month</span>
            </div>
          </div>

          <div className="dt-hero-actions">
            <button className="dt-btn dt-btn-primary dt-btn-lg" onClick={onStartTransfer}>
              <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <path d="M4 10H16M16 10L12 6M16 10L12 14" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
              Start Transfer
            </button>
            <button className="dt-btn dt-btn-secondary dt-btn-lg" onClick={onViewDemo}>
              <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <path d="M8 6.5L14 10L8 13.5V6.5Z" fill="currentColor"/>
                <circle cx="10" cy="10" r="8" stroke="currentColor" strokeWidth="1.5"/>
              </svg>
              View Demo
            </button>
            <button className="dt-btn dt-btn-neon dt-btn-lg" onClick={onAIStudio}>
              <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <path d="M10 2L12.5 7L18 8L14 12L15 18L10 15L5 18L6 12L2 8L7.5 7L10 2Z" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round"/>
              </svg>
              AI Migration Studio
            </button>
          </div>

          <div className="dt-hero-trust">
            <span className="dt-hero-trust-label">Trusted by Fortune 500</span>
            <div className="dt-hero-trust-logos">
              <span>SOC 2</span>
              <span>ISO 27001</span>
              <span>GDPR</span>
              <span>HIPAA</span>
            </div>
          </div>
        </div>

        <div className="dt-hero-visual">
          <DataUniverseVisualization />
        </div>
      </div>
    </div>
  );
}

export function HeroLandingStyles() {
  return (
    <style>{`
      .dt-hero {
        position: relative;
        min-height: 100vh;
        display: flex;
        align-items: center;
        padding: var(--dt-space-8);
        overflow: hidden;
      }

      .dt-hero-bg {
        position: absolute;
        inset: 0;
        background: radial-gradient(ellipse 80% 60% at 50% 40%, rgba(0, 212, 255, 0.08), transparent),
                    radial-gradient(ellipse 60% 50% at 80% 20%, rgba(123, 97, 255, 0.06), transparent),
                    radial-gradient(ellipse 50% 40% at 20% 80%, rgba(0, 255, 157, 0.04), transparent);
      }

      .dt-hero-particle {
        position: absolute;
        width: 2px;
        height: 2px;
        background: var(--dt-electric);
        border-radius: 50%;
        opacity: 0;
        animation: dt-particle-float 8s ease-in-out infinite;
      }

      @keyframes dt-particle-float {
        0%, 100% { opacity: 0; transform: translateY(0); }
        50% { opacity: 0.6; transform: translateY(-20px); }
      }

      .dt-hero-gradient {
        position: absolute;
        inset: 0;
        background: linear-gradient(180deg, transparent 0%, var(--dt-black) 100%);
        pointer-events: none;
      }

      .dt-hero-content {
        position: relative;
        z-index: 1;
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: var(--dt-space-16);
        align-items: center;
        max-width: 1400px;
        margin: 0 auto;
        width: 100%;
      }

      .dt-hero-text {
        max-width: 600px;
      }

      .dt-hero-badge {
        display: inline-flex;
        align-items: center;
        gap: var(--dt-space-2);
        padding: var(--dt-space-2) var(--dt-space-4);
        background: rgba(0, 212, 255, 0.1);
        border: 1px solid rgba(0, 212, 255, 0.3);
        border-radius: var(--dt-radius-full);
        font-size: var(--dt-text-xs);
        font-weight: 600;
        color: var(--dt-electric);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: var(--dt-space-6);
      }

      .dt-hero-badge-dot {
        width: 8px;
        height: 8px;
        background: var(--dt-electric);
        border-radius: 50%;
        animation: dt-pulse 2s ease-in-out infinite;
      }

      .dt-hero-title {
        font-size: var(--dt-text-hero);
        font-weight: 800;
        line-height: 1.05;
        letter-spacing: -0.03em;
        color: var(--dt-text);
        margin-bottom: var(--dt-space-6);
      }

      .dt-hero-title-gradient {
        background: linear-gradient(135deg, var(--dt-electric), var(--dt-purple), var(--dt-emerald));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
      }

      .dt-hero-desc {
        font-size: var(--dt-text-lg);
        line-height: 1.7;
        color: var(--dt-text-secondary);
        margin-bottom: var(--dt-space-8);
      }

      .dt-hero-stats {
        display: flex;
        align-items: center;
        gap: var(--dt-space-6);
        margin-bottom: var(--dt-space-10);
        padding: var(--dt-space-5) var(--dt-space-6);
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-hero-stat {
        display: flex;
        flex-direction: column;
        gap: 4px;
      }

      .dt-hero-stat-value {
        font-size: var(--dt-text-2xl);
        font-weight: 700;
        color: var(--dt-text);
        font-variant-numeric: tabular-nums;
      }

      .dt-hero-stat-label {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-tertiary);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .dt-hero-stat-divider {
        width: 1px;
        height: 40px;
        background: var(--dt-border);
      }

      .dt-hero-actions {
        display: flex;
        flex-wrap: wrap;
        gap: var(--dt-space-4);
        margin-bottom: var(--dt-space-10);
      }

      .dt-hero-trust {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-3);
      }

      .dt-hero-trust-label {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .dt-hero-trust-logos {
        display: flex;
        gap: var(--dt-space-4);
      }

      .dt-hero-trust-logos span {
        padding: var(--dt-space-2) var(--dt-space-3);
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-sm);
        font-size: var(--dt-text-xs);
        font-weight: 600;
        color: var(--dt-text-tertiary);
      }

      .dt-hero-visual {
        position: relative;
        height: 600px;
      }

      .dt-data-universe {
        position: absolute;
        inset: 0;
      }

      .dt-data-universe svg {
        width: 100%;
        height: 100%;
      }

      .dt-data-universe-overlay {
        position: absolute;
        inset: 0;
        background: radial-gradient(ellipse at center, transparent 40%, var(--dt-black) 100%);
        pointer-events: none;
      }

      .dt-connector-node {
        animation: dt-node-appear 0.6s var(--dt-ease) both;
      }

      @keyframes dt-node-appear {
        from { opacity: 0; transform: scale(0.5) translate(var(--x), var(--y)); }
        to { opacity: 1; transform: scale(1) translate(var(--x), var(--y)); }
      }

      .dt-node-pulse {
        animation: dt-node-pulse 3s ease-in-out infinite;
      }

      @keyframes dt-node-pulse {
        0%, 100% { r: 28; opacity: 0.3; }
        50% { r: 35; opacity: 0.1; }
      }

      .dt-data-line {
        stroke-dasharray: 8 4;
        animation: dt-data-flow 2s linear infinite;
      }

      @keyframes dt-data-flow {
        to { stroke-dashoffset: -24; }
      }

      @media (max-width: 1200px) {
        .dt-hero-content {
          grid-template-columns: 1fr;
          text-align: center;
        }

        .dt-hero-text {
          max-width: none;
        }

        .dt-hero-stats,
        .dt-hero-actions,
        .dt-hero-trust {
          justify-content: center;
        }

        .dt-hero-visual {
          height: 400px;
        }

        .dt-hero-title {
          font-size: var(--dt-text-4xl);
        }
      }
    `}</style>
  );
}
