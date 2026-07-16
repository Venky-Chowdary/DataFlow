import type { CSSProperties } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtLogo } from "../DtLogo";

const SOURCE_IDS = ["postgresql", "mysql", "mongodb", "amazon_s3"];
const DEST_IDS = ["snowflake", "bigquery", "redshift", "elasticsearch"];

const PIPELINE_STEPS = [
  { label: "Ingest", active: true },
  { label: "Map", active: true },
  { label: "Preflight", active: true },
  { label: "Load", active: false },
  { label: "Reconcile", active: false },
];

export function LandingHeroVisual() {
  return (
    <div className="lp-hero-stage">
      <div className="lp-hero-stage-glow" aria-hidden />
      <div className="lp-hero-stage-grid" aria-hidden />

      <div className="lp-flow-card lp-flow-card--premium">
        <div className="lp-flow-head">
          <div className="lp-flow-head-left">
            <span className="lp-flow-live-dot" />
            <strong>Live data plane</strong>
          </div>
          <span className="df2-badge df2-badge-live">Operational</span>
        </div>

        <div className="lp-flow-canvas-wrap">
          <svg className="lp-flow-svg" viewBox="0 0 400 140" preserveAspectRatio="xMidYMid meet" aria-hidden>
            <defs>
              <linearGradient id="lp-flow-grad" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="#14b8a6" />
                <stop offset="100%" stopColor="#0d9488" />
              </linearGradient>
            </defs>
            <path className="lp-flow-path lp-flow-path--a" d="M 72 70 C 120 70, 140 45, 200 70" />
            <path className="lp-flow-path lp-flow-path--b" d="M 72 90 C 120 90, 140 115, 200 90" />
            <path className="lp-flow-path lp-flow-path--c" d="M 200 70 C 260 70, 280 45, 328 70" />
            <path className="lp-flow-path lp-flow-path--d" d="M 200 90 C 260 90, 280 115, 328 90" />
            <circle className="lp-flow-packet lp-flow-packet--1" r="4" fill="url(#lp-flow-grad)">
              <animateMotion dur="2.4s" repeatCount="indefinite" path="M 72 70 C 120 70, 140 45, 200 70" />
            </circle>
            <circle className="lp-flow-packet lp-flow-packet--2" r="4" fill="#14b8a6">
              <animateMotion dur="2.8s" repeatCount="indefinite" begin="0.6s" path="M 72 90 C 120 90, 140 115, 200 90" />
            </circle>
            <circle className="lp-flow-packet lp-flow-packet--3" r="4" fill="#2dd4bf">
              <animateMotion dur="2.2s" repeatCount="indefinite" begin="1.2s" path="M 200 70 C 260 70, 280 45, 328 70" />
            </circle>
            <circle className="lp-flow-packet lp-flow-packet--4" r="3" fill="#5eead4">
              <animateMotion dur="2.6s" repeatCount="indefinite" begin="0.3s" path="M 200 90 C 260 90, 280 115, 328 90" />
            </circle>
          </svg>

          <div className="lp-flow-lane lp-flow-lane--sources">
            <span className="lp-flow-label">Sources</span>
            <ul className="lp-flow-orbit">
              {SOURCE_IDS.map((id, i) => (
                <li key={id} style={{ "--i": i } as CSSProperties}>
                  <ConnectorIcon id={id} size={22} />
                </li>
              ))}
            </ul>
          </div>

          <div className="lp-flow-hub-wrap">
            <span className="lp-flow-ring lp-flow-ring--1" />
            <span className="lp-flow-ring lp-flow-ring--2" />
            <div className="lp-flow-hub">
              <DtLogo size={30} />
              <span>DataFlow</span>
            </div>
          </div>

          <div className="lp-flow-lane lp-flow-lane--dests">
            <span className="lp-flow-label">Destinations</span>
            <ul className="lp-flow-orbit">
              {DEST_IDS.map((id, i) => (
                <li key={id} style={{ "--i": i } as CSSProperties}>
                  <ConnectorIcon id={id} size={22} />
                </li>
              ))}
            </ul>
          </div>
        </div>

        <div className="lp-pipeline-bar" role="list" aria-label="Pipeline progress">
          {PIPELINE_STEPS.map((step, i) => (
            <div
              key={step.label}
              className={`lp-pipeline-step ${step.active ? "active" : ""}`}
              style={{ "--step": i } as CSSProperties}
              role="listitem"
            >
              <span className="lp-pipeline-dot" />
              <span>{step.label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
