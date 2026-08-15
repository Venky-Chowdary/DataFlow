import { useState, type ReactNode } from "react";
import { DtIcon } from "../components/DtIcon";
import { ConnectorIcon } from "../app/brand-icons";
import {
  MappingCinema,
  ProofCinema,
} from "../components/landing/AlgorithmCinema";
import { TrustSection } from "../components/landing/TrustSection";
import { ProofEvidenceSection } from "../components/landing/ProofEvidenceSection";
import { LandingHeroFlow } from "../components/landing/LandingHeroFlow";
import { ObservabilityInAction } from "../components/landing/ObservabilityInAction";
import { useRevealOnScroll } from "../hooks/useRevealOnScroll";
import type { PublicRoute } from "../lib/publicNavigation";

export interface LandingHomeProps {
  onLogin: () => void;
  onGetStarted: () => void;
  onNavigate: (route: PublicRoute) => void;
}

const STACK_IDS = [
  "postgresql",
  "snowflake",
  "bigquery",
  "redshift",
  "mongodb",
  "mysql",
  "kafka",
  "salesforce",
  "s3",
];

/** Wide marquee set — every branded icon we ship, plus stack staples. */
const CONNECTOR_MARQUEE_IDS = [
  "postgresql",
  "mysql",
  "mongodb",
  "snowflake",
  "bigquery",
  "redshift",
  "clickhouse",
  "dynamodb",
  "elasticsearch",
  "kafka",
  "redis",
  "s3",
  "salesforce",
  "csv",
  "json",
  "generic_sql",
  "postgresql",
  "snowflake",
  "bigquery",
  "mongodb",
  "kafka",
  "mysql",
  "redshift",
  "s3",
  "clickhouse",
  "elasticsearch",
  "dynamodb",
  "redis",
  "salesforce",
  "csv",
];

const CONNECTOR_MARQUEE_ROW_B = [
  "snowflake",
  "kafka",
  "postgresql",
  "bigquery",
  "redis",
  "mongodb",
  "s3",
  "mysql",
  "clickhouse",
  "salesforce",
  "redshift",
  "elasticsearch",
  "dynamodb",
  "json",
  "generic_sql",
  "csv",
  "snowflake",
  "postgresql",
  "kafka",
  "bigquery",
  "mongodb",
  "s3",
  "mysql",
  "redis",
  "clickhouse",
  "salesforce",
  "redshift",
  "elasticsearch",
];

function ConnectorMarquee({
  ids,
  reverse = false,
  duration = 42,
}: {
  ids: string[];
  reverse?: boolean;
  duration?: number;
}) {
  const loop = [...ids, ...ids];
  return (
    <div className={`lp-conn-marquee${reverse ? " is-reverse" : ""}`} aria-hidden>
      <div className="lp-conn-marquee-track" style={{ ["--lp-marquee-dur" as string]: `${duration}s` }}>
        {loop.map((id, i) => (
          <span key={`${id}-${i}`} className="lp-conn-marquee-tile">
            <ConnectorIcon id={id} size={30} />
            <em>{id.replace(/_/g, " ")}</em>
          </span>
        ))}
      </div>
    </div>
  );
}

const SURFACES: {
  id: string;
  label: string;
  title: string;
  body: string;
  route: PublicRoute;
  cta: string;
}[] = [
  {
    id: "studio",
    label: "Studio",
    title: "Transfer Studio plans every load",
    body: "Connect source and destination, review semantic maps with confidence scores, pass nine preflight gates, then write with quarantine. The same path Pilot and MCP reuse — no silent shortcut.",
    route: "product-transfer",
    cta: "Open Transfer Studio",
  },
  {
    id: "theater",
    label: "Theater",
    title: "Job Theater proves what ran",
    body: "Live phases, quarantine samples with reasons, and checksum reconcile reports. If a job finished, you can show the proof — not just a green status.",
    route: "product-jobs",
    cta: "See Job Theater",
  },
  {
    id: "pipelines",
    label: "Pipelines",
    title: "Schedules that still run gates",
    body: "Watermark incremental, upsert, append, or overwrite — every tick is a real job. Schema drift blocks the next run until you review the diff.",
    route: "product-pipelines",
    cta: "Explore Pipelines",
  },
  {
    id: "mcp",
    label: "MCP",
    title: "Agents inherit the same gates",
    body: "MCP tools never receive raw destination passwords. Agents map, preflight, and reconcile under workspace RBAC — the same engine as the UI.",
    route: "product-mcp",
    cta: "Read MCP docs",
  },
];

function Reveal({ children, className = "" }: { children: ReactNode; className?: string }) {
  const reveal = useRevealOnScroll();
  return (
    <div ref={reveal.ref} className={`${reveal.className} ${className}`.trim()}>
      {children}
    </div>
  );
}

function SurfaceTabs({ onNavigate, onGetStarted }: Pick<LandingHomeProps, "onNavigate" | "onGetStarted">) {
  const [active, setActive] = useState(SURFACES[0].id);
  const current = SURFACES.find((s) => s.id === active) ?? SURFACES[0];

  return (
    <section className="lp-home-surfaces" id="product" aria-label="Product surfaces">
      <div className="lp-home-surfaces-inner">
        <Reveal className="lp-home-section-head">
          <p className="lp-section-kicker">For operators &amp; agents</p>
          <h2>Your control plane. Our governed engine.</h2>
          <p>
            Use Transfer Studio, Job Theater, Pipelines, or MCP. Datawrap manages mapping, gates,
            quarantine, and proof underneath every surface.
          </p>
        </Reveal>

        <div className="lp-home-tabs" role="tablist" aria-label="Product surfaces">
          {SURFACES.map((s) => (
            <button
              key={s.id}
              type="button"
              role="tab"
              aria-selected={s.id === active}
              className={`lp-home-tab ${s.id === active ? "is-active" : ""}`}
              onClick={() => setActive(s.id)}
            >
              {s.label}
            </button>
          ))}
        </div>

        <div className="lp-home-tab-panel" role="tabpanel">
          <div className="lp-home-tab-copy">
            <h3>{current.title}</h3>
            <p>{current.body}</p>
            <div className="lp-home-tab-actions">
              <button
                type="button"
                className="lp-btn lp-btn--brand"
                onClick={() => (current.id === "studio" ? onGetStarted() : onNavigate(current.route))}
              >
                {current.cta}
              </button>
              <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate(current.route)}>
                Learn more →
              </button>
            </div>
          </div>
          <div className="lp-home-tab-visual" aria-hidden>
            <div className="lp-home-tab-card">
              <header>
                <span className="lp-home-tab-dot" />
                {current.label}
              </header>
              <strong>{current.title}</strong>
              <ul>
                <li>Semantic map</li>
                <li>G1–G9 preflight</li>
                <li>Quarantine + checksum</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

/** Home — Airbyte-class composition, Datawrap product truth. */
export function LandingHome({ onLogin: _onLogin, onGetStarted, onNavigate }: LandingHomeProps) {
  return (
    <>
      {/* 1) Hero — brand-first, one headline, one visual */}
      <section className="lp-hero lp-hero--home">
        <div className="lp-hero-home-bg" aria-hidden>
          <span className="lp-hero-home-mesh" />
          <span className="lp-hero-home-glow lp-hero-home-glow--a" />
          <span className="lp-hero-home-glow lp-hero-home-glow--b" />
          <svg className="lp-hero-home-waves" viewBox="0 0 1440 180" preserveAspectRatio="none">
            <path d="M0,90 C240,40 480,140 720,90 C960,40 1200,120 1440,70 L1440,180 L0,180 Z" />
            <path d="M0,120 C300,70 540,160 840,110 C1080,70 1260,130 1440,100 L1440,180 L0,180 Z" />
          </svg>
        </div>
        <div className="lp-hero-home-grid">
          <div className="lp-hero-copy">
            <p className="lp-hero-eyebrow">
              <span className="lp-hero-eyebrow-dot" aria-hidden />
              Universal data movement with proof
            </p>
            <h1 className="lp-hero-title">
              <span className="lp-hero-title-a">Move any schema</span>
              <span className="lp-hero-title-b">anywhere — proven.</span>
            </h1>
            <p className="lp-hero-sub">
              Semantic mapping, nine preflight gates, quarantine, and checksum reconcile on every
              load — Transfer Studio, Pipelines, Pilot, and MCP.
            </p>
            <div className="lp-hero-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
                Try Datawrap free
                <DtIcon name="arrow-right" size={16} />
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("contact")}>
                Talk to sales
              </button>
            </div>
            <p className="lp-hero-meta">
              Snowflake · BigQuery · S3 · PostgreSQL
              <span aria-hidden>·</span>
              0 silent drops by design
            </p>
          </div>
          <div className="lp-hero-visual lp-hero-visual--stage">
            <LandingHeroFlow />
          </div>
        </div>
      </section>

      {/* 2) Stack strip */}
      <section className="lp-home-stack" aria-label="Works with your stack">
        <p className="lp-home-stack-label">Works with the stacks you already run</p>
        <div className="lp-home-stack-row">
          {STACK_IDS.map((id) => (
            <span key={id} className="lp-home-stack-item" title={id}>
              <ConnectorIcon id={id} size={28} />
              <em>{id}</em>
            </span>
          ))}
        </div>
      </section>

      {/* 3) Problem — Airbyte-style pain list, our product framing */}
      <section className="lp-home-problem" id="why">
        <div className="lp-home-problem-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Why pipelines fail operators</p>
            <h2>Sync status is not proof.</h2>
            <p>
              Most ELT tools celebrate “job complete.” Operators still cannot answer: did every row
              land, did types coerce safely, and what was silently dropped?
            </p>
          </Reveal>
          <ol className="lp-home-pain-list lp-home-pain-rail">
            <li className="is-ink">
              <span className="lp-home-pain-num">01</span>
              <div>
                <h3>String-match mapping</h3>
                <p>
                  <code>order_amt</code> and <code>total_amount</code> miss each other on string
                  match. Blindly aliasing every <code>amt</code> onto <code>payment_amount</code>
                  writes the wrong money column.
                </p>
              </div>
            </li>
            <li className="is-soft">
              <span className="lp-home-pain-num">02</span>
              <div>
                <h3>Best-effort coercion</h3>
                <p>
                  Currency strings land in NUMERIC columns as nulls or truncated values — no
                  quarantine, no reason, no replay path.
                </p>
              </div>
            </li>
            <li className="is-outline">
              <span className="lp-home-pain-num">03</span>
              <div>
                <h3>No reconcile artifact</h3>
                <p>
                  Green status without row-count and checksum proof. Finance and compliance cannot
                  archive the load.
                </p>
              </div>
            </li>
          </ol>
        </div>
      </section>

      {/* 4) Architecture — governed path */}
      <section className="lp-home-arch" id="platform">
        <div className="lp-home-arch-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">The architecture</p>
            <h2>One governed layer between source and destination.</h2>
            <p>
              Datawrap connects your systems, maps with roles and type fit, fails-fast before write,
              and proves every load — for humans and agents.
            </p>
          </Reveal>

          <ol className="lp-home-arch-flow" aria-label="Governed transfer path">
            <li className="lp-home-arch-step">
              <div className="lp-home-arch-step-top">
                <span className="lp-home-arch-step-num">01</span>
                <span className="lp-home-arch-step-tag">Map</span>
              </div>
              <h3>Semantic mapping</h3>
              <p>
                Roles, synonyms, and qualifiers score every edge — not name equality.
                Same-role collisions wait for Map review. They do not auto-write.
              </p>
              <ul>
                <li>Role-aware column matching</li>
                <li>Confidence thresholds</li>
                <li>Human review on drift</li>
              </ul>
            </li>
            <li className="lp-home-arch-step lp-home-arch-step--connector" aria-hidden>
              <span className="lp-home-arch-arrow">→</span>
            </li>
            <li className="lp-home-arch-step">
              <div className="lp-home-arch-step-top">
                <span className="lp-home-arch-step-num">02</span>
                <span className="lp-home-arch-step-tag">Preflight</span>
              </div>
              <h3>Eight fail-fast gates</h3>
              <p>
                G1–G9 run before any write. Dry-run isolates coerce failures into quarantine with
                column, value, and reason — never silent drops.
              </p>
              <ul>
                <li>Schema &amp; type contracts</li>
                <li>Capacity probes</li>
                <li>Dry-run coerce samples</li>
              </ul>
            </li>
            <li className="lp-home-arch-step lp-home-arch-step--connector" aria-hidden>
              <span className="lp-home-arch-arrow">→</span>
            </li>
            <li className="lp-home-arch-step">
              <div className="lp-home-arch-step-top">
                <span className="lp-home-arch-step-num">03</span>
                <span className="lp-home-arch-step-tag">Prove</span>
              </div>
              <h3>Write with proof</h3>
              <p>
                Clean rows land; bad rows quarantine in the open. Checksum + row counts flash MATCH
                only when source and destination agree.
              </p>
              <ul>
                <li>Quarantine with reasons</li>
                <li>Checksum reconcile</li>
                <li>Exportable audit pack</li>
              </ul>
            </li>
          </ol>

          <div className="lp-home-arch-cta">
            <button type="button" className="lp-btn lp-btn--brand" onClick={onGetStarted}>
              Meet Transfer Studio
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("product-transfer")}>
              See the algorithms →
            </button>
          </div>
        </div>
      </section>

      <ObservabilityInAction />

      {/* 5) Product surfaces — tabbed like Airbyte CLI/SDK/API/MCP */}
      <SurfaceTabs onNavigate={onNavigate} onGetStarted={onGetStarted} />

      {/* 6) Mapping proof — clear chapters + live stage */}
      <section className="lp-home-proof-band" aria-label="Semantic mapping">
        <div className="lp-home-proof-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Semantic mapping</p>
            <h2>Every column earns a confidence score</h2>
            <p>
              Format, role, qualifier, and type fit outrank string similarity. Ambiguous edges wait
              for review before they pin. A 96% headline is not how Map works.
            </p>
          </Reveal>
          <div className="lp-home-proof-grid">
            <div className="lp-home-proof-chapters">
              <article>
                <span>01</span>
                <h3>Role-aware matching</h3>
                <p>
                  <code>order_amt</code> pairs with <code>total_amount</code> because the order
                  qualifier matches. <code>payment_amount</code> is a different column — both
                  being NUMERIC amounts is not identity.
                </p>
              </article>
              <article>
                <span>02</span>
                <h3>Continuous confidence</h3>
                <p>
                  Each edge gets a score from format, role, and type fit. Low scores never auto-pin.
                </p>
              </article>
              <article>
                <span>03</span>
                <h3>Human review gate</h3>
                <p>
                  Ambiguous maps pause for confirmation. Operators decide; the workspace remembers.
                </p>
              </article>
            </div>
            <div className="lp-home-proof-stage">
              <MappingCinema />
            </div>
          </div>
        </div>
      </section>

      {/* 7) Runtime proof */}
      <section className="lp-home-proof-band lp-home-proof-band--alt" aria-label="Runtime proof">
        <div className="lp-home-proof-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Runtime proof</p>
            <h2>Checksum reconcile flashes MATCH</h2>
            <p>
              Success is never status alone. The engine hashes mapped source rows, reads the
              destination, and only then flashes MATCH — with quarantine counts surfaced.
            </p>
          </Reveal>
          <div className="lp-home-proof-grid lp-home-proof-grid--reverse">
            <div className="lp-home-proof-chapters">
              <article>
                <span>01</span>
                <h3>Write clean rows</h3>
                <p>Only rows that clear coerce land in the destination table.</p>
              </article>
              <article>
                <span>02</span>
                <h3>Quarantine the rest</h3>
                <p>
                  Bad values keep column, sample, and reason — never a silent drop into “complete.”
                </p>
              </article>
              <article>
                <span>03</span>
                <h3>Prove with checksums</h3>
                <p>Row counts and content hashes must agree before MATCH is shown.</p>
              </article>
            </div>
            <div className="lp-home-proof-stage">
              <ProofCinema />
            </div>
          </div>
        </div>
      </section>

      {/* 8) Use cases */}
      <section className="lp-home-usecases" id="usecases">
        <div className="lp-home-usecases-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Use cases</p>
            <h2>What operators run</h2>
            <p>Migration, recurring sync, and warehouse loading — same nine core gates every time.</p>
          </Reveal>
          <div className="lp-home-usecase-list lp-home-usecase-bento">
            <article className="lp-home-usecase is-featured">
              <span>Migration</span>
              <h3>Cross-schema cutover with dual-run proof</h3>
              <p>Pilot a subset for checksum confidence, then cutover with quarantine visible.</p>
              <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("solution-migrations")}>
                Migration path →
              </button>
            </article>
            <article className="lp-home-usecase is-soft">
              <span>Sync</span>
              <h3>Recurring sync that still runs preflight</h3>
              <p>Every tick is a real job. Drift blocks the next run until you review.</p>
              <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("solution-sync")}>
                Sync path →
              </button>
            </article>
            <article className="lp-home-usecase is-outline">
              <span>Warehouse</span>
              <h3>Bulk loads finance can archive</h3>
              <p>Snowflake, BigQuery, Redshift with capacity probes and reconcile reports.</p>
              <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("solution-warehouse")}>
                Warehouse path →
              </button>
            </article>
          </div>
        </div>
      </section>

      {/* 9) Connectors honesty */}
      <section className="lp-home-connectors" id="tools">
        <div className="lp-home-connectors-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Connectors</p>
            <h2>Load Snowflake, BigQuery, and your lake</h2>
            <p>
              Native connectors for warehouses, object storage, databases, and apps. Map once,
              validate before write, quarantine bad rows, and keep a checksum — one catalog,
              one governed path.
            </p>
          </Reveal>
        </div>
        <div className="lp-home-connectors-marquee" aria-hidden>
          <ConnectorMarquee ids={CONNECTOR_MARQUEE_IDS} duration={48} />
          <ConnectorMarquee ids={CONNECTOR_MARQUEE_ROW_B} reverse duration={56} />
        </div>
        <div className="lp-home-connectors-inner">
          <p className="lp-home-connectors-note">
            Open the catalog to pick a source and destination — warehouses, lakes, databases, and apps.
          </p>
          <div className="lp-home-arch-cta">
            <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("integrations")}>
              Browse connectors
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("help")}>
              Driver docs
            </button>
          </div>
        </div>
      </section>

      <ProofEvidenceSection onNavigate={onNavigate} />
      <TrustSection />

      {/* 10) Final CTA — Airbyte-style close */}
      <section className="lp-home-final">
        <div className="lp-home-final-inner">
          <h2>Ship a governed transfer today</h2>
          <p>
            Start free on the same engine enterprises use for SSO, BYOK, and audit — semantic mapping,
            nine core gates, quarantine, and checksum proof included.
          </p>
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Try Datawrap free
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("pricing")}>
              See pricing
            </button>
            <button type="button" className="lp-btn lp-btn--ghost lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("enterprise")}>
              Enterprise
            </button>
          </div>
        </div>
      </section>
    </>
  );
}

/** @deprecated Prefer MarketingSite — kept for any direct imports. */
export function LandingPage(props: LandingHomeProps) {
  return <LandingHome {...props} />;
}
