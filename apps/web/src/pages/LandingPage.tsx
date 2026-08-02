import { useEffect, useState, type ReactNode } from "react";
import { DtIcon } from "../components/DtIcon";
import { ConnectorIcon } from "../app/brand-icons";
import {
  AlgorithmCinemaBand,
  MappingCinema,
  ProofCinema,
} from "../components/landing/AlgorithmCinema";
import { TrustSection } from "../components/landing/TrustSection";
import { TestimonialSection } from "../components/landing/TestimonialSection";
import { LandingHeroFlow } from "../components/landing/LandingHeroFlow";
import { fetchCatalogStats } from "../lib/api";
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
    body: "Connect source and destination, review semantic maps with confidence scores, pass eight preflight gates, then write with quarantine. The same path Pilot and MCP reuse — no silent shortcut.",
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
            Use Transfer Studio, Job Theater, Pipelines, or MCP. DataFlow manages mapping, gates,
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
                <li>G1–G8 preflight</li>
                <li>Quarantine + checksum</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

/** Home — Airbyte-class composition, DataFlow product truth. */
export function LandingHome({ onLogin: _onLogin, onGetStarted, onNavigate }: LandingHomeProps) {
  const [liveDrivers, setLiveDrivers] = useState<number | null>(null);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => {
        setLiveDrivers(s.unique_drivers ?? s.transfer_live ?? s.live);
      })
      .catch(() => setLiveDrivers(null));
  }, []);

  const driverLabel =
    liveDrivers != null ? `${liveDrivers} transfer-ready drivers` : "Transfer-ready drivers";

  return (
    <>
      {/* 1) Hero — short headline, one job, one visual */}
      <section className="lp-hero lp-hero--home">
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
              Connect once. Semantic mapping, eight preflight gates, quarantine, and checksum
              reconcile run on every load — in Transfer Studio, Pipelines, Pilot, and MCP.
            </p>
            <div className="lp-hero-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
                Try DataFlow free
                <DtIcon name="arrow-right" size={16} />
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("contact")}>
                Talk to sales
              </button>
            </div>
            <p className="lp-hero-meta">
              {driverLabel}
              <span aria-hidden>·</span>
              Catalog tiles labelled honestly
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
          <ol className="lp-home-pain-list">
            <li>
              <span className="lp-home-pain-num">01</span>
              <div>
                <h3>String-match mapping</h3>
                <p>
                  <code>order_amt</code> and <code>payment_amount</code> miss each other. Bad aliases
                  write garbage until someone notices in a dashboard.
                </p>
              </div>
            </li>
            <li>
              <span className="lp-home-pain-num">02</span>
              <div>
                <h3>Best-effort coercion</h3>
                <p>
                  Currency strings land in NUMERIC columns as nulls or truncated values — no
                  quarantine, no reason, no replay path.
                </p>
              </div>
            </li>
            <li>
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

      {/* 4) Architecture — numbered path */}
      <section className="lp-home-arch" id="platform">
        <div className="lp-home-arch-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">The architecture</p>
            <h2>One governed layer between source and destination.</h2>
            <p>
              DataFlow connects your systems, maps with roles and type fit, fails-fast before write,
              and proves every load — for humans and agents.
            </p>
          </Reveal>
          <ol className="lp-home-arch-steps">
            <li>
              <span>01</span>
              <strong>Map</strong>
              <p>Semantic roles, synonyms, and continuous confidence — not name equality.</p>
            </li>
            <li>
              <span>02</span>
              <strong>Preflight G1–G8</strong>
              <p>Fail-fast before write. Dry-run isolates coerce failures into quarantine.</p>
            </li>
            <li>
              <span>03</span>
              <strong>Write + prove</strong>
              <p>Quarantine surfaces bad rows. Checksum + counts prove the clean set.</p>
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

      {/* 5) Product surfaces — tabbed like Airbyte CLI/SDK/API/MCP */}
      <SurfaceTabs onNavigate={onNavigate} onGetStarted={onGetStarted} />

      {/* 6) One cinema — mapping as the product story */}
      <Reveal>
        <AlgorithmCinemaBand
          kicker="Semantic mapping"
          title="Every column earns a confidence score"
          lead="Format, role, and type compatibility outrank string similarity. Ambiguous edges wait for review before they pin into workspace synonyms."
        >
          <MappingCinema />
        </AlgorithmCinemaBand>
      </Reveal>

      {/* 7) Proof cinema — second dark band only */}
      <Reveal>
        <AlgorithmCinemaBand
          kicker="Runtime proof"
          title="Checksum reconcile flashes MATCH"
          lead="Success is never status alone. The engine hashes mapped source rows, reads the destination, and only then flashes MATCH — with quarantine counts surfaced."
          compact
        >
          <ProofCinema />
        </AlgorithmCinemaBand>
      </Reveal>

      {/* 8) Use cases */}
      <section className="lp-home-usecases" id="usecases">
        <div className="lp-home-usecases-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Use cases</p>
            <h2>What operators run</h2>
            <p>Migration, recurring sync, and warehouse loading — same eight gates every time.</p>
          </Reveal>
          <div className="lp-home-usecase-list">
            <article className="lp-home-usecase">
              <span>Migration</span>
              <h3>Cross-schema cutover with dual-run proof</h3>
              <p>Pilot a subset for checksum confidence, then cutover with quarantine visible.</p>
              <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("solution-migrations")}>
                Migration path →
              </button>
            </article>
            <article className="lp-home-usecase">
              <span>Sync</span>
              <h3>Recurring sync that still runs preflight</h3>
              <p>Every tick is a real job. Drift blocks the next run until you review.</p>
              <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("solution-sync")}>
                Sync path →
              </button>
            </article>
            <article className="lp-home-usecase">
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
            <h2>Hundreds of systems. Honest labels.</h2>
            <p>
              Catalog tiles are not the same as transfer-ready drivers. We publish both — and every
              production path still runs mapping, gates, quarantine, and proof.
              {liveDrivers != null ? ` ${liveDrivers} unique transfer-ready drivers today.` : ""}
            </p>
          </Reveal>
          <div className="lp-home-connectors-grid" aria-hidden>
            {STACK_IDS.map((id) => (
              <span key={id} className="lp-home-connectors-tile">
                <ConnectorIcon id={id} size={32} />
                <em>{id}</em>
              </span>
            ))}
          </div>
          <p className="lp-home-connectors-note">
            Only routes with <code>TRANSFER_READY</code> evidence get the transfer-ready badge. Everything
            else is labelled Planned.
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

      <TestimonialSection onNavigate={onNavigate} />
      <TrustSection />

      {/* 10) Final CTA — Airbyte-style close */}
      <section className="lp-home-final">
        <div className="lp-home-final-inner">
          <h2>Ship a governed transfer today</h2>
          <p>
            Start free on the same engine enterprises use for SSO, BYOK, and audit — semantic mapping,
            eight gates, quarantine, and checksum proof included.
          </p>
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Try DataFlow free
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
