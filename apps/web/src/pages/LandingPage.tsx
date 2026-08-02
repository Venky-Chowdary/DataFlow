import { useEffect, useState, type ReactNode } from "react";
import { DtIcon } from "../components/DtIcon";
import { ConnectorIcon } from "../app/brand-icons";
import {
  AlgorithmCinemaBand,
  MappingCinema,
  ProductSurfaceStrip,
  ProofCinema,
} from "../components/landing/AlgorithmCinema";
import { ComparisonSection } from "../components/landing/ComparisonSection";
import { TrustSection } from "../components/landing/TrustSection";
import { TestimonialSection } from "../components/landing/TestimonialSection";
import { LandingHeroFlow } from "../components/landing/LandingHeroFlow";
import { LandingInfraRibbon } from "../components/landing/LandingInfraRibbon";
import { fetchCatalogStats } from "../lib/api";
import { useInView } from "../hooks/useInView";
import { useRevealOnScroll } from "../hooks/useRevealOnScroll";
import type { PublicRoute } from "../lib/publicNavigation";

export interface LandingHomeProps {
  onLogin: () => void;
  onGetStarted: () => void;
  onNavigate: (route: PublicRoute) => void;
}

const MARQUEE_IDS = [
  "postgresql", "snowflake", "mysql", "mongodb", "bigquery", "redshift",
  "s3", "json", "csv", "dynamodb", "elasticsearch", "redis",
  "salesforce", "kafka",
];

function Reveal({ children, className = "" }: { children: ReactNode; className?: string }) {
  const reveal = useRevealOnScroll();
  return (
    <div ref={reveal.ref} className={`${reveal.className} ${className}`.trim()}>
      {children}
    </div>
  );
}

function KnowledgeField() {
  const { ref, inView } = useInView<HTMLDivElement>("80px 0px");
  const [step, setStep] = useState(0);
  const items = [
    {
      label: "Use when",
      rule: "Source amount fields map to payment_amount",
      status: "pending" as const,
    },
    {
      label: "Approved mapping",
      rule: "order_amt → payment_amount (96% confidence)",
      status: "ok" as const,
    },
    {
      label: "Rejected alias",
      rule: "order_id should never map to email",
      status: "no" as const,
    },
  ];

  useEffect(() => {
    if (!inView) return;
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      setStep(1);
      return;
    }
    const id = window.setInterval(() => setStep((s) => (s + 1) % 3), 2800);
    return () => window.clearInterval(id);
  }, [inView]);

  const current = items[step];

  return (
    <div className="lp-knowledge-field" ref={ref} key={step}>
      <span className="lp-knowledge-field-label">{current.label}</span>
      <p className="lp-knowledge-field-rule">{current.rule}</p>
      <div className="lp-knowledge-field-actions">
        {current.status === "pending" ? (
          <>
            <button type="button" className="lp-knowledge-btn lp-knowledge-btn--ok">Accept</button>
            <button type="button" className="lp-knowledge-btn lp-knowledge-btn--no">Reject</button>
          </>
        ) : current.status === "ok" ? (
          <span className="lp-knowledge-tag is-ok">Accepted into synonym dictionary</span>
        ) : (
          <span className="lp-knowledge-tag is-no">Blocked from future auto-maps</span>
        )}
      </div>
    </div>
  );
}

function ConnectorMarqueeBand() {
  const { ref, inView } = useInView<HTMLDivElement>("60px 0px");
  const track = [...MARQUEE_IDS, ...MARQUEE_IDS];
  return (
    <div className={`lp-marquee ${inView ? "is-running" : ""}`} ref={ref} aria-hidden>
      <div className="lp-marquee-track">
        {track.map((id, i) => (
          <span key={`${id}-${i}`} className="lp-marquee-item">
            <ConnectorIcon id={id} size={28} />
          </span>
        ))}
      </div>
      <div className="lp-marquee-track lp-marquee-track--reverse">
        {track.map((id, i) => (
          <span key={`b-${id}-${i}`} className="lp-marquee-item">
            <ConnectorIcon id={id} size={28} />
          </span>
        ))}
      </div>
    </div>
  );
}

function OutcomesBand({
  uniqueDrivers,
  catalogTiles: _catalogTiles,
}: {
  uniqueDrivers: number | null;
  catalogTiles: number | null;
}) {
  const driverLabel = uniqueDrivers != null ? `${uniqueDrivers} transfer-ready drivers` : "Transfer-ready drivers";

  return (
    <section className="lp-outcomes-rail" id="outcomes" aria-label="Outcomes">
      <Reveal>
        <div className="lp-outcomes-rail-inner">
          <p className="lp-outcomes-rail-eyebrow">Enforced on every run</p>
          <ul className="lp-outcomes-rail-list">
            <li><strong>8</strong><span>preflight gates before write</span></li>
            <li><strong>{driverLabel}</strong><span>unique drivers, not alias inflation</span></li>
            <li><strong>Every job</strong><span>row-count + checksum reconcile</span></li>
            <li><strong>0</strong><span>silently dropped rows — quarantine surfaces them</span></li>
          </ul>
        </div>
      </Reveal>
    </section>
  );
}

/** Home marketing body — chrome (nav/footer) lives in MarketingChrome. */
export function LandingHome({ onLogin: _onLogin, onGetStarted, onNavigate }: LandingHomeProps) {
  const [liveDrivers, setLiveDrivers] = useState<number | null>(null);
  const [catalogTiles, setCatalogTiles] = useState<number | null>(null);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => {
        setLiveDrivers(s.unique_drivers ?? s.transfer_live ?? s.live);
        setCatalogTiles(s.catalog_tiles ?? s.transfer_live_tiles ?? null);
      })
      .catch(() => {
        setLiveDrivers(null);
        setCatalogTiles(null);
      });
  }, []);

  return (
    <>
      <section className="lp-hero lp-hero--immersive lp-hero--bleed">
        <div className="lp-hero-immersive-grid">
          <div className="lp-hero-copy">
            <span className="lp-hero-eyebrow">
              <span className="lp-hero-eyebrow-dot" aria-hidden />
              Universal data movement, proven end-to-end
            </span>
            <h1 className="lp-hero-title">
              <span className="lp-hero-title-line">Move any schema</span>
              <span className="lp-hero-title-b lp-hero-title-line">anywhere.</span>
            </h1>
            <p className="lp-hero-sub">
              Semantic mapping earns a confidence score on every column — no string-match guessing.
              Eight preflight gates fail-fast before write, quarantine isolates bad rows with reasons,
              and checksum reconciliation proves every load. Transfer Studio, Pipelines, Pilot, and MCP
              share one governed engine — never a silent shortcut.
              {liveDrivers != null ? ` ${liveDrivers} unique transfer-ready drivers today.` : ""}
            </p>

            <div className="lp-hero-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
                Try DataFlow
                <DtIcon name="arrow-right" size={16} />
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("help")}>
                Read the docs
              </button>
            </div>

            <ul className="lp-hero-proof-line" aria-label="Platform highlights">
              <li>8 preflight gates</li>
              <li>Checksum proof</li>
              <li>{liveDrivers != null ? `${liveDrivers} unique drivers` : "Transfer-ready drivers"}</li>
            </ul>
          </div>

          <div className="lp-hero-visual lp-hero-visual--stage">
            <LandingHeroFlow />
          </div>
        </div>
      </section>

      <section className="lp-logos lp-logos--float lp-band--center" aria-label="Trusted stacks">
        <h5>Works with the stacks you already run</h5>
        <div className="lp-logos-float-row">
          {[
            "postgresql",
            "snowflake",
            "bigquery",
            "redshift",
            "mongodb",
            "mysql",
            "kafka",
            "salesforce",
            "s3",
          ].map((id) => (
            <span key={id} className="lp-logo-float" title={id}>
              <ConnectorIcon id={id} size={40} />
              <em>{id}</em>
            </span>
          ))}
        </div>
      </section>

      <OutcomesBand uniqueDrivers={liveDrivers} catalogTiles={catalogTiles} />

      <section className="lp-section lp-section-platform lp-band--full" id="platform">
        <Reveal>
          <div className="lp-section-head lp-band-copy--center">
            <p className="lp-section-kicker">Platform</p>
            <h2>From source to proof in four steps</h2>
            <p>The same governed path in Transfer Studio, Data Pilot, MCP, and scheduled pipelines.</p>
          </div>
        </Reveal>
        <LandingInfraRibbon />
      </section>

      <section className="lp-surface-strip-band" id="product">
        <div className="lp-surface-strip-inner">
          <Reveal className="lp-surface-strip-copy">
            <p className="lp-section-kicker">Product</p>
            <h2>Every surface. One governed engine.</h2>
            <p>Transfer Studio plans the load; Job Theater proves it. Pipelines, Query, Pilot, and MCP reuse the same path — pick a surface to see the chapter.</p>
          </Reveal>
          <Reveal>
            <ProductSurfaceStrip<PublicRoute> onNavigate={onNavigate} />
          </Reveal>
        </div>
      </section>

      <Reveal>
        <AlgorithmCinemaBand
          kicker="Mapping"
          title="Semantic mapping — roles, synonyms, type fit"
          lead="Watch every source column earn a continuous confidence score. Format, role, and type compatibility outrank string similarity — and ambiguous edges wait for review before they can pin into workspace synonyms."
        >
          <MappingCinema />
        </AlgorithmCinemaBand>
      </Reveal>

      <Reveal>
        <AlgorithmCinemaBand
          kicker="Proof"
          title="Checksum + row-count reconcile flashes MATCH"
          lead="Success is never “status = complete.” The engine hashes mapped source rows, reads the destination sample, compares, and only then flashes MATCH — with quarantine counts surfaced alongside so nothing is silently dropped."
        >
          <ProofCinema />
        </AlgorithmCinemaBand>
      </Reveal>

      <section className="lp-story-band" id="usecases">
        <div className="lp-story-band-inner">
          <Reveal className="lp-story-band-head">
            <h2>What operators run</h2>
            <p>Three story arcs — migration, recurring sync, warehouse loading — each with the same eight gates and reconciliation guarantees.</p>
          </Reveal>

          <Reveal>
            <article className="lp-story-row">
              <span className="lp-story-row-tag">Migration</span>
              <div className="lp-story-row-copy">
                <h3>Cross-schema cutover with dual-run proof</h3>
                <p>Profile both sides, propose role-aware maps, pilot a subset for checksum confidence, then cutover with quarantine keeping bad rows visible instead of dropped.</p>
              </div>
              <div className="lp-story-row-actions">
                <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("solution-migrations")}>
                  Read the migration path →
                </button>
              </div>
            </article>
            <article className="lp-story-row">
              <span className="lp-story-row-tag">Sync</span>
              <div className="lp-story-row-copy">
                <h3>Recurring sync that still runs preflight</h3>
                <p>Every tick is a real job in Job Theater — watermark incremental, upsert, append, or overwrite — with schema drift blocking the next tick until you review the diff.</p>
              </div>
              <div className="lp-story-row-actions">
                <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("solution-sync")}>
                  Read the sync path →
                </button>
              </div>
            </article>
            <article className="lp-story-row">
              <span className="lp-story-row-tag">Warehouse</span>
              <div className="lp-story-row-copy">
                <h3>Bulk warehouse loads finance can archive</h3>
                <p>Snowflake, BigQuery, and Redshift with destination probes, capacity checks, and reconciliation reports — the numbers analytics and finance teams need to sign off.</p>
              </div>
              <div className="lp-story-row-actions">
                <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("solution-warehouse")}>
                  Read the warehouse path →
                </button>
              </div>
            </article>
          </Reveal>
        </div>
      </section>

      <ComparisonSection />
      <TestimonialSection onNavigate={onNavigate} />

      <Reveal>
        <AlgorithmCinemaBand
          kicker="Together"
          title="Learns your schemas and mapping corrections"
          lead="Every accepted or rejected mapping updates the workspace synonym dictionary. Data Pilot can propose additions from failed jobs — humans still confirm, gates still run."
        >
          <div className="lp-cinema-stage" aria-label="Knowledge field animation">
            <KnowledgeField />
          </div>
        </AlgorithmCinemaBand>
      </Reveal>

      <section className="lp-connectors-callout" id="tools">
        <div className="lp-connectors-callout-inner">
          <Reveal className="lp-connectors-callout-copy">
            <p className="lp-section-kicker">Connectors</p>
            <h2>Hundreds of systems — with honest labels</h2>
            <p>
              Native transfer drivers plus SQLAlchemy generics and file formats (CSV, JSON, Parquet). We
              publish two counts: catalog tiles you can browse, and unique <strong>transfer-ready</strong>
              drivers with production evidence.
            </p>
          </Reveal>
          <Reveal>
            <ConnectorMarqueeBand />
          </Reveal>
          <Reveal>
            <p className="lp-connectors-callout-honesty">
              Catalog count ≠ transfer-live. Only routes that carry <code>TRANSFER_READY</code>
              &nbsp;evidence get the transfer-ready badge — every other tile is labelled Planned so
              operators know exactly what they can run today.
            </p>
          </Reveal>
          <Reveal className="lp-connectors-callout-actions">
            <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("integrations")}>
              Browse the connector catalog
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("help")}>
              Read the driver docs
            </button>
          </Reveal>
        </div>
      </section>

      <TrustSection />

      <section className="lp-section" id="enterprise">
        <div className="lp-enterprise">
          <div>
            <h3>Need DataFlow for your enterprise?</h3>
            <p>
              DataFlow Enterprise adds workspace RBAC, SSO, audit trails, tenant controls, and the
              same governed transfer engine your team already trusts.
            </p>
          </div>
          <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("enterprise")}>
            Learn about DataFlow Enterprise
          </button>
        </div>
      </section>

    </>
  );
}

/** @deprecated Prefer MarketingSite — kept for any direct imports. */
export function LandingPage(props: LandingHomeProps) {
  return <LandingHome {...props} />;
}
