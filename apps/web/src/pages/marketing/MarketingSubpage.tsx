import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type ChangeEvent, type KeyboardEvent } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { MarketingFaq } from "../../components/marketing/MarketingFaq";
import { DesignPartners } from "../../components/marketing/DesignPartners";
import { QuarantinePanel, ROLES, StatusGlyph } from "../../components/marketing/ProductPanels";
import { GuillocheSeal } from "../../components/marketing/StatementHero";
import { JobsWithTrust } from "../../components/marketing/app-frame/AppFrame";
import {
  ConnectorWizardShowcase,
  ProofsShowcase,
  SettingsShowcase,
  SignOffShowcase,
} from "./MarketingSubpageScenes";
import { isHelpDocRoute } from "../../lib/helpDocs";
import {
  CONNECTOR_CLAIMS,
  EVIDENCE_AS_OF,
  EVIDENCE_RUN_ID,
  ALL_CONNECTORS,
  CATALOG_TOTAL,
  EMULATED_DRIVERS,
  MARKETING_STACK,
  PROOF_GROUPS,
  PROVEN_DRIVERS,
  proofClaim,
  cdcSourcesPhrase,
  provenDriversPhrase,
  type CatalogConnector,
  type ProofGroup,
} from "../../lib/provenEvidence";
import type { PublicRoute } from "../../lib/publicNavigation";
import { DocArticlePage, DocsPortal } from "./DocsPortal";
import { ContactSalesPage } from "./ContactSalesPage";
import { DemosPage } from "./DemosPage";
import { LegalPage } from "./LegalPages";
import { StatusPage, SubprocessorsPage } from "./TrustPages";
import {
  DataPilotPage,
  JobTheaterPage,
  McpServerPage,
  MigrationsSolutionPage,
  PipelinesPage,
  QueryPlaygroundPage,
  SyncSolutionPage,
  TransferStudioPage,
  WarehouseSolutionPage,
} from "./StoryPages";

interface PageActions {
  onGetStarted: () => void;
  onLogin: () => void;
  onNavigate: (route: PublicRoute) => void;
}

type Nav = Pick<PageActions, "onGetStarted" | "onNavigate">;

export function MarketingSubpage({ route, onGetStarted, onNavigate }: { route: PublicRoute } & PageActions) {
  if (route === "home") return null;
  if (isHelpDocRoute(route)) {
    return <DocArticlePage docId={route} onNavigate={onNavigate} onGetStarted={onGetStarted} />;
  }

  switch (route) {
    case "pricing":
      return <PricingPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "enterprise":
      return <EnterprisePage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "customers":
      return <ProofPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "contact":
      return <ContactSalesPage onNavigate={onNavigate} />;
    case "privacy":
      return <LegalPage kind="privacy" />;
    case "terms":
      return <LegalPage kind="terms" />;
    case "dpa":
      return <LegalPage kind="dpa" />;
    case "subprocessors":
      return <SubprocessorsPage />;
    case "status":
      return <StatusPage />;
    case "security":
      return <SecurityPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "help":
      return <DocsPortal onNavigate={onNavigate} onGetStarted={onGetStarted} />;
    case "product-transfer":
      return <TransferStudioPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-jobs":
      return <JobTheaterPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-pipelines":
      return <PipelinesPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-query":
      return <QueryPlaygroundPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-pilot":
      return <DataPilotPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-mcp":
      return <McpServerPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "integrations":
      return <IntegrationsPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "demos":
      return <DemosPage onNavigate={onNavigate} />;
    case "solution-migrations":
      return <MigrationsSolutionPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "solution-warehouse":
      return <WarehouseSolutionPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "solution-sync":
      return <SyncSolutionPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    default:
      return null;
  }
}

/* ------------------------------------------------------------------ Shared */

type Action = { label: string; onClick: () => void };

/** The page's two actions: one primary, one secondary. */
function Actions({ primary, secondary }: { primary: Action; secondary: Action }) {
  return (
    <div className="mk-actions">
      <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={primary.onClick}>
        {primary.label}
      </button>
      <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={secondary.onClick}>
        {secondary.label}
      </button>
    </div>
  );
}

function Closing({ title, body, primary, secondary }: { title: string; body: string; primary: Action; secondary: Action }) {
  return (
    <MarketingClosing title={title} body={body}>
      <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={primary.onClick}>
        {primary.label}
      </button>
      <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={secondary.onClick}>
        {secondary.label}
      </button>
    </MarketingClosing>
  );
}

function prefersReducedMotion(): boolean {
  try {
    return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  } catch {
    return true;
  }
}

const fmt = new Intl.NumberFormat("en-US");

/** A proven driver's label. Emulator-backed proof says so, in words. */
function proofLabel(emulated: boolean): string {
  return emulated ? "Proven in CI on an emulator" : "Proven in CI";
}

type GlyphKind = "plan" | "checks" | "quarantine" | "readback" | "key" | "rows" | "chain";

/** Small engraved glyphs: line work only, drawn in the current ink. */
function Glyph({ kind }: { kind: GlyphKind }) {
  return (
    <svg className={`mk-a-glyph mk-a-glyph--${kind}`} viewBox="0 0 40 40" aria-hidden focusable="false">
      {kind === "plan" ? (
        <>
          <path d="M11 6h13l6 6v22H11z" />
          <path d="M24 6v6h6M15 18h11M15 23h11M15 28h7" />
        </>
      ) : null}
      {kind === "checks" ? (
        <>
          <rect x="7" y="7" width="26" height="26" rx="3" />
          <path d="M13 20.5l5 5 9-11" />
        </>
      ) : null}
      {kind === "quarantine" ? (
        <>
          <path d="M12 7H8v26h4M28 7h4v26h-4" />
          <path d="M17 14v12M23 14v12" />
        </>
      ) : null}
      {kind === "readback" ? (
        <>
          <path d="M31 17a11.5 11.5 0 0 0-21-4M9 23a11.5 11.5 0 0 0 21 4" />
          <path d="M10 6v7h7M30 34v-7h-7" />
        </>
      ) : null}
      {kind === "key" ? (
        <>
          <circle cx="14" cy="20" r="7" />
          <path d="M21 20h13M29 20v5M33 20v4" />
        </>
      ) : null}
      {kind === "rows" ? (
        <>
          <path d="M6 12h22M6 20h28M6 28h18" />
          <path d="M28 8l5 4-5 4" />
        </>
      ) : null}
      {kind === "chain" ? (
        <>
          <rect x="4" y="14" width="12" height="12" rx="2" />
          <rect x="24" y="14" width="12" height="12" rx="2" />
          <path d="M16 20h8" />
        </>
      ) : null}
    </svg>
  );
}

/* ------------------------------------------------------------------ Pricing */

type Tier = "starter" | "team" | "enterprise";
const TIERS: Tier[] = ["starter", "team", "enterprise"];

const PLAN_ROWS: { feature: string; in: Tier[] }[] = [
  { feature: "Transfer Studio: tables, destination queries, stored procedures", in: TIERS },
  { feature: "A plan you review before anything is written", in: TIERS },
  { feature: "Checks on the real write path, before the write", in: TIERS },
  { feature: "Quarantine with the reason, and replay", in: TIERS },
  { feature: "Read-back of the destination, counted by its own engine", in: TIERS },
  { feature: "Signed proof pack for every run", in: TIERS },
  { feature: "Schedules with typed watermarks", in: ["team", "enterprise"] },
  { feature: `Change data capture from ${cdcSourcesPhrase()}`, in: ["team", "enterprise"] },
  { feature: "Datawrap Pilot", in: ["team", "enterprise"] },
  { feature: "Shared workspace with four roles", in: ["team", "enterprise"] },
  { feature: "Single sign-on (OIDC or SAML) and SCIM", in: ["enterprise"] },
  { feature: "Connector secrets wrapped with your own key", in: ["enterprise"] },
  { feature: "Data-region fence on the workspace", in: ["enterprise"] },
  { feature: "Tamper-evident audit log and export", in: ["enterprise"] },
  { feature: "MCP server for agents", in: ["enterprise"] },
  { feature: "Self-managed deployment in your cloud or VPC", in: ["enterprise"] },
];

/** The comparison's three row groups, by how many plans carry the row. */
const PLAN_GROUPS: { id: string; title: string; rows: typeof PLAN_ROWS }[] = [
  { id: "all", title: "The load path, in every plan", rows: PLAN_ROWS.filter((r) => r.in.length === 3) },
  { id: "team", title: "Added in Team", rows: PLAN_ROWS.filter((r) => r.in.length === 2) },
  { id: "enterprise", title: "Added in Enterprise", rows: PLAN_ROWS.filter((r) => r.in.length === 1) },
];

const PLANS: {
  tier: Tier;
  name: string;
  figure: string;
  qualifier: string;
  who: string;
  adds: string;
  highlights: string[];
}[] = [
  {
    tier: "starter",
    name: "Starter",
    figure: "Free",
    qualifier: "while in early access",
    who: "For one person or a small team moving data they need to trust.",
    adds: "The whole load path",
    highlights: [
      "Plan, checks, quarantine, and read-back on every run",
      "A signed proof pack you can verify",
      `${provenDriversPhrase()}, more implemented`,
    ],
  },
  {
    tier: "team",
    name: "Team",
    figure: "Custom",
    qualifier: "pricing",
    who: "For a data team that runs loads on a schedule.",
    adds: "Everything in Starter, plus",
    highlights: ["Schedules and change data capture", "Datawrap Pilot", "A shared workspace with four roles"],
  },
  {
    tier: "enterprise",
    name: "Enterprise",
    figure: "Custom",
    qualifier: "pricing",
    who: "For organisations with identity, key, and residency requirements.",
    adds: "Everything in Team, plus",
    highlights: [
      "SSO, SCIM, and your own key on connector secrets",
      "A data-region fence and a tamper-evident audit log",
      "MCP server for agents",
      "Self-managed deployment in your cloud or VPC",
    ],
  },
];

/** The statement path: what every plan runs, in order. */
const EVERY_PLAN: { name: string; does: string; glyph: GlyphKind | null }[] = [
  { name: "Plan", does: "You review it before anything is written.", glyph: "plan" },
  { name: "Checks", does: "Sample rows go through the real write path first.", glyph: "checks" },
  { name: "Quarantine", does: "A refused row is held with its reason, then replayed.", glyph: "quarantine" },
  { name: "Read-back", does: "The destination is counted by its own engine.", glyph: "readback" },
  { name: "Proof", does: "A signed proof pack, for every run.", glyph: null },
];

function PricingPage({ onGetStarted, onNavigate }: Nav) {
  const start = { label: "Start free", onClick: onGetStarted };
  const sales = { label: "Contact sales", onClick: () => onNavigate("contact") };
  return (
    <div className="mk-page mk-pr">
      <section className="mk-pr-hero" aria-labelledby="mk-pr-title">
        <div className="mk-wrap">
          <header className="mk-pr-intro">
            <h1 id="mk-pr-title">Start free. Talk to us when a team depends on it.</h1>
            <p>Every plan runs the same engine. Verification is never an add-on.</p>
          </header>

          <ol className="mk-pr-sheet" aria-label="Plans">
            {PLANS.map((plan) => (
              <li key={plan.tier} className={`mk-pr-plan mk-pr-plan--${plan.tier}`}>
                <div className="mk-pr-plan-head">
                  <h2>{plan.name}</h2>
                  <p className="mk-pr-price">
                    <span className="mk-pr-figure">{plan.figure}</span> <span className="mk-pr-qual">{plan.qualifier}</span>
                  </p>
                  <p className="mk-pr-who">{plan.who}</p>
                </div>
                <button
                  type="button"
                  className={`mk-btn mk-btn--lg mk-pr-cta ${plan.tier === "starter" ? "mk-btn--primary" : "mk-btn--secondary"}`}
                  onClick={plan.tier === "starter" ? start.onClick : sales.onClick}
                >
                  {plan.tier === "starter" ? start.label : sales.label}
                  <span className="mk-sr-only"> for {plan.name}</span>
                </button>
                <p className="mk-pr-adds">{plan.adds}</p>
                <ul className="mk-pr-list">
                  {plan.highlights.map((h) => (
                    <li key={h}>
                      <StatusGlyph kind="landed" />
                      {h}
                    </li>
                  ))}
                </ul>
              </li>
            ))}
          </ol>
        </div>
      </section>

      <section className="mk-pr-path" aria-labelledby="mk-pr-path-title">
        <div className="mk-wrap">
          <header className="mk-pr-path-head">
            <h2 id="mk-pr-path-title">Every plan includes the whole path</h2>
            <p>Starter, Team, and Enterprise run the same five steps on every load.</p>
          </header>
          <ol className="mk-pr-rail">
            {EVERY_PLAN.map((s) => (
              <li key={s.name} className="mk-pr-stop">
                <span className="mk-pr-node" aria-hidden>
                  {s.glyph ? <Glyph kind={s.glyph} /> : <GuillocheSeal drawn />}
                </span>
                <h3>{s.name}</h3>
                <p>{s.does}</p>
              </li>
            ))}
          </ol>
          <div className="mk-pr-frame">
            <h3 className="mk-pr-frame-title">What Starter runs, in the product</h3>
            <JobsWithTrust caption="Example workspace. Choose a run to open its Trust Report." />
          </div>
        </div>
      </section>

      <section className="mk-section mk-pr-compare" aria-labelledby="mk-compare-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-compare-title">Compare the plans</h2>
          </header>
          <table className="mk-pr-table">
            <caption className="mk-sr-only">What each plan includes</caption>
            <colgroup>
              <col className="mk-pr-col-feature" />
              <col className="mk-pr-col-starter" />
              <col />
              <col />
            </colgroup>
            <thead>
              <tr>
                <th scope="col">
                  <span className="mk-sr-only">Capability</span>
                </th>
                {PLANS.map((p) => (
                  <th key={p.tier} scope="col" className={`mk-pr-th mk-pr-th--${p.tier}`}>
                    <span className="mk-pr-th-name">{p.name}</span>
                    <span className="mk-pr-th-price">
                      {p.figure} {p.qualifier}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            {PLAN_GROUPS.map((g) => (
              <tbody key={g.id}>
                <tr className="mk-pr-group">
                  <th scope="rowgroup" colSpan={4}>
                    {g.title}
                  </th>
                </tr>
                {g.rows.map((row) => (
                  <tr key={row.feature}>
                    <th scope="row">{row.feature}</th>
                    {TIERS.map((t) => (
                      <td key={t} className={t === "starter" ? "is-starter" : undefined}>
                        {row.in.includes(t) ? (
                          <span className="mk-pr-yes">
                            <StatusGlyph kind="landed" />
                            <span className="mk-pr-yes-label">Included</span>
                          </span>
                        ) : (
                          <span className="mk-pr-no">
                            <span aria-hidden>–</span>
                            <span className="mk-sr-only">Not included</span>
                          </span>
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            ))}
          </table>
        </div>
      </section>

      <section className="mk-section mk-section--band mk-pr-where" aria-labelledby="mk-pricing-deploy">
        <div className="mk-wrap mk-pr-where-grid">
          <h2 id="mk-pricing-deploy">Where it runs</h2>
          <div className="mk-pr-stub">
            <h3>Hosted</h3>
            <p className="mk-pr-stub-plan">Every plan. The default.</p>
            <p>We run Datawrap for you. Connect your sources and destinations; there is nothing to install.</p>
          </div>
          <div className="mk-pr-stub">
            <h3>Self-managed</h3>
            <p className="mk-pr-stub-plan">An Enterprise plan option</p>
            <p>
              Two images, the API and the web app, in your cloud or VPC with Docker Compose or Helm.{" "}
              <a
                className="mk-link"
                href="#/enterprise"
                onClick={(e) => {
                  e.preventDefault();
                  onNavigate("enterprise");
                }}
              >
                See where data, keys, and audit live
              </a>
            </p>
          </div>
        </div>
      </section>

      <section className="mk-section" aria-labelledby="mk-pricing-faq">
        <div className="mk-wrap mk-pr-faq">
          <h2 id="mk-pricing-faq">Questions about pricing</h2>
          <MarketingFaq
            items={[
              {
                q: "Why is there no price for Team or Enterprise?",
                a: "Datawrap is in early access, and team plans are agreed with each customer. Tell us what you move and how often, and we will quote it.",
              },
              {
                q: "Does Starter have usage limits?",
                a: "No usage limits are enforced while Starter is free in early access.",
              },
              {
                q: "Is verification part of every plan?",
                a: "Yes. The checks before the write, quarantine with replay, and the read-back after it are how Datawrap moves data, so every plan has them.",
              },
              {
                q: "Can we run Datawrap in our own cloud?",
                a: "Yes, on the Enterprise plan: self-managed deployment in your cloud or VPC, with Docker Compose or Helm. Hosted is the default.",
              },
            ]}
          />
        </div>
      </section>

      <Closing
        title="Run your first load today"
        body="Starter is free while Datawrap is in early access. Team plans start with a conversation."
        primary={start}
        secondary={sales}
      />
    </div>
  );
}

/* --------------------------------------------------------------- Enterprise */

type Lives = { kind: GlyphKind; name: string; where: string };

const ESTATES: {
  id: string;
  name: string;
  plan: string;
  runs: string;
  yoursRuns: boolean;
  nodes: string[];
  install: string;
  lives: Lives[];
  systems: string[];
}[] = [
  {
    id: "hosted",
    name: "Hosted",
    plan: "The default, on every plan",
    runs: "Datawrap's account",
    yoursRuns: false,
    nodes: ["Web app", "API and engine"],
    install: "Nothing to install",
    lives: [
      { kind: "rows", name: "Data", where: "Rows pass through the engine; held rows stay in the job's quarantine" },
      { kind: "key", name: "Keys", where: "Connector secrets encrypted at rest, wrapped with your KMS key if you bring one" },
      { kind: "chain", name: "Audit", where: "Signed entries per workspace, each linked to the one before" },
    ],
    systems: ["Your source", "Your destination", "Your AWS KMS key"],
  },
  {
    id: "self",
    name: "Self-managed",
    plan: "An Enterprise plan option",
    runs: "Your cloud or VPC",
    yoursRuns: true,
    nodes: ["Web app image", "API image"],
    install: "Docker Compose or Helm",
    lives: [
      { kind: "rows", name: "Data", where: "Jobs, schedules, and evidence stay in your account, in your own MongoDB" },
      { kind: "key", name: "Keys", where: "Connector credentials stay in your account" },
      { kind: "chain", name: "Audit", where: "Audit events live in your own MongoDB" },
    ],
    systems: ["Your source", "Your destination"],
  },
];

/** Hosted and self-managed, drawn side by side: where data, keys, and audit live. */
function DeploymentPlates() {
  return (
    <figure className="mk-ent-plates" aria-labelledby="mk-ent-plates-cap">
      <figcaption id="mk-ent-plates-cap" className="mk-ent-plates-cap">
        Where data, keys, and audit live
      </figcaption>
      <div className="mk-ent-plates-grid">
        {ESTATES.map((e, i) => (
          <article key={e.id} className={`mk-ent-plate mk-ent-plate--${e.id}`} aria-labelledby={`mk-ent-plate-${e.id}`}>
            <header className="mk-ent-plate-head">
              <h2 id={`mk-ent-plate-${e.id}`}>{e.name}</h2>
              <p>{e.plan}</p>
            </header>
            <div className={`mk-ent-zone ${e.yoursRuns ? "is-yours" : "is-ours"}`}>
              <p className="mk-ent-zone-label">{e.runs}</p>
              <ul className="mk-ent-nodes">
                {e.nodes.map((n) => (
                  <li key={n}>{n}</li>
                ))}
              </ul>
              <p className="mk-ent-install">{e.install}</p>
              <dl className="mk-ent-lives">
                {e.lives.map((l) => (
                  <div key={l.name} className={`mk-ent-live mk-ent-live--${l.kind}`}>
                    <dt>
                      <Glyph kind={l.kind} />
                      {l.name}
                    </dt>
                    <dd>{l.where}</dd>
                  </div>
                ))}
              </dl>
            </div>
            <div className="mk-ent-wire" aria-hidden>
              <svg viewBox="0 0 40 44">
                <path d="M12 2v40M28 2v40" />
                <path d="M8 37l4 5 4-5M24 7l4-5 4 5" />
              </svg>
              <span>Rows read and written</span>
            </div>
            <div className="mk-ent-zone is-yours is-systems">
              <p className="mk-ent-zone-label">Your systems</p>
              <ul className="mk-ent-nodes">
                {e.systems.map((n) => (
                  <li key={n}>{n}</li>
                ))}
              </ul>
            </div>
            {i === 0 ? (
              <span className="mk-ent-or" aria-hidden>
                or
              </span>
            ) : null}
          </article>
        ))}
      </div>
    </figure>
  );
}

const SPEC: { group: string; rows: [string, string][] }[] = [
  {
    group: "Identity",
    rows: [
      ["Single sign-on", "OIDC or SAML, routed to the right identity provider by email domain"],
      ["Provisioning", "SCIM 2.0; identity-provider groups map to workspace roles"],
      ["Sessions", "Idle session timeout"],
    ],
  },
  {
    group: "Keys",
    rows: [
      ["Connector secrets", "Encrypted at rest"],
      ["Your own key", "Secrets wrapped with it; the platform key wraps the workspace key"],
    ],
  },
  {
    group: "Residency",
    rows: [["Data region", "Recorded on the workspace. A run is refused before it starts when the destination is elsewhere or shows no region"]],
  },
  {
    group: "Audit",
    rows: [
      ["Log", "Every action, signed and linked to the one before"],
      ["Verification", "Verify the chain and export it; a removed or altered entry is detected"],
    ],
  },
  {
    group: "Policy",
    rows: [
      ["Webhooks", "Signed, with a secret per channel"],
      ["Retention", "Job-history purge that runs a dry run first"],
    ],
  },
];

function EnterprisePage({ onNavigate }: Nav) {
  const pilot = { label: "Request a pilot", onClick: () => onNavigate("contact") };
  const security = { label: "Security overview", onClick: () => onNavigate("security") };
  return (
    <div className="mk-page mk-ent">
      <section className="mk-ent-hero" aria-labelledby="mk-ent-title">
        <div className="mk-wrap">
          <header className="mk-ent-intro">
            <h1 id="mk-ent-title">Your identity provider, your keys, your region.</h1>
            <div className="mk-ent-intro-side">
              <p>Run it hosted or in your own cloud. The plan, the checks, and the read-back do not change.</p>
              <Actions primary={pilot} secondary={security} />
            </div>
          </header>
          <DeploymentPlates />
        </div>
      </section>

      <section className="mk-a-stage" aria-labelledby="mk-ent-settings">
        <div className="mk-wrap">
          <header className="mk-a-stage-head">
            <h2 id="mk-ent-settings">Set it once, in Settings</h2>
            <p>Single sign-on, SCIM group mappings, roles, your key, and the region fence. Choose a section.</p>
          </header>
          <SettingsShowcase initial="sso" caption="Example workspace settings. Choose a section to open it." />
        </div>
      </section>

      <section className="mk-section mk-ent-spec-section" aria-labelledby="mk-ent-controls">
        <div className="mk-wrap">
          <div className="mk-ent-spec">
            <header className="mk-ent-spec-head">
              <h2 id="mk-ent-controls">Workspace controls</h2>
              <p>Switched on per workspace by an admin. Deployment-wide settings are reserved for platform admins.</p>
            </header>
            {SPEC.map((g) => (
              <section key={g.group} className="mk-ent-spec-group" aria-labelledby={`mk-ent-spec-${g.group}`}>
                <h3 id={`mk-ent-spec-${g.group}`}>{g.group}</h3>
                <dl>
                  {g.rows.map(([k, v]) => (
                    <div key={k}>
                      <dt>{k}</dt>
                      <dd>{v}</dd>
                    </div>
                  ))}
                </dl>
              </section>
            ))}
          </div>
        </div>
      </section>

      <section className="mk-ent-roles" aria-labelledby="mk-ent-roles-title">
        <div className="mk-wrap">
          <header className="mk-ent-roles-head">
            <h2 id="mk-ent-roles-title">Four roles, checked on every request</h2>
            <p>In Transfer Studio, on schedules, in Pilot, and over MCP. An API key never carries more than the person who created it.</p>
          </header>
          <ol className="mk-ent-ladder" aria-label="Workspace roles, least access first">
            {ROLES.map((r, i) => (
              <li key={r.name} style={{ ["--step" as string]: i }}>
                <span className="mk-ent-ladder-bar" aria-hidden />
                <h3>{r.name}</h3>
                <p>{r.does}</p>
              </li>
            ))}
          </ol>
        </div>
      </section>

      <section className="mk-section mk-ent-buy" aria-labelledby="mk-ent-buy-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-ent-buy-title">Buying Datawrap</h2>
          </header>
          <dl className="mk-ent-buy-grid">
            <div>
              <dt>A pilot on your own data</dt>
              <dd>One scoped load, your source to your destination, ending in a signed proof pack you keep.</dd>
            </div>
            <div>
              <dt>Data processing agreement</dt>
              <dd>
                On request: <a href="mailto:contact@datawrap.io?subject=DPA%20request">contact@datawrap.io</a>
              </dd>
            </div>
            <div>
              <dt>Subprocessor list</dt>
              <dd>
                On request: <a href="mailto:contact@datawrap.io?subject=Subprocessor%20list">contact@datawrap.io</a>
              </dd>
            </div>
            <div>
              <dt>Commercial terms</dt>
              <dd>
                <a href="mailto:sales@datawrap.io">sales@datawrap.io</a>
              </dd>
            </div>
          </dl>
        </div>
      </section>

      <Closing
        title="Run a pilot under your own policy"
        body="Start on the engine your operators will use, and switch on SSO, SCIM, and your own key when security is ready."
        primary={pilot}
        secondary={security}
      />
    </div>
  );
}

/* -------------------------------------------------------------------- Proof */

/** "SQLite, CSV, JSON, and 10 more" */
function groupSummary(g: ProofGroup): string {
  const names = g.drivers.map((d) => d.name);
  if (names.length === 1) return names[0];
  if (names.length <= 3) return `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`;
  return `${names.slice(0, 3).join(", ")}, and ${names.length - 3} more`;
}

/** The selected proof group, set as a signed certificate. */
function Certificate({ group, drawn }: { group: ProofGroup; drawn: boolean }) {
  const n = group.drivers.length;
  return (
    <article className="mk-proof-cert" aria-labelledby="mk-proof-cert-title">
      <header className="mk-proof-cert-head">
        <p id="mk-proof-cert-title" className="mk-proof-cert-title">
          CI evidence
        </p>
        <p className="mk-proof-cert-date">Recorded {EVIDENCE_AS_OF}</p>
      </header>
      <div className="mk-proof-cert-figure">
        <p className="mk-proof-cert-num">{fmt.format(group.executed)}</p>
        <p className="mk-proof-cert-unit">
          tests executed by {group.jobs.length === 1 ? "this job" : "these jobs"}, none skipped
        </p>
      </div>
      <p className="mk-proof-cert-claim">{proofClaim(group.emulated)}.</p>
      <div className="mk-proof-cert-drivers">
        <p className="mk-proof-cert-label">
          {n} {n === 1 ? "driver" : "drivers"} proven
        </p>
        <ul>
          {group.drivers.map((d) => (
            <li key={d.driver}>
              <span className="mk-proof-icon">
                <ConnectorIcon id={d.driver} size={16} />
              </span>
              {d.name}
            </li>
          ))}
        </ul>
      </div>
      <footer className="mk-proof-cert-foot">
        <dl>
          <div>
            <dt>{group.jobs.length === 1 ? "CI job" : "CI jobs"}</dt>
            <dd>
              {group.jobs.map((j) => (
                <code key={j}>{j}</code>
              ))}
            </dd>
          </div>
          <div>
            <dt>CI run</dt>
            <dd>
              <code>{EVIDENCE_RUN_ID}</code>
            </dd>
          </div>
        </dl>
        <div className="mk-proof-seal">
          <GuillocheSeal drawn={drawn} />
          <span>{group.emulated ? "Green on the emulator" : "Green, none skipped"}</span>
        </div>
      </footer>
    </article>
  );
}

/** Proof groups as an index of certificates; choosing one brings it to the front. */
function ProofDesk() {
  const [active, setActive] = useState(0);
  const [drawn, setDrawn] = useState(false);
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);
  const group = PROOF_GROUPS[active];

  // The page's one self-starting moment: the seal draws once. After that it
  // only redraws when a visitor picks another certificate.
  useEffect(() => {
    if (prefersReducedMotion()) {
      setDrawn(true);
      return;
    }
    setDrawn(false);
    let inner = 0;
    const outer = requestAnimationFrame(() => {
      inner = requestAnimationFrame(() => setDrawn(true));
    });
    return () => {
      cancelAnimationFrame(outer);
      cancelAnimationFrame(inner);
    };
  }, [active]);

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const last = PROOF_GROUPS.length - 1;
    let next = active;
    if (e.key === "ArrowDown" || e.key === "ArrowRight") next = active === last ? 0 : active + 1;
    else if (e.key === "ArrowUp" || e.key === "ArrowLeft") next = active === 0 ? last : active - 1;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = last;
    else return;
    e.preventDefault();
    setActive(next);
    tabs.current[next]?.focus();
  };

  if (!group) return null;
  return (
    <div className="mk-proof-desk">
      <div className="mk-proof-stack" role="tabpanel" id="mk-proof-panel" aria-labelledby={`mk-proof-tab-${active}`}>
        <Certificate key={active} group={group} drawn={drawn} />
      </div>
      <div className="mk-proof-index" role="tablist" aria-label="Proof groups" aria-orientation="vertical" onKeyDown={onKey}>
        {PROOF_GROUPS.map((g, i) => (
          <button
            key={`${g.jobs.join(",")}-${g.emulated}`}
            ref={(el) => {
              tabs.current[i] = el;
            }}
            type="button"
            role="tab"
            id={`mk-proof-tab-${i}`}
            aria-selected={i === active}
            aria-controls="mk-proof-panel"
            tabIndex={i === active ? 0 : -1}
            className={`mk-proof-tab${i === active ? " is-active" : ""}`}
            onClick={() => setActive(i)}
          >
            <span className="mk-proof-tab-count">{g.drivers.length}</span>
            <span className="mk-proof-tab-names">{groupSummary(g)}</span>
            <span className="mk-proof-tab-meta">
              {fmt.format(g.executed)} tests{g.emulated ? ", on an emulator" : ""}
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}

const LIVE_PROVEN = PROVEN_DRIVERS - EMULATED_DRIVERS.length;
const IMPLEMENTED = CATALOG_TOTAL - PROVEN_DRIVERS;

function ProofPage({ onNavigate }: Nav) {
  const pilot = { label: "Request a pilot", onClick: () => onNavigate("contact") };
  const connectors = { label: "See connectors", onClick: () => onNavigate("integrations") };
  return (
    <div className="mk-page mk-proof">
      <section className="mk-proof-hero" aria-labelledby="mk-proof-title">
        <div className="mk-wrap mk-proof-hero-grid">
          <div className="mk-proof-copy">
            <h1 id="mk-proof-title">Evidence you can check yourself.</h1>
            <p>Every connector claim is generated from a CI run, not typed by hand. Choose a certificate to read it.</p>
            <Actions primary={pilot} secondary={connectors} />
            <p className="mk-proof-note">A CI job runs a whole suite, so the drivers it proves share its test total.</p>
          </div>
          <ProofDesk />
        </div>
      </section>

      <section className="mk-section mk-section--band mk-proof-sum-section" aria-labelledby="mk-proof-sum">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-proof-sum">The catalogue, accounted for</h2>
            <p>
              {EMULATED_DRIVERS.join(" and ")} run against emulators. That proves the load, MERGE, and read-back logic,
              not a production account; a pilot proves that.
            </p>
          </header>
          <p className="mk-sr-only">
            {CATALOG_TOTAL} drivers in the catalogue equals {LIVE_PROVEN} proven in CI against the live engine, plus{" "}
            {EMULATED_DRIVERS.length} proven against an emulator, plus {IMPLEMENTED} implemented and running their CI proof.
          </p>
          <dl className="mk-proof-sum" aria-hidden>
            <div className="mk-proof-term mk-proof-term--all">
              <dt>in the catalogue</dt>
              <dd>{CATALOG_TOTAL}</dd>
            </div>
            <span className="mk-proof-op">=</span>
            <div className="mk-proof-term mk-proof-term--live">
              <dt>proven, live engine</dt>
              <dd>{LIVE_PROVEN}</dd>
            </div>
            <span className="mk-proof-op">+</span>
            <div className="mk-proof-term mk-proof-term--emu">
              <dt>proven, emulator</dt>
              <dd>{EMULATED_DRIVERS.length}</dd>
            </div>
            <span className="mk-proof-op">+</span>
            <div className="mk-proof-term mk-proof-term--impl">
              <dt>implemented, proof running</dt>
              <dd>{IMPLEMENTED}</dd>
            </div>
          </dl>
          <div className="mk-proof-ledger">
            <ProofsShowcase caption={`The Proofs screen, filled with this page's CI evidence, recorded ${EVIDENCE_AS_OF}.`} />
          </div>
        </div>
      </section>

      <section className="mk-proof-verify" aria-labelledby="mk-proof-pack">
        <div className="mk-wrap mk-proof-verify-grid">
          <div className="mk-proof-verify-copy">
            <h2 id="mk-proof-pack">Verify a proof pack</h2>
            <p>Every load can export a signed record of its checks and mapping. Anyone in the workspace can check it.</p>
            <ol className="mk-proof-calls">
              <li>
                <span>Download the pack for a finished job</span>
                <code>GET /api/v1/transfer/{"{job_id}"}/proof-pack</code>
              </li>
              <li>
                <span>Check its signature and content hash</span>
                <code>POST /api/v1/transfer/proof-pack/verify</code>
              </li>
              <li>
                <span>Check the audit log still holds its record</span>
                <code>POST /api/v1/audit/verify-pack</code>
              </li>
            </ol>
            <dl className="mk-proof-rule">
              <div className="is-full">
                <dt>Full checksum after the write</dt>
                <dd>The pack may say “migration proven”.</dd>
              </div>
              <div className="is-sample">
                <dt>A sample after the write</dt>
                <dd>The pack exports, and refuses that claim.</dd>
              </div>
            </dl>
          </div>
        </div>
        <div className="mk-wrap mk-proof-verify-frame">
          <SignOffShowcase caption="Example run. Sign as each role; the pack is signed once all three have." />
        </div>
      </section>

      <DesignPartners headingId="mk-proof-partners" />

      <Closing
        title="Get the same evidence on your data"
        body="A pilot is one scoped load on your own source and destination, ending with a proof pack you keep."
        primary={pilot}
        secondary={connectors}
      />
    </div>
  );
}

/* ----------------------------------------------------------------- Security */

/** One labelled box in the architecture drawing; lines are centred. */
function Box({ x, y, w, h, lines, tone = "" }: { x: number; y: number; w: number; h: number; lines: string[]; tone?: string }) {
  const cx = x + w / 2;
  const lh = 16;
  const top = y + h / 2 - ((lines.length - 1) * lh) / 2 + 5;
  return (
    <g className={`mk-sec-box ${tone}`.trim()}>
      <rect x={x} y={y} width={w} height={h} rx={3} />
      {lines.map((t, i) => (
        <text key={t} x={cx} y={top + i * lh} textAnchor="middle" className={i === 0 ? "mk-sec-t mk-sec-t--b" : "mk-sec-t mk-sec-t--s"}>
          {t}
        </text>
      ))}
    </g>
  );
}

/** A boundary: an engraved double frame with its name set on the line. */
function Frame({
  x,
  y,
  w,
  h,
  name,
  note,
  tone,
  labelW,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  name: string;
  note?: string;
  tone: string;
  labelW: number;
}) {
  // The knock-out behind the label is sized to the set text once it renders;
  // labelW is the estimate used before that (or while the drawing is hidden).
  const label = useRef<SVGTextElement | null>(null);
  const [knock, setKnock] = useState(labelW);
  useLayoutEffect(() => {
    try {
      const width = label.current?.getBBox().width ?? 0;
      if (width > 0) setKnock(Math.ceil(width) + 13);
    } catch {
      /* not rendered: keep the estimate */
    }
  }, [name, note]);
  return (
    <g className={`mk-sec-frame mk-sec-frame--${tone}`}>
      <rect x={x} y={y} width={w} height={h} rx={6} className="mk-sec-frame-outer" />
      <rect x={x + 4} y={y + 4} width={w - 8} height={h - 8} rx={4} className="mk-sec-frame-inner" />
      <rect x={x + 12} y={y - 9} width={knock} height={18} className="mk-sec-knock" />
      <text ref={label} x={x + 18} y={y + 4.5} className="mk-sec-t mk-sec-t--label">
        <tspan className="mk-sec-t--b">{name}</tspan>
        {note ? <tspan>{` ${note}`}</tspan> : null}
      </text>
    </g>
  );
}

function Chain({ x, y, n }: { x: number; y: number; n: number }) {
  return (
    <g className="mk-sec-chain">
      {Array.from({ length: n }, (_, i) => (
        <g key={i}>
          <rect x={x + i * 36} y={y} width={26} height={18} rx={2} />
          {i < n - 1 ? <path d={`M${x + i * 36 + 26} ${y + 9}h10`} /> : null}
        </g>
      ))}
    </g>
  );
}

function Markers({ arrow, held }: { arrow: string; held: string }) {
  return (
    <defs>
      <marker id={arrow} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
        <path d="M0 0L10 5L0 10z" className="mk-sec-head" />
      </marker>
      <marker id={held} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
        <path d="M0 0L10 5L0 10z" className="mk-sec-head mk-sec-head--held" />
      </marker>
    </defs>
  );
}

/** The layered architecture, wide: the data path runs left to right through the boundaries. */
function ArchitectureWide({ mid }: { mid: string }) {
  const arrow = `mk-sec-arrow-${mid}`;
  const held = `mk-sec-held-${mid}`;
  const callers: [string, number, number][] = [
    ["A person", 215, 84],
    ["A schedule", 325, 96],
    ["Pilot", 425, 62],
    ["An agent over MCP", 545, 142],
  ];
  return (
    <svg className="mk-sec-svg mk-sec-svg--wide" viewBox="0 0 760 620" aria-hidden focusable="false">
      <Markers arrow={arrow} held={held} />

      {callers.map(([t, cx, w]) => (
        <g key={t} className="mk-sec-caller">
          <path d={`M${cx} 36C${cx} 62 380 54 380 72`} className="mk-sec-line mk-sec-line--thin" />
          <rect x={cx - w / 2} y={8} width={w} height={28} rx={14} />
          <text x={cx} y={26.5} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
            {t}
          </text>
        </g>
      ))}

      <Frame x={130} y={80} w={500} h={470} name="Identity" note="SSO, SCIM, four roles" tone="identity" labelW={226} />
      <circle cx={380} cy={80} r={8} className="mk-sec-gate" />
      <path d="M376 80.5l3 3 5-6" className="mk-sec-gate-mark" />

      <Frame x={160} y={120} w={440} h={398} name="Workspace" note="every read scoped to it" tone="workspace" labelW={240} />

      <text x={180} y={170} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        Each connection is checked
      </text>
      <text x={180} y={186} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        where it is made
      </text>

      <path d="M120 234H278" className="mk-sec-line mk-sec-line--data" markerEnd={`url(#${arrow})`} />
      <path d="M480 234H638" className="mk-sec-line mk-sec-line--data" markerEnd={`url(#${arrow})`} />
      <Box x={4} y={204} w={116} h={60} lines={["Your source"]} tone="is-ext" />
      <Box x={640} y={204} w={116} h={60} lines={["Your destination"]} tone="is-ext" />
      <text x={214} y={225} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        rows
      </text>
      <text x={530} y={225} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        rows
      </text>
      <rect x={155} y={229} width={10} height={10} transform="rotate(45 160 234)" className="mk-sec-check" />
      <rect x={595} y={229} width={10} height={10} transform="rotate(45 600 234)" className="mk-sec-check" />

      <Box x={280} y={196} w={200} h={76} lines={["Datawrap engine", "checks, writes,", "holds, reads back"]} tone="is-engine" />

      <path d="M320 272L296 340" className="mk-sec-line mk-sec-line--key" />
      <path d="M440 272L470 336" className="mk-sec-line mk-sec-line--held" markerEnd={`url(#${held})`} />

      <Frame x={190} y={340} w={170} h={84} name="Encryption" tone="encryption" labelW={90} />
      <text x={275} y={374} textAnchor="middle" className="mk-sec-t mk-sec-t--b">
        Connector secrets
      </text>
      <text x={275} y={392} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        encrypted at rest,
      </text>
      <text x={275} y={408} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        wrapped with your key
      </text>

      <Box x={400} y={340} w={170} h={84} lines={["Held rows", "kept with the reason,", "replayed once fixed"]} tone="is-held" />

      <path d="M275 424v18M485 424v18" className="mk-sec-line mk-sec-line--thin" markerEnd={`url(#${arrow})`} />

      <Frame x={190} y={446} w={380} h={54} name="Audit" tone="audit" labelW={52} />
      <Chain x={208} y={464} n={6} />
      <text x={424} y={470} className="mk-sec-t mk-sec-t--s">
        Signed, each linked
      </text>
      <text x={424} y={486} className="mk-sec-t mk-sec-t--s">
        to the one before
      </text>

      <path d="M380 500V562" className="mk-sec-line mk-sec-line--thin mk-sec-line--dash" markerEnd={`url(#${arrow})`} />
      <Box x={290} y={566} w={180} h={46} lines={["Can be anchored in S3", "with Object Lock"]} tone="is-anchor" />
    </svg>
  );
}

/** The same architecture, tall, for phones: rows run down into the engine and back up out. */
function ArchitectureTall({ mid }: { mid: string }) {
  const arrow = `mk-sec-tarrow-${mid}`;
  const held = `mk-sec-theld-${mid}`;
  const callers = ["A person", "A schedule", "Pilot", "An agent over MCP"];
  return (
    <svg className="mk-sec-svg mk-sec-svg--tall" viewBox="0 0 360 700" aria-hidden focusable="false">
      <Markers arrow={arrow} held={held} />

      {callers.map((t, i) => (
        <g key={t} className="mk-sec-caller">
          <path d={`M128 ${16 + i * 30}H140V142`} className="mk-sec-line mk-sec-line--thin" />
          <rect x={4} y={4 + i * 30} width={124} height={24} rx={12} />
          <text x={66} y={20.5 + i * 30} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
            {t}
          </text>
        </g>
      ))}

      <Box x={156} y={44} w={92} h={50} lines={["Your", "source"]} tone="is-ext" />
      <Box x={260} y={44} w={96} h={50} lines={["Your", "destination"]} tone="is-ext" />

      <Frame x={8} y={150} w={344} h={470} name="Identity" tone="identity" labelW={76} />
      <circle cx={140} cy={150} r={7} className="mk-sec-gate" />
      <path d="M136.5 150.5l2.6 2.6 4.4-5.2" className="mk-sec-gate-mark" />
      <Frame x={24} y={190} w={312} h={414} name="Workspace" tone="workspace" labelW={88} />

      <path d="M202 94V234" className="mk-sec-line mk-sec-line--data" markerEnd={`url(#${arrow})`} />
      <path d="M308 238V98" className="mk-sec-line mk-sec-line--data" markerEnd={`url(#${arrow})`} />
      <rect x={197} y={185} width={10} height={10} transform="rotate(45 202 190)" className="mk-sec-check" />
      <rect x={303} y={185} width={10} height={10} transform="rotate(45 308 190)" className="mk-sec-check" />

      <Box x={160} y={238} w={176} h={70} lines={["Datawrap engine", "checks, writes,", "holds, reads back"]} tone="is-engine" />
      <text x={40} y={252} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        Each
      </text>
      <text x={40} y={268} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        connection
      </text>
      <text x={40} y={284} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        is checked
      </text>

      <path d="M196 308L130 352" className="mk-sec-line mk-sec-line--key" />
      <path d="M290 308V348" className="mk-sec-line mk-sec-line--held" markerEnd={`url(#${held})`} />

      <Frame x={40} y={352} w={146} h={92} name="Encryption" tone="encryption" labelW={90} />
      <text x={113} y={388} textAnchor="middle" className="mk-sec-t mk-sec-t--b">
        Secrets
      </text>
      <text x={113} y={406} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        encrypted at rest,
      </text>
      <text x={113} y={422} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        wrapped with your key
      </text>
      <Box x={198} y={352} w={122} h={92} lines={["Held rows", "kept with", "the reason"]} tone="is-held" />

      <Frame x={40} y={472} w={280} h={104} name="Audit" tone="audit" labelW={52} />
      <Chain x={67} y={494} n={6} />
      <text x={180} y={540} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        Signed, each linked
      </text>
      <text x={180} y={556} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        to the one before
      </text>

      <path d="M180 576V636" className="mk-sec-line mk-sec-line--thin mk-sec-line--dash" markerEnd={`url(#${arrow})`} />
      <Box x={90} y={640} w={180} h={50} lines={["Can be anchored in S3", "with Object Lock"]} tone="is-anchor" />
    </svg>
  );
}

/** Security hero: the architecture, and what each boundary does in words for screen readers. */
function Architecture() {
  const mid = useId().replace(/[^a-zA-Z0-9-]/g, "");
  return (
    <figure className="mk-sec-arch" aria-labelledby="mk-sec-arch-cap">
      <ArchitectureWide mid={mid} />
      <ArchitectureTall mid={mid} />
      <figcaption id="mk-sec-arch-cap" className="mk-sec-arch-cap">
        The path of a row, and the boundaries it crosses
      </figcaption>
      <ul className="mk-sr-only">
        <li>Identity: a person, a schedule, Pilot, or an agent over MCP signs in through single sign-on and is checked against one of four roles.</li>
        <li>Workspace: every read is scoped to the caller's workspace.</li>
        <li>Data path: rows travel from your source through the Datawrap engine to your destination. Each connection is checked where it is made.</li>
        <li>Encryption: connector secrets are encrypted at rest and wrapped with your key if you bring one.</li>
        <li>Held rows are kept in the job's quarantine with the reason.</li>
        <li>Audit: every entry is signed and linked to the one before. The chain head can be anchored in S3 with Object Lock.</li>
      </ul>
    </figure>
  );
}

const SEC_CONTROLS: { control: string; boundary: string; does: string; always: boolean }[] = [
  {
    control: "Workspace isolation",
    boundary: "Workspace",
    does: "Every read of jobs, connectors, schedules, notifications, and audit is scoped to the caller's workspace, Pilot and MCP included. Another workspace's id returns not found.",
    always: true,
  },
  {
    control: "Roles",
    boundary: "Identity",
    does: "Viewer, operator, editor, and admin. Every API route is checked against one permission, and a route with no rule is refused.",
    always: true,
  },
  {
    control: "Single sign-on and SCIM",
    boundary: "Identity",
    does: "OIDC or SAML, routed by email domain. Groups from your identity provider map to workspace roles.",
    always: false,
  },
  {
    control: "Connector secrets",
    boundary: "Encryption",
    does: "Encrypted at rest, can be wrapped with your own key, and can be stored in AWS Secrets Manager. Error responses do not carry raw driver text.",
    always: true,
  },
  {
    control: "Outbound connections",
    boundary: "Data path",
    does: "Checked where each connection is made. Cloud metadata addresses are always refused; private networks are refused unless the workspace lists them.",
    always: true,
  },
  {
    control: "Data region",
    boundary: "Data path",
    does: "Once a workspace records its region, a destination elsewhere, or with no region, is refused before the run starts.",
    always: false,
  },
  {
    control: "Audit log",
    boundary: "Audit",
    does: "Each entry is signed and linked to the one before. The chain head can be anchored in S3 with Object Lock, outside the application's reach.",
    always: true,
  },
  {
    control: "Signed webhooks",
    boundary: "Workspace",
    does: "Each channel has its own secret, and every delivery carries a timestamped HMAC-SHA256 signature.",
    always: false,
  },
  {
    control: "Retention",
    boundary: "Workspace",
    does: "Purge finished jobs older than your window, after a dry run and a confirm token.",
    always: false,
  },
];

const DEPLOY_ROWS: { what: string; hosted: string | null; self: string | null }[] = [
  { what: "Where it runs", hosted: "Datawrap's account", self: "Your cloud or VPC" },
  { what: "Where jobs and audit events live", hosted: "Your workspace, in Datawrap's account", self: "Your own MongoDB" },
  { what: "Connector credentials and evidence", hosted: "Encrypted in your workspace", self: "Stay in your account" },
  { what: "Deployment", hosted: "Nothing to install", self: "Two images, with Docker Compose or Helm" },
  { what: "Your own KMS key on connector secrets", hosted: "Enterprise plan", self: "Enterprise plan" },
  { what: "Plan", hosted: "Every plan", self: "Enterprise" },
];

function SecurityPage({ onNavigate }: Nav) {
  const pilot = { label: "Request a pilot", onClick: () => onNavigate("contact") };
  const enterprise = { label: "Enterprise", onClick: () => onNavigate("enterprise") };
  return (
    <div className="mk-page mk-sec">
      <section className="mk-sec-hero" aria-labelledby="mk-sec-title">
        <div className="mk-wrap">
          <div className="mk-sec-sheet">
            <div className="mk-sec-copy">
              <h1 id="mk-sec-title">What protects your data while it moves.</h1>
              <p>The same boundaries hold in Transfer Studio, on schedules, in Pilot, and over MCP.</p>
              <Actions primary={pilot} secondary={enterprise} />
            </div>
            <Architecture />
          </div>
        </div>
      </section>

      <section className="mk-section mk-sec-ledger-section" aria-labelledby="mk-sec-every">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-sec-every">The controls, boundary by boundary</h2>
          </header>
          <table className="mk-sec-ledger">
            <caption className="mk-sr-only">Security controls</caption>
            <thead>
              <tr>
                <th scope="col">Control</th>
                <th scope="col">Boundary</th>
                <th scope="col">What it does</th>
                <th scope="col">When</th>
              </tr>
            </thead>
            <tbody>
              {SEC_CONTROLS.map((c) => (
                <tr key={c.control}>
                  <th scope="row">{c.control}</th>
                  <td className="mk-sec-td-boundary">
                    <span className={`mk-sec-tag mk-sec-tag--${c.boundary.toLowerCase().replace(/\s+/g, "-")}`}>{c.boundary}</span>
                  </td>
                  <td className="mk-sec-td-does">{c.does}</td>
                  <td className={`mk-sec-td-when ${c.always ? "is-always" : "is-admin"}`}>
                    {c.always ? (
                      <>
                        <StatusGlyph kind="landed" />
                        On every transfer
                      </>
                    ) : (
                      "When an admin sets it"
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-sec-deploy-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-sec-deploy-title">Hosted or self-managed</h2>
          </header>
          <table className="mk-sec-deploy">
            <caption className="mk-sr-only">Hosted and self-managed deployment compared</caption>
            <thead>
              <tr>
                <td />
                <th scope="col">
                  Hosted<small>The default</small>
                </th>
                <th scope="col">
                  Self-managed<small>An Enterprise option</small>
                </th>
              </tr>
            </thead>
            <tbody>
              {DEPLOY_ROWS.map((r) => (
                <tr key={r.what}>
                  <th scope="row">{r.what}</th>
                  {[r.hosted, r.self].map((v, i) => (
                    <td key={i} data-label={i === 0 ? "Hosted" : "Self-managed"}>
                      {v ? (
                        <span className="mk-sec-deploy-mark">
                          <StatusGlyph kind="landed" />
                          {v}
                        </span>
                      ) : (
                        <span className="mk-sec-deploy-dash">
                          <span aria-hidden>–</span>
                          <span className="mk-sr-only">Not applicable</span>
                        </span>
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="mk-sec-audit" aria-labelledby="mk-sec-audit-title">
        <div className="mk-wrap mk-sec-audit-grid">
          <div className="mk-sec-audit-copy">
            <h2 id="mk-sec-audit-title">An audit log that checks itself</h2>
            <p>Remove or alter an entry and the chain no longer verifies. Verify it, then export it.</p>
          </div>
          <div className="mk-sec-audit-panel">
            <SettingsShowcase
              initial="audit"
              sections={["audit", "sso", "team", "enterprise"]}
              caption="Example audit log. Choose Verify chain; each entry links to the hash of the one before."
            />
          </div>
        </div>
      </section>

      <section className="mk-section mk-sec-disclose-section" id="vulnerability-disclosure" aria-labelledby="mk-sec-disclosure">
        <div className="mk-wrap">
          <div className="mk-sec-letter">
            <h2 id="mk-sec-disclosure">Report a vulnerability</h2>
            <p className="mk-sec-letter-lead">
              Email <a href="mailto:contact@datawrap.io?subject=Security%20report">contact@datawrap.io</a> with the subject
              “Security report”, and please do not open a public issue. We acknowledge reports within three business days
              and keep you updated until resolved.
            </p>
            <p>We will not pursue good-faith research that:</p>
            <ul className="mk-sec-letter-list">
              <li>tests only accounts you own</li>
              <li>stops at the first sign of another customer’s data</li>
              <li>avoids denial of service and social engineering</li>
              <li>gives us reasonable time to fix before disclosure</li>
            </ul>
            <p className="mk-sec-letter-foot">
              Machine-readable contact: <a href="/.well-known/security.txt">/.well-known/security.txt</a>
            </p>
          </div>
        </div>
      </section>

      <Closing
        title="Review it on your own data"
        body="A pilot runs under your workspace policy, and its audit log and proof pack are yours to inspect."
        primary={pilot}
        secondary={enterprise}
      />
    </div>
  );
}

/* ------------------------------------------------------------- Connectors */

type ConnectorRow = {
  name: string;
  driver: string;
  family: string;
  proven: boolean;
  emulated: boolean;
  cdc: "proven" | "implemented" | null;
};

const CONNECTOR_ROWS: ConnectorRow[] = ALL_CONNECTORS.map((c): ConnectorRow => {
  const cdc = CONNECTOR_CLAIMS.cdc_sources.find((x) => x.driver === c.driver);
  return { ...c, cdc: cdc ? (cdc.proven ? "proven" : "implemented") : null };
});

function tileState(c: CatalogConnector): "proven" | "emulated" | "implemented" {
  if (!c.proven) return "implemented";
  return c.emulated ? "emulated" : "proven";
}

/** The hero: every driver the evidence counts, by family, marked by what CI proves. */
function CoverageMatrix({ onPick }: { onPick: (family: string) => void }) {
  const [only, setOnly] = useState("All");
  const shown = MARKETING_STACK.filter((f) => only === "All" || f.family === only);
  return (
    <figure className="mk-int-matrix" aria-labelledby="mk-int-matrix-cap">
      <figcaption className="mk-int-matrix-cap">
        <span id="mk-int-matrix-cap">Supported systems, by family</span>
        <span className="mk-int-key">
          <span>
            <i className="mk-int-swatch is-proven" aria-hidden />
            Proven in CI
          </span>
          <span>
            <i className="mk-int-swatch is-emulated" aria-hidden />
            Proven on an emulator
          </span>
          <span>
            <i className="mk-int-swatch is-implemented" aria-hidden />
            Implemented
          </span>
        </span>
      </figcaption>
      <div className="mk-int-filter mk-int-chips" role="group" aria-label="Show one family">
        {["All", ...MARKETING_STACK.map((f) => f.family)].map((f) => (
          <button key={f} type="button" className="mk-int-chip" aria-pressed={only === f} onClick={() => setOnly(f)}>
            {f}
          </button>
        ))}
      </div>
      <ul className="mk-int-rows">
        {shown.map((f) => (
          <li key={f.family} className="mk-int-row">
            <div className="mk-int-fam">
              <button
                type="button"
                className="mk-int-fam-name"
                onClick={() => onPick(f.family)}
                aria-label={`${f.family}: show in the driver table`}
              >
                {f.family}
              </button>
              <p className="mk-int-fam-count">
                <strong>{f.provenCount}</strong> of {f.total} proven
              </p>
            </div>
            <ul className="mk-int-tiles" aria-label={`${f.family} drivers`}>
              {ALL_CONNECTORS.filter((c) => c.family === f.family).map((c) => {
                const state = tileState(c);
                return (
                  <li key={c.driver} className={`mk-int-tile is-${state}`}>
                    <span className="mk-int-tile-icon">
                      <ConnectorIcon id={c.driver} size={22} />
                    </span>
                    <span className="mk-int-tile-name">{c.name}</span>
                    <span className="mk-sr-only">, {c.proven ? proofLabel(c.emulated) : "implemented"}</span>
                    {c.proven ? (
                      <span className="mk-int-tile-mark" aria-hidden>
                        <StatusGlyph kind="landed" />
                      </span>
                    ) : null}
                  </li>
                );
              })}
            </ul>
          </li>
        ))}
      </ul>
    </figure>
  );
}

function IntegrationsPage({ onGetStarted, onNavigate }: Nav) {
  const families = useMemo(() => ["All", ...new Set(ALL_CONNECTORS.map((c) => c.family))], []);
  const [family, setFamily] = useState("All");
  const [query, setQuery] = useState("");
  const [provenOnly, setProvenOnly] = useState(false);
  const searchId = useId();
  const provenId = useId();
  const tableRef = useRef<HTMLDivElement | null>(null);
  const q = query.trim().toLowerCase();
  const rows = CONNECTOR_ROWS.filter(
    (r) => (family === "All" || r.family === family) && (!provenOnly || r.proven) && (!q || r.name.toLowerCase().includes(q)),
  );
  const start = { label: "Start free", onClick: onGetStarted };
  const docs = { label: "Read the docs", onClick: () => onNavigate("help") };

  const pickFamily = (f: string) => {
    setFamily(f);
    setQuery("");
    setProvenOnly(false);
    const el = tableRef.current;
    if (!el) return;
    el.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "start" });
    el.focus({ preventScroll: true });
  };

  return (
    <div className="mk-page mk-int">
      <section className="mk-int-hero" aria-labelledby="mk-int-title">
        <div className="mk-wrap">
          <header className="mk-int-head">
            <p className="mk-int-tally" aria-hidden>
              <span className="mk-int-big">{PROVEN_DRIVERS}</span>
              <span className="mk-int-of">
                <span>of {CATALOG_TOTAL}</span>
                <span>drivers proven in CI</span>
              </span>
            </p>
            <div className="mk-int-headline">
              <h1 id="mk-int-title">Connect what you run, and see what CI has proven.</h1>
              <p>
                {PROVEN_DRIVERS} of {CATALOG_TOTAL} drivers ran green in CI with no skipped test. The other{" "}
                {IMPLEMENTED} are implemented and running their CI proof.
              </p>
              <Actions primary={start} secondary={docs} />
            </div>
          </header>
          <CoverageMatrix onPick={pickFamily} />
        </div>
      </section>

      <section className="mk-a-stage" aria-labelledby="mk-int-wizard">
        <div className="mk-wrap">
          <header className="mk-a-stage-head">
            <h2 id="mk-int-wizard">The same evidence where you pick a connector</h2>
            <p>Every tile in the connector wizard carries its driver's proof state. Filter it here.</p>
          </header>
          <ConnectorWizardShowcase caption="Example workspace. Tiles and proof states come from the CI evidence file." />
        </div>
      </section>

      <section className="mk-section mk-int-table-section" aria-labelledby="mk-matrix-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-matrix-title">
              All {CATALOG_TOTAL} drivers, {PROVEN_DRIVERS} proven in CI
            </h2>
            <p>Each listed once. Evidence recorded {EVIDENCE_AS_OF}.</p>
          </header>
          <div className="mk-int-console">
            <div className="mk-int-chips" role="group" aria-label="Filter by family">
              {families.map((f) => (
                <button key={f} type="button" className="mk-int-chip" aria-pressed={family === f} onClick={() => setFamily(f)}>
                  {f}
                  <span className="mk-int-chip-n" aria-hidden>
                    {f === "All" ? CATALOG_TOTAL : ALL_CONNECTORS.filter((c) => c.family === f).length}
                  </span>
                </button>
              ))}
            </div>
            <div className="mk-int-tools">
              <div className="mk-int-search">
                <label htmlFor={searchId} className="mk-sr-only">
                  Search drivers
                </label>
                <svg viewBox="0 0 20 20" aria-hidden focusable="false">
                  <circle cx="8.5" cy="8.5" r="5.5" />
                  <path d="M13 13l4 4" />
                </svg>
                <input
                  id={searchId}
                  type="search"
                  value={query}
                  autoComplete="off"
                  placeholder="Search drivers"
                  onChange={(e: ChangeEvent<HTMLInputElement>) => setQuery(e.target.value)}
                />
              </div>
              <label className="mk-int-switch" htmlFor={provenId}>
                <input
                  id={provenId}
                  type="checkbox"
                  role="switch"
                  checked={provenOnly}
                  onChange={(e) => setProvenOnly(e.target.checked)}
                />
                <span className="mk-int-switch-track" aria-hidden />
                <span>Proven in CI only</span>
              </label>
              <p className="mk-int-count" role="status" aria-live="polite">
                {rows.length} of {CATALOG_TOTAL} drivers shown, {rows.filter((r) => r.proven).length} of them proven in CI
              </p>
            </div>
          </div>
          <div className="mk-int-table-wrap" ref={tableRef} role="region" aria-labelledby="mk-matrix-title" tabIndex={-1}>
            <table className="mk-int-table">
              <thead>
                <tr>
                  <th scope="col">Driver</th>
                  <th scope="col">Family</th>
                  <th scope="col">Status</th>
                  <th scope="col">Change data capture</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={`${r.family}-${r.driver}`}>
                    <th scope="row">
                      <span className="mk-int-row-icon">
                        <ConnectorIcon id={r.driver} size={18} />
                      </span>
                      {r.name}
                    </th>
                    <td className="mk-int-td-family">{r.family}</td>
                    <td className="mk-int-td-status">
                      {r.proven ? (
                        <span className={`mk-int-badge is-proven${r.emulated ? " is-emulated" : ""}`}>
                          <StatusGlyph kind="landed" />
                          {proofLabel(r.emulated)}
                        </span>
                      ) : (
                        <span className="mk-int-badge">
                          <StatusGlyph kind="pending" />
                          Implemented
                        </span>
                      )}
                    </td>
                    <td className="mk-int-td-cdc">
                      {r.cdc === "proven" ? (
                        <span className="mk-int-cdc is-proven">
                          <StatusGlyph kind="landed" />
                          CDC source, proven in CI
                        </span>
                      ) : r.cdc === "implemented" ? (
                        <span className="mk-int-cdc">CDC source, implemented</span>
                      ) : (
                        <span className="mk-int-cdc is-none">
                          <span aria-hidden>–</span>
                          <span className="mk-sr-only">No</span>
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
                {rows.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="mk-int-empty">
                      No driver matches. Clear the search or choose All.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <section className="mk-int-path" aria-labelledby="mk-conn-path">
        <div className="mk-wrap mk-int-path-grid">
          <div className="mk-int-path-copy">
            <h2 id="mk-conn-path">One path for every family</h2>
            <dl>
              <div>
                <dt>Upsert and MERGE</dt>
                <dd>Wherever the destination's keys support them.</dd>
              </div>
              <div>
                <dt>Incremental sync</dt>
                <dd>A typed watermark on the cursor column, so the next run starts where the last one ended.</dd>
              </div>
              <div>
                <dt>Held rows</dt>
                <dd>Quarantined with the column, value, and reason, and replayed into the same table once fixed.</dd>
              </div>
            </dl>
          </div>
          <div className="mk-subhero-visual mk-int-path-panel">
            <QuarantinePanel />
          </div>
        </div>
      </section>

      <Closing
        title="Pick a source and a destination"
        body="Choose from the connectors your workspace can load today, on the same engine as production."
        primary={start}
        secondary={docs}
      />
    </div>
  );
}
