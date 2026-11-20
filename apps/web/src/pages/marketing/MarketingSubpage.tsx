import { useId, useLayoutEffect, useMemo, useRef, useState, type ChangeEvent, type KeyboardEvent, type ReactNode } from "react";
import { ConnectorIcon } from "../../app/vendor-marks";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { MarketingFaq } from "../../components/marketing/MarketingFaq";
import { QuarantinePanel, ROLES, StatusGlyph } from "../../components/marketing/ProductPanels";
import { Bento, BentoTile, type TileSpan } from "../../components/marketing/kit/Bento";
import { SplitHead } from "../../components/marketing/kit/SplitHead";
import { ProofsShowcase, SettingsShowcase, SignOffShowcase } from "./MarketingSubpageScenes";
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
import { hashForPublicRoute, type PublicRoute } from "../../lib/publicNavigation";
import { DocArticlePage, DocsPortal } from "./DocsPortal";
import { ContactSalesPage, SALES_ACTION_LABEL, START_ACTION_LABEL } from "./ContactSalesPage";
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
      return <ContactSalesPage onNavigate={onNavigate} onGetStarted={onGetStarted} />;
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
      return <DocsPortal onNavigate={onNavigate} onGetStarted={onGetStarted} marketing />;
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
      return <DemosPage onNavigate={onNavigate} onGetStarted={onGetStarted} />;
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
/*
 * Pricing, Enterprise, Proof, Security, and Connectors share one structure
 * (pass 7 kit): a split hero with one light frame under it, body sections
 * headed by SplitHead (a feature row, a bento of tiles, or a table), and the
 * CtaCard closer. Ink links, no dark bands.
 */

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

/** A link to another public page: ink with an underline, handled in-app. */
function PageLink({ to, onNavigate, children }: { to: PublicRoute; onNavigate: Nav["onNavigate"]; children: ReactNode }) {
  return (
    <a
      className="mk-link"
      href={hashForPublicRoute(to)}
      onClick={(e) => {
        e.preventDefault();
        onNavigate(to);
      }}
    >
      {children}
    </a>
  );
}

/** Hero: the h1, a lead, the actions, then one light frame at full width. */
function Hero({
  id,
  title,
  lead,
  actions,
  children,
}: {
  id: string;
  title: string;
  lead: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="mk-a-hero" aria-labelledby={id}>
      <div className="mk-wrap">
        <header className="mk-a-hero-copy">
          <h1 id={id}>{title}</h1>
          <p className="mk-a-lead">{lead}</p>
          {actions}
        </header>
        <div className="mk-a-hero-visual">{children}</div>
      </div>
    </section>
  );
}

/** A body section: a split head (h2 left, one line right), then tiles or a table. */
function Section({
  id,
  title,
  lead,
  className = "",
  children,
}: {
  id: string;
  title: ReactNode;
  lead?: ReactNode;
  className?: string;
  children: ReactNode;
}) {
  return (
    <section className={`mk-a-section mk-kit-sec ${className}`.trim()} aria-labelledby={id}>
      <div className="mk-wrap">
        <SplitHead id={id} title={title} lead={lead} />
        {children}
      </div>
    </section>
  );
}

/** A feature row: short copy on one side, one light frame on the other. */
function RowSection({
  id,
  title,
  lead,
  flip = false,
  copy,
  visual,
}: {
  id: string;
  title: string;
  lead: ReactNode;
  flip?: boolean;
  copy?: ReactNode;
  visual: ReactNode;
}) {
  return (
    <section className="mk-a-section" aria-labelledby={id}>
      <div className={`mk-wrap mk-a-row${flip ? " is-flip" : ""}`}>
        <div className="mk-a-row-copy">
          <h2 id={id}>{title}</h2>
          <p className="mk-a-row-lead">{lead}</p>
          {copy}
        </div>
        <div className="mk-a-row-visual">{visual}</div>
      </div>
    </section>
  );
}

type Fact = {
  key: string;
  title: ReactNode;
  body: ReactNode;
  /** A fragment of the real product UI at the tile's foot, from repo data. */
  ui?: ReactNode;
  span?: TileSpan;
  figure?: ReactNode;
  op?: "+" | "=";
};

/** Parallel facts as a bento: one tile each, its UI fragment at the foot. */
function FactTiles({ label, items }: { label: string; items: Fact[] }) {
  return (
    <Bento label={label}>
      {items.map(({ key, title, body, ui, span = 4, figure, op }) => (
        <BentoTile key={key} span={span} title={title} body={body} figure={figure} op={op}>
          {ui}
        </BentoTile>
      ))}
    </Bento>
  );
}

/** A row of connector chips, as the app draws a route's systems. */
function DriverChips({ drivers, more = 0, label }: { drivers: CatalogConnector[]; more?: number; label: string }) {
  return (
    <div className="mk-app-surface mk-tui">
      <ul className="mk-a-tui-chips" aria-label={label}>
        {drivers.map((d) => (
          <li key={d.driver} className="mk-app-chip">
            <ConnectorIcon id={d.driver} size={14} />
            {d.name}
          </li>
        ))}
        {more > 0 ? <li className="mk-a-tui-more">and {more} more</li> : null}
      </ul>
    </div>
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

const LIVE_PROVEN = PROVEN_DRIVERS - EMULATED_DRIVERS.length;
const IMPLEMENTED = CATALOG_TOTAL - PROVEN_DRIVERS;

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
  { id: "all", title: "In every plan", rows: PLAN_ROWS.filter((r) => r.in.length === 3) },
  { id: "team", title: "Added in Team", rows: PLAN_ROWS.filter((r) => r.in.length === 2) },
  { id: "enterprise", title: "Added in Enterprise", rows: PLAN_ROWS.filter((r) => r.in.length === 1) },
];

/**
 * The three plans. No price is invented: Starter is free in early access,
 * Team is quoted, Enterprise is agreed in an order form. Each card says who
 * it is for and what it adds; Team is the one recommended card.
 */
const PLANS: {
  tier: Tier;
  name: string;
  figure: string;
  qualifier: string;
  who: string;
  adds: string;
  highlights: string[];
  action: "start" | "sales" | "enterprise";
  recommended?: string;
}[] = [
  {
    tier: "starter",
    name: "Starter",
    figure: "Free",
    qualifier: "while in early access",
    who: "One person or a small team, trying the whole load path.",
    adds: "The whole load path",
    highlights: [
      "Plan, checks, quarantine, and read-back on every run",
      "A signed proof pack you can verify",
      `${provenDriversPhrase()}, more implemented`,
    ],
    action: "start",
  },
  {
    tier: "team",
    name: "Team",
    figure: "Talk to us",
    qualifier: "quoted on what you move and how often",
    who: "A data team that runs loads on a schedule.",
    adds: "Everything in Starter, plus",
    highlights: ["Schedules and change data capture", "Datawrap Pilot", "A shared workspace with four roles"],
    action: "sales",
    recommended: "Recommended for data teams",
  },
  {
    tier: "enterprise",
    name: "Enterprise",
    figure: "Custom",
    qualifier: "terms agreed in an order form",
    who: "Organizations with identity, key, and residency requirements.",
    adds: "Everything in Team, plus",
    highlights: [
      "SSO, SCIM, and your own key on connector secrets",
      "A data-region fence and a tamper-evident audit log",
      "MCP server for agents",
      "Self-managed deployment in your cloud or VPC",
    ],
    action: "enterprise",
  },
];

function PricingPage({ onGetStarted, onNavigate }: Nav) {
  const start = { label: START_ACTION_LABEL, onClick: onGetStarted };
  const sales = { label: SALES_ACTION_LABEL, onClick: () => onNavigate("contact") };
  const actions: Record<(typeof PLANS)[number]["action"], Action> = {
    start,
    sales,
    enterprise: { label: "See what Enterprise adds", onClick: () => onNavigate("enterprise") },
  };
  return (
    <div className="mk-page mk-a mk-pr">
      {/* The plan cards carry the actions, so the hero has none of its own. */}
      <Hero
        id="mk-pr-title"
        title="Start free. Talk to us when a team depends on it."
        lead="Every plan runs the same engine. Verification is never an add-on."
      >
        <h2 className="mk-sr-only">Plans</h2>
        <ol className="mk-pr-plans">
          {PLANS.map((plan) => {
            const act = actions[plan.action];
            return (
              <li
                key={plan.tier}
                className={`mk-pr-plan${plan.recommended ? " is-recommended" : ""}`}
                aria-labelledby={`mk-pr-plan-${plan.tier}`}
              >
                <div className="mk-pr-plan-head">
                  <h3 id={`mk-pr-plan-${plan.tier}`}>{plan.name}</h3>
                  {plan.recommended ? <p className="mk-pr-badge">{plan.recommended}</p> : null}
                </div>
                <p className="mk-pr-who">{plan.who}</p>
                <p className="mk-pr-price">
                  <span className="mk-pr-figure">{plan.figure}</span>
                  <span className="mk-pr-qual">{plan.qualifier}</span>
                </p>
                <p className="mk-pr-adds">{plan.adds}</p>
                <ul className="mk-a-checks">
                  {plan.highlights.map((h) => (
                    <li key={h}>
                      <StatusGlyph kind="landed" />
                      {h}
                    </li>
                  ))}
                </ul>
                <button
                  type="button"
                  className={`mk-btn mk-btn--lg mk-pr-cta ${plan.recommended ? "mk-btn--primary" : "mk-btn--secondary"}`}
                  onClick={act.onClick}
                >
                  {act.label}
                  <span className="mk-sr-only"> ({plan.name} plan)</span>
                </button>
              </li>
            );
          })}
        </ol>
      </Hero>

      <Section
        id="mk-compare-title"
        title="Compare the plans"
        lead={
          <>
            Every plan runs the whole load path.{" "}
            <PageLink to="product-transfer" onNavigate={onNavigate}>
              See how a load runs
            </PageLink>
          </>
        }
      >
        <table className="mk-a-table mk-pr-table">
          <caption className="mk-sr-only">What each plan includes</caption>
          <colgroup>
            <col className="mk-pr-col-feature" />
            <col />
            <col />
            <col />
          </colgroup>
          <thead>
            <tr>
              <th scope="col">
                <span className="mk-sr-only">Capability</span>
              </th>
              {PLANS.map((p) => (
                <th key={p.tier} scope="col" className="mk-pr-th">
                  <span className="mk-pr-th-name">{p.name}</span>
                  <span className="mk-pr-th-price">{p.tier === "starter" ? `${p.figure} ${p.qualifier}` : p.figure}</span>
                </th>
              ))}
            </tr>
          </thead>
          {PLAN_GROUPS.map((g) => (
            <tbody key={g.id}>
              <tr className="mk-a-table-group">
                <th scope="rowgroup" colSpan={4}>
                  {g.title}
                </th>
              </tr>
              {g.rows.map((row) => (
                <tr key={row.feature}>
                  <th scope="row">{row.feature}</th>
                  {TIERS.map((t) => (
                    <td key={t} className="mk-pr-cell">
                      {row.in.includes(t) ? (
                        <span className="mk-pr-yes">
                          <StatusGlyph kind="landed" />
                          <span className="mk-sr-only">Included</span>
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
      </Section>

      <Section id="mk-pricing-faq" title="Questions about pricing" lead="Early access, quotes, and where Datawrap runs." className="mk-pr-faq">
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
      </Section>

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

const ESTATES: { id: string; name: string; plan: string; rows: [string, string][] }[] = [
  {
    id: "hosted",
    name: "Hosted",
    plan: "The default, on every plan",
    rows: [
      ["Runs in", "Datawrap's account: the web app, the API, and the engine"],
      ["Install", "Nothing to install"],
      ["Data", "Rows pass through the engine; held rows stay in the job's quarantine"],
      ["Keys", "Connector secrets encrypted at rest, wrapped with your KMS key if you bring one"],
      ["Audit", "Signed entries per workspace, each linked to the one before"],
    ],
  },
  {
    id: "self",
    name: "Self-managed",
    plan: "An Enterprise plan option",
    rows: [
      ["Runs in", "Your cloud or VPC: two images, the web app and the API"],
      ["Install", "Docker Compose or Helm"],
      ["Data", "Jobs, schedules, and evidence stay in your account, in your own MongoDB"],
      ["Keys", "Connector credentials stay in your account"],
      ["Audit", "Audit events live in your own MongoDB"],
    ],
  },
];

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
  const pilot = { label: SALES_ACTION_LABEL, onClick: () => onNavigate("contact") };
  const security = { label: "Read the security overview", onClick: () => onNavigate("security") };
  return (
    <div className="mk-page mk-a mk-ent">
      <Hero
        id="mk-ent-title"
        title="Your identity provider, your keys, your region."
        lead="Run it hosted or in your own cloud. The plan, the checks, and the read-back do not change."
        actions={<Actions primary={pilot} secondary={security} />}
      >
        <SettingsShowcase initial="sso" caption="Example workspace settings. Choose a section to open it." />
      </Hero>

      <Section
        id="mk-ent-deploy"
        title="Hosted or self-managed"
        lead="The same engine either way. What changes is where data, keys, and audit live."
      >
        <Bento label="Hosted and self-managed deployment" className="mk-ent-estates">
          {ESTATES.map((e) => (
            <BentoTile key={e.id} span={6} title={e.name} body={e.plan}>
              <dl className="mk-a-dl mk-ent-estate-dl">
                {e.rows.map(([k, v]) => (
                  <div key={k}>
                    <dt>{k}</dt>
                    <dd>{v}</dd>
                  </div>
                ))}
              </dl>
            </BentoTile>
          ))}
        </Bento>
      </Section>

      <Section
        id="mk-ent-roles-title"
        title="Four roles, checked on every request"
        lead="In Transfer Studio, on schedules, in Pilot, and over MCP. An API key never carries more than the person who created it."
      >
        <table className="mk-a-table mk-ent-roles">
          <caption className="mk-sr-only">Workspace roles, least access first</caption>
          <thead>
            <tr>
              <th scope="col">Role</th>
              <th scope="col">What it can do</th>
            </tr>
          </thead>
          <tbody>
            {ROLES.map((r) => (
              <tr key={r.name}>
                <th scope="row">{r.name}</th>
                <td>{r.does}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      <Section
        id="mk-ent-controls"
        title="Workspace controls"
        lead={
          <>
            Switched on per workspace by an admin; deployment-wide settings are reserved for platform admins.{" "}
            <PageLink to="security" onNavigate={onNavigate}>
              How each control works
            </PageLink>
          </>
        }
      >
        <table className="mk-a-table mk-ent-spec">
          <caption className="mk-sr-only">Workspace controls by area</caption>
          <thead>
            <tr>
              <th scope="col">Area</th>
              <th scope="col">Control</th>
              <th scope="col">What it does</th>
            </tr>
          </thead>
          {SPEC.map((g) => (
            <tbody key={g.group}>
              {g.rows.map(([k, v], i) => (
                <tr key={k} className={i === 0 ? "is-first" : undefined}>
                  {i === 0 ? (
                    <th scope="rowgroup" rowSpan={g.rows.length} className="mk-ent-spec-group">
                      {g.group}
                    </th>
                  ) : null}
                  <th scope="row">{k}</th>
                  <td>{v}</td>
                </tr>
              ))}
            </tbody>
          ))}
        </table>
      </Section>

      <Section id="mk-ent-buy-title" title="Buying Datawrap" lead="A pilot first, then the paperwork your procurement team asks for.">
        <FactTiles
          label="Buying Datawrap"
          items={[
            {
              key: "pilot",
              title: "A pilot on your own data",
              body: "One scoped load, your source to your destination, ending in a signed proof pack you keep.",
            },
            {
              key: "dpa",
              title: "DPA and subprocessor list",
              body: (
                <>
                  We email both on request. See what the{" "}
                  <PageLink to="dpa" onNavigate={onNavigate}>
                    data processing addendum
                  </PageLink>{" "}
                  covers and the{" "}
                  <PageLink to="subprocessors" onNavigate={onNavigate}>
                    subprocessor categories
                  </PageLink>
                  .
                </>
              ),
            },
            {
              key: "terms",
              title: "Commercial terms",
              body: (
                <>
                  Agreed with each customer. Write to{" "}
                  <a className="mk-link" href="mailto:sales@datawrap.io">
                    sales@datawrap.io
                  </a>
                  .
                </>
              ),
            },
          ]}
        />
      </Section>

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

/** "SQLite and 12 more", "Snowflake and BigQuery", "Redis" */
function groupLabel(g: ProofGroup): string {
  const names = g.drivers.map((d) => d.name);
  if (names.length <= 2) return names.join(" and ");
  return `${names[0]} and ${names.length - 1} more`;
}

/** The selected CI job's evidence, as one card. */
function Certificate({ group }: { group: ProofGroup }) {
  const n = group.drivers.length;
  return (
    <article className="mk-a-card mk-proof-cert" aria-labelledby="mk-proof-cert-title">
      <header className="mk-proof-cert-head">
        <p id="mk-proof-cert-title" className="mk-proof-cert-title">
          CI evidence
        </p>
        <span className="mk-proof-state">
          <StatusGlyph kind="landed" />
          {group.emulated ? "Green on the emulator" : "Green, none skipped"}
        </span>
      </header>
      <p className="mk-proof-cert-figure">
        <span className="mk-proof-cert-num">{fmt.format(group.executed)}</span>
        <span className="mk-proof-cert-unit">
          tests executed by {group.jobs.length === 1 ? "this job" : "these jobs"}, none skipped
        </span>
      </p>
      <p className="mk-proof-cert-claim">{proofClaim(group.emulated)}.</p>
      <p className="mk-proof-cert-label">
        {n} {n === 1 ? "driver" : "drivers"} proven
      </p>
      <ul className="mk-proof-cert-drivers">
        {group.drivers.map((d) => (
          <li key={d.driver}>
            <span className="mk-proof-icon">
              <ConnectorIcon id={d.driver} size={16} />
            </span>
            {d.name}
          </li>
        ))}
      </ul>
      <dl className="mk-proof-cert-foot">
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
        <div>
          <dt>Recorded</dt>
          <dd>{EVIDENCE_AS_OF}</dd>
        </div>
      </dl>
    </article>
  );
}

/** One chip per CI job group; choosing one shows its evidence card. */
function ProofDesk() {
  const [active, setActive] = useState(0);
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);
  const group = PROOF_GROUPS[active];

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
      <div className="mk-proof-tabs" role="tablist" aria-label="CI job groups" aria-orientation="vertical" onKeyDown={onKey}>
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
            className="mk-proof-tab"
            onClick={() => setActive(i)}
          >
            <span className="mk-proof-tab-name">{groupLabel(g)}</span>
            <span className="mk-proof-tab-meta">
              {g.drivers.length} {g.drivers.length === 1 ? "driver" : "drivers"}, {fmt.format(g.executed)} tests
              {g.emulated ? ", on an emulator" : ""}
            </span>
          </button>
        ))}
      </div>
      <div role="tabpanel" id="mk-proof-panel" aria-labelledby={`mk-proof-tab-${active}`}>
        <Certificate key={active} group={group} />
      </div>
    </div>
  );
}

const LIVE_DRIVERS = ALL_CONNECTORS.filter((c) => c.proven && !c.emulated);
const EMULATED_CONNECTORS = ALL_CONNECTORS.filter((c) => c.emulated);
const IMPLEMENTED_DRIVERS = ALL_CONNECTORS.filter((c) => !c.proven);

/*
 * Proof (#/customers): Datawrap is in early access and no design partner has
 * agreed to be named, so this page carries no logos or quotes. It shows the
 * evidence a buyer can check instead, and says so in the lead.
 */
function ProofPage({ onNavigate }: Nav) {
  const pilot = { label: SALES_ACTION_LABEL, onClick: () => onNavigate("contact") };
  const connectors = { label: "See connectors", onClick: () => onNavigate("integrations") };
  return (
    <div className="mk-page mk-a mk-proof">
      <Hero
        id="mk-proof-title"
        title="Proof you can check yourself."
        lead="Datawrap is in early access, so this page shows evidence rather than logos: every connector claim is generated from a CI run, not typed by hand."
        actions={<Actions primary={pilot} secondary={connectors} />}
      >
        <ProofsShowcase caption={`The Proofs screen, filled with this page's CI evidence, recorded ${EVIDENCE_AS_OF}.`} />
      </Hero>

      <Section
        id="mk-proof-sum"
        title={`${CATALOG_TOTAL} drivers, each counted once`}
        lead="Every driver in the catalog sits in exactly one of three states, and the three add up to the catalog."
      >
        <FactTiles
          label={`${LIVE_PROVEN} + ${EMULATED_DRIVERS.length} + ${IMPLEMENTED} = ${CATALOG_TOTAL} drivers`}
          items={[
            {
              key: "live",
              figure: LIVE_PROVEN,
              title: "Proven on the live engine",
              body: "Each ran green in CI, with no skipped test.",
              ui: <DriverChips label="Some of the drivers proven on the live engine" drivers={LIVE_DRIVERS.slice(0, 5)} more={LIVE_DRIVERS.length - 5} />,
            },
            {
              key: "emulated",
              figure: EMULATED_DRIVERS.length,
              op: "+",
              title: "Proven on an emulator",
              body: "An emulator proves the load, MERGE, and read-back logic, not a production account; a pilot proves that.",
              ui: <DriverChips label="Drivers proven on an emulator" drivers={EMULATED_CONNECTORS} />,
            },
            {
              key: "implemented",
              figure: IMPLEMENTED,
              op: "+",
              title: "Implemented, proof running",
              body: "In the engine and running their CI proof. None is counted as proven yet.",
              ui: (
                <DriverChips
                  label="Some of the implemented drivers"
                  drivers={IMPLEMENTED_DRIVERS.slice(0, 3)}
                  more={IMPLEMENTED_DRIVERS.length - 3}
                />
              ),
            },
          ]}
        />
      </Section>

      <Section
        id="mk-proof-jobs"
        title="Read one CI job's evidence"
        lead={
          <>
            A CI job runs a whole suite, so the drivers it proves share its test total.{" "}
            <PageLink to="integrations" onNavigate={onNavigate}>
              Every driver, by family
            </PageLink>
          </>
        }
      >
        <ProofDesk />
      </Section>

      <RowSection
        id="mk-proof-pack"
        flip
        title="Verify a proof pack"
        lead="Every load can export a signed record of its checks and mapping. Anyone in the workspace can check it."
        copy={
          <>
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
            <dl className="mk-a-dl mk-proof-rule">
              <div>
                <dt>Full checksum after the write</dt>
                <dd>The pack may say “migration proven”.</dd>
              </div>
              <div>
                <dt>A sample after the write</dt>
                <dd>The pack exports, and refuses that claim.</dd>
              </div>
            </dl>
          </>
        }
        visual={<SignOffShowcase frame="panel" caption="Example run. Sign as each role; the pack is signed once all three have." />}
      />

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
  const lh = 17;
  const top = y + h / 2 - ((lines.length - 1) * lh) / 2 + 5;
  return (
    <g className={`mk-sec-box ${tone}`.trim()}>
      <rect x={x} y={y} width={w} height={h} rx={6} />
      {lines.map((t, i) => (
        <text key={t} x={cx} y={top + i * lh} textAnchor="middle" className={i === 0 ? "mk-sec-t mk-sec-t--b" : "mk-sec-t mk-sec-t--s"}>
          {t}
        </text>
      ))}
    </g>
  );
}

/** A boundary: one rule with its name set on the line. */
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
      <rect x={x} y={y} width={w} height={h} rx={8} className="mk-sec-frame-rule" />
      <rect x={x + 12} y={y - 10} width={knock} height={20} className="mk-sec-knock" />
      <text ref={label} x={x + 18} y={y + 5} className="mk-sec-t mk-sec-t--label">
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
          <rect x={x + i * 36} y={y} width={26} height={18} rx={3} />
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
    ["A person", 205, 88],
    ["A schedule", 315, 104],
    ["Pilot", 420, 70],
    ["An agent over MCP", 545, 150],
  ];
  return (
    <svg className="mk-sec-svg mk-sec-svg--wide" viewBox="-32 0 824 620" aria-hidden focusable="false">
      <Markers arrow={arrow} held={held} />

      {callers.map(([t, cx, w]) => (
        <g key={t} className="mk-sec-caller">
          <path d={`M${cx} 36C${cx} 50 380 42 380 52`} className="mk-sec-line mk-sec-line--thin" />
          <rect x={cx - w / 2} y={6} width={w} height={30} rx={15} />
          <text x={cx} y={26} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
            {t}
          </text>
        </g>
      ))}

      {/* The identity check sits on its own row, above the boundary it guards. */}
      <path d="M380 68V80" className="mk-sec-line mk-sec-line--thin" />
      <Frame x={130} y={80} w={500} h={470} name="Identity" note="SSO, SCIM, four roles" tone="identity" labelW={226} />
      <circle cx={380} cy={60} r={8} className="mk-sec-gate" />
      <path d="M376 60.5l3 3 5-6" className="mk-sec-gate-mark" />

      <Frame x={160} y={120} w={440} h={398} name="Workspace" note="every read scoped to it" tone="workspace" labelW={240} />

      <text x={180} y={168} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        Each connection is checked
      </text>
      <text x={180} y={185} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        where it is made
      </text>

      <path d="M120 234H278" className="mk-sec-line mk-sec-line--data" markerEnd={`url(#${arrow})`} />
      <path d="M480 234H638" className="mk-sec-line mk-sec-line--data" markerEnd={`url(#${arrow})`} />
      <Box x={-30} y={204} w={150} h={60} lines={["Your source"]} tone="is-ext" />
      <Box x={640} y={204} w={150} h={60} lines={["Your destination"]} tone="is-ext" />
      <text x={212} y={224} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        rows
      </text>
      <text x={528} y={224} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        rows
      </text>

      <Box x={280} y={196} w={200} h={76} lines={["Datawrap engine", "checks, writes,", "holds, reads back"]} tone="is-engine" />

      <path d="M320 272L296 340" className="mk-sec-line mk-sec-line--key" />
      <path d="M440 272L470 336" className="mk-sec-line mk-sec-line--held" markerEnd={`url(#${held})`} />

      <Frame x={190} y={340} w={170} h={84} name="Encryption" tone="encryption" labelW={90} />
      <text x={275} y={373} textAnchor="middle" className="mk-sec-t mk-sec-t--b">
        Connector secrets
      </text>
      <text x={275} y={391} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        encrypted at rest,
      </text>
      <text x={275} y={408} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        wrapped with your key
      </text>

      <Box x={400} y={340} w={170} h={84} lines={["Held rows", "kept with the reason,", "replayed once fixed"]} tone="is-held" />

      <path d="M275 424v18M485 424v18" className="mk-sec-line mk-sec-line--thin" markerEnd={`url(#${arrow})`} />

      <Frame x={190} y={446} w={380} h={56} name="Audit" tone="audit" labelW={52} />
      <Chain x={208} y={465} n={6} />
      <text x={424} y={471} className="mk-sec-t mk-sec-t--s">
        Signed, each linked
      </text>
      <text x={424} y={488} className="mk-sec-t mk-sec-t--s">
        to the one before
      </text>

      <path d="M380 502V562" className="mk-sec-line mk-sec-line--thin mk-sec-line--dash" markerEnd={`url(#${arrow})`} />
      <Box x={280} y={566} w={200} h={48} lines={["Can be anchored in S3", "with Object Lock"]} tone="is-anchor" />
    </svg>
  );
}

/** The same architecture, tall, for phones: rows run down into the engine and back up out. */
function ArchitectureTall({ mid }: { mid: string }) {
  const arrow = `mk-sec-tarrow-${mid}`;
  const held = `mk-sec-theld-${mid}`;
  const callers = ["A person", "A schedule", "Pilot", "An agent over MCP"];
  return (
    <svg className="mk-sec-svg mk-sec-svg--tall" viewBox="0 0 340 712" aria-hidden focusable="false">
      <Markers arrow={arrow} held={held} />

      {callers.map((t, i) => (
        <g key={t} className="mk-sec-caller">
          <path d={`M138 ${19 + i * 34}H150V152`} className="mk-sec-line mk-sec-line--thin" />
          <rect x={2} y={4 + i * 34} width={136} height={28} rx={14} />
          <text x={70} y={23 + i * 34} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
            {t}
          </text>
        </g>
      ))}

      <Box x={162} y={40} w={82} h={54} lines={["Your", "source"]} tone="is-ext" />
      <Box x={250} y={40} w={88} h={54} lines={["Your", "destination"]} tone="is-ext" />

      <Frame x={2} y={160} w={336} h={468} name="Identity" tone="identity" labelW={76} />
      <circle cx={150} cy={160} r={7} className="mk-sec-gate" />
      <path d="M146.5 160.5l2.6 2.6 4.4-5.2" className="mk-sec-gate-mark" />
      <Frame x={16} y={200} w={308} h={412} name="Workspace" tone="workspace" labelW={88} />

      <path d="M203 94V244" className="mk-sec-line mk-sec-line--data" markerEnd={`url(#${arrow})`} />
      <path d="M294 248V98" className="mk-sec-line mk-sec-line--data" markerEnd={`url(#${arrow})`} />

      <Box x={150} y={248} w={164} h={72} lines={["Datawrap engine", "checks, writes,", "holds, reads back"]} tone="is-engine" />
      <text x={28} y={262} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        Each
      </text>
      <text x={28} y={279} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        connection
      </text>
      <text x={28} y={296} className="mk-sec-t mk-sec-t--s mk-sec-t--muted">
        is checked
      </text>

      <path d="M190 320L112 360" className="mk-sec-line mk-sec-line--key" />
      <path d="M266 320V356" className="mk-sec-line mk-sec-line--held" markerEnd={`url(#${held})`} />

      <Frame x={28} y={362} w={150} h={96} name="Encryption" tone="encryption" labelW={90} />
      <text x={103} y={396} textAnchor="middle" className="mk-sec-t mk-sec-t--b">
        Secrets
      </text>
      <text x={103} y={414} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        encrypted,
      </text>
      <text x={103} y={431} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        wrapped with
      </text>
      <text x={103} y={448} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        your key
      </text>
      <Box x={188} y={362} w={124} h={96} lines={["Held rows", "kept with", "the reason"]} tone="is-held" />

      <Frame x={28} y={484} w={284} h={108} name="Audit" tone="audit" labelW={52} />
      <Chain x={63} y={506} n={6} />
      <text x={170} y={552} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        Signed, each linked
      </text>
      <text x={170} y={569} textAnchor="middle" className="mk-sec-t mk-sec-t--s">
        to the one before
      </text>

      <path d="M170 592V650" className="mk-sec-line mk-sec-line--thin mk-sec-line--dash" markerEnd={`url(#${arrow})`} />
      <Box x={60} y={654} w={220} h={54} lines={["Can be anchored in S3", "with Object Lock"]} tone="is-anchor" />
    </svg>
  );
}

/** Security hero: the architecture, and what each boundary does in words for screen readers. */
function Architecture() {
  const mid = useId().replace(/[^a-zA-Z0-9-]/g, "");
  return (
    <figure className="mk-a-card mk-sec-arch" aria-labelledby="mk-sec-arch-cap">
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

function SecurityPage({ onNavigate }: Nav) {
  const pilot = { label: SALES_ACTION_LABEL, onClick: () => onNavigate("contact") };
  const enterprise = { label: "See Enterprise controls", onClick: () => onNavigate("enterprise") };
  return (
    <div className="mk-page mk-a mk-sec">
      <Hero
        id="mk-sec-title"
        title="What protects your data while it moves."
        lead="The same boundaries hold in Transfer Studio, on schedules, in Pilot, and over MCP."
        actions={<Actions primary={pilot} secondary={enterprise} />}
      >
        <Architecture />
      </Hero>

      <Section
        id="mk-sec-every"
        title="The controls, boundary by boundary"
        lead={
          <>
            Which boundary each control sits on, and whether it is always on. The same controls hold hosted or
            self-managed;{" "}
            <PageLink to="enterprise" onNavigate={onNavigate}>
              compare the two deployments
            </PageLink>
            .
          </>
        }
      >
        <table className="mk-a-table mk-sec-ledger">
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
                  <span className="mk-a-tag">{c.boundary}</span>
                </td>
                <td className="mk-sec-td-does">{c.does}</td>
                <td className={`mk-sec-td-when${c.always ? " is-always" : ""}`}>
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
      </Section>

      <RowSection
        id="mk-sec-audit-title"
        title="An audit log that checks itself"
        lead="Remove or alter an entry and the chain no longer verifies."
        copy={
          <ul className="mk-a-bullets">
            <li>Every action is signed and linked to the hash of the one before.</li>
            <li>Verify the chain, then export it.</li>
            <li>The chain head can be anchored in S3 with Object Lock, outside the application's reach.</li>
          </ul>
        }
        visual={
          <SettingsShowcase
            initial="audit"
            sections={["audit"]}
            frame="panel"
            caption="Example audit log. Choose Verify chain; each entry links to the hash of the one before."
          />
        }
      />

      <section className="mk-a-section mk-kit-sec mk-sec-disclose" id="vulnerability-disclosure" aria-labelledby="mk-sec-disclosure">
        <div className="mk-wrap">
          <SplitHead
            id="mk-sec-disclosure"
            title="Report a vulnerability"
            lead="Email us with the subject “Security report”, and please do not open a public issue."
          />
          <div className="mk-sec-disclose-grid">
            <div className="mk-sec-disclose-copy">
              <p>We will not pursue good-faith research that:</p>
              <ul className="mk-a-bullets">
                <li>tests only accounts you own</li>
                <li>stops at the first sign of another customer’s data</li>
                <li>avoids denial of service and social engineering</li>
                <li>gives us reasonable time to fix before disclosure</li>
              </ul>
            </div>
            <dl className="mk-a-dl mk-sec-disclose-facts">
              <div>
                <dt>Email</dt>
                <dd>
                  <a className="mk-link" href="mailto:contact@datawrap.io?subject=Security%20report">
                    contact@datawrap.io
                  </a>
                </dd>
              </div>
              <div>
                <dt>Subject</dt>
                <dd>Security report</dd>
              </div>
              <div>
                <dt>Acknowledgment</dt>
                <dd>Within three business days, with updates until it is resolved</dd>
              </div>
              <div>
                <dt>Machine-readable contact</dt>
                <dd>
                  <a className="mk-link" href="/.well-known/security.txt">
                    /.well-known/security.txt
                  </a>
                </dd>
              </div>
            </dl>
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
  return (
    <figure className="mk-a-card mk-int-matrix" aria-labelledby="mk-int-matrix-cap">
      <figcaption className="mk-int-matrix-cap">
        <span id="mk-int-matrix-cap">Supported systems, by family</span>
        {/* The key draws each state the way the tiles draw it: the border,
            the tint, and the check badge in the corner. */}
        <span className="mk-int-key">
          {(["proven", "emulated", "implemented"] as const).map((state) => (
            <span key={state}>
              <i className={`mk-int-swatch is-${state}`} aria-hidden>
                {state === "implemented" ? null : (
                  <b className="mk-int-swatch-mark">
                    <StatusGlyph kind="landed" />
                  </b>
                )}
              </i>
              {state === "implemented" ? "Implemented" : proofLabel(state === "emulated")}
            </span>
          ))}
        </span>
      </figcaption>
      <ul className="mk-int-rows">
        {MARKETING_STACK.map((f) => (
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
                {f.provenCount} of {f.total} proven
              </p>
            </div>
            <ul className="mk-int-tiles" aria-label={`${f.family} drivers`}>
              {ALL_CONNECTORS.filter((c) => c.family === f.family).map((c) => {
                const state = tileState(c);
                return (
                  <li key={c.driver} className={`mk-int-tile is-${state}`}>
                    <span className="mk-int-tile-icon">
                      <ConnectorIcon id={c.driver} size={20} />
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
  const start = { label: START_ACTION_LABEL, onClick: onGetStarted };
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
    <div className="mk-page mk-a mk-int">
      <Hero
        id="mk-int-title"
        title="Connect what you run, and see what CI has proven."
        lead={`${PROVEN_DRIVERS} of ${CATALOG_TOTAL} drivers ran green in CI with no skipped test. The other ${IMPLEMENTED} are implemented and running their CI proof.`}
        actions={<Actions primary={start} secondary={docs} />}
      >
        <CoverageMatrix onPick={pickFamily} />
      </Hero>

      <Section id="mk-int-proven" title="What “proven in CI” means here" lead="A driver earns the label from a CI run, never from this page's copy.">
        <FactTiles
          label="What proven in CI means"
          items={[
            {
              key: "green",
              title: "Green, with no skipped test",
              body: "A driver counts as proven only when its CI job ran green and skipped nothing.",
              ui: (
                <div className="mk-app-surface mk-tui">
                  <table className="mk-tui-table">
                    <caption className="mk-sr-only">CI evidence by driver group (excerpt)</caption>
                    <thead>
                      <tr>
                        <th scope="col">Drivers</th>
                        <th scope="col" className="is-num">
                          Tests
                        </th>
                        <th scope="col">Result</th>
                      </tr>
                    </thead>
                    <tbody>
                      {PROOF_GROUPS.filter((g) => !g.emulated)
                        .slice(0, 2)
                        .map((g) => (
                          <tr key={g.jobs.join(",")}>
                            <th scope="row">{groupLabel(g)}</th>
                            <td className="is-num">{fmt.format(g.executed)}</td>
                            <td>
                              <span className="mk-app-pill mk-app-pill--completed">None skipped</span>
                            </td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              ),
            },
            {
              key: "emulated",
              title: "Emulators, said in words",
              body: `${EMULATED_DRIVERS.join(" and ")} run against emulators, and every label for them says so.`,
              ui: (
                <div className="mk-app-surface mk-tui">
                  <ul className="mk-a-tui-rows" aria-label="Drivers proven on an emulator">
                    {EMULATED_CONNECTORS.map((c) => (
                      <li key={c.driver}>
                        <span className="mk-app-chip">
                          <ConnectorIcon id={c.driver} size={14} />
                          {c.name}
                        </span>
                        <span className="mk-app-pill mk-app-pill--completed">On an emulator</span>
                      </li>
                    ))}
                  </ul>
                </div>
              ),
            },
            {
              key: "generated",
              title: "Generated, not typed",
              body: (
                <>
                  Every state on this page comes from the CI evidence file.{" "}
                  <PageLink to="customers" onNavigate={onNavigate}>
                    Read the evidence
                  </PageLink>
                </>
              ),
              ui: (
                <div className="mk-app-surface mk-tui">
                  <dl className="mk-a-tui-dl">
                    <div>
                      <dt>CI run</dt>
                      <dd>
                        <code>{EVIDENCE_RUN_ID}</code>
                      </dd>
                    </div>
                    <div>
                      <dt>Recorded</dt>
                      <dd>{EVIDENCE_AS_OF}</dd>
                    </div>
                    <div>
                      <dt>Drivers counted</dt>
                      <dd>
                        {PROVEN_DRIVERS} proven of {CATALOG_TOTAL}
                      </dd>
                    </div>
                  </dl>
                </div>
              ),
            },
          ]}
        />
      </Section>

      <Section
        id="mk-matrix-title"
        title={`All ${CATALOG_TOTAL} drivers, ${PROVEN_DRIVERS} proven in CI`}
        lead={`Each listed once. Evidence recorded ${EVIDENCE_AS_OF}.`}
      >
        <div className="mk-int-console">
          <div className="mk-a-chips" role="group" aria-label="Filter by family">
            {families.map((f) => (
              <button key={f} type="button" className="mk-a-chip" aria-pressed={family === f} onClick={() => setFamily(f)}>
                {f}
                <span className="mk-a-chip-n" aria-hidden>
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
          <table className="mk-a-table mk-int-table">
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
      </Section>

      <RowSection
        id="mk-conn-path"
        title="One path for every family"
        lead="Whatever the driver, a load takes the same path through the engine."
        copy={
          <dl className="mk-a-dl">
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
        }
        visual={
          <div className="mk-subhero-visual mk-int-path-panel">
            <QuarantinePanel />
          </div>
        }
      />

      <Closing
        title="Pick a source and a destination"
        body="Choose from the connectors your workspace can load today, on the same engine as production."
        primary={start}
        secondary={docs}
      />
    </div>
  );
}
