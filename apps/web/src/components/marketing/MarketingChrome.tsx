import { useEffect, useRef, useState, type MouseEvent, type ReactNode } from "react";
import { BrandWordmark } from "../BrandWordmark";
import { DtIcon } from "../DtIcon";
import type { PublicRoute } from "../../lib/publicNavigation";
import { hashForPublicRoute } from "../../lib/publicNavigation";
import { isHelpDocRoute } from "../../lib/helpDocs";
import {
  ArrowLeftRight,
  Boxes,
  Building2,
  CalendarClock,
  Cpu,
  RefreshCw,
  Search,
  ShieldCheck,
  Shuffle,
  Sparkles,
  Warehouse,
} from "lucide-react";
import { EXAMPLE_READ, EXAMPLE_STATEMENT } from "./StatementHero";

/** Pages that open on a document header rather than the ink hero. */
const PAPER_ROUTES = new Set<string>(["privacy", "terms", "dpa", "subprocessors", "status", "help"]);

type NavMenu = "product" | "solutions" | null;

export interface MarketingChromeProps {
  route: PublicRoute;
  onNavigate: (route: PublicRoute) => void;
  onLogin: () => void;
  onGetStarted: () => void;
  children: ReactNode;
}

type NavEntry = { route: PublicRoute; label: string; note: string };

/** One icon per menu entry, drawn at the same size and weight. */
const NAV_ICON: Partial<Record<PublicRoute, ReactNode>> = {
  "product-transfer": <ArrowLeftRight size={18} />,
  "product-jobs": <Boxes size={18} />,
  "product-pipelines": <CalendarClock size={18} />,
  "product-query": <Search size={18} />,
  "product-pilot": <Sparkles size={18} />,
  "product-mcp": <Cpu size={18} />,
  customers: <ShieldCheck size={18} />,
  "solution-migrations": <Shuffle size={18} />,
  "solution-warehouse": <Warehouse size={18} />,
  "solution-sync": <RefreshCw size={18} />,
  enterprise: <Building2 size={18} />,
};

/** The Product menu in two groups: what moves data, and what proves it. */
const PRODUCT_GROUPS: { title: string; routes: PublicRoute[] }[] = [
  { title: "Move", routes: ["product-transfer", "product-pipelines", "product-mcp"] },
  { title: "Prove", routes: ["product-jobs", "product-query", "product-pilot", "customers"] },
];

const navFmt = new Intl.NumberFormat("en-US");

const PRODUCT_LINKS: NavEntry[] = [
  { route: "product-transfer", label: "Transfer Studio", note: "Map, check, write, and verify a load" },
  { route: "product-jobs", label: "Job space", note: "Trust Report, quarantine, and sign-off per run" },
  { route: "product-pipelines", label: "Schedules", note: "Recurring loads, incremental and CDC" },
  { route: "product-query", label: "Query", note: "Read-only SQL on saved connections" },
  { route: "product-pilot", label: "Pilot", note: "Questions answered with the SQL shown" },
  { route: "product-mcp", label: "MCP server", note: "The same governed tools for agents" },
  { route: "customers", label: "Proof", note: "CI evidence and proof packs you can verify" },
];

const SOLUTION_LINKS: NavEntry[] = [
  { route: "solution-migrations", label: "Migrations", note: "Cut over between schemas with evidence" },
  { route: "solution-warehouse", label: "Warehouse loading", note: "Snowflake, BigQuery, and Iceberg" },
  { route: "solution-sync", label: "Recurring sync", note: "CDC and incremental schedules" },
  { route: "enterprise", label: "Enterprise", note: "SSO, SCIM, roles, keys, and audit" },
];

const TOP_LINKS: NavEntry[] = [
  { route: "integrations", label: "Connectors", note: "" },
  { route: "security", label: "Security", note: "" },
  { route: "pricing", label: "Pricing", note: "" },
  { route: "help", label: "Docs", note: "" },
];

const FOOTER_COLUMNS: { title: string; links: { route: PublicRoute; label: string; note?: string }[] }[] = [
  {
    title: "Product",
    links: [
      { route: "product-transfer", label: "Transfer Studio" },
      { route: "product-jobs", label: "Job space" },
      { route: "product-pipelines", label: "Schedules" },
      { route: "product-query", label: "Query" },
      { route: "product-pilot", label: "Pilot" },
      { route: "product-mcp", label: "MCP server" },
      { route: "customers", label: "Proof" },
    ],
  },
  {
    title: "Solutions",
    links: [
      { route: "solution-migrations", label: "Migrations" },
      { route: "solution-warehouse", label: "Warehouse loading" },
      { route: "solution-sync", label: "Recurring sync" },
      { route: "enterprise", label: "Enterprise" },
    ],
  },
  {
    title: "Resources",
    links: [
      { route: "integrations", label: "Connectors" },
      { route: "security", label: "Security" },
      { route: "pricing", label: "Pricing" },
      { route: "help", label: "Docs" },
      { route: "demos", label: "Video demos" },
      { route: "status", label: "Service status" },
    ],
  },
  {
    title: "Legal",
    links: [
      { route: "privacy", label: "Privacy" },
      { route: "terms", label: "Terms" },
      { route: "dpa", label: "Data processing", note: "On request" },
      { route: "subprocessors", label: "Subprocessors", note: "On request" },
    ],
  },
];

export function MarketingChrome({ route, onNavigate, onLogin, onGetStarted, children }: MarketingChromeProps) {
  const [navOpen, setNavOpen] = useState(false);
  const [menu, setMenu] = useState<NavMenu>(null);
  const [scrolled, setScrolled] = useState(false);
  const navRef = useRef<HTMLElement>(null);

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 8);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    setNavOpen(false);
    setMenu(null);
    window.scrollTo({ top: 0, behavior: "instant" in window ? "instant" : "auto" } as ScrollToOptions);
  }, [route]);

  useEffect(() => {
    if (!menu && !navOpen) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (target && navRef.current?.contains(target)) return;
      setMenu(null);
      setNavOpen(false);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setMenu(null);
        setNavOpen(false);
      }
    };
    document.addEventListener("pointerdown", onPointerDown, true);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown, true);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [menu, navOpen]);

  const go = (next: PublicRoute) => {
    setMenu(null);
    setNavOpen(false);
    onNavigate(next);
  };

  const href = (next: PublicRoute) => hashForPublicRoute(next);
  const follow = (next: PublicRoute) => (e: MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault();
    go(next);
  };
  const isHome = route === "home";
  const inkHero = !PAPER_ROUTES.has(route) && !isHelpDocRoute(route);
  const current = (r: PublicRoute) => (route === r ? "page" : undefined);
  const groupHolds = (links: NavEntry[]) => links.some((l) => l.route === route);

  const entry = (l: NavEntry) => (
    <a href={href(l.route)} aria-current={current(l.route)} onClick={follow(l.route)} className="mk-mega-item">
      {NAV_ICON[l.route] ? (
        <span className="mk-mega-icon" aria-hidden>
          {NAV_ICON[l.route]}
        </span>
      ) : null}
      <span className="mk-mega-text">
        <strong>{l.label}</strong>
        <span>{l.note}</span>
      </span>
    </a>
  );

  const menuGroup = (id: Exclude<NavMenu, null>, label: string, links: NavEntry[]) => (
    <div className={`mk-nav-group${menu === id ? " is-open" : ""}`}>
      <button
        type="button"
        className={`mk-nav-link${groupHolds(links) ? " is-current" : ""}`}
        aria-expanded={menu === id}
        aria-controls={`mk-menu-${id}`}
        onClick={() => setMenu((m) => (m === id ? null : id))}
      >
        {label}
        <DtIcon name="chevron-down" size={14} chrome="product" />
      </button>
      <div id={`mk-menu-${id}`} className={`mk-nav-menu mk-nav-menu--${id}`} hidden={menu !== id}>
        {id === "product" ? (
          <div className="mk-mega">
            {PRODUCT_GROUPS.map((g) => (
              <div key={g.title} className="mk-mega-group">
                <p className="mk-mega-title">{g.title}</p>
                <ul>
                  {g.routes.map((r) => {
                    const l = links.find((x) => x.route === r);
                    return l ? <li key={l.route}>{entry(l)}</li> : null;
                  })}
                </ul>
              </div>
            ))}
            <a className="mk-mega-feature" href={href("demos")} onClick={follow("demos")}>
              <span className="mk-mega-feature-label">Example statement</span>
              <span className="mk-mega-feature-sum">
                {navFmt.format(EXAMPLE_READ)} read
                <br />= {navFmt.format(EXAMPLE_STATEMENT.landed)} landed
                <br />+ {EXAMPLE_STATEMENT.held} held + {EXAMPLE_STATEMENT.skipped} skipped
              </span>
              <strong>Watch a recorded run</strong>
            </a>
          </div>
        ) : (
          <ul>
            {links.map((l) => (
              <li key={l.route}>{entry(l)}</li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );

  const mobileGroups: { title: string; links: NavEntry[] }[] = [
    { title: "Product", links: PRODUCT_LINKS },
    { title: "Solutions", links: SOLUTION_LINKS },
    { title: "More", links: TOP_LINKS },
  ];

  return (
    <div className={`lp mk ${isHome ? "mk-home" : "lp-subpage"}${inkHero ? " has-ink-hero" : ""}`} data-lp-route={route}>
      <a className="mk-skip" href="#mk-main">
        Skip to content
      </a>
      <header ref={navRef} className={`mk-header${scrolled ? " is-scrolled" : ""}${navOpen ? " is-open" : ""}`}>
        <div className="mk-header-bar">
          <a className="mk-header-brand" href={href("home")} aria-label="Datawrap home" onClick={follow("home")}>
            <BrandWordmark markSize={28} title="" />
          </a>

          <nav className="mk-nav" aria-label="Main">
            {menuGroup("product", "Product", PRODUCT_LINKS)}
            {menuGroup("solutions", "Solutions", SOLUTION_LINKS)}
            {TOP_LINKS.map((l) => (
              <a key={l.route} className="mk-nav-link" href={href(l.route)} aria-current={current(l.route)} onClick={follow(l.route)}>
                {l.label}
              </a>
            ))}
          </nav>

          <div className="mk-header-actions">
            <button type="button" className="mk-text-btn" onClick={onLogin}>
              Log in
            </button>
            <a className="mk-text-btn" href={href("contact")} aria-current={current("contact")} onClick={follow("contact")}>
              Contact sales
            </a>
            <button type="button" className="mk-btn mk-btn--primary mk-btn--sm" onClick={onGetStarted}>
              Start free
            </button>
          </div>

          <button
            type="button"
            className="mk-menu-toggle"
            aria-label={navOpen ? "Close menu" : "Open menu"}
            aria-expanded={navOpen}
            aria-controls="mk-mobile-menu"
            onClick={() => setNavOpen((o) => !o)}
          >
            <DtIcon name={navOpen ? "x" : "menu"} size={20} chrome="product" />
          </button>
        </div>

        <div id="mk-mobile-menu" className="mk-mobile" hidden={!navOpen}>
          <div className="mk-mobile-groups">
            {mobileGroups.map((group) => (
              <div key={group.title} className="mk-mobile-group">
                <h2>{group.title}</h2>
                <ul>
                  {group.links.map((l) => (
                    <li key={l.route}>
                      <a href={href(l.route)} aria-current={current(l.route)} onClick={follow(l.route)}>
                        {l.label}
                      </a>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
          <div className="mk-mobile-actions">
            <button type="button" className="mk-btn mk-btn--primary" onClick={() => { setNavOpen(false); onGetStarted(); }}>
              Start free
            </button>
            <button type="button" className="mk-btn mk-btn--secondary" onClick={() => go("contact")}>
              Contact sales
            </button>
            <button type="button" className="mk-btn mk-btn--quiet" onClick={() => { setNavOpen(false); onLogin(); }}>
              Log in
            </button>
          </div>
        </div>
      </header>

      <main id="mk-main" className="lp-main" tabIndex={-1}>
        {children}
      </main>

      <footer className="mk-footer">
        <div className="mk-footer-inner">
          <div className="mk-footer-lead">
            <a className="mk-footer-brand" href={href("home")} aria-label="Datawrap home" onClick={follow("home")}>
              <BrandWordmark markSize={24} title="" size="sm" />
            </a>
            <p>
              Datawrap moves data between databases, warehouses, files, and streams, and keeps a
              record of what arrived.
            </p>
            <dl className="mk-footer-contacts">
              <div>
                <dt>Sales</dt>
                <dd>
                  <a href="mailto:sales@datawrap.io">sales@datawrap.io</a>
                </dd>
              </div>
              <div>
                <dt>Everything else</dt>
                <dd>
                  <a href="mailto:contact@datawrap.io">contact@datawrap.io</a>
                </dd>
              </div>
            </dl>
          </div>
          <nav className="mk-footer-nav" aria-label="Footer">
            {FOOTER_COLUMNS.map((col) => (
              <div key={col.title}>
                <h2>{col.title}</h2>
                <ul>
                  {col.links.map((l) => (
                    <li key={l.route}>
                      <a href={href(l.route)} aria-current={current(l.route)} onClick={follow(l.route)}>
                        {l.label}
                        {l.note ? <span className="mk-footer-tag">{l.note}</span> : null}
                      </a>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </nav>
        </div>
        <div className="mk-footer-base">
          <p>© {new Date().getFullYear()} Datawrap</p>
          <p className="mk-footer-base-links">
            <button type="button" className="mk-text-btn mk-text-btn--small" onClick={onLogin}>
              Log in
            </button>
            <button type="button" className="mk-text-btn mk-text-btn--small" onClick={onGetStarted}>
              Create a workspace
            </button>
          </p>
        </div>
      </footer>
    </div>
  );
}
