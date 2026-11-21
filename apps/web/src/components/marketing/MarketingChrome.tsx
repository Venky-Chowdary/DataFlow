import { useEffect, useLayoutEffect, useRef, useState, type MouseEvent, type ReactNode } from "react";
import * as NavigationMenu from "@radix-ui/react-navigation-menu";
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

/** The two actions the chrome offers, worded the same everywhere it offers them. */
export const PRIMARY_CTA_LABEL = "Start free";
export const SALES_CTA_LABEL = "Request a pilot";

type NavMenu = "product" | "solutions";

/** Hover-intent delay before a menu opens under a mouse (Radix closes 150 ms after the pointer leaves). */
const HOVER_OPEN_DELAY_MS = 150;
/** A click this soon after a hover opened the menu keeps it open instead of toggling it shut. */
const HOVER_CLICK_GRACE_MS = 700;
/** The event a Radix Navigation Menu content listens for to close itself (navigationMenu.rootContentDismiss). */
const RADIX_CONTENT_DISMISS = "navigationMenu.rootContentDismiss";

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

const navFmt = new Intl.NumberFormat("en-US");

/** In menu order: what moves data, what proves it landed, what answers questions about it. */
export const PRODUCT_LINKS: NavEntry[] = [
  { route: "product-transfer", label: "Transfer Studio", note: "Map, check, write, and verify a load" },
  { route: "product-pipelines", label: "Schedules", note: "Recurring loads, incremental and CDC" },
  { route: "product-jobs", label: "Job space", note: "Trust Report, quarantine, and sign-off per run" },
  { route: "customers", label: "Proof", note: "CI evidence and proof packs you can verify" },
  { route: "product-query", label: "Query", note: "Read-only SQL on saved connections" },
  { route: "product-pilot", label: "Pilot", note: "Questions answered with the SQL shown" },
  { route: "product-mcp", label: "MCP server", note: "Pilot's tools for Cursor, Claude, and VS Code" },
];

/**
 * The Product menu grouped by what each part does: moves data, proves what
 * landed, or answers questions about it (Pilot and its MCP tools included).
 */
export const PRODUCT_GROUPS: { title: string; routes: PublicRoute[] }[] = [
  { title: "Move", routes: ["product-transfer", "product-pipelines"] },
  { title: "Prove", routes: ["product-jobs", "customers"] },
  { title: "Ask", routes: ["product-query", "product-pilot", "product-mcp"] },
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
  { title: "Product", links: PRODUCT_LINKS.map(({ route, label }) => ({ route, label })) },
  { title: "Solutions", links: SOLUTION_LINKS.map(({ route, label }) => ({ route, label })) },
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

const MOBILE_GROUPS: { title: string; links: NavEntry[] }[] = [
  { title: "Product", links: PRODUCT_LINKS },
  { title: "Solutions", links: SOLUTION_LINKS },
  { title: "Resources", links: TOP_LINKS },
];

/** A plain click: not a modified click that should open a new tab or window. */
function isPlainClick(e: MouseEvent<HTMLAnchorElement>): boolean {
  return e.button === 0 && !e.metaKey && !e.ctrlKey && !e.shiftKey && !e.altKey;
}

export function MarketingChrome({ route, onNavigate, onLogin, onGetStarted, children }: MarketingChromeProps) {
  const [navOpen, setNavOpen] = useState(false);
  const [menu, setMenu] = useState<NavMenu | "">("");
  const [scrolled, setScrolled] = useState(false);
  const headerRef = useRef<HTMLElement>(null);
  const toggleRef = useRef<HTMLButtonElement>(null);
  const itemRefs = useRef<Partial<Record<NavMenu, HTMLLIElement | null>>>({});
  const openedAt = useRef(0);
  const pointerType = useRef("");

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 8);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    setNavOpen(false);
    setMenu("");
    window.scrollTo({ top: 0, behavior: "instant" in window ? "instant" : "auto" } as ScrollToOptions);
  }, [route]);

  // A desktop menu closes as soon as focus moves anywhere outside it,
  // including the next item in the bar (Radix alone keeps it open there).
  // The close goes through Radix's own dismiss event so its hover-intent
  // timing resets as it does after any other close; setMenu is the fallback.
  useEffect(() => {
    if (!menu) return;
    const onFocusIn = (event: FocusEvent) => {
      const item = itemRefs.current[menu];
      const target = event.target as Node | null;
      if (!item || !target || item.contains(target)) return;
      item
        .querySelector(".mk-nav-menu")
        ?.dispatchEvent(new CustomEvent(RADIX_CONTENT_DISMISS, { bubbles: true, cancelable: true }));
      setMenu("");
    };
    document.addEventListener("focusin", onFocusIn);
    return () => document.removeEventListener("focusin", onFocusIn);
  }, [menu]);

  // Radix puts an aria-hidden, tabbable focus proxy after an open trigger; it
  // only hands focus on to the menu, so it does not need hiding from
  // assistive tech, and a focusable aria-hidden node fails axe.
  useLayoutEffect(() => {
    if (!menu) return;
    const proxy = itemRefs.current[menu]?.querySelector(":scope > button + span[aria-hidden][tabindex]");
    proxy?.removeAttribute("aria-hidden");
  }, [menu]);

  // The mobile sheet: Escape closes it and returns focus to the toggle;
  // a tap outside the header, or focus leaving it, closes it.
  useEffect(() => {
    if (!navOpen) return;
    const header = headerRef.current;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (target && header?.contains(target)) return;
      setNavOpen(false);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setNavOpen(false);
      toggleRef.current?.focus();
    };
    const onFocusIn = (event: FocusEvent) => {
      const target = event.target as Node | null;
      if (target && header && !header.contains(target)) setNavOpen(false);
    };
    document.addEventListener("pointerdown", onPointerDown, true);
    document.addEventListener("keydown", onKeyDown);
    document.addEventListener("focusin", onFocusIn);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown, true);
      document.removeEventListener("keydown", onKeyDown);
      document.removeEventListener("focusin", onFocusIn);
    };
  }, [navOpen]);

  const onMenuChange = (next: string) => {
    if (next) openedAt.current = Date.now();
    setMenu(next === "product" || next === "solutions" ? next : "");
  };

  /** Leave the chrome for another page: close everything and start reading at its content. */
  const go = (next: PublicRoute) => {
    setMenu("");
    setNavOpen(false);
    onNavigate(next);
    window.requestAnimationFrame(() => document.getElementById("mk-main")?.focus({ preventScroll: true }));
  };

  const href = (next: PublicRoute) => hashForPublicRoute(next);
  const follow = (next: PublicRoute) => (e: MouseEvent<HTMLAnchorElement>) => {
    if (!isPlainClick(e)) return;
    e.preventDefault();
    go(next);
  };
  const isHome = route === "home";
  const inkHero = !PAPER_ROUTES.has(route) && !isHelpDocRoute(route);
  const current = (r: PublicRoute) => (route === r ? "page" : undefined);
  const groupHolds = (links: NavEntry[]) => links.some((l) => l.route === route);

  const menuLink = (l: NavEntry) => (
    <NavigationMenu.Link asChild active={route === l.route}>
      <a href={href(l.route)} onClick={follow(l.route)} className="mk-mega-item">
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
    </NavigationMenu.Link>
  );

  const menuItem = (id: NavMenu, label: string, links: NavEntry[], body: ReactNode) => (
    <NavigationMenu.Item
      value={id}
      className="mk-nav-group"
      ref={(el: HTMLLIElement | null) => {
        itemRefs.current[id] = el;
      }}
    >
      <NavigationMenu.Trigger
        className={`mk-nav-link${groupHolds(links) ? " is-current" : ""}`}
        onPointerDown={(e) => {
          pointerType.current = e.pointerType;
        }}
        onKeyDown={() => {
          pointerType.current = "";
        }}
        onClick={(e) => {
          // A mouse that hovered the menu open and then clicks wants it to stay open.
          const hoverOpened = menu === id && pointerType.current === "mouse";
          if (hoverOpened && Date.now() - openedAt.current < HOVER_CLICK_GRACE_MS) e.preventDefault();
        }}
      >
        {label}
        <DtIcon name="chevron-down" size={14} chrome="product" />
      </NavigationMenu.Trigger>
      <NavigationMenu.Content className={`mk-nav-menu mk-nav-menu--${id}`}>{body}</NavigationMenu.Content>
    </NavigationMenu.Item>
  );

  const productBody = (
    <>
      <div className="mk-mega">
        {PRODUCT_GROUPS.map((g) => {
          const titleId = `mk-mega-${g.title.toLowerCase()}`;
          return (
            <div key={g.title} className="mk-mega-group">
              <p className="mk-mega-title" id={titleId}>
                {g.title}
              </p>
              <ul aria-labelledby={titleId}>
                {g.routes.map((r) => {
                  const l = PRODUCT_LINKS.find((x) => x.route === r);
                  return l ? <li key={l.route}>{menuLink(l)}</li> : null;
                })}
              </ul>
            </div>
          );
        })}
      </div>
      <div className="mk-mega-foot">
        <p className="mk-mega-foot-text">
          <span className="mk-mega-foot-label">Example run</span>
          <span className="mk-mega-run-sum">
            {navFmt.format(EXAMPLE_READ)} read = {navFmt.format(EXAMPLE_STATEMENT.landed)} landed +{" "}
            {EXAMPLE_STATEMENT.held} held + {EXAMPLE_STATEMENT.skipped} skipped
          </span>
        </p>
        <NavigationMenu.Link asChild active={route === "demos"}>
          <a className="mk-mega-foot-link" href={href("demos")} onClick={follow("demos")}>
            Watch a recorded run
          </a>
        </NavigationMenu.Link>
      </div>
    </>
  );

  const solutionsBody = (
    <>
      <ul>
        {SOLUTION_LINKS.map((l) => (
          <li key={l.route}>{menuLink(l)}</li>
        ))}
      </ul>
      <div className="mk-mega-foot">
        <p className="mk-mega-foot-text">Planning a cutover or a first warehouse load?</p>
        <NavigationMenu.Link asChild active={route === "contact"}>
          <a className="mk-mega-foot-link" href={href("contact")} onClick={follow("contact")}>
            {SALES_CTA_LABEL}
          </a>
        </NavigationMenu.Link>
      </div>
    </>
  );

  return (
    <div className={`lp mk ${isHome ? "mk-home" : "lp-subpage"}${inkHero ? " has-ink-hero" : ""}`} data-lp-route={route}>
      <a className="mk-skip" href="#mk-main">
        Skip to content
      </a>
      <header ref={headerRef} className={`mk-header${scrolled ? " is-scrolled" : ""}${navOpen ? " is-open" : ""}`}>
        <div className="mk-header-bar">
          <a className="mk-header-brand" href={href("home")} aria-label="Datawrap home" onClick={follow("home")}>
            <BrandWordmark markSize={28} title="" />
          </a>

          <NavigationMenu.Root
            className="mk-nav"
            aria-label="Main"
            value={menu}
            onValueChange={onMenuChange}
            delayDuration={HOVER_OPEN_DELAY_MS}
          >
            <NavigationMenu.List className="mk-nav-list">
              {menuItem("product", "Product", PRODUCT_LINKS, productBody)}
              {menuItem("solutions", "Solutions", SOLUTION_LINKS, solutionsBody)}
              {TOP_LINKS.map((l) => (
                <NavigationMenu.Item key={l.route}>
                  <NavigationMenu.Link asChild active={route === l.route}>
                    <a className="mk-nav-link" href={href(l.route)} onClick={follow(l.route)}>
                      {l.label}
                    </a>
                  </NavigationMenu.Link>
                </NavigationMenu.Item>
              ))}
            </NavigationMenu.List>
          </NavigationMenu.Root>

          <div className="mk-header-actions">
            <button type="button" className="mk-text-btn" onClick={onLogin}>
              Log in
            </button>
            <a className="mk-text-btn" href={href("contact")} aria-current={current("contact")} onClick={follow("contact")}>
              {SALES_CTA_LABEL}
            </a>
            <button type="button" className="mk-btn mk-btn--primary mk-btn--sm" onClick={onGetStarted}>
              {PRIMARY_CTA_LABEL}
            </button>
          </div>

          <button
            ref={toggleRef}
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

        <nav id="mk-mobile-menu" className="mk-mobile" aria-label="Main" hidden={!navOpen}>
          <div className="mk-mobile-groups">
            {MOBILE_GROUPS.map((group) => {
              const titleId = `mk-mobile-${group.title.toLowerCase()}`;
              return (
                <div key={group.title} className="mk-mobile-group">
                  <h2 id={titleId}>{group.title}</h2>
                  <ul aria-labelledby={titleId}>
                    {group.links.map((l) => (
                      <li key={l.route}>
                        <a href={href(l.route)} aria-current={current(l.route)} onClick={follow(l.route)}>
                          {l.label}
                        </a>
                      </li>
                    ))}
                  </ul>
                </div>
              );
            })}
          </div>
          <div className="mk-mobile-actions">
            <button
              type="button"
              className="mk-btn mk-btn--primary"
              onClick={() => {
                setNavOpen(false);
                onGetStarted();
              }}
            >
              {PRIMARY_CTA_LABEL}
            </button>
            <a
              className="mk-btn mk-btn--secondary"
              href={href("contact")}
              aria-current={current("contact")}
              onClick={follow("contact")}
            >
              {SALES_CTA_LABEL}
            </a>
            <button
              type="button"
              className="mk-btn mk-btn--quiet mk-mobile-login"
              onClick={() => {
                setNavOpen(false);
                onLogin();
              }}
            >
              Log in
            </button>
          </div>
        </nav>
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
            {FOOTER_COLUMNS.map((col) => {
              const titleId = `mk-footer-${col.title.toLowerCase()}`;
              return (
                <div key={col.title}>
                  <h2 id={titleId}>{col.title}</h2>
                  <ul aria-labelledby={titleId}>
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
              );
            })}
          </nav>
        </div>
        <div className="mk-footer-base">
          <p>© {new Date().getFullYear()} Datawrap</p>
          <p className="mk-footer-base-links">
            <button type="button" className="mk-text-btn mk-text-btn--small" onClick={onLogin}>
              Log in
            </button>
            <button type="button" className="mk-text-btn mk-text-btn--small" onClick={onGetStarted}>
              {PRIMARY_CTA_LABEL}
            </button>
            <a
              className="mk-text-btn mk-text-btn--small"
              href={href("contact")}
              aria-current={current("contact")}
              onClick={follow("contact")}
            >
              {SALES_CTA_LABEL}
            </a>
          </p>
        </div>
      </footer>
    </div>
  );
}
