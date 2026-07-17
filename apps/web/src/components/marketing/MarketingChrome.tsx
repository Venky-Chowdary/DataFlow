import { useEffect, useState, type ReactNode } from "react";
import { DtLogo } from "../DtLogo";
import { DtIcon } from "../DtIcon";
import type { PublicRoute } from "../../lib/publicNavigation";
import { hashForPublicRoute } from "../../lib/publicNavigation";

type NavMenu = "product" | "solutions" | "resources" | null;

export interface MarketingChromeProps {
  route: PublicRoute;
  onNavigate: (route: PublicRoute) => void;
  onLogin: () => void;
  onGetStarted: () => void;
  children: ReactNode;
}

export function MarketingChrome({ route, onNavigate, onLogin, onGetStarted, children }: MarketingChromeProps) {
  const [navOpen, setNavOpen] = useState(false);
  const [menu, setMenu] = useState<NavMenu>(null);
  const [scrolled, setScrolled] = useState(false);

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 4);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    setNavOpen(false);
    setMenu(null);
    window.scrollTo({ top: 0, behavior: "instant" in window ? "instant" : "auto" } as ScrollToOptions);
  }, [route]);

  const go = (next: PublicRoute) => {
    setMenu(null);
    setNavOpen(false);
    onNavigate(next);
  };

  const closeMenus = () => {
    setMenu(null);
    setNavOpen(false);
  };

  const link = (next: PublicRoute) => hashForPublicRoute(next);

  const isHome = route === "home";

  return (
    <div className={`lp ${isHome ? "lp-home" : "lp-subpage"}`} onMouseLeave={() => setMenu(null)}>
      <header className={`lp-nav ${scrolled ? "is-scrolled" : ""}`}>
        <div className="lp-nav-shell">
          <div className="lp-nav-pill">
          <div className="lp-nav-start">
            <a
              className="lp-nav-brand"
              href={link("home")}
              onClick={(e) => {
                e.preventDefault();
                go("home");
              }}
            >
              <DtLogo size={34} />
              <span className="lp-nav-brand-text">DataFlow</span>
            </a>
          </div>

          <nav className={`lp-nav-links ${navOpen ? "is-open" : ""}`} aria-label="Site">
            <div
              className={`lp-nav-item ${menu === "product" ? "is-open" : ""}`}
              onMouseEnter={() => setMenu("product")}
            >
              <button
                type="button"
                className="lp-nav-link"
                aria-expanded={menu === "product"}
                onClick={() => setMenu((m) => (m === "product" ? null : "product"))}
              >
                Product <DtIcon name="chevron-down" size={12} />
              </button>
              <div className="lp-nav-dropdown">
                <a href={link("product-transfer")} onClick={(e) => { e.preventDefault(); go("product-transfer"); }}>
                  <strong>Transfer Studio</strong>
                  <span>Map, preflight, and prove any→any loads</span>
                </a>
                <a href={link("product-pilot")} onClick={(e) => { e.preventDefault(); go("product-pilot"); }}>
                  <strong>Data Pilot</strong>
                  <span>Natural-language triage for transfers</span>
                </a>
                <a href={link("product-mcp")} onClick={(e) => { e.preventDefault(); go("product-mcp"); }}>
                  <strong>MCP Server</strong>
                  <span>Governed transfers from Cursor &amp; Claude</span>
                </a>
                <a href={link("integrations")} onClick={(e) => { e.preventDefault(); go("integrations"); }}>
                  <strong>Connectors</strong>
                  <span>Native drivers + SQLAlchemy generics</span>
                </a>
              </div>
            </div>

            <div
              className={`lp-nav-item ${menu === "solutions" ? "is-open" : ""}`}
              onMouseEnter={() => setMenu("solutions")}
            >
              <button
                type="button"
                className="lp-nav-link"
                aria-expanded={menu === "solutions"}
                onClick={() => setMenu((m) => (m === "solutions" ? null : "solutions"))}
              >
                Solutions <DtIcon name="chevron-down" size={12} />
              </button>
              <div className="lp-nav-dropdown">
                <a href={link("solution-migrations")} onClick={(e) => { e.preventDefault(); go("solution-migrations"); }}>
                  <strong>Migrations</strong>
                  <span>Cross-schema moves with proof</span>
                </a>
                <a href={link("solution-warehouse")} onClick={(e) => { e.preventDefault(); go("solution-warehouse"); }}>
                  <strong>Warehouse loading</strong>
                  <span>Snowflake, BigQuery, Redshift routes</span>
                </a>
                <a href={link("solution-sync")} onClick={(e) => { e.preventDefault(); go("solution-sync"); }}>
                  <strong>Recurring sync</strong>
                  <span>Incremental pipelines with quarantine</span>
                </a>
              </div>
            </div>

            <a
              href={link("customers")}
              className="lp-nav-link"
              onClick={(e) => {
                e.preventDefault();
                go("customers");
              }}
            >
              Customers
            </a>

            <div
              className={`lp-nav-item ${menu === "resources" ? "is-open" : ""}`}
              onMouseEnter={() => setMenu("resources")}
            >
              <button
                type="button"
                className="lp-nav-link"
                aria-expanded={menu === "resources"}
                onClick={() => setMenu((m) => (m === "resources" ? null : "resources"))}
              >
                Resources <DtIcon name="chevron-down" size={12} />
              </button>
              <div className="lp-nav-dropdown">
                <a href={link("help")} onClick={(e) => { e.preventDefault(); go("help"); }}>
                  <strong>Docs &amp; help</strong>
                  <span>Guides for Transfer Studio &amp; drivers</span>
                </a>
                <a href={link("enterprise")} onClick={(e) => { e.preventDefault(); go("enterprise"); }}>
                  <strong>Enterprise</strong>
                  <span>SSO, RBAC, audit, tenants</span>
                </a>
                <a href={link("security")} onClick={(e) => { e.preventDefault(); go("security"); }}>
                  <strong>Security</strong>
                  <span>Encryption, residency, governance</span>
                </a>
                <a href={link("integrations")} onClick={(e) => { e.preventDefault(); go("integrations"); }}>
                  <strong>Connector catalog</strong>
                  <span>Honest transfer-ready labels</span>
                </a>
              </div>
            </div>

            <a
              href={link("pricing")}
              className="lp-nav-link"
              onClick={(e) => {
                e.preventDefault();
                go("pricing");
              }}
            >
              Pricing
            </a>

            <div className="lp-nav-mobile-ctas">
              <button type="button" className="lp-btn lp-btn--ghost lp-btn--block" onClick={() => { closeMenus(); go("contact"); }}>
                Contact sales
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--block" onClick={() => { closeMenus(); onLogin(); }}>
                Log in
              </button>
              <button type="button" className="lp-btn lp-btn--brand lp-btn--block" onClick={() => { closeMenus(); onGetStarted(); }}>
                Get started
              </button>
            </div>
          </nav>

          <div className="lp-nav-end">
            <div className="lp-nav-actions">
              <button type="button" className="lp-btn lp-btn--ghost lp-nav-action-secondary" onClick={() => go("contact")}>
                Contact sales
              </button>
              <button type="button" className="lp-btn lp-btn--ghost lp-nav-action-login" onClick={onLogin}>
                Log in
              </button>
              <button type="button" className="lp-btn lp-btn--brand" onClick={onGetStarted}>
                Get started
              </button>
            </div>
            <button
              type="button"
              className="lp-nav-toggle"
              aria-label="Toggle menu"
              aria-expanded={navOpen}
              onClick={() => setNavOpen((o) => !o)}
            >
              <DtIcon name={navOpen ? "x" : "menu"} size={18} />
            </button>
          </div>
          </div>
        </div>
      </header>

      {children}

      <footer className="lp-footer">
        <div className="lp-footer-grid">
          <div className="lp-footer-brand">
            <strong>DataFlow</strong>
            <p>Universal data freedom — move any data, anywhere, with proof.</p>
          </div>
          <div>
            <h4>Product</h4>
            <a href={link("product-transfer")} onClick={(e) => { e.preventDefault(); go("product-transfer"); }}>Transfer Studio</a>
            <a href={link("product-pilot")} onClick={(e) => { e.preventDefault(); go("product-pilot"); }}>Data Pilot</a>
            <a href={link("product-mcp")} onClick={(e) => { e.preventDefault(); go("product-mcp"); }}>MCP Server</a>
            <a href={link("integrations")} onClick={(e) => { e.preventDefault(); go("integrations"); }}>Connectors</a>
          </div>
          <div>
            <h4>Solutions</h4>
            <a href={link("solution-migrations")} onClick={(e) => { e.preventDefault(); go("solution-migrations"); }}>Migrations</a>
            <a href={link("solution-sync")} onClick={(e) => { e.preventDefault(); go("solution-sync"); }}>Recurring sync</a>
            <a href={link("solution-warehouse")} onClick={(e) => { e.preventDefault(); go("solution-warehouse"); }}>Warehouse loading</a>
          </div>
          <div>
            <h4>Resources</h4>
            <a href={link("help")} onClick={(e) => { e.preventDefault(); go("help"); }}>Docs &amp; help</a>
            <a href={link("enterprise")} onClick={(e) => { e.preventDefault(); go("enterprise"); }}>Enterprise</a>
            <a href={link("customers")} onClick={(e) => { e.preventDefault(); go("customers"); }}>Customers</a>
            <a href={link("security")} onClick={(e) => { e.preventDefault(); go("security"); }}>Security</a>
            <a href={link("pricing")} onClick={(e) => { e.preventDefault(); go("pricing"); }}>Pricing</a>
          </div>
          <div>
            <h4>Company</h4>
            <a href={link("contact")} onClick={(e) => { e.preventDefault(); go("contact"); }}>Contact sales</a>
            <button type="button" className="lp-footer-link" onClick={onLogin}>Log in</button>
            <a href={link("privacy")} onClick={(e) => { e.preventDefault(); go("privacy"); }}>Privacy</a>
            <a href={link("terms")} onClick={(e) => { e.preventDefault(); go("terms"); }}>Terms</a>
          </div>
        </div>
        <div className="lp-footer-bottom">
          <span>© {new Date().getFullYear()} DataFlow</span>
          <span className="lp-footer-legal">
            <a href={link("privacy")} onClick={(e) => { e.preventDefault(); go("privacy"); }}>Privacy</a>
            <span aria-hidden>·</span>
            <a href={link("terms")} onClick={(e) => { e.preventDefault(); go("terms"); }}>Terms</a>
          </span>
        </div>
      </footer>
    </div>
  );
}
