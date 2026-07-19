import { useMemo, useState, type ReactNode } from "react";
import { DtIcon } from "../../components/DtIcon";
import { DocsShotReel } from "../../components/docs/DocsShotReel";
import {
  HELP_DOC_CATEGORIES,
  getHelpDoc,
  helpDocNeighbors,
  searchHelpDocs,
  type HelpDocId,
} from "../../lib/helpDocs";
import type { PublicRoute } from "../../lib/publicNavigation";

interface DocsPortalProps {
  onNavigate: (route: PublicRoute) => void;
  onGetStarted: () => void;
}

const SPACE_FRAMES = [
  {
    src: "/docs/screenshots/app-overview.png",
    alt: "Workspace Overview with live rows moved and connection health",
    caption: "Overview — live throughput and connection health",
  },
  {
    src: "/docs/screenshots/app-transfer-source.png",
    alt: "Transfer Studio source step with sample orders CSV",
    caption: "Transfer Studio — typed columns and sample rows",
  },
  {
    src: "/docs/screenshots/app-connectors.png",
    alt: "Connectors page with Postgres MySQL MongoDB status",
    caption: "Connectors — Test passed / failed on real systems",
  },
  {
    src: "/docs/screenshots/app-jobs.png",
    alt: "Job Theater reconcile timeline",
    caption: "Job Theater — reconcile and row fidelity",
  },
  {
    src: "/docs/screenshots/app-query.png",
    alt: "Query Playground",
    caption: "Query Playground — read-only SQL before transfer",
  },
  {
    src: "/docs/screenshots/app-pilot.png",
    alt: "Data Pilot",
    caption: "Data Pilot — natural-language triage",
  },
];

function DocsSpaceSidebar({
  activeId,
  onNavigate,
}: {
  activeId: HelpDocId | "help";
  onNavigate: (route: PublicRoute) => void;
}) {
  return (
    <aside className="docs-sidebar docs-sidebar--space" aria-label="Documentation space">
      <div className="docs-sidebar-brand">
        <DtIcon name="book" size={16} />
        <div>
          <strong>DataFlow Docs</strong>
          <span>Product space</span>
        </div>
      </div>

      <button
        type="button"
        className={`docs-sidebar-home ${activeId === "help" ? "is-active" : ""}`}
        onClick={() => onNavigate("help")}
      >
        Space home
      </button>

      {HELP_DOC_CATEGORIES.map((cat) => (
        <div key={cat.id} className="docs-sidebar-group">
          <h3>{cat.title}</h3>
          <ul>
            {cat.docs.map((id) => {
              const item = getHelpDoc(id);
              return (
                <li key={id}>
                  <button
                    type="button"
                    className={id === activeId ? "is-active" : ""}
                    onClick={() => onNavigate(id)}
                  >
                    {item.title}
                  </button>
                </li>
              );
            })}
          </ul>
        </div>
      ))}
    </aside>
  );
}

function DocsSpaceShell({
  activeId,
  onNavigate,
  children,
}: {
  activeId: HelpDocId | "help";
  onNavigate: (route: PublicRoute) => void;
  children: ReactNode;
}) {
  return (
    <div className="docs-space">
      <DocsSpaceSidebar activeId={activeId} onNavigate={onNavigate} />
      <div className="docs-space-main">{children}</div>
    </div>
  );
}

/** Confluence-style space home — left nav + one page, not a mega scroll of every article. */
export function DocsPortal({ onNavigate, onGetStarted }: DocsPortalProps) {
  const [query, setQuery] = useState("");
  const searchHits = useMemo(() => searchHelpDocs(query).slice(0, 8), [query]);

  return (
    <DocsSpaceShell activeId="help" onNavigate={onNavigate}>
      <header className="docs-space-page-head">
        <p className="docs-article-kicker">Space home</p>
        <h1>DataFlow product documentation</h1>
        <p className="docs-article-lead">
          Operations runbooks with screenshots captured inside the signed-in workspace. Pick a page
          from the left — each feature is its own Confluence-style article.
        </p>
        <form className="docs-search docs-search--inline" role="search" onSubmit={(e) => e.preventDefault()}>
          <DtIcon name="search" size={18} />
          <input
            type="search"
            placeholder="Search this space…"
            aria-label="Search documentation"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </form>
        {query.trim() && searchHits.length > 0 ? (
          <div className="docs-search-results docs-search-results--inline" role="listbox">
            {searchHits.map((doc) => (
              <button
                key={doc.id}
                type="button"
                className="docs-search-hit"
                onClick={() => {
                  setQuery("");
                  onNavigate(doc.id);
                }}
              >
                <strong>{doc.title}</strong>
                <span>{doc.description}</span>
              </button>
            ))}
          </div>
        ) : null}
      </header>

      <DocsShotReel frames={SPACE_FRAMES} className="docs-shot-reel--hero" />

      <section className="docs-space-start">
        <h2>Start here</h2>
        <p>
          Follow the same path operators use: connect systems, map schemas, pass eight preflight
          gates, write with quarantine, and prove the load in Job Theater.
        </p>
        <div className="docs-featured-actions">
          <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("help-getting-started")}>
            Introduction guide
          </button>
          <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("help-transfer-studio")}>
            Transfer Studio
          </button>
          <button type="button" className="lp-btn lp-btn--ghost" onClick={onGetStarted}>
            Open the app
          </button>
        </div>
      </section>

      <section className="docs-space-tree">
        <h2>Pages in this space</h2>
        <div className="docs-space-tree-grid">
          {HELP_DOC_CATEGORIES.map((cat) => (
            <div key={cat.id} className="docs-space-tree-group">
              <h3>{cat.title}</h3>
              <ul>
                {cat.docs.map((id) => {
                  const doc = getHelpDoc(id);
                  return (
                    <li key={id}>
                      <button type="button" onClick={() => onNavigate(id)}>
                        <DtIcon name={doc.icon as "book"} size={14} />
                        <span>
                          <strong>{doc.title}</strong>
                          <em>{doc.readTime} read</em>
                        </span>
                      </button>
                    </li>
                  );
                })}
              </ul>
            </div>
          ))}
        </div>
      </section>
    </DocsSpaceShell>
  );
}

interface DocArticlePageProps {
  docId: HelpDocId;
  onNavigate: (route: PublicRoute) => void;
  onGetStarted: () => void;
}

export function DocArticlePage({ docId, onNavigate, onGetStarted }: DocArticlePageProps) {
  const doc = getHelpDoc(docId);
  const { prev, next } = helpDocNeighbors(docId);
  const sectionFrames = doc.sections
    .filter((s) => s.figure)
    .map((s) => ({
      src: s.figure!.src,
      alt: s.figure!.alt,
      caption: s.figure!.caption,
    }));

  return (
    <DocsSpaceShell activeId={docId} onNavigate={onNavigate}>
      <nav className="docs-breadcrumb" aria-label="Breadcrumb">
        <button type="button" onClick={() => onNavigate("help")}>
          Space home
        </button>
        <span aria-hidden>/</span>
        <span>{doc.category}</span>
        <span aria-hidden>/</span>
        <span aria-current="page">{doc.title}</span>
      </nav>

      <header className="docs-article-head">
        <p className="docs-article-kicker">{doc.category}</p>
        <h1>{doc.title}</h1>
        <p className="docs-article-lead">{doc.description}</p>
        <span className="docs-article-meta">{doc.readTime} read</span>
      </header>

      {sectionFrames.length > 0 ? (
        <DocsShotReel frames={sectionFrames.length > 1 ? sectionFrames : [...sectionFrames, ...SPACE_FRAMES.slice(0, 3)]} />
      ) : (
        <DocsShotReel frames={SPACE_FRAMES.slice(0, 4)} />
      )}

      <nav className="docs-toc docs-toc--inline" aria-label="On this page">
        <h2>On this page</h2>
        <ul>
          {doc.sections.map((s) => (
            <li key={s.id}>
              <a href={`#${s.id}`}>{s.title}</a>
            </li>
          ))}
        </ul>
      </nav>

      <div className="docs-article-content">
        {doc.sections.map((section) => (
          <section key={section.id} id={section.id} className="docs-section">
            <h2>{section.title}</h2>
            <p>{section.body}</p>
            {section.figure ? (
              <figure className="docs-figure docs-figure--live">
                <div className="docs-figure-live">
                  <img src={section.figure.src} alt={section.figure.alt} loading="lazy" />
                </div>
                <figcaption>{section.figure.caption}</figcaption>
              </figure>
            ) : null}
            {section.steps ? (
              <ol className="docs-steps">
                {section.steps.map((step) => (
                  <li key={step}>{step}</li>
                ))}
              </ol>
            ) : null}
            {section.code ? (
              <pre className="docs-code">
                <code>{section.code}</code>
              </pre>
            ) : null}
            {section.tip ? (
              <aside className="docs-callout">
                <strong>Tip</strong>
                <p>{section.tip}</p>
              </aside>
            ) : null}
          </section>
        ))}
      </div>

      <footer className="docs-article-footer">
        <div className="docs-pager">
          {prev ? (
            <button type="button" className="docs-pager-prev" onClick={() => onNavigate(prev)}>
              ← {getHelpDoc(prev).title}
            </button>
          ) : (
            <span />
          )}
          {next ? (
            <button type="button" className="docs-pager-next" onClick={() => onNavigate(next)}>
              {getHelpDoc(next).title} →
            </button>
          ) : null}
        </div>
        <div className="docs-article-cta">
          <button type="button" className="lp-btn lp-btn--brand" onClick={onGetStarted}>
            Try in Transfer Studio
          </button>
          <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("contact")}>
            Contact sales
          </button>
        </div>
      </footer>
    </DocsSpaceShell>
  );
}
