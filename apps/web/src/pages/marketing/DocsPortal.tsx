import { useMemo, useState, type ReactNode } from "react";
import { DtIcon } from "../../components/DtIcon";
import { DocsShotReel } from "../../components/docs/DocsShotReel";
import {
  HELP_DOC_CATEGORIES,
  getHelpDoc,
  helpDocNeighbors,
  searchHelpDocs,
  type HelpDocFigure,
  type HelpDocId,
  type HelpDocWorkflowStep,
} from "../../lib/helpDocs";
import {
  BACKEND_SUITE,
  EVIDENCE_AS_OF,
  NOT_PROVEN,
  PROVEN_EVIDENCE,
} from "../../lib/provenEvidence";
import type { PublicRoute } from "../../lib/publicNavigation";

/** Render product-doc emphasis: **bold** UI labels and `inline code`. */
function formatDocRichText(text: string): ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return parts.map((part, index) => {
    if (part.startsWith("**") && part.endsWith("**")) {
      return <strong key={index}>{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith("`") && part.endsWith("`")) {
      return <code key={index}>{part.slice(1, -1)}</code>;
    }
    return <span key={index}>{part}</span>;
  });
}

function DocParagraphs({ text }: { text: string }) {
  const blocks = text.split(/\n\n+/).map((b) => b.trim()).filter(Boolean);
  if (blocks.length <= 1) {
    return <p className="docs-rich-p">{formatDocRichText(text)}</p>;
  }
  return (
    <>
      {blocks.map((block, index) => (
        <p key={`${index}-${block.slice(0, 32)}`} className="docs-rich-p">
          {formatDocRichText(block)}
        </p>
      ))}
    </>
  );
}

interface DocsPortalProps {
  onNavigate: (route: PublicRoute) => void;
  onGetStarted: () => void;
}

/** Bust browser cache when docs screenshots are recaptured at full desktop size. */
const DOCS_SHOT_V = "20260804f";
function docsShotUrl(src: string): string {
  const sep = src.includes("?") ? "&" : "?";
  return `${src}${sep}v=${DOCS_SHOT_V}`;
}

const SPACE_FRAMES = [
  {
    src: docsShotUrl("/docs/screenshots/app-overview.png"),
    alt: "Workspace Overview with sidebar and health cards",
    caption: "1 · Overview — Platform / Operations / System sidebar and workspace health",
  },
  {
    src: docsShotUrl("/docs/screenshots/app-connectors-wizard.png"),
    alt: "Choose a connector dialog on Connectors",
    caption: "2 · Connectors — New connection opens Choose a connector",
  },
  {
    src: docsShotUrl("/docs/screenshots/app-transfer-source.png"),
    alt: "Transfer Studio source step with sample orders CSV",
    caption: "3 · Transfer Studio — Source with sample-orders.csv profiled",
  },
  {
    src: docsShotUrl("/docs/screenshots/app-transfer-map.png"),
    alt: "Transfer Studio Map columns",
    caption: "4 · Map — column edges, confidence, Accept risk",
  },
  {
    src: docsShotUrl("/docs/screenshots/app-transfer-validate.png"),
    alt: "Transfer Studio Validate gates",
    caption: "5 · Validate — gate dashboard before Execute unlocks",
  },
  {
    src: docsShotUrl("/docs/screenshots/app-pipelines-create.png"),
    alt: "Create recurring sync pipeline form",
    caption: "6 · Pipelines — Create recurring sync form (inline)",
  },
  {
    src: docsShotUrl("/docs/screenshots/app-pipelines-drawer.png"),
    alt: "Pipeline detail drawer with Run now",
    caption: "7 · Pipelines — right drawer Overview / Run now / Edit",
  },
  {
    src: docsShotUrl("/docs/screenshots/app-jobs.png"),
    alt: "Job Theater reconcile view",
    caption: "8 · Jobs — Job Theater proof for every transfer and pipeline tick",
  },
  {
    src: docsShotUrl("/docs/screenshots/app-mcp.png"),
    alt: "MCP Server page",
    caption: "9 · MCP — Cursor / Claude / VS Code governed agent tools",
  },
  {
    src: docsShotUrl("/docs/screenshots/app-pilot.png"),
    alt: "Datawrap Pilot",
    caption: "10 · Pilot — natural-language triage on the same engine",
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
          <strong>Product resources</strong>
          <span>Enterprise operator guides</span>
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
  const activeLabel = activeId === "help" ? "Space home" : getHelpDoc(activeId).title;

  return (
    <div className="docs-space">
      <div className="docs-space-nav-mobile">
        <details>
          <summary>
            <DtIcon name="book" size={16} />
            <span>Browse docs</span>
            <em>{activeLabel}</em>
          </summary>
          <DocsSpaceSidebar activeId={activeId} onNavigate={onNavigate} />
        </details>
      </div>
      <div className="docs-space-nav-desktop">
        <DocsSpaceSidebar activeId={activeId} onNavigate={onNavigate} />
      </div>
      <div className="docs-space-main">{children}</div>
    </div>
  );
}

const CATEGORY_SHOTS: Record<string, string> = {
  start: docsShotUrl("/docs/screenshots/app-overview.png"),
  transfer: docsShotUrl("/docs/screenshots/app-transfer-validate.png"),
  connect: docsShotUrl("/docs/screenshots/app-pipelines-create.png"),
  ai: docsShotUrl("/docs/screenshots/app-mcp.png"),
  ops: docsShotUrl("/docs/screenshots/app-jobs.png"),
  enterprise: docsShotUrl("/docs/screenshots/app-settings-sso.png"),
};

/** Confluence-style space home — left nav + product-first start page. */
export function DocsPortal({ onNavigate, onGetStarted }: DocsPortalProps) {
  const [query, setQuery] = useState("");
  const searchHits = useMemo(() => searchHelpDocs(query).slice(0, 8), [query]);

  return (
    <DocsSpaceShell activeId="help" onNavigate={onNavigate}>
      <header className="docs-space-page-head">
        <p className="docs-article-kicker">Enterprise product resources</p>
        <h1>Operate Datawrap in your tenant</h1>
        <p className="docs-article-lead">
          Confluence-style operator guides with real workspace screenshots. Follow the same path
          your team uses after SSO sign-in: connect systems, map schemas, pass Validate, schedule
          Pipelines, and prove every load in Job Theater. Nothing to install on operator laptops.
        </p>
        <form className="docs-search docs-search--inline" role="search" onSubmit={(e) => e.preventDefault()}>
          <DtIcon name="search" size={18} />
          <input
            type="search"
            placeholder="Search guides — e.g. Validate, Pipelines, SSO, MCP…"
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
        <h2>Start here (recommended order)</h2>
        <p>
          Work through these four guides first. Each one pins real UI controls — sidebar labels,
          buttons, and dialogs — so operators can follow along inside the signed-in product.
        </p>
        <div className="docs-featured-grid" aria-label="Start here guides">
          <button type="button" className="docs-featured-card" onClick={() => onNavigate("help-installation")}>
            <span>01</span>
            <strong>Access your workspace</strong>
            <em>Tenant URL, SSO sign-in, Viewer / Editor / Owner roles, Owner checklist.</em>
          </button>
          <button type="button" className="docs-featured-card" onClick={() => onNavigate("help-getting-started")}>
            <span>02</span>
            <strong>How Datawrap works</strong>
            <em>Sidebar map (Platform / Operations / System) and the governed transfer path.</em>
          </button>
          <button type="button" className="docs-featured-card" onClick={() => onNavigate("help-connectors")}>
            <span>03</span>
            <strong>Add & manage connectors</strong>
            <em>New connection → Choose a connector dialog → Test passed → use in Studio.</em>
          </button>
          <button type="button" className="docs-featured-card" onClick={() => onNavigate("help-transfer-studio")}>
            <span>04</span>
            <strong>Transfer Studio guide</strong>
            <em>Full sample-orders example: Source → Destination → Map → Validate → Run.</em>
          </button>
        </div>
      </section>

      <section className="docs-space-start" aria-label="Browse by category">
        <h2>Browse by category</h2>
        <p>
          Each card opens the first guide in that section. Use the left sidebar for every article —
          Pipelines create form, Contracts, Transforms, MCP, Settings / SSO, and more.
        </p>
        <div className="docs-category-grid">
          {HELP_DOC_CATEGORIES.map((cat) => {
            const first = getHelpDoc(cat.docs[0]);
            const shot = CATEGORY_SHOTS[cat.id] ?? SPACE_FRAMES[0].src;
            return (
              <button
                key={cat.id}
                type="button"
                className="docs-category-card"
                onClick={() => onNavigate(cat.docs[0])}
              >
                <img src={shot} alt="" loading="lazy" />
                <span className="docs-category-card-body">
                  <strong>{cat.title}</strong>
                  <span>{first.title} · {cat.docs.length} guides</span>
                  <em className="docs-category-card-desc">{cat.description}</em>
                </span>
              </button>
            );
          })}
        </div>
      </section>

      <section className="docs-space-algorithm" aria-label="Governed path overview">
        <h2>The governed path (every transfer & pipeline tick)</h2>
        <ol className="docs-space-algorithm-steps">
          <li>
            <strong>1 · Map</strong>
            <span>Align source → destination columns. Accept risk for intentional casts before Validate.</span>
          </li>
          <li>
            <strong>2 · Validate</strong>
            <span>Run preflight gates. Execute stays locked until the API decision is approve.</span>
          </li>
          <li>
            <strong>3 · Write + quarantine</strong>
            <span>Clean rows land; bad cells isolate with column, value, and reason — never silently dropped.</span>
          </li>
          <li>
            <strong>4 · Reconcile in Jobs</strong>
            <span>Row counts and checksums prove the load. Pipeline ticks use this same Job Theater path.</span>
          </li>
        </ol>
      </section>

      <section className="docs-space-algorithm" aria-label="Measured evidence">
        <h2>What is proven, and what is not ({EVIDENCE_AS_OF})</h2>
        <p className="docs-space-evidence-lead">
          Each line is a live matrix run through the product path against a real engine, with the
          destination re-read afterwards. Backend suite: {BACKEND_SUITE.passed.toLocaleString()}{" "}
          passed, {BACKEND_SUITE.failed} failed, {BACKEND_SUITE.skipped.toLocaleString()} skipped.
        </p>
        <ul className="docs-space-evidence-list">
          {PROVEN_EVIDENCE.map((row) => (
            <li key={row.artifact}>
              <strong>{row.claim}</strong>
              <span>
                {row.engines} · {row.cases} cases · {row.result}
              </span>
            </li>
          ))}
        </ul>
        <p className="docs-space-evidence-lead">Not proven yet — do not plan around these:</p>
        <ul className="docs-space-evidence-list docs-space-evidence-list--gaps">
          {NOT_PROVEN.map((gap) => (
            <li key={gap.area}>
              <strong>{gap.area}</strong>
              <span>{gap.reason}</span>
            </li>
          ))}
        </ul>
      </section>
    </DocsSpaceShell>
  );
}

interface DocArticlePageProps {
  docId: HelpDocId;
  onNavigate: (route: PublicRoute) => void;
  onGetStarted: () => void;
}

function DocsAnnotatedFigure({ figure }: { figure: HelpDocFigure }) {
  return (
    <figure className="docs-figure docs-figure--live docs-figure--annotated">
      <div className="docs-figure-live">
        <img src={docsShotUrl(figure.src)} alt={figure.alt} loading="lazy" />
        {figure.markers?.map((m) => (
          <span
            key={`${m.n}-${m.label}`}
            className="docs-shot-marker"
            style={{ left: m.x, top: m.y }}
            title={m.label}
          >
            <em>{m.n}</em>
            <span>{m.label}</span>
          </span>
        ))}
      </div>
      <figcaption>{figure.caption}</figcaption>
    </figure>
  );
}

function DocsWorkflow({ steps }: { steps: HelpDocWorkflowStep[] }) {
  return (
    <ol className="docs-workflow">
      {steps.map((step, index) => (
        <li key={`${step.title}-${index}`} className="docs-workflow-step">
          <div className="docs-workflow-index" aria-hidden>
            <strong>{index + 1}</strong>
            {index < steps.length - 1 ? <i className="docs-workflow-arrow" /> : null}
          </div>
          <div className="docs-workflow-body">
            <h3>{step.title}</h3>
            {step.pin ? <p className="docs-workflow-pin">{step.pin}</p> : null}
            <DocParagraphs text={step.body} />
          </div>
          {step.figure ? (
            <div className="docs-workflow-figure">
              <DocsAnnotatedFigure figure={step.figure} />
            </div>
          ) : null}
          {step.tip ? (
            <aside className="docs-callout docs-callout--tip docs-workflow-tip">
              <strong>Tip</strong>
              <DocParagraphs text={step.tip} />
            </aside>
          ) : null}
        </li>
      ))}
    </ol>
  );
}

export function DocArticlePage({ docId, onNavigate, onGetStarted }: DocArticlePageProps) {
  const doc = getHelpDoc(docId);
  const { prev, next } = helpDocNeighbors(docId);
  const hasWorkflow = doc.sections.some((s) => s.workflow && s.workflow.length > 0);

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
        <div className="docs-article-meta-row">
          <span className="docs-article-meta">{doc.readTime} read</span>
          {hasWorkflow ? <span className="docs-article-badge">Procedure guide</span> : null}
        </div>
      </header>

      <div className="docs-article-layout docs-article-layout--full">
        <div className="docs-article-content">
          {doc.sections.map((section) => (
            <section
              key={section.id}
              id={section.id}
              className={`docs-section${section.procedure ? " docs-section--procedure" : ""}`}
            >
              <h2>{section.title}</h2>
              <DocParagraphs text={section.body} />
              {section.figure ? <DocsAnnotatedFigure figure={section.figure} /> : null}
              {section.workflow ? <DocsWorkflow steps={section.workflow} /> : null}
              {section.steps ? (
                <ol className="docs-steps">
                  {section.steps.map((step) => (
                    <li key={step}>{formatDocRichText(step)}</li>
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
                  <DocParagraphs text={section.tip} />
                </aside>
              ) : null}
            </section>
          ))}
        </div>
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
          <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("contact")}>
            Contact sales
          </button>
        </div>
      </footer>
    </DocsSpaceShell>
  );
}
