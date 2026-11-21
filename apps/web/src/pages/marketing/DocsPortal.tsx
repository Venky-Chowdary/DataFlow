import { useMemo, useState, type ReactNode } from "react";
import { DtIcon } from "../../components/DtIcon";
import {
  HELP_DOC_CATEGORIES,
  getHelpDoc,
  hashForHelpDoc,
  helpDocNeighbors,
  searchHelpDocs,
  type HelpDocFigure,
  type HelpDocId,
  type HelpDocWorkflowStep,
} from "../../lib/helpDocs";
import type { PublicRoute } from "../../lib/publicNavigation";
import { shotSize } from "../../lib/workspaceShots";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { Bento, BentoTile } from "../../components/marketing/kit/Bento";
import { SplitHead } from "../../components/marketing/kit/SplitHead";
import { SALES_ACTION_LABEL, START_ACTION_LABEL } from "./ContactSalesPage";

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
  /** The public site (not the signed-in Help): adds the closing card. */
  marketing?: boolean;
}

/** Bust browser cache when docs screenshots are recaptured at full desktop size. */
const DOCS_SHOT_V = "20261115b";
function docsShotUrl(src: string): string {
  const sep = src.includes("?") ? "&" : "?";
  return `${src}${sep}v=${DOCS_SHOT_V}`;
}


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


type GuideEntry = { id: HelpDocId; label?: string; note?: string };
/** `app` names the screens of the app the step happens on. */
type DocsStep = { name: string; line: string; app: string; guides: GuideEntry[] };

/** Guides you read once, before the first load. */
const BEFORE_FIRST_LOAD: GuideEntry[] = [
  { id: "help-installation", label: "Access your workspace", note: "Tenant URL, SSO sign-in, and the four workspace roles." },
  { id: "help-getting-started", note: "The sidebar map and the governed transfer path." },
  { id: "help-enterprise", note: "SSO, roles, your own key, and the data region, set by a workspace admin." },
];

/**
 * Every guide, grouped by the step of a load it belongs to: the same four
 * steps as the Transfer Studio page (Plan, Validate, Execute, Verify). Each
 * step names the app screens it covers, so the docs map onto the app's
 * stepper (Source, Destination, Transform, Map, Validate, Run).
 */
const DOCS_STEPS: DocsStep[] = [
  {
    name: "Plan",
    line: "Connect both systems and pair the columns.",
    app: "Source, Destination, Transform, and Map",
    guides: [{ id: "help-connectors" }, { id: "help-transfer-studio" }, { id: "help-semantic-mapping" }],
  },
  {
    name: "Validate",
    line: "Check the plan before the first write.",
    app: "Validate",
    guides: [{ id: "help-preflight-gates" }, { id: "help-contracts" }],
  },
  {
    name: "Execute",
    line: "Run it once or on a schedule.",
    app: "Run, and Schedules",
    guides: [{ id: "help-pipelines" }, { id: "help-transforms" }],
  },
  {
    name: "Verify",
    line: "Read the destination back, compare, and sign the report.",
    app: "Job space and its Trust Report",
    guides: [{ id: "help-job-theater", label: "Job space, the Trust Report, and sign-off" }, { id: "help-query" }],
  },
];

/** Other ways into the same engine, and reference. */
const AROUND_THE_LOAD: GuideEntry[] = [
  { id: "help-data-pilot" },
  { id: "help-mcp" },
  { id: "help-api" },
  { id: "help-faq" },
];

const GUIDE_COUNT = new Set(
  [...BEFORE_FIRST_LOAD, ...DOCS_STEPS.flatMap((s) => s.guides), ...AROUND_THE_LOAD].map((g) => g.id),
).size;

function GuideLink({
  entry,
  onNavigate,
  showNote = false,
}: {
  entry: GuideEntry;
  onNavigate: (route: PublicRoute) => void;
  showNote?: boolean;
}) {
  const doc = getHelpDoc(entry.id);
  return (
    <a
      className="mk-docs-guide"
      href={hashForHelpDoc(entry.id)}
      onClick={(e) => {
        e.preventDefault();
        onNavigate(entry.id);
      }}
    >
      <span className="mk-docs-guide-title">{entry.label ?? doc.title}</span>
      {showNote ? <span className="mk-docs-guide-note">{entry.note ?? doc.description}</span> : null}
      <span className="mk-docs-guide-time">{doc.readTime}</span>
    </a>
  );
}

/** Guides that sit side by side: a tile each, the title as the link. */
function GuideTiles({
  entries,
  label,
  span,
  onNavigate,
}: {
  entries: GuideEntry[];
  label: string;
  span: 3 | 4 | 6;
  onNavigate: (route: PublicRoute) => void;
}) {
  return (
    <Bento label={label} className="mk-docs-tiles">
      {entries.map((entry) => {
        const doc = getHelpDoc(entry.id);
        return (
          <BentoTile
            key={entry.id}
            span={span}
            className="mk-docs-tile"
            title={
              <a
                className="mk-docs-tile-link"
                href={hashForHelpDoc(entry.id)}
                onClick={(e) => {
                  e.preventDefault();
                  onNavigate(entry.id);
                }}
              >
                {entry.label ?? doc.title}
              </a>
            }
            body={entry.note ?? doc.description}
          >
            <p className="mk-docs-tile-time">{doc.readTime} read</p>
          </BentoTile>
        );
      })}
    </Bento>
  );
}

/** Documentation home: one search over every guide, and the guides laid out
 * along the four steps of a load. */
export function DocsPortal({ onNavigate, onGetStarted, marketing = false }: DocsPortalProps) {
  const [query, setQuery] = useState("");
  const q = query.trim();
  const hits = useMemo(() => new Set(searchHelpDocs(q).map((d) => d.id)), [q]);
  const keep = (entry: GuideEntry) => !q || hits.has(entry.id);
  const matched = q ? hits.size : GUIDE_COUNT;

  const before = BEFORE_FIRST_LOAD.filter(keep);
  const around = AROUND_THE_LOAD.filter(keep);

  return (
    <div className="mk mk-docs">
      <header className="mk-docs-hero">
        <div className="mk-wrap">
          <h1>Datawrap documentation</h1>
          <p className="mk-docs-lead">
            Operator guides for the hosted workspace, in the order a load runs. Nothing to install on operator laptops.
          </p>
          <form
            className="mk-docs-search"
            role="search"
            onSubmit={(e) => {
              e.preventDefault();
              const first = [...BEFORE_FIRST_LOAD, ...DOCS_STEPS.flatMap((s) => s.guides), ...AROUND_THE_LOAD].find(keep);
              if (q && first) onNavigate(first.id);
            }}
          >
            <span className="mk-docs-search-icon" aria-hidden="true">
              <DtIcon name="search" size={20} />
            </span>
            <label className="mk-sr-only" htmlFor="mk-docs-q">
              Search documentation
            </label>
            <input
              id="mk-docs-q"
              type="search"
              placeholder="Search the guides: Validate, schedules, SSO, MCP"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              autoComplete="off"
            />
            {q ? (
              <button type="button" className="mk-docs-search-clear" onClick={() => setQuery("")}>
                Clear
              </button>
            ) : null}
          </form>
          <p className="mk-docs-search-count" aria-live="polite">
            {q
              ? matched === 0
                ? "No guide matches that search."
                : `${matched} of ${GUIDE_COUNT} guides match. Press Enter to open the first.`
              : `${GUIDE_COUNT} guides, grouped by the four steps of a load.`}
          </p>
        </div>
      </header>

      <section className="mk-docs-map" aria-label="Guides">
        <div className="mk-wrap">
          {before.length ? (
            <section className="mk-docs-group" aria-labelledby="mk-docs-before">
              <SplitHead id="mk-docs-before" title="Before the first load" lead="Read once: how you sign in, how the app is laid out, and what an admin sets." />
              <GuideTiles entries={before} label="Before the first load" span={4} onNavigate={onNavigate} />
            </section>
          ) : null}

          <section className="mk-docs-group mk-docs-group--steps" aria-labelledby="mk-docs-map-title">
            <SplitHead
              id="mk-docs-map-title"
              title="Guides by step"
              lead="The four steps of a load, as Transfer Studio names them, with the app screens each one covers."
            />
            <ol className="mk-docs-steps" aria-labelledby="mk-docs-map-title">
              {DOCS_STEPS.map((step, i) => {
                const guides = step.guides.filter(keep);
                return (
                  <li key={step.name} className={`mk-docs-step${q && !guides.length ? " is-empty" : ""}`}>
                    <span className="mk-docs-step-num" aria-hidden="true">
                      {i + 1}
                    </span>
                    <h3>{step.name}</h3>
                    <p className="mk-docs-step-line">{step.line}</p>
                    <p className="mk-docs-step-app">In the app: {step.app}</p>
                    {guides.length ? (
                      <ul>
                        {guides.map((g) => (
                          <li key={`${g.id}-${g.label ?? ""}`}>
                            <GuideLink entry={g} onNavigate={onNavigate} />
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="mk-docs-step-none">No match in this step.</p>
                    )}
                  </li>
                );
              })}
            </ol>
          </section>

          {around.length ? (
            <section className="mk-docs-group" aria-labelledby="mk-docs-around">
              <SplitHead id="mk-docs-around" title="Other ways in, and reference" lead="Pilot, MCP, and the API reach the same engine and the same checks." />
              <GuideTiles entries={around} label="Other ways in, and reference" span={6} onNavigate={onNavigate} />
            </section>
          ) : null}
        </div>
      </section>

      {marketing ? (
        <MarketingClosing title="Follow a guide on your own data" body="Start a free workspace and run the four steps on a load of your own.">
          <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={onGetStarted}>
            {START_ACTION_LABEL}
          </button>
          <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={() => onNavigate("contact")}>
            {SALES_ACTION_LABEL}
          </button>
        </MarketingClosing>
      ) : null}
    </div>
  );
}

interface DocArticlePageProps {
  docId: HelpDocId;
  onNavigate: (route: PublicRoute) => void;
  onGetStarted: () => void;
  /** Signed-in workspace: hide the marketing Contact sales CTA. */
  hideMarketingCta?: boolean;
}

function DocsAnnotatedFigure({ figure }: { figure: HelpDocFigure }) {
  return (
    <figure className="docs-figure docs-figure--live docs-figure--annotated">
      <div className="docs-figure-live">
        <img
          src={docsShotUrl(figure.src)}
          alt={figure.alt}
          loading="lazy"
          decoding="async"
          {...shotSize(figure.src)}
          onError={(e) => {
            e.currentTarget.closest("figure")?.setAttribute("hidden", "");
          }}
        />
        {figure.markers?.map((m) => (
          <span
            key={`${m.n}-${m.label}`}
            className={`docs-shot-marker${Number.parseFloat(m.x) > 70 ? " is-flip" : ""}`}
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

export function DocArticlePage({ docId, onNavigate, onGetStarted, hideMarketingCta = false }: DocArticlePageProps) {
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
              {getHelpDoc(next).title}
            </button>
          ) : null}
        </div>
        <div className="docs-article-cta">
          <button type="button" className="mk-btn mk-btn--primary" onClick={onGetStarted}>
            Try in Transfer Studio
          </button>
          {hideMarketingCta ? null : (
            <button type="button" className="mk-btn mk-btn--secondary" onClick={() => onNavigate("contact")}>
              {SALES_ACTION_LABEL}
            </button>
          )}
        </div>
      </footer>
    </DocsSpaceShell>
  );
}
