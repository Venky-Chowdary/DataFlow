import { useEffect, useState, type ReactNode } from "react";

/** Privacy, Terms, and DPA: document pages with an on-page index. */
export function LegalPage({ kind }: { kind: "privacy" | "terms" | "dpa" }) {
  const privacy = [
    {
      h: "Who we are",
      p: "Datawrap, Inc. (“Datawrap”, “we”, “us”) provides a governed data-movement platform. This policy explains how we handle personal data when you visit datawrap.io, create a workspace, or use Transfer Studio, pipelines, Pilot, or MCP. For source and destination datasets you connect, you are the controller; Datawrap is the processor.",
    },
    {
      h: "What we process",
      p: "Account identity (name, email, role), workspace metadata, encrypted connector configurations, job logs, mapping decisions, quarantine summaries, audit events, and support correspondence. We do not sell personal data. We do not use connected source or destination records for advertising.",
    },
    {
      h: "Credentials and secrets",
      p: "Connector passwords, tokens, and keys are encrypted at rest and scoped to the workspace that stored them. Enterprise plans support customer-managed keys (BYOK) so your KMS wraps those secrets. Purpose keys stay bound to the job that needs them. Agents and MCP tools never receive raw destination passwords.",
    },
    {
      h: "Job artifacts",
      p: "Sample rows, mapping proofs, quarantine extracts, and reconcile reports stay inside your tenant boundary. Retention follows the workspace's retention setting or your agreement. You can export or delete artifacts from Settings or by written request.",
    },
    {
      h: "How we use data",
      p: "We use workspace data to operate the service, authenticate users, enforce RBAC, run transfers you start, surface quarantine and reconcile proof, provide support, and improve reliability. We do not train public models on your connected datasets.",
    },
    {
      h: "Legal bases",
      p: "Where GDPR or UK GDPR applies, we process account data to perform the contract, to meet legal obligations, and — for product analytics on the marketing site — on legitimate interests or consent where required. Processing of records inside your sources and destinations is on your instructions as controller.",
    },
    {
      h: "Sharing and subprocessors",
      p: "We share data with infrastructure providers that host the control plane, send transactional email, and (on Enterprise) your identity provider and KMS. We do not share connected datasets with other customers. Our current subprocessor list is available on request from contact@datawrap.io.",
    },
    {
      h: "International transfers",
      p: "If we transfer personal data out of the EEA, UK, or Switzerland, we use an approved mechanism such as Standard Contractual Clauses and apply the same encryption and access controls described here.",
    },
    {
      h: "Retention",
      p: "Account records last for the life of the workspace plus a short wind-down after deletion. Job logs and samples follow your retention setting. Backup copies expire on the backup cycle. SSO-managed accounts follow your identity-provider lifecycle.",
    },
    {
      h: "Your rights",
      p: "You may request access, correction, export, or deletion of workspace personal data through your admin or contact@datawrap.io. You may object to or restrict certain processing where the law allows. You may lodge a complaint with your supervisory authority.",
    },
    {
      h: "Cookies",
      p: "The marketing site uses essential cookies to keep a session and remember consent. Optional analytics cookies load only after you accept. The signed-in product uses session cookies required to stay authenticated.",
    },
    {
      h: "Security",
      p: "We encrypt data in transit (TLS) and credentials at rest, isolate tenants, and record admin, mapping, and job actions in an audit trail. A data processing addendum is available on request.",
    },
    {
      h: "Children",
      p: "The service is for organizations. We do not knowingly collect personal data from children under 16.",
    },
    {
      h: "Changes and contact",
      p: "We will post material changes on this page and update the date below. Questions: contact@datawrap.io. A data processing addendum is available on request.",
    },
  ];
  const terms = [
    {
      h: "Agreement",
      p: "These Terms of Service (“Terms”) govern access to the Datawrap platform, including Transfer Studio, connectors, schedules, Job space, Query, Pilot, and MCP. By creating a workspace or signing an order form you agree to these Terms. If your organization has a signed master agreement, that agreement controls where it conflicts.",
    },
    {
      h: "The service",
      p: "Datawrap moves data you authorize from sources to destinations with mapping, preflight, quarantine, and reconcile proof. Features vary by plan (Starter, Team, Enterprise). We may improve the product with notice when a change affects production behavior. Preview or beta features are labelled and are not warranted as generally available.",
    },
    {
      h: "Accounts and access",
      p: "You are responsible for users you invite, for SSO configuration, and for keeping credentials confidential. Workspace admins control roles for who can map, approve drift, and run production loads. You will promptly revoke access when a person leaves your organization.",
    },
    {
      h: "Acceptable use",
      p: "Use Datawrap only to move data you are authorized to access. Do not probe other tenants, attempt to bypass preflight or quarantine in production, mine the service for vulnerabilities outside a coordinated disclosure, or use the platform to violate export, privacy, or industry law.",
    },
    {
      h: "Customer data",
      p: "You retain all rights to source and destination data. You grant Datawrap a limited license to process that data solely to provide the service you request. You are the controller; Datawrap is the processor. You represent that you have a lawful basis to transfer the data you connect.",
    },
    {
      h: "Connector credentials",
      p: "You supply and control source and destination credentials. We store them encrypted and, on Enterprise, wrapped with your KMS key. You must use least-privilege accounts. Revoking a credential in the source system immediately stops new reads or writes that depend on it.",
    },
    {
      h: "Proof and cutover",
      p: "The platform provides preflight, quarantine, and checksum reports so you can accept or refuse a load. You decide when a route is ready for production. Recurring change delivery uses idempotent upserts so a retried batch cannot create duplicate business keys. Service levels, if any, are set in the order form.",
    },
    {
      h: "Intellectual property",
      p: "Datawrap and its licensors own the platform, documentation, and trademarks. You own your mappings, job artifacts, and connected data. Feedback you send may be used to improve the product without obligation.",
    },
    {
      h: "Confidentiality",
      p: "Each party will protect the other’s confidential information with reasonable care and use it only to perform under these Terms. Job samples and connector secrets are your confidential information.",
    },
    {
      h: "Warranties",
      p: "We warrant that we will provide the service with reasonable skill and care. Except as stated in an order form, THE SERVICE IS OTHERWISE PROVIDED AS IS. Preview and beta features are labelled and are not warranted as generally available.",
    },
    {
      h: "Limitation of liability",
      p: "Neither party is liable for indirect, incidental, special, or consequential damages. Except for confidentiality breaches, IP infringement, or payment obligations, each party’s aggregate liability in a twelve-month period is limited to the fees you paid for the service in that period. Use preflight and reconciliation before production cutovers. Signed enterprise agreements may set different caps.",
    },
    {
      h: "Indemnity",
      p: "You will defend Datawrap against claims arising from data you connect or from use that violates these Terms. We will defend you against claims that the unmodified platform infringes a third-party IP right, and we may replace or refund the affected service.",
    },
    {
      h: "Term and termination",
      p: "These Terms last while you have an active workspace or order. Either party may terminate for material breach not cured within 30 days. On termination we will disable access and, on request, export or delete workspace data within the retention window.",
    },
    {
      h: "Governing law",
      p: "Unless an order form says otherwise, these Terms are governed by the laws of the State of Delaware, excluding conflict-of-law rules. The courts of Delaware have exclusive jurisdiction, except that either party may seek injunctive relief in any court of competent jurisdiction.",
    },
    {
      h: "Changes and contact",
      p: "We will post material changes on this page and update the date below. Continued use after the effective date constitutes acceptance, except that enterprise customers are governed by the notice terms in their order form. Questions: contact@datawrap.io.",
    },
  ];
  const [active, setActive] = useState("");
  const blocks = kind === "privacy" ? privacy : kind === "terms" ? terms : [];
  const slug = (h: string) => h.toLowerCase().replace(/[^a-z0-9]+/g, "-");

  // The contents follow the reader: the section nearest the top is marked.
  useEffect(() => {
    if (!blocks.length || typeof IntersectionObserver === "undefined") return;
    const seen = new Map<string, boolean>();
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) seen.set(entry.target.id, entry.isIntersecting);
        const first = blocks.map((b) => slug(b.h)).find((id) => seen.get(id));
        if (first) setActive(first);
      },
      { rootMargin: "-96px 0px -55% 0px" },
    );
    for (const b of blocks) {
      const el = document.getElementById(slug(b.h));
      if (el) observer.observe(el);
    }
    return () => observer.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [kind]);

  if (kind === "dpa") {
    return (
      <div className="mk-page mk-doc mk-legal">
        <DocHeader
          title="Data processing addendum"
          lead="The processor terms that govern how Datawrap handles personal data in the workspaces and datasets you connect."
          plate={[
            ["Document", "Data processing addendum"],
            ["Availability", "On request"],
            ["Request from", <a href="mailto:contact@datawrap.io?subject=DPA%20request">contact@datawrap.io</a>],
          ]}
        />
        <section className="mk-legal-request" aria-label="Request the addendum">
          <div className="mk-wrap">
            <div className="mk-legal-request-sheet">
              <p>
                Our data processing addendum is available on request — email{" "}
                <a href="mailto:contact@datawrap.io?subject=DPA%20request">contact@datawrap.io</a>.
              </p>
              <a className="mk-btn mk-btn--primary" href="mailto:contact@datawrap.io?subject=DPA%20request">
                Request the addendum
              </a>
            </div>
          </div>
        </section>
      </div>
    );
  }

  const jump = (id: string) => {
    let reduce = false;
    try {
      reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch {
      reduce = true;
    }
    document.getElementById(id)?.scrollIntoView({ behavior: reduce ? "auto" : "smooth", block: "start" });
    setActive(id);
  };
  const title = kind === "privacy" ? "Privacy policy" : "Terms of service";

  const contents = (
    <ol>
      {blocks.map((b, i) => (
        <li key={b.h}>
          <a
            href={`#${slug(b.h)}`}
            aria-current={active === slug(b.h) ? "true" : undefined}
            onClick={(e) => {
              e.preventDefault();
              jump(slug(b.h));
            }}
          >
            <span className="mk-legal-toc-num" aria-hidden="true">{i + 1}</span>
            {b.h}
          </a>
        </li>
      ))}
    </ol>
  );

  return (
    <div className="mk-page mk-doc mk-legal">
      <DocHeader
        title={title}
        lead={
          kind === "privacy"
            ? "How Datawrap handles workspace data, credentials, and audit logs, written for security and legal review."
            : "The agreement that governs use of Transfer Studio, schedules, Pilot, and MCP. A signed master agreement controls where it conflicts."
        }
        plate={[
          ["Document", title],
          ["Last updated", "August 2026"],
          ["Sections", String(blocks.length)],
          ["Questions", <a href="mailto:contact@datawrap.io">contact@datawrap.io</a>],
        ]}
      />
      <div className="mk-wrap mk-legal-grid">
        <nav className="mk-legal-toc" aria-label="On this page">
          <details className="mk-legal-toc-fold">
            <summary>Contents, {blocks.length} sections</summary>
            {contents}
          </details>
          <div className="mk-legal-toc-rail">
            <h2>Contents</h2>
            {contents}
          </div>
        </nav>
        <article className="mk-legal-body" aria-label={title}>
          {blocks.map((b, i) => (
            <section key={b.h} id={slug(b.h)} aria-labelledby={`${slug(b.h)}-h`}>
              <h2 id={`${slug(b.h)}-h`}>
                <span className="mk-legal-num" aria-hidden="true">{i + 1}</span>
                {b.h}
              </h2>
              <p>{b.p}</p>
            </section>
          ))}
        </article>
      </div>
    </div>
  );
}

/** The document header for Privacy, Terms, DPA, and Subprocessors: the title,
 * one sentence, and a plate that states what the document is. */
export function DocHeader({
  title,
  lead,
  plate,
}: {
  title: string;
  lead: ReactNode;
  plate?: [string, ReactNode][];
}) {
  return (
    <header className="mk-legal-hero">
      <div className="mk-wrap mk-legal-hero-grid">
        <div className="mk-legal-hero-copy">
          <h1>{title}</h1>
          <p className="mk-legal-lead">{lead}</p>
        </div>
        {plate?.length ? (
          <dl className="mk-legal-plate">
            {plate.map(([k, v]) => (
              <div key={k}>
                <dt>{k}</dt>
                <dd>{v}</dd>
              </div>
            ))}
          </dl>
        ) : null}
      </div>
    </header>
  );
}
