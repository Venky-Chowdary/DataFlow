import { useEffect, useRef, useState, type FormEvent } from "react";
import { ShieldCheck } from "lucide-react";
import { DtIcon } from "../../components/DtIcon";
import { TurnstileWidget } from "../../components/TurnstileWidget";
import { API_BASE } from "../../lib/types";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { MarketingFaq } from "../../components/marketing/MarketingFaq";
import { Bento, BentoTile } from "../../components/marketing/kit/Bento";
import { SplitHead } from "../../components/marketing/kit/SplitHead";
import { catalogHonestyLead, cdcSourcesPhrase } from "../../lib/provenEvidence";
import type { PublicRoute } from "../../lib/publicNavigation";
import { AUDIT_EXAMPLE } from "./MarketingSubpageScenes";

/**
 * One label per action across the subpages, taken from the site header
 * (MarketingChrome): the primary action starts a free workspace, the sales
 * action opens this page's form.
 */
export {
  PRIMARY_CTA_LABEL as START_ACTION_LABEL,
  SALES_CTA_LABEL as SALES_ACTION_LABEL,
} from "../../components/marketing/MarketingChrome";
import { PRIMARY_CTA_LABEL as START_ACTION_LABEL } from "../../components/marketing/MarketingChrome";

const SOURCES = [
  "PostgreSQL",
  "MySQL",
  "SQL Server",
  "Oracle",
  "MongoDB",
  "Salesforce",
  "S3",
  "Kafka",
  "Other",
] as const;

const DESTINATIONS = [
  "Snowflake",
  "BigQuery",
  "Iceberg / S3",
  "PostgreSQL",
  "SQL Server",
  "Oracle",
  "Other",
] as const;

const NEXT_STEPS = [
  {
    title: "We reply",
    body: "Usually within one business day, with a few questions about your source, destination, and constraints.",
  },
  {
    title: "A call about your stack",
    body: "Your source and destination, the sync mode, and what your security team will ask.",
  },
  {
    title: "A scoped pilot",
    body: "One load on your own data, on the production engine, ending with a signed proof pack you keep.",
  },
];

const FAQS = [
  {
    q: "Is the pilot the same engine as production?",
    a: "Yes. A pilot runs the same plan, checks, quarantine, and read-back as every load in Transfer Studio, on schedules, in Pilot, and over MCP.",
  },
  {
    q: "Which connectors can we use?",
    a: catalogHonestyLead(),
  },
  {
    q: "Which databases can we capture changes from?",
    a: `Change data capture is proven in CI on ${cdcSourcesPhrase()}. Each reads the database log, starts from a consistent snapshot, resumes from the log position, and carries deletes through.`,
  },
  {
    q: "What can we give our security review?",
    a: "The Security page, an export of the workspace's tamper-evident audit log, and, on request, our data processing addendum and subprocessor list.",
  },
];

function ChipGrid({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: readonly string[];
  value: string[];
  onChange: (next: string[]) => void;
}) {
  return (
    <fieldset className="mk-ct-f-chips">
      <legend>{label}</legend>
      <div className="mk-ct-f-chip-row">
        {options.map((opt) => {
          const on = value.includes(opt);
          return (
            <button
              key={opt}
              type="button"
              className={`mk-ct-f-chip${on ? " is-on" : ""}`}
              aria-pressed={on}
              onClick={() =>
                onChange(on ? value.filter((v) => v !== opt) : [...value, opt])
              }
            >
              {opt}
            </button>
          );
        })}
      </div>
    </fieldset>
  );
}

/** What the visitor is told when the lead was not accepted. Every branch
 * names the direct address, so a refused form never strands a prospect. */
export function contactSalesError(status: number, data: { error?: string } | null): string {
  const code = data?.error ?? "";
  if (code === "rate_limited" || status === 429) {
    return "Too many submissions — please email sales@datawrap.io directly.";
  }
  if (code === "turnstile_required") {
    return "We couldn't verify this browser. Complete the check and send again, or email sales@datawrap.io.";
  }
  if (status === 422 && code) {
    return `Check the form: ${code}.`;
  }
  return "We could not send your request. Please email sales@datawrap.io directly.";
}

/** What the visitor is told after the API answered (D103). The lead is
 *  stored before any mail, so "ok" with ``queued: false`` means it was kept
 *  but the sales team was not alerted: promising a reply would be untrue. */
export type ContactSalesOutcome = "received" | "saved_unsent" | "error";

export function contactSalesOutcome(status: number, data: { ok?: boolean; queued?: boolean } | null): ContactSalesOutcome {
  if (status < 200 || status >= 300 || !data?.ok) return "error";
  return data.queued === false ? "saved_unsent" : "received";
}

interface ContactSalesConfig {
  siteKey: string;
  formToken: string;
}

/** Turnstile site key the API enforces + the signed one-use form token. */
async function fetchContactSalesConfig(): Promise<ContactSalesConfig> {
  try {
    const res = await fetch(`${API_BASE}/contact-sales/config`);
    if (!res.ok) return { siteKey: "", formToken: "" };
    const data = (await res.json()) as {
      turnstile_site_key?: unknown;
      form_token?: unknown;
    };
    return {
      siteKey: typeof data.turnstile_site_key === "string" ? data.turnstile_site_key : "",
      formToken: typeof data.form_token === "string" ? data.form_token : "",
    };
  } catch {
    return { siteKey: "", formToken: "" };
  }
}

/** The four required fields, in form order, as the visitor reads them. */
const REQUIRED_FIELDS = [
  { key: "firstName", label: "first name" },
  { key: "lastName", label: "last name" },
  { key: "email", label: "work email" },
  { key: "company", label: "company" },
] as const;

/** "first name, work email, and company" */
function listPhrase(items: string[]): string {
  if (items.length <= 1) return items.join("");
  if (items.length === 2) return items.join(" and ");
  return `${items.slice(0, -1).join(", ")}, and ${items[items.length - 1]}`;
}

/** The proof pack calls a pilot leaves you with (as on the Proof page). */
const PACK_CALLS: [string, string][] = [
  ["Download the pack", "GET /api/v1/transfer/{job_id}/proof-pack"],
  ["Check its signature", "POST /api/v1/transfer/proof-pack/verify"],
];

/** An API path that may wrap on a phone, but only after a slash. */
function breakAtSlashes(path: string) {
  return path.split("/").map((part, i, all) => (
    <span key={i}>
      {part}
      {i < all.length - 1 ? (
        <>
          /<wbr />
        </>
      ) : null}
    </span>
  ));
}

/** Design partner program: what partners get and what we ask. No partner is named. */
const PARTNER_TERMS = [
  {
    key: "get",
    title: "What partners get",
    items: [
      "Early access to new connectors and features",
      "A direct line to the engineers who build Datawrap",
      "Influence on the roadmap",
    ],
  },
  {
    key: "ask",
    title: "What we ask",
    items: ["A real migration or sync to run on Datawrap", "Regular, candid feedback on what works and what does not"],
  },
];

export function ContactSalesPage({
  onNavigate,
  onGetStarted,
}: {
  onNavigate: (route: PublicRoute) => void;
  onGetStarted?: () => void;
}) {
  const [outcome, setOutcome] = useState<"" | "received" | "saved_unsent">("");
  const sent = outcome !== "";
  const [sources, setSources] = useState<string[]>([]);
  const [destinations, setDestinations] = useState<string[]>([]);
  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [email, setEmail] = useState("");
  const [company, setCompany] = useState("");
  const [role, setRole] = useState("");
  const [volume, setVolume] = useState("");
  const [region, setRegion] = useState("");
  const [timeframe, setTimeframe] = useState("");
  const [phone, setPhone] = useState("");
  const [message, setMessage] = useState("");
  const [honeypot, setHoneypot] = useState("");

  const [turnstileSiteKey, setTurnstileSiteKey] = useState("");
  const [turnstileToken, setTurnstileToken] = useState("");
  const formTokenRef = useRef("");
  useEffect(() => {
    void fetchContactSalesConfig().then((cfg) => {
      setTurnstileSiteKey(cfg.siteKey);
      formTokenRef.current = cfg.formToken;
    });
  }, []);

  const fieldValues = { firstName, lastName, email, company };
  const missing = REQUIRED_FIELDS.filter((f) => !fieldValues[f.key].trim());
  const canSubmit = missing.length === 0 && (!turnstileSiteKey || Boolean(turnstileToken));
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [showMissing, setShowMissing] = useState(false);
  const formRef = useRef<HTMLFormElement>(null);

  const postLead = async () =>
    fetch(`${API_BASE}/contact-sales`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        first_name: firstName.trim(),
        last_name: lastName.trim(),
        email: email.trim(),
        company: company.trim(),
        role: role.trim(),
        phone: phone.trim(),
        message: message.trim().slice(0, 1200),
        sources,
        destinations,
        volume,
        region,
        timeframe,
        website: honeypot,
        turnstile_token: turnstileToken || undefined,
        form_token: formTokenRef.current || undefined,
      }),
    });

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (honeypot.trim()) {
      setOutcome("received");
      return;
    }
    if (submitting) return;
    // The button stays enabled; an incomplete form says what is missing and
    // moves focus to the first empty field.
    if (missing.length) {
      setShowMissing(true);
      setError(`Add your ${listPhrase(missing.map((f) => f.label))}.`);
      formRef.current?.querySelector<HTMLInputElement>(`[name="${missing[0].key}"]`)?.focus();
      return;
    }
    if (!canSubmit) {
      setError("Complete the check above and send again, or email sales@datawrap.io.");
      return;
    }
    setShowMissing(false);
    setSubmitting(true);
    setError("");
    try {
      let res = await postLead();
      let data = (await res.json().catch(() => null)) as { ok?: boolean; queued?: boolean; error?: string } | null;
      // A stale page (>1h) carries an expired/used token: refetch config and
      // retry once — the visitor never sees it.
      if (res.status === 400 && data?.error === "form_expired") {
        const cfg = await fetchContactSalesConfig();
        setTurnstileSiteKey(cfg.siteKey);
        formTokenRef.current = cfg.formToken;
        res = await postLead();
        data = (await res.json().catch(() => null)) as { ok?: boolean; queued?: boolean; error?: string } | null;
      }
      const result = contactSalesOutcome(res.status, data);
      if (result === "error") {
        setError(contactSalesError(res.status, data));
        return;
      }
      setOutcome(result);
    } catch {
      setError("We could not reach the server. Please email sales@datawrap.io directly.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="mk-page mk-contact-page mk-ct-page">
      <section className="mk-ct" aria-labelledby="mk-ct-title">
        <div className="mk-wrap mk-ct-grid">
          <div className="mk-ct-intro">
            <h1 id="mk-ct-title">Tell us what you need to move.</h1>
            <p className="mk-ct-lead">
              A migration, a recurring sync, or a pilot on your own data.
            </p>
            <h2 className="mk-ct-next-title" id="mk-ct-next">What happens next</h2>
            <ol className="mk-ct-next" aria-labelledby="mk-ct-next">
              {NEXT_STEPS.map((s, i) => (
                <li key={s.title}>
                  <span className="mk-ct-next-num" aria-hidden="true">{i + 1}</span>
                  <div>
                    <h3>{s.title}</h3>
                    <p>{s.body}</p>
                  </div>
                </li>
              ))}
            </ol>
          </div>

          <div className="mk-ct-side">
          <div className="mk-ct-card">
          {sent ? (
            <div className="mk-ct-f-success" role="status">
              <div className="mk-ct-f-success-mark" aria-hidden>
                <DtIcon name={outcome === "saved_unsent" ? "alert" : "check"} size={20} />
              </div>
              {outcome === "saved_unsent" ? (
                <>
                  <h2>Request saved</h2>
                  <p>We saved your request, but couldn&apos;t alert the sales team automatically.</p>
                  <p>
                    Email{" "}
                    <a href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">sales@datawrap.io</a>{" "}
                    so a solutions engineer sees it today.
                  </p>
                </>
              ) : (
                <>
                  <h2>Request received</h2>
                  <p>
                    We usually reply within one business day
                    {sources.length ? ` about ${sources.slice(0, 2).join(", ")}` : ""}
                    {destinations.length ? ` into ${destinations.slice(0, 2).join(", ")}` : ""}.
                  </p>
                  <p>
                    Or write now:{" "}
                    <a href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">sales@datawrap.io</a>
                  </p>
                </>
              )}
              <div className="mk-ct-f-success-actions">
                <button type="button" className="mk-btn mk-btn--primary" onClick={() => onNavigate("help")}>
                  Browse docs
                </button>
                <button type="button" className="mk-btn mk-btn--secondary" onClick={() => onNavigate("product-transfer")}>
                  Transfer Studio
                </button>
              </div>
            </div>
          ) : (
            <form className="mk-ct-f-form" onSubmit={submit} noValidate ref={formRef}>
              <header className="mk-ct-f-form-head">
                <h2>Tell us about the load</h2>
                <p>Name, work email, and company. Stack is optional.</p>
              </header>

              <div className="mk-ct-f-fields">
                <label>
                  First name
                  <input
                    className="mk-ct-f-input"
                    name="firstName"
                    value={firstName}
                    onChange={(e) => setFirstName(e.target.value)}
                    required
                    aria-invalid={showMissing && !firstName.trim() ? true : undefined}
                    autoComplete="given-name"
                  />
                </label>
                <label>
                  Last name
                  <input
                    className="mk-ct-f-input"
                    name="lastName"
                    value={lastName}
                    onChange={(e) => setLastName(e.target.value)}
                    required
                    aria-invalid={showMissing && !lastName.trim() ? true : undefined}
                    autoComplete="family-name"
                  />
                </label>
                <label>
                  Work email
                  <input
                    className="mk-ct-f-input"
                    name="email"
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    required
                    aria-invalid={showMissing && !email.trim() ? true : undefined}
                    autoComplete="email"
                    placeholder="name@company.com"
                  />
                </label>
                <label>
                  Company
                  <input
                    className="mk-ct-f-input"
                    name="company"
                    value={company}
                    onChange={(e) => setCompany(e.target.value)}
                    required
                    aria-invalid={showMissing && !company.trim() ? true : undefined}
                    autoComplete="organization"
                  />
                </label>
              </div>

              <details className="mk-ct-f-more">
                <summary>Role, phone, and stack <em>optional</em></summary>
                <div className="mk-ct-f-fields">
                  <label>
                    Role
                    <select
                      className="mk-ct-f-input"
                      value={role}
                      onChange={(e) => setRole(e.target.value)}
                      autoComplete="organization-title"
                    >
                      <option value="">Optional</option>
                      <option value="platform">Data platform / engineering lead</option>
                      <option value="analytics">Analytics / BI</option>
                      <option value="cdo">CDO / CIO</option>
                      <option value="security">Security / compliance</option>
                      <option value="procurement">Procurement</option>
                      <option value="other">Other</option>
                    </select>
                  </label>
                  <label>
                    Phone
                    <input
                      className="mk-ct-f-input"
                      type="tel"
                      value={phone}
                      onChange={(e) => setPhone(e.target.value)}
                      autoComplete="tel"
                      placeholder="+1 …"
                    />
                  </label>
                </div>
                <ChipGrid label="Sources" options={SOURCES} value={sources} onChange={setSources} />
                <ChipGrid
                  label="Destinations"
                  options={DESTINATIONS}
                  value={destinations}
                  onChange={setDestinations}
                />
                <div className="mk-ct-f-fields mk-ct-f-fields--3">
                  <label>
                    Daily volume
                    <select className="mk-ct-f-input" value={volume} onChange={(e) => setVolume(e.target.value)}>
                      <option value="">Optional</option>
                      <option value="lt-1m">&lt; 1M rows/day</option>
                      <option value="1m-100m">1M – 100M rows/day</option>
                      <option value="100m-1b">100M – 1B rows/day</option>
                      <option value="gt-1b">&gt; 1B rows/day</option>
                    </select>
                  </label>
                  <label>
                    Region
                    <select className="mk-ct-f-input" value={region} onChange={(e) => setRegion(e.target.value)}>
                      <option value="">Optional</option>
                      <option value="us">US</option>
                      <option value="eu">EU</option>
                      <option value="apac">APAC</option>
                      <option value="other">Other</option>
                    </select>
                  </label>
                  <label>
                    Timeframe
                    <select className="mk-ct-f-input" value={timeframe} onChange={(e) => setTimeframe(e.target.value)}>
                      <option value="">Optional</option>
                      <option value="pilot">Pilot now</option>
                      <option value="prod-30d">Production in 30 days</option>
                      <option value="evaluating">Still evaluating</option>
                    </select>
                  </label>
                </div>
                <label className="mk-ct-f-span">
                  What must the first load prove? <em>Optional</em>
                  <textarea
                    className="mk-ct-f-input mk-ct-f-textarea"
                    value={message}
                    onChange={(e) => setMessage(e.target.value)}
                    rows={2}
                    placeholder="Cutover window, dest stored procedure, incremental cursor…"
                  />
                </label>
              </details>

              <label className="mk-ct-f-hp" aria-hidden="true">
                Do not fill
                <input tabIndex={-1} autoComplete="off" value={honeypot} onChange={(e) => setHoneypot(e.target.value)} />
              </label>

              <TurnstileWidget siteKey={turnstileSiteKey} onToken={setTurnstileToken} action="contact_sales" />

              <div className="mk-ct-f-form-foot">
                <p>
                  We may email you about this request. See{" "}
                  <button type="button" className="mk-ct-f-inline" onClick={() => onNavigate("privacy")}>
                    Privacy
                  </button>
                  .
                </p>
                {error ? (
                  <p className="mk-ct-f-form-error" role="alert">
                    {error}
                  </p>
                ) : null}
                <button type="submit" className="mk-btn mk-btn--primary" disabled={submitting}>
                  {submitting ? "Sending…" : "Send request"}
                </button>
              </div>
            </form>
          )}
          </div>
            <dl className="mk-ct-mail">
              <div>
                <dt>Sales</dt>
                <dd>
                  <a href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">sales@datawrap.io</a>
                </dd>
              </div>
              <div>
                <dt>General questions</dt>
                <dd>
                  <a href="mailto:contact@datawrap.io">contact@datawrap.io</a>
                </dd>
              </div>
            </dl>
          </div>
        </div>
      </section>

      {/* What a pilot leaves behind, without the home page's example figures
          (those belong to Home and Job space only). */}
      <section className="mk-section mk-kit-sec mk-ct-keep" aria-labelledby="mk-ct-keep-title">
        <div className="mk-wrap">
          <SplitHead
            id="mk-ct-keep-title"
            title="What a pilot leaves you with"
            lead="One load on your own data, on the production engine. You keep everything it produces."
          />
          <Bento label="What a pilot leaves you with">
            <BentoTile
              span={4}
              title="A report signed by role"
              body="Engineering, the data owner, and compliance sign the same Trust Report."
              bleed
            >
              <div className="mk-app-surface mk-tui">
                <div className="mk-tui-head">
                  <strong>Sign-off</strong>
                  <span className="mk-tui-muted">Example</span>
                </div>
                <ul className="mk-tui-verdicts" aria-label="Roles that sign (example)">
                  {["Engineering", "Data owner", "Compliance"].map((role) => (
                    <li key={role}>
                      <span>{role}</span>
                      <span className="mk-app-pill mk-app-pill--v-not-run">Waiting</span>
                    </li>
                  ))}
                </ul>
              </div>
            </BentoTile>
            <BentoTile
              span={4}
              title="A proof pack you can verify"
              body="A signed record of the checks and the mapping. Check it through the API."
              bleed
            >
              <div className="mk-app-surface mk-tui">
                <ul className="mk-ct-tui-calls" aria-label="Proof pack API calls">
                  {PACK_CALLS.map(([what, call]) => (
                    <li key={call}>
                      <span>{what}</span>
                      <code>{breakAtSlashes(call)}</code>
                    </li>
                  ))}
                </ul>
              </div>
            </BentoTile>
            <BentoTile
              span={4}
              title="An audit log you can export"
              body="Every action is signed and linked to the one before, so a removed entry is detected."
              bleed
            >
              <div className="mk-app-surface mk-tui">
                <div className="mk-tui-head">
                  <span className="mk-app-pill mk-app-pill--completed">Chain verified</span>
                  <span className="mk-tui-muted">Example</span>
                </div>
                <ul className="mk-ct-tui-chain" aria-label="Audit log entries (example)">
                  {AUDIT_EXAMPLE.slice(0, 3).map((e) => (
                    <li key={e.hash}>
                      <ShieldCheck size={13} aria-hidden />
                      <span>{e.what}</span>
                      <code>{e.hash}</code>
                    </li>
                  ))}
                </ul>
              </div>
            </BentoTile>
          </Bento>
        </div>
      </section>

      <section className="mk-section mk-kit-sec mk-ct-partners" aria-labelledby="mk-contact-partners">
        <div className="mk-wrap">
          <SplitHead
            id="mk-contact-partners"
            title="Design partner program"
            lead={
              <>
                We work with a small number of data teams on real migrations and syncs, and partners shape what we
                build next. Apply by email:{" "}
                <a className="mk-link" href="mailto:sales@datawrap.io?subject=Design%20partner%20application">
                  sales@datawrap.io
                </a>
                .
              </>
            }
          />
          <Bento label="Design partner program">
            {PARTNER_TERMS.map((t) => (
              <BentoTile key={t.key} span={6} title={t.title}>
                <ul className="mk-ct-terms">
                  {t.items.map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </BentoTile>
            ))}
          </Bento>
        </div>
      </section>

      <section className="mk-section mk-kit-sec mk-ct-faq" aria-labelledby="mk-contact-faq">
        <div className="mk-wrap">
          <SplitHead id="mk-contact-faq" title="Questions people ask first" lead="The engine, the connectors, and your security review." />
          <MarketingFaq items={FAQS} />
        </div>
      </section>

      <MarketingClosing
        title="Rather try it first?"
        body="Start a free workspace and run a load on the same engine a pilot uses."
      >
        <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={() => (onGetStarted ? onGetStarted() : onNavigate("help"))}>
          {START_ACTION_LABEL}
        </button>
        <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={() => onNavigate("help")}>
          Read the docs
        </button>
      </MarketingClosing>
    </div>
  );
}
