import { useEffect, useRef, useState, type FormEvent } from "react";
import { DtIcon } from "../../components/DtIcon";
import { TurnstileWidget } from "../../components/TurnstileWidget";
import { API_BASE } from "../../lib/types";
import { MarketingFaq } from "../../components/marketing/MarketingFaq";
import { DesignPartners } from "../../components/marketing/DesignPartners";
import { catalogHonestyLead, cdcSourcesPhrase } from "../../lib/provenEvidence";
import type { PublicRoute } from "../../lib/publicNavigation";

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
    body: "We usually reply within one business day, with a few questions about your source, destination, and constraints.",
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
    <fieldset className="lp-sales-chips">
      <legend>{label}</legend>
      <div className="lp-sales-chip-row">
        {options.map((opt) => {
          const on = value.includes(opt);
          return (
            <button
              key={opt}
              type="button"
              className={`lp-sales-chip${on ? " is-on" : ""}`}
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

export function ContactSalesPage({
  onNavigate,
}: {
  onNavigate: (route: PublicRoute) => void;
}) {
  const [sent, setSent] = useState(false);
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

  const canSubmit = Boolean(
    firstName.trim() && lastName.trim() && email.trim() && company.trim()
      && (!turnstileSiteKey || turnstileToken),
  );
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

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
      setSent(true);
      return;
    }
    if (!canSubmit || submitting) return;
    setSubmitting(true);
    setError("");
    try {
      let res = await postLead();
      let data = (await res.json().catch(() => null)) as { ok?: boolean; error?: string } | null;
      // A stale page (>1h) carries an expired/used token: refetch config and
      // retry once — the visitor never sees it.
      if (res.status === 400 && data?.error === "form_expired") {
        const cfg = await fetchContactSalesConfig();
        setTurnstileSiteKey(cfg.siteKey);
        formTokenRef.current = cfg.formToken;
        res = await postLead();
        data = (await res.json().catch(() => null)) as { ok?: boolean; error?: string } | null;
      }
      if (!res.ok || !data?.ok) {
        setError(contactSalesError(res.status, data));
        return;
      }
      setSent(true);
    } catch {
      setError("We could not reach the server. Please email sales@datawrap.io directly.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="mk-page mk-contact-page lp-sales">
      <section className="mk-subhero mk-contact" aria-label="Contact sales">
        <div className="mk-wrap mk-subhero-grid">
          <div className="mk-subhero-copy">
            <p className="mk-subhero-kicker">Contact sales</p>
            <h1>Tell us what you need to move.</h1>
            <p className="mk-subhero-lead">
              A migration, a recurring sync, or a pilot on your own data. We usually reply within one business day.
            </p>
            <p className="mk-contact-mail">
              <a href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">sales@datawrap.io</a>
              <span>
                General questions: <a href="mailto:contact@datawrap.io">contact@datawrap.io</a>
              </span>
            </p>
            <h2 className="mk-contact-steps-title">What happens next</h2>
            <ol className="mk-contact-steps">
              {NEXT_STEPS.map((s, i) => (
                <li key={s.title}>
                  <span aria-hidden>{i + 1}</span>
                  <div>
                    <strong>{s.title}</strong>
                    <p>{s.body}</p>
                  </div>
                </li>
              ))}
            </ol>
          </div>

          {sent ? (
            <div className="lp-sales-success" role="status">
              <div className="lp-sales-success-mark" aria-hidden>
                <DtIcon name="check" size={28} />
              </div>
              <h2>Request received</h2>
              <p>
                We usually reply within one business day
                {sources.length ? ` about ${sources.slice(0, 2).join(", ")}` : ""}
                {destinations.length ? ` → ${destinations.slice(0, 2).join(", ")}` : ""}.
              </p>
              <p>
                Or write now:{" "}
                <a href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">sales@datawrap.io</a>
              </p>
              <div className="lp-sales-success-actions">
                <button type="button" className="mk-btn mk-btn--primary" onClick={() => onNavigate("help")}>
                  Browse docs
                </button>
                <button type="button" className="mk-btn mk-btn--secondary" onClick={() => onNavigate("product-transfer")}>
                  Transfer Studio
                </button>
              </div>
            </div>
          ) : (
            <form className="lp-sales-form" onSubmit={submit} noValidate>
              <header className="lp-sales-form-head">
                <h2>Request a scoped pilot</h2>
                <p>Name, work email, and company. Stack is optional.</p>
              </header>

              <div className="lp-sales-fields">
                <label>
                  First name
                  <input
                    className="lp-sales-input"
                    value={firstName}
                    onChange={(e) => setFirstName(e.target.value)}
                    required
                    autoComplete="given-name"
                  />
                </label>
                <label>
                  Last name
                  <input
                    className="lp-sales-input"
                    value={lastName}
                    onChange={(e) => setLastName(e.target.value)}
                    required
                    autoComplete="family-name"
                  />
                </label>
                <label>
                  Work email
                  <input
                    className="lp-sales-input"
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    required
                    autoComplete="email"
                    placeholder="name@company.com"
                  />
                </label>
                <label>
                  Company
                  <input
                    className="lp-sales-input"
                    value={company}
                    onChange={(e) => setCompany(e.target.value)}
                    required
                    autoComplete="organization"
                  />
                </label>
              </div>

              <details className="lp-sales-more">
                <summary>Role, phone, and stack <em>optional</em></summary>
                <div className="lp-sales-fields">
                  <label>
                    Role
                    <select
                      className="lp-sales-input"
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
                      className="lp-sales-input"
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
                <div className="lp-sales-fields lp-sales-fields--3">
                  <label>
                    Daily volume
                    <select className="lp-sales-input" value={volume} onChange={(e) => setVolume(e.target.value)}>
                      <option value="">Optional</option>
                      <option value="lt-1m">&lt; 1M rows/day</option>
                      <option value="1m-100m">1M – 100M rows/day</option>
                      <option value="100m-1b">100M – 1B rows/day</option>
                      <option value="gt-1b">&gt; 1B rows/day</option>
                    </select>
                  </label>
                  <label>
                    Region
                    <select className="lp-sales-input" value={region} onChange={(e) => setRegion(e.target.value)}>
                      <option value="">Optional</option>
                      <option value="us">US</option>
                      <option value="eu">EU</option>
                      <option value="apac">APAC</option>
                      <option value="other">Other</option>
                    </select>
                  </label>
                  <label>
                    Timeframe
                    <select className="lp-sales-input" value={timeframe} onChange={(e) => setTimeframe(e.target.value)}>
                      <option value="">Optional</option>
                      <option value="pilot">Pilot now</option>
                      <option value="prod-30d">Production in 30 days</option>
                      <option value="evaluating">Still evaluating</option>
                    </select>
                  </label>
                </div>
                <label className="lp-sales-span">
                  What must the first load prove? <em>Optional</em>
                  <textarea
                    className="lp-sales-input lp-sales-textarea"
                    value={message}
                    onChange={(e) => setMessage(e.target.value)}
                    rows={2}
                    placeholder="Cutover window, dest stored procedure, incremental cursor…"
                  />
                </label>
              </details>

              <label className="lp-sales-hp" aria-hidden="true">
                Do not fill
                <input tabIndex={-1} autoComplete="off" value={honeypot} onChange={(e) => setHoneypot(e.target.value)} />
              </label>

              <TurnstileWidget siteKey={turnstileSiteKey} onToken={setTurnstileToken} action="contact_sales" />

              <div className="lp-sales-form-foot">
                <p>
                  We may email you about this request. See{" "}
                  <button type="button" className="lp-sales-inline" onClick={() => onNavigate("privacy")}>
                    Privacy
                  </button>
                  .
                </p>
                {error ? (
                  <p className="lp-sales-form-error" role="alert">
                    {error}
                  </p>
                ) : null}
                <button type="submit" className="mk-btn mk-btn--primary" disabled={!canSubmit || submitting}>
                  {submitting ? "Sending…" : "Request a pilot"}
                </button>
              </div>
            </form>
          )}
        </div>
      </section>

      <DesignPartners headingId="mk-contact-partners" />

      <section className="mk-section mk-section--band" aria-labelledby="mk-contact-faq">
        <div className="mk-wrap mk-story-grid">
          <header className="mk-story-head">
            <h2 id="mk-contact-faq">Questions people ask first</h2>
          </header>
          <MarketingFaq items={FAQS} />
        </div>
      </section>
    </div>
  );
}
