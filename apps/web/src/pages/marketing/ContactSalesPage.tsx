import { useEffect, useState, type FormEvent } from "react";
import { DtIcon } from "../../components/DtIcon";
import { TurnstileWidget } from "../../components/TurnstileWidget";
import { API_BASE } from "../../lib/types";
import { PilotRouteArt } from "../../components/marketing/hero-art/contactArt";
import { TRANSFER_READY_DRIVERS } from "../../lib/provenEvidence";
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
  "Redshift",
  "Databricks",
  "PostgreSQL",
  "SQL Server",
  "Other",
] as const;

const NEXT_STEPS = [
  {
    n: "01",
    title: "Discovery call — 30 minutes",
    body: "A solutions engineer walks your source → dest, sync mode, and compliance constraints.",
  },
  {
    n: "02",
    title: "Scoped pilot on your stack",
    body: "The production Transfer Studio path: semantic map, G1–G9, write with quarantine and replay.",
  },
  {
    n: "03",
    title: "Signed proof pack you keep",
    body: "Gate-8 read-back with destination COUNT, checksums, and a value and key census that MATCH.",
  },
];

const FAQS = [
  {
    q: "How fast do you reply?",
    a: "A solutions engineer replies within one business day. You can also write sales@datawrap.io now.",
  },
  {
    q: "Is the pilot the same engine as production?",
    a: "Yes. The pilot runs the production engine with every preflight gate, quarantine, and post-write verification — the same path Studio, Pipelines, Pilot, and MCP use.",
  },
  {
    q: "How many connectors are live?",
    a: `${TRANSFER_READY_DRIVERS} drivers are transfer-ready today across databases, warehouses, object storage, files, Kafka, vector databases, and apps — each with the same map, gates, and verification.`,
  },
  {
    q: "Which change data capture engines do you support?",
    a: "SQL Server native CDC, PostgreSQL logical decoding, MySQL binlog, and Oracle LogMiner — each with a consistent snapshot, resume from the log position, and deletes carried through.",
  },
  {
    q: "What makes Datawrap different?",
    a: "Semantic mapping you can review, nine preflight gates before every write, quarantine with replay, signed risk contracts for intentional casts, destination query and stored-procedure writes with binds, and a value-by-value read-back from the destination on every load.",
  },
  {
    q: "What do you provide for security review?",
    a: "A security questionnaire, a DPA, and a workspace audit export from a hash-chained, signed audit log your reviewers can verify. SSO (OIDC / SAML), SCIM, RBAC, and BYOK are available on Enterprise.",
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

/** Turnstile site key the API enforces for this form ("" when it doesn't). */
async function fetchContactSalesTurnstileKey(): Promise<string> {
  try {
    const res = await fetch(`${API_BASE}/contact-sales/config`);
    if (!res.ok) return "";
    const data = (await res.json()) as { turnstile_site_key?: unknown };
    return typeof data.turnstile_site_key === "string" ? data.turnstile_site_key : "";
  } catch {
    return "";
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
  useEffect(() => {
    void fetchContactSalesTurnstileKey().then(setTurnstileSiteKey);
  }, []);

  const canSubmit = Boolean(
    firstName.trim() && lastName.trim() && email.trim() && company.trim()
      && (!turnstileSiteKey || turnstileToken),
  );
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

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
      const res = await fetch(`${API_BASE}/contact-sales`, {
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
        }),
      });
      const data = (await res.json().catch(() => null)) as { ok?: boolean; error?: string } | null;
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
    <div className="lp-mkt-page lp-sales">
      <section className="lp-sales-hero lp-sales-hero--split lp-sales-hero--ink" aria-label="Contact sales">
        <div className="lp-mkt-wrap lp-sales-hero-inner">
          <div className="lp-sales-hero-copy">
            <p className="lp-sales-kicker">Contact sales</p>
            <h1>Talk to a solutions engineer.</h1>
            <p className="lp-sales-hero-lead">
              Scope Map → G1–G9 → quarantine → Gate-8 verification on your stack. A solutions
              engineer replies within one business day.
            </p>
            <p className="lp-sales-hero-mail">
              <a className="lp-sales-mail" href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">
                sales@datawrap.io
              </a>
              <span>
                One business day. General questions:{" "}
                <a className="lp-sales-mail" href="mailto:contact@datawrap.io">contact@datawrap.io</a>.
                No SOC 2 certificate is claimed.
              </span>
            </p>
            {/* The drawing carries these three steps above 960px; below it, the list does. */}
            <ol className="lp-sales-hero-steps lp-sales-hero-steps--narrow">
              {NEXT_STEPS.map((s) => (
                <li key={s.n}>
                  <span>{s.n}</span>
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
                A solutions engineer will reply within one business day
                {sources.length ? ` about ${sources.slice(0, 2).join(", ")}` : ""}
                {destinations.length ? ` → ${destinations.slice(0, 2).join(", ")}` : ""}.
              </p>
              <p>
                Or write now:{" "}
                <a href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">sales@datawrap.io</a>
              </p>
              <div className="lp-sales-success-actions">
                <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("help")}>
                  Browse docs
                </button>
                <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("product-transfer")}>
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
                <button type="submit" className="lp-btn lp-btn--brand" disabled={!canSubmit || submitting}>
                  {submitting ? "Sending…" : "Request pilot"}
                </button>
              </div>
            </form>
          )}

          <div className="lp-sales-hero-route">
            <PilotRouteArt />
          </div>
        </div>
      </section>

      <section className="lp-sales-follow" aria-label="What you will see on the call">
        <div className="lp-mkt-wrap lp-sales-honesty">
          <div>
            <p className="lp-sales-kicker lp-sales-kicker--ink">On the call</p>
            <h2>What you will see</h2>
          </div>
          <ul>
            <li>Your source and destination, mapped by meaning</li>
            <li>Every preflight gate, live, on your schema</li>
            <li>A verified read-back from your destination</li>
            <li>The enterprise controls your security team asks for</li>
          </ul>
          <div className="lp-sales-rail-links">
            <button type="button" onClick={() => onNavigate("security")}>Security</button>
            <button type="button" onClick={() => onNavigate("enterprise")}>Enterprise</button>
            <button type="button" onClick={() => onNavigate("pricing")}>Pricing</button>
          </div>
        </div>
      </section>

      <section className="lp-sales-compare" aria-label="What the pilot proves">
        <div className="lp-mkt-wrap">
          <header>
            <p className="lp-sales-kicker lp-sales-kicker--ink">What the pilot proves</p>
            <h2>Every load verified, on your own stack.</h2>
            <p>
              Mapping you can review, gates before the write, quarantine you can replay, destination
              SQL you can bind, and a read-back you can archive.
            </p>
          </header>
          <div className="lp-sales-table-wrap">
            <table className="lp-sales-table">
              <thead>
                <tr>
                  <th scope="col">Capability</th>
                  <th scope="col">What Datawrap does</th>
                  <th scope="col">What you keep</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <th scope="row">Column mapping</th>
                  <td>Semantic role + confidence; extra source columns stay visible on Map</td>
                  <td>Reviewed mapping decisions</td>
                </tr>
                <tr>
                  <th scope="row">Preflight</th>
                  <td>Nine core gates G1–G9 before every write, collation-aware key checks included</td>
                  <td>Gate results per run</td>
                </tr>
                <tr>
                  <th scope="row">Held-back rows</th>
                  <td>Quarantine with column, value, and reason; replay once fixed</td>
                  <td>A durable quarantine queue</td>
                </tr>
                <tr>
                  <th scope="row">Destination SQL</th>
                  <td>Destination INSERT/MERGE and stored-procedure writes, binds only, with quarantine</td>
                  <td>Per-row outcomes</td>
                </tr>
                <tr>
                  <th scope="row">Proof</th>
                  <td>Gate-8 read-back: destination COUNT, checksums, value and key census</td>
                  <td>A signed proof pack</td>
                </tr>
                <tr>
                  <th scope="row">CDC</th>
                  <td>SQL Server, PostgreSQL, MySQL, and Oracle — snapshot, stream, resume</td>
                  <td>Idempotent upserts on your keys</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <section className="lp-sales-faq" aria-label="Sales FAQ">
        <div className="lp-mkt-wrap">
          <h2>Questions procurement usually asks</h2>
          <dl>
            {FAQS.map((item) => (
              <div key={item.q}>
                <dt>{item.q}</dt>
                <dd>{item.a}</dd>
              </div>
            ))}
          </dl>
        </div>
      </section>
    </div>
  );
}
