import { useState, type FormEvent } from "react";
import { DtIcon } from "../../components/DtIcon";
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
    body: "Same Transfer Studio path: semantic map, G1–G9, write with quarantine.",
  },
  {
    n: "03",
    title: "Reconcile artifact you keep",
    body: "Dest-engine COUNT and checksum MATCH. We do not call a green status “done.”",
  },
];

const FAQS = [
  {
    q: "How fast do you reply?",
    a: "A solutions engineer replies within one business day. This is not a drip sequence. You can also write sales@datawrap.io now.",
  },
  {
    q: "Is the pilot the same engine as production?",
    a: "Yes. skip_preflight never comes from this form, Pilot chat, or the public Studio execute path. CDC stays at-least-once upsert until a route proves dest-owned exactly-once.",
  },
  {
    q: "Do you claim 700+ live connectors?",
    a: `No. Catalog tiles are not transfer-live. ${TRANSFER_READY_DRIVERS} drivers are TRANSFER_READY. Warehouse and SaaS tiles that are not live-certified stay labeled Planned until a named matrix exists.`,
  },
  {
    q: "How is this different from Fivetran, Airbyte, or Informatica?",
    a: "Fivetran and Airbyte optimize connector breadth and managed ELT. Informatica CDI offers Target Pre/Post-load SQL and optional continue-on-error. Databricks Lakeflow is cursor-column lakehouse ingest. Datawrap’s wedge is semantic mapping you can review, fail-fast G1–G9, quarantine instead of silent drop, dest query/CALL with binds, and dest-engine checksum proof.",
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

  const canSubmit = Boolean(firstName.trim() && lastName.trim() && email.trim() && company.trim());

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (honeypot.trim()) {
      setSent(true);
      return;
    }
    if (!canSubmit) return;
    // eslint-disable-next-line no-console
    console.info("[marketing/contact] pilot request", {
      sources,
      destinations,
      volume,
      region,
      timeframe,
      name: `${firstName.trim()} ${lastName.trim()}`,
      email: email.trim(),
      company: company.trim(),
      role: role.trim(),
      phone: phone.trim(),
      message: message.trim().slice(0, 1200),
      submittedAt: new Date().toISOString(),
    });
    setSent(true);
  };

  return (
    <div className="lp-mkt-page lp-sales">
      <section className="lp-sales-hero lp-sales-hero--split" aria-label="Talk to sales">
        <div className="lp-mkt-wrap lp-sales-hero-inner">
          <div className="lp-sales-hero-copy">
            <p className="lp-sales-kicker">Contact sales</p>
            <h1>Talk to a solutions engineer.</h1>
            <p>
              Scope Map → G1–G9 → quarantine → dest-engine checksum on your sources and
              destinations. Reply within one business day — not a drip campaign.
            </p>
            <ul className="lp-sales-hero-slas">
              <li>
                <strong>&lt; 1 day</strong>
                <span>Engineer reply</span>
              </li>
              <li>
                <strong>G1–G9</strong>
                <span>Same gates as production</span>
              </li>
              <li>
                <strong>{TRANSFER_READY_DRIVERS}</strong>
                <span>TRANSFER_READY drivers</span>
              </li>
              <li>
                <strong>MATCH</strong>
                <span>Checksum you can archive</span>
              </li>
            </ul>
            <p className="lp-sales-hero-mail">
              <a className="lp-sales-mail" href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">
                sales@datawrap.io
              </a>
              <span>Reply in one business day. No SOC 2 certificate is claimed.</span>
            </p>
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
                <p>Required: name, work email, company. Stack is optional.</p>
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
                  Phone <em>Optional</em>
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

              <details className="lp-sales-more">
                <summary>Add stack details <em>optional</em></summary>
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

              <div className="lp-sales-form-foot">
                <p>
                  We may email you about this request. See{" "}
                  <button type="button" className="lp-sales-inline" onClick={() => onNavigate("privacy")}>
                    Privacy
                  </button>
                  .
                </p>
                <button type="submit" className="lp-btn lp-btn--brand" disabled={!canSubmit}>
                  Request pilot
                </button>
              </div>
            </form>
          )}
        </div>
      </section>

      <section className="lp-sales-follow" aria-label="What happens next">
        <div className="lp-mkt-wrap lp-sales-follow-inner">
          <ol className="lp-sales-next">
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
          <div className="lp-sales-rail-box">
            <h3>What we will not claim on the call</h3>
            <ul>
              <li>Catalog count as transfer-live</li>
              <li>Platform-wide exactly-once</li>
              <li>SOC 2 / ISO certificate (controls exist; audit is not done)</li>
              <li>Informatica continue-on-error as success</li>
            </ul>
            <div className="lp-sales-rail-links">
              <button type="button" onClick={() => onNavigate("security")}>Security</button>
              <button type="button" onClick={() => onNavigate("enterprise")}>Enterprise</button>
              <button type="button" onClick={() => onNavigate("pricing")}>Pricing</button>
            </div>
          </div>
        </div>
      </section>

      <section className="lp-sales-compare" aria-label="How we position against the market">
        <div className="lp-mkt-wrap">
          <header>
            <p className="lp-sales-kicker lp-sales-kicker--ink">Market position</p>
            <h2>Advanced where Fivetran, Airbyte, Informatica, and Databricks stop.</h2>
            <p>
              We do not out-claim their connector catalogs. We out-prove the load: mapping you can
              review, gates that block write, quarantine you can replay, dest SQL you can bind.
            </p>
          </header>
          <div className="lp-sales-table-wrap">
            <table className="lp-sales-table">
              <thead>
                <tr>
                  <th scope="col">Capability</th>
                  <th scope="col">Fivetran / Airbyte</th>
                  <th scope="col">Informatica CDI</th>
                  <th scope="col">Databricks Lakeflow</th>
                  <th scope="col">Datawrap</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <th scope="row">Column mapping</th>
                  <td>Schema sync / name match</td>
                  <td>Designer mappings</td>
                  <td>Lakehouse table ingest</td>
                  <td>Semantic role + confidence; extra source stays on Map (G13)</td>
                </tr>
                <tr>
                  <th scope="row">Failed rows</th>
                  <td>Retry / skip / job status</td>
                  <td>Optional continue-on-error</td>
                  <td>Pipeline / job status</td>
                  <td>Quarantine with column, value, reason — never silent drop</td>
                </tr>
                <tr>
                  <th scope="row">Dest SQL</th>
                  <td>Table / stream writers</td>
                  <td>Target Pre/Post-load + SQL override</td>
                  <td>Cursor-column table ingest — dest CALL is not the writer</td>
                  <td>Dest query INSERT/MERGE and dest CALL, binds only, quarantine</td>
                </tr>
                <tr>
                  <th scope="row">Preflight</th>
                  <td>Connector checks</td>
                  <td>Session validation</td>
                  <td>Ingest / pipeline checks</td>
                  <td>Nine core gates G1–G9 before write</td>
                </tr>
                <tr>
                  <th scope="row">Proof</th>
                  <td>Logs and row counts</td>
                  <td>Session logs</td>
                  <td>Lakehouse job metrics</td>
                  <td>Dest-engine COUNT + checksum MATCH</td>
                </tr>
                <tr>
                  <th scope="row">CDC</th>
                  <td>Managed incremental / CDC</td>
                  <td>PowerCenter / CDI CDC</td>
                  <td>Lakeflow CDC into Delta</td>
                  <td>At-least-once upsert default; dest-owned EOS only when a route proves it</td>
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
