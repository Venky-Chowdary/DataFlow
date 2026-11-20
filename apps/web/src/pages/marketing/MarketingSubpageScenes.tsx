import { useEffect, useLayoutEffect, useState, type ReactNode } from "react";
import {
  Activity,
  Bell,
  Check,
  Download,
  FileSignature,
  KeyRound,
  Settings,
  Shield,
  ShieldCheck,
  Users,
  Zap,
} from "lucide-react";
import { ConnectorIcon } from "../../app/brand-icons";
import {
  AppWindow,
  EXAMPLE_JOBS,
  JobsScene,
  PanelWindow,
} from "../../components/marketing/app-frame/AppFrame";
import { ROLES } from "../../components/marketing/ProductPanels";
import {
  CATALOG_TOTAL,
  EMULATED_DRIVERS,
  EVIDENCE_AS_OF,
  EVIDENCE_RUN_ID,
  PROOF_GROUPS,
  PROVEN_DRIVERS,
} from "../../lib/provenEvidence";

/**
 * Product scenes for the Enterprise, Proof, and Security pages, built on the
 * shared AppFrame kit. Each mirrors a real workspace screen (Settings, the
 * Jobs sign-off drawer, Proofs).
 * Example data says so in the caption; evidence data comes from the CI
 * evidence file and is never typed by hand.
 */

const fmt = new Intl.NumberFormat("en-US");

/**
 * Layout width of a phone crop. It matches a 390px screen's content column,
 * so the panel renders at about 1:1 and its UI text stays at 12px or more.
 */
const PHONE_WIDTH = 360;

/** A full window, or one panel of it (a feature row's frame, or any phone). */
export type FrameKind = "window" | "panel";

/** True when the viewport is phone-sized: frames crop to one panel there. */
export function useNarrow(query = "(max-width: 767px)") {
  const [narrow, setNarrow] = useState(() => {
    try {
      return window.matchMedia(query).matches;
    } catch {
      return false;
    }
  });
  useLayoutEffect(() => {
    let mq: MediaQueryList;
    try {
      mq = window.matchMedia(query);
    } catch {
      return;
    }
    const on = () => setNarrow(mq.matches);
    on();
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, [query]);
  return narrow;
}

/** The phone picker above a cropped panel. */
function Picker<T extends string>({
  label,
  items,
  value,
  onChange,
}: {
  label: string;
  items: { id: T; label: string }[];
  value: T;
  onChange: (id: T) => void;
}) {
  return (
    <div className="mk-app-picker" role="tablist" aria-label={label}>
      {items.map((it) => (
        <button
          key={it.id}
          type="button"
          role="tab"
          aria-selected={value === it.id}
          className={value === it.id ? "is-active" : undefined}
          onClick={() => onChange(it.id)}
        >
          {it.label}
        </button>
      ))}
    </div>
  );
}

/* ══ Settings: SSO and SCIM, team roles, keys and region, audit chain ══ */

export type SettingsSection = "sso" | "team" | "enterprise" | "audit";

const SETTINGS_NAV: { id: SettingsSection | null; title: string; sub: string; icon: ReactNode }[] = [
  { id: null, title: "General", sub: "Workspace defaults", icon: <Settings size={16} /> },
  { id: null, title: "Security", sub: "Policies & compliance", icon: <Shield size={16} /> },
  { id: "enterprise", title: "Enterprise", sub: "Tenant, BYOK, residency", icon: <Shield size={16} /> },
  { id: "sso", title: "SSO", sub: "Identity providers", icon: <ShieldCheck size={16} /> },
  { id: "team", title: "Team", sub: "Members & roles", icon: <Users size={16} /> },
  { id: null, title: "Notifications", sub: "Alerts & integrations", icon: <Bell size={16} /> },
  { id: null, title: "API Keys", sub: "Programmatic access", icon: <Zap size={16} /> },
  { id: "audit", title: "Audit Logs", sub: "Activity trail", icon: <Activity size={16} /> },
];

const SECTION_LABEL: Record<SettingsSection, string> = {
  sso: "SSO and SCIM",
  team: "Team",
  enterprise: "Keys and region",
  audit: "Audit log",
};

function Card({ title, sub, children, banner }: { title: string; sub: string; children: ReactNode; banner?: ReactNode }) {
  return (
    <section className="mk-sc-card">
      <header className="mk-sc-card-head">
        <strong>{title}</strong>
        <p>{sub}</p>
      </header>
      {banner}
      <div className="mk-sc-card-body">{children}</div>
    </section>
  );
}

function AdminBanner({ what }: { what: string }) {
  return (
    <p className="mk-sc-banner">
      <Shield size={14} aria-hidden />
      <span>
        <strong>{what} apply to every workspace.</strong> Only a platform admin can change deployment-wide settings.
      </span>
    </p>
  );
}

function SsoPane() {
  const providers = [
    { name: "SAML 2.0", sub: "Okta, Azure AD, OneLogin", on: true },
    { name: "OpenID Connect", sub: "Google Workspace, Auth0", on: false },
    { name: "Azure AD", sub: "Microsoft Entra ID", on: false },
  ];
  const groups: [string, string][] = [
    ["data-platform-admins", "Admin"],
    ["data-engineers", "Editor"],
    ["pipeline-on-call", "Operator"],
    ["analysts", "Viewer"],
  ];
  return (
    <>
      <Card
        title="Authentication & SSO"
        sub="Connect your identity provider for enterprise single sign-on."
        banner={<AdminBanner what="Identity provider settings" />}
      >
        <div className="mk-sc-providers">
          {providers.map((p) => (
            <div key={p.name} className={`mk-sc-provider${p.on ? " is-on" : ""}`}>
              <strong>{p.name}</strong>
              <span>{p.sub}</span>
              <span className={`mk-app-pill ${p.on ? "mk-app-pill--completed" : "mk-app-pill--v-not-run"}`}>
                {p.on ? "Configured" : "Not configured"}
              </span>
              <span className={`mk-app-tbtn${p.on ? "" : " mk-app-tbtn--primary"}`}>View {p.name}</span>
            </div>
          ))}
        </div>
        <p className="mk-sc-fine">
          Set the callback URL in your IdP to <code>https://www.datawrap.io/api/v1/auth/sso/&lt;type&gt;/callback</code>.
          Enabled providers appear on the sign-in page.
        </p>
      </Card>
      <Card title="SCIM provisioning" sub="Provision workspace membership from your identity provider.">
        <div className="mk-sc-scim">
          <span className="mk-sc-field">
            <small>Token name</small>
            <span>scim-provisioning</span>
          </span>
          <span className="mk-sc-field">
            <small>Expiration</small>
            <span>90 days</span>
          </span>
          <span className="mk-app-tbtn mk-app-tbtn--primary">Generate SCIM token</span>
        </div>
        <p className="mk-sc-subhead">Group mappings</p>
        <div className="mk-sc-rows">
          {groups.map(([g, role]) => (
            <div key={g} className="mk-sc-row">
              <code>{g}</code>
              <span className="mk-sc-arrow" aria-hidden>
                maps to
              </span>
              <span className="mk-app-chip">{role}</span>
            </div>
          ))}
        </div>
      </Card>
    </>
  );
}

function TeamPane() {
  const members: [string, string, string][] = [
    ["Platform admin", "Admin", "SSO"],
    ["Data engineer", "Editor", "SSO"],
    ["On-call engineer", "Operator", "SSO"],
    ["Analyst", "Viewer", "SSO"],
    ["Nightly loads (API key)", "Editor", "Capped at its creator"],
  ];
  return (
    <>
      <Card title="Members" sub="Every API route is checked against one permission. A route with no rule is refused.">
        <div className="mk-sc-table">
          <div className="mk-sc-tr mk-sc-th">
            <span>Member</span>
            <span>Role</span>
            <span>Signs in with</span>
          </div>
          {members.map(([who, role, how]) => (
            <div key={who} className="mk-sc-tr">
              <span className="mk-sc-who">
                <i aria-hidden>{who[0]}</i>
                {who}
              </span>
              <span>
                <span className="mk-app-chip">{role}</span>
              </span>
              <span className="mk-sc-muted">{how}</span>
            </div>
          ))}
        </div>
      </Card>
      <Card title="Roles" sub="Four roles, the same in Transfer Studio, schedules, Pilot, and MCP.">
        <dl className="mk-sc-roles">
          {ROLES.map((r) => (
            <div key={r.name}>
              <dt>{r.name}</dt>
              <dd>{r.does}</dd>
            </div>
          ))}
        </dl>
      </Card>
    </>
  );
}

function EnterprisePane() {
  return (
    <>
      <Card
        title="Bring your own key"
        sub="Connector secrets are encrypted at rest. With your key, secrets are wrapped with it, and the platform key wraps the workspace key."
      >
        <div className="mk-sc-keyline">
          <span className="mk-sc-keyicon" aria-hidden>
            <KeyRound size={18} />
          </span>
          <span className="mk-sc-keytext">
            <strong>AWS KMS key</strong>
            <code>arn:aws:kms:eu-west-1:••••:key/••••</code>
          </span>
          <span className="mk-app-pill mk-app-pill--completed">Wrapping connector secrets</span>
        </div>
      </Card>
      <Card
        title="Data region"
        sub="Recorded on the workspace. A run is refused before it starts when the destination is elsewhere or shows no region."
        banner={<AdminBanner what="Residency settings" />}
      >
        <div className="mk-sc-region">
          <span className="mk-sc-field">
            <small>Workspace region</small>
            <span>eu-west-1</span>
          </span>
          <div className="mk-sc-rows">
            <div className="mk-sc-row">
              <span>Warehouse Snowflake, eu-west-1</span>
              <span className="mk-app-pill mk-app-pill--completed">Allowed</span>
            </div>
            <div className="mk-sc-row">
              <span>Archive bucket, us-east-1</span>
              <span className="mk-app-pill mk-app-pill--failed">Refused before the run</span>
            </div>
            <div className="mk-sc-row">
              <span>Scratch Postgres, no region</span>
              <span className="mk-app-pill mk-app-pill--failed">Refused before the run</span>
            </div>
          </div>
        </div>
      </Card>
    </>
  );
}

const AUDIT: { at: string; what: string; who: string; hash: string }[] = [
  { at: "09:12:04", what: "Plan approved", who: "Data owner, via SSO", hash: "9c02e1" },
  { at: "09:12:31", what: "Transfer started", who: "API key, Editor role", hash: "a41f8d" },
  { at: "09:53:08", what: "Rows held in quarantine", who: "Transfer engine", hash: "0de76b" },
  { at: "10:20:45", what: "Trust Report signed", who: "Compliance, via SSO", hash: "e5c913" },
  { at: "10:41:17", what: "SCIM group mapped to a role", who: "Platform admin, via SSO", hash: "71b3c0" },
  { at: "11:02:56", what: "Retention purge, dry run", who: "Platform admin, via SSO", hash: "3fa6d2" },
];

function AuditPane() {
  // The log opens verified; Verify chain checks it again, as the product does.
  const [verified, setVerified] = useState(true);
  const [run, setRun] = useState(0);
  useEffect(() => {
    if (run === 0) return;
    const id = window.setTimeout(() => setVerified(true), 700);
    return () => window.clearTimeout(id);
  }, [run]);
  return (
    <Card
      title="Audit log"
      sub="Tamper-evident: each entry is signed and linked to the one before, so a removed or altered entry is detected."
    >
      <div className="mk-sc-audit-bar">
        <button type="button" className="mk-sc-btn mk-sc-btn--primary" onClick={() => {
            setVerified(false);
            setRun((n) => n + 1);
          }}
        >
          <ShieldCheck size={14} aria-hidden />
          Verify chain
        </button>
        <span className="mk-app-tbtn" aria-hidden>
          <Download size={13} />
          Export
        </span>
        <span className="mk-sc-audit-state" role="status" aria-live="polite">
          {verified ? (
            <span className="mk-app-pill mk-app-pill--completed">Chain verified: {AUDIT.length} of {AUDIT.length} entries linked</span>
          ) : (
            <span className="mk-sc-muted">Checking each link</span>
          )}
        </span>
      </div>
      <ol className={`mk-sc-chain${verified ? " is-verified" : ""}`}>
        {AUDIT.map((e, i) => (
          <li key={e.hash}>
            <span className="mk-sc-chain-time">{e.at}</span>
            <span className="mk-sc-chain-node" aria-hidden>
              {verified ? <Check size={11} /> : null}
            </span>
            <span className="mk-sc-chain-what">
              <strong>{e.what}</strong>
              <small>{e.who}</small>
            </span>
            <span className="mk-sc-chain-hash">
              <code>{e.hash}</code>
              <small>
                links to <code>{i === 0 ? "4b7a30" : AUDIT[i - 1].hash}</code>
              </small>
            </span>
          </li>
        ))}
      </ol>
      <p className="mk-sc-fine">The chain head can be anchored in S3 with Object Lock, outside the application's reach.</p>
    </Card>
  );
}

function SettingsPane({ section }: { section: SettingsSection }) {
  if (section === "sso") return <SsoPane />;
  if (section === "team") return <TeamPane />;
  if (section === "enterprise") return <EnterprisePane />;
  return <AuditPane />;
}

function SettingsScene({ section, onSection }: { section: SettingsSection; onSection: (s: SettingsSection) => void }) {
  return (
    <div className="mk-sc-settings">
      <nav className="mk-sc-nav" aria-label="Settings sections (example)">
        {SETTINGS_NAV.map((n) =>
          n.id ? (
            <button
              key={n.title}
              type="button"
              className={`mk-sc-nav-item${n.id === section ? " is-active" : ""}`}
              aria-pressed={n.id === section}
              onClick={() => onSection(n.id as SettingsSection)}
            >
              {n.icon}
              <span>
                <strong>{n.title}</strong>
                <small>{n.sub}</small>
              </span>
            </button>
          ) : (
            <span key={n.title} className="mk-sc-nav-item is-static" aria-hidden>
              {n.icon}
              <span>
                <strong>{n.title}</strong>
                <small>{n.sub}</small>
              </span>
            </span>
          ),
        )}
      </nav>
      <div className="mk-sc-pane">
        <SettingsPane section={section} />
      </div>
    </div>
  );
}

/**
 * Settings in a full window. As a panel (and always on phones) it crops to one
 * pane, with a section picker when there is more than one section to show.
 */
export function SettingsShowcase({
  initial,
  caption,
  sections = ["sso", "team", "enterprise", "audit"],
  frame = "window",
}: {
  initial: SettingsSection;
  caption: string;
  sections?: SettingsSection[];
  frame?: FrameKind;
}) {
  const narrow = useNarrow();
  const [section, setSection] = useState<SettingsSection>(initial);
  if (narrow || frame === "panel") {
    return (
      <div className="mk-app-phone">
        {sections.length > 1 ? (
          <Picker
            label="Choose a settings section (example)"
            items={sections.map((id) => ({ id, label: SECTION_LABEL[id] }))}
            value={section}
            onChange={setSection}
          />
        ) : null}
        <PanelWindow path={`settings/${section}`} caption={caption} designWidth={narrow ? PHONE_WIDTH : 660}>
          <div className="mk-sc-pane is-panel">
            <SettingsPane section={section} />
          </div>
        </PanelWindow>
      </div>
    );
  }
  return (
    <AppWindow path={`settings/${section}`} active="settings" caption={caption}>
      <SettingsScene section={section} onSection={setSection} />
    </AppWindow>
  );
}

/* ══ Proof: the sign-off drawer and the Proofs ledger ══ */

const SIGNERS = ["Engineering", "Data owner", "Compliance"] as const;
type Signer = (typeof SIGNERS)[number];

const PACK_CONTAINS = [
  "Verdict, parity per table and the confidence statement",
  "Every difference with its cause and how it was resolved",
  "Transforms applied, PII protection proof, schema changes",
  "Approvals with names, roles and times",
  "The signature, once every required role has signed",
];

function SignOff({ inPanel }: { inPanel?: boolean }) {
  const job = EXAMPLE_JOBS.find((j) => j.id === "6af912d7") ?? EXAMPLE_JOBS[0];
  const [signed, setSigned] = useState<Signer[]>(["Engineering"]);
  const [as, setAs] = useState<Signer>("Data owner");
  const all = signed.length === SIGNERS.length;
  const sign = () => {
    if (signed.includes(as)) return;
    const next = [...signed, as];
    setSigned(next);
    const left = SIGNERS.find((s) => !next.includes(s));
    if (left) setAs(left);
  };
  return (
    <aside className={`mk-app-drawer mk-sc-signoff${inPanel ? " is-panel" : ""}`} aria-label="Sign-off and proof pack (example)">
      <div className="mk-app-drawer-head">
        <div>
          <strong>
            {job.name} → {job.dest}
          </strong>
          <span className="mk-app-pill mk-app-pill--completed">Completed</span>
          <p>
            {job.from.label} to {job.to.label}, {job.mode.toLowerCase()}. Check: {job.trust.check}, {job.trust.verdict.toLowerCase()}.
          </p>
        </div>
      </div>
      <div className="mk-app-drawer-tabs" aria-hidden>
        <span>Detail</span>
        <span>
          Mapping <em>11</em>
        </span>
        <span className="is-active">
          <ShieldCheck size={13} />
          Trust Report
        </span>
        <span>Reversibility</span>
      </div>
      <div className="mk-app-drawer-body">
        <ul className="mk-sc-signers">
          {SIGNERS.map((s) => (
            <li key={s}>
              <strong>{s}</strong>
              <span className={`mk-app-pill ${signed.includes(s) ? "mk-app-pill--completed" : "mk-app-pill--v-not-run"}`}>
                {signed.includes(s) ? "Signed" : "Waiting"}
              </span>
            </li>
          ))}
        </ul>
        {all ? (
          <p className="mk-sc-packstate" role="status">
            <ShieldCheck size={16} aria-hidden />
            <span>
              <strong>Proof pack signed.</strong> Every required role has signed. The pack claims migration proven: the
              check after the write was a full checksum.
            </span>
          </p>
        ) : (
          <div className="mk-sc-signform">
            <div className="mk-sc-seg" role="group" aria-label="Sign as">
              {SIGNERS.map((s) => (
                <button
                  key={s}
                  type="button"
                  aria-pressed={as === s}
                  className={as === s ? "is-active" : undefined}
                  disabled={signed.includes(s)}
                  onClick={() => setAs(s)}
                >
                  {s}
                </button>
              ))}
            </div>
            <button type="button" className="mk-sc-btn mk-sc-btn--primary" onClick={sign}>
              <FileSignature size={14} aria-hidden />
              Sign as {as}
            </button>
            <span className="mk-sc-muted" role="status" aria-live="polite">
              {SIGNERS.length - signed.length} of {SIGNERS.length} still to sign
            </span>
          </div>
        )}
        <div className="mk-sc-contains">
          <strong>What the pack contains</strong>
          <ul>
            {PACK_CONTAINS.map((c) => (
              <li key={c}>{c}</li>
            ))}
          </ul>
          <span className="mk-app-tbtn" aria-hidden>
            <Download size={13} />
            Download report (.md)
          </span>
        </div>
        {all ? null : (
          <button type="button" className="mk-sc-reset" onClick={() => setSigned(SIGNERS.slice())}>
            Sign all three
          </button>
        )}
      </div>
    </aside>
  );
}

/** The Jobs grid with the sign-off drawer open on the example run; as a panel, the drawer alone. */
export function SignOffShowcase({ caption, frame = "window" }: { caption: string; frame?: FrameKind }) {
  const narrow = useNarrow();
  if (narrow || frame === "panel") {
    return (
      <div className="mk-app-phone">
        <PanelWindow path="jobs/6af912d7/sign-off" caption={caption} designWidth={narrow ? PHONE_WIDTH : 660}>
          <SignOff inPanel />
        </PanelWindow>
      </div>
    );
  }
  return (
    <AppWindow
      path="jobs/6af912d7/sign-off"
      active="jobs"
      caption={caption}
      className="has-drawer mk-sc-signoff-window"
      overlay={<SignOff />}
    >
      <JobsScene openId="6af912d7" />
    </AppWindow>
  );
}

/** The Proofs screen, filled with this page's CI evidence. */
function ProofsLedger({ compact }: { compact?: boolean }) {
  const jobs = new Set(PROOF_GROUPS.flatMap((g) => g.jobs));
  const kpis: [string, string, string][] = [
    ["Drivers proven in CI", `${PROVEN_DRIVERS} of ${CATALOG_TOTAL}`, "Green, with no skipped test"],
    ["Proven on an emulator", String(EMULATED_DRIVERS.length), EMULATED_DRIVERS.join(" and ")],
    ["Implemented, proof running", String(CATALOG_TOTAL - PROVEN_DRIVERS), "Running their CI proof"],
    ["CI jobs behind the proof", String(jobs.size), `CI run ${EVIDENCE_RUN_ID}`],
  ];
  return (
    <div className={`mk-sc-proofs${compact ? " is-panel" : ""}`}>
      <div className="mk-sc-proofs-tabs" aria-hidden>
        <span className="is-active">Integrity ledger</span>
        <span>Scale throughput</span>
      </div>
      <div className="mk-app-kpis">
        {kpis.map(([k, v, s]) => (
          <div key={k} className="mk-app-kpi">
            <small>{k}</small>
            <strong>{v}</strong>
            <span>{s}</span>
          </div>
        ))}
      </div>
      <div className="mk-app-table mk-sc-ledger" role="table" aria-label={`CI evidence, recorded ${EVIDENCE_AS_OF}`}>
        <div className="mk-sc-ltr mk-app-th" role="row">
          <span role="columnheader">Drivers</span>
          <span role="columnheader">CI jobs</span>
          <span role="columnheader" className="is-num">
            Tests executed
          </span>
          <span role="columnheader">Result</span>
        </div>
        {PROOF_GROUPS.map((g) => (
          <div key={`${g.jobs.join(",")}-${g.emulated}`} className="mk-sc-ltr" role="row">
            <span role="cell" className="mk-sc-ldrivers">
              <span className="mk-sc-icons" aria-hidden>
                {g.drivers.slice(0, 4).map((d) => (
                  <i key={d.driver}>
                    <ConnectorIcon id={d.driver} size={14} />
                  </i>
                ))}
              </span>
              <span>
                {g.drivers.length > 3
                  ? `${g.drivers
                      .slice(0, 3)
                      .map((d) => d.name)
                      .join(", ")}, and ${g.drivers.length - 3} more`
                  : g.drivers.map((d) => d.name).join(", ")}
              </span>
            </span>
            <span role="cell" className="mk-sc-ljobs">
              {g.jobs.map((j) => (
                <code key={j}>{j}</code>
              ))}
            </span>
            <span role="cell" className="is-num">
              {fmt.format(g.executed)}
            </span>
            <span role="cell">
              <span className="mk-app-pill mk-app-pill--completed">
                {g.emulated ? "Green on an emulator" : "Green, none skipped"}
              </span>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function ProofsShowcase({ caption }: { caption: string }) {
  const narrow = useNarrow();
  if (narrow) {
    return (
      <div className="mk-app-phone">
        <PanelWindow path="proofs" caption={caption} designWidth={PHONE_WIDTH}>
          <ProofsLedger compact />
        </PanelWindow>
      </div>
    );
  }
  return (
    <AppWindow path="proofs" active="proofs" caption={caption} designWidth={1280}>
      <ProofsLedger />
    </AppWindow>
  );
}

