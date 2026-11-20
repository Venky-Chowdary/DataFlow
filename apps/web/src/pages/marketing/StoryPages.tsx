import {
  EvidencePanel,
  GatesPanel,
  McpPanel,
  MigrationMapPanel,
  PilotPanel,
  PlanPanel,
  QuarantinePanel,
  QueryPanel,
  RunPanel,
  ScheduleTimeline,
  VerifyPanel,
} from "../../components/marketing/ProductPanels";
import { cdcSourcesPhrase } from "../../lib/provenEvidence";
import type { PublicRoute } from "../../lib/publicNavigation";
import { DEMO_ROUTE, DEMO_SCHEDULE_ROUTE, WORKSPACE_SHOT } from "../../lib/workspaceShots";
import { StoryPage, type Story } from "./StoryPage";

/*
 * Product and solution pages. Every claim here is something the product does
 * today; the "What you can check" rows say where a reader can confirm it.
 * Panels are examples, labelled as such inside the panel.
 */

type Nav = (r: PublicRoute) => void;
type PageProps = { onGetStarted: () => void; onNavigate: Nav };

const DEMO_PAIR = { source: DEMO_ROUTE.source, dest: DEMO_ROUTE.dest };

function closing(_p: PageProps, title: string, body: string): Story["closing"] {
  return { title, body };
}

export function TransferStudioPage(p: PageProps) {
  const story: Story = {
    steps: ["Plan", "Validate"],
    kicker: "Transfer Studio",
    title: "Plan the load. Check it. Then write it.",
    lead: "Transfer Studio is where a person plans a transfer: choose a source and a destination, approve how every column lands, and clear the checks before a single row is written.",
    primary: { label: "Start free", onClick: p.onGetStarted },
    secondary: { label: "See Job space", onClick: () => p.onNavigate("product-jobs") },
    hero: <GatesPanel />,
    problem: {
      title: "Most migrations fail at the write",
      body: "The outcome of a load is usually decided before the first row moves: a type that cannot hold a value, a key that is not unique, a column nobody mapped. Without a plan check, those surface hours into the load or after cutover.",
      failures: [
        { title: "A cast that truncates", body: "A decimal or timestamp that fits the sample but not the destination type loses precision on the real write." },
        { title: "A column nobody owns", body: "A source column that is neither mapped nor declared dropped disappears without a record of why." },
      ],
    },
    how: {
      title: "Four steps, in this order",
      sequence: true,
      points: [
        { title: "Map by meaning", body: "Each pairing is scored on the column's role, qualifier, and type fit. Confident pairs map themselves; ambiguous ones wait for a person, with the reason on the row." },
        { title: "Validate on the real write path", body: "Nine core checks run on every route, plus the policy checks the route needs. Sample rows go through the real write path, so a value the destination cannot hold is a finding." },
        { title: "Approve the plan", body: "Approving records a hash of the plan. A run whose plan no longer matches the approved one is refused." },
        { title: "Write, and hold what fails", body: "Rows are written in batches with bound parameters. A row the destination refuses is held with its column, value, and reason instead of stopping the load." },
      ],
    },
    surface: {
      title: "The map a reviewer approves",
      body: "Every source column is mapped or declared dropped, with its destination type and the transform that gets it there. Open any row to see why it was paired.",
      visual: <PlanPanel />,
      capture: { src: WORKSPACE_SHOT.transferMap, alt: "Transfer Studio Map step with every sales_orders column mapped to its Snowflake type, each marked safe", surface: "Transfer Studio, map step", route: DEMO_PAIR },
    },
    checks: {
      title: "Rules you can hold it to",
      items: [
        { claim: "Nine core checks, G1 to G9, run on every route", where: "Validate step; Docs, Verification" },
        { claim: "A source column must be mapped or declared omitted, or the run is refused", where: "Docs, Verification: dropping fields on purpose" },
        { claim: "Accepting a known loss takes a signed risk record, never a yes/no flag", where: "Docs, Verification: accepting a known loss" },
        { claim: "In strict mode, mappings below 0.85 confidence block until reviewed", where: "Docs, Verification: validation modes" },
      ],
    },
    related: [
      { route: "product-jobs", label: "Job space", body: "Open the finished run and its Trust Report" },
      { route: "product-pipelines", label: "Schedules", body: "Run the approved plan on a cadence" },
      { route: "solution-migrations", label: "Migrations", body: "Cut over between schemas with evidence" },
    ],
    closing: closing(p, "Plan a transfer on your own data", "Start with a free workspace and approve your first plan."),
  };
  return <StoryPage story={story} onNavigate={p.onNavigate} />;
}

export function JobTheaterPage(p: PageProps) {
  const story: Story = {
    steps: ["Verify", "Evidence"],
    kicker: "Job space",
    title: "Every run, checked against its destination",
    lead: "Job space is where you open a finished run: what was read, what landed, what was held, and the comparison that shows it, ready for sign-off.",
    primary: { label: "Start free", onClick: p.onGetStarted },
    secondary: { label: "See Transfer Studio", onClick: () => p.onNavigate("product-transfer") },
    hero: <VerifyPanel />,
    problem: {
      title: "A green job is not proof",
      body: "A job can finish without an error while the destination is short or a column was nulled. The writer's count says what it sent, not what the destination holds.",
      failures: [
        { title: "Counting what was sent", body: "An acknowledgement from the writer closes the books on rows the destination may never have kept." },
        { title: "A sample called proof", body: "Comparing a sample can find a problem. It cannot show that every row is right." },
      ],
    },
    how: {
      title: "How a run is checked",
      points: [
        { title: "Counted by the destination's engine", body: "After the write, Datawrap queries the destination itself and compares it with what was read." },
        { title: "Checksum and census", body: "Row counts, an order-independent checksum over every row, and a value census by key, as far as the destination allows." },
        { title: "A verdict that names its evidence", body: "Proven, explained, no difference found, unexplained, or unverifiable. Only a full comparison is called proven." },
        { title: "Reversibility and sign-off", body: "Column by column: can the destination give the source back? Engineering, the data owner, and compliance sign the same report." },
      ],
    },
    surface: {
      title: "Held rows stay on the job",
      body: "A row the destination refused is listed with its column, value, and reason. Fix the cause, replay it through the same writer, and the accounting closes.",
      visual: <QuarantinePanel />,
      capture: { src: WORKSPACE_SHOT.jobTrustEvidence, alt: "Trust Report evidence for a run into Snowflake: reconciliation passed, full checksum assurance, and the sign-off and evidence pack", surface: "Job space, Trust Report", route: DEMO_PAIR },
    },
    checks: {
      title: "Evidence you can verify yourself",
      items: [
        { claim: "Export a signed proof pack for any job", where: "API: GET /api/v1/transfer/{job_id}/proof-pack" },
        { claim: "Verify a proof pack independently", where: "API: POST /api/v1/transfer/proof-pack/verify" },
        { claim: "A pack claims \"migration proven\" only after a full checksum", where: "Docs, Verification: evidence you can export" },
        { claim: "Each table carries the comparison that ran", where: "Trust Report: full checksum, key diff, sample, or count" },
      ],
    },
    related: [
      { route: "customers", label: "Proof", body: "CI evidence and proof packs you can verify" },
      { route: "product-transfer", label: "Transfer Studio", body: "Where the plan is made and approved" },
      { route: "security", label: "Security", body: "Audit chain, keys, and isolation" },
    ],
    closing: closing(p, "Open a run you can sign", "Start a workspace and run a transfer; the Trust Report comes with every job."),
  };
  return <StoryPage story={story} onNavigate={p.onNavigate} />;
}

export function PipelinesPage(p: PageProps) {
  const story: Story = {
    steps: ["Execute"],
    kicker: "Schedules",
    title: "Recurring loads, held to the first run's standard",
    lead: "A schedule runs an approved transfer on a cadence. Every tick is a full job with the same checks and the same read-back as a transfer started by hand.",
    primary: { label: "Start free", onClick: p.onGetStarted },
    secondary: { label: "Recurring sync guide", onClick: () => p.onNavigate("solution-sync") },
    hero: <ScheduleTimeline />,
    problem: {
      title: "Recurring loads drift quietly",
      body: "The first load gets reviewed. The hundredth runs at night, against a source that has changed since.",
      failures: [
        { title: "Changes an incremental cannot see", body: "A row changed without its cursor moving is missed, and incremental reads never see deletes." },
        { title: "Schema change that reaches the destination", body: "A new or renamed column flows into the destination before anyone has decided where it belongs." },
      ],
    },
    how: {
      title: "What every tick does",
      points: [
        { title: "Runs as a job", body: "Each tick has its own Trust Report, with the same checks and read-back as a run started by hand." },
        { title: "Uses the mode you chose", body: "Full refresh, append, upsert, mirror, incremental by a typed cursor, SCD Type 2, or CDC. Unknown mode names are refused." },
        { title: "Reads the log for CDC", body: `CDC is proven in CI on ${cdcSourcesPhrase()}. Inserts, updates, and deletes apply by key.` },
        { title: "Waits for review on drift", body: "A new or renamed source column waits in Map for review before the next run uses it." },
      ],
    },
    surface: {
      title: "The run history, tick by tick",
      body: "Each run links to its job and its evidence. A paused tick says why it paused and what it is waiting for.",
      capture: { src: WORKSPACE_SHOT.schedulesHistory, alt: "Run history for the Orders feed schedule, each run linked to its job", surface: "Schedules, run history", route: { source: DEMO_SCHEDULE_ROUTE.source, dest: DEMO_SCHEDULE_ROUTE.dest } },
    },
    checks: {
      title: "Behaviour you can rely on",
      items: [
        { claim: "Mirror deletes are soft by default: the row is kept with _deleted and _deleted_at", where: "Docs, Sync modes: mirror" },
        { claim: "Before an append, the destination must match the fingerprint left by the last run", where: "Docs, Sync modes: append and continuity" },
        { claim: "Keyed modes absorb redelivery: a second write updates the same row", where: "Docs, Sync modes" },
        { claim: "One job at a time reads a given replication slot or binlog position", where: "Docs, Sync modes: CDC" },
      ],
    },
    related: [
      { route: "solution-sync", label: "Recurring sync", body: "Keep two systems in step" },
      { route: "product-jobs", label: "Job space", body: "Every tick's evidence" },
      { route: "product-mcp", label: "MCP server", body: "Agents can stage schedules for you to confirm" },
    ],
    closing: closing(p, "Put a transfer on a schedule", "Approve a transfer once, then let it run on a cadence with the same checks."),
  };
  return <StoryPage story={story} onNavigate={p.onNavigate} />;
}

export function QueryPlaygroundPage(p: PageProps) {
  const story: Story = {
    steps: ["Verify"],
    kicker: "Query",
    title: "Look at the source and the copy, read-only",
    lead: "Run the same read-only SQL against any saved connection, so checking a copy takes one query, not two consoles and two sets of credentials.",
    primary: { label: "Start free", onClick: p.onGetStarted },
    secondary: { label: "See Pilot", onClick: () => p.onNavigate("product-pilot") },
    hero: <QueryPanel />,
    problem: {
      title: "Checking a copy is a risk of its own",
      body: "Spot-checking a migration usually means two database consoles, two credentials, and a terminal that can write.",
      failures: [
        { title: "Credentials spread around", body: "Every person who checks a copy needs their own way into both systems." },
        { title: "A stray write", body: "An UPDATE typed into the wrong tab changes the data you were trying to check." },
      ],
    },
    how: {
      title: "How Query keeps it safe",
      points: [
        { title: "Saved connections", body: "Query uses the connections already in your workspace, under your role. Credentials stay in the vault." },
        { title: "Read-only in the editor", body: "One statement, a read-only head, no write keyword. Anything else is refused before it reaches the database." },
        { title: "Read-only on the server too", body: "On PostgreSQL, MySQL, SQLite, and Oracle the statement runs in a transaction the server itself keeps read-only." },
        { title: "A row limit by default", body: "Results are capped unless you raise the limit, so a wide table does not flood the page." },
      ],
    },
    surface: {
      title: "The same question, asked of both sides",
      body: "Run an aggregate on the copy, switch the connection, and run it on the source. A write is refused with the reason.",
      capture: { src: WORKSPACE_SHOT.queryReadonly, alt: "Query refuses a DELETE statement: only read and metadata queries run in the playground", surface: "Query, read-only" },
    },
    checks: {
      title: "What Query will and will not do",
      items: [
        { claim: "Statements that write are refused in the editor", where: "Query: try any INSERT, UPDATE, or DELETE" },
        { claim: "Server-side read-only transaction on PostgreSQL, MySQL, SQLite, and Oracle", where: "Query on a saved connection of those types" },
        { claim: "Runs under your workspace role", where: "Settings, Team: roles" },
      ],
    },
    related: [
      { route: "product-pilot", label: "Pilot", body: "Ask in plain language instead" },
      { route: "product-jobs", label: "Job space", body: "The comparison Datawrap already ran" },
      { route: "integrations", label: "Connectors", body: "What you can connect" },
    ],
    closing: closing(p, "Check a copy without a second console", "Start a workspace, save two connections, and ask the same question of both."),
  };
  return <StoryPage story={story} onNavigate={p.onNavigate} />;
}

export function DataPilotPage(p: PageProps) {
  const story: Story = {
    steps: ["Plan", "Verify"],
    kicker: "Pilot",
    title: "Ask about your data, and see how the answer was found",
    lead: "Pilot answers questions about connectors, jobs, and data by running tools against your workspace, and stages any change for you to confirm.",
    primary: { label: "Start free", onClick: p.onGetStarted },
    secondary: { label: "See MCP server", onClick: () => p.onNavigate("product-mcp") },
    hero: <PilotPanel />,
    problem: {
      title: "Answers about data should not be guesses",
      body: "An assistant that answers from memory, or acts on its own, is not something a data team can put in front of production systems.",
      failures: [
        { title: "A plausible number", body: "A model's recollection of a table is not a count of the table." },
        { title: "An action nobody approved", body: "An assistant that can start a transfer on its own can start the wrong one." },
      ],
    },
    how: {
      title: "How Pilot works",
      points: [
        { title: "Answers by running tools", body: "Pilot and the MCP server share one tool registry. Each tool is checked against the same permission as the matching API route." },
        { title: "Shows its work", body: "Counts and aggregates run on the server, with the SQL shown under the answer." },
        { title: "Stages changes for Confirm", body: "A tool that would change something stages the action and stops. An approval expires after 15 minutes and is used once." },
        { title: "Leaves decisions to the engine", body: "Mappings come from the mapping engine and verdicts from the deterministic checks. Pilot can explain a result; it cannot change one." },
      ],
    },
    surface: {
      title: "The answer and the query behind it",
      body: "Every number Pilot gives comes with the statement it ran and the connection it ran on.",
    },
    checks: {
      title: "Limits that are part of the design",
      items: [
        { claim: "Nothing changes until you press Confirm", where: "Docs, Pilot: what Pilot cannot do" },
        { claim: "Approvals expire after 15 minutes and are used once", where: "Docs, Pilot" },
        { claim: "Queries are read-only", where: "Docs, Pilot" },
        { claim: "No tool exists to change members, roles, API keys, or to sign a Trust Report", where: "Docs, Pilot" },
      ],
    },
    related: [
      { route: "product-mcp", label: "MCP server", body: "The same tools for Cursor and Claude" },
      { route: "product-query", label: "Query", body: "Write the SQL yourself" },
      { route: "product-jobs", label: "Job space", body: "The evidence Pilot reads from" },
    ],
    closing: closing(p, "Ask your workspace a question", "Start free and ask Pilot about a saved connection or a finished job."),
  };
  return <StoryPage story={story} onNavigate={p.onNavigate} />;
}

const MCP_TOOLS: { tool: string; perm: string; effect: string }[] = [
  { tool: "list_connectors", perm: "connector.read", effect: "Read" },
  { tool: "plan_transfer", perm: "job.plan", effect: "Plan" },
  { tool: "explain_parity", perm: "job.read", effect: "Read" },
  { tool: "start_transfer", perm: "job.run", effect: "Staged for Confirm" },
  { tool: "replay_quarantine", perm: "job.manage", effect: "Staged for Confirm" },
  { tool: "create_schedule", perm: "schedule.manage", effect: "Staged for Confirm" },
];

function McpToolTable() {
  return (
    <figure className="mk-panel">
      <div className="mk-panel-head">
        <div>
          <p className="mk-panel-title">Tools and permissions</p>
          <p className="mk-panel-meta">An excerpt of the tool list in the docs</p>
        </div>
      </div>
      <table className="mk-panel-table">
        <caption>MCP tools, the permission each needs, and its effect</caption>
        <thead>
          <tr>
            <th scope="col">Tool</th>
            <th scope="col">Permission</th>
            <th scope="col">Effect</th>
          </tr>
        </thead>
        <tbody>
          {MCP_TOOLS.map((t) => (
            <tr key={t.tool}>
              <th scope="row"><code>{t.tool}</code></th>
              <td><code>{t.perm}</code></td>
              <td>{t.effect}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </figure>
  );
}

export function McpServerPage(p: PageProps) {
  const story: Story = {
    steps: ["Plan", "Execute"],
    kicker: "MCP server",
    title: "Give agents the same tools, and keep the gates",
    lead: "Cursor, Claude, and VS Code connect to Datawrap's MCP server with a workspace API key. Agents can plan and stage work; nothing that changes data runs until it is confirmed.",
    primary: { label: "Start free", onClick: p.onGetStarted },
    secondary: { label: "Security overview", onClick: () => p.onNavigate("security") },
    hero: <McpPanel />,
    problem: {
      title: "Agents should not hold database passwords",
      body: "Handing an agent direct database access means a password in a config file and writes that skip every check a person would run.",
      failures: [
        { title: "A password in a prompt", body: "Credentials pasted into an agent's context are hard to scope and harder to revoke." },
        { title: "A write that skips the checks", body: "An agent writing straight to a database bypasses mapping review, validation, and the read-back." },
      ],
    },
    how: {
      title: "How the MCP server governs agents",
      points: [
        { title: "Role-scoped API keys", body: "A key carries a role and scopes that never exceed the grants of the person who created it." },
        { title: "Secret references", body: "Tools use saved connections by reference. Credentials never leave the vault." },
        { title: "Staged mutations", body: "A tool that changes something returns an ack_id. The action runs only when confirm_action is called with it, once, after the permission is checked again." },
        { title: "The same engine", body: "Agent transfers run the same map, checks, quarantine, and read-back as Studio, and land in Jobs beside everyone else's." },
      ],
    },
    surface: {
      title: "Every tool, with the permission it needs",
      body: "Read tools answer; plan tools propose; anything that changes data is staged for confirmation.",
      visual: <McpToolTable />,
      capture: { src: WORKSPACE_SHOT.mcp, alt: "MCP Server page: server URL, client setup for Cursor, Claude Desktop, VS Code, and custom GPT", surface: "MCP server" },
    },
    checks: {
      title: "The confirmation model",
      items: [
        { claim: "A staged action does nothing until confirm_action is called with its ack_id", where: "Docs, MCP: the confirmation model" },
        { claim: "An ack_id is used once; a second confirm is refused", where: "Docs, MCP" },
        { claim: "At confirm time, the permission for that action is checked again", where: "Docs, MCP" },
        { claim: "Every agent action is in the audit log, naming the key", where: "Settings, Audit log" },
      ],
    },
    related: [
      { route: "product-pilot", label: "Pilot", body: "The same tools, inside the app" },
      { route: "security", label: "Security", body: "Keys, roles, and the audit chain" },
      { route: "enterprise", label: "Enterprise", body: "SSO, SCIM, and deployment options" },
    ],
    closing: closing(p, "Connect an agent under a role", "Create a workspace API key with the role you choose, then add the server URL to your client."),
  };
  return <StoryPage story={story} onNavigate={p.onNavigate} />;
}

export function MigrationsSolutionPage(p: PageProps) {
  const story: Story = {
    steps: ["Plan", "Validate"],
    kicker: "Migrations",
    title: "Cross-schema cutover you can prove",
    lead: "Move between schemas that do not match name for name, with a map you can review, checks before the write, and a comparison of the destination before you cut over.",
    primary: { label: "Start free", onClick: p.onGetStarted },
    secondary: { label: "Talk to an engineer", onClick: () => p.onNavigate("contact") },
    hero: <MigrationMapPanel />,
    problem: {
      title: "Cutover is a one-way door",
      body: "Once applications point at the new system, a missing key or a truncated value becomes a production incident, and the old system is no longer the source of truth.",
      failures: [
        { title: "Identity that does not survive", body: "Sequences, foreign keys, and constraints that do not carry make the new schema accept rows the old one refused." },
        { title: "A sign-off with nothing behind it", body: "Cutover approved on a spreadsheet of counts that nobody can reproduce." },
      ],
    },
    how: {
      title: "A path from discovery to cutover",
      sequence: true,
      points: [
        { title: "Profile both sides", body: "Datawrap profiles the source and the destination, then proposes a role-aware map instead of assuming names match." },
        { title: "Review the map", body: "Confident pairs map themselves; ambiguous edges wait for a person and nothing is pinned until someone accepts it." },
        { title: "Clear the checks", body: "Contracts, types, capacity, and a dry run must pass before the write. One blocking finding stops the load." },
        { title: "Cut over with proof", body: "Run a subset first, then the full write, and sign the Trust Report before switching applications." },
      ],
    },
    surface: {
      title: "The report the cutover meeting signs",
      body: "The comparison that ran, the held rows, and the sign-offs by role, on one record.",
      visual: <EvidencePanel />,
      capture: { src: WORKSPACE_SHOT.transferMapWhy, alt: "Map step showing JSONB attributes mapped to Snowflake VARIANT with Parse JSON, with its confidence and a sample", surface: "Transfer Studio, why a pair was made" },
    },
    checks: {
      title: "Details that survive real schemas",
      items: [
        { claim: "Sequences, foreign keys, CHECK constraints, and indexes carry on create-new, in dependency order", where: "Transfer Studio, Destination: create new table" },
        { claim: "Unicode text keeps its encoding; collation-aware key checks run in Validate", where: "Validate step" },
        { claim: "Timestamps are compared at the precision the destination column holds", where: "Docs, Verification: known causes of a difference" },
        { claim: "Held rows keep column, value, and reason, and replay into the same table", where: "Job space, Quarantine" },
      ],
    },
    related: [
      { route: "product-transfer", label: "Transfer Studio", body: "Plan and approve the move" },
      { route: "product-jobs", label: "Job space", body: "The run's evidence" },
      { route: "solution-warehouse", label: "Warehouse loading", body: "Bulk loads into Snowflake, BigQuery, and Iceberg" },
    ],
    closing: closing(p, "Plan your cutover with evidence", "Start free with a subset of your data and review the plan before anything is written."),
  };
  return <StoryPage story={story} onNavigate={p.onNavigate} />;
}

export function WarehouseSolutionPage(p: PageProps) {
  const story: Story = {
    steps: ["Execute", "Verify"],
    kicker: "Warehouse loading",
    title: "Warehouse loads finance can archive",
    lead: "Load Snowflake, BigQuery, Apache Iceberg, and S3, ADLS, or GCS with create-new tables and MERGE upserts, each load compared against the destination engine afterwards.",
    primary: { label: "Start free", onClick: p.onGetStarted },
    secondary: { label: "See Schedules", onClick: () => p.onNavigate("product-pipelines") },
    hero: <RunPanel />,
    problem: {
      title: "Bulk loads hide partial failures",
      body: "A warehouse load reports success when the bulk job finishes, not when every row is queryable with the right type.",
      failures: [
        { title: "Precision lost in the cast", body: "Decimals and timestamps that land in a narrower type look fine until a total is off." },
        { title: "Capacity found the hard way", body: "A load window that fails halfway because the destination could not take the volume." },
      ],
    },
    how: {
      title: "How a warehouse load runs",
      sequence: true,
      points: [
        { title: "Probe the destination", body: "Write rights, privileges, and capacity are checked in Validate before the load window opens." },
        { title: "Map and check", body: "The same semantic map and checks as any route, with exact decimals and timestamps." },
        { title: "Write in bulk", body: "Create new tables, append, overwrite, or MERGE by key wherever the destination's keys support it." },
        { title: "Read it back", body: "The warehouse counts and checksums what landed, and the job records which comparison ran." },
      ],
    },
    surface: {
      title: "The comparison on every load",
      body: "Counts from the warehouse engine, the checksum result, and the column census, on the job.",
      visual: <VerifyPanel />,
    },
    checks: {
      title: "Warehouse specifics",
      items: [
        { claim: "Snowflake connects with a password or key-pair authentication", where: "Connectors, Snowflake" },
        { claim: "Iceberg connects to a REST catalog or a filesystem warehouse", where: "Connectors, Apache Iceberg" },
        { claim: "Upserts use MERGE, or a keyed delete and insert, with no index created", where: "Docs, Sync modes: upsert" },
        { claim: "Duplicate keys already in the destination block an upsert", where: "Docs, Sync modes: upsert" },
      ],
    },
    related: [
      { route: "solution-sync", label: "Recurring sync", body: "Keep the warehouse current" },
      { route: "integrations", label: "Connectors", body: "Warehouses, lakes, and object storage" },
      { route: "product-jobs", label: "Job space", body: "Each load's evidence" },
    ],
    closing: closing(p, "Load a warehouse on your own data", "Start free on the same engine your production loads will use."),
  };
  return <StoryPage story={story} onNavigate={p.onNavigate} />;
}

export function SyncSolutionPage(p: PageProps) {
  const story: Story = {
    steps: ["Execute"],
    kicker: "Recurring sync",
    title: "Keep two systems in step",
    lead: "Run an approved transfer on a schedule with CDC or a typed cursor. Every run is a checked job, and schema drift waits for review.",
    primary: { label: "Start free", onClick: p.onGetStarted },
    secondary: { label: "See Schedules", onClick: () => p.onNavigate("product-pipelines") },
    hero: <ScheduleTimeline />,
    problem: {
      title: "Two systems that agree on Monday disagree by Friday",
      body: "Deletes that never propagate, a cursor that skipped a batch, a column that appeared upstream: each is small, and together they make the copy untrustworthy.",
      failures: [
        { title: "Deletes that never arrive", body: "An incremental read sees new and changed rows, not rows removed at the source." },
        { title: "Retries that duplicate", body: "A retried batch appends the same rows twice when nothing keys them." },
      ],
    },
    how: {
      title: "How a sync stays honest",
      points: [
        { title: "CDC from the database log", body: `Proven in CI on ${cdcSourcesPhrase()}: a consistent snapshot, then changes from the log, with deletes carried.` },
        { title: "Mirror with soft deletes", body: "Rows gone from the source are flagged _deleted with a timestamp, and the physical row is kept." },
        { title: "Keys absorb redelivery", body: "Keyed modes update the same row on a second write, so a retry does not duplicate it." },
        { title: "Drift waits for review", body: "A new or renamed column waits in Map, and the next run uses the map you approve." },
      ],
    },
    surface: {
      title: "A history where every run has evidence",
      body: "Each tick links to its job. A paused tick says what it is waiting for.",
      visual: <QuarantinePanel />,
      capture: { src: WORKSPACE_SHOT.schedulesDrawer, alt: "Orders feed schedule detail: every 2 minutes, incremental deduped on order_id", surface: "Schedules, detail", route: { source: DEMO_SCHEDULE_ROUTE.source, dest: DEMO_SCHEDULE_ROUTE.dest } },
    },
    checks: {
      title: "Sync behaviour, stated plainly",
      items: [
        { claim: "CDC refuses sources it does not support at Validate", where: "Docs, Sync modes: CDC" },
        { claim: "If capture cannot attach to a log the server emits, the run fails with the remedy rather than polling", where: "Docs, Sync modes: CDC" },
        { claim: "Append-only destinations such as Kafka topics accept only append modes", where: "Docs, Sync modes: append-only destinations" },
      ],
    },
    related: [
      { route: "product-pipelines", label: "Schedules", body: "Cadence, history, and drift review" },
      { route: "solution-warehouse", label: "Warehouse loading", body: "Bulk loads into a warehouse" },
      { route: "product-jobs", label: "Job space", body: "Each run's evidence" },
    ],
    closing: closing(p, "Keep a copy in step", "Start free with a schedule on your own connections."),
  };
  return <StoryPage story={story} onNavigate={p.onNavigate} />;
}
