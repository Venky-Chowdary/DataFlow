import { CalendarClock, Database, FileSearch, GitCompareArrows, KeyRound, ListFilter, ShieldCheck, Trash2 } from "lucide-react";
import { MirrorDeletePanel, TickDetailPanel } from "../../components/marketing/ProductPanels";
import {
  CannotList,
  CdcSources,
  ConfirmChecks,
  CoverageTable,
  CutoverTable,
  DriftReview,
  GreenVsCounted,
  GuardLayers,
  MappingDetail,
  McpKeyPanel,
  McpLanes,
  MIGRATION_RUN,
  ModeSelector,
  PilotLoop,
  PrecisionSpecimen,
  ProofPackApi,
  QueryCompare,
  RefusalPanel,
  RiskRecordPanel,
  RUN,
  SignoffPanel,
  TransferSteps,
  TypeMapping,
  UpsertCheck,
  ValidateFindings,
} from "../../components/marketing/StoryPanels";
import { SYNC_MODE_COUNT_WORD } from "../../lib/syncModesPhrase";
import type { PublicRoute } from "../../lib/publicNavigation";
import { JobsWithTrust, McpScene, PilotScene, QueryScene, SchedulesScene, SyncScene, TransferScene, WarehouseScene } from "../../components/marketing/StoryScenes";
import { FactGrid, FeatureRow, RulesTable, StoryFrame, StoryHero, TableSection, type StoryAction } from "./StoryPage";

/*
 * Product and solution pages. Every claim here is something the product does
 * today; the rules tables say where a reader can confirm it. Frames are
 * examples, labelled as such inside the frame.
 *
 * Every page has the same structure: a left-aligned hero with one light
 * product frame, three to five sections (feature rows that alternate sides,
 * a grid of facts, or a table), related links, and the closing section.
 * The home page's run (48,213,906 read) appears here on Job space only; the
 * five-step story appears on Transfer Studio only.
 */

type Nav = (r: PublicRoute) => void;
type PageProps = { onGetStarted: () => void; onNavigate: Nav };

function actions(p: PageProps, secondary: StoryAction) {
  return { primary: { label: "Start free", onClick: p.onGetStarted }, secondary };
}

export function TransferStudioPage(p: PageProps) {
  const act = actions(p, { label: "See Job space", onClick: () => p.onNavigate("product-jobs") });
  const how = [
    { step: "Plan", title: "Map by meaning", body: "Confident pairs map themselves. Ambiguous ones wait for a person, with the reason on the row." },
    { step: "Validate", title: "Check on the real write path", body: "Sample rows go through the real writer. Approving records a hash of the plan." },
    { step: "Execute", title: "Write, and hold what fails", body: "A run whose plan no longer matches is refused. A refused row is held with its reason." },
    { step: "Verify", title: "Read it back", body: "The destination counts what landed and compares it. The Trust Report names the comparison that ran." },
  ];
  return (
    <StoryFrame
      page="ts"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <StoryHero id="mk-ts-title" title={
            <>
              Plan the load. Check it.
              <br /> Then write it.
            </>
          } lead="Approve how every column lands, and clear the checks before a single row is written." actions={act}>
          <TransferScene example="transfer" />
        </StoryHero>
      }
      related={[
        { route: "product-jobs", label: "Job space", body: "Open the finished run and its Trust Report" },
        { route: "product-pipelines", label: "Schedules", body: "Run the approved plan on a cadence" },
        { route: "solution-migrations", label: "Migrations", body: "Cut over between schemas with evidence" },
      ]}
      closing={{ title: "Plan a transfer on your own data", body: "Start with a free workspace and approve your first plan." }}
    >
      <FeatureRow id="mk-ts-fail" title="Most loads fail at the write" body="A cast that truncates, a column nobody owns. Validate finds both before the first row moves.">
        <ValidateFindings />
      </FeatureRow>

      <TableSection id="mk-ts-how" title="From a map to a write" body="Each step has to pass before the next one unlocks.">
        <TransferSteps points={how} />
      </TableSection>

      <FeatureRow id="mk-ts-why" flip title="See why each column was paired" body="Open any row in Map: the reason, the confidence, and a sample.">
        <MappingDetail />
      </FeatureRow>

      <TableSection id="mk-ts-rules" title="Rules you can hold it to" body="Each rule, and where it is written down.">
        <RulesTable
          caption="Transfer Studio rules, and where to check each one"
          items={[
            { claim: "The same core checks run on every route, plus the ones the route needs", where: "Validate step; Docs, Verification" },
            { claim: "A source column must be mapped or declared omitted, or the run is refused", where: "Docs, Verification: dropping fields on purpose" },
            { claim: "Accepting a known loss takes a signed risk record, never a yes/no flag", where: "Docs, Verification: accepting a known loss" },
            { claim: "In strict mode, mappings below 0.85 confidence block until reviewed", where: "Docs, Verification: validation modes" },
          ]}
        />
      </TableSection>
    </StoryFrame>
  );
}

export function JobTheaterPage(p: PageProps) {
  const act = actions(p, { label: "See Transfer Studio", onClick: () => p.onNavigate("product-transfer") });
  return (
    <StoryFrame
      page="jobs"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <StoryHero id="mk-jobs-title" title="Every run, checked against its destination" lead="Open a finished run: what was read, what landed, what was held, and the comparison that shows it." actions={act}>
          <JobsWithTrust initial="6af912d7" caption="Example workspace: six runs, each with the comparison that ran. sales_orders is the home page's run." />
        </StoryHero>
      }
      related={[
        { route: "customers", label: "Proof", body: "CI evidence and proof packs you can verify" },
        { route: "product-transfer", label: "Transfer Studio", body: "Where the plan is made and approved" },
        { route: "security", label: "Security", body: "Audit chain, keys, and isolation" },
      ]}
      closing={{ title: "Open a run you can sign", body: "Start a workspace and run a transfer; the Trust Report comes with every job." }}
    >
      <FeatureRow id="mk-jobs-green" title="A green job is not proof" body="The writer's count says what it sent. Job space reads the destination back and counts what it holds.">
        <GreenVsCounted />
      </FeatureRow>

      <TableSection id="mk-jobs-ladder" title="Each table names the comparison that ran" body="Only a comparison of every row is called proven.">
        <CoverageTable />
      </TableSection>

      <FeatureRow id="mk-jobs-sign" flip title="Sign the report, not a spreadsheet" body="A Trust Report carries the statement, the check that ran and the verdict, and goes to sign-off from Job space.">
        <SignoffPanel title="Trust Report, sign-off" meta="Example rows, the home page's run" counts={RUN} />
      </FeatureRow>

      <FeatureRow id="mk-jobs-evid" title="Evidence you can verify yourself" body="Download a run's proof pack and check it without trusting the screen.">
        <ProofPackApi />
      </FeatureRow>
    </StoryFrame>
  );
}

export function PipelinesPage(p: PageProps) {
  const act = actions(p, { label: "Recurring sync guide", onClick: () => p.onNavigate("solution-sync") });
  return (
    <StoryFrame
      page="pipe"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <StoryHero id="mk-pipe-title" title="Recurring loads, held to the first run's standard" lead="Every tick is a full job, with the same checks and read-back as a run started by hand." actions={act}>
          <SchedulesScene />
        </StoryHero>
      }
      related={[
        { route: "solution-sync", label: "Recurring sync", body: "Keep two systems in step" },
        { route: "product-jobs", label: "Job space", body: "Every tick's evidence" },
        { route: "product-mcp", label: "MCP server", body: "Agents can stage schedules for you to confirm" },
      ]}
      closing={{ title: "Put a transfer on a schedule", body: "Approve a transfer once, then let it run on a cadence with the same checks." }}
    >
      <FeatureRow id="mk-pipe-drift" title="The first run gets reviewed. So does the hundredth." body="A new or renamed source column pauses the schedule until someone decides where it belongs.">
        <DriftReview />
      </FeatureRow>

      <FeatureRow
        id="mk-pipe-modes"
        flip
        title={`${SYNC_MODE_COUNT_WORD[0].toUpperCase()}${SYNC_MODE_COUNT_WORD.slice(1)} ways to run a tick`}
        body="Each mode says what it needs. Unknown mode names are refused."
      >
        <ModeSelector />
      </FeatureRow>

      <FeatureRow id="mk-pipe-tick" title="Open any tick" body="Its map version, the checks it cleared, and its statement.">
        <TickDetailPanel />
      </FeatureRow>

      <TableSection id="mk-pipe-rules" title="Where each rule is written down" body="The rules a schedule runs under, and the docs page for each.">
        <RulesTable
          caption="Schedule rules, and the docs page for each"
          whereLabel="Docs"
          items={[
            { claim: "Mirror deletes are soft by default: the row is kept with _deleted and _deleted_at", where: "Sync modes: mirror" },
            { claim: "Before an append, the destination must match the fingerprint left by the last run", where: "Sync modes: append" },
            { claim: "Keyed modes absorb redelivery: a second write updates the same row", where: "Sync modes" },
            { claim: "One job at a time reads a given replication slot or binlog position", where: "Sync modes: CDC" },
          ]}
        />
      </TableSection>
    </StoryFrame>
  );
}

export function QueryPlaygroundPage(p: PageProps) {
  const act = actions(p, { label: "See Pilot", onClick: () => p.onNavigate("product-pilot") });
  return (
    <StoryFrame
      page="q"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <StoryHero id="mk-q-title" title="Look at the source and the copy, read-only" lead="The same SQL against any saved connection. Writes are refused before they reach the database." actions={act}>
          <QueryScene example="customers" />
        </StoryHero>
      }
      related={[
        { route: "product-pilot", label: "Pilot", body: "Ask in plain language instead" },
        { route: "product-jobs", label: "Job space", body: "The comparison Datawrap already ran" },
        { route: "integrations", label: "Connectors", body: "What you can connect" },
      ]}
      closing={{ title: "Check a copy without a second console", body: "Start a workspace, save two connections, and ask the same question of both." }}
    >
      <FeatureRow id="mk-q-both" title="Ask both sides the same question" body="Run one statement on the source and on the copy, and set the answers side by side.">
        <QueryCompare />
      </FeatureRow>

      <FeatureRow id="mk-q-guards" flip title="Three guards between a statement and your data" body="Pick a statement and see where it stops.">
        <GuardLayers />
      </FeatureRow>

      <FeatureRow id="mk-q-refused" title="A write never reaches the database" body="Statements that write are refused in the editor, with the reason, before anything is sent.">
        <RefusalPanel />
      </FeatureRow>

      <FactGrid
        id="mk-q-will"
        title="What Query will and will not do"
        body="It reads. It never writes, and it runs as you."
        items={[
          { icon: ShieldCheck, title: "Read-only on the server", body: "On PostgreSQL, MySQL, SQLite and Oracle, statements run in a read-only transaction." },
          { icon: KeyRound, title: "Your role, saved connections", body: "Query runs under your workspace role, on saved connections. Credentials stay in the vault." },
          { icon: ListFilter, title: "Capped by a row limit", body: "Results stop at the row limit unless you raise it, so a broad read stays small." },
        ]}
      />
    </StoryFrame>
  );
}

export function DataPilotPage(p: PageProps) {
  const act = actions(p, { label: "See MCP server", onClick: () => p.onNavigate("product-mcp") });
  return (
    <StoryFrame
      page="pilot"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <StoryHero id="mk-pilot-title" title="Ask about your data, and see how the answer was found" lead="Pilot answers by running tools against your workspace, and stages any change for you to confirm." actions={act}>
          <PilotScene example="users" />
        </StoryHero>
      }
      related={[
        { route: "product-mcp", label: "MCP server", body: "The same tools for Cursor and Claude" },
        { route: "product-query", label: "Query", body: "Write the SQL yourself" },
        { route: "product-jobs", label: "Job space", body: "The evidence Pilot reads from" },
      ]}
      closing={{ title: "Ask your workspace a question", body: "Start free and ask Pilot about a saved connection or a finished job." }}
    >
      <FeatureRow
        id="mk-pilot-loop"
        title="Answers come from tool results, not recollection"
        body="Each tool is checked against the same permission as the matching API route. Pilot and the MCP server share one registry."
      >
        <PilotLoop />
      </FeatureRow>

      <FactGrid
        id="mk-pilot-ask"
        title="What you can ask about"
        body="Pilot answers from your workspace, under your role."
        items={[
          { icon: Database, title: "A saved connection", body: "It runs a read-only query under your role, and shows you the SQL it ran." },
          { icon: FileSearch, title: "A finished job", body: "It reads the run's statement and Trust Report, and answers from what they say." },
          { icon: CalendarClock, title: "A change you want", body: "It stages a transfer or a schedule for you to confirm. Nothing runs before you do." },
        ]}
      />

      <FeatureRow
        id="mk-pilot-confirm"
        flip
        title="Nothing changes until you press Confirm"
        body="A change is staged with a preview. Confirm checks your permission again and runs it once."
        more={
          <dl className="mk-pilot-fig">
            <div>
              <dt>An approval expires after</dt>
              <dd>15 minutes</dd>
            </div>
            <div>
              <dt>and is good for</dt>
              <dd>1 use</dd>
            </div>
          </dl>
        }
      >
        <ConfirmChecks />
      </FeatureRow>

      <TableSection id="mk-pilot-cannot" title="No tool exists for these" body="Pilot can word a result. It cannot change one.">
        <CannotList />
      </TableSection>
    </StoryFrame>
  );
}

const MCP_TOOLS: { tool: string; perm: string; effect: "read" | "plan" | "mutate" }[] = [
  { tool: "list_connectors", perm: "connector.read", effect: "read" },
  { tool: "explain_parity", perm: "job.read", effect: "read" },
  { tool: "run_query", perm: "query.use", effect: "read" },
  { tool: "plan_transfer", perm: "job.plan", effect: "plan" },
  { tool: "start_transfer", perm: "job.run", effect: "mutate" },
  { tool: "replay_quarantine", perm: "job.manage", effect: "mutate" },
  { tool: "create_schedule", perm: "schedule.manage", effect: "mutate" },
];

const EFFECT: Record<"read" | "plan" | "mutate", { name: string; does: string }> = {
  read: { name: "Read", does: "Reads workspace state. Changes nothing." },
  plan: { name: "Plan", does: "Produces a proposal. Changes nothing." },
  mutate: { name: "Staged", does: "Returns an ack_id and a preview. Does not act." },
};

export function McpServerPage(p: PageProps) {
  const act = actions(p, { label: "Security overview", onClick: () => p.onNavigate("security") });
  return (
    <StoryFrame
      page="mcp"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <StoryHero id="mk-mcp-title" title="Give agents the same tools, and keep the gates" lead="Agents connect with a workspace API key. They can plan and stage work; nothing that changes data runs until it is confirmed." actions={act}>
          <McpScene />
        </StoryHero>
      }
      related={[
        { route: "product-pilot", label: "Pilot", body: "The same tools, inside the app" },
        { route: "security", label: "Security", body: "Keys, roles, and the audit chain" },
        { route: "enterprise", label: "Enterprise", body: "SSO, SCIM, and deployment options" },
      ]}
      closing={{ title: "Connect an agent under a role", body: "Create a workspace API key with the role you choose, then add the server URL to your client." }}
    >
      <FeatureRow id="mk-mcp-lanes" title="Stage, then confirm" body="What passes between an agent and Datawrap when the agent asks for a change.">
        <McpLanes />
      </FeatureRow>

      <TableSection id="mk-mcp-tools" title="Every tool needs the permission its API route needs" body="An excerpt of the tool list in the docs.">
        <table className="mk-sb-table mk-mcp-tools">
          <caption className="mk-sr-only">MCP tools, the permission each needs, and what it does (an excerpt)</caption>
          <thead>
            <tr>
              <th scope="col">Tool</th>
              <th scope="col">Permission</th>
              <th scope="col">Kind</th>
              <th scope="col">What it does</th>
            </tr>
          </thead>
          <tbody>
            {MCP_TOOLS.map((t) => (
              <tr key={t.tool}>
                <th scope="row">
                  <code>{t.tool}</code>
                </th>
                <td>
                  <code>{t.perm}</code>
                </td>
                <td>
                  <span className="mk-status">{EFFECT[t.effect].name}</span>
                </td>
                <td>{EFFECT[t.effect].does}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </TableSection>

      <FeatureRow id="mk-mcp-key" flip title="A URL and a key, never a database password" body="The agent holds a workspace API key with the role you chose. Every call it makes is on record.">
        <McpKeyPanel />
      </FeatureRow>

      <TableSection id="mk-mcp-setup" title="The confirmation model" body="Each rule, and where the docs state it.">
        <RulesTable
          caption="The MCP confirmation model, and where the docs state each rule"
          items={[
            { claim: "A staged action does nothing until confirm_action is called with its ack_id", where: "Docs, MCP: the confirmation model" },
            { claim: "An approval expires 15 minutes after staging and is used once", where: "Docs, MCP" },
            { claim: "At confirm time, the permission for that action is checked again", where: "Docs, MCP" },
            { claim: "An API key never acts with more than the person who created it holds today", where: "Docs, MCP: permissions" },
          ]}
        />
      </TableSection>
    </StoryFrame>
  );
}

export function MigrationsSolutionPage(p: PageProps) {
  const act = actions(p, { label: "Request a pilot", onClick: () => p.onNavigate("contact") });
  return (
    <StoryFrame
      page="mig"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <StoryHero id="mk-mig-title" title="Cross-schema cutover you can prove" lead="A map you can review, checks before the write, and a comparison of the destination before anyone cuts over." actions={act}>
          <TransferScene migration initial="Map" example="migration" />
        </StoryHero>
      }
      related={[
        { route: "product-transfer", label: "Transfer Studio", body: "Plan and approve the move" },
        { route: "product-jobs", label: "Job space", body: "The run's evidence" },
        { route: "solution-warehouse", label: "Warehouse loading", body: "Bulk loads into Snowflake, BigQuery, and Iceberg" },
      ]}
      closing={{ title: "Plan your cutover with evidence", body: "Start free with a subset of your data and review the plan before anything is written." }}
    >
      <TableSection id="mk-mig-door" title="Cutover is a one-way door" body="Once applications point at the new system, a missing key is a production incident. Plan it so every step leaves a record.">
        <CutoverTable />
      </TableSection>

      <FeatureRow id="mk-mig-sign" title="Sign the destination, not a spreadsheet of counts" body="The cutover is signed on the Trust Report: the statement, the check that ran, and the verdict.">
        <SignoffPanel title="Trust Report, sign-off" meta="Example cutover load, legacy.orders to CORE.ORDERS" counts={MIGRATION_RUN} />
      </FeatureRow>

      <FeatureRow id="mk-mig-risk" flip title="Accept a known loss on the record" body="A column that narrows needs a signed risk record before the load can go ahead.">
        <RiskRecordPanel />
      </FeatureRow>

      <TableSection id="mk-mig-rules" title="Details that survive real schemas" body="What carries over, and where to see it.">
        <RulesTable
          caption="Migration details, and where to see each one"
          whereLabel="Where to see it"
          items={[
            { claim: "Sequences, foreign keys, CHECK constraints, and indexes carry on create-new, in dependency order", where: "Transfer Studio, Destination: create new table" },
            { claim: "Unicode text keeps its encoding; collation-aware key checks run in Validate", where: "Validate step" },
            { claim: "Timestamps are compared at the precision the destination column holds", where: "Docs, Verification: known causes of a difference" },
            { claim: "Held rows keep column, value, and reason, and replay into the same table", where: "Job space, Quarantine" },
          ]}
        />
      </TableSection>
    </StoryFrame>
  );
}

export function WarehouseSolutionPage(p: PageProps) {
  const act = actions(p, { label: "See Schedules", onClick: () => p.onNavigate("product-pipelines") });
  return (
    <StoryFrame
      page="wh"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <StoryHero id="mk-wh-title" title="Warehouse loads that add up" lead="Snowflake, BigQuery, Apache Iceberg, and S3, ADLS or GCS, each load compared against the destination engine afterwards." actions={act}>
          <WarehouseScene />
        </StoryHero>
      }
      related={[
        { route: "solution-sync", label: "Recurring sync", body: "Keep the warehouse current" },
        { route: "integrations", label: "Connectors", body: "Warehouses, lakes, and object storage" },
        { route: "product-jobs", label: "Job space", body: "Each load's evidence" },
      ]}
      closing={{ title: "Load a warehouse on your own data", body: "Start free on the same engine your production loads will use." }}
    >
      <FeatureRow id="mk-wh-prec" title="A load can succeed and still lose a cent" body="Bulk jobs report success when they finish, not when every value is right. Validate checks the fit first.">
        <PrecisionSpecimen />
      </FeatureRow>

      <FeatureRow id="mk-wh-types" flip title="Exact types, the same map as any route" body="Decimals and timestamps keep their precision, and JSON lands as a semi-structured column.">
        <TypeMapping />
      </FeatureRow>

      <FeatureRow id="mk-wh-upsert" title="Upserts by key, checked first" body="Duplicate keys already in the destination block an upsert at Validate, before the load window.">
        <UpsertCheck />
      </FeatureRow>

      <TableSection id="mk-wh-rules" title="Warehouse specifics" body="How each warehouse connects and how upserts are written.">
        <RulesTable
          caption="Warehouse specifics, and where to check each one"
          ruleLabel="Detail"
          items={[
            { claim: "Snowflake connects with a password or key-pair authentication", where: "Connectors, Snowflake" },
            { claim: "Iceberg connects to a REST catalog or a filesystem warehouse", where: "Connectors, Apache Iceberg" },
            { claim: "Upserts use MERGE, or a keyed delete and insert, with no index created", where: "Docs, Sync modes: upsert" },
            { claim: "Duplicate keys already in the destination block an upsert", where: "Docs, Sync modes: upsert" },
          ]}
        />
      </TableSection>
    </StoryFrame>
  );
}

export function SyncSolutionPage(p: PageProps) {
  const act = actions(p, { label: "See Schedules", onClick: () => p.onNavigate("product-pipelines") });
  const fixes: { fails: string; holds: string }[] = [
    { fails: "Deletes never arrive: an incremental read sees new and changed rows, not removed ones.", holds: "CDC reads the delete from the database log. Mirror keeps the row and flags it." },
    { fails: "Retries duplicate: a retried batch appends the same rows twice.", holds: "Keyed modes update the same row on a second write." },
    { fails: "A column appears upstream and flows straight through.", holds: "It waits in Map. The next run uses the map you approve." },
  ];
  return (
    <StoryFrame
      page="sync"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <StoryHero id="mk-sync-title" title="Keep two systems in step" lead="Run an approved transfer on a schedule, with CDC or a typed cursor. Every run is a checked job with its own statement." actions={act}>
          <SyncScene />
        </StoryHero>
      }
      related={[
        { route: "product-pipelines", label: "Schedules", body: "Cadence, history, and drift review" },
        { route: "solution-warehouse", label: "Warehouse loading", body: "Bulk loads into a warehouse" },
        { route: "product-jobs", label: "Job space", body: "Each run's evidence" },
      ]}
      closing={{ title: "Keep a copy in step", body: "Start free with a schedule on your own connections." }}
    >
      <TableSection id="mk-sync-drift" title="Two systems that agree on Monday disagree by Friday" body="Three ways a copy drifts, and what keeps it in step.">
        <table className="mk-sb-table mk-sync-fixes">
          <caption className="mk-sr-only">How a copy drifts, and what keeps it in step</caption>
          <thead>
            <tr>
              <th scope="col">How a copy drifts</th>
              <th scope="col">What keeps it in step</th>
            </tr>
          </thead>
          <tbody>
            {fixes.map((f) => (
              <tr key={f.fails}>
                <td>{f.fails}</td>
                <td>{f.holds}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </TableSection>

      <FeatureRow id="mk-sync-delete" title="A delete at the source, carried to the copy" body="Soft by default: the row is kept and flagged, so the copy shows what was removed and when.">
        <MirrorDeletePanel />
      </FeatureRow>

      <FactGrid
        id="mk-sync-read"
        title="Choose how each run reads"
        body="The mode is set once, in the approved transfer."
        items={[
          { icon: GitCompareArrows, title: "CDC", body: "Reads changes from the database log, deletes included, and applies them by key." },
          { icon: ListFilter, title: "A typed cursor", body: "Reads new and changed rows past the last run's cursor, and updates them in place by key." },
          { icon: Trash2, title: "Mirror", body: "Flags rows gone from the source with _deleted and _deleted_at, and keeps them." },
        ]}
      />

      <TableSection id="mk-sync-cdc" title="CDC from the database log" body="A consistent snapshot first, then changes from the log, deletes included. Proven in CI on these sources:">
        <CdcSources />
        <RulesTable
          caption="What CDC refuses, and where the docs say so"
          ruleLabel="What CDC refuses"
          items={[
            { claim: "CDC refuses sources it does not support at Validate", where: "Docs, Sync modes: CDC" },
            { claim: "If capture cannot attach to a log the server emits, the run fails with the remedy rather than polling", where: "Docs, Sync modes: CDC" },
            { claim: "Append-only destinations such as Kafka topics accept only append modes", where: "Docs, Sync modes: append-only destinations" },
          ]}
        />
      </TableSection>
    </StoryFrame>
  );
}
