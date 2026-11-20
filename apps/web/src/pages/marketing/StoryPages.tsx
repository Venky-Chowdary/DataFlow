import {
  Archive,
  Ban,
  BookOpen,
  Braces,
  Building2,
  CalendarClock,
  Code,
  Database,
  Eye,
  EyeOff,
  FileSearch,
  FileSignature,
  Gauge,
  GitCompareArrows,
  Hash,
  History,
  Info,
  KeyRound,
  ListChecks,
  ListFilter,
  Lock,
  MessageSquare,
  Percent,
  Plug,
  Power,
  RefreshCw,
  Repeat,
  RotateCcw,
  Ruler,
  Scale,
  Server,
  ShieldCheck,
  TableProperties,
  TriangleAlert,
  UserCheck,
  Workflow,
} from "lucide-react";
import { MirrorDeletePanel } from "../../components/marketing/ProductPanels";
import {
  ConfirmChecks,
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
  QueryCompare,
  RiskRecordPanel,
  RUN,
  SignoffPanel,
  ValidateFindings,
} from "../../components/marketing/StoryPanels";
import { SYNC_MODE_COUNT_WORD } from "../../lib/syncModesPhrase";
import type { PublicRoute } from "../../lib/publicNavigation";
import { PRIMARY_CTA_LABEL, SALES_CTA_LABEL } from "../../components/marketing/MarketingChrome";
import { JobsWithTrust, McpScene, PilotScene, QueryScene, SchedulesScene, SyncScene, TransferScene, WarehouseScene } from "../../components/marketing/StoryScenes";
import { BentoSection, CodeBand, FeatureRow, RowPoints, RulesTable, StoryFrame, StoryHero, TableSection, type StoryAction } from "./StoryPage";
import { FeatureLinks } from "../../components/marketing/kit/FeatureLinks";
import { SplitHead } from "../../components/marketing/kit/SplitHead";
import { JobsBento, McpBento, MigrationBento, PilotBento, QueryBento, SchedulesBento, SyncBento, TransferBento, WarehouseBento } from "../../components/marketing/StoryBentos";
import {
  JOBS_FOLLOW,
  JOBS_PROOF_PACK,
  JOBS_TRUST_REPORT,
  MCP_CLIENT_CONFIG,
  MCP_DISCOVER,
  QUICKSTART_EXECUTE,
  QUICKSTART_LOGIN,
  SCHEDULE_EXPORT,
  SCHEDULES_MANIFEST,
} from "./apiSnippets";

/*
 * Product and solution pages. Every claim here is something the product does
 * today; the rules tables say where a reader can confirm it. Frames are
 * examples, labelled as such inside the frame.
 *
 * Every page has the same structure (pass 7): a hero with one product frame,
 * FeatureLinks to related pages, a Bento whose tiles carry fragments of the
 * product UI, feature rows that alternate sides, a code band only where the
 * docs give a real snippet, a rules table where it carries facts found
 * nowhere else on the page, and the closing card. Each claim is said once
 * per page.
 * The home page's run (48,213,906 read) appears here on Job space only; the
 * five-step story appears on Transfer Studio only.
 */

type Nav = (r: PublicRoute) => void;
type PageProps = { onGetStarted: () => void; onNavigate: Nav };

function actions(p: PageProps, secondary: StoryAction) {
  return { primary: { label: PRIMARY_CTA_LABEL, onClick: p.onGetStarted }, secondary };
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
      closing={{ title: "Plan a transfer on your own data", body: "Start with a free workspace and approve your first plan." }}
    >
      <FeatureLinks
        label="Related to Transfer Studio"
        onNavigate={p.onNavigate}
        items={[
          { icon: BookOpen, label: "Transfer Studio guide", body: "Each step, as the docs describe it", route: "help-transfer-studio" },
          { icon: Gauge, label: "Job space", body: "Open the finished run and its Trust Report", route: "product-jobs" },
          { icon: CalendarClock, label: "Schedules", body: "Run the approved plan on a cadence", route: "product-pipelines" },
          { icon: GitCompareArrows, label: "Migrations", body: "Cut over between schemas with evidence", route: "solution-migrations" },
        ]}
      />

      <section className="mk-sb-section mk-kit-sec" aria-labelledby="mk-ts-how">
        <div className="mk-wrap">
          <SplitHead id="mk-ts-how" title="From a map to a write" lead="Each step has to pass before the next one unlocks." />
          <TransferBento points={how} />
        </div>
      </section>

      <FeatureRow
        id="mk-ts-fail"
        title="Most loads fail at the write"
        body="A cast that truncates, a column nobody owns. Validate finds both before the first row moves."
        more={
          <RowPoints
            items={[
              { icon: ListChecks, lead: "Nine core gates", text: "run on every route, plus the gates that route needs." },
              { icon: Lock, lead: "Execute stays locked", text: "until the server returns approve with no gate blocked." },
              { icon: TableProperties, lead: "A missing table", text: "is created only after you approve its DDL." },
            ]}
            link={{ label: "Read how Validate works", route: "help-preflight-gates", onNavigate: p.onNavigate }}
          />
        }
      >
        <ValidateFindings />
      </FeatureRow>

      <FeatureRow
        id="mk-ts-why"
        flip
        title="See why each column was paired"
        body="Open any row in Map: the reason, the confidence, and a sample."
        more={
          <RowPoints
            items={[
              { icon: Scale, lead: "Look-alike pairs", text: "such as quantity and amount are confirmed one by one." },
              { icon: BookOpen, lead: "Accepted pairs", text: "join your workspace synonym dictionary for future maps." },
              { icon: ListFilter, lead: "Needs attention", text: "shows only the rows still waiting on a decision." },
            ]}
            link={{ label: "Read how mapping works", route: "help-semantic-mapping", onNavigate: p.onNavigate }}
          />
        }
      >
        <MappingDetail />
      </FeatureRow>

      <CodeBand
        id="mk-ts-api"
        title="Run the approved plan from the API"
        body="The plan routes mirror the Studio steps. A transfer started from the API is the same checked job, with the same Trust Report."
        points={[
          "State sync_mode explicitly. On this route the default is overwrite.",
          "Take mappings from your plan's run payload. The empty list is a placeholder.",
          "Idempotency-Key lets you repeat the request safely after a timeout.",
        ]}
        snippets={[
          { label: "Run", code: QUICKSTART_EXECUTE, source: "Docs, quickstart: run (needs job.run)" },
          { label: "Sign in", code: QUICKSTART_LOGIN, source: "Docs, quickstart: sign in" },
        ]}
        cardLabel="Transfer API examples from the docs"
      />

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
      closing={{ title: "Open a run you can sign", body: "Start a workspace and run a transfer; the Trust Report comes with every job." }}
    >
      <FeatureLinks
        label="Related to Job space"
        onNavigate={p.onNavigate}
        items={[
          { icon: BookOpen, label: "Job space guide", body: "Runs, Trust Reports and sign-off in the docs", route: "help-job-theater" },
          { icon: FileSignature, label: "Proof", body: "CI evidence and proof packs you can verify", route: "customers" },
          { icon: Workflow, label: "Transfer Studio", body: "Where the plan is made and approved", route: "product-transfer" },
          { icon: ShieldCheck, label: "Security", body: "Audit chain, keys, and isolation", route: "security" },
        ]}
      />

      <section className="mk-sb-section mk-kit-sec" aria-labelledby="mk-jobs-ladder">
        <div className="mk-wrap">
          <SplitHead
            id="mk-jobs-ladder"
            title="What a finished run carries"
            lead="The comparison that ran, the rows held back, and a verdict a reviewer can sign."
          />
          <JobsBento />
        </div>
      </section>

      <FeatureRow
        id="mk-jobs-green"
        title="A green job is not proof"
        body="The writer's count says what it sent. Job space reads the destination back and counts what it holds."
        more={
          <RowPoints
            items={[
              { icon: Scale, lead: "Every row read", text: "is landed, held with a reason, or skipped by rule." },
              { icon: Archive, lead: "Held rows", text: "keep column, value and reason, and replay into the same table." },
              { icon: Info, lead: "A run with held rows", text: "ends as completed_with_quarantine, not completed." },
            ]}
            link={{ label: "Read how Job space works", route: "help-job-theater", onNavigate: p.onNavigate }}
          />
        }
      >
        <GreenVsCounted />
      </FeatureRow>

      <FeatureRow
        id="mk-jobs-sign"
        flip
        title="Sign the report, not a spreadsheet"
        body="A Trust Report carries the statement, the check that ran and the verdict, and goes to sign-off from Job space."
        more={
          <RowPoints
            items={[
              { icon: ShieldCheck, lead: "Sign-off waits", text: "until no unexplained difference remains." },
              { icon: UserCheck, lead: "Roles carry authority:", text: "by default only an admin signs for compliance." },
              { icon: RefreshCw, lead: "A changed report", text: "is signed again; approvals cover only what was signed." },
            ]}
          />
        }
      >
        <SignoffPanel title="Trust Report, sign-off" meta="Example rows, the home page's run" counts={RUN} />
      </FeatureRow>

      <CodeBand
        id="mk-jobs-evid"
        title="Evidence you can verify yourself"
        body="Read a run's Trust Report and download its proof pack from the API, then check the pack without trusting the screen."
        points={[
          "A pack claims \u201cmigration proven\u201d only after a full checksum. A sampled pack can be exported, but refuses that claim.",
          "The Markdown report is rendered from the signed pack, approvals included.",
          "A pack is engineering evidence, not an attestation or an audit.",
        ]}
        snippets={[
          { label: "Proof pack", code: JOBS_PROOF_PACK, source: "Docs, verification: proof pack (export needs job.read)" },
          { label: "Trust Report", code: JOBS_TRUST_REPORT, source: "Docs, quickstart: read the verdict (needs job.read)" },
          { label: "Follow the job", code: JOBS_FOLLOW, source: "Docs, quickstart: follow the job" },
        ]}
        cardLabel="Job space API examples from the docs"
      />
    </StoryFrame>
  );
}

export function PipelinesPage(p: PageProps) {
  const act = actions(p, { label: "See Recurring sync", onClick: () => p.onNavigate("solution-sync") });
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
      closing={{ title: "Put a transfer on a schedule", body: "Approve a transfer once, then let it run on a cadence with the same checks." }}
    >
      <FeatureLinks
        label="Related to Schedules"
        onNavigate={p.onNavigate}
        items={[
          { icon: BookOpen, label: "Schedules guide", body: "Cadence, history and sync modes in the docs", route: "help-pipelines" },
          { icon: Repeat, label: "Recurring sync", body: "Keep two systems in step", route: "solution-sync" },
          { icon: Gauge, label: "Job space", body: "Every tick's run and Trust Report", route: "product-jobs" },
          { icon: Server, label: "MCP server", body: "Agents stage schedules for you to confirm", route: "product-mcp" },
        ]}
      />

      <BentoSection id="mk-pipe-what" title="What a schedule runs under" lead="The route, map and mode you approved, with the retries and alerts you set.">
        <SchedulesBento />
      </BentoSection>

      <FeatureRow
        id="mk-pipe-drift"
        title="The first run gets reviewed. So does the hundredth."
        body="A new or renamed source column parks the schedule with the reason, instead of retrying, until someone decides where it belongs."
        more={
          <RowPoints
            items={[
              { icon: FileSignature, lead: "A required contract", text: "keeps unattended runs off a schema nobody signed." },
              { icon: History, lead: "History", text: "shows rows read, written and rejected per run, with its job ID." },
            ]}
            link={{ label: "Read how schedules work", route: "help-pipelines", onNavigate: p.onNavigate }}
          />
        }
      >
        <DriftReview />
      </FeatureRow>

      <FeatureRow
        id="mk-pipe-modes"
        flip
        title={`${SYNC_MODE_COUNT_WORD[0].toUpperCase()}${SYNC_MODE_COUNT_WORD.slice(1)} ways to run a tick`}
        body="Each mode says what it needs. Unknown mode names are refused."
        more={
          <RowPoints
            items={[
              { icon: EyeOff, lead: "Incremental reads", text: "cannot see deletes; use mirror or CDC when they matter." },
              { icon: Ban, lead: "Append-only destinations", text: "such as Kafka topics accept append modes only." },
              { icon: Plug, lead: "The modes offered", text: "depend on the two connectors in the route." },
            ]}
          />
        }
      >
        <ModeSelector />
      </FeatureRow>

      <CodeBand
        id="mk-pipe-yaml"
        title="Keep schedules in version control"
        body="Export one schedule, or every schedule in the workspace, as YAML: the reviewable definition of cadence, route and sync mode."
        points={[
          "Import YAML from the Schedules toolbar to apply a reviewed definition.",
          "Require signed refuses an imported schedule unless its contract is signed.",
          "Both exports need schedule.read.",
        ]}
        snippets={[
          { label: "One schedule", code: SCHEDULE_EXPORT, source: "Docs, Schedules: schedules as YAML (the drawer's Export YAML)" },
          { label: "Whole workspace", code: SCHEDULES_MANIFEST, source: "Docs, Schedules: schedules as YAML (the toolbar's Export YAML)" },
        ]}
        cardLabel="Schedule export API examples"
      />

      <TableSection id="mk-pipe-rules" title="Where each rule is written down" body="The rules a schedule runs under, and the docs page for each.">
        <RulesTable
          caption="Schedule rules, and the docs page for each"
          whereLabel="Docs"
          onNavigate={p.onNavigate}
          items={[
            { claim: "Mirror deletes are soft by default: the row is kept with _deleted and _deleted_at", where: "Schedules guide: sync modes", route: "help-pipelines" },
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
      closing={{ title: "Check a copy without a second console", body: "Start a workspace, save two connections, and ask the same question of both." }}
    >
      <FeatureLinks
        label="Related to Query"
        onNavigate={p.onNavigate}
        items={[
          { icon: BookOpen, label: "Query guide", body: "Read-only SQL, as the docs describe it", route: "help-query" },
          { icon: MessageSquare, label: "Pilot", body: "Ask in plain language instead", route: "product-pilot" },
          { icon: Gauge, label: "Job space", body: "The comparison Datawrap already ran", route: "product-jobs" },
          { icon: Plug, label: "Connectors", body: "What you can connect", route: "integrations" },
        ]}
      />

      <BentoSection id="mk-q-will" title="What Query will and will not do" lead="It reads. It never writes, and it runs as you.">
        <QueryBento />
      </BentoSection>

      <FeatureRow
        id="mk-q-both"
        title="Ask both sides the same question"
        body="Run one statement on the source and on the copy, and set the answers side by side."
        more={
          <RowPoints
            items={[
              { icon: Code, lead: "The editor", text: "knows each connection's SQL dialect." },
              { icon: Ban, lead: "Not queried here:", text: "Kafka topics and vector stores." },
            ]}
            link={{ label: "Read the Query guide", route: "help-query", onNavigate: p.onNavigate }}
          />
        }
      >
        <QueryCompare />
      </FeatureRow>

      <FeatureRow
        id="mk-q-guards"
        flip
        title="Three guards between a statement and your data"
        body="Pick a statement and see where it stops. A refused statement never reaches the database."
        more={
          <RowPoints
            items={[
              { icon: Code, lead: "One statement per run:", text: "a second one after a semicolon is refused." },
              { icon: Eye, lead: "A read-only head:", text: "it must start as a read, such as SELECT or WITH." },
            ]}
          />
        }
      >
        <GuardLayers />
      </FeatureRow>
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
      closing={{ title: "Ask your workspace a question", body: "Start free and ask Pilot about a saved connection or a finished job." }}
    >
      <FeatureLinks
        label="Related to Pilot"
        onNavigate={p.onNavigate}
        items={[
          { icon: BookOpen, label: "Pilot guide", body: "Tools, grounding and engines in the docs", route: "help-data-pilot" },
          { icon: Server, label: "MCP server", body: "The same tools for Cursor and Claude", route: "product-mcp" },
          { icon: Code, label: "Query", body: "Write the SQL yourself", route: "product-query" },
          { icon: Gauge, label: "Job space", body: "The evidence Pilot reads from", route: "product-jobs" },
        ]}
      />

      <BentoSection id="mk-pilot-ask" title="What you can ask about" lead="Pilot answers from your workspace, under your role.">
        <PilotBento />
      </BentoSection>

      <FeatureRow
        id="mk-pilot-loop"
        title="Answers come from tool results, not recollection"
        body="Each tool is checked against the same permission as the matching API route. Pilot and the MCP server share one registry."
        more={
          <RowPoints
            items={[
              { icon: Info, lead: "An ungrounded answer", text: "is general guidance, not a fact about your workspace." },
              { icon: FileSearch, lead: "For a number that matters,", text: "open the job, table or report it came from." },
              { icon: KeyRound, lead: "A viewer", text: "can ask questions but cannot reach a tool that changes anything." },
            ]}
            link={{ label: "Read the Pilot guide", route: "help-data-pilot", onNavigate: p.onNavigate }}
          />
        }
      >
        <PilotLoop />
      </FeatureRow>

      <FeatureRow
        id="mk-pilot-confirm"
        flip
        title="Nothing changes until you press Confirm"
        body="A change comes back staged, with a preview, and waits for you."
        more={
          <RowPoints
            items={[
              { icon: ListChecks, lead: "Staged changes", text: "come back in pending_actions until you confirm them." },
              { icon: Workflow, lead: "A confirmed transfer", text: "passes Validate and appears in Job space like any other run." },
            ]}
          />
        }
      >
        <ConfirmChecks />
      </FeatureRow>
    </StoryFrame>
  );
}

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
      closing={{ title: "Connect an agent under a role", body: "Create a workspace API key with the role you choose, then add the server URL to your client." }}
    >
      <FeatureLinks
        label="Related to the MCP server"
        onNavigate={p.onNavigate}
        items={[
          { icon: BookOpen, label: "MCP guide", body: "Connect a client, step by step", route: "help-mcp" },
          { icon: MessageSquare, label: "Pilot", body: "The same tools, inside the app", route: "product-pilot" },
          { icon: ShieldCheck, label: "Security", body: "Keys, roles and the audit chain", route: "security" },
          { icon: Building2, label: "Enterprise", body: "SSO, SCIM and deployment options", route: "enterprise" },
        ]}
      />

      <BentoSection id="mk-mcp-reach" title="What an agent can reach" lead="The tools Pilot uses, under the role on the agent's key.">
        <McpBento />
      </BentoSection>

      <FeatureRow
        id="mk-mcp-lanes"
        title="Stage, then confirm"
        body="What passes between an agent and Datawrap when the agent asks for a change."
        more={
          <RowPoints
            items={[
              { icon: ListChecks, lead: "Confirmed transfers", text: "pass Validate and appear in Job space." },
              { icon: UserCheck, lead: "A client that confirms on its own", text: "takes the person out of the loop; give keys the lowest role." },
            ]}
            link={{ label: "Read the MCP guide", route: "help-mcp", onNavigate: p.onNavigate }}
          />
        }
      >
        <McpLanes />
      </FeatureRow>

      <FeatureRow
        id="mk-mcp-key"
        flip
        title="A URL and a key, never a database password"
        body="The agent holds a workspace API key with the role you chose. Every call it makes is on record."
        more={
          <RowPoints
            items={[
              { icon: Power, lead: "An admin", text: "can turn MCP off, or limit it to a list of tools." },
              { icon: Gauge, lead: "Calls are rate limited,", text: "and recent calls are listed on the MCP page." },
            ]}
          />
        }
      >
        <McpKeyPanel />
      </FeatureRow>

      <CodeBand
        id="mk-mcp-api"
        title="Connect a client"
        body="Add the server URL and a workspace API key to your MCP client. Discovery needs no key; running a tool does."
        points={[
          "Without a key, running a tool answers 401 with a WWW-Authenticate challenge.",
          "Each tool's full JSON schema comes from GET /api/v1/mcp/tools.",
          "OAuth resource metadata is published at /.well-known/oauth-protected-resource.",
        ]}
        snippets={[
          { label: "Client config", code: MCP_CLIENT_CONFIG, source: "Docs, MCP guide: connect Cursor" },
          { label: "Discover", code: MCP_DISCOVER, source: "Docs, MCP: connecting (no token needed)" },
        ]}
        cardLabel="MCP examples from the docs"
      />
    </StoryFrame>
  );
}

export function MigrationsSolutionPage(p: PageProps) {
  const act = actions(p, { label: SALES_CTA_LABEL, onClick: () => p.onNavigate("contact") });
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
      closing={{ title: "Plan your cutover with evidence", body: "Start free with a subset of your data and review the plan before anything is written." }}
    >
      <FeatureLinks
        label="Related to migrations"
        onNavigate={p.onNavigate}
        items={[
          { icon: Workflow, label: "Transfer Studio", body: "Plan and approve the move", route: "product-transfer" },
          { icon: Gauge, label: "Job space", body: "The load's evidence and sign-off", route: "product-jobs" },
          { icon: BookOpen, label: "Sign-off guide", body: "How a Trust Report is signed", route: "help-job-theater" },
          { icon: Database, label: "Warehouse loading", body: "Bulk loads into Snowflake, BigQuery and Iceberg", route: "solution-warehouse" },
        ]}
      />

      <BentoSection
        id="mk-mig-rows"
        title="Where every row of the cutover load went"
        lead="Read equals landed plus held plus skipped, so no row leaves the old system unaccounted for."
      >
        <MigrationBento />
      </BentoSection>

      <TableSection id="mk-mig-door" title="Cutover is a one-way door" body="Once applications point at the new system, a missing key is a production incident. Plan it so every step leaves a record.">
        <CutoverTable />
      </TableSection>

      <FeatureRow
        id="mk-mig-sign"
        title="Sign the cutover on its Trust Report"
        body="The statement, the check that ran and the verdict, signed by each role before applications move."
        more={
          <RowPoints
            items={[
              { icon: ShieldCheck, lead: "Proven", text: "is used only when every row was compared." },
              { icon: Percent, lead: "A sampled result", text: "is never called proven; it bounds a rate." },
              { icon: Lock, lead: "Once every role signs,", text: "the report is final; no later decision changes it." },
            ]}
            link={{ label: "Read how sign-off works", route: "help-job-theater", onNavigate: p.onNavigate }}
          />
        }
      >
        <SignoffPanel title="Trust Report, sign-off" meta="Example cutover load, legacy.orders to CORE.ORDERS" counts={MIGRATION_RUN} />
      </FeatureRow>

      <FeatureRow
        id="mk-mig-risk"
        flip
        title="Accept a known loss on the record"
        body="A column that narrows needs a signed risk record before the load can go ahead."
        more={
          <RowPoints
            items={[
              { icon: TriangleAlert, lead: "What can be lost:", text: "precision, truncation or nulls, named on the record." },
              { icon: FileSignature, lead: "Who approved it,", text: "when and why, with the quarantine and rollback posture." },
              { icon: Ban, lead: "Unapproved risks", text: "fail the job by default, not continue silently." },
            ]}
          />
        }
      >
        <RiskRecordPanel />
      </FeatureRow>

      <TableSection id="mk-mig-rules" title="Details that survive real schemas" body="What carries over, and where to see it.">
        <RulesTable
          caption="Migration details, and where to see each one"
          whereLabel="Where to see it"
          onNavigate={p.onNavigate}
          items={[
            { claim: "Sequences, foreign keys, CHECK constraints, and indexes carry on create-new, in dependency order", where: "Transfer Studio, Destination: create new table" },
            { claim: "Unicode text keeps its encoding; collation-aware key checks run in Validate", where: "Validate step" },
            { claim: "Timestamps are compared at the precision the destination column holds", where: "Docs, Verification: known causes of a difference" },
            { claim: "Held rows keep column, value, and reason, and replay into the same table", where: "Job space guide: fix and replay", route: "help-job-theater" },
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
      closing={{ title: "Load a warehouse on your own data", body: "Start free on the same engine your production loads will use." }}
    >
      <FeatureLinks
        label="Related to warehouse loading"
        onNavigate={p.onNavigate}
        items={[
          { icon: Plug, label: "Connectors", body: "Warehouses, lakes and object storage", route: "integrations" },
          { icon: ListChecks, label: "Validate guide", body: "What is checked before a load", route: "help-preflight-gates" },
          { icon: Repeat, label: "Recurring sync", body: "Keep the warehouse current", route: "solution-sync" },
          { icon: Gauge, label: "Job space", body: "Each load's evidence", route: "product-jobs" },
        ]}
      />

      <BentoSection id="mk-wh-checked" title="How a warehouse load is checked" lead="Before the load window on the real warehouse, and afterwards by the warehouse's own engine.">
        <WarehouseBento />
      </BentoSection>

      <FeatureRow
        id="mk-wh-prec"
        title="A load can succeed and still lose a cent"
        body="Bulk jobs report success when they finish, not when every value is right. Validate checks the fit first."
        more={
          <RowPoints
            items={[
              { icon: Ruler, lead: "Population fit", text: "checks DECIMAL(p,s) and VARCHAR(n) on this run's rows." },
              { icon: Braces, lead: "A JSON number", text: "the destination would store differently is refused first." },
              { icon: TableProperties, lead: "A new table", text: "is created on first write, from DDL you approve in Validate." },
            ]}
            link={{ label: "Read what Validate checks", route: "help-preflight-gates", onNavigate: p.onNavigate }}
          />
        }
      >
        <PrecisionSpecimen />
      </FeatureRow>

      <TableSection id="mk-wh-rules" title="Warehouse specifics" body="How each warehouse connects and how upserts are written.">
        <RulesTable
          caption="Warehouse specifics, and where to check each one"
          ruleLabel="Detail"
          items={[
            { claim: "Snowflake connects with a password or key-pair authentication", where: "Connectors, Snowflake" },
            { claim: "Iceberg connects to a REST catalog or a filesystem warehouse", where: "Connectors, Apache Iceberg" },
            { claim: "Upserts use MERGE, or a keyed delete and insert, with no index created", where: "Docs, Sync modes: upsert" },
            { claim: "Rows only in the destination are left alone by an upsert", where: "Docs, Sync modes: upsert" },
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
      closing={{ title: "Keep a copy in step", body: "Start free with a schedule on your own connections." }}
    >
      <FeatureLinks
        label="Related to recurring sync"
        onNavigate={p.onNavigate}
        items={[
          { icon: CalendarClock, label: "Schedules", body: "Cadence, history and drift review", route: "product-pipelines" },
          { icon: BookOpen, label: "Sync modes guide", body: "Every mode and what it needs", route: "help-pipelines" },
          { icon: Database, label: "Warehouse loading", body: "Bulk loads into a warehouse", route: "solution-warehouse" },
          { icon: Gauge, label: "Job space", body: "Each run's evidence", route: "product-jobs" },
        ]}
      />

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

      <BentoSection id="mk-sync-read" title="Choose how each run reads" lead="The mode is set once, in the approved transfer.">
        <SyncBento />
      </BentoSection>

      <FeatureRow
        id="mk-sync-delete"
        title="A delete at the source, carried to the copy"
        body="Soft by default: the row is kept and flagged, so the copy shows what was removed and when."
        more={
          <RowPoints
            items={[
              { icon: RotateCcw, lead: "A row that returns", text: "at the source is reactivated." },
              { icon: Hash, lead: "Row counts hold:", text: "filter on _deleted to count active rows." },
              { icon: TriangleAlert, lead: "Complete reads only:", text: "a partial source read would delete rows." },
            ]}
            link={{ label: "Read about sync modes", route: "help-pipelines", onNavigate: p.onNavigate }}
          />
        }
      >
        <MirrorDeletePanel />
      </FeatureRow>

      <TableSection id="mk-sync-cdc" title="What CDC refuses" body="A consistent snapshot first, then changes from the log. Where it cannot do that, it says so.">
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
