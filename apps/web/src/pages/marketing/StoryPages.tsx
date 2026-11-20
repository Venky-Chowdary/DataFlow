import {
  GatesPanel,
  MirrorDeletePanel,
  TickDetailPanel,
} from "../../components/marketing/ProductPanels";
import {
  CdcSources,
  ConfirmChecks,
  ConnectionList,
  CoverageLadder,
  CutoverLedger,
  DriftReview,
  GreenVsCounted,
  GuardLayers,
  MapExcerpt,
  McpKeyCard,
  McpLanes,
  McpStaged,
  McpTranscript,
  ModeSelector,
  PilotLoop,
  PilotQuestion,
  PilotSql,
  PilotStatement,
  PlanHashLine,
  PrecisionSpecimen,
  ProofPackApi,
  QueryEditor,
  RefusalCard,
  RunCensus,
  RunTimeline,
  ScheduleStrip,
  SignoffSheet,
  StepRail,
  SyncRail,
  TypeMapping,
  ValidateFindings,
  VerdictCard,
  WarehouseRoute,
} from "../../components/marketing/StoryPanels";
import { cdcSourcesPhrase } from "../../lib/provenEvidence";
import { SYNC_MODE_COUNT_WORD } from "../../lib/syncModesPhrase";
import type { PublicRoute } from "../../lib/publicNavigation";
import { WORKSPACE_SHOT } from "../../lib/workspaceShots";
import { JobsWithTrust, McpScene, PilotScene, QueryScene, SCENE_NOTES, SchedulesScene, TransferScene } from "../../components/marketing/StoryScenes";
import { FrameSection, HeroCopy, RulesLedger, Screen, SectionHead, StoryCapture, StoryFrame, type StoryAction } from "./StoryPage";

/*
 * Product and solution pages. Every claim here is something the product does
 * today; the rules ledgers say where a reader can confirm it. Panels are
 * examples, labelled as such inside the panel. Each page composes its own
 * hero and body: no two share a layout.
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
  ];
  return (
    <StoryFrame
      page="ts"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <section className="mk-ts-hero" aria-labelledby="mk-ts-title">
          <div className="mk-wrap mk-ts-hero-grid">
            <HeroCopy id="mk-ts-title" title="Plan the load. Check it. Then write it." lead="Approve how every column lands, and clear the checks before a single row is written." actions={act} />
            <Screen className="mk-ts-stack">
              <div className="mk-ts-railcard">
                <StepRail current={1} />
              </div>
              <div className="mk-ts-back">
                <MapExcerpt />
              </div>
              <div className="mk-ts-front">
                <GatesPanel />
              </div>
            </Screen>
          </div>
        </section>
      }
      related={[
        { route: "product-jobs", label: "Job space", body: "Open the finished run and its Trust Report" },
        { route: "product-pipelines", label: "Schedules", body: "Run the approved plan on a cadence" },
        { route: "solution-migrations", label: "Migrations", body: "Cut over between schemas with evidence" },
      ]}
      closing={{ title: "Plan a transfer on your own data", body: "Start with a free workspace and approve your first plan." }}
    >
      <FrameSection
        id="mk-ts-frame"
        tone="dark"
        title="The Studio, step by step"
        body="Choose Map, Validate or Run in the rail."
        cta={{ label: "Open the Studio on your own data", onClick: p.onGetStarted }}
        notes={SCENE_NOTES.transfer}
      >
        <TransferScene marks />
      </FrameSection>

      <section className="mk-section mk-ts-fail" aria-labelledby="mk-ts-fail">
        <div className="mk-wrap mk-ts-fail-grid">
          <SectionHead id="mk-ts-fail" title="Most loads fail at the write" body="A cast that truncates, a column nobody owns. Validate finds both before the first row moves." />
          <Screen className="mk-ts-fail-screen">
            <ValidateFindings />
          </Screen>
        </div>
      </section>

      <section className="mk-section mk-ts-how" aria-labelledby="mk-ts-how">
        <div className="mk-wrap">
          <SectionHead id="mk-ts-how" title="From a map to a write" />
          <PlanHashLine points={how} />
          <StoryCapture
            className="mk-ts-why"
            capture={{
              src: WORKSPACE_SHOT.transferWhyCrop,
              alt: "Map step detail: JSONB attributes mapped to Snowflake VARIANT with Parse JSON, with the reason, confidence, source type, and a sample",
              caption: "Open any row in Map to see why it was paired: the reason, the confidence, and a sample.",
            }}
          />
        </div>
      </section>

      <section className="mk-section mk-ts-rulesec" aria-labelledby="mk-ts-rules">
        <div className="mk-wrap">
          <RulesLedger
            id="mk-ts-rules"
            title="Rules you can hold it to"
            className="mk-ts-rules"
            items={[
              { claim: "The same core checks run on every route, plus the ones the route needs", where: "Validate step; Docs, Verification" },
              { claim: "A source column must be mapped or declared omitted, or the run is refused", where: "Docs, Verification: dropping fields on purpose" },
              { claim: "Accepting a known loss takes a signed risk record, never a yes/no flag", where: "Docs, Verification: accepting a known loss" },
              { claim: "In strict mode, mappings below 0.85 confidence block until reviewed", where: "Docs, Verification: validation modes" },
            ]}
          />
        </div>
      </section>
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
        <section className="mk-jobs-hero" aria-labelledby="mk-jobs-title">
          <div className="mk-wrap">
            <HeroCopy id="mk-jobs-title" className="mk-jobs-copy" title="Every run, checked against its destination" lead="Open a finished run: what was read, what landed, what was held, and the comparison that shows it." actions={act} />
            <Screen className="mk-jobs-stack">
              <div className="mk-jobs-back">
                <RunTimeline />
              </div>
              <div className="mk-jobs-front">
                <VerdictCard />
              </div>
            </Screen>
          </div>
        </section>
      }
      related={[
        { route: "customers", label: "Proof", body: "CI evidence and proof packs you can verify" },
        { route: "product-transfer", label: "Transfer Studio", body: "Where the plan is made and approved" },
        { route: "security", label: "Security", body: "Audit chain, keys, and isolation" },
      ]}
      closing={{ title: "Open a run you can sign", body: "Start a workspace and run a transfer; the Trust Report comes with every job." }}
    >
      <FrameSection
        id="mk-jobs-frame"
        tone="dark"
        title="Open any run"
        body="Choose a row to open its Trust Report."
        cta={{ label: "Run a transfer and open its report", onClick: p.onGetStarted }}
      >
        <JobsWithTrust initial="5d20b7c4" caption="Example workspace: six runs, each with the comparison that ran. sales_orders is the home page's run." />
      </FrameSection>

      <section className="mk-section mk-jobs-green" aria-labelledby="mk-jobs-green">
        <div className="mk-wrap">
          <SectionHead id="mk-jobs-green" title="A green job is not proof" />
          <GreenVsCounted />
        </div>
      </section>

      <section className="mk-section mk-section--band mk-jobs-ladsec" aria-labelledby="mk-jobs-ladder">
        <div className="mk-wrap mk-jobs-ladder-grid">
          <SectionHead id="mk-jobs-ladder" title="Each table names the comparison that ran" body="Only a comparison of every row is called proven." />
          <CoverageLadder />
        </div>
      </section>

      <section className="mk-section mk-jobs-evid" aria-labelledby="mk-jobs-evid">
        <div className="mk-wrap">
          <SectionHead id="mk-jobs-evid" title="Evidence you can verify yourself" />
          <div className="mk-jobs-evid-grid">
            <StoryCapture
              className="mk-jobs-shot"
              capture={{
                src: WORKSPACE_SHOT.jobsEvidenceCrop,
                alt: "Trust Report in Job space: reconciliation passed, full checksum assurance, trust score, and the sign-off and evidence pack",
                caption: "A Trust Report's evidence and sign-off, as Job space shows them.",
              }}
            />
            <Screen className="mk-jobs-api-screen">
              <ProofPackApi />
            </Screen>
          </div>
        </div>
      </section>
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
        <section className="mk-pipe-hero" aria-labelledby="mk-pipe-title">
          <div className="mk-wrap mk-pipe-hero-grid">
            <HeroCopy id="mk-pipe-title" title="Recurring loads, held to the first run's standard" lead="Every tick is a full job, with the same checks and read-back as a run started by hand." actions={act} />
            <Screen className="mk-pipe-stack">
              <div className="mk-pipe-back">
                <ScheduleStrip />
              </div>
              <div className="mk-pipe-front">
                <DriftReview />
              </div>
            </Screen>
          </div>
        </section>
      }
      related={[
        { route: "solution-sync", label: "Recurring sync", body: "Keep two systems in step" },
        { route: "product-jobs", label: "Job space", body: "Every tick's evidence" },
        { route: "product-mcp", label: "MCP server", body: "Agents can stage schedules for you to confirm" },
      ]}
      closing={{ title: "Put a transfer on a schedule", body: "Approve a transfer once, then let it run on a cadence with the same checks." }}
    >
      <FrameSection
        id="mk-pipe-frame"
        tone="dark"
        title="Every schedule, and every tick it ran"
        body="Choose a schedule to open its run history."
        cta={{ label: "Put your own transfer on a schedule", onClick: p.onGetStarted }}
        notes={SCENE_NOTES.schedules}
      >
        <SchedulesScene marks />
      </FrameSection>

      <section className="mk-section mk-pipe-drift-sec" aria-labelledby="mk-pipe-drift">
        <div className="mk-wrap">
          <SectionHead id="mk-pipe-drift" title="The first run gets reviewed. So does the hundredth." body="A new or renamed source column pauses the schedule until someone decides where it belongs." />
          <RunCensus />
        </div>
      </section>

      <section className="mk-section mk-section--band mk-pipe-modesec" aria-labelledby="mk-pipe-modes">
        <div className="mk-wrap mk-pipe-modes-grid">
          <SectionHead
            id="mk-pipe-modes"
            title={`${SYNC_MODE_COUNT_WORD[0].toUpperCase()}${SYNC_MODE_COUNT_WORD.slice(1)} ways to run a tick`}
            body={`Unknown mode names are refused. CDC is proven in CI on ${cdcSourcesPhrase()}.`}
          />
          <ModeSelector />
        </div>
      </section>

      <section className="mk-pipe-stage" aria-labelledby="mk-pipe-tick">
        <div className="mk-wrap mk-pipe-stage-grid">
          <Screen className="mk-pipe-stage-panel">
            <TickDetailPanel />
          </Screen>
          <div>
            <SectionHead id="mk-pipe-tick" title="Open any tick" body="Its map version, the checks it cleared, and its statement." />
            <RulesLedger
              id="mk-pipe-rules"
              title="Where each rule is written down"
              className="mk-pipe-rules"
              whereLabel="Docs"
              items={[
                { claim: "Mirror deletes are soft by default: the row is kept with _deleted and _deleted_at", where: "Sync modes: mirror" },
                { claim: "Before an append, the destination must match the fingerprint left by the last run", where: "Sync modes: append" },
                { claim: "Keyed modes absorb redelivery: a second write updates the same row", where: "Sync modes" },
                { claim: "One job at a time reads a given replication slot or binlog position", where: "Sync modes: CDC" },
              ]}
            />
          </div>
        </div>
      </section>
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
        <section className="mk-q-hero" aria-labelledby="mk-q-title">
          <div className="mk-wrap mk-q-hero-grid">
            <Screen className="mk-q-stack">
              <div className="mk-q-back">
                <ConnectionList />
              </div>
              <div className="mk-q-main">
                <QueryEditor />
              </div>
              <div className="mk-q-front">
                <RefusalCard />
              </div>
            </Screen>
            <HeroCopy id="mk-q-title" title="Look at the source and the copy, read-only" lead="The same SQL against any saved connection. Writes are refused before they reach the database." actions={act} />
          </div>
        </section>
      }
      related={[
        { route: "product-pilot", label: "Pilot", body: "Ask in plain language instead" },
        { route: "product-jobs", label: "Job space", body: "The comparison Datawrap already ran" },
        { route: "integrations", label: "Connectors", body: "What you can connect" },
      ]}
      closing={{ title: "Check a copy without a second console", body: "Start a workspace, save two connections, and ask the same question of both." }}
    >
      <FrameSection
        id="mk-q-frame"
        tone="band"
        title="Ask both sides the same question"
        body="Switch tabs between the copy and the source, or try a write."
        cta={{ label: "Query your own connections", onClick: p.onGetStarted }}
        notes={SCENE_NOTES.query}
      >
        <QueryScene marks />
      </FrameSection>

      <section className="mk-section mk-q-guardsec" aria-labelledby="mk-q-guards">
        <div className="mk-wrap">
          <SectionHead id="mk-q-guards" title="Three guards between a statement and your data" body="Pick a statement and see where it stops." />
          <GuardLayers />
        </div>
      </section>

      <section className="mk-section mk-q-will" aria-labelledby="mk-q-will">
        <div className="mk-wrap mk-q-will-grid">
          <StoryCapture
            className="mk-q-shot"
            capture={{
              src: WORKSPACE_SHOT.queryRefusalCrop,
              alt: "Query editor with a DELETE statement, refused: only read and metadata queries run in the playground",
              caption: "A write, refused in the editor before it reaches the database.",
            }}
          />
          <RulesLedger
            id="mk-q-will"
            title="What Query will and will not do"
            className="mk-q-rules"
            items={[
              { claim: "Statements that write are refused in the editor", where: "Query: try any INSERT, UPDATE, or DELETE" },
              { claim: "Server-side read-only transaction on PostgreSQL, MySQL, SQLite, and Oracle", where: "Query on a saved connection of those types" },
              { claim: "Runs under your workspace role, on saved connections; credentials stay in the vault", where: "Settings, Team: roles" },
            ]}
          />
        </div>
      </section>
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
        <section className="mk-pilot-hero" aria-labelledby="mk-pilot-title">
          <div className="mk-wrap">
            <HeroCopy id="mk-pilot-title" className="mk-pilot-copy" title="Ask about your data, and see how the answer was found" lead="Pilot answers by running tools against your workspace, and stages any change for you to confirm." actions={act} />
            <div className="mk-pilot-scene">
              <PilotQuestion />
              <Screen className="mk-pilot-stack">
                <div className="mk-pilot-back">
                  <PilotSql />
                </div>
                <div className="mk-pilot-front">
                  <PilotStatement />
                </div>
              </Screen>
            </div>
          </div>
        </section>
      }
      related={[
        { route: "product-mcp", label: "MCP server", body: "The same tools for Cursor and Claude" },
        { route: "product-query", label: "Query", body: "Write the SQL yourself" },
        { route: "product-jobs", label: "Job space", body: "The evidence Pilot reads from" },
      ]}
      closing={{ title: "Ask your workspace a question", body: "Start free and ask Pilot about a saved connection or a finished job." }}
    >
      <FrameSection
        id="mk-pilot-frame"
        tone="dark"
        title="An answer with its statement attached"
        body="Confirm the staged schedule to see what happens next."
        cta={{ label: "Ask Pilot about your own workspace", onClick: p.onGetStarted }}
        notes={SCENE_NOTES.pilot}
      >
        <PilotScene marks />
      </FrameSection>

      <section className="mk-section mk-pilot-loopsec" aria-labelledby="mk-pilot-loop">
        <div className="mk-wrap mk-pilot-loop-grid">
          <SectionHead
            id="mk-pilot-loop"
            title="Answers come from tool results, not recollection"
            body="Each tool is checked against the same permission as the matching API route. Pilot and the MCP server share one registry."
          />
          <PilotLoop />
        </div>
      </section>

      <section className="mk-pilot-stage" aria-labelledby="mk-pilot-confirm">
        <div className="mk-wrap mk-pilot-stage-grid">
          <div className="mk-pilot-figs">
            <SectionHead id="mk-pilot-confirm" title="Nothing changes until you press Confirm" />
            <dl className="mk-pilot-fig">
              <div>
                <dt>An approval expires after</dt>
                <dd>
                  15<span>min</span>
                </dd>
              </div>
              <div>
                <dt>and is good for</dt>
                <dd>
                  1<span>use</span>
                </dd>
              </div>
            </dl>
          </div>
          <Screen className="mk-pilot-stage-panel">
            <ConfirmChecks />
          </Screen>
        </div>
      </section>

      <section className="mk-section mk-pilot-cannot" aria-labelledby="mk-pilot-cannot">
        <div className="mk-wrap">
          <SectionHead id="mk-pilot-cannot" title="No tool exists for these" body="Pilot can word a result. It cannot change one." />
          <ul className="mk-pilot-void">
            <li>Change members, roles, or API keys</li>
            <li>Change workspace settings</li>
            <li>Sign a Trust Report</li>
            <li>Approve a Trust Report fix</li>
            <li>Run a query that writes</li>
            <li>Decide a mapping or a verdict</li>
          </ul>
        </div>
      </section>
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
        <section className="mk-mcp-hero" aria-labelledby="mk-mcp-title">
          <div className="mk-wrap mk-mcp-hero-grid">
            <HeroCopy id="mk-mcp-title" title="Give agents the same tools, and keep the gates" lead="Agents connect with a workspace API key. They can plan and stage work; nothing that changes data runs until it is confirmed." actions={act} />
            <Screen className="mk-mcp-stack">
              <div className="mk-mcp-keyslot">
                <McpKeyCard />
              </div>
              <div className="mk-mcp-logslot">
                <McpTranscript />
              </div>
              <div className="mk-mcp-stagedslot">
                <McpStaged />
              </div>
            </Screen>
          </div>
        </section>
      }
      related={[
        { route: "product-pilot", label: "Pilot", body: "The same tools, inside the app" },
        { route: "security", label: "Security", body: "Keys, roles, and the audit chain" },
        { route: "enterprise", label: "Enterprise", body: "SSO, SCIM, and deployment options" },
      ]}
      closing={{ title: "Connect an agent under a role", body: "Create a workspace API key with the role you choose, then add the server URL to your client." }}
    >
      <FrameSection
        id="mk-mcp-frame"
        tone="band"
        title="One URL, one key, every call on record"
        body="The agent holds a URL and a key, never a database password. Open a client's setup, and read the calls it made."
        cta={{ label: "Create a key and connect a client", onClick: p.onGetStarted }}
        notes={SCENE_NOTES.mcp}
      >
        <McpScene marks />
      </FrameSection>

      <section className="mk-section mk-section--band mk-mcp-lanesec" aria-labelledby="mk-mcp-lanes">
        <div className="mk-wrap mk-mcp-lanes-grid">
          <SectionHead id="mk-mcp-lanes" title="Stage, then confirm" body="What passes between an agent and Datawrap when the agent asks for a change." />
          <McpLanes />
        </div>
      </section>

      <section className="mk-mcp-stage" aria-labelledby="mk-mcp-tools">
        <div className="mk-wrap">
          <SectionHead id="mk-mcp-tools" title="Every tool needs the permission its API route needs" />
          <div className="mk-mcp-tools-grid">
            {(["read", "plan", "mutate"] as const).map((e) => (
              <Screen key={e} className={`mk-mcp-col is-${e}`}>
                <h3>{EFFECT[e].name}</h3>
                <p>{EFFECT[e].does}</p>
                <table>
                  <caption className="mk-sr-only">{EFFECT[e].name} tools and the permission each needs (an excerpt)</caption>
                  <thead>
                    <tr>
                      <th scope="col">Tool</th>
                      <th scope="col">Permission</th>
                    </tr>
                  </thead>
                  <tbody>
                    {MCP_TOOLS.filter((t) => t.effect === e).map((t) => (
                      <tr key={t.tool}>
                        <th scope="row">
                          <code>{t.tool}</code>
                        </th>
                        <td>
                          <code>{t.perm}</code>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </Screen>
            ))}
          </div>
          <p className="mk-mcp-tools-note">An excerpt of the tool list in the docs.</p>
        </div>
      </section>

      <section className="mk-section mk-mcp-setup" aria-labelledby="mk-mcp-setup">
        <div className="mk-wrap">
          <RulesLedger
            id="mk-mcp-setup"
            title="The confirmation model"
            className="mk-mcp-rules"
            items={[
              { claim: "A staged action does nothing until confirm_action is called with its ack_id", where: "Docs, MCP: the confirmation model" },
              { claim: "An approval expires 15 minutes after staging and is used once", where: "Docs, MCP" },
              { claim: "At confirm time, the permission for that action is checked again", where: "Docs, MCP" },
              { claim: "An API key never acts with more than the person who created it holds today", where: "Docs, MCP: permissions" },
            ]}
          />
        </div>
      </section>
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
        <section className="mk-mig-hero" aria-labelledby="mk-mig-title">
          <div className="mk-wrap">
            <HeroCopy id="mk-mig-title" className="mk-mig-copy" title="Cross-schema cutover you can prove" lead="A map you can review, checks before the write, and a comparison of the destination before anyone cuts over." actions={act} />
            <div className="mk-mig-plan">
              <CutoverLedger />
              <SignoffSheet />
            </div>
          </div>
        </section>
      }
      related={[
        { route: "product-transfer", label: "Transfer Studio", body: "Plan and approve the move" },
        { route: "product-jobs", label: "Job space", body: "The run's evidence" },
        { route: "solution-warehouse", label: "Warehouse loading", body: "Bulk loads into Snowflake, BigQuery, and Iceberg" },
      ]}
      closing={{ title: "Plan your cutover with evidence", body: "Start free with a subset of your data and review the plan before anything is written." }}
    >
      <section className="mk-section mk-mig-door" aria-labelledby="mk-mig-door">
        <div className="mk-wrap">
          <h2 id="mk-mig-door">Cutover is a one-way door.</h2>
          <p>Once applications point at the new system, a missing key is a production incident. A spreadsheet of counts nobody can reproduce does not change that.</p>
        </div>
      </section>

      <FrameSection
        id="mk-mig-map"
        tone="dark"
        title="Schemas that do not match name for name"
        body="Renamed pairs map by meaning. An ambiguous pair waits for a person; a narrowing needs a signed risk record."
        cta={{ label: "Map a subset of your own schema", onClick: p.onGetStarted }}
      >
        <TransferScene migration initial="Map" caption="Example schemas. Validate and Run show the example run: 48,213,906 read." />
      </FrameSection>

      <section className="mk-section mk-mig-details" aria-labelledby="mk-mig-rules">
        <div className="mk-wrap">
          <RulesLedger
            id="mk-mig-rules"
            title="Details that survive real schemas"
            whereLabel="Where to see it"
            className="mk-mig-rules"
            items={[
              { claim: "Sequences, foreign keys, CHECK constraints, and indexes carry on create-new, in dependency order", where: "Transfer Studio, Destination: create new table" },
              { claim: "Unicode text keeps its encoding; collation-aware key checks run in Validate", where: "Validate step" },
              { claim: "Timestamps are compared at the precision the destination column holds", where: "Docs, Verification: known causes of a difference" },
              { claim: "Held rows keep column, value, and reason, and replay into the same table", where: "Job space, Quarantine" },
            ]}
          />
        </div>
      </section>
    </StoryFrame>
  );
}

export function WarehouseSolutionPage(p: PageProps) {
  const act = actions(p, { label: "See Schedules", onClick: () => p.onNavigate("product-pipelines") });
  const route = [
    { step: "Plan", title: "Map by meaning", body: "Exact decimals and timestamps, the same map as any route." },
    { step: "Validate", title: "Probe the destination", body: "Write rights, privileges and capacity, before the load window." },
    { step: "Execute", title: "Write in bulk", body: "Create new, append, overwrite, or MERGE by key." },
    { step: "Verify", title: "Read it back", body: "The warehouse counts and checksums what landed." },
  ];
  return (
    <StoryFrame
      page="wh"
      onNavigate={p.onNavigate}
      actions={act}
      hero={
        <section className="mk-wh-hero" aria-labelledby="mk-wh-title">
          <div className="mk-wrap mk-wh-hero-grid">
            <HeroCopy id="mk-wh-title" title="Warehouse loads that add up" lead="Snowflake, BigQuery, Apache Iceberg, and S3, ADLS or GCS, each load compared against the destination engine afterwards." actions={act} />
            <div className="mk-wh-art">
              <WarehouseRoute />
              <Screen className="mk-wh-types-slot">
                <TypeMapping />
              </Screen>
            </div>
          </div>
        </section>
      }
      related={[
        { route: "solution-sync", label: "Recurring sync", body: "Keep the warehouse current" },
        { route: "integrations", label: "Connectors", body: "Warehouses, lakes, and object storage" },
        { route: "product-jobs", label: "Job space", body: "Each load's evidence" },
      ]}
      closing={{ title: "Load a warehouse on your own data", body: "Start free on the same engine your production loads will use." }}
    >
      <FrameSection
        id="mk-wh-frame"
        tone="dark"
        title="Every load, compared by the warehouse"
        body="Choose a run to open its Trust Report."
        cta={{ label: "Load your own warehouse", onClick: p.onGetStarted }}
      >
        <JobsWithTrust caption="Example workspace. sales_orders into Snowflake: 48,213,906 read = 48,213,871 landed + 33 held + 2 skipped." />
      </FrameSection>

      <section className="mk-section mk-wh-prec" aria-labelledby="mk-wh-prec">
        <div className="mk-wrap mk-wh-prec-grid">
          <SectionHead id="mk-wh-prec" title="A load can succeed and still lose a cent" body="Bulk jobs report success when they finish, not when every value is right. Validate checks the fit first." />
          <PrecisionSpecimen />
        </div>
      </section>

      <section className="mk-section mk-section--band mk-wh-steps" aria-labelledby="mk-wh-steps">
        <div className="mk-wrap">
          <SectionHead id="mk-wh-steps" title="How a warehouse load runs" />
          <ol className="mk-wh-steplist">
            {route.map((s, i) => (
              <li key={s.step}>
                <span className="mk-wh-stepnum" aria-hidden>
                  {i + 1}
                </span>
                <p className="mk-wh-stepname">{s.step}</p>
                <h3>{s.title}</h3>
                <p>{s.body}</p>
              </li>
            ))}
          </ol>
        </div>
      </section>

      <section className="mk-section mk-wh-rulesec" aria-labelledby="mk-wh-rules">
        <div className="mk-wrap">
          <RulesLedger
            id="mk-wh-rules"
            title="Warehouse specifics"
            className="mk-wh-rules"
            items={[
              { claim: "Snowflake connects with a password or key-pair authentication", where: "Connectors, Snowflake" },
              { claim: "Iceberg connects to a REST catalog or a filesystem warehouse", where: "Connectors, Apache Iceberg" },
              { claim: "Upserts use MERGE, or a keyed delete and insert, with no index created", where: "Docs, Sync modes: upsert" },
              { claim: "Duplicate keys already in the destination block an upsert", where: "Docs, Sync modes: upsert" },
            ]}
          />
        </div>
      </section>
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
        <section className="mk-sync-hero" aria-labelledby="mk-sync-title">
          <div className="mk-wrap mk-sync-hero-copy">
            <HeroCopy id="mk-sync-title" title="Keep two systems in step" lead="Run an approved transfer on a schedule, with CDC or a typed cursor. Every run is a checked job with its own statement." actions={act} />
          </div>
          <div className="mk-sync-hero-rail">
            <SyncRail />
            <p className="mk-wrap mk-sync-rail-cap">Example runs of one schedule. Scroll the rail.</p>
          </div>
        </section>
      }
      related={[
        { route: "product-pipelines", label: "Schedules", body: "Cadence, history, and drift review" },
        { route: "solution-warehouse", label: "Warehouse loading", body: "Bulk loads into a warehouse" },
        { route: "product-jobs", label: "Job space", body: "Each run's evidence" },
      ]}
      closing={{ title: "Keep a copy in step", body: "Start free with a schedule on your own connections." }}
    >
      <FrameSection
        id="mk-sync-frame"
        tone="dark"
        title="Every sync, with what it is waiting for"
        body="A schedule paused on drift says so. Choose one to open its runs."
        cta={{ label: "Schedule a sync on your own connections", onClick: p.onGetStarted }}
      >
        <SchedulesScene initial={null} />
      </FrameSection>

      <section className="mk-section mk-sync-drift" aria-labelledby="mk-sync-drift">
        <div className="mk-wrap">
          <SectionHead id="mk-sync-drift" title="Two systems that agree on Monday disagree by Friday" />
          <table className="mk-sync-fixes">
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
        </div>
      </section>

      <section className="mk-sb-dark mk-sync-stage" aria-labelledby="mk-sync-delete">
        <div className="mk-wrap mk-sync-stage-grid">
          <SectionHead id="mk-sync-delete" title="A delete at the source, carried to the copy" body="Soft by default: the row is kept and flagged, so the copy shows what was removed and when." />
          <Screen className="mk-sync-stage-panel">
            <MirrorDeletePanel />
          </Screen>
        </div>
      </section>

      <section className="mk-section mk-sync-cdcsec" aria-labelledby="mk-sync-cdc">
        <div className="mk-wrap">
          <SectionHead id="mk-sync-cdc" title="CDC from the database log" body="A consistent snapshot first, then changes from the log, deletes included. Proven in CI on these sources:" />
          <CdcSources />
          <RulesLedger
            id="mk-sync-rules"
            title="What CDC refuses, and why"
            className="mk-sync-rules"
            items={[
              { claim: "CDC refuses sources it does not support at Validate", where: "Docs, Sync modes: CDC" },
              { claim: "If capture cannot attach to a log the server emits, the run fails with the remedy rather than polling", where: "Docs, Sync modes: CDC" },
              { claim: "Append-only destinations such as Kafka topics accept only append modes", where: "Docs, Sync modes: append-only destinations" },
            ]}
          />
        </div>
      </section>
    </StoryFrame>
  );
}
