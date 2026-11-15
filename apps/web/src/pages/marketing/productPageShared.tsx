import type { ReactNode } from "react";
import { DocsShotReel, type DocsShotFrame } from "../../components/docs/DocsShotReel";
import { WORKSPACE_SHOT } from "../../lib/workspaceShots";

export { WORKSPACE_SHOT, DEMO_ROUTE, DEMO_SCHEDULE_ROUTE } from "../../lib/workspaceShots";

export const PRODUCT_FRAMES = {
  transfer: [
    {
      src: WORKSPACE_SHOT.transferSource,
      alt: "Transfer Studio Source step with Orders Postgres (demo) sales_orders profiled: 11 fields, 100 sample rows",
      caption: "Source · sales_orders on Orders Postgres, profiled from 100 sample rows before anything else happens",
    },
    {
      src: WORKSPACE_SHOT.transferDest,
      alt: "Transfer Studio Destination step targeting Warehouse Snowflake (demo) DW_DEMO.SALES_ORDERS_REPORTING",
      caption: "Destination · Snowflake DW_DEMO, a new SALES_ORDERS_REPORTING table, 11 typed columns planned",
    },
    {
      src: WORKSPACE_SHOT.transferMapWhy,
      alt: "Map step showing JSONB attributes mapped to Snowflake VARIANT with Parse JSON at 93% confidence",
      caption: "Map · JSONB lands as VARIANT with Parse JSON; the row explains why, with confidence and a sample",
    },
    {
      src: WORKSPACE_SHOT.transferValidateBlocked,
      alt: "Validate blocked on one gate: the destination table does not exist and schema policy needs DDL approval",
      caption: "Validate · one gate blocks: the CREATE TABLE needs approval first, so Execute stays locked",
    },
    {
      src: WORKSPACE_SHOT.transferValidate,
      alt: "Validate approved: 17 passed, 0 blocked, 6 skipped across 23 gates in 107.7 seconds",
      caption: "Validate · approved after DDL sign-off: 23 gates, 0 blocking, Execute unlocked",
    },
    {
      src: WORKSPACE_SHOT.transferRunComplete,
      alt: "Run step finished: 5,100 rows appended, checksum recorded, proof scope full checksum with independent source re-read",
      caption: "Run · 5,100 read, 5,100 written, destination before 0 and after 5,100, checksum on record",
    },
  ],
  jobs: [
    {
      src: WORKSPACE_SHOT.jobTrust,
      alt: "Job space Trust Report: 100% rows proven equal, 5,100 rows in source and destination, 0 differ",
      caption: "Trust Report · 5,100 rows in source, 5,100 in Snowflake, 0 differ, full checksum per table",
    },
    {
      src: WORKSPACE_SHOT.jobTrustExplained,
      alt: "Trust Report for a filtered MongoDB load: 5,100 source rows, 3,756 landed, 1,344 differences all explained",
      caption: "When rows differ on purpose · a filter kept 3,756 of 5,100; all 1,344 differences are explained",
    },
    {
      src: WORKSPACE_SHOT.jobSignoff,
      alt: "Sign-off panel waiting on Engineering, Data owner, and Compliance before the evidence pack is signed",
      caption: "Sign-off · Engineering, Data owner, and Compliance each sign the same report",
    },
    {
      src: WORKSPACE_SHOT.jobReversibility,
      alt: "Reversibility tab: 0 lossy, 0 conditional, 11 invertible columns; rollback by restoring from destination",
      caption: "Reversibility · 11 of 11 columns round-trip exactly, so the destination can give the data back",
    },
    {
      src: WORKSPACE_SHOT.jobReversibilityLossy,
      alt: "Reversibility on a lossy plan: ordered_at truncated to a date and a filter_rows step mark the table Partly reversible",
      caption: "And when it can't · a truncated timestamp and a row filter are named as lossy, column by column",
    },
  ],
  pipelines: [
    {
      src: WORKSPACE_SHOT.schedules,
      alt: "Schedules list with one schedule parked on a finding: upsert needs a business key",
      caption: "Schedules · a run that can't be safe is parked with the reason and the fix, not retried",
    },
    {
      src: WORKSPACE_SHOT.schedulesDrawer,
      alt: "Orders feed schedule drawer: every 2 minutes, incremental deduped, trust score 78 C",
      caption: "Schedule detail · cadence, sync mode, reversibility requirement, and the last run's trust score",
    },
    {
      src: WORKSPACE_SHOT.schedulesHistory,
      alt: "Run history for the Orders feed schedule, each run linked to its job",
      caption: "History · every run is a real job, with rows written, rejects, and a link to its evidence",
    },
    {
      src: WORKSPACE_SHOT.schedulesReversibilityRetries,
      alt: "Create recurring sync form with Require reversibility before each run checked and two retries",
      caption: "Fail-closed options · require reversibility before each run; retries and alerts per schedule",
    },
  ],
  query: [
    {
      src: WORKSPACE_SHOT.query,
      alt: "Query on Snowflake: orders, units, and revenue by region from SALES_ORDERS_REPORTING, five rows",
      caption: "Query · revenue by region on the Snowflake copy, read-only, 5 rows in 3.1 s",
    },
    {
      src: WORKSPACE_SHOT.querySource,
      alt: "The same aggregate on the source Postgres sales_orders table returns identical numbers",
      caption: "Same query on the Postgres source · identical totals, region by region",
    },
    {
      src: WORKSPACE_SHOT.queryReadonly,
      alt: "Query refuses a DELETE statement: only read and metadata queries run in the playground",
      caption: "Read-only by design · a DELETE is refused before it reaches the database",
    },
  ],
  pilot: [
    {
      src: WORKSPACE_SHOT.pilot,
      alt: "Pilot answering an exact row count and revenue by region for sales_orders with the SQL it ran",
      caption: "Pilot · exact server-side counts and aggregates, with the SQL shown under every answer",
    },
    {
      src: WORKSPACE_SHOT.jobTrust,
      alt: "Job space Trust Report for the Snowflake run",
      caption: "The evidence it answers from · the same job records operators open in Job space",
    },
  ],
  mcp: [
    {
      src: WORKSPACE_SHOT.mcp,
      alt: "MCP Server page: server URL, client setup for Cursor, Claude Desktop, VS Code, and custom GPT",
      caption: "MCP Server · one URL, a workspace API key, and setup for each client",
    },
    {
      src: WORKSPACE_SHOT.jobs,
      alt: "Jobs list where agent-started transfers appear beside UI runs",
      caption: "Agent runs land in Jobs next to everyone else's, with the same verification column",
    },
    {
      src: WORKSPACE_SHOT.connectors,
      alt: "Saved connections an agent can use under its key's role",
      caption: "Agents use saved connections by reference; credentials never leave the vault",
    },
  ],
} as const satisfies Record<string, DocsShotFrame[]>;

export const SOLUTION_FRAMES = {
  migrations: PRODUCT_FRAMES.transfer,
  warehouse: [PRODUCT_FRAMES.transfer[2], PRODUCT_FRAMES.transfer[5], PRODUCT_FRAMES.jobs[0], PRODUCT_FRAMES.query[0]],
  sync: PRODUCT_FRAMES.pipelines,
} as const;

/** Real preflight gates from packages/preflight (G1–G9). */
export const REAL_PREFLIGHT_GATES: { id: string; title: string; algorithm: string }[] = [
  {
    id: "G1", // Source readable / parseable
    title: "Source",
    algorithm:
      "Connect → parse headers/encoding → require ≥1 column. Block on corrupt files, empty schemas, or unreachable sources.",
  },
  {
    id: "G2", // Destination reachable with write access
    title: "Destination",
    algorithm:
      "Probe reachability and write privileges. Block when credentials fail or the role cannot write the target object.",
  },
  {
    id: "G3", // Schema contract — typed dest DDL / schemaless SKIP
    title: "Schema contract",
    algorithm:
      "For typed destinations, validate every mapped field against destination DDL (type family, nullability, precision). Schemaless destinations skip DDL but still map.",
  },
  {
    id: "G4", // Mapping confidence ≥ threshold; required fields mapped
    title: "Mapping confidence",
    algorithm:
      "Score each edge (exact → synonym → semantic role → type compatibility). Edges below the workspace threshold (default 0.85 strict / 0.72 floor) block until pinned or remapped.",
  },
  {
    id: "G5", // Dry-run transform on sample rows
    title: "Dry-run",
    algorithm:
      "Push a sample through the real transform + coerce path. Surface duplicates, 100% null columns, and irreversible casts before production write.",
  },
  {
    id: "G6", // Target DDL compatible
    title: "Target DDL",
    algorithm:
      "Verify the target table/collection accepts the write plan (create-if-missing vs existing PKs/required fields).",
  },
  {
    id: "G7", // Staging capacity — unknown estimate fails closed
    title: "Capacity",
    algorithm:
      "Compare estimated volume to destination limits / warehouse slots. Warn or block per policy before the window opens.",
  },
  {
    id: "G8", // Pre-write sample reconciliation; post-write checksum after Execute
    title: "Reconciliation plan",
    algorithm:
      "Select the row-count, checksum, and census strategy for post-load proof. After the write, the engine reads the destination back and verifies it.",
  },
  {
    id: "G9", // Data integrity audit — unproven / not-configured fails closed
    title: "Data integrity",
    algorithm:
      "Audit encoding, required nulls, identity-key duplicates, and financial precision on the Validate sample. Unproven or missing audit adapters fail closed.",
  },
];

export function LiveProductReel({
  frames,
  title,
  surface = "Workspace",
}: {
  frames: readonly DocsShotFrame[];
  title: string;
  surface?: string;
}) {
  return (
    <div className="lp-mkt-live-reel">
      <div className="lp-mkt-live-reel-head">
        <span className="lp-mkt-live-pill">Live workspace</span>
        <h3>{title}</h3>
        <p>Captured in the hosted workspace on a real Postgres → Snowflake load of 5,100 orders.</p>
      </div>
      <DocsShotReel frames={[...frames]} className="docs-shot-reel--product" surface={surface} />
    </div>
  );
}

export function AlgoBlock({
  title,
  lead,
  steps,
}: {
  title: string;
  lead: string;
  steps: { name: string; detail: string }[];
}) {
  return (
    <div className="lp-mkt-algo">
      <div className="lp-mkt-algo-copy">
        <h3>{title}</h3>
        <p>{lead}</p>
      </div>
      <ol className="lp-mkt-algo-steps">
        {steps.map((s, i) => (
          <li key={s.name} style={{ ["--i" as string]: i }}>
            <span className="lp-mkt-algo-num">{String(i + 1).padStart(2, "0")}</span>
            <div>
              <strong>{s.name}</strong>
              <p>{s.detail}</p>
            </div>
          </li>
        ))}
      </ol>
    </div>
  );
}

export function ProofCallout({ children }: { children: ReactNode }) {
  return <aside className="lp-mkt-proof-callout">{children}</aside>;
}

export function GateTable() {
  return (
    <div className="lp-mkt-gate-table" role="table" aria-label="Preflight gates">
      <div className="lp-mkt-gate-table-head" role="row">
        <span role="columnheader">Gate</span>
        <span role="columnheader">Algorithm</span>
      </div>
      {REAL_PREFLIGHT_GATES.map((g) => (
        <div key={g.id} className="lp-mkt-gate-table-row" role="row">
          <span role="cell">
            <code>{g.id}</code>
            <strong>{g.title}</strong>
          </span>
          <span role="cell">{g.algorithm}</span>
        </div>
      ))}
    </div>
  );
}
