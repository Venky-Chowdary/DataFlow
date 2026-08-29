import type { ReactNode } from "react";
import { DocsShotReel, type DocsShotFrame } from "../../components/docs/DocsShotReel";

/** Current workspace photography — never Overview (first-load count is a separate fix). */
export const WORKSPACE_SHOT = {
  jobs: "/docs/screenshots/app-jobs.png",
  connectors: "/docs/screenshots/app-connectors.png",
  pipelines: "/docs/screenshots/app-pipelines.png",
  query: "/docs/screenshots/app-query.png",
  pilot: "/docs/screenshots/app-pilot.png",
  mcp: "/docs/screenshots/app-mcp.png",
  transferSource: "/docs/screenshots/app-transfer-source.png",
  transferDest: "/docs/screenshots/app-transfer-destination.png",
  transferMap: "/docs/screenshots/app-transfer-map.png",
  transferValidate: "/docs/screenshots/app-transfer-validate.png",
  transferRun: "/docs/screenshots/app-transfer-run.png",
} as const;

export const PRODUCT_FRAMES = {
  transfer: [
    {
      src: WORKSPACE_SHOT.transferSource,
      alt: "Transfer Studio source step — File, Database, or Cloud storage",
      caption: "1 · Source — File, Database, or Cloud. Profile before Dest.",
    },
    {
      src: WORKSPACE_SHOT.transferDest,
      alt: "Transfer Studio destination File Export CSV",
      caption: "2 · Destination — File Export CSV or a saved warehouse",
    },
    {
      src: WORKSPACE_SHOT.transferMap,
      alt: "Transfer Studio Map columns for sample-orders",
      caption: "3 · Map — Align columns, Accept risk, continue",
    },
    {
      src: WORKSPACE_SHOT.transferValidate,
      alt: "Transfer Studio Validate gates dashboard",
      caption: "4 · Validate — Clear blocking gates before Execute unlocks",
    },
    {
      src: WORKSPACE_SHOT.transferRun,
      alt: "Transfer Studio Run step before Execute",
      caption: "5 · Run — Execute when Preflight is approved",
    },
  ],
  jobs: [
    {
      src: WORKSPACE_SHOT.jobs,
      alt: "Job Theater with whole-history counts and destination population",
      caption: "Job Theater — queue → preflight → extract → load → reconcile",
    },
    {
      src: WORKSPACE_SHOT.transferSource,
      alt: "Transfer Studio source that produces jobs",
      caption: "Upstream plan — the Studio source that feeds Theater",
    },
    {
      src: WORKSPACE_SHOT.connectors,
      alt: "Connectors catalog in the live workspace",
      caption: "Connectors — saved drivers Jobs and Studio share",
    },
  ],
  pipelines: [
    {
      src: WORKSPACE_SHOT.pipelines,
      alt: "Schedules workspace",
      caption: "Schedules — cadence, mode, and health for recurring sync",
    },
    {
      src: WORKSPACE_SHOT.jobs,
      alt: "Job created by a pipeline tick",
      caption: "Every tick is a real job — same Theater proof as Studio",
    },
    {
      src: WORKSPACE_SHOT.connectors,
      alt: "Connectors used by scheduled routes",
      caption: "Same connectors — schedules never invent a second vault",
    },
  ],
  query: [
    {
      src: WORKSPACE_SHOT.query,
      alt: "Query Playground in the live workspace",
      caption: "Query Playground — read-only SQL against saved connectors",
    },
    {
      src: WORKSPACE_SHOT.connectors,
      alt: "Connectors available to Query",
      caption: "Same connectors — Query never invents a second credential path",
    },
    {
      src: WORKSPACE_SHOT.transferSource,
      alt: "Handoff into Transfer Studio",
      caption: "Handoff — validated slices become Studio plans",
    },
  ],
  pilot: [
    {
      src: WORKSPACE_SHOT.pilot,
      alt: "Datawrap Pilot natural-language triage",
      caption: "Datawrap Pilot — NL triage on the governed engine",
    },
    {
      src: WORKSPACE_SHOT.jobs,
      alt: "Job Theater evidence Pilot references",
      caption: "Evidence — Pilot cites the same Theater artifacts humans see",
    },
    {
      src: WORKSPACE_SHOT.transferSource,
      alt: "Transfer Studio handoff from Pilot",
      caption: "Handoff — fixes still flow through Studio review + gates",
    },
  ],
  mcp: [
    {
      src: WORKSPACE_SHOT.mcp,
      alt: "MCP Server page in the live workspace",
      caption: "Agents share the workspace — MCP never returns raw passwords",
    },
    {
      src: WORKSPACE_SHOT.jobs,
      alt: "MCP-triggered job in Theater",
      caption: "Agent runs appear in Job Theater with full gate + proof audit",
    },
    {
      src: WORKSPACE_SHOT.connectors,
      alt: "Connectors agents inherit under RBAC",
      caption: "Same connectors — agents never receive raw destination secrets",
    },
  ],
} as const satisfies Record<string, DocsShotFrame[]>;

export const SOLUTION_FRAMES = {
  migrations: PRODUCT_FRAMES.transfer,
  warehouse: [PRODUCT_FRAMES.jobs[0], PRODUCT_FRAMES.pipelines[0], PRODUCT_FRAMES.transfer[3]],
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
      "Compare estimated volume to destination limits / warehouse slots. Warn or block per policy — never assume infinite capacity.",
  },
  {
    id: "G8", // Pre-write sample reconciliation; post-write checksum after Execute
    title: "Reconciliation plan",
    algorithm:
      "Select row-count + content-checksum strategy for post-load proof. Without a reconcile plan, the run cannot claim success.",
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
        <p>The same operator surfaces — framed in product chrome, not redrawn as a second UI.</p>
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
          <li key={s.name}>
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
