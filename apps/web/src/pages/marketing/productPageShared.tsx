import type { ReactNode } from "react";
import { DocsShotReel, type DocsShotFrame } from "../../components/docs/DocsShotReel";

export const PRODUCT_FRAMES = {
  transfer: [
    {
      src: "/docs/screenshots/app-transfer-source.png",
      alt: "Transfer Studio source step with sample-orders.csv typed columns",
      caption: "Transfer Studio · Source — profiled sample-orders.csv with live column types",
    },
    {
      src: "/docs/screenshots/app-jobs.png",
      alt: "Job Theater after a Transfer Studio run",
      caption: "After Run — Job Theater shows reconcile and row fidelity",
    },
    {
      src: "/docs/screenshots/app-overview.png",
      alt: "Workspace Overview with live throughput",
      caption: "Overview — rows moved and connection health from the same workspace",
    },
  ],
  jobs: [
    {
      src: "/docs/screenshots/app-jobs.png",
      alt: "Job Theater reconcile timeline for e2e_customers",
      caption: "Job Theater — queue → preflight → extract → load → reconcile",
    },
    {
      src: "/docs/screenshots/app-transfer-source.png",
      alt: "Source that produced the job",
      caption: "Upstream plan — the Studio source that fed this job",
    },
    {
      src: "/docs/screenshots/app-connectors.png",
      alt: "Connectors used by the job",
      caption: "Connectors — Postgres / MySQL / Mongo with Test passed status",
    },
  ],
  pipelines: [
    {
      src: "/docs/screenshots/app-pipelines.png",
      alt: "Pipelines schedules workspace",
      caption: "Pipelines — cadence, mode, and health for recurring sync",
    },
    {
      src: "/docs/screenshots/app-jobs.png",
      alt: "Job created by a pipeline tick",
      caption: "Every tick is a real job — same Theater proof as Studio",
    },
    {
      src: "/docs/screenshots/app-overview.png",
      alt: "Overview of pipeline throughput",
      caption: "Overview — throughput from scheduled and ad-hoc loads",
    },
  ],
  query: [
    {
      src: "/docs/screenshots/app-query.png",
      alt: "Query Playground SQL editor",
      caption: "Query Playground — read-only SQL against saved connectors",
    },
    {
      src: "/docs/screenshots/app-connectors.png",
      alt: "Connectors available to Query",
      caption: "Same connectors — Query never invents a second credential path",
    },
    {
      src: "/docs/screenshots/app-transfer-source.png",
      alt: "Handoff into Transfer Studio",
      caption: "Handoff — validated slices become Studio plans",
    },
  ],
  pilot: [
    {
      src: "/docs/screenshots/app-pilot.png",
      alt: "Data Pilot natural-language triage",
      caption: "Data Pilot — NL triage on the governed engine",
    },
    {
      src: "/docs/screenshots/app-jobs.png",
      alt: "Job Theater evidence Pilot references",
      caption: "Evidence — Pilot cites the same Theater artifacts humans see",
    },
    {
      src: "/docs/screenshots/app-transfer-source.png",
      alt: "Transfer Studio handoff from Pilot",
      caption: "Handoff — fixes still flow through Studio review + gates",
    },
  ],
  mcp: [
    {
      src: "/docs/screenshots/app-pilot.png",
      alt: "Agent-adjacent workspace surface",
      caption: "Agents share the workspace — MCP never returns raw passwords",
    },
    {
      src: "/docs/screenshots/app-jobs.png",
      alt: "MCP-triggered job in Theater",
      caption: "Agent runs appear in Job Theater with full gate + proof audit",
    },
    {
      src: "/docs/screenshots/app-overview.png",
      alt: "Workspace overview for MCP operators",
      caption: "Same Overview metrics whether the operator is human or agent",
    },
  ],
} as const satisfies Record<string, DocsShotFrame[]>;

/** Real preflight gates from packages/preflight (G1–G8). */
export const REAL_PREFLIGHT_GATES: { id: string; title: string; algorithm: string }[] = [
  {
    id: "G1",
    title: "Source",
    algorithm:
      "Connect → parse headers/encoding → require ≥1 column. Block on corrupt files, empty schemas, or unreachable sources.",
  },
  {
    id: "G2",
    title: "Destination",
    algorithm:
      "Probe reachability and write privileges. Block when credentials fail or the role cannot write the target object.",
  },
  {
    id: "G3",
    title: "Schema contract",
    algorithm:
      "For typed destinations, validate every mapped field against destination DDL (type family, nullability, precision). Schemaless destinations skip DDL but still map.",
  },
  {
    id: "G4",
    title: "Mapping confidence",
    algorithm:
      "Score each edge (exact → synonym → semantic role → type compatibility). Edges below the workspace threshold (default 0.85 strict / 0.72 floor) block until pinned or remapped.",
  },
  {
    id: "G5",
    title: "Dry-run",
    algorithm:
      "Push a sample through the real transform + coerce path. Surface duplicates, 100% null columns, and irreversible casts before production write.",
  },
  {
    id: "G6",
    title: "Target DDL",
    algorithm:
      "Verify the target table/collection accepts the write plan (create-if-missing vs existing PKs/required fields).",
  },
  {
    id: "G7",
    title: "Capacity",
    algorithm:
      "Compare estimated volume to destination limits / warehouse slots. Warn or block per policy — never assume infinite capacity.",
  },
  {
    id: "G8",
    title: "Reconciliation plan",
    algorithm:
      "Select row-count + content-checksum strategy for post-load proof. Without a reconcile plan, the run cannot claim success.",
  },
];

export function LiveProductReel({
  frames,
  title,
}: {
  frames: readonly DocsShotFrame[];
  title: string;
}) {
  return (
    <div className="lp-mkt-live-reel">
      <div className="lp-mkt-live-reel-head">
        <span className="lp-mkt-live-pill">Live workspace</span>
        <h3>{title}</h3>
        <p>Screenshots captured inside the signed-in application — not marketing mocks.</p>
      </div>
      <DocsShotReel frames={[...frames]} className="docs-shot-reel--product" />
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
