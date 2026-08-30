/**
 * Enterprise / catalog / knowledge hero drawings —
 * see docs/MARKETING_HERO_DESIGN_SYSTEM.md.
 */

import {
  ArtField,
  ArtFilament,
  ArtLabel,
  ArtPacket,
  ArtPlate,
  ArtText,
  HeroArtFrame,
  INK,
} from "./HeroArtFrame";

/* ── Enterprise · the governed perimeter ────────────────────────── */

const RINGS = [
  { r: 292, label: "identity · sso / scim", angle: -42, lx: 946, ly: 120, anchor: "end" as const },
  { r: 232, label: "rbac · role permissions", angle: 42, lx: 946, ly: 556, anchor: "end" as const },
  { r: 172, label: "audit · actor · artifact", angle: 138, lx: 58, ly: 556, anchor: "start" as const },
];

export function PerimeterArt() {
  const cx = 500;
  const cy = 330;
  const point = (r: number, deg: number) => {
    const a = (deg * Math.PI) / 180;
    return { x: cx + Math.cos(a) * r, y: cy + Math.sin(a) * r };
  };

  return (
    <HeroArtFrame
      label="Concentric identity, permission, and audit rings around an isolated workspace"
      caption="Every action crosses identity, then permission, then the audit record."
      focus={{ x: 200, y: 60, w: 600, h: 540 }}
    >
      {RINGS.map((ring) => {
        const p = point(ring.r, ring.angle);
        return (
          <g key={ring.label}>
            <circle
              cx={cx}
              cy={cy}
              r={ring.r}
              fill="none"
              stroke={INK.line}
              strokeWidth="1.5"
              strokeDasharray={ring.r === 292 ? "6 10" : undefined}
            />
            <circle cx={p.x} cy={p.y} r="7" fill={INK.teal} />
            <line x1={p.x} y1={p.y} x2={ring.lx} y2={ring.ly} stroke={INK.line} strokeWidth="1.5" />
            <ArtLabel x={ring.lx} y={ring.ly - 16} anchor={ring.anchor} tone="teal">
              {ring.label}
            </ArtLabel>
          </g>
        );
      })}

      <circle cx={cx} cy={cy} r="112" fill={INK.plate} stroke={INK.tealDeep} strokeWidth="2" />
      <ArtText x={cx} y={cy - 16} anchor="middle" size={20}>
        Workspace
      </ArtText>
      <ArtText x={cx} y={cy + 14} anchor="middle" size={15} tone="teal">
        tenant isolated
      </ArtText>
      <ArtText x={cx} y={cy + 44} anchor="middle" size={14} tone="muted" mono>
        region pinned
      </ArtText>

      <ArtPacket path={`M${cx} ${cy - 292} A292 292 0 0 1 ${cx} ${cy + 292}`} dur={7} />

      <ArtLabel x={58} y={120}>
        every call enters here
      </ArtLabel>
      <ArtText x={58} y={148} size={15} tone="muted">
        UI · Pilot · MCP — one gate
      </ArtText>
    </HeroArtFrame>
  );
}

/* ── Security · the vault wall ──────────────────────────────────── */

export function VaultArt() {
  return (
    <HeroArtFrame
      label="Credentials terminating at the secret store while data moves separately"
      caption="The data path never carries the secret — only a reference the engine resolves."
      focus={{ x: 300, y: 112, w: 680, h: 340 }}
    >
      <ArtLabel x={64} y={104}>
        connectors
      </ArtLabel>
      {["Prod Postgres", "Snowflake", "S3 exports"].map((c, i) => (
        <ArtField key={c} x={64} y={128 + i * 62} w={252} name={c} type="ref" />
      ))}

      {/* Only references cross */}
      {[0, 1, 2].map((i) => (
        <g key={i}>
          <path
            d={`M316 ${149 + i * 62} C440 ${149 + i * 62} 470 240 ${568} 240`}
            fill="none"
            stroke={INK.teal}
            strokeWidth="2"
            strokeOpacity="0.7"
          />
        </g>
      ))}
      <ArtPacket path="M316 149 C440 149 470 240 568 240" dur={3.6} />
      <ArtText x={330} y={330} size={14} tone="teal">
        secret_ref only
      </ArtText>

      {/* Raw material is refused */}
      <path d="M316 396 H520" stroke={INK.danger} strokeWidth="2.5" strokeDasharray="8 7" />
      <g>
        <circle cx={548} cy={396} r="15" fill="none" stroke={INK.danger} strokeWidth="2.5" />
        <path d="M538 386l20 20M558 386l-20 20" stroke={INK.danger} strokeWidth="2.5" strokeLinecap="round" />
      </g>
      <ArtText x={316} y={374} size={14} tone="danger">
        raw password to a caller
      </ArtText>

      {/* The vault */}
      <ArtPlate x={600} y={132} w={362} h={296} tone="teal" radius={16} />
      <ArtLabel x={628} y={172} tone="teal">
        secret store
      </ArtLabel>
      <ArtField x={628} y={188} w={306} name="envelope encrypted" type="AES-GCM" />
      <ArtField x={628} y={244} w={306} name="BYOK key reference" type="tenant KMS" />
      <ArtField x={628} y={300} w={306} name="rotation + audit" type="who · when" />
      <circle cx={781} cy={392} r="22" fill={INK.field0} stroke={INK.teal} strokeWidth="2.5" />
      {[0, 60, 120, 180, 240, 300].map((deg) => {
        const a = (deg * Math.PI) / 180;
        return (
          <line
            key={deg}
            x1={781 + Math.cos(a) * 22}
            y1={392 + Math.sin(a) * 22}
            x2={781 + Math.cos(a) * 32}
            y2={392 + Math.sin(a) * 32}
            stroke={INK.teal}
            strokeWidth="2"
          />
        );
      })}

      {/* The data lane runs outside the vault entirely */}
      <ArtFilament x1={64} x2={962} y={514} tone="teal" width={2.5} />
      <ArtPacket path="M64 514 H962" dur={5} />
      <ArtLabel x={64} y={558}>
        data path
      </ArtLabel>
      <ArtText x={962} y={558} anchor="end" size={15} tone="muted">
        rows move · secrets do not travel with them
      </ArtText>
    </HeroArtFrame>
  );
}

/* ── Integrations · the lattice ─────────────────────────────────── */

const FAMILIES: { title: string; nodes: string[] }[] = [
  { title: "files", nodes: ["CSV", "JSONL", "Parquet", "Excel"] },
  { title: "oltp", nodes: ["PostgreSQL", "MySQL", "Oracle", "SQL Server"] },
  { title: "warehouse", nodes: ["Snowflake", "BigQuery", "Redshift", "Databricks"] },
  { title: "object · nosql", nodes: ["S3", "GCS", "ADLS", "MongoDB"] },
];

export function LatticeArt({ readyCount }: { readyCount?: number }) {
  const colX = (i: number) => 118 + i * 256;
  const nodeY = (i: number) => 176 + i * 78;

  return (
    <HeroArtFrame
      label="Connector families joined by typed routes"
      caption="Any family to any family — the route is typed, not assumed."
      focus={{ x: 8, y: 108, w: 620, h: 420 }}
    >
      {FAMILIES.map((f, ci) => (
        <g key={f.title}>
          <ArtLabel x={colX(ci)} y={128} anchor="middle" tone={ci === 2 ? "teal" : "muted"}>
            {f.title}
          </ArtLabel>
          {f.nodes.map((n, ri) => (
            <g key={n}>
              <rect
                x={colX(ci) - 92}
                y={nodeY(ri)}
                width="184"
                height="46"
                rx="10"
                fill={INK.field0}
                stroke={ci === 2 ? INK.tealDeep : INK.plateEdge}
                strokeWidth="1.5"
              />
              <ArtText x={colX(ci)} y={nodeY(ri) + 29} anchor="middle" size={16} weight={550}>
                {n}
              </ArtText>
            </g>
          ))}
        </g>
      ))}

      {/* Typed lanes across families — a graph, not a hub and spoke */}
      {[
        { a: [0, 0], b: [1, 1] },
        { a: [1, 0], b: [2, 0] },
        { a: [1, 2], b: [2, 3] },
        { a: [2, 1], b: [3, 0] },
        { a: [0, 3], b: [2, 2] },
        { a: [3, 3], b: [1, 3] },
      ].map((lane, i) => {
        const x1 = colX(lane.a[0]) + 92;
        const y1 = nodeY(lane.a[1]) + 23;
        const x2 = colX(lane.b[0]) - 92;
        const y2 = nodeY(lane.b[1]) + 23;
        const d = `M${x1} ${y1} C${(x1 + x2) / 2} ${y1} ${(x1 + x2) / 2} ${y2} ${x2} ${y2}`;
        return (
          <g key={i}>
            <path d={d} fill="none" stroke={INK.teal} strokeOpacity="0.34" strokeWidth="2" />
            {i === 1 ? <ArtPacket path={d} dur={4} /> : null}
          </g>
        );
      })}

      <ArtFilament x1={64} x2={962} y={536} width={1.5} />
      <ArtText x={64} y={578} size={16} tone="teal">
        {readyCount ? `${readyCount} drivers marked TRANSFER_READY` : "typed routes across every family"}
      </ArtText>
      <ArtText x={962} y={578} anchor="end" size={15} tone="muted">
        unproven routes stay labelled unproven
      </ArtText>
    </HeroArtFrame>
  );
}
