/**
 * Product-surface hero drawings. Each one argues that page's own mechanism —
 * see docs/MARKETING_HERO_DESIGN_SYSTEM.md. Geometry only; the frame owns the
 * field, grid, light, and type scale.
 */

import {
  ArtField,
  ArtFilament,
  ArtLabel,
  ArtPacket,
  ArtPlate,
  ArtSeal,
  ArtText,
  HeroArtFrame,
  INK,
} from "./HeroArtFrame";

/* ── Transfer Studio · the gate comb ────────────────────────────── */

const GATES = ["G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8", "G9"];
const HELD_GATE = 3; // G4 — mapping confidence

export function GateCombArt() {
  const rows = [200, 268, 336, 404, 472];
  const bladeX = (i: number) => 220 + i * 70;
  const heldX = bladeX(HELD_GATE);

  return (
    <HeroArtFrame
      label="Nine preflight blades with one row held at gate G4"
      caption="Schematic of preflight — nine blades, one held row. Not a live run."
      focus={{ x: 396, y: 116, w: 580, h: 470 }}
    >
      <ArtLabel x={64} y={96}>
        source rows
      </ArtLabel>
      <ArtLabel x={500} y={96} tone="teal" anchor="middle">
        preflight g1–g9
      </ArtLabel>
      <ArtLabel x={962} y={96} anchor="end">
        destination
      </ArtLabel>

      {/* Blades — the comb every row must pass through */}
      {GATES.map((g, i) => {
        const held = i === HELD_GATE;
        return (
          <g key={g}>
            <rect
              x={bladeX(i)}
              y={132}
              width="12"
              height="392"
              rx="6"
              fill={held ? INK.amberSoft : INK.field0}
              stroke={held ? INK.amber : INK.line}
              strokeWidth={held ? 2 : 1.5}
            />
            <ArtText x={bladeX(i) + 6} y={556} anchor="middle" size={14} tone={held ? "amber" : "muted"} mono>
              {g}
            </ArtText>
          </g>
        );
      })}

      {/* Rows as filaments threading the comb */}
      {rows.map((y, i) => {
        const held = i === 2;
        return (
          <g key={y}>
            <ArtFilament x1={64} x2={held ? heldX : 838} y={y} tone={held ? "amber" : "teal"} width={held ? 2.5 : 2} />
            {held ? (
              <>
                <ArtFilament x1={heldX + 18} x2={838} y={y} dashed />
                <circle cx={heldX + 6} cy={y} r="9" fill={INK.field0} stroke={INK.amber} strokeWidth="2.5" />
              </>
            ) : null}
          </g>
        );
      })}

      <ArtPacket path={`M64 ${rows[0]} H838`} dur={5} />
      <ArtPacket path={`M64 ${rows[4]} H838`} dur={5} delay={1.6} />

      {/* The refusal, in the engine's own words */}
      <ArtPlate x={heldX + 26} y={310} w={300} h={54} tone="amber" radius={10} />
      <ArtText x={heldX + 44} y={343} size={16} tone="amber">
        holds: id ≠ warehouse key
      </ArtText>

      {/* Destination — only rows that cleared every blade arrive */}
      <ArtPlate x={848} y={160} w={114} h={336} tone="sunken" />
      {rows.map((y, i) => (
        <rect
          key={y}
          x={868}
          y={y - 8}
          width="74"
          height="16"
          rx="8"
          fill={i === 2 ? "transparent" : "rgba(45, 212, 191, 0.32)"}
          stroke={i === 2 ? INK.line : "none"}
          strokeDasharray={i === 2 ? "4 5" : undefined}
        />
      ))}
      <ArtText x={905} y={528} anchor="middle" size={15} tone="teal">
        4 of 5 written
      </ArtText>
    </HeroArtFrame>
  );
}

/* ── Job Theater · the run spine ────────────────────────────────── */

const PHASES = ["Queued", "Preflight", "Write", "Reread", "Reconcile"];

export function RunSpineArt() {
  const y = 232;
  const nodeX = (i: number) => 96 + i * 172;

  return (
    <HeroArtFrame
      label="Run spine from queue to reconciliation, with a quarantine branch"
      caption="Every phase on the record: bad rows branch off, proof terminates the line."
      focus={{ x: 300, y: 160, w: 640, h: 410 }}
    >
      <ArtLabel x={64} y={120}>
        run spine
      </ArtLabel>

      <ArtFilament x1={96} x2={nodeX(4)} y={y} tone="teal" width={3} />
      <ArtPacket path={`M96 ${y} H${nodeX(4)}`} dur={4.6} />

      {PHASES.map((p, i) => (
        <g key={p}>
          <circle cx={nodeX(i)} cy={y} r="15" fill={INK.field0} stroke={INK.teal} strokeWidth="2.5" />
          <circle cx={nodeX(i)} cy={y} r="5" fill={INK.teal} />
          <ArtText x={nodeX(i)} y={y - 34} anchor="middle" size={16}>
            {p}
          </ArtText>
        </g>
      ))}

      {/* Counters that never invent success */}
      <ArtText x={96} y={y + 44} size={14} tone="muted" mono>
        12,480 src
      </ArtText>
      <ArtText x={nodeX(2)} y={y + 44} anchor="middle" size={14} tone="muted" mono>
        12,471 written
      </ArtText>
      <ArtText x={nodeX(3)} y={y + 44} anchor="middle" size={14} tone="teal" mono>
        reread dest
      </ArtText>

      {/* Quarantine branches down — nothing disappears */}
      <path
        d={`M${nodeX(2)} ${y + 15} C${nodeX(2)} 340 ${nodeX(2) - 120} 330 ${nodeX(2) - 120} 396`}
        fill="none"
        stroke={INK.amber}
        strokeWidth="2.5"
        strokeDasharray="8 7"
      />
      <ArtPlate x={nodeX(2) - 300} y={396} w={360} h={148} tone="amber" />
      <ArtLabel x={nodeX(2) - 280} y={428} tone="amber">
        quarantine · 9 rows
      </ArtLabel>
      <ArtField x={nodeX(2) - 280} y={444} w={320} name="pay_amt" type="currency symbol" tone="amber" />
      <ArtText x={nodeX(2) - 280} y={526} size={14} tone="muted">
        column · value · reason — replayable
      </ArtText>

      <ArtSeal cx={nodeX(4) + 76} cy={y} r={40} label="counts + checksum" />
    </HeroArtFrame>
  );
}

/* ── Pipelines · the cadence dial ───────────────────────────────── */

const TICKS = 12;
const SCHEDULES = [
  { name: "Orders hourly", cadence: "Every hour", mode: "watermark", tone: "plate" as const },
  { name: "Customers daily", cadence: "Daily 02:00 UTC", mode: "upsert", tone: "plate" as const },
  { name: "Events → Snowflake", cadence: "Every 15 min", mode: "append · drift", tone: "amber" as const },
];

export function CadenceDialArt() {
  const cx = 268;
  const cy = 320;
  const r = 168;

  return (
    <HeroArtFrame
      label="Cadence dial driving recurring gated runs"
      caption="Recurrence is a rhythm — every tick becomes a gated job, drift is surfaced."
      focus={{ x: 56, y: 116, w: 560, h: 420 }}
    >
      <ArtLabel x={64} y={112}>
        cadence
      </ArtLabel>

      <circle cx={cx} cy={cy} r={r} fill="none" stroke={INK.line} strokeWidth="1.5" />
      <circle cx={cx} cy={cy} r={r - 26} fill="none" stroke={INK.grid} strokeDasharray="2 10" />
      {Array.from({ length: TICKS }, (_, i) => {
        const a = (i / TICKS) * Math.PI * 2 - Math.PI / 2;
        const drift = i === 4;
        const inner = r - (i % 3 === 0 ? 26 : 14);
        return (
          <line
            key={i}
            x1={cx + Math.cos(a) * inner}
            y1={cy + Math.sin(a) * inner}
            x2={cx + Math.cos(a) * r}
            y2={cy + Math.sin(a) * r}
            stroke={drift ? INK.amber : INK.teal}
            strokeOpacity={drift ? 1 : 0.55}
            strokeWidth={drift ? 3 : 2}
          />
        );
      })}
      <circle cx={cx} cy={cy} r="60" fill={INK.plate} stroke={INK.tealDeep} strokeWidth="1.5" />
      <ArtText x={cx} y={cy - 4} anchor="middle" size={17}>
        every tick
      </ArtText>
      <ArtText x={cx} y={cy + 22} anchor="middle" size={14} tone="teal">
        runs G1–G9
      </ArtText>
      <g className="dw-hero-art-sweep" style={{ transformOrigin: `${cx}px ${cy}px` }}>
        <line x1={cx} y1={cy} x2={cx} y2={cy - r + 8} stroke={INK.teal} strokeWidth="2.5" strokeLinecap="round" />
      </g>

      {/* Each tick lands as a real job on the right */}
      {SCHEDULES.map((s, i) => {
        const y = 148 + i * 132;
        return (
          <g key={s.name}>
            <path
              d={`M${cx + r + 8} ${cy} C560 ${cy} 540 ${y + 46} 600 ${y + 46}`}
              fill="none"
              stroke={s.tone === "amber" ? INK.amber : INK.teal}
              strokeOpacity="0.5"
              strokeWidth="2"
              strokeDasharray="7 8"
            />
            <ArtPlate x={600} y={y} w={362} h={96} tone={s.tone} />
            <ArtText x={624} y={y + 38} size={18}>
              {s.name}
            </ArtText>
            <ArtText x={624} y={y + 68} size={15} tone="muted">
              {s.cadence}
            </ArtText>
            <ArtText x={938} y={y + 68} size={14} anchor="end" tone={s.tone === "amber" ? "amber" : "teal"} mono>
              {s.mode}
            </ArtText>
          </g>
        );
      })}
    </HeroArtFrame>
  );
}

/* ── Datawrap Pilot · sentence becomes a governed act ───────────── */

const TOKENS = [
  { x: 96, w: 214, label: "route" },
  { x: 326, w: 132, label: "table" },
  { x: 474, w: 196, label: "cadence" },
  { x: 686, w: 150, label: "timezone" },
];

export function IntentArt() {
  return (
    <HeroArtFrame
      label="Natural language resolved into a staged, confirmable action"
      caption="Language is parsed into an engine-shaped plan that stays locked until Confirm."
      focus={{ x: 56, y: 256, w: 640, h: 250 }}
    >
      <ArtLabel x={64} y={92}>
        what the operator typed
      </ArtLabel>
      <ArtPlate x={64} y={112} w={898} h={70} tone="sunken" />
      <ArtText x={92} y={156} size={20} mono weight={500}>
        sync orders to snowflake nightly at 01:30 IST
      </ArtText>

      {/* Tokens the parser actually resolves */}
      {TOKENS.map((t) => (
        <g key={t.label}>
          <ArtFilament x1={t.x} x2={t.x + t.w} y={172} tone="teal" width={2.5} />
          <path
            d={`M${t.x + t.w / 2} 176 V212`}
            stroke={INK.line}
            strokeWidth="1.5"
            strokeDasharray="4 6"
            fill="none"
          />
          <ArtText x={t.x + t.w / 2} y={234} anchor="middle" size={14} tone="teal">
            {t.label}
          </ArtText>
        </g>
      ))}

      <ArtPlate x={64} y={264} w={620} h={228} />
      <ArtLabel x={92} y={300} tone="teal">
        resolved plan
      </ArtLabel>
      <ArtField x={92} y={314} w={564} name="pg.public.orders → sf.ANALYTICS.ORDERS" type="upsert" />
      <ArtField x={92} y={368} w={564} name="cron 30 1 * * *" type="tz refused: IST" tone="amber" />
      <ArtField x={92} y={422} w={564} name="preflight G1–G9" type="approve-grade" tone="teal" />

      {/* The lock: a mutation never runs on the model's word alone */}
      <ArtPlate x={716} y={264} w={246} h={228} tone="teal" />
      <rect x={812} y={306} width="56" height="44" rx="10" fill={INK.field0} stroke={INK.teal} strokeWidth="2.5" />
      <path
        d="M824 306v-16a16 16 0 0 1 32 0v16"
        fill="none"
        stroke={INK.teal}
        strokeWidth="2.5"
        strokeLinecap="round"
      />
      <circle cx={840} cy={328} r="5" fill={INK.teal} />
      <ArtText x={839} y={392} anchor="middle" size={17}>
        awaits Confirm
      </ArtText>
      <ArtText x={839} y={420} anchor="middle" size={14} tone="muted" mono>
        schedule.manage
      </ArtText>
      <ArtText x={839} y={452} anchor="middle" size={14} tone="amber">
        ambiguous tz refused
      </ArtText>

      <ArtText x={64} y={532} size={15} tone="muted">
        Unresolved detail is asked for, never assumed.
      </ArtText>
      <ArtText x={64} y={562} size={15} tone="muted">
        No mutation executes before the acknowledgement.
      </ArtText>
    </HeroArtFrame>
  );
}

/* ── MCP · one door for humans and agents ───────────────────────── */

export function PolicyDoorArt() {
  const wallX = 436;
  return (
    <HeroArtFrame
      label="Human and agent callers entering through one policy wall"
      caption="Agents get the same gate as people — credentials never cross the wall."
      focus={{ x: 48, y: 60, w: 560, h: 560 }}
    >
      <ArtLabel x={64} y={104}>
        callers
      </ArtLabel>

      <ArtPlate x={64} y={132} w={274} h={112} />
      <ArtText x={92} y={176} size={19}>
        Human operator
      </ArtText>
      <ArtText x={92} y={206} size={14} tone="muted" mono>
        role: editor
      </ArtText>

      <ArtPlate x={64} y={380} w={274} h={112} />
      <ArtText x={92} y={424} size={19}>
        Agent · MCP client
      </ArtText>
      <ArtText x={92} y={454} size={14} tone="muted" mono>
        role: bound at call
      </ArtText>

      {/* Both converge on the single aperture */}
      <path d={`M338 188 C398 188 392 300 ${wallX} 300`} fill="none" stroke={INK.teal} strokeWidth="2.5" />
      <path d={`M338 436 C398 436 392 340 ${wallX} 340`} fill="none" stroke={INK.teal} strokeWidth="2.5" />
      <ArtPacket path={`M338 188 C398 188 392 300 ${wallX} 300`} dur={4} />
      <ArtPacket path={`M338 436 C398 436 392 340 ${wallX} 340`} dur={4} delay={2} />

      {/* The wall */}
      <rect x={wallX} y={72} width="66" height="496" rx="10" fill={INK.plate} stroke={INK.plateEdge} strokeWidth="1.5" />
      {Array.from({ length: 15 }, (_, i) => (
        <line
          key={i}
          x1={wallX + 4}
          y1={86 + i * 33}
          x2={wallX + 62}
          y2={86 + i * 33}
          stroke={INK.grid}
          strokeWidth="1"
        />
      ))}
      <rect x={wallX - 2} y={288} width="70" height="64" rx="8" fill="rgba(45, 212, 191, 0.2)" stroke={INK.teal} strokeWidth="2" />
      <ArtText x={wallX + 33} y={608} anchor="middle" size={15} tone="teal">
        one permission gate
      </ArtText>

      {/* Secrets stop dead at the wall face */}
      <g>
        <circle cx={404} cy={252} r="11" fill="none" stroke={INK.danger} strokeWidth="2.5" />
        <path d="M414 252h26" stroke={INK.danger} strokeWidth="2.5" strokeLinecap="round" />
        <path d="M430 252v9" stroke={INK.danger} strokeWidth="2.5" strokeLinecap="round" />
        <ArtText x={444} y={300} anchor="end" size={14} tone="danger">
          credentials stop here
        </ArtText>
      </g>

      <ArtPlate x={578} y={168} w={384} h={304} tone="teal" />
      <ArtLabel x={606} y={210} tone="teal">
        governed engine
      </ArtLabel>
      <ArtField x={606} y={226} w={328} name="tool → required permission" type="registry" />
      <ArtField x={606} y={286} w={328} name="mutations staged" type="confirm" />
      <ArtField x={606} y={346} w={328} name="audit actor = session" type="not payload" />
      <ArtText x={606} y={426} size={14} tone="muted">
        Unknown tool → admin-only,
      </ArtText>
      <ArtText x={606} y={452} size={14} tone="muted">
        fail closed.
      </ArtText>
    </HeroArtFrame>
  );
}

/* ── Query · the read-only lens ─────────────────────────────────── */

const CELL_TYPES = ["int8", "numeric", "text", "timestamptz"];

export function ReadLensArt() {
  return (
    <HeroArtFrame
      label="Read-only lens over typed rows, deflecting write statements"
      caption="Inspect production types without a write path — SELECT only, by construction."
      focus={{ x: 408, y: 132, w: 570, h: 450 }}
    >
      <ArtLabel x={64} y={100}>
        live rows · typed
      </ArtLabel>

      {/* Typed cell field */}
      {CELL_TYPES.map((t, c) => (
        <g key={t}>
          <ArtText x={90 + c * 150} y={140} size={13} tone="muted" mono>
            {t}
          </ArtText>
          {Array.from({ length: 6 }, (_, r) => (
            <rect
              key={r}
              x={90 + c * 150}
              y={158 + r * 62}
              width="126"
              height="42"
              rx="8"
              fill={INK.field0}
              stroke={INK.grid}
              strokeWidth="1"
            />
          ))}
        </g>
      ))}

      {/* The lens */}
      <circle cx={604} cy={330} r="176" fill="rgba(45, 212, 191, 0.07)" stroke={INK.teal} strokeWidth="2.5" />
      <circle cx={604} cy={330} r="188" fill="none" stroke={INK.teal} strokeOpacity="0.28" strokeDasharray="3 9" />
      <ArtText x={604} y={230} anchor="middle" size={15} tone="teal">
        read-only lens
      </ArtText>
      <ArtField x={498} y={262} w={212} name="order_id" type="int8" tone="teal" />
      <ArtField x={498} y={318} w={212} name="129.00" type="numeric(10,2)" tone="teal" />
      <ArtField x={498} y={374} w={212} name="2026-08-17T02:14Z" type="timestamptz" tone="teal" />
      <ArtText x={604} y={452} anchor="middle" size={14} tone="muted" mono>
        200 rows · 48 ms
      </ArtText>

      {/* Writes deflect off the rim */}
      {[
        { y: 214, text: "INSERT" },
        { y: 330, text: "UPDATE" },
        { y: 446, text: "DROP" },
      ].map((w, i) => (
        <g key={w.text}>
          <path
            d={`M958 ${w.y} H${800 - i * 6}`}
            stroke={INK.danger}
            strokeWidth="2.5"
            strokeDasharray="9 7"
            strokeLinecap="round"
          />
          <path
            d={`M${800 - i * 6} ${w.y} l-26 ${w.y > 330 ? 30 : w.y < 330 ? -30 : 0} ${w.y === 330 ? "" : ""}`}
            stroke={INK.danger}
            strokeWidth="2"
            fill="none"
            strokeLinecap="round"
          />
          <ArtText x={958} y={w.y - 16} anchor="end" size={14} tone="danger" mono>
            {w.text}
          </ArtText>
        </g>
      ))}
      <ArtText x={958} y={556} anchor="end" size={15} tone="danger">
        refused — no write path
      </ArtText>
    </HeroArtFrame>
  );
}
