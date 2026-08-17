/**
 * Home + solution hero drawings. Each argues the outcome of that page —
 * see docs/MARKETING_HERO_DESIGN_SYSTEM.md.
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

/* ── Home · the proof loop ──────────────────────────────────────── */

export function ProofLoopArt() {
  return (
    <HeroArtFrame
      label="A transfer that closes the loop by rereading the destination"
      caption="The write is not the proof — the independent destination reread is."
      focus={{ x: 366, y: 96, w: 600, h: 400 }}
    >
      <ArtPlate x={56} y={128} w={236} h={172} />
      <ArtLabel x={80} y={166}>
        source
      </ArtLabel>
      <ArtText x={80} y={198} size={15}>
        PostgreSQL · orders
      </ArtText>
      <ArtField x={80} y={212} w={188} name="order_amt" type="numeric" />
      <ArtField x={80} y={258} w={188} name="cust_id" type="int8" />

      <ArtPlate x={382} y={104} w={236} h={220} tone="teal" />
      <ArtLabel x={406} y={142} tone="teal">
        governed engine
      </ArtLabel>
      <ArtField x={406} y={158} w={188} name="map · confidence" type="0.92" />
      <ArtField x={406} y={210} w={188} name="preflight" type="G1–G9" />
      <ArtField x={406} y={262} w={188} name="quarantine" type="9 rows" tone="amber" />

      <ArtPlate x={708} y={128} w={236} h={172} />
      <ArtLabel x={732} y={166} anchor="start">
        destination
      </ArtLabel>
      <ArtText x={732} y={198} size={15}>
        Snowflake · ORDERS
      </ArtText>
      <ArtField x={732} y={212} w={188} name="total_amount" type="NUMBER" />
      <ArtField x={732} y={258} w={188} name="customer_key" type="NUMBER" />

      <ArtFilament x1={292} x2={382} y={214} tone="teal" width={2.5} />
      <ArtFilament x1={618} x2={708} y={214} tone="teal" width={2.5} />
      <ArtPacket path="M292 214 H382" dur={2.6} />
      <ArtPacket path="M618 214 H708" dur={2.6} delay={1.3} />

      {/* The return path — what makes the verdict destination-authoritative */}
      <path
        d="M826 300 C826 412 700 452 556 452 H392"
        fill="none"
        stroke={INK.teal}
        strokeWidth="2.5"
        strokeDasharray="10 8"
      />
      <ArtPacket path="M826 300 C826 412 700 452 556 452 H392" dur={5.2} />
      <ArtLabel x={806} y={362} anchor="end" tone="teal">
        destination reread
      </ArtLabel>

      <ArtSeal cx={296} cy={452} r={40} label="verdict: MATCH" />

      <ArtField x={420} y={510} w={168} name="counts" type="=" tone="teal" />
      <ArtField x={604} y={510} w={168} name="fingerprint" type="=" tone="teal" />
      <ArtField x={788} y={510} w={156} name="keys · FK" type="=" tone="teal" />
      <ArtText x={296} y={598} anchor="middle" size={14} tone="muted">
        a writer acknowledgement is not proof
      </ArtText>
    </HeroArtFrame>
  );
}

/* ── Migrations · the cutover runway ────────────────────────────── */

const SRC_FIELDS = [
  { name: "order_amt", type: "DECIMAL(10,4)" },
  { name: "pay_amt", type: "DECIMAL(10,4)" },
  { name: "cust_id", type: "VARCHAR(36)" },
  { name: "created", type: "TIMESTAMP_NTZ" },
  { name: "notes", type: "CLOB" },
];

const DST_FIELDS = [
  { name: "total_amount", type: "numeric(12,4)" },
  { name: "payment_amount", type: "numeric(12,4)" },
  { name: "customer_key", type: "bigint" },
  { name: "created_at", type: "timestamptz" },
];

const EDGES: { from: number; to: number; score: string; review?: boolean }[] = [
  { from: 0, to: 0, score: "0.92" },
  { from: 1, to: 1, score: "0.99" },
  { from: 2, to: 2, score: "rev", review: true },
  { from: 3, to: 3, score: "0.96" },
];

export function CutoverArt() {
  const srcY = (i: number) => 128 + i * 60;
  const dstY = (i: number) => 140 + i * 76;

  return (
    <HeroArtFrame
      label="Two schemas that were never one-to-one, aligned for a proven cutover"
      caption="Shapes differ, so every edge earns a score — and one waits for a human."
      focus={{ x: 48, y: 96, w: 640, h: 400 }}
    >
      <ArtLabel x={64} y={104}>
        legacy schema
      </ArtLabel>
      <ArtLabel x={962} y={104} anchor="end" tone="teal">
        target schema
      </ArtLabel>

      {SRC_FIELDS.map((f, i) => (
        <ArtField key={f.name} x={64} y={srcY(i)} w={252} name={f.name} type={f.type} />
      ))}
      {DST_FIELDS.map((f, i) => (
        <ArtField key={f.name} x={676} y={dstY(i)} w={286} name={f.name} type={f.type} tone="teal" />
      ))}

      {EDGES.map((e) => {
        const y1 = srcY(e.from) + 21;
        const y2 = dstY(e.to) + 21;
        return (
          <g key={`${e.from}-${e.to}`}>
            <path
              d={`M316 ${y1} C460 ${y1} 530 ${y2} 676 ${y2}`}
              fill="none"
              stroke={e.review ? INK.amber : INK.teal}
              strokeWidth="2.5"
              strokeOpacity={e.review ? 1 : 0.75}
              strokeDasharray={e.review ? "8 7" : undefined}
            />
            <circle
              cx={496}
              cy={(y1 + y2) / 2}
              r="18"
              fill={INK.field0}
              stroke={e.review ? INK.amber : INK.teal}
              strokeWidth="2"
            />
            <ArtText
              x={496}
              y={(y1 + y2) / 2 + 5}
              anchor="middle"
              size={13}
              tone={e.review ? "amber" : "teal"}
              mono
            >
              {e.score}
            </ArtText>
          </g>
        );
      })}

      {/* notes → no target: stated, never silently dropped */}
      <path d="M316 470 H420" stroke={INK.danger} strokeWidth="2.5" strokeDasharray="7 7" />
      <ArtText x={436} y={476} size={14} tone="danger">
        no target column — declared, not dropped
      </ArtText>

      {/* The runway: four checkpoints, then a signed cutover */}
      <ArtFilament x1={64} x2={840} y={562} tone="teal" width={2.5} />
      {["Discover", "Review", "Gates", "Cutover"].map((c, i) => {
        const x = 96 + i * 240;
        return (
          <g key={c}>
            <circle cx={x} cy={562} r="10" fill={INK.field0} stroke={INK.teal} strokeWidth="2.5" />
            <ArtText x={x} y={538} anchor="middle" size={15}>
              {c}
            </ArtText>
          </g>
        );
      })}
      <ArtSeal cx={906} cy={562} r={26} />
    </HeroArtFrame>
  );
}

/* ── Warehouse loading · typed delivery into layers ─────────────── */

const WH_COLUMNS = [
  { x: 332, name: "amount", type: "numeric(12,2)" },
  { x: 476, name: "loaded_at", type: "timestamptz" },
  { x: 620, name: "order_id", type: "int8" },
];

export function WarehouseLayersArt() {
  const slab = (i: number) => {
    const x = 300 - i * 34;
    const y = 372 + i * 74;
    return `${x},${y} ${x + 470},${y} ${x + 436},${y + 58} ${x - 34},${y + 58}`;
  };

  return (
    <HeroArtFrame
      label="Typed columns landing in warehouse layers without becoming strings"
      caption="Types survive the load — a numeric source never lands as text."
      focus={{ x: 44, y: 100, w: 640, h: 470 }}
    >
      <ArtLabel x={64} y={104}>
        typed source columns
      </ArtLabel>

      {WH_COLUMNS.map((c, i) => {
        const fieldY = 124 + i * 58;
        const route = `M300 ${fieldY + 22} H${c.x + 22} V${356 - i * 4}`;
        return (
          <g key={c.name}>
            <ArtField x={64} y={fieldY} w={236} name={c.name} type={c.type} />
            <path d={route} stroke={INK.teal} strokeWidth="2" strokeDasharray="7 8" fill="none" />
            <ArtPacket path={route} dur={3.4} delay={i * 0.8} />
          </g>
        );
      })}

      {[2, 1, 0].map((i) => (
        <polygon
          key={i}
          points={slab(i)}
          fill={i === 0 ? "rgba(13, 148, 136, 0.22)" : INK.plate}
          stroke={i === 0 ? INK.teal : INK.plateEdge}
          strokeWidth="1.5"
        />
      ))}
      <ArtText x={330} y={408} size={17}>
        MERGE · native loader
      </ArtText>
      <ArtText x={296} y={482} size={16} tone="muted">
        staged bulk window
      </ArtText>
      <ArtText x={262} y={556} size={16} tone="muted">
        reconcile report
      </ArtText>

      <ArtPlate x={760} y={148} w={202} h={158} />
      <ArtLabel x={784} y={186}>
        capacity
      </ArtLabel>
      <rect x={784} y={206} width="154" height="12" rx="6" fill={INK.field0} stroke={INK.line} />
      <rect x={784} y={206} width="104" height="12" rx="6" fill={INK.teal} />
      <ArtText x={784} y={252} size={15} tone="muted">
        slots probed
      </ArtText>
      <ArtText x={784} y={280} size={15} tone="teal">
        before the window
      </ArtText>

      <ArtSeal cx={861} cy={430} r={38} label="checksum archived" />
    </HeroArtFrame>
  );
}

/* ── Sync / CDC · the watermark ─────────────────────────────────── */

const WATERMARK_FRESH = 4;
const WATERMARK_GAP = 72;

export function WatermarkArt() {
  const rows = Array.from({ length: 7 }, (_, i) =>
    124 + i * 52 + (i >= WATERMARK_FRESH ? WATERMARK_GAP : 0),
  );
  const line = rows[WATERMARK_FRESH - 1] + 42 + WATERMARK_GAP / 2;

  return (
    <HeroArtFrame
      label="A watermark separating new rows from rows already delivered"
      caption="Only rows above the watermark move; drift stops the run instead of guessing."
      focus={{ x: 44, y: 108, w: 600, h: 460 }}
    >
      <ArtLabel x={64} y={104}>
        source table
      </ArtLabel>

      {rows.map((y, i) => {
        const fresh = i < WATERMARK_FRESH;
        return (
          <g key={y}>
            <rect
              x={64}
              y={y}
              width={352}
              height={42}
              rx="9"
              fill={INK.field0}
              stroke={fresh ? INK.tealDeep : INK.grid}
              strokeWidth="1.5"
            />
            <ArtText x={84} y={y + 27} size={15} mono tone={fresh ? "strong" : "muted"} weight={500}>
              {`ord_${1840 + i}`}
            </ArtText>
            <ArtText x={396} y={y + 27} size={13} anchor="end" tone="muted" mono weight={500}>
              {fresh ? "02:1" + (4 + i) + "Z" : "01:0" + i + "Z"}
            </ArtText>
            {fresh ? (
              <>
                <ArtFilament x1={424} x2={628} y={y + 21} tone="teal" width={2} />
                <ArtPacket path={`M424 ${y + 21} H628`} dur={3} delay={i * 0.5} />
              </>
            ) : null}
          </g>
        );
      })}

      <ArtFilament x1={48} x2={604} y={line} tone="amber" width={2.5} dashed />
      <ArtLabel x={48} y={line - 14} tone="amber">
        watermark · updated_at &gt; 02:14Z
      </ArtLabel>
      <ArtText x={64} y={606} size={15} tone="muted">
        rows below the watermark are already delivered
      </ArtText>

      <ArtPlate x={628} y={132} w={334} h={214} tone="teal" />
      <ArtLabel x={654} y={172} tone="teal">
        destination
      </ArtLabel>
      <ArtField x={654} y={188} w={282} name="upsert by key" type="at-least-once" />
      <ArtField x={654} y={244} w={282} name="4 new · 0 rewritten" type="reconciled" tone="teal" />
      <ArtText x={654} y={306} size={13} tone="muted">
        exactly-once only where
      </ArtText>
      <ArtText x={654} y={332} size={13} tone="muted">
        proven for that route
      </ArtText>

      <ArtPlate x={628} y={396} w={334} h={150} tone="amber" />
      <ArtLabel x={654} y={436} tone="amber">
        schema drift
      </ArtLabel>
      <ArtText x={654} y={472} size={16}>
        new column detected
      </ArtText>
      <ArtText x={654} y={502} size={14} tone="amber">
        run stops — no widening
      </ArtText>
    </HeroArtFrame>
  );
}
