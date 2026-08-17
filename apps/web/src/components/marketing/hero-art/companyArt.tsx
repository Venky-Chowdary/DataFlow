/**
 * Company-surface hero drawings (pricing, customers) —
 * see docs/MARKETING_HERO_DESIGN_SYSTEM.md.
 */

import { EVIDENCE_AS_OF } from "../../../lib/provenEvidence";
import {
  ArtFilament,
  ArtLabel,
  ArtPacket,
  ArtPlate,
  ArtSeal,
  ArtText,
  HeroArtFrame,
  INK,
} from "./HeroArtFrame";

/* ── Pricing · the measured plan ladder ─────────────────────────── */

const RAIL_Y = 512;

const PLAN_STEPS: {
  name: string;
  price: string;
  period: string;
  adds: string[];
  x: number;
  top: number;
  tone: "plate" | "teal";
}[] = [
  {
    name: "Starter",
    price: "Free",
    period: "forever",
    adds: ["Transfer Studio", "quarantine + checksum"],
    x: 64,
    top: 330,
    tone: "plate",
  },
  {
    name: "Team",
    price: "Custom",
    period: "usage-aligned",
    adds: ["Pipelines + Pilot", "shared connectors"],
    x: 366,
    top: 250,
    tone: "teal",
  },
  {
    name: "Enterprise",
    price: "Custom",
    period: "security & scale",
    adds: ["SSO / SAML · BYOK", "MCP under policy"],
    x: 668,
    top: 170,
    tone: "plate",
  },
];

const STEP_W = 268;

/**
 * The ladder rises on security and cadence, and every step stands on the same
 * rail — the argument that the proof engine is not an upgrade.
 */
export function PlanLadderArt() {
  return (
    <HeroArtFrame
      label="Three plan steps rising from one shared proof rail"
      caption="The steps differ by cadence and security. The rail under all three is the same engine."
      focus={{ x: 352, y: 200, w: 620, h: 420 }}
    >
      <ArtLabel x={64} y={104} tone="teal">
        priced by cadence and security
      </ArtLabel>

      <ArtText x={64} y={144} size={18}>
        The ladder climbs on cadence
      </ArtText>
      <ArtText x={64} y={176} size={18}>
        and security.
      </ArtText>
      {/* The meter we do not run: rows moved never sets the invoice. */}
      <g>
        <ArtText x={64} y={228} size={15} tone="danger">
          monthly active rows
        </ArtText>
        <line x1={58} y1={222} x2={266} y2={222} stroke={INK.danger} strokeWidth="2" />
      </g>
      <ArtText x={64} y={258} size={15} tone="muted">
        never sets the invoice
      </ArtText>

      {PLAN_STEPS.map((step, i) => {
        const h = RAIL_Y - step.top;
        return (
          <g key={step.name}>
            <ArtPlate x={step.x} y={step.top} w={STEP_W} h={h} tone={step.tone} radius={12} />
            <ArtText x={step.x + 24} y={step.top + 44} size={21}>
              {step.name}
            </ArtText>
            <ArtText x={step.x + 24} y={step.top + 76} size={17} tone="teal">
              {step.price}
            </ArtText>
            <ArtText x={step.x + 24} y={step.top + 102} size={12} tone="muted" mono>
              {step.period}
            </ArtText>
            {step.adds.map((add, ai) => (
              <ArtText key={add} x={step.x + 24} y={step.top + 140 + ai * 28} size={14} tone="muted">
                {add}
              </ArtText>
            ))}
            {i > 0 ? (
              <ArtFilament x1={step.x - 34} x2={step.x} y={step.top + 22} tone="teal" width={2} dashed />
            ) : null}
          </g>
        );
      })}

      {/* One rail under every step: the proof engine is not an upgrade tier. */}
      <ArtFilament x1={48} x2={952} y={RAIL_Y} tone="teal" width={3} />
      <ArtPacket path={`M48 ${RAIL_Y} H952`} dur={5.4} />
      <ArtLabel x={48} y={RAIL_Y + 40} tone="teal">
        in every plan
      </ArtLabel>
      <ArtText x={48} y={RAIL_Y + 78} size={15} tone="muted">
        semantic map · G1–G9 · quarantine · destination reread
      </ArtText>
    </HeroArtFrame>
  );
}

/* ── Customers · named evidence plates, not a logo wall ─────────── */

const EVIDENCE_PLATES: { stat: string; claim: string; scope: string }[] = [
  { stat: "48 cases", claim: "schema drift held", scope: "widen · refuse narrow · defaults" },
  { stat: "43 cases", claim: "identity and keys", scope: "carried or explicitly refused" },
  { stat: "14 cases", claim: "retry cannot corrupt", scope: "committed rows never replayed blind" },
];

export function EvidencePlatesArt() {
  return (
    <HeroArtFrame
      label="Measured evidence plates with an empty, refused logo plate"
      caption="Each plate names its case count and engines. The empty plate is the one we will not fill."
      focus={{ x: 40, y: 96, w: 560, h: 440 }}
    >
      {EVIDENCE_PLATES.map((plate, i) => {
        const y = 108 + i * 128;
        return (
          <g key={plate.claim}>
            <ArtPlate x={64} y={y} w={472} h={108} />
            <ArtText x={92} y={y + 40} size={18} tone="teal" mono>
              {plate.stat}
            </ArtText>
            <ArtText x={92} y={y + 70} size={16}>
              {plate.claim}
            </ArtText>
            <ArtText x={92} y={y + 94} size={12} tone="muted">
              {plate.scope}
            </ArtText>
            <ArtFilament x1={536} x2={628} y={y + 54} tone="teal" width={2} dashed />
          </g>
        );
      })}

      <ArtLabel x={64} y={92}>
        recorded on live engines
      </ArtLabel>
      <ArtText x={64} y={534} size={14} tone="muted">
        PostgreSQL · MySQL · SQL Server · Oracle
      </ArtText>

      {/* The plate we refuse to fill — the page's actual claim. */}
      <rect
        x={664}
        y={108}
        width={272}
        height={148}
        rx="12"
        fill="none"
        stroke={INK.line}
        strokeWidth="1.5"
        strokeDasharray="8 9"
      />
      <ArtText x={800} y={172} anchor="middle" size={16} tone="muted">
        logo wall
      </ArtText>
      <line x1={700} y1={196} x2={900} y2={196} stroke={INK.danger} strokeWidth="2.5" />
      <ArtText x={800} y={230} anchor="middle" size={13} tone="danger">
        no invented marks
      </ArtText>

      <ArtSeal cx={800} cy={378} r={44} label={`measured ${EVIDENCE_AS_OF}`} />
      <ArtText x={800} y={496} anchor="middle" size={14} tone="muted">
        destination reread, not writer counts
      </ArtText>
    </HeroArtFrame>
  );
}
