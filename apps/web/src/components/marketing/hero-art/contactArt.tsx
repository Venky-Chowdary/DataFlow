/**
 * Contact — "the pilot route".
 *
 * The page's claim is not "get in touch": it is that a request becomes a scoped
 * pilot on the caller's own stack, on the production engine, and ends in an
 * artifact they keep. So the drawing is a route with three checkpoints and one
 * refused branch (the nurture queue we do not run), not a decorated envelope.
 *
 * It runs the full width of the hero band under the copy and the form, which is
 * why it uses the wide band canvas rather than the column canvas.
 */

import {
  ArtFilament,
  ArtLabel,
  ArtPacket,
  ArtPlate,
  ArtSeal,
  ArtText,
  BAND_H,
  BAND_W,
  HeroArtFrame,
  INK,
} from "./HeroArtFrame";

const RUNWAY_Y = 250;

const CHECKPOINTS: {
  n: string;
  head: string;
  strong: string;
  detail: [string, string];
  x: number;
}[] = [
  {
    n: "01",
    head: "discovery",
    strong: "30 minutes",
    detail: ["source → dest, sync mode,", "compliance constraints"],
    x: 470,
  },
  {
    n: "02",
    head: "scoped pilot",
    strong: "on your stack",
    detail: ["semantic map · G1–G9", "quarantine, not silent drop"],
    x: 790,
  },
  {
    n: "03",
    head: "reconcile",
    strong: "dest-engine reread",
    detail: ["COUNT + checksum MATCH", "refused if it does not agree"],
    x: 1110,
  },
];

const CP_W = 270;
const CP_TOP = 150;
const CP_H = 200;

export function PilotRouteArt() {
  return (
    <HeroArtFrame
      canvas={{ w: BAND_W, h: BAND_H }}
      label="The route from a pilot request to a reconcile artifact: a thirty-minute discovery call, a scoped pilot on your own stack through semantic mapping, gates G1 to G9 and quarantine, then a destination-engine reread that produces a COUNT and checksum artifact you keep — and a refused nurture-queue branch, because a solutions engineer replies within one business day."
      caption="Schematic of the engagement, not a sales funnel. The pilot runs the production engine; the artifact at the end is yours."
    >
      <ArtLabel x={44} y={92} tone="teal">
        one business day
      </ArtLabel>

      {/* The request, in the caller's own terms. */}
      <ArtPlate x={44} y={CP_TOP} w={330} h={CP_H} />
      <ArtLabel x={72} y={190}>
        your request
      </ArtLabel>
      <ArtText x={72} y={228} size={16}>
        PostgreSQL → Snowflake
      </ArtText>
      <ArtText x={72} y={258} size={13} tone="muted">
        3 tables · Sat 02:00 window
      </ArtText>
      <ArtText x={72} y={286} size={13} tone="muted">
        PII columns declared
      </ArtText>
      <ArtText x={72} y={318} size={12.5} tone="teal" mono>
        sales@datawrap.io
      </ArtText>

      {/* The branch we refuse: a nurture queue that never reaches an engineer. */}
      <path
        d={`M420 ${RUNWAY_Y} V112`}
        fill="none"
        stroke={INK.danger}
        strokeOpacity="0.55"
        strokeWidth="2"
        strokeDasharray="6 8"
      />
      <g stroke={INK.danger} strokeWidth="2.5" strokeLinecap="round">
        <line x1={412} y1={96} x2={428} y2={112} />
        <line x1={428} y1={96} x2={412} y2={112} />
      </g>
      <ArtText x={446} y={98} size={13.5} tone="danger">
        no nurture queue
      </ArtText>
      <ArtText x={446} y={122} size={12.5} tone="muted">
        a person answers, not a sequence
      </ArtText>

      {/* The route itself — drawn between the plates, never across their labels. */}
      {[
        [374, CHECKPOINTS[0].x],
        [CHECKPOINTS[0].x + CP_W, CHECKPOINTS[1].x],
        [CHECKPOINTS[1].x + CP_W, CHECKPOINTS[2].x],
        [CHECKPOINTS[2].x + CP_W, 1440],
      ].map(([x1, x2]) => (
        <ArtFilament key={x1} x1={x1} x2={x2} y={RUNWAY_Y} tone="teal" width={3} />
      ))}
      <ArtPacket path={`M374 ${RUNWAY_Y} H${CHECKPOINTS[0].x}`} dur={2.6} />

      {CHECKPOINTS.map((cp) => (
        <g key={cp.n}>
          <ArtPlate x={cp.x} y={CP_TOP} w={CP_W} h={CP_H} tone={cp.n === "03" ? "teal" : "plate"} />
          <ArtText x={cp.x + 24} y={190} size={13} tone="teal" mono>
            {cp.n}
          </ArtText>
          <ArtLabel x={cp.x + 62} y={190}>
            {cp.head}
          </ArtLabel>
          <ArtText x={cp.x + 24} y={232} size={16}>
            {cp.strong}
          </ArtText>
          <ArtText x={cp.x + 24} y={268} size={12.5} tone="muted">
            {cp.detail[0]}
          </ArtText>
          <ArtText x={cp.x + 24} y={292} size={12.5} tone="muted">
            {cp.detail[1]}
          </ArtText>
        </g>
      ))}

      {/* What the caller leaves with. */}
      <ArtPlate x={1440} y={CP_TOP + 12} w={220} h={CP_H - 24} tone="sunken" />
      <ArtSeal cx={1550} cy={224} r={30} />
      <ArtText x={1550} y={300} size={13.5} anchor="middle" tone="teal">
        artifact you keep
      </ArtText>

      <ArtText x={470} y={396} size={13.5} tone="muted">
        the pilot runs the production engine — skip_preflight is never set from this form
      </ArtText>
    </HeroArtFrame>
  );
}
