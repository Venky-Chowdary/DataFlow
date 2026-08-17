/**
 * Shared canvas for every marketing hero drawing.
 *
 * One viewBox (1000×640) so a hero scales fluidly without breakpoints, one grid,
 * one light direction, one type scale. Art modules contribute geometry only —
 * never their own background, palette, or font sizes. See
 * docs/MARKETING_HERO_DESIGN_SYSTEM.md for the per-page concepts.
 */

import { useEffect, useId, useState, type ReactNode } from "react";

export const ART_W = 1000;
export const ART_H = 640;

/**
 * A hero drawing renders about 520–620 CSS px wide, so a 16-unit label would
 * land near 9px on screen. Type is therefore authored in geometry units and
 * scaled once, here, to stay legible at the width the frame actually gets.
 */
const TYPE_SCALE = 1.5;

export const artType = (size: number) => Math.round(size * TYPE_SCALE * 10) / 10;

/** Palette — mirrors tokens.css. SVG attributes cannot read CSS vars reliably in all engines. */
export const INK = {
  field0: "#081221",
  field1: "#0e1a2c",
  plate: "#111f33",
  plateEdge: "#1e2f47",
  grid: "#1b2b41",
  line: "#33496a",
  label: "#8ea3bd",
  labelStrong: "#e8eef6",
  teal: "#2dd4bf",
  tealDeep: "#0d9488",
  tealSoft: "rgba(45, 212, 191, 0.14)",
  amber: "#f59e0b",
  amberSoft: "rgba(245, 158, 11, 0.16)",
  danger: "#f87171",
} as const;

export type HeroArtProps = {
  /** Screen-reader description of what the drawing argues. */
  label: string;
  /** Short line under the frame — states that this is a schematic, not a run. */
  caption?: string;
  className?: string;
};

/**
 * A phone gives a hero drawing roughly 350px. The full 1000-unit canvas would
 * then set its type near 7px, so each drawing declares the focal region that
 * carries its argument; narrow screens frame that region instead of shrinking
 * the whole schematic into illegibility.
 */
export type FocalRegion = { x: number; y: number; w: number; h: number };

const NARROW_QUERY = "(max-width: 720px)";

function useNarrowViewport(): boolean {
  const [narrow, setNarrow] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia(NARROW_QUERY);
    const sync = () => setNarrow(mq.matches);
    sync();
    mq.addEventListener("change", sync);
    return () => mq.removeEventListener("change", sync);
  }, []);
  return narrow;
}

/**
 * Frame: ink field, engineering grid, directional light, mounting plate.
 * `defs` lets a drawing add gradients/markers without redefining the frame's own.
 */
export function HeroArtFrame({
  label,
  caption,
  className = "",
  focus,
  defs,
  children,
}: HeroArtProps & { focus?: FocalRegion; defs?: ReactNode; children: ReactNode }) {
  // Ids must be unique per instance: two drawings on one page must not share defs.
  const uid = `dw${useId().replace(/[^a-z0-9]+/gi, "")}`;
  const narrow = useNarrowViewport();
  const view = narrow && focus ? focus : { x: 0, y: 0, w: ART_W, h: ART_H };
  return (
    <figure className={`dw-hero-art ${className}`.trim()}>
      <svg
        className="dw-hero-art-svg"
        viewBox={`${view.x} ${view.y} ${view.w} ${view.h}`}
        style={{ aspectRatio: `${view.w} / ${view.h}` }}
        role="img"
        aria-label={label}
        preserveAspectRatio="xMidYMid meet"
      >
        <defs>
          <linearGradient id={`${uid}-field`} x1="0" y1="0" x2="0.7" y2="1">
            <stop offset="0%" stopColor={INK.field1} />
            <stop offset="100%" stopColor={INK.field0} />
          </linearGradient>
          <radialGradient id={`${uid}-light`} cx="18%" cy="8%" r="82%">
            <stop offset="0%" stopColor="#2dd4bf" stopOpacity="0.16" />
            <stop offset="55%" stopColor="#2dd4bf" stopOpacity="0.03" />
            <stop offset="100%" stopColor="#2dd4bf" stopOpacity="0" />
          </radialGradient>
          <pattern id={`${uid}-grid`} width="40" height="40" patternUnits="userSpaceOnUse">
            <path d="M40 0H0V40" fill="none" stroke={INK.grid} strokeWidth="1" />
          </pattern>
          {defs}
        </defs>

        <rect width={ART_W} height={ART_H} rx="20" fill={`url(#${uid}-field)`} />
        <rect width={ART_W} height={ART_H} rx="20" fill={`url(#${uid}-grid)`} opacity="0.55" />
        <rect width={ART_W} height={ART_H} rx="20" fill={`url(#${uid}-light)`} />
        <rect
          x="0.75"
          y="0.75"
          width={ART_W - 1.5}
          height={ART_H - 1.5}
          rx="20"
          fill="none"
          stroke={INK.plateEdge}
        />
        {children}
      </svg>
      {caption ? <figcaption className="dw-hero-art-caption">{caption}</figcaption> : null}
    </figure>
  );
}

/* ── Primitives shared by the drawings ──────────────────────────── */

/** Section label: small caps set on the grid, used to name a region of the drawing. */
export function ArtLabel({
  x,
  y,
  children,
  tone = "muted",
  anchor = "start",
}: {
  x: number;
  y: number;
  children: string;
  tone?: "muted" | "teal" | "amber" | "strong";
  anchor?: "start" | "middle" | "end";
}) {
  const fill =
    tone === "teal" ? INK.teal : tone === "amber" ? INK.amber : tone === "strong" ? INK.labelStrong : INK.label;
  return (
    <text
      x={x}
      y={y}
      fill={fill}
      fontSize={artType(11)}
      fontWeight="700"
      letterSpacing="1.4"
      textAnchor={anchor}
      fontFamily="'Plus Jakarta Sans', system-ui, sans-serif"
    >
      {children.toUpperCase()}
    </text>
  );
}

export function ArtText({
  x,
  y,
  children,
  size = 18,
  tone = "strong",
  anchor = "start",
  mono = false,
  weight = 600,
}: {
  x: number;
  y: number;
  children: ReactNode;
  size?: number;
  tone?: "muted" | "teal" | "amber" | "strong" | "danger";
  anchor?: "start" | "middle" | "end";
  mono?: boolean;
  weight?: number;
}) {
  const fill =
    tone === "teal"
      ? INK.teal
      : tone === "amber"
        ? INK.amber
        : tone === "danger"
          ? INK.danger
          : tone === "muted"
            ? INK.label
            : INK.labelStrong;
  return (
    <text
      x={x}
      y={y}
      fill={fill}
      fontSize={artType(size)}
      fontWeight={weight}
      textAnchor={anchor}
      fontFamily={
        mono ? "'JetBrains Mono', ui-monospace, monospace" : "'Plus Jakarta Sans', system-ui, sans-serif"
      }
    >
      {children}
    </text>
  );
}

/** Mounted plate — the only container shape in the system. */
export function ArtPlate({
  x,
  y,
  w,
  h,
  tone = "plate",
  radius = 14,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  tone?: "plate" | "teal" | "amber" | "sunken";
  radius?: number;
}) {
  const fill = tone === "sunken" ? INK.field0 : tone === "teal" ? "rgba(13, 148, 136, 0.18)" : INK.plate;
  const stroke = tone === "teal" ? INK.tealDeep : tone === "amber" ? INK.amber : INK.plateEdge;
  return <rect x={x} y={y} width={w} height={h} rx={radius} fill={fill} stroke={stroke} strokeWidth="1.5" />;
}

/** A row of data, drawn as a filament rather than a table row. */
export function ArtFilament({
  x1,
  x2,
  y,
  tone = "line",
  dashed = false,
  width = 2,
}: {
  x1: number;
  x2: number;
  y: number;
  tone?: "line" | "teal" | "amber";
  dashed?: boolean;
  width?: number;
}) {
  const stroke = tone === "teal" ? INK.teal : tone === "amber" ? INK.amber : INK.line;
  return (
    <line
      x1={x1}
      y1={y}
      x2={x2}
      y2={y}
      stroke={stroke}
      strokeWidth={width}
      strokeLinecap="round"
      strokeDasharray={dashed ? "7 8" : undefined}
    />
  );
}

/** Proof seal — the terminating mark of a run. Used only where a reread happened. */
export function ArtSeal({ cx, cy, r = 34, label }: { cx: number; cy: number; r?: number; label?: string }) {
  return (
    <g>
      <circle cx={cx} cy={cy} r={r} fill="rgba(13, 148, 136, 0.16)" stroke={INK.teal} strokeWidth="2" />
      <circle cx={cx} cy={cy} r={r + 9} fill="none" stroke={INK.teal} strokeOpacity="0.3" strokeDasharray="3 7" />
      <path
        d={`M${cx - r * 0.34} ${cy + r * 0.04}l${r * 0.26} ${r * 0.28} ${r * 0.46} -${r * 0.5}`}
        fill="none"
        stroke={INK.teal}
        strokeWidth="3"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {label ? (
        <ArtText x={cx} y={cy + r + 26} anchor="middle" size={15} tone="teal">
          {label}
        </ArtText>
      ) : null}
    </g>
  );
}

/** Travelling packet — the single motion allowed per drawing. */
export function ArtPacket({
  path,
  dur = 4.2,
  delay = 0,
  tone = "teal",
}: {
  path: string;
  dur?: number;
  delay?: number;
  tone?: "teal" | "amber";
}) {
  return (
    <g className="dw-hero-art-packet">
      <circle r="7" fill={tone === "amber" ? INK.amber : INK.teal} opacity="0.28">
        <animateMotion dur={`${dur}s`} begin={`${delay}s`} repeatCount="indefinite" path={path} />
      </circle>
      <circle r="3.2" fill={tone === "amber" ? INK.amber : INK.teal}>
        <animateMotion dur={`${dur}s`} begin={`${delay}s`} repeatCount="indefinite" path={path} />
      </circle>
    </g>
  );
}

/*
 * A field chip sets its name and its type on one line, so the pair has to fit
 * the chip by construction — a drawing must never ship a name overprinting a
 * type. Mono advance is ~0.6em, which is enough to scale the pair down to fit.
 */
const MONO_ADVANCE = 0.6;
const FIELD_NAME_SIZE = 13.5;
const FIELD_TYPE_SIZE = 11;
const FIELD_GAP = 14;

/** Column/field chip — typed, because type identity is the product's argument. */
export function ArtField({
  x,
  y,
  w = 168,
  name,
  type,
  tone = "plate",
}: {
  x: number;
  y: number;
  w?: number;
  name: string;
  type?: string;
  tone?: "plate" | "teal" | "amber";
}) {
  const stroke = tone === "teal" ? INK.tealDeep : tone === "amber" ? INK.amber : INK.plateEdge;
  const need =
    name.length * artType(FIELD_NAME_SIZE) * MONO_ADVANCE +
    (type ? type.length * artType(FIELD_TYPE_SIZE) * MONO_ADVANCE + FIELD_GAP : 0);
  const fit = Math.min(1, (w - 28) / need);
  return (
    <g>
      <rect x={x} y={y} width={w} height="44" rx="9" fill={INK.field0} stroke={stroke} strokeWidth="1.5" />
      <ArtText
        x={x + 14}
        y={y + 29}
        size={FIELD_NAME_SIZE * fit}
        mono
        tone={tone === "amber" ? "amber" : "strong"}
        weight={500}
      >
        {name}
      </ArtText>
      {type ? (
        <ArtText x={x + w - 14} y={y + 29} size={FIELD_TYPE_SIZE * fit} anchor="end" tone="muted" mono weight={500}>
          {type}
        </ArtText>
      ) : null}
    </g>
  );
}
