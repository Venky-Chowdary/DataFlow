/**
 * Mapping Proof drawer — honest operator evidence for any source×dest pair.
 * Prefers API `mapping_proof` when present; otherwise builds a transparent
 * client summary from EditableMapping (no invented 99% confidence).
 */
import { useMemo, useState } from "react";
import { Drawer } from "./ui/Drawer";
import { Dialog } from "./ui/Dialog";
import { DtIcon } from "./DtIcon";
import type { EditableMapping } from "../lib/mapping";
import { typeBadgeClass } from "../lib/typeDisplay";

export interface MappingProofEvidence {
  strategy?: string;
  name_match?: boolean;
  type_aligned?: boolean;
  sample_n?: number | null;
  sample_parse_rate?: number | null;
  score_gap?: number | null;
  quality_notes?: string[];
  create_new?: boolean;
  sample_preview?: string[];
  confidence_breakdown?: {
    strategy?: number;
    name?: number;
    type?: number;
    sample?: number;
  };
}

export interface MappingProofRisk {
  code: string;
  severity: string;
  message: string;
}

export interface MappingProofRow {
  source: string;
  target: string;
  source_type: string;
  target_type: string;
  dest_native_type?: string | null;
  transform: string;
  transform_fidelity: string;
  confidence: number;
  reasoning?: string;
  requires_review?: boolean;
  evidence?: MappingProofEvidence;
  risks?: MappingProofRisk[];
  pii?: string[];
  schema_decision?: string;
  assignment_strategy?: string;
  match_quality?: string;
  sample_preview?: string[];
}

export interface MappingProof {
  dest_mode: "create_new" | "match_existing" | string;
  destination_db_type?: string;
  source_kind?: string;
  dest_kind?: string;
  quarantine_posture?: string;
  delivery_semantics?: string;
  summary?: {
    mapped_count?: number;
    create_ddl_count?: number;
    match_existing_count?: number;
    risk_count?: number;
    review_count?: number;
    avg_confidence?: number;
    max_confidence?: number;
    confidence_cap_create_new?: number;
    cdc_detected?: boolean;
  };
  sync_mode?: string;
  mappings?: MappingProofRow[];
  global_risks?: MappingProofRisk[];
}

const CREATE_NEW_CAP = 0.93;

function fidelityOf(transform?: string): string {
  const t = (transform || "none").toLowerCase();
  if (!t || t === "none" || t === "identity") return "preserve";
  if (["decimal", "integer", "boolean", "date", "datetime", "time", "uuid", "json", "binary"].includes(t)) {
    return "lossy_cast";
  }
  return "mutate";
}

function clientBreakdown(conf: number, nameMatch: boolean, typeAligned: boolean): MappingProofEvidence["confidence_breakdown"] {
  const raw = {
    strategy: 0.55,
    name: nameMatch ? 0.22 : 0.10,
    type: typeAligned ? 0.18 : 0.08,
    sample: 0.05,
  };
  const total = Object.values(raw).reduce((a, b) => a + b, 0) || 1;
  return {
    strategy: Math.round((conf * raw.strategy) / total * 1000) / 1000,
    name: Math.round((conf * raw.name) / total * 1000) / 1000,
    type: Math.round((conf * raw.type) / total * 1000) / 1000,
    sample: Math.round((conf * raw.sample) / total * 1000) / 1000,
  };
}

/** Client-side proof when API payload is missing — never invents 0.99. */
export function buildClientMappingProof(
  mappings: EditableMapping[],
  opts: {
    destColumns?: string[];
    destType?: string;
  } = {},
): MappingProof {
  const destCols = opts.destColumns ?? [];
  const destMode = destCols.length === 0 ? "create_new" : "match_existing";
  const rows: MappingProofRow[] = mappings.map((m) => {
    let conf = m.confidence;
    if (destMode === "create_new") conf = Math.min(conf, CREATE_NEW_CAP);
    const transform = m.transform ?? "none";
    const fidelity = fidelityOf(transform);
    const risks: MappingProofRisk[] = [];
    if (fidelity === "mutate" && (transform === "trim" || transform === "upper" || transform === "lower")) {
      risks.push({
        code: "value_mutate",
        severity: "info",
        message: `Transform '${transform}' changes values vs source before write.`,
      });
    }
    if (fidelity === "lossy_cast") {
      risks.push({
        code: "coerce_cast",
        severity: "warn",
        message: `Cast '${transform}' may coerce-to-null on bad samples; failures quarantine.`,
      });
    }
    if (m.isPii || (m.reason || "").toLowerCase().includes("email")) {
      risks.push({
        code: "pii_governance",
        severity: "info",
        message: "PII/semantic classification — choose Mask / hash / tokenize / preserve.",
      });
    }
    const exists = Boolean(m.existsInDestination);
    const nameMatch = m.source.toLowerCase() === m.target.toLowerCase();
    const typeAligned = (m.inferredType || "").toLowerCase() === (m.destType || m.inferredType || "").toLowerCase();
    const schema =
      destMode === "create_new"
        ? `CREATE column \`${m.target}\` as ${m.destType || m.inferredType || "VARCHAR"}`
        : exists
          ? `MATCH existing \`${m.target}\` (${m.destType || m.inferredType || "VARCHAR"})`
          : `ADD new column \`${m.target}\` as ${m.destType || m.inferredType || "VARCHAR"}`;

    return {
      source: m.source,
      target: m.target,
      source_type: m.inferredType || "VARCHAR",
      target_type: m.destType || m.inferredType || "VARCHAR",
      transform,
      transform_fidelity: fidelity,
      confidence: conf,
      reasoning: m.reason,
      requires_review: m.requiresReview,
      evidence: {
        strategy: destMode === "create_new" ? "identity_passthrough" : "operator_or_pipeline",
        name_match: nameMatch,
        type_aligned: typeAligned,
        create_new: destMode === "create_new",
        quality_notes: [],
        confidence_breakdown: clientBreakdown(conf, nameMatch, typeAligned),
      },
      risks,
      pii: m.isPii ? ["pii"] : [],
      schema_decision: schema,
      assignment_strategy: destMode === "create_new" ? "identity_passthrough" : undefined,
      match_quality: nameMatch && typeAligned ? "exact_name" : nameMatch ? "name_only" : typeAligned ? "type_only" : "semantic",
    };
  });

  const confidences = rows.map((r) => r.confidence);
  const avg = confidences.length ? confidences.reduce((a, b) => a + b, 0) / confidences.length : 0;
  const max = confidences.length ? Math.max(...confidences) : 0;
  const globalRisks = rows.flatMap((r) => r.risks || []);
  const seen = new Set<string>();
  const uniqueRisks = globalRisks.filter((r) => {
    const k = `${r.code}|${r.message}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  return {
    dest_mode: destMode,
    destination_db_type: opts.destType || "",
    quarantine_posture:
      "Bad or unparseable rows are quarantined and surfaced for review — DataFlow does not silently drop them.",
    delivery_semantics:
      "Default delivery is at-least-once with upsert/idempotent write where supported; exactly-once is not claimed unless a route proves it.",
    summary: {
      mapped_count: rows.length,
      create_ddl_count: destMode === "create_new" ? rows.length : rows.filter((r) => r.schema_decision?.startsWith("ADD")).length,
      match_existing_count: destMode === "match_existing" ? rows.filter((r) => r.schema_decision?.startsWith("MATCH")).length : 0,
      risk_count: uniqueRisks.length,
      review_count: rows.filter((r) => r.requires_review).length,
      avg_confidence: Math.round(avg * 1000) / 1000,
      max_confidence: Math.round(max * 1000) / 1000,
      confidence_cap_create_new: CREATE_NEW_CAP,
    },
    mappings: rows,
    global_risks: uniqueRisks.slice(0, 40),
  };
}

/** Prefer API proof; overlay live editor transforms/confidence. */
export function mergeMappingProof(
  mappingProof: MappingProof | null | undefined,
  mappings: EditableMapping[],
  opts: { destColumns?: string[]; destType?: string } = {},
): MappingProof {
  const client = buildClientMappingProof(mappings, opts);
  if (!mappingProof?.mappings?.length) return client;
  const bySource = new Map(mappings.map((m) => [m.source, m]));
  const createNew = mappingProof.dest_mode === "create_new" || (opts.destColumns?.length ?? 0) === 0;
  const mergedRows = mappingProof.mappings.map((row) => {
    const live = bySource.get(row.source);
    if (!live) return row;
    const transform = live.transform || row.transform;
    let confidence = live.confidence;
    if (createNew) confidence = Math.min(confidence, CREATE_NEW_CAP);
    const fidelity = fidelityOf(transform);
    const risks = [...(row.risks || [])];
    // Refresh mutate/cast risk when operator changes transform in Map.
    const withoutTransformRisks = risks.filter(
      (r) => !["trim_mutates", "value_mutate", "pii_transform", "coerce_cast"].includes(r.code),
    );
    if (fidelity === "mutate") {
      withoutTransformRisks.push({
        code: transform === "trim" || transform === "trim_id" ? "trim_mutates" : "value_mutate",
        severity: "info",
        message: `Transform '${transform}' changes values vs source before write.`,
      });
    } else if (fidelity === "lossy_cast") {
      withoutTransformRisks.push({
        code: "coerce_cast",
        severity: "warn",
        message: `Cast '${transform}' may coerce-to-null on bad samples; failures quarantine.`,
      });
    }
    return {
      ...row,
      target: live.target,
      target_type: live.destType || row.target_type,
      transform,
      transform_fidelity: fidelity,
      confidence,
      reasoning: live.reason || row.reasoning,
      requires_review: live.requiresReview,
      risks: withoutTransformRisks,
    };
  });
  return {
    ...mappingProof,
    destination_db_type: mappingProof.destination_db_type || opts.destType || "",
    mappings: mergedRows,
    summary: {
      ...mappingProof.summary,
      mapped_count: mergedRows.length,
      avg_confidence: client.summary?.avg_confidence,
      max_confidence: createNew
        ? Math.min(client.summary?.max_confidence ?? CREATE_NEW_CAP, CREATE_NEW_CAP)
        : client.summary?.max_confidence,
      risk_count: new Set(mergedRows.flatMap((r) => (r.risks || []).map((x) => x.code))).size,
      review_count: mergedRows.filter((r) => r.requires_review).length,
    },
  };
}

function pct(n?: number) {
  if (n == null || Number.isNaN(n)) return "—";
  return `${Math.round(n * 100)}%`;
}

function fidelityLabel(f: string) {
  if (f === "preserve") return "Preserve";
  if (f === "lossy_cast") return "Cast risk";
  return "Mutates";
}

function fidelityClass(f: string) {
  if (f === "preserve") return "ok";
  if (f === "lossy_cast") return "warn";
  return "info";
}

function matchQualityLabel(q?: string) {
  if (q === "exact_name") return "Exact name + type";
  if (q === "name_only") return "Name match";
  if (q === "type_only") return "Type aligned";
  return "Semantic match";
}

function ConfidenceBars({ breakdown, total }: { breakdown?: MappingProofEvidence["confidence_breakdown"]; total: number }) {
  if (!breakdown) return null;
  const parts: Array<{ key: string; label: string; value: number }> = [
    { key: "strategy", label: "Strategy", value: breakdown.strategy ?? 0 },
    { key: "name", label: "Name", value: breakdown.name ?? 0 },
    { key: "type", label: "Type", value: breakdown.type ?? 0 },
    { key: "sample", label: "Sample", value: breakdown.sample ?? 0 },
  ];
  return (
    <div className="df2-map-proof-bars" aria-label={`Confidence breakdown totaling ${pct(total)}`}>
      {parts.map((p) => (
        <div key={p.key} className="df2-map-proof-bar-row">
          <span>{p.label}</span>
          <div className="df2-map-proof-bar-track" aria-hidden>
            <div
              className={`df2-map-proof-bar-fill is-${p.key}`}
              style={{ width: `${Math.max(2, Math.round(p.value * 100))}%` }}
            />
          </div>
          <em>{pct(p.value)}</em>
        </div>
      ))}
    </div>
  );
}

function PairCard({ r }: { r: MappingProofRow }) {
  const aligned = r.evidence?.name_match && r.evidence?.type_aligned;
  return (
    <li className={`df2-map-proof-pair${aligned ? " is-aligned" : ""}${r.requires_review ? " is-review" : ""}`}>
      <div className="df2-map-proof-rail" aria-label={`${r.source} maps to ${r.target}`}>
        <div className="df2-map-proof-rail-end is-source">
          <span className="df2-map-proof-rail-kicker">Source</span>
          <strong className="df2-map-proof-src" title={r.source}>{r.source}</strong>
          <span className={`df2-type-badge ${typeBadgeClass(r.source_type)}`}>{r.source_type}</span>
        </div>
        <div className="df2-map-proof-rail-mid">
          <div className="df2-map-proof-rail-line" aria-hidden />
          <span className="df2-map-proof-rail-pct">{pct(r.confidence)}</span>
          <div className="df2-map-proof-rail-line" aria-hidden />
        </div>
        <div className="df2-map-proof-rail-end is-dest">
          <span className="df2-map-proof-rail-kicker">Destination</span>
          <strong className="df2-map-proof-tgt" title={r.target}>{r.target}</strong>
          <span className={`df2-type-badge ${typeBadgeClass(r.target_type)}`}>
            {r.dest_native_type || r.target_type}
          </span>
        </div>
      </div>

      <div className="df2-map-proof-pair-meta">
        <span className={`df2-badge df2-badge-xs fidelity-${fidelityClass(r.transform_fidelity)}`}>
          {fidelityLabel(r.transform_fidelity)} · {r.transform || "none"}
        </span>
        <span className="df2-badge df2-badge-muted df2-badge-xs">{matchQualityLabel(r.match_quality)}</span>
        {r.pii?.map((p) => (
          <span key={p} className="df2-badge df2-badge-run df2-badge-xs">{p}</span>
        ))}
        {r.requires_review && (
          <span className="df2-badge df2-badge-warn df2-badge-xs">review</span>
        )}
      </div>

      {r.schema_decision && (
        <p className="df2-map-proof-schema">{r.schema_decision}</p>
      )}
      {r.reasoning && <p className="df2-map-proof-why">{r.reasoning}</p>}

      {((r.sample_preview?.length ?? 0) > 0 || (r.evidence?.sample_preview?.length ?? 0) > 0) && (
        <div className="df2-map-proof-samples" aria-label="Sample values from source">
          <span className="df2-map-proof-samples-label">Sample values (source)</span>
          {(r.sample_preview?.length ? r.sample_preview : r.evidence?.sample_preview || []).map((s) => (
            <code key={s} className="df2-map-proof-sample" title={s}>{s}</code>
          ))}
        </div>
      )}

      <ConfidenceBars breakdown={r.evidence?.confidence_breakdown} total={r.confidence} />

      {r.evidence && (
        <dl className="df2-map-proof-evidence">
          <div>
            <dt>Strategy</dt>
            <dd>{r.evidence.strategy || "—"}</dd>
          </div>
          <div>
            <dt>Name</dt>
            <dd>{r.evidence.name_match ? "match" : "semantic"}</dd>
          </div>
          <div>
            <dt>Type</dt>
            <dd>{r.evidence.type_aligned ? "aligned" : "check"}</dd>
          </div>
          <div>
            <dt>Samples</dt>
            <dd>
              {r.evidence.sample_n != null ? `n=${r.evidence.sample_n}` : "—"}
              {r.evidence.sample_parse_rate != null
                ? ` · ${Math.round(r.evidence.sample_parse_rate * 100)}%`
                : ""}
            </dd>
          </div>
        </dl>
      )}

      {(r.risks?.length ?? 0) > 0 && (
        <ul className="df2-map-proof-pair-risks">
          {r.risks!.map((risk) => (
            <li key={`${risk.code}-${risk.message}`} className={`sev-${risk.severity}`}>
              <span className="df2-map-proof-risk-code">{risk.code}</span>
              {risk.message}
            </li>
          ))}
        </ul>
      )}
    </li>
  );
}

interface MappingProofDrawerProps {
  open: boolean;
  onClose: () => void;
  proof: MappingProof;
  sourceLabel?: string;
  destLabel?: string;
}

export function MappingProofDrawer({
  open,
  onClose,
  proof,
  sourceLabel,
  destLabel,
}: MappingProofDrawerProps) {
  const [expanded, setExpanded] = useState(false);
  const [filter, setFilter] = useState<"all" | "risks" | "review" | "pii">("all");
  const [query, setQuery] = useState("");

  const rows = useMemo(() => {
    let list = proof.mappings ?? [];
    if (filter === "risks") list = list.filter((r) => (r.risks?.length ?? 0) > 0);
    if (filter === "review") list = list.filter((r) => r.requires_review);
    if (filter === "pii") list = list.filter((r) => (r.pii?.length ?? 0) > 0);
    const q = query.trim().toLowerCase();
    if (q) {
      list = list.filter(
        (r) =>
          r.source.toLowerCase().includes(q)
          || r.target.toLowerCase().includes(q)
          || (r.schema_decision || "").toLowerCase().includes(q),
      );
    }
    return list;
  }, [proof.mappings, filter, query]);

  const modeLabel =
    proof.dest_mode === "create_new"
      ? "Create-new table — columns CREATE on first write"
      : "Match existing destination schema";

  const summary = proof.summary;
  const exactCount = (proof.mappings ?? []).filter((r) => r.match_quality === "exact_name").length;
  const body = (
    <div className="df2-map-proof">
      <div className="df2-map-proof-hero" aria-label="Mapping proof summary">
        <div className="df2-map-proof-hero-mode">
          <span className={`df2-badge ${proof.dest_mode === "create_new" ? "df2-badge-warn" : "df2-badge-live"}`}>
            {proof.dest_mode === "create_new" ? "Create new" : "Match existing"}
          </span>
          {(proof.summary?.cdc_detected || (proof.sync_mode || "").toLowerCase().includes("cdc")) && (
            <span className="df2-badge df2-badge-info df2-badge-xs">CDC · at-least-once</span>
          )}
          <strong>{modeLabel}</strong>
        </div>
        <div className="df2-drawer-facts df2-map-proof-kpis">
          <div className="df2-drawer-fact">
            <span>Pairs</span>
            <strong>{summary?.mapped_count ?? rows.length}</strong>
          </div>
          <div className="df2-drawer-fact">
            <span>Exact overlaps</span>
            <strong>{exactCount}</strong>
          </div>
          <div className="df2-drawer-fact">
            <span>Avg / max conf</span>
            <strong>
              {pct(summary?.avg_confidence)} / {pct(summary?.max_confidence)}
              {proof.dest_mode === "create_new" ? (
                <span className="df2-map-proof-cap"> · cap {pct(summary?.confidence_cap_create_new ?? CREATE_NEW_CAP)}</span>
              ) : null}
            </strong>
          </div>
          <div className="df2-drawer-fact">
            <span>Risks / review</span>
            <strong>
              {summary?.risk_count ?? 0} / {summary?.review_count ?? 0}
            </strong>
          </div>
        </div>
      </div>

      {(sourceLabel || destLabel) && (
        <p className="df2-map-proof-route">
          <strong>{sourceLabel || "Source"}</strong>
          <DtIcon name="transfer" size={12} />
          <strong>{destLabel || "Destination"}</strong>
          {proof.destination_db_type ? (
            <span className="df2-map-proof-route-meta">· {proof.destination_db_type}</span>
          ) : null}
        </p>
      )}

      <section className="df2-drawer-section" aria-label="Integrity posture">
        <div className="df2-drawer-section-head">
          <h3>Integrity posture</h3>
        </div>
        <ul className="df2-map-proof-posture">
          <li>{proof.quarantine_posture}</li>
          <li>{proof.delivery_semantics}</li>
        </ul>
      </section>

      {(proof.global_risks?.length ?? 0) > 0 && (
        <section className="df2-drawer-section" aria-label="Fidelity risks">
          <div className="df2-drawer-section-head">
            <h3>Fidelity / data-loss risks</h3>
            <span className="df2-drawer-count">{proof.global_risks!.length}</span>
          </div>
          <ul className="df2-map-proof-risks">
            {proof.global_risks!.slice(0, 16).map((r) => (
              <li key={`${r.code}-${r.message}`} className={`sev-${r.severity}`}>
                <span className="df2-map-proof-risk-code">{r.code}</span>
                {r.message}
              </li>
            ))}
          </ul>
        </section>
      )}

      <section className="df2-drawer-section" aria-label="Field mappings">
        <div className="df2-drawer-section-head">
          <h3>Column matches</h3>
          <span className="df2-drawer-count">{rows.length}</span>
        </div>
        <div className="df2-map-proof-toolbar">
          <input
            className="df2-input df2-map-proof-search"
            type="search"
            placeholder="Search source or destination column…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            aria-label="Search mapped columns"
          />
          <div className="df2-map-proof-filters" role="tablist">
            {([
              ["all", "All"],
              ["risks", "Risks"],
              ["review", "Review"],
              ["pii", "PII"],
            ] as const).map(([id, label]) => (
              <button
                key={id}
                type="button"
                role="tab"
                aria-selected={filter === id}
                className={`df2-btn df2-btn-sm ${filter === id ? "df2-btn-primary" : "df2-btn-ghost"}`}
                onClick={() => setFilter(id)}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        <ul className="df2-map-proof-pairs">
          {rows.map((r) => (
            <PairCard key={`${r.source}->${r.target}`} r={r} />
          ))}
          {rows.length === 0 && (
            <li className="df2-drawer-empty-line">No pairs match this filter.</li>
          )}
        </ul>
      </section>
    </div>
  );

  const footer = (
    <div className="df2-map-proof-footer">
      <button type="button" className="df2-btn df2-btn-sm" onClick={() => setExpanded(true)}>
        <DtIcon name="expand" size={14} /> Open full proof
      </button>
      <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={onClose}>
        Done
      </button>
    </div>
  );

  return (
    <>
      <Drawer
        open={open && !expanded}
        onClose={onClose}
        title="Mapping proof"
        subtitle="Exactly how columns match — confidence evidence, transforms, and fidelity risks"
        icon={<DtIcon name="sparkle" size={18} />}
        width={720}
        ariaLabel="Mapping proof"
        footer={footer}
        className="df2-map-proof-drawer"
      >
        {body}
      </Drawer>

      <Dialog
        open={open && expanded}
        onClose={() => {
          setExpanded(false);
          onClose();
        }}
        size="xl"
        title="Mapping proof — full detail"
        subtitle="Source → destination overlaps, schema decisions, transforms, and quarantine posture"
        ariaLabel="Full mapping proof"
        className="df2-map-proof-dialog"
        footer={
          <button
            type="button"
            className="df2-btn df2-btn-primary"
            onClick={() => {
              setExpanded(false);
              onClose();
            }}
          >
            Done
          </button>
        }
      >
        {body}
      </Dialog>
    </>
  );
}
