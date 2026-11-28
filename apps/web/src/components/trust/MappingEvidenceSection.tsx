import { useEffect, useState } from "react";
import { LoadingBlock } from "../LoadingState";
import { badgeClass, type ParityTone } from "../../lib/parity";
import {
  FLAG_LABEL,
  MODE_LABEL,
  STATUS_LABEL,
  fetchJobMappingWitness,
  type WitnessPair,
  type WitnessReport,
  type WitnessSnapshot,
  type WitnessStatus,
} from "../../lib/mappingWitness";

const TONE: Record<WitnessStatus, ParityTone> = {
  witnessed: "ok",
  consistent: "muted",
  flagged: "warn",
  differs: "muted",
  not_comparable: "muted",
  not_read: "muted",
  no_evidence: "muted",
};

function plural(n: number, one: string, many = `${one}s`): string {
  return `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;
}

/** One sentence: what the witness looked at and what it found. */
export function witnessHeadline(report: WitnessReport): string {
  const s = report.summary;
  const parts = [plural(s.pairs, "mapped column")];
  if (s.witnessed) parts.push(`${s.witnessed.toLocaleString("en-US")} confirmed by values`);
  if (s.flagged) parts.push(`${s.flagged.toLocaleString("en-US")} to review`);
  if (s.no_evidence) parts.push(`${s.no_evidence.toLocaleString("en-US")} without evidence`);
  return parts.join(" · ");
}

function decisionText(snapshot: WitnessSnapshot, pair: WitnessPair): string {
  const id = pair.flag?.id;
  const reviews = snapshot.flag_reviews ?? [];
  const kept = reviews.find((r) => r.decision === "kept" && id && r.flag_id === id);
  if (kept) return `Kept by ${kept.by} (Keep this mapping)`;
  const continued = reviews.find((r) => r.decision === "continued" && id && r.flag_id === id);
  if (continued) return `Not reviewed: ${continued.by} continued past it`;
  return "Not reviewed";
}

function when(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toISOString().replace("T", " ").slice(0, 16) + " UTC";
}

/**
 * The Mapping Witness section of the Trust Report: per-column evidence that
 * each mapped pair means what it says. Self-contained (it loads its own data)
 * so the Trust Report layout can mount it anywhere.
 */
export function MappingEvidenceView({ record }: { record: WitnessSnapshot | null }) {
  if (!record) {
    return (
      <p className="df2-parity-muted">
        This job was submitted before mapping evidence was recorded with each run.
      </p>
    );
  }
  if (record.status !== "ok") {
    return <p className="df2-parity-muted">{record.reason ?? "The mapping evidence is unavailable for this job."}</p>;
  }
  if (!record.report) {
    return (
      <p className="df2-parity-muted">
        Not checked: {record.not_checked_reason ?? "no mapping check ran for the mapping this job used."}
      </p>
    );
  }
  const report = record.report;
  const resolved = (record.flag_reviews ?? []).filter((r) => r.decision === "changed");
  const flagged = report.pairs.filter((p) => p.status === "flagged");
  return (
    <>
      <p className="df2-parity-muted">
        {witnessHeadline(report)}. {MODE_LABEL[report.mode]}
        {report.sample.matching_keys ? ` (${plural(report.sample.matching_keys, "matching key")}).` : "."}
        {record.requested_by ? ` Checked by ${record.requested_by}` : " Checked"}
        {record.report_recorded_at ? ` at ${when(record.report_recorded_at)}.` : "."}
      </p>
      {flagged.length > 0 && (
        <ul className="df2-trust-ai-list" aria-label="Flagged mappings">
          {flagged.map((p) => (
            <li key={p.source} className="df2-trust-ai-item">
              <span className="df2-trust-ai-subject">
                {p.source} → {p.target}
              </span>
              <span className={badgeClass("warn")}>{p.flag ? FLAG_LABEL[p.flag.kind] : "Review"}</span>
              <span className="df2-trust-ai-detail">{p.flag?.reason ?? p.reason}</span>
              <span className="df2-parity-muted">{decisionText(record, p)}</span>
            </li>
          ))}
        </ul>
      )}
      {resolved.length > 0 && (
        <ul className="df2-trust-ai-list" aria-label="Flags resolved before the run">
          {resolved.map((r) => (
            <li key={`${r.source}-${r.flagged_target}`} className="df2-trust-ai-item">
              <span className="df2-trust-ai-subject">
                {r.source} → {r.flagged_target}
              </span>
              <span className={badgeClass("ok")}>Resolved</span>
              <span className="df2-trust-ai-detail">
                Flagged earlier; changed to {r.new_target} by {r.by}, which this job ran.
              </span>
            </li>
          ))}
        </ul>
      )}
      <details className="df2-trust-ai-details">
        <summary>Every mapped column</summary>
        <ul className="df2-trust-ai-list df2-trust-ai-all" aria-label="Every mapped column">
          {report.pairs.map((p) => (
            <li key={p.source} className="df2-trust-ai-item">
              <span className="df2-trust-ai-subject">
                {p.source} → {p.target}
              </span>
              <span className={badgeClass(TONE[p.status])}>{STATUS_LABEL[p.status]}</span>
              <span className="df2-trust-ai-detail">{p.flag?.reason ?? p.reason}</span>
            </li>
          ))}
        </ul>
      </details>
    </>
  );
}

export function MappingEvidenceSection({ jobId }: { jobId: string }) {
  const [record, setRecord] = useState<WitnessSnapshot | null>(null);
  const [state, setState] = useState<"loading" | "ready" | "error">("loading");
  const [error, setError] = useState("");
  useEffect(() => {
    let live = true;
    setState("loading");
    fetchJobMappingWitness(jobId)
      .then((r) => {
        if (!live) return;
        setRecord(r);
        setState("ready");
      })
      .catch((e: unknown) => {
        if (!live) return;
        setError(e instanceof Error ? e.message : "Mapping evidence could not be loaded");
        setState("error");
      });
    return () => {
      live = false;
    };
  }, [jobId]);
  return (
    <section className="df2-parity-card" aria-label="Mapping evidence">
      <header className="df2-parity-section-head">
        <h3 className="df2-parity-h3">Mapping evidence</h3>
      </header>
      {state === "loading" && <LoadingBlock title="Loading mapping evidence" size="sm" />}
      {state === "error" && (
        <p className="df2-parity-muted" role="alert">
          {error}
        </p>
      )}
      {state === "ready" && <MappingEvidenceView record={record} />}
    </section>
  );
}
