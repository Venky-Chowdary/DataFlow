import { useEffect, useState } from "react";
import { LoadingBlock } from "../LoadingState";
import { badgeClass, type ParityTone } from "../../lib/parity";
import {
  FLAG_LABEL,
  MODE_LABEL,
  STATUS_LABEL,
  fetchJobMappingWitness,
  type WitnessPair,
  type WitnessRecord,
  type WitnessStatus,
} from "../../lib/mappingWitness";

const TONE: Record<WitnessStatus, ParityTone> = {
  witnessed: "ok",
  consistent: "muted",
  flagged: "warn",
  differs: "muted",
  no_evidence: "muted",
};

function plural(n: number, one: string, many = `${one}s`): string {
  return `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;
}

/** One sentence: what the witness looked at and what it found. */
export function witnessHeadline(record: WitnessRecord): string {
  const s = record.report.summary;
  const parts = [plural(s.pairs, "mapped column")];
  if (s.witnessed) parts.push(`${s.witnessed.toLocaleString("en-US")} confirmed by values`);
  if (s.flagged) parts.push(`${s.flagged.toLocaleString("en-US")} to review`);
  if (s.no_evidence) parts.push(`${s.no_evidence.toLocaleString("en-US")} without evidence`);
  return parts.join(" · ");
}

function decisionText(record: WitnessRecord, pair: WitnessPair): string {
  const d = record.decisions.find((x) => x.source === pair.source);
  if (!d) return "Not reviewed";
  return d.decision === "changed"
    ? `Changed to ${d.new_target ?? "another column"} by ${d.by}`
    : `Kept by ${d.by}`;
}

/**
 * The Mapping Witness section of the Trust Report: per-column evidence that
 * each mapped pair means what it says. Self-contained (it loads its own data)
 * so the Trust Report layout can mount it anywhere.
 */
export function MappingEvidenceView({ record }: { record: WitnessRecord | null }) {
  if (!record) {
    return (
      <p className="df2-parity-muted">
        No mapping check ran for the mapping this job used. Map runs it when the sample is loaded.
      </p>
    );
  }
  const flagged = record.report.pairs.filter((p) => p.status === "flagged");
  return (
    <>
      <p className="df2-parity-muted">
        {witnessHeadline(record)}. {MODE_LABEL[record.report.mode]}
        {record.report.sample.matching_keys
          ? ` (${plural(record.report.sample.matching_keys, "matching key")}).`
          : "."}
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
      <details className="df2-trust-ai-details">
        <summary>Every mapped column</summary>
        <div className="df2-trust-ai-table-wrap">
        <table className="df2-trust-ai-table">
          <thead>
            <tr>
              <th scope="col">Column</th>
              <th scope="col">Result</th>
              <th scope="col">Why</th>
            </tr>
          </thead>
          <tbody>
            {record.report.pairs.map((p) => (
              <tr key={p.source}>
                <td>
                  {p.source} → {p.target}
                </td>
                <td>
                  <span className={badgeClass(TONE[p.status])}>{STATUS_LABEL[p.status]}</span>
                </td>
                <td>{p.flag?.reason ?? p.reason}</td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      </details>
    </>
  );
}

export function MappingEvidenceSection({ jobId }: { jobId: string }) {
  const [record, setRecord] = useState<WitnessRecord | null>(null);
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
