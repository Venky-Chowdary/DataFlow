import { useEffect, useState } from "react";
import { LoadingBlock } from "../LoadingState";
import { badgeClass } from "../../lib/parity";
import {
  KIND_LABEL,
  checkLabel,
  checkTone,
  decisionLabel,
  fetchAiDecisions,
  originLabel,
  type AiDecisionLedger,
} from "../../lib/aiDecisions";

function plural(n: number, one: string, many = `${one}s`): string {
  return `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;
}

/** One sentence: how much AI touched this job, and how much of it a check confirmed. */
export function aiHeadline(ledger: AiDecisionLedger): string {
  const entries = ledger.entries;
  const model = entries.filter((e) => e.origin.type === "model").length;
  const checked = entries.filter((e) => checkTone(e) === "ok").length;
  return `${plural(entries.length, "AI decision")} · ${model.toLocaleString("en-US")} by a model · ${checked.toLocaleString("en-US")} confirmed by a check`;
}

export function AiInvolvementView({ ledger }: { ledger: AiDecisionLedger }) {
  if (ledger.status === "unavailable") {
    return <p className="df2-parity-muted">{ledger.reason ?? "The AI decisions ledger is unavailable."}</p>;
  }
  if (!ledger.entries.length) {
    return <p className="df2-parity-muted">No AI proposals or model calls are recorded for this job.</p>;
  }
  return (
    <>
      <p className="df2-parity-muted">{aiHeadline(ledger)}. Sealed into the signed report.</p>
      <ul className="df2-trust-ai-list" aria-label="AI decisions">
        {ledger.entries.map((e) => (
          <li key={e.id} className="df2-trust-ai-item">
            <span className="df2-trust-ai-kind">{KIND_LABEL[e.kind] ?? e.kind}</span>
            <span className="df2-trust-ai-subject">{e.subject}</span>
            <span className={badgeClass(checkTone(e))} title={e.verification.detail || undefined}>
              {checkLabel(e)}
            </span>
            <span className="df2-trust-ai-detail">
              Proposed by {originLabel(e.origin)} · {decisionLabel(e)}
            </span>
          </li>
        ))}
      </ul>
      {ledger.ledger_sha256 && (
        <p className="df2-parity-muted df2-trust-ai-digest">Ledger SHA-256 {ledger.ledger_sha256}</p>
      )}
    </>
  );
}

/** The "AI involvement" section of the Trust Report. Self-contained: loads its own data. */
export function AiInvolvementSection({ jobId }: { jobId: string }) {
  const [ledger, setLedger] = useState<AiDecisionLedger | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    let live = true;
    setLedger(null);
    setError("");
    fetchAiDecisions(jobId)
      .then((l) => live && setLedger(l))
      .catch((e: unknown) => live && setError(e instanceof Error ? e.message : "AI decisions could not be loaded"));
    return () => {
      live = false;
    };
  }, [jobId]);
  return (
    <section className="df2-parity-card" aria-label="AI involvement">
      <header className="df2-parity-section-head">
        <h3 className="df2-parity-h3">AI involvement</h3>
      </header>
      {error ? (
        <p className="df2-parity-muted" role="alert">
          {error}
        </p>
      ) : ledger ? (
        <AiInvolvementView ledger={ledger} />
      ) : (
        <LoadingBlock title="Loading AI decisions" size="sm" />
      )}
    </section>
  );
}
