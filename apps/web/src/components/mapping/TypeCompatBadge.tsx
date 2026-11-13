import { useId } from "react";
import type { TypeGrade } from "../../lib/mappingStudioApi";

const LABEL: Record<string, string> = {
  safe: "Safe",
  lossy: "Lossy",
  incompatible: "Incompatible",
  unknown: "Unread",
};

const KIND_LABEL: Record<string, string> = {
  narrowing: "Narrowing",
  precision: "Precision",
  timezone: "Time zone",
  domain: "Value domain",
  incompatible: "Incompatible",
  unknown: "Unread",
  safe: "Safe",
};

/** One sentence for a screen reader and the tooltip. */
export function compatSentence(grade: TypeGrade): string {
  const head = `${grade.sourceType} → ${grade.targetType || "?"}: ${LABEL[grade.verdict] ?? grade.verdict}.`;
  const why = grade.reasons.map((r) => r.message).filter(Boolean).join(" ");
  return why ? `${head} ${why}` : head;
}

/**
 * Live type-compatibility chip for one mapping row.
 *
 * The verdict and reasons come from the API's grader — the conversion
 * contract and fidelity verdict Validate uses — never from client rules.
 * Focusable so keyboard users reach the tooltip; the reasons are also the
 * chip's accessible description.
 */
export function TypeCompatBadge({ grade, pending = false }: { grade?: TypeGrade; pending?: boolean }) {
  const tipId = useId();
  if (!grade) {
    return pending ? (
      <span className="df2-ms-compat is-pending" aria-label="Checking type compatibility">
        Checking…
      </span>
    ) : null;
  }
  const kinds = Array.from(new Set(grade.reasons.map((r) => r.kind))).filter(
    (k) => k !== "safe" && k !== grade.verdict,
  );
  return (
    <span className="df2-ms-compat-wrap">
      <span
        className={`df2-ms-compat is-${grade.verdict}`}
        // A focusable chip, not a control: it only discloses the reasons.
        tabIndex={0}
        role="note"
        aria-label={`Type check ${LABEL[grade.verdict] ?? grade.verdict}`}
        aria-describedby={tipId}
        data-verdict={grade.verdict}
      >
        <span aria-hidden="true" className="df2-ms-compat-dot" />
        {LABEL[grade.verdict] ?? grade.verdict}
        {kinds.length ? <span className="df2-ms-compat-kind"> · {kinds.map((k) => KIND_LABEL[k] ?? k).join(", ")}</span> : null}
      </span>
      <span id={tipId} role="tooltip" className="df2-ms-compat-tip">
        {compatSentence(grade)}
      </span>
    </span>
  );
}
