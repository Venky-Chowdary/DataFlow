/**
 * The verdict chip: one component for the Trust Report's five verdicts and the
 * two waiting states (lib/verdict.ts). Icon + word, a hairline in the verdict's
 * colour and a faint tint; the word is always primary ink, so it reads AA in
 * both themes, and the icon carries the colour (never colour alone).
 */
import { DtIcon } from "../DtIcon";
import { verdictCountSuffix, verdictPresentation, type VerdictState } from "../../lib/verdict";

export interface VerdictChipProps {
  state: VerdictState;
  /** Rows no rule explains; shown as "Unexplained · 4 rows". */
  unexplainedRows?: number | null;
  /** Words read before the verdict, e.g. "Verification:" (visible). */
  prefix?: string;
  /** Overrides the one-line meaning in the tooltip (e.g. the API's reason). */
  title?: string;
  className?: string;
}

export function VerdictChip({ state, unexplainedRows, prefix, title, className }: VerdictChipProps) {
  const shown = verdictPresentation(state);
  const suffix = verdictCountSuffix(state, unexplainedRows);
  return (
    <span
      className={["df2-verdict", className].filter(Boolean).join(" ")}
      data-tone={shown.tone}
      data-verdict={state}
      title={title ?? shown.meaning}
    >
      <DtIcon
        name={shown.icon}
        size={14}
        chrome="product"
        className={state === "checking" ? "df2-verdict-icon is-spinning" : "df2-verdict-icon"}
      />
      {prefix && <span className="df2-verdict-prefix">{prefix}</span>}
      <span className="df2-verdict-word">{shown.word}</span>
      {suffix && <span className="df2-verdict-count">· {suffix}</span>}
    </span>
  );
}
