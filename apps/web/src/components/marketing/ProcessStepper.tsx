/** The five steps every load runs, named the same way on every page. */
export const PROCESS_STEPS = ["Plan", "Validate", "Execute", "Verify", "Evidence"] as const;
export type ProcessStep = (typeof PROCESS_STEPS)[number];

/**
 * A quiet "you are here" rail: the five steps, with the ones this page is
 * about marked. Text carries the state; colour only reinforces it.
 */
export function ProcessStepper({ current, label = "Where this fits" }: { current: ProcessStep[]; label?: string }) {
  const idx = PROCESS_STEPS.map((s, i) => (current.includes(s) ? i + 1 : 0)).filter(Boolean);
  const range = idx.length === 1 ? `Step ${idx[0]} of 5` : `Steps ${idx[0]}–${idx[idx.length - 1]} of 5`;
  return (
    <div className="mk-stepper">
      <p className="mk-stepper-label">{label}</p>
      <p className="mk-stepper-compact" aria-hidden>
        {range}: {PROCESS_STEPS.filter((s) => current.includes(s)).join(" and ")}
      </p>
      <ol aria-label="The five steps of every load">
        {PROCESS_STEPS.map((step, i) => {
          const on = current.includes(step);
          return (
            <li key={step} className={on ? "is-current" : undefined} aria-current={on ? "step" : undefined}>
              <span className="mk-stepper-num" aria-hidden>{i + 1}</span>
              <span>{step}</span>
              {on ? <span className="mk-sr-only"> (this page)</span> : null}
            </li>
          );
        })}
      </ol>
    </div>
  );
}
