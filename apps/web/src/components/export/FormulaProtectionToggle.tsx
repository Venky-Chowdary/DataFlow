/**
 * "Protect against spreadsheet formulas" (G7-F2) — one control for the Query
 * console export and the Studio file_export destination.
 *
 * The hint sits outside the <label>: the checkbox's accessible name is the
 * short title, and the hint is its description (aria-describedby) — read once,
 * not twice (it used to be inside the label and referenced again).
 */
export function FormulaProtectionToggle({
  id,
  checked,
  onChange,
  help,
  className = "",
}: {
  id: string;
  checked: boolean;
  onChange: (next: boolean) => void;
  help: string;
  className?: string;
}) {
  const hintId = `${id}-hint`;
  return (
    <div className={`df2-formula-toggle ${className}`.trim()}>
      <label className="df2-policy-toggle" htmlFor={id}>
        <input
          id={id}
          type="checkbox"
          checked={checked}
          onChange={(e) => onChange(e.target.checked)}
          aria-describedby={hintId}
        />
        <span>
          <strong>Protect against spreadsheet formulas</strong>
        </span>
      </label>
      <small id={hintId} className="df2-label-hint">{help}</small>
    </div>
  );
}
