import { TSV_NULL_CHOICES, TSV_NULL_HELP, type TsvNull } from "../../lib/fileExportDestination";

/** "NULL marker" for a TSV file destination (G7-F3): Empty cell (default) or \N. */
export function TsvNullMarkerField({
  id,
  value,
  onChange,
  className = "df2-field",
  compact = false,
}: {
  id: string;
  value: TsvNull;
  onChange: (next: TsvNull) => void;
  className?: string;
  compact?: boolean;
}) {
  return (
    <div className={className}>
      <label className="df2-label" htmlFor={id}>NULL marker</label>
      <select
        id={id}
        className={compact ? "df2-input df2-input-sm" : "df2-input"}
        value={value}
        aria-describedby={`${id}-hint`}
        onChange={(e) => onChange(e.target.value === "\\N" ? "\\N" : "empty")}
      >
        {TSV_NULL_CHOICES.map((choice) => (
          <option key={choice.value} value={choice.value}>{choice.label}</option>
        ))}
      </select>
      <p className="df2-label-hint" id={`${id}-hint`}>{TSV_NULL_HELP}</p>
    </div>
  );
}
