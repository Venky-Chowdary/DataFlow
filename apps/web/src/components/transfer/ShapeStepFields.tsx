import { StudioMultiPicker, StudioPicker } from "../ui/StudioPicker";
import type { StudioPickerOption } from "../../lib/studioPicker";
import {
  fieldsFor,
  linesToList,
  parseNumberOption,
  type ShapeField,
  type ShapeOperation,
} from "../../lib/shape";

interface ShapeStepFieldsProps {
  operation: ShapeOperation;
  columns: string[];
  options: Record<string, unknown>;
  onChange: (options: Record<string, unknown>) => void;
  /** Fired when a field blurs, so undo records a commit and not a keystroke. */
  onCommit?: () => void;
  canPlan: boolean;
  expressionError: string;
  onExpressionChange: (text: string) => void;
  idPrefix: string;
  blankRequired: Set<string>;
}

/**
 * The option fields for one shape operation. The wizard builder and the
 * canvas inspector both render this, so a required field cannot drift.
 */
export function ShapeStepFields({
  operation,
  columns,
  options,
  onChange,
  onCommit,
  canPlan,
  expressionError,
  onExpressionChange,
  idPrefix,
  blankRequired,
}: ShapeStepFieldsProps) {
  const columnOptions: StudioPickerOption[] = columns.map((name) => ({ value: name, label: name }));
  const setField = (name: string, value: unknown) => onChange({ ...options, [name]: value });

  return (
    <>
      {fieldsFor(operation).map((field) => (
        <Field
          key={field.name}
          field={field}
          operation={operation}
          value={options[field.name]}
          invalid={field.required && blankRequired.has(field.name)}
          canPlan={canPlan}
          expressionError={expressionError}
          columnOptions={columnOptions}
          id={`${idPrefix}-${field.name}`}
          onChange={(value) => setField(field.name, value)}
          onExpressionChange={onExpressionChange}
          onCommit={onCommit}
        />
      ))}
    </>
  );
}

function Field({
  field,
  operation,
  value,
  invalid,
  canPlan,
  expressionError,
  columnOptions,
  id,
  onChange,
  onExpressionChange,
  onCommit,
}: {
  field: ShapeField;
  operation: ShapeOperation;
  value: unknown;
  invalid: boolean;
  canPlan: boolean;
  expressionError: string;
  columnOptions: StudioPickerOption[];
  id: string;
  onChange: (value: unknown) => void;
  onExpressionChange: (text: string) => void;
  onCommit?: () => void;
}) {
  if (field.kind === "boolean") {
    return (
      <label key={field.name} className="df2-policy-toggle df2-xform-toggle">
        <input
          type="checkbox"
          checked={value === true}
          disabled={!canPlan}
          onChange={(event) => onChange(event.target.checked)}
          onBlur={onCommit}
        />
        <span><strong>{field.label}</strong><small>{field.hint}</small></span>
      </label>
    );
  }
  if (field.kind === "choice") {
    return (
      <StudioPicker
        id={id}
        label={field.label}
        required={field.required}
        invalid={invalid}
        value={typeof value === "string" ? value : ""}
        options={(field.choices ?? []).map((choice) => ({ value: choice, label: choice }))}
        placeholder="Pick…"
        disabled={!canPlan}
        searchable={(field.choices ?? []).length > 6}
        hint={field.hint}
        onChange={onChange}
      />
    );
  }
  if (field.kind === "columns") {
    return (
      <StudioMultiPicker
        id={id}
        label={field.label}
        required={field.required}
        invalid={invalid}
        value={Array.isArray(value) ? (value as string[]) : []}
        options={columnOptions}
        disabled={!canPlan}
        hint={field.hint}
        onChange={onChange}
      />
    );
  }
  return (
    <div className={`df2-field${invalid ? " is-invalid" : ""}`}>
      <label className="df2-label" htmlFor={id}>
        {field.label}{field.required ? " *" : ""}
      </label>
      {field.kind === "list" ? (
        <textarea
          id={id}
          className="df2-input df2-studio-field"
          rows={3}
          value={Array.isArray(value) ? (value as string[]).join("\n") : ""}
          disabled={!canPlan}
          onBlur={onCommit}
          onChange={(event) => onChange(linesToList(event.target.value))}
        />
      ) : field.kind === "expression" ? (
        <textarea
          id={id}
          className={`df2-input df2-studio-field df2-xform-code${expressionError ? " is-invalid" : ""}`}
          rows={3}
          placeholder="[status] <> 'void'"
          value={typeof value === "string" ? value : ""}
          disabled={!canPlan}
          aria-invalid={expressionError ? true : undefined}
          onBlur={onCommit}
          onChange={(event) => {
            onChange(event.target.value);
            onExpressionChange(event.target.value);
          }}
        />
      ) : (
        <input
          id={id}
          className={`df2-input df2-studio-field${field.name === "format" || field.name === "output_format" || field.name === "characters" ? " df2-xform-code" : ""}`}
          inputMode={field.kind === "number" ? "numeric" : undefined}
          value={value === undefined || value === null ? "" : String(value)}
          disabled={!canPlan}
          onBlur={onCommit}
          onChange={(event) => {
            const raw = event.target.value;
            onChange(field.kind === "number" ? parseNumberOption(raw) : raw);
          }}
        />
      )}
      {field.hint && <span className="df2-label-hint">{field.hint}</span>}
      {invalid && (
        <span className="df2-xform-required" role="alert">
          {field.label} is required for {operation.op}.
        </span>
      )}
    </div>
  );
}
