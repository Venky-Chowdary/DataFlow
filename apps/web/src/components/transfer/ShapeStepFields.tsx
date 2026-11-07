import { useState } from "react";
import { StudioMultiPicker, StudioPicker } from "../ui/StudioPicker";
import type { StudioPickerOption } from "../../lib/studioPicker";
import {
  composeFilterGroup,
  FILTER_OPERATORS,
  filterOperatorNeedsValue,
  parseFilterGroup,
} from "../../lib/filterCondition";
import type { FilterCombinator, FilterConditionParts, FilterGroup } from "../../lib/filterCondition";
import {
  fieldsFor,
  insertExpressionToken,
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
  /** Field, operator, and value for a filter. Off unless the caller asks. */
  conditionBuilder?: boolean;
  /** Insert function and column tokens into an expression. Off unless the caller asks. */
  expressionAssist?: boolean;
  functions?: { name: string }[];
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
  conditionBuilder = false,
  expressionAssist = false,
  functions = [],
}: ShapeStepFieldsProps) {
  const columnOptions: StudioPickerOption[] = columns.map((name) => ({ value: name, label: name }));
  const setField = (name: string, value: unknown) => onChange({ ...options, [name]: value });

  return (
    <>
      {fieldsFor(operation).filter((field) => fieldApplies(operation.op, field.name, options)).map((field) => (
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
          conditionBuilder={conditionBuilder && field.name === "condition"}
          expressionAssist={expressionAssist && field.kind === "expression" && field.name !== "condition"}
          functions={functions}
        />
      ))}
    </>
  );
}

/** Precision, scale, and a zone are meaningful only on the cast that consumes them. */
function fieldApplies(op: string, name: string, options: Record<string, unknown>): boolean {
  if (op !== "cast_column") return true;
  const type = String(options.to_type ?? "");
  if (name === "precision" || name === "scale") {
    return type === "decimal" || (options[name] !== undefined && options[name] !== "");
  }
  if (name === "timezone") {
    return type === "timestamp" || Boolean(String(options.timezone ?? "").trim());
  }
  return true;
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
  conditionBuilder = false,
  expressionAssist = false,
  functions = [],
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
  conditionBuilder?: boolean;
  expressionAssist?: boolean;
  functions?: { name: string }[];
}) {
  if (conditionBuilder && field.kind === "expression") {
    return (
      <FilterConditionBuilder
        id={id}
        field={field}
        value={typeof value === "string" ? value : ""}
        columns={columnOptions}
        canPlan={canPlan}
        invalid={invalid}
        expressionError={expressionError}
        onChange={onChange}
        onExpressionChange={onExpressionChange}
        onCommit={onCommit}
      />
    );
  }
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
        <>
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
          {expressionAssist && (
            <ExpressionAssist
              text={typeof value === "string" ? value : ""}
              columns={columnOptions}
              functions={functions}
              disabled={!canPlan}
              onInsert={(next) => {
                onChange(next);
                onExpressionChange(next);
              }}
            />
          )}
        </>
      ) : field.kind === "json" ? (
        <textarea
          id={id}
          className="df2-input df2-studio-field df2-xform-code"
          rows={4}
          placeholder='{"key":"value"}'
          value={
            typeof value === "string"
              ? value
              : value === undefined || value === null
                ? ""
                : JSON.stringify(value)
          }
          disabled={!canPlan}
          onBlur={onCommit}
          onChange={(event) => onChange(event.target.value)}
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

function ExpressionAssist({
  text,
  columns,
  functions,
  disabled,
  onInsert,
}: {
  text: string;
  columns: StudioPickerOption[];
  functions: { name: string }[];
  disabled: boolean;
  onInsert: (next: string) => void;
}) {
  if (columns.length === 0 && functions.length === 0) return null;
  return (
    <div className="df2-xform-assist" aria-label="Insert into the expression">
      {columns.map((column) => (
        <button
          key={column.value}
          type="button"
          disabled={disabled}
          onClick={() => onInsert(insertExpressionToken(text, `[${column.value}]`))}
        >
          {column.label}
        </button>
      ))}
      {functions.map((fn) => (
        <button
          key={fn.name}
          type="button"
          disabled={disabled}
          onClick={() => onInsert(insertExpressionToken(text, `${fn.name}()`))}
        >
          {fn.name}()
        </button>
      ))}
    </div>
  );
}

export function FilterConditionBuilder({
  id,
  field,
  value,
  columns,
  canPlan,
  invalid,
  expressionError,
  onChange,
  onExpressionChange,
  onCommit,
}: {
  id: string;
  field: ShapeField;
  value: string;
  columns: StudioPickerOption[];
  canPlan: boolean;
  invalid: boolean;
  expressionError: string;
  onChange: (value: unknown) => void;
  onExpressionChange: (text: string) => void;
  onCommit?: () => void;
}) {
  const parsed = parseFilterGroup(value);
  const [advanced, setAdvanced] = useState(parsed == null);
  const group = parsed ?? { combinator: "and" as const, conditions: [{ field: "", operator: "<>", value: "" }] };
  const write = (next: FilterGroup) => {
    const text = composeFilterGroup(next);
    onChange(text);
    onExpressionChange(text);
  };
  const writeCondition = (idx: number, part: FilterConditionParts) =>
    write({ ...group, conditions: group.conditions.map((c, i) => (i === idx ? part : c)) });
  const compound = parsed == null && value.trim().length > 0;
  if (advanced) {
    return (
      <div className="df2-canvas-filter">
        <div className={`df2-field${invalid || expressionError ? " is-invalid" : ""}`}>
          <label className="df2-label" htmlFor={id}>
            {field.label}{field.required ? " *" : ""}
          </label>
          <textarea
            id={id}
            className={`df2-input df2-studio-field df2-xform-code${expressionError ? " is-invalid" : ""}`}
            rows={3}
            placeholder="[status] <> 'void'"
            value={value}
            disabled={!canPlan}
            aria-invalid={expressionError ? true : undefined}
            onBlur={onCommit}
            onChange={(event) => {
              onChange(event.target.value);
              onExpressionChange(event.target.value);
            }}
          />
          {field.hint && <span className="df2-label-hint">{field.hint}</span>}
          {expressionError && <span className="df2-xform-required" role="alert">{expressionError}</span>}
        </div>
        {compound && (
          <span className="df2-label-hint">This expression uses more than one comparison, so it stays in advanced mode.</span>
        )}
        <button
          type="button"
          className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-filter-advanced"
          disabled={compound}
          onClick={() => setAdvanced(false)}
        >
          Use field, operator, and value
        </button>
      </div>
    );
  }
  return (
    <div className="df2-canvas-filter">
      {group.conditions.map((parts, ci) => (
        <div key={ci} className="df2-canvas-filter-row" style={{ display: "grid", gap: 8 }}>
          {ci > 0 && (
            <div className="df2-canvas-filter-join">
              <select
                aria-label={`Join condition ${ci + 1}`}
                className="df2-input df2-input-sm"
                value={group.combinator}
                disabled={!canPlan}
                onChange={(event) =>
                  write({ ...group, combinator: event.target.value as FilterCombinator })
                }
              >
                <option value="and">AND</option>
                <option value="or">OR</option>
              </select>
            </div>
          )}
          <StudioPicker
            id={`${id}-field-${ci}`}
            label="Field"
            required={field.required && ci === 0}
            invalid={invalid && !parts.field}
            value={parts.field}
            options={columns}
            placeholder="Pick a column…"
            disabled={!canPlan}
            onChange={(fieldName) => writeCondition(ci, { ...parts, field: fieldName })}
          />
          <StudioPicker
            id={`${id}-operator-${ci}`}
            label="Operator"
            value={parts.operator}
            options={FILTER_OPERATORS.map((operator) => ({ value: operator.id, label: operator.label }))}
            disabled={!canPlan}
            searchable={false}
            onChange={(operator) => writeCondition(ci, { ...parts, operator })}
          />
          {filterOperatorNeedsValue(parts.operator) && (
            <div className="df2-field">
              <label className="df2-label" htmlFor={`${id}-value-${ci}`}>Value</label>
              <input
                id={`${id}-value-${ci}`}
                className="df2-input df2-studio-field"
                value={parts.value}
                disabled={!canPlan}
                placeholder="Empty matches a blank value"
                onBlur={onCommit}
                onChange={(event) => writeCondition(ci, { ...parts, value: event.target.value })}
              />
            </div>
          )}
          {group.conditions.length > 1 && (
            <button
              type="button"
              className="df2-btn df2-btn-ghost df2-btn-sm"
              disabled={!canPlan}
              onClick={() =>
                write({ ...group, conditions: group.conditions.filter((_, j) => j !== ci) })
              }
            >
              Remove condition
            </button>
          )}
        </div>
      ))}
      <button
        type="button"
        className="df2-btn df2-btn-ghost df2-btn-sm"
        disabled={!canPlan}
        onClick={() =>
          write({
            ...group,
            conditions: [...group.conditions, { field: "", operator: "<>", value: "" }],
          })
        }
      >
        + Add condition ({group.combinator.toUpperCase()})
      </button>
      {field.hint && <span className="df2-label-hint">{field.hint}</span>}
      {expressionError && <span className="df2-xform-required" role="alert">{expressionError}</span>}
      <button
        type="button"
        className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-filter-advanced"
        onClick={() => setAdvanced(true)}
      >
        Advanced expression
      </button>
    </div>
  );
}
