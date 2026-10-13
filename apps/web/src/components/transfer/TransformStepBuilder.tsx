import { useCallback, useMemo, useRef, useState } from "react";
import { DtIcon } from "../DtIcon";
import { StudioMultiPicker, StudioPicker } from "../ui/StudioPicker";
import { checkShapeExpression } from "../../lib/api";
import type { StudioPickerOption } from "../../lib/studioPicker";
import {
  draftOptionsForWire,
  fieldsFor,
  isBlankOption,
  linesToList,
  missingRequired,
  operationsByFamily,
  parseNumberOption,
  settleExpressionCheck,
  type ShapeCatalog,
  type ShapeOperation,
  type ShapeStepWire,
} from "../../lib/shape";

interface TransformStepBuilderProps {
  catalog: ShapeCatalog | null;
  /** Columns as they exist *after* the steps already applied. */
  columns: string[];
  /** False for a viewer: the vocabulary stays readable, applying is plan work. */
  canPlan: boolean;
  disabledReason: string;
  onAdd: (step: ShapeStepWire) => void;
}

/**
 * The one step being composed, kept out of the recipe until it is complete.
 *
 * A half-typed expression must never become part of a recipe identity, so the
 * draft lives here and leaves only through `onAdd` — after the required options
 * are present and the engine itself has accepted the expression.
 */
export function TransformStepBuilder({
  catalog,
  columns,
  canPlan,
  disabledReason,
  onAdd,
}: TransformStepBuilderProps) {
  const [op, setOp] = useState("");
  const [column, setColumn] = useState("");
  const [options, setOptions] = useState<Record<string, unknown>>({});
  const [label, setLabel] = useState("");
  const [policy, setPolicy] = useState("refuse");
  const [error, setError] = useState("");
  const [expressionError, setExpressionError] = useState("");
  const [showFunctions, setShowFunctions] = useState(false);
  const expressionGen = useRef(0);

  const operation: ShapeOperation | undefined = useMemo(
    () => catalog?.operations.find((entry) => entry.op === op),
    [catalog, op],
  );

  const operationOptions = useMemo<StudioPickerOption[]>(
    () => operationsByFamily(catalog?.operations ?? []).flatMap((group) => (
      group.operations.map((entry) => ({
        value: entry.op,
        label: entry.summary,
        hint: entry.op,
        group: group.label,
        meta: entry.expands ? "expands rows" : entry.active ? "moves ledger" : "value",
      }))
    )),
    [catalog],
  );

  const columnOptions = useMemo<StudioPickerOption[]>(
    () => columns.map((name) => ({ value: name, label: name })),
    [columns],
  );

  const policyOptions = useMemo<StudioPickerOption[]>(
    () => (catalog?.error_policies ?? []).map((entry) => ({
      value: entry.value,
      label: entry.label,
      hint: entry.detail,
    })),
    [catalog],
  );

  /**
   * Which required option is still blank, named before the click rather than
   * after it. A step that silently fails to append is worse than a refusal:
   * the operator reads the unchanged preview as "the transform did nothing".
   */
  const missing = useMemo(
    () => (operation ? missingRequired(operation, column, options) : ""),
    [column, operation, options],
  );
  const blankRequired = useMemo(() => {
    if (!operation) return new Set<string>();
    return new Set(
      operation.required.filter((name) => isBlankOption(name, options[name])),
    );
  }, [operation, options]);

  const reset = useCallback(() => {
    expressionGen.current += 1;
    setOp("");
    setColumn("");
    setOptions({});
    setLabel("");
    setPolicy("refuse");
    setError("");
    setExpressionError("");
  }, []);

  const validateExpression = useCallback(async (text: string) => {
    const requestId = ++expressionGen.current;
    if (!text.trim()) {
      setExpressionError("");
      return;
    }
    try {
      const answer = await checkShapeExpression({ expression: text, source_columns: columns });
      const next = settleExpressionCheck(requestId, expressionGen.current, answer);
      if (next !== undefined) setExpressionError(next);
    } catch (err) {
      const next = settleExpressionCheck(requestId, expressionGen.current, {
        error: err instanceof Error ? err.message : String(err),
      });
      if (next !== undefined) setExpressionError(next);
    }
  }, [columns]);

  const add = useCallback(() => {
    if (!operation) {
      setError("Pick an operation.");
      return;
    }
    const missing = missingRequired(operation, column, options);
    if (missing) {
      setError(missing);
      return;
    }
    if (expressionError) {
      setError(expressionError);
      return;
    }
    const step: ShapeStepWire = { op: operation.op, options: draftOptionsForWire(options) };
    if (operation.needs_column) step.column = column;
    if (label.trim()) step.label = label.trim();
    if (policy !== "refuse") step.on_error = policy;
    onAdd(step);
    reset();
  }, [column, expressionError, label, onAdd, operation, options, policy, reset]);

  // A viewer is told it may read the vocabulary, so it is rendered as a list.
  // The same operations inside disabled selects are unreadable — a disabled
  // select cannot be opened — which would make the refusal text a false promise.
  if (!canPlan) {
    return (
      <div className="df2-xform-builder">
        <p className="df2-xform-builder-kicker">Engine vocabulary</p>
        <p className="df2-label-hint">{disabledReason}</p>
        <ul className="df2-xform-vocab">
          {(catalog?.operations ?? []).map((entry) => (
            <li key={entry.op}>
              <code>{entry.op}</code>
              <span>{entry.summary}</span>
              <small>
                {entry.active
                  ? "changes the row count, so it moves the ledger"
                  : "value-only, the row count is unchanged"}
              </small>
            </li>
          ))}
        </ul>
      </div>
    );
  }

  return (
    <div className="df2-xform-builder">
      <div className="df2-xform-builder-intro">
        <p className="df2-xform-builder-kicker">Compose a step</p>
        <p className="df2-xform-builder-lead">
          Search the engine vocabulary. The step joins the recipe only when every required
          field is filled and any expression compiles.
        </p>
      </div>
      <div className="df2-xform-builder-row">
        <div className="df2-xform-field-op">
          <StudioPicker
            id="xform-op"
            label="Operation"
            value={op}
            options={operationOptions}
            placeholder="Search trim, parse date, filter…"
            disabled={!canPlan || !catalog}
            emptyHint="No operation matches that search."
            onChange={(next) => {
              expressionGen.current += 1;
              setOp(next);
              setOptions({});
              setError("");
              setExpressionError("");
            }}
            hint={operation
              ? `${operation.op}${operation.expands
                ? " · adds rows (unnest) — dest COUNT is the expanded image, not a surplus"
                : operation.active
                  ? " · changes the row count, so it moves the ledger"
                  : " · value-only, the row count is unchanged"}`
              : "Grouped the way the engine catalogues them — values, columns, rows, nested JSON."}
          />
        </div>
        {operation?.needs_column && (
          <StudioPicker
            id="xform-column"
            label="Column"
            value={column}
            options={columnOptions}
            placeholder="Search a column…"
            disabled={!canPlan}
            required
            emptyHint="No column matches that search."
            onChange={(next) => { setColumn(next); setError(""); }}
          />
        )}
        {operation && (
          <StudioPicker
            id="xform-policy"
            label="If a value cannot be computed"
            value={policy}
            options={policyOptions}
            disabled={!canPlan}
            searchable={false}
            onChange={setPolicy}
            hint={catalog?.error_policies.find((entry) => entry.value === policy)?.detail ?? ""}
          />
        )}
      </div>

      {operation && (
        <div className="df2-xform-builder-row">
          {fieldsFor(operation).map((field) => {
            const id = `xform-opt-${field.name}`;
            const value = options[field.name];
            if (field.kind === "boolean") {
              return (
                <label key={field.name} className="df2-policy-toggle df2-xform-toggle">
                  <input
                    type="checkbox"
                    checked={value === true}
                    disabled={!canPlan}
                    onChange={(e) => setOptions({ ...options, [field.name]: e.target.checked })}
                  />
                  <span><strong>{field.label}</strong><small>{field.hint}</small></span>
                </label>
              );
            }
            const invalid = field.required && blankRequired.has(field.name);
            if (field.kind === "choice") {
              return (
                <StudioPicker
                  key={field.name}
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
                  onChange={(next) => setOptions({ ...options, [field.name]: next })}
                />
              );
            }
            if (field.kind === "columns") {
              return (
                <StudioMultiPicker
                  key={field.name}
                  id={id}
                  label={field.label}
                  required={field.required}
                  invalid={invalid}
                  value={Array.isArray(value) ? (value as string[]) : []}
                  options={columnOptions}
                  disabled={!canPlan}
                  hint={field.hint}
                  onChange={(next) => setOptions({ ...options, [field.name]: next })}
                />
              );
            }
            return (
              <div
                className={`df2-field${invalid ? " is-invalid" : ""}`}
                key={field.name}
              >
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
                    onChange={(e) => setOptions({ ...options, [field.name]: linesToList(e.target.value) })}
                  />
                ) : field.kind === "expression" ? (
                  <textarea
                    id={id}
                    className={`df2-input df2-studio-field df2-xform-code${expressionError ? " is-invalid" : ""}`}
                    rows={3}
                    placeholder="[status] <> 'void'"
                    value={typeof value === "string" ? value : ""}
                    disabled={!canPlan}
                    onChange={(e) => {
                      const text = e.target.value;
                      setOptions({ ...options, [field.name]: text });
                      void validateExpression(text);
                    }}
                  />
                ) : (
                  <input
                    id={id}
                    className={`df2-input df2-studio-field${field.name === "format" || field.name === "output_format" || field.name === "characters" ? " df2-xform-code" : ""}`}
                    inputMode={field.kind === "number" ? "numeric" : undefined}
                    value={value === undefined || value === null ? "" : String(value)}
                    disabled={!canPlan}
                    onChange={(e) => {
                      const raw = e.target.value;
                      setOptions({
                        ...options,
                        [field.name]: field.kind === "number" ? parseNumberOption(raw) : raw,
                      });
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
          })}
          <div className="df2-field">
            <label className="df2-label" htmlFor="xform-label">Step name (optional)</label>
            <input
              id="xform-label"
              className="df2-input df2-studio-field"
              value={label}
              disabled={!canPlan}
              placeholder="Tidy customer names"
              onChange={(e) => setLabel(e.target.value)}
            />
          </div>
        </div>
      )}

      {expressionError && (
        <div className="df2-alert df2-alert-error" role="alert">
          <DtIcon name="x" size={16} />
          <div><p>{expressionError}</p></div>
        </div>
      )}
      {error && (
        <div className="df2-alert df2-alert-warn" role="alert">
          <DtIcon name="alert" size={16} />
          <div><p>{error}</p></div>
        </div>
      )}

      <div className="df2-xform-builder-actions">
        <button
          type="button"
          className="df2-btn df2-btn-primary df2-btn-sm"
          disabled={!canPlan || !operation || Boolean(missing) || Boolean(expressionError)}
          title={disabledReason || missing || expressionError || "Append this step to the recipe"}
          onClick={add}
        >
          <DtIcon name="plus" size={14} /> Add step
        </button>
        {op && (
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={reset}>
            Clear
          </button>
        )}
        {missing && !expressionError && (
          <span className="df2-xform-required df2-xform-builder-why">{missing}</span>
        )}
        {catalog && (
          <button
            type="button"
            className="df2-btn df2-btn-ghost df2-btn-sm"
            aria-expanded={showFunctions}
            onClick={() => setShowFunctions((open) => !open)}
          >
            <DtIcon name="book" size={14} /> {showFunctions ? "Hide expression help" : "Expression help"}
          </button>
        )}
      </div>

      {showFunctions && catalog && (
        <div className="df2-xform-functions">
          <p>
            Columns are written <code>[column name]</code>. Arithmetic is decimal, never binary
            float. There is no clock, no randomness and no SQL — the same row always yields the same
            answer, which is what lets Execute be held to this recipe's identity.
          </p>
          <ul>
            {catalog.functions.map((fn) => (
              <li key={fn.name}>
                <code>{fn.name}</code>
                <span>{fn.summary}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
