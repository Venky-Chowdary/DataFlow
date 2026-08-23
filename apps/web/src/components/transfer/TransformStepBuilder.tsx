import { useCallback, useMemo, useState } from "react";
import { DtIcon } from "../DtIcon";
import { checkShapeExpression } from "../../lib/api";
import {
  fieldsFor,
  linesToList,
  missingRequired,
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

  const operation: ShapeOperation | undefined = useMemo(
    () => catalog?.operations.find((entry) => entry.op === op),
    [catalog, op],
  );

  const reset = useCallback(() => {
    setOp("");
    setColumn("");
    setOptions({});
    setLabel("");
    setPolicy("refuse");
    setError("");
    setExpressionError("");
  }, []);

  const validateExpression = useCallback(async (text: string) => {
    if (!text.trim()) {
      setExpressionError("");
      return;
    }
    try {
      const answer = await checkShapeExpression({ expression: text, source_columns: columns });
      setExpressionError(answer.valid ? "" : (answer.error ?? "Expression is not valid."));
    } catch (err) {
      setExpressionError(err instanceof Error ? err.message : String(err));
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
    const step: ShapeStepWire = { op: operation.op, options };
    if (operation.needs_column) step.column = column;
    if (label.trim()) step.label = label.trim();
    if (policy !== "refuse") step.on_error = policy;
    onAdd(step);
    reset();
  }, [column, expressionError, label, onAdd, operation, options, policy, reset]);

  return (
    <div className="df2-xform-builder">
      <div className="df2-xform-builder-row">
        <div className="df2-field df2-xform-field-op">
          <label className="df2-label" htmlFor="xform-op">Operation</label>
          <select
            id="xform-op"
            className="df2-input df2-select"
            value={op}
            disabled={!canPlan || !catalog}
            onChange={(e) => { setOp(e.target.value); setOptions({}); setError(""); setExpressionError(""); }}
          >
            <option value="">Pick an operation…</option>
            {(catalog?.operations ?? []).map((entry) => (
              <option key={entry.op} value={entry.op}>{entry.summary}</option>
            ))}
          </select>
          {operation && (
            <span className="df2-label-hint">
              <code>{operation.op}</code>
              {operation.active ? " · changes the row count, so it moves the ledger" : " · value-only, the row count is unchanged"}
            </span>
          )}
        </div>
        {operation?.needs_column && (
          <div className="df2-field">
            <label className="df2-label" htmlFor="xform-column">Column</label>
            <select
              id="xform-column"
              className="df2-input df2-select"
              value={column}
              disabled={!canPlan}
              onChange={(e) => { setColumn(e.target.value); setError(""); }}
            >
              <option value="">Pick a column…</option>
              {columns.map((name) => <option key={name} value={name}>{name}</option>)}
            </select>
          </div>
        )}
        {operation && (
          <div className="df2-field">
            <label className="df2-label" htmlFor="xform-policy">If a value cannot be computed</label>
            <select
              id="xform-policy"
              className="df2-input df2-select"
              value={policy}
              disabled={!canPlan}
              onChange={(e) => setPolicy(e.target.value)}
            >
              {(catalog?.error_policies ?? []).map((entry) => (
                <option key={entry.value} value={entry.value}>{entry.label}</option>
              ))}
            </select>
            <span className="df2-label-hint">
              {catalog?.error_policies.find((entry) => entry.value === policy)?.detail ?? ""}
            </span>
          </div>
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
            return (
              <div className="df2-field" key={field.name}>
                <label className="df2-label" htmlFor={id}>
                  {field.label}{field.required ? " *" : ""}
                </label>
                {field.kind === "choice" ? (
                  <select
                    id={id}
                    className="df2-input df2-select"
                    value={typeof value === "string" ? value : ""}
                    disabled={!canPlan}
                    onChange={(e) => setOptions({ ...options, [field.name]: e.target.value })}
                  >
                    <option value="">Pick…</option>
                    {(field.choices ?? []).map((choice) => (
                      <option key={choice} value={choice}>{choice}</option>
                    ))}
                  </select>
                ) : field.kind === "columns" ? (
                  <select
                    id={id}
                    multiple
                    className="df2-input df2-select df2-xform-multi"
                    value={Array.isArray(value) ? (value as string[]) : []}
                    disabled={!canPlan}
                    onChange={(e) => setOptions({
                      ...options,
                      [field.name]: Array.from(e.target.selectedOptions, (o) => o.value),
                    })}
                  >
                    {columns.map((name) => <option key={name} value={name}>{name}</option>)}
                  </select>
                ) : field.kind === "list" ? (
                  <textarea
                    id={id}
                    className="df2-input"
                    rows={3}
                    value={Array.isArray(value) ? (value as string[]).join("\n") : ""}
                    disabled={!canPlan}
                    onChange={(e) => setOptions({ ...options, [field.name]: linesToList(e.target.value) })}
                  />
                ) : field.kind === "expression" ? (
                  <textarea
                    id={id}
                    className={`df2-input df2-xform-code${expressionError ? " is-invalid" : ""}`}
                    rows={2}
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
                    className="df2-input"
                    inputMode={field.kind === "number" ? "numeric" : undefined}
                    value={value === undefined || value === null ? "" : String(value)}
                    disabled={!canPlan}
                    onChange={(e) => {
                      const raw = e.target.value;
                      setOptions({
                        ...options,
                        [field.name]: field.kind === "number"
                          ? (raw.trim() === "" ? "" : Number(raw))
                          : raw,
                      });
                    }}
                  />
                )}
                {field.hint && <span className="df2-label-hint">{field.hint}</span>}
              </div>
            );
          })}
          <div className="df2-field">
            <label className="df2-label" htmlFor="xform-label">Step name (optional)</label>
            <input
              id="xform-label"
              className="df2-input"
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
          disabled={!canPlan || !operation || Boolean(expressionError)}
          title={disabledReason || "Append this step to the recipe"}
          onClick={add}
        >
          <DtIcon name="plus" size={14} /> Add step
        </button>
        {op && (
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={reset}>
            Clear
          </button>
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
