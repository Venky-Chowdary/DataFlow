import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DtIcon } from "../../components/DtIcon";
import {
  checkShapeExpression,
  fetchShapeCatalog,
  previewShapeRecipe,
  profileShapeSource,
} from "../../lib/api";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import {
  changedCellIndex,
  describeStep,
  fieldsFor,
  linesToList,
  missingRequired,
  moveStep,
  removeStep,
  sortSuggestions,
  summarizeEffect,
  toggleStep,
  type ShapeCatalog,
  type ShapeColumnProfile,
  type ShapeOperation,
  type ShapePreviewResponse,
  type ShapeProfileResponse,
  type ShapeStepWire,
  type ShapeSuggestion,
} from "../../lib/shape";

interface TransferShapeStepProps {
  /** Sampled source rows already held by the studio — no connector round-trip. */
  sampleRows: Record<string, unknown>[];
  sourceColumns: string[];
  /** Declared destination carriers, so a narrowing decimal is caught here. */
  targetSchema: Record<string, string>;
  sourceLabel: string;
  destRouteLabel: string;
  /** Total source population, so the sample notice can state what it is not. */
  rowCount?: number;
  steps: ShapeStepWire[];
  onChangeSteps: (steps: ShapeStepWire[]) => void;
  /** Recipe identity and shaped column set, for Map and the run request. */
  onIdentity: (identity: { hash: string; columns: string[] } | null) => void;
  onBack: () => void;
  onContinue: () => void;
}

const PREVIEW_ROWS = 12;
const PREVIEW_DEBOUNCE_MS = 250;

/** Severity → the badge class the rest of the studio already uses. */
function severityClass(severity: string): string {
  if (severity === "blocking") return "df2-badge df2-badge-error";
  if (severity === "decision") return "df2-badge df2-badge-warn";
  return "df2-badge";
}

function cellText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

/** What the profile found worth saying about one column, in one line. */
function profileNotes(profile: ShapeColumnProfile): string {
  const notes: string[] = [];
  if (profile.blanks) notes.push(`${profile.blanks} blank`);
  if (profile.untrimmed) notes.push(`${profile.untrimmed} padded`);
  if (profile.inner_whitespace) notes.push(`${profile.inner_whitespace} inner space`);
  const sentinels = Object.entries(profile.sentinels);
  if (sentinels.length) {
    notes.push(sentinels.map(([token, count]) => `${count}× ${token}`).join(", "));
  }
  if (profile.non_printable) notes.push(`${profile.non_printable} control char`);
  if (profile.unnormalized_unicode) notes.push(`${profile.unnormalized_unicode} un-normalised`);
  if (profile.max_scale) notes.push(`scale ${profile.max_scale}`);
  if (profile.ambiguous_date_order) notes.push("ambiguous date order");
  return notes.join(" · ");
}

/**
 * Shape — prepare the raw source before Map decides carriers and Validate scans.
 *
 * The panel is an Applied Steps list over a live before/after preview, the shape
 * Power Query and Alteryx taught operators to expect, with three things they do
 * not have: every step states what it did to the sample (cells changed, nulls
 * introduced, rows shaped out), the recipe carries an identity that Execute is
 * held to, and an operation that cannot be honoured on a stream is refused by
 * name here instead of failing at row 431.
 *
 * Nothing in this panel mutates the source. The recipe travels with the plan and
 * is re-applied deterministically at Execute; this screen is where the operator
 * decides what it should say.
 */
export function TransferShapeStep({
  sampleRows,
  sourceColumns,
  targetSchema,
  sourceLabel,
  destRouteLabel,
  rowCount,
  steps,
  onChangeSteps,
  onIdentity,
  onBack,
  onContinue,
}: TransferShapeStepProps) {
  const plan = useWriteGate(PERMISSIONS.jobPlan);
  const [catalog, setCatalog] = useState<ShapeCatalog | null>(null);
  const [catalogError, setCatalogError] = useState("");
  const [profile, setProfile] = useState<ShapeProfileResponse | null>(null);
  const [profileError, setProfileError] = useState("");
  const [preview, setPreview] = useState<ShapePreviewResponse | null>(null);
  const [previewError, setPreviewError] = useState("");
  const [busy, setBusy] = useState(false);
  const [showProfile, setShowProfile] = useState(false);
  const [showFunctions, setShowFunctions] = useState(false);

  // The step being composed. Kept out of the recipe until it is complete, so a
  // half-typed expression never becomes part of an identity.
  const [draftOp, setDraftOp] = useState("");
  const [draftColumn, setDraftColumn] = useState("");
  const [draftOptions, setDraftOptions] = useState<Record<string, unknown>>({});
  const [draftLabel, setDraftLabel] = useState("");
  const [draftPolicy, setDraftPolicy] = useState("refuse");
  const [draftError, setDraftError] = useState("");
  const [expressionError, setExpressionError] = useState("");

  const rowsKey = useMemo(() => JSON.stringify(sampleRows.slice(0, 200)), [sampleRows]);
  const stepsKey = useMemo(() => JSON.stringify(steps), [steps]);
  const schemaKey = useMemo(() => JSON.stringify(targetSchema), [targetSchema]);
  const shapedColumns = preview?.recipe.output_columns ?? sourceColumns;

  useEffect(() => {
    let cancelled = false;
    fetchShapeCatalog()
      .then((next) => { if (!cancelled) setCatalog(next); })
      .catch((err) => { if (!cancelled) setCatalogError(err instanceof Error ? err.message : String(err)); });
    return () => { cancelled = true; };
  }, []);

  // Profiling and previewing are plan-time work. A viewer may read the operation
  // vocabulary — it is real, and refusing it would render an empty screen — but
  // the API refuses the design calls, so they are not attempted.
  useEffect(() => {
    if (!plan.allowed || !sampleRows.length) return;
    let cancelled = false;
    profileShapeSource({
      sample_rows: sampleRows.slice(0, 200),
      source_columns: sourceColumns,
      target_schema: targetSchema,
    })
      .then((next) => { if (!cancelled) { setProfile(next); setProfileError(""); } })
      .catch((err) => { if (!cancelled) setProfileError(err instanceof Error ? err.message : String(err)); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [plan.allowed, rowsKey, schemaKey, sourceColumns.join("|")]);

  const timer = useRef<number | null>(null);
  useEffect(() => {
    if (!plan.allowed || !sampleRows.length) return;
    if (timer.current !== null) window.clearTimeout(timer.current);
    let cancelled = false;
    timer.current = window.setTimeout(() => {
      setBusy(true);
      previewShapeRecipe({
        recipe: { steps },
        sample_rows: sampleRows.slice(0, 200),
        source_columns: sourceColumns,
        target_schema: targetSchema,
      })
        .then((next) => {
          if (cancelled) return;
          setPreview(next);
          setPreviewError("");
          onIdentity({ hash: next.recipe.recipe_hash, columns: next.recipe.output_columns });
        })
        .catch((err) => {
          if (cancelled) return;
          setPreview(null);
          setPreviewError(err instanceof Error ? err.message : String(err));
          // A recipe the engine refuses has no identity to approve.
          onIdentity(null);
        })
        .finally(() => { if (!cancelled) setBusy(false); });
    }, PREVIEW_DEBOUNCE_MS);
    return () => {
      cancelled = true;
      if (timer.current !== null) window.clearTimeout(timer.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [plan.allowed, rowsKey, stepsKey, schemaKey, sourceColumns.join("|")]);

  const operation: ShapeOperation | undefined = useMemo(
    () => catalog?.operations.find((op) => op.op === draftOp),
    [catalog, draftOp],
  );
  const operationsByName = useMemo(() => {
    const index = new Map<string, ShapeOperation>();
    for (const op of catalog?.operations ?? []) index.set(op.op, op);
    return index;
  }, [catalog]);

  const resetDraft = useCallback(() => {
    setDraftOp("");
    setDraftColumn("");
    setDraftOptions({});
    setDraftLabel("");
    setDraftPolicy("refuse");
    setDraftError("");
    setExpressionError("");
  }, []);

  const addStep = useCallback(() => {
    if (!operation) {
      setDraftError("Pick an operation.");
      return;
    }
    const missing = missingRequired(operation, draftColumn, draftOptions);
    if (missing) {
      setDraftError(missing);
      return;
    }
    if (expressionError) {
      setDraftError(expressionError);
      return;
    }
    const step: ShapeStepWire = { op: operation.op, options: draftOptions };
    if (operation.needs_column) step.column = draftColumn;
    if (draftLabel.trim()) step.label = draftLabel.trim();
    if (draftPolicy !== "refuse") step.on_error = draftPolicy;
    onChangeSteps([...steps, step]);
    resetDraft();
  }, [draftColumn, draftLabel, draftOptions, draftPolicy, expressionError, onChangeSteps, operation, resetDraft, steps]);

  const applySuggestion = useCallback((suggestion: ShapeSuggestion) => {
    onChangeSteps([...steps, suggestion.step]);
  }, [onChangeSteps, steps]);

  const validateExpression = useCallback(async (name: string, text: string) => {
    if (!text.trim()) {
      setExpressionError("");
      return;
    }
    try {
      const answer = await checkShapeExpression({
        expression: text,
        source_columns: shapedColumns,
      });
      setExpressionError(answer.valid ? "" : (answer.error ?? "Expression is not valid."));
    } catch (err) {
      setExpressionError(err instanceof Error ? err.message : String(err));
    }
    void name;
  }, [shapedColumns]);

  const suggestions = useMemo(
    () => sortSuggestions(preview?.suggestions?.length ? preview.suggestions : (profile?.suggestions ?? [])),
    [preview, profile],
  );
  const appliedIds = useMemo(
    () => new Set(steps.map((step) => `${step.op}:${step.column ?? ""}`)),
    [steps],
  );
  const openSuggestions = suggestions.filter((s) => !appliedIds.has(`${s.step.op}:${s.step.column ?? ""}`));

  const effect = preview?.effect ?? null;
  const changed = useMemo(() => changedCellIndex(preview?.changed_cells ?? []), [preview]);
  const beforeRows = preview?.before?.slice(0, PREVIEW_ROWS) ?? sampleRows.slice(0, PREVIEW_ROWS);
  const afterRows = preview?.after?.slice(0, PREVIEW_ROWS) ?? [];
  const columnsBefore = sourceColumns.length ? sourceColumns : Object.keys(beforeRows[0] ?? {});
  const columnsAfter = shapedColumns.length ? shapedColumns : columnsBefore;

  return (
    <div className="df2-shape-step">
      <header className="df2-shape-head">
        <div>
          <h2 className="df2-step-title">Shape the source before it is mapped</h2>
          <p className="df2-label-hint">
            {sourceLabel} → {destRouteLabel}. Steps run on the read, in order, row by row — the source
            file or table is never modified. Map, Validate and the writer all see the shaped columns.
          </p>
        </div>
        <div className="df2-shape-identity">
          {preview ? (
            <>
              <span className="df2-badge df2-badge-live" title="Pinned at approval and re-checked before Execute">
                recipe {preview.recipe.recipe_hash}
              </span>
              <span className="df2-label-hint">{preview.recipe.summary}</span>
            </>
          ) : (
            <span className="df2-label-hint">{busy ? "Previewing…" : "No shaping declared"}</span>
          )}
        </div>
      </header>

      {!plan.allowed && (
        <div className="df2-alert df2-alert-info" role="status">
          <DtIcon name="lock" size={16} />
          <div>
            <p>{plan.reason}</p>
            <p className="df2-label-hint">
              The operations below are the real vocabulary this engine accepts. You can read them; applying
              one is plan work.
            </p>
          </div>
        </div>
      )}

      {catalogError && (
        <div className="df2-alert df2-alert-error" role="alert">
          <DtIcon name="x" size={16} />
          <div><p>{catalogError}</p></div>
        </div>
      )}
      {profileError && (
        <div className="df2-alert df2-alert-warn" role="status">
          <DtIcon name="alert" size={16} />
          <div><p>Profile unavailable: {profileError}</p></div>
        </div>
      )}
      {previewError && (
        <div className="df2-alert df2-alert-error" role="alert">
          <DtIcon name="x" size={16} />
          <div>
            <p>{previewError}</p>
            <p className="df2-label-hint">The recipe is refused, so it has no identity to approve. Fix or remove the step.</p>
          </div>
        </div>
      )}
      {preview?.refusal && (
        <div className="df2-alert df2-alert-error" role="alert">
          <DtIcon name="x" size={16} />
          <div>
            <p>
              Step {preview.refusal.step} ({preview.refusal.op}) refused row {preview.refusal.row}
              {preview.refusal.column ? ` on ${preview.refusal.column}` : ""}: {preview.refusal.message}
            </p>
            <p className="df2-label-hint">
              This row stops the run. Change the step's error policy to divert or null if that is the decision
              you want, or fix the value at source.
            </p>
          </div>
        </div>
      )}

      <div className="df2-shape-body">
        <section className="df2-shape-pane">
          <h3 className="df2-pane-title">
            What the sample shows
            {profile ? (
              <span className="df2-label-hint">
                {profile.sampled_rows.toLocaleString()} sampled row(s)
                {rowCount ? ` of ${rowCount.toLocaleString()}` : ""}
              </span>
            ) : null}
          </h3>
          {profile?.sample_notice && <p className="df2-label-hint">{profile.sample_notice}</p>}

          {openSuggestions.length === 0 && profile && (
            <p className="df2-label-hint">
              Nothing in the sample needs shaping. Validate still re-checks the whole population.
            </p>
          )}
          <ul className="df2-shape-suggestions">
            {openSuggestions.map((suggestion) => (
              <li key={suggestion.id}>
                <div className="df2-shape-suggestion-head">
                  <span className={severityClass(suggestion.severity)}>{suggestion.severity}</span>
                  <strong>{suggestion.title}</strong>
                </div>
                <p className="df2-label-hint">{suggestion.reason}</p>
                <div className="df2-shape-suggestion-foot">
                  <span className="df2-label-hint">
                    {suggestion.rows_affected.toLocaleString()} sampled row(s) affected
                  </span>
                  <button
                    type="button"
                    className="df2-btn df2-btn-sm"
                    disabled={!plan.allowed}
                    title={plan.reason || "Add this step to the recipe"}
                    onClick={() => applySuggestion(suggestion)}
                  >
                    Add step
                  </button>
                </div>
              </li>
            ))}
          </ul>

          {profile && (
            <details className="df2-shape-profile" open={showProfile} onToggle={(e) => setShowProfile((e.target as HTMLDetailsElement).open)}>
              <summary>Column profile ({profile.columns.length})</summary>
              <table className="df2-table df2-table-compact">
                <thead>
                  <tr><th>Column</th><th>Reads as</th><th>Distinct</th><th>Findings</th></tr>
                </thead>
                <tbody>
                  {profile.columns.map((column) => (
                    <tr key={column.name}>
                      <td>{column.name}</td>
                      <td>{column.logical_type}</td>
                      <td>{column.distinct.toLocaleString()}{column.distinct_capped ? "+" : ""}</td>
                      <td className="df2-label-hint">{profileNotes(column) || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </details>
          )}
        </section>

        <section className="df2-shape-pane df2-shape-pane-wide">
          <h3 className="df2-pane-title">
            Applied steps
            <span className="df2-label-hint">
              {steps.length}
              {catalog ? ` of ${catalog.max_steps} allowed` : ""}
            </span>
          </h3>

          {steps.length === 0 ? (
            <p className="df2-label-hint">
              No steps yet. The source passes through unchanged, which is exactly today's behaviour.
            </p>
          ) : (
            <ol className="df2-shape-steps">
              {steps.map((step, index) => {
                const stepEffect = effect?.steps?.[index];
                const disabled = step.enabled === false;
                return (
                  <li key={`${step.op}:${index}`} className={disabled ? "is-disabled" : ""}>
                    <div className="df2-shape-step-head">
                      <span className="df2-shape-step-index">{index + 1}</span>
                      <strong>{describeStep(step, operationsByName.get(step.op))}</strong>
                      {step.on_error && step.on_error !== "refuse" && (
                        <span className="df2-badge">on error: {step.on_error}</span>
                      )}
                      {operationsByName.get(step.op)?.active && (
                        <span className="df2-badge df2-badge-warn" title="Changes the row count, so it moves the conservation ledger">
                          moves the ledger
                        </span>
                      )}
                    </div>
                    <p className="df2-label-hint">
                      {stepEffect
                        ? [
                            `${stepEffect.rows_in.toLocaleString()} in`,
                            `${stepEffect.rows_out.toLocaleString()} out`,
                            stepEffect.cells_changed ? `${stepEffect.cells_changed.toLocaleString()} cell(s) changed` : "",
                            stepEffect.rows_removed ? `${stepEffect.rows_removed.toLocaleString()} shaped out` : "",
                            stepEffect.rows_diverted ? `${stepEffect.rows_diverted.toLocaleString()} diverted` : "",
                            stepEffect.nulls_introduced ? `${stepEffect.nulls_introduced.toLocaleString()} null(s) introduced` : "",
                          ].filter(Boolean).join(" · ")
                        : disabled
                          ? "Disabled — excluded from the recipe and its identity."
                          : "Not yet measured."}
                    </p>
                    <div className="df2-shape-step-controls">
                      <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" disabled={!plan.allowed || index === 0}
                        title="Move earlier" onClick={() => onChangeSteps(moveStep(steps, index, -1))}>↑</button>
                      <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" disabled={!plan.allowed || index === steps.length - 1}
                        title="Move later" onClick={() => onChangeSteps(moveStep(steps, index, 1))}>↓</button>
                      <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" disabled={!plan.allowed}
                        onClick={() => onChangeSteps(toggleStep(steps, index))}>
                        {disabled ? "Enable" : "Disable"}
                      </button>
                      <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" disabled={!plan.allowed}
                        onClick={() => onChangeSteps(removeStep(steps, index))}>Remove</button>
                    </div>
                  </li>
                );
              })}
            </ol>
          )}

          {effect && (
            <p className={`df2-shape-ledger${effect.balanced ? "" : " is-unbalanced"}`}>
              {summarizeEffect(effect)}
              {effect.balanced
                ? " · every sampled row is accounted for"
                : " · ledger does not balance — this is a defect, do not approve"}
            </p>
          )}

          <div className="df2-shape-add">
            <div className="df2-form-row">
              <div className="df2-field">
                <label className="df2-label" htmlFor="shape-op">Operation</label>
                <select
                  id="shape-op"
                  className="df2-input df2-select"
                  value={draftOp}
                  disabled={!plan.allowed || !catalog}
                  onChange={(e) => { setDraftOp(e.target.value); setDraftOptions({}); setDraftError(""); setExpressionError(""); }}
                >
                  <option value="">Pick an operation…</option>
                  {(catalog?.operations ?? []).map((op) => (
                    <option key={op.op} value={op.op}>{op.op} — {op.summary}</option>
                  ))}
                </select>
              </div>
              {operation?.needs_column && (
                <div className="df2-field">
                  <label className="df2-label" htmlFor="shape-column">Column</label>
                  <select
                    id="shape-column"
                    className="df2-input df2-select"
                    value={draftColumn}
                    disabled={!plan.allowed}
                    onChange={(e) => { setDraftColumn(e.target.value); setDraftError(""); }}
                  >
                    <option value="">Pick a column…</option>
                    {columnsAfter.map((column) => (
                      <option key={column} value={column}>{column}</option>
                    ))}
                  </select>
                </div>
              )}
              {operation && (
                <div className="df2-field">
                  <label className="df2-label" htmlFor="shape-policy">If a value cannot be computed</label>
                  <select
                    id="shape-policy"
                    className="df2-input df2-select"
                    value={draftPolicy}
                    disabled={!plan.allowed}
                    onChange={(e) => setDraftPolicy(e.target.value)}
                  >
                    {(catalog?.error_policies ?? []).map((policy) => (
                      <option key={policy.value} value={policy.value}>{policy.label}</option>
                    ))}
                  </select>
                  <span className="df2-label-hint">
                    {catalog?.error_policies.find((p) => p.value === draftPolicy)?.detail ?? ""}
                  </span>
                </div>
              )}
            </div>

            {operation && (
              <div className="df2-form-row">
                {fieldsFor(operation).map((field) => {
                  const id = `shape-opt-${field.name}`;
                  const value = draftOptions[field.name];
                  if (field.kind === "boolean") {
                    return (
                      <label key={field.name} className="df2-policy-toggle">
                        <input
                          type="checkbox"
                          checked={value === true}
                          disabled={!plan.allowed}
                          onChange={(e) => setDraftOptions({ ...draftOptions, [field.name]: e.target.checked })}
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
                          disabled={!plan.allowed}
                          onChange={(e) => setDraftOptions({ ...draftOptions, [field.name]: e.target.value })}
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
                          className="df2-input df2-select"
                          value={Array.isArray(value) ? (value as string[]) : []}
                          disabled={!plan.allowed}
                          onChange={(e) => setDraftOptions({
                            ...draftOptions,
                            [field.name]: Array.from(e.target.selectedOptions, (o) => o.value),
                          })}
                        >
                          {columnsAfter.map((column) => (
                            <option key={column} value={column}>{column}</option>
                          ))}
                        </select>
                      ) : field.kind === "list" ? (
                        <textarea
                          id={id}
                          className="df2-input"
                          rows={3}
                          value={Array.isArray(value) ? (value as string[]).join("\n") : ""}
                          disabled={!plan.allowed}
                          onChange={(e) => setDraftOptions({ ...draftOptions, [field.name]: linesToList(e.target.value) })}
                        />
                      ) : field.kind === "expression" ? (
                        <textarea
                          id={id}
                          className={`df2-input df2-code-input${expressionError ? " is-invalid" : ""}`}
                          rows={2}
                          placeholder="[status] <> 'void'"
                          value={typeof value === "string" ? value : ""}
                          disabled={!plan.allowed}
                          onChange={(e) => {
                            const text = e.target.value;
                            setDraftOptions({ ...draftOptions, [field.name]: text });
                            void validateExpression(field.name, text);
                          }}
                        />
                      ) : (
                        <input
                          id={id}
                          className="df2-input"
                          inputMode={field.kind === "number" ? "numeric" : undefined}
                          value={value === undefined || value === null ? "" : String(value)}
                          disabled={!plan.allowed}
                          onChange={(e) => {
                            const raw = e.target.value;
                            const next = field.kind === "number"
                              ? (raw.trim() === "" ? "" : Number(raw))
                              : raw;
                            setDraftOptions({ ...draftOptions, [field.name]: next });
                          }}
                        />
                      )}
                      {field.hint && (
                        <span className="df2-label-hint">{field.hint}</span>
                      )}
                    </div>
                  );
                })}
                <div className="df2-field">
                  <label className="df2-label" htmlFor="shape-label">Step name (optional)</label>
                  <input
                    id="shape-label"
                    className="df2-input"
                    value={draftLabel}
                    disabled={!plan.allowed}
                    placeholder="Tidy customer names"
                    onChange={(e) => setDraftLabel(e.target.value)}
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
            {draftError && (
              <div className="df2-alert df2-alert-warn" role="alert">
                <DtIcon name="alert" size={16} />
                <div><p>{draftError}</p></div>
              </div>
            )}

            <div className="df2-upload-sample-row">
              <button
                type="button"
                className="df2-btn df2-btn-sm df2-btn-primary"
                disabled={!plan.allowed || !operation || Boolean(expressionError)}
                title={plan.reason || "Append this step to the recipe"}
                onClick={addStep}
              >
                Add step
              </button>
              {draftOp && (
                <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={resetDraft}>
                  Clear
                </button>
              )}
              {catalog && (
                <button
                  type="button"
                  className="df2-btn df2-btn-sm df2-btn-ghost"
                  onClick={() => setShowFunctions((open) => !open)}
                >
                  {showFunctions ? "Hide expression help" : "Expression help"}
                </button>
              )}
            </div>

            {showFunctions && catalog && (
              <div className="df2-shape-functions">
                <p className="df2-label-hint">
                  Columns are written <code>[column name]</code>. Arithmetic is decimal, never binary float.
                  There is no clock, no randomness and no SQL — the same row always yields the same answer,
                  which is what lets Execute be held to this recipe's identity.
                </p>
                <ul>
                  {catalog.functions.map((fn) => (
                    <li key={fn.name}>
                      <code>{fn.name}</code> <span className="df2-label-hint">{fn.summary}</span>
                    </li>
                  ))}
                </ul>
                <p className="df2-label-hint">
                  Not available in flight: {catalog.post_load_only.operations.join(", ")}. {catalog.post_load_only.reason}
                </p>
              </div>
            )}
          </div>
        </section>
      </div>

      <section className="df2-shape-preview">
        <h3 className="df2-pane-title">
          Before and after
          <span className="df2-label-hint">
            {busy ? "Previewing…" : `first ${Math.min(PREVIEW_ROWS, beforeRows.length)} sampled row(s)`}
          </span>
        </h3>
        <div className="df2-shape-grids">
          <div className="df2-shape-grid">
            <h4>Source (unchanged)</h4>
            <table className="df2-table df2-table-compact">
              <thead>
                <tr>{columnsBefore.map((column) => <th key={column}>{column}</th>)}</tr>
              </thead>
              <tbody>
                {beforeRows.map((row, index) => (
                  <tr key={index}>
                    {columnsBefore.map((column) => <td key={column}>{cellText(row[column])}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="df2-shape-grid">
            <h4>Shaped (what Map will see)</h4>
            {afterRows.length === 0 ? (
              <p className="df2-label-hint">
                {steps.length ? "No rows survive the recipe on this sample." : "Nothing shaped yet."}
              </p>
            ) : (
              <table className="df2-table df2-table-compact">
                <thead>
                  <tr>{columnsAfter.map((column) => <th key={column}>{column}</th>)}</tr>
                </thead>
                <tbody>
                  {afterRows.map((row, index) => (
                    <tr key={index}>
                      {columnsAfter.map((column) => (
                        <td
                          key={column}
                          className={changed.has(`${index}:${column}`) ? "is-shape-changed" : ""}
                          title={changed.has(`${index}:${column}`) ? "Changed by the recipe" : undefined}
                        >
                          {cellText(row[column])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </section>

      <div className="df2-step-actions">
        <button type="button" className="df2-btn df2-btn-ghost" onClick={onBack}>Back</button>
        <button
          type="button"
          className="df2-btn df2-btn-primary"
          disabled={Boolean(previewError) || Boolean(preview?.refusal)}
          title={
            previewError || (preview?.refusal ? "A refused row must be decided before Map." : "Continue to Map")
          }
          onClick={onContinue}
        >
          {steps.length ? "Continue with this recipe" : "Continue without shaping"}
        </button>
      </div>
    </div>
  );
}
