import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DtIcon } from "../../components/DtIcon";
import { TransformColumnCatalog } from "../../components/transfer/TransformColumnCatalog";
import { TransformColumnChart } from "../../components/transfer/TransformColumnChart";
import { TransformGuidePanel } from "../../components/transfer/TransformGuidePanel";
import { TransformStepBuilder } from "../../components/transfer/TransformStepBuilder";
import { fetchShapeCatalog, previewShapeRecipe, profileShapeSource } from "../../lib/api";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import {
  changedCellIndex,
  describeStep,
  moveStep,
  removeStep,
  sortSuggestions,
  summarizeEffect,
  toggleStep,
  type ShapeCatalog,
  type ShapeOperation,
  type ShapePreviewResponse,
  type ShapeProfileResponse,
  type ShapeStepWire,
  type ShapeSuggestion,
  type TransformImage,
} from "../../lib/shape";
import { columnsNeedingAttention } from "../../lib/transformProfile";

interface TransferTransformStepProps {
  /** Sampled source rows already held by the studio — no connector round-trip. */
  sampleRows: Record<string, unknown>[];
  sourceColumns: string[];
  /** Declared source carriers from the catalog — the types the recipe starts from. */
  sourceSchema: Record<string, string>;
  /** Declared destination carriers, so a narrowing decimal is caught here. */
  targetSchema: Record<string, string>;
  sourceLabel: string;
  destRouteLabel: string;
  /** Total source population, so the sample notice can state what it is not. */
  rowCount?: number;
  steps: ShapeStepWire[];
  onChangeSteps: (steps: ShapeStepWire[]) => void;
  /**
   * The transformed image Map, Validate and the run request are held to: the
   * recipe identity, the columns it produces, and the carriers those columns
   * now hold. Null when no recipe can run, so nothing downstream claims one.
   */
  onIdentity: (identity: TransformImage | null) => void;
  onBack: () => void;
  onContinue: () => void;
}

const PREVIEW_ROWS = 12;
const PREVIEW_DEBOUNCE_MS = 250;
const GUIDE_KEY = "df.transform.guide.dismissed";

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

/**
 * Transform (pre-load) — repair the source on the read, before Map and the write.
 *
 * Three panels in the order the decision is made: what the sample holds (charted
 * per column, with profile-driven suggestions), the ordered steps to apply, and
 * the before/after the recipe produces. Nothing here mutates the source; the
 * recipe travels with the plan under an identity Execute is held to, and every
 * step states what it did — cells changed, nulls introduced, rows removed.
 *
 * The step is named for the operator, not for the engine: post-load SQL
 * transforms are a different plane, and this one is explicitly *pre-load*.
 */
export function TransferTransformStep({
  sampleRows,
  sourceColumns,
  sourceSchema,
  targetSchema,
  sourceLabel,
  destRouteLabel,
  rowCount,
  steps,
  onChangeSteps,
  onIdentity,
  onBack,
  onContinue,
}: TransferTransformStepProps) {
  const plan = useWriteGate(PERMISSIONS.jobPlan);
  const [catalog, setCatalog] = useState<ShapeCatalog | null>(null);
  const [catalogError, setCatalogError] = useState("");
  const [profile, setProfile] = useState<ShapeProfileResponse | null>(null);
  const [profileError, setProfileError] = useState("");
  const [preview, setPreview] = useState<ShapePreviewResponse | null>(null);
  const [previewError, setPreviewError] = useState("");
  const [busy, setBusy] = useState(false);
  const [showGuide, setShowGuide] = useState(() => {
    try {
      return window.localStorage.getItem(GUIDE_KEY) !== "1";
    } catch {
      return true;
    }
  });
  const [showAllColumns, setShowAllColumns] = useState(false);
  const [selectedColumn, setSelectedColumn] = useState("");
  const [showBuilder, setShowBuilder] = useState(false);

  const rowsKey = useMemo(() => JSON.stringify(sampleRows.slice(0, 200)), [sampleRows]);
  const stepsKey = useMemo(() => JSON.stringify(steps), [steps]);
  const schemaKey = useMemo(() => JSON.stringify(targetSchema), [targetSchema]);
  const sourceSchemaKey = useMemo(() => JSON.stringify(sourceSchema), [sourceSchema]);
  const shapedColumns = preview?.recipe.output_columns ?? sourceColumns;
  const retyped = useMemo(
    () => Object.entries(preview?.retyped_columns ?? {}).sort(([a], [b]) => a.localeCompare(b)),
    [preview],
  );

  const toggleGuide = useCallback(() => {
    setShowGuide((open) => {
      const next = !open;
      try {
        window.localStorage.setItem(GUIDE_KEY, next ? "0" : "1");
      } catch {
        /* a private-mode browser simply shows the guide every visit */
      }
      return next;
    });
  }, []);

  useEffect(() => {
    let cancelled = false;
    fetchShapeCatalog()
      .then((next) => { if (!cancelled) setCatalog(next); })
      .catch((err) => { if (!cancelled) setCatalogError(err instanceof Error ? err.message : String(err)); });
    return () => { cancelled = true; };
  }, []);

  // Profiling only describes rows the caller already holds, so it is a read: a
  // viewer sees the same findings an operator would act on. Previewing composes a
  // recipe identity, which is plan work, so that call stays behind the gate.
  useEffect(() => {
    if (!sampleRows.length) return;
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
  }, [rowsKey, schemaKey, sourceColumns.join("|")]);

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
        column_types: sourceSchema,
        target_schema: targetSchema,
      })
        .then((next) => {
          if (cancelled) return;
          setPreview(next);
          setPreviewError("");
          onIdentity({
            hash: next.recipe.recipe_hash,
            columns: next.recipe.output_columns,
            columnTypes: next.column_types ?? {},
            retypedColumns: next.retyped_columns ?? {},
            sampleRows: next.after,
          });
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
  }, [plan.allowed, rowsKey, stepsKey, schemaKey, sourceSchemaKey, sourceColumns.join("|")]);

  const operationsByName = useMemo(() => {
    const index = new Map<string, ShapeOperation>();
    for (const op of catalog?.operations ?? []) index.set(op.op, op);
    return index;
  }, [catalog]);

  const addStep = useCallback((step: ShapeStepWire) => {
    onChangeSteps([...steps, step]);
  }, [onChangeSteps, steps]);

  const applySuggestion = useCallback((suggestion: ShapeSuggestion) => {
    onChangeSteps([...steps, suggestion.step]);
  }, [onChangeSteps, steps]);

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

  const profiledColumns = profile?.columns ?? [];
  const attention = columnsNeedingAttention(profiledColumns);
  const catalogColumns = showAllColumns || attention === 0
    ? profiledColumns
    : profiledColumns.filter((column) => column.blanks || column.untrimmed || column.inner_whitespace
      || column.non_printable || column.unnormalized_unicode || Object.keys(column.sentinels).length);
  const visibleColumns = catalogColumns.length ? catalogColumns : profiledColumns;
  const selectedProfile = visibleColumns.find((column) => column.name === selectedColumn)
    ?? visibleColumns[0]
    ?? null;
  const selectedSuggestion = selectedProfile
    ? openSuggestions.find((item) => item.step.column === selectedProfile.name) ?? null
    : null;
  const selectedSamples = selectedProfile
    ? sampleRows.map((row) => row[selectedProfile.name])
    : [];

  return (
    <section className="df2-xform" aria-labelledby="xform-title">
      <header className="df2-xform-head">
        <div className="df2-xform-head-copy">
          <p className="df2-xform-eyebrow">Before the load · the source is never modified</p>
          <h2 className="df2-xform-title" id="xform-title">Transform (pre-load)</h2>
          <p className="df2-xform-route">
            <span>{sourceLabel}</span>
            <DtIcon name="transfer" size={14} />
            <span>{destRouteLabel}</span>
          </p>
        </div>
        <div className="df2-xform-head-side">
          {preview ? (
            <span className="df2-xform-identity" title="Pinned at approval and re-checked before Execute">
              <DtIcon name="shield" size={14} />
              recipe {preview.recipe.recipe_hash}
            </span>
          ) : (
            <span className="df2-xform-identity is-empty">
              {busy ? "Previewing…" : "No transform declared"}
            </span>
          )}
          <button
            type="button"
            className="df2-btn df2-btn-ghost df2-btn-sm"
            aria-expanded={showGuide}
            onClick={toggleGuide}
          >
            <DtIcon name="book" size={14} /> {showGuide ? "Hide how this works" : "How this works"}
          </button>
        </div>
      </header>

      {showGuide && (
        <TransformGuidePanel postLoadOnly={catalog?.post_load_only.operations ?? []} />
      )}

      <dl className="df2-xform-stats">
        <div>
          <dt>Sampled rows</dt>
          <dd>
            {(profile?.sampled_rows ?? sampleRows.length).toLocaleString()}
            {rowCount ? <small> of {rowCount.toLocaleString()}</small> : null}
          </dd>
        </div>
        <div>
          <dt>Columns</dt>
          <dd>{(profiledColumns.length || sourceColumns.length).toLocaleString()}</dd>
        </div>
        <div className={attention ? "is-attention" : ""}>
          <dt>Columns with findings</dt>
          <dd>{attention.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Steps applied</dt>
          <dd>
            {steps.length.toLocaleString()}
            {catalog ? <small> of {catalog.max_steps} allowed</small> : null}
          </dd>
        </div>
      </dl>

      {!plan.allowed && (
        <div className="df2-alert df2-alert-info" role="status">
          <DtIcon name="lock" size={16} />
          <div>
            <p>{plan.reason}</p>
            <p className="df2-label-hint">
              The profile, the charts and “Read the operations” are yours to inspect — they are the
              real findings and the real vocabulary. Applying one is plan work.
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
            <p className="df2-label-hint">
              The recipe is refused, so it has no identity to approve. Fix or remove the step.
            </p>
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
              This row stops the run. Change the step's error policy to divert or null if that is the
              decision you want, or repair the value at source.
            </p>
          </div>
        </div>
      )}

      {profiledColumns.length > 0 && (
        <section className="df2-xform-card df2-xform-kitchen-card" aria-labelledby="xform-profile-title">
          <header className="df2-xform-card-head">
            <h3 id="xform-profile-title">
              <span className="df2-xform-card-num">1</span> Column catalog
            </h3>
            <button
              type="button"
              className="df2-btn df2-btn-ghost df2-btn-sm"
              onClick={() => setShowAllColumns((all) => !all)}
            >
              {showAllColumns || attention === 0
                ? (attention ? "Only columns with findings" : `All ${profiledColumns.length} columns`)
                : `All ${profiledColumns.length} columns`}
            </button>
          </header>
          {profile?.sample_notice && <p className="df2-xform-note">{profile.sample_notice}</p>}
          <div className="df2-xform-kitchen">
            <TransformColumnCatalog
              columns={visibleColumns}
              selected={selectedProfile?.name ?? ""}
              onSelect={setSelectedColumn}
            />
            {selectedProfile ? (
              <TransformColumnChart
                profile={selectedProfile}
                targetType={targetSchema[selectedProfile.name]}
                sampleValues={selectedSamples}
                suggestion={selectedSuggestion}
                canApply={plan.allowed}
                applyReason={plan.reason}
                onApplySuggestion={applySuggestion}
              />
            ) : (
              <p className="df2-xform-empty">Select a column to inspect its profile.</p>
            )}
          </div>
        </section>
      )}

      <div className="df2-xform-grid">
        <section className="df2-xform-card" aria-labelledby="xform-decisions-title">
          <header className="df2-xform-card-head">
            <h3 id="xform-decisions-title">
              <span className="df2-xform-card-num">1b</span> Suggested repairs
            </h3>
          </header>
          {openSuggestions.length > 0 ? (
            <ul className="df2-xform-suggestions">
              {openSuggestions.map((suggestion) => (
                <li key={suggestion.id}>
                  <div className="df2-xform-suggestion-head">
                    <span className={severityClass(suggestion.severity)}>{suggestion.severity}</span>
                    <strong>{suggestion.title}</strong>
                  </div>
                  <p>{suggestion.reason}</p>
                  <div className="df2-xform-suggestion-foot">
                    <span>{suggestion.rows_affected.toLocaleString()} sampled row(s) affected</span>
                    <button
                      type="button"
                      className="df2-btn df2-btn-sm"
                      disabled={!plan.allowed}
                      title={plan.reason || "Add this step to the recipe"}
                      onClick={() => applySuggestion(suggestion)}
                    >
                      <DtIcon name="plus" size={14} /> Add step
                    </button>
                  </div>
                </li>
              ))}
            </ul>
          ) : profile ? (
            <p className="df2-xform-empty">
              <DtIcon name="check" size={16} />
              Nothing in the sample needs a transform. Validate still re-checks the whole population.
            </p>
          ) : null}
        </section>

        <section className="df2-xform-card" aria-labelledby="xform-recipe-title">
          <header className="df2-xform-card-head">
            <h3 id="xform-recipe-title">
              <span className="df2-xform-card-num">2</span> Steps to apply, in order
            </h3>
            {/* A viewer opens the same panel read-only: the operations are what the
                refusal text says are readable, and nothing in it can be applied. */}
            <button
              type="button"
              className="df2-btn df2-btn-sm"
              aria-expanded={showBuilder}
              title={plan.allowed ? "Compose a step by hand" : "Read the operations this engine accepts"}
              onClick={() => setShowBuilder((open) => !open)}
            >
              <DtIcon name={showBuilder ? "minus" : "plus"} size={14} />
              {showBuilder
                ? " Close"
                : plan.allowed ? " Add a step" : " Read the operations"}
            </button>
          </header>

          {steps.length === 0 ? (
            <p className="df2-xform-empty">
              <DtIcon name="check" size={16} />
              No steps. The source passes through unchanged — exactly today's behaviour.
            </p>
          ) : (
            <ol className="df2-xform-steps">
              {steps.map((step, index) => {
                const stepEffect = effect?.steps?.[index];
                const disabled = step.enabled === false;
                return (
                  <li key={`${step.op}:${index}`} className={disabled ? "is-disabled" : ""}>
                    <div className="df2-xform-step-head">
                      <span className="df2-xform-step-index">{index + 1}</span>
                      <strong>{describeStep(step, operationsByName.get(step.op))}</strong>
                      {step.on_error && step.on_error !== "refuse" && (
                        <span className="df2-badge">on error: {step.on_error}</span>
                      )}
                      {operationsByName.get(step.op)?.active && (
                        <span
                          className="df2-badge df2-badge-warn"
                          title="Changes the row count, so it moves the conservation ledger"
                        >
                          moves the ledger
                        </span>
                      )}
                    </div>
                    <p className="df2-xform-step-effect">
                      {stepEffect
                        ? [
                            `${stepEffect.rows_in.toLocaleString()} in`,
                            `${stepEffect.rows_out.toLocaleString()} out`,
                            stepEffect.cells_changed ? `${stepEffect.cells_changed.toLocaleString()} cell(s) changed` : "",
                            stepEffect.rows_removed ? `${stepEffect.rows_removed.toLocaleString()} removed` : "",
                            stepEffect.rows_diverted ? `${stepEffect.rows_diverted.toLocaleString()} diverted` : "",
                            stepEffect.nulls_introduced ? `${stepEffect.nulls_introduced.toLocaleString()} null(s) introduced` : "",
                          ].filter(Boolean).join(" · ")
                        : disabled
                          ? "Disabled — excluded from the recipe and its identity."
                          : "Not yet measured."}
                    </p>
                    <div className="df2-xform-step-controls">
                      <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" disabled={!plan.allowed || index === 0}
                        title="Move earlier" aria-label={`Move step ${index + 1} earlier`}
                        onClick={() => onChangeSteps(moveStep(steps, index, -1))}>↑</button>
                      <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" disabled={!plan.allowed || index === steps.length - 1}
                        title="Move later" aria-label={`Move step ${index + 1} later`}
                        onClick={() => onChangeSteps(moveStep(steps, index, 1))}>↓</button>
                      <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" disabled={!plan.allowed}
                        onClick={() => onChangeSteps(toggleStep(steps, index))}>
                        {disabled ? "Enable" : "Disable"}
                      </button>
                      <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost is-danger" disabled={!plan.allowed}
                        onClick={() => onChangeSteps(removeStep(steps, index))}>Remove</button>
                    </div>
                  </li>
                );
              })}
            </ol>
          )}

          {effect && (
            /* A refused row is the operator's own Refuse policy fail-closing, so
               the sample legitimately stops short of a balance — calling that a
               defect blamed the engine for the step's configured behaviour. Only
               an unbalanced sample with no refusal is an accounting defect. */
            <p className={`df2-xform-ledger${effect.balanced || preview?.refusal ? "" : " is-unbalanced"}`}>
              <DtIcon name={effect.balanced ? "check" : "alert"} size={14} />
              {summarizeEffect(effect)}
              {effect.balanced
                ? " · every sampled row is accounted for"
                : preview?.refusal
                  ? " · the preview stopped at the refused row above, so the remaining sampled rows were not transformed"
                  : " · ledger does not balance — this is a defect, do not approve"}
            </p>
          )}

          {showBuilder && (
            <TransformStepBuilder
              catalog={catalog}
              columns={columnsAfter}
              canPlan={plan.allowed}
              disabledReason={plan.reason}
              onAdd={addStep}
            />
          )}
        </section>
      </div>

      <section className="df2-xform-card df2-xform-preview" aria-labelledby="xform-preview-title">
        <header className="df2-xform-card-head">
          <h3 id="xform-preview-title">
            <span className="df2-xform-card-num">3</span> Before and after
          </h3>
          <span className="df2-xform-note">
            {busy
              ? "Previewing…"
              : `first ${Math.min(PREVIEW_ROWS, beforeRows.length)} sampled row(s) · changed cells are highlighted`}
          </span>
        </header>
        <div className="df2-xform-grids">
          <div className="df2-xform-gridpane">
            <h4>Source, unchanged</h4>
            <div className="df2-xform-scroll">
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
          </div>
          <div className="df2-xform-gridpane">
            <h4>Transformed — what Map and the writer will see</h4>
            {afterRows.length === 0 ? (
              <p className="df2-xform-empty">
                {steps.length ? "No rows survive the recipe on this sample." : "Nothing transformed yet."}
              </p>
            ) : (
              <div className="df2-xform-scroll">
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
                            className={changed.has(`${index}:${column}`) ? "is-xform-changed" : ""}
                            title={changed.has(`${index}:${column}`) ? "Changed by the recipe" : undefined}
                          >
                            {cellText(row[column])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
        {retyped.length > 0 && (
          <p className="df2-xform-note df2-xform-retyped">
            <DtIcon name="info" size={14} />
            {" "}Map and Validate now read {retyped.length === 1 ? "this column" : "these columns"} as
            the transform leaves {retyped.length === 1 ? "it" : "them"}:{" "}
            {retyped.map(([column, carrier]) => `${column} → ${carrier}`).join(", ")}. Columns no step
            wrote keep their declared source type.
          </p>
        )}
      </section>

      <footer className="df2-xform-actions">
        <button type="button" className="df2-btn df2-btn-ghost" onClick={onBack}>
          Back to Destination
        </button>
        <button
          type="button"
          className="df2-btn df2-btn-primary"
          disabled={Boolean(previewError) || Boolean(preview?.refusal)}
          title={previewError || (preview?.refusal ? "A refused row must be decided before Map." : "Continue to Map")}
          onClick={onContinue}
        >
          {steps.length ? "Continue with this transform" : "Continue without transforming"}
        </button>
      </footer>
    </section>
  );
}
