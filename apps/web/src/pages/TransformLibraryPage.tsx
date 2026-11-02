import { useCallback, useEffect, useMemo, useState } from "react";
import { SectionLoader } from "../components/LoadingState";
import { Button } from "../components/ui/Button";
import { EmptyState } from "../components/ui/EmptyState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageSection } from "../components/ui/PageSection";
import { PageShell } from "../components/ui/PageShell";
import { PageToolbar } from "../components/ui/PageToolbar";
import { useToast } from "../components/Toast";
import { useConfirm } from "../components/ui/ConfirmDialog";
import { DtIcon } from "../components/DtIcon";
import { FilterConditionBuilder } from "../components/transfer/ShapeStepFields";
import {
  cloneTransformTemplate,
  createTransformRecipe,
  deleteTransformRecipe,
  fetchTransformLibraryOps,
  listTransformLibrary,
  listTransformTemplates,
  previewTransformRecipe,
  transformRecipeWhereUsed,
  updateTransformRecipe,
  type TransformLibraryOps,
  type TransformLibraryRecipe,
  type TransformRecipeStepWire,
  type TransformTemplate,
} from "../lib/api";

interface ParamDraft {
  name: string;
  def: string;
  description: string;
}

const EMPTY_STEP = (): TransformRecipeStepWire => ({
  column: "",
  op: "trim",
  on_error: "fail",
  enabled: true,
  options: {},
  output_column: "",
});

/**
 * Transform Library — the standalone, versioned half of the two-transform
 * model (Informatica Mapplet equivalent). A recipe here is a named asset a
 * transfer binds to from the Transform step; editing a recipe publishes a new
 * version, and bound transfers keep their pinned version until rebound.
 */
export function TransformLibraryPage() {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const [recipes, setRecipes] = useState<TransformLibraryRecipe[] | null>(null);
  const [ops, setOps] = useState<TransformLibraryOps | null>(null);
  const [templates, setTemplates] = useState<TransformTemplate[]>([]);
  const [loadError, setLoadError] = useState("");

  // Editor state — null means the list view is showing.
  const [editing, setEditing] = useState<TransformLibraryRecipe | null>(null);
  const [isNew, setIsNew] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [enabled, setEnabled] = useState(true);
  const [steps, setSteps] = useState<TransformRecipeStepWire[]>([EMPTY_STEP()]);
  const [params, setParams] = useState<ParamDraft[]>([]);
  const [saveError, setSaveError] = useState("");
  const [busy, setBusy] = useState(false);
  const [whereUsed, setWhereUsed] = useState<
    { schedule_name: string; bound_version: number; version_mode: string }[] | null
  >(null);
  const [previewRows, setPreviewRows] = useState("");
  const [previewResult, setPreviewResult] = useState<Record<string, unknown>[] | null>(null);
  const [previewError, setPreviewError] = useState("");

  const load = useCallback(async () => {
    try {
      const [list, opList, tmpl] = await Promise.all([
        listTransformLibrary(),
        fetchTransformLibraryOps(),
        listTransformTemplates(),
      ]);
      setRecipes(list);
      setOps(opList);
      setTemplates(tmpl);
    } catch (err) {
      setLoadError(err instanceof Error ? err.message : "Library unavailable");
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const openEditor = useCallback((recipe: TransformLibraryRecipe | null) => {
    setIsNew(recipe === null);
    setEditing(recipe);
    setName(recipe?.name ?? "");
    setDescription(recipe?.description ?? "");
    setEnabled(recipe?.enabled ?? true);
    setSteps(
      recipe?.steps?.length
        ? recipe.steps.map((s) => ({ ...s, on_error: s.on_error as "fail" | "keep_original" }))
        : [EMPTY_STEP()],
    );
    setParams(
      Object.entries(recipe?.parameters || {}).map(([pName, spec]) => ({
        name: pName,
        def: spec.default == null ? "" : String(spec.default),
        description: spec.description || "",
      })),
    );
    setSaveError("");
    setPreviewRows("");
    setPreviewResult(null);
    setPreviewError("");
    setWhereUsed(null);
    if (recipe) {
      transformRecipeWhereUsed(recipe.id)
        .then((r) => setWhereUsed(r.used_by))
        .catch(() => setWhereUsed([]));
    }
  }, []);

  const save = async () => {
    if (!name.trim()) {
      setSaveError("Name the recipe — it is what transfers bind to.");
      return;
    }
    const liveSteps = steps.filter((s) => s.op);
    if (!liveSteps.length) {
      setSaveError("Add at least one step.");
      return;
    }
    setBusy(true);
    setSaveError("");
    try {
      const parameters = Object.fromEntries(
        params
          .filter((p) => p.name.trim())
          .map((p) => [p.name.trim(), { default: p.def || null, description: p.description }]),
      );
      const body = {
        name: name.trim(),
        description: description.trim(),
        enabled,
        steps: liveSteps,
        parameters,
      };
      if (isNew) {
        await createTransformRecipe(body);
      } else if (editing) {
        await updateTransformRecipe(editing.id, { ...body, expected_version: editing.version });
      }
      toast({ title: isNew ? "Recipe saved to the Library." : "Recipe saved", message: isNew ? "" : "New version published.", tone: "success" });
      setEditing(null);
      await load();
    } catch (err) {
      setSaveError(err instanceof Error ? err.message : "Save failed");
    } finally {
      setBusy(false);
    }
  };

  const remove = async (recipe: TransformLibraryRecipe) => {
    const ok = await confirm({
      title: `Delete “${recipe.name}”?`,
      message: "Pinned transfers keep the version they bound; new binds stop resolving.",
      confirmLabel: "Delete recipe",
      cancelLabel: "Keep recipe",
      tone: "danger",
    });
    if (!ok) return;
    try {
      await deleteTransformRecipe(recipe.id);
      toast({ title: "Recipe deleted.", tone: "success" });
      await load();
    } catch (err) {
      toast({ title: "Delete failed", message: err instanceof Error ? err.message : "", tone: "error" });
    }
  };

  const clone = async (key: string) => {
    try {
      const recipe = await cloneTransformTemplate(key);
      toast({ title: `“${recipe.name}” added to the Library.`, tone: "success" });
      await load();
    } catch (err) {
      toast({ title: "Could not add template", message: err instanceof Error ? err.message : "", tone: "error" });
    }
  };

  const runPreview = async () => {
    if (!editing) return;
    setPreviewError("");
    setPreviewResult(null);
    let rows: Record<string, unknown>[];
    try {
      const parsed = JSON.parse(previewRows);
      rows = Array.isArray(parsed) ? parsed : [parsed];
    } catch {
      setPreviewError('Sample rows must be JSON — e.g. [{"email":" A@B.COM "}]');
      return;
    }
    try {
      const result = await previewTransformRecipe(editing.id, { rows, steps, per_step: false });
      setPreviewResult(result.rows || []);
    } catch (err) {
      setPreviewError(err instanceof Error ? err.message : "Preview failed");
    }
  };

  const updateStep = (index: number, patch: Partial<TransformRecipeStepWire>) => {
    setSteps((prev) => prev.map((s, i) => (i === index ? { ...s, ...patch } : s)));
  };

  const setStepOption = (index: number, key: string, value: string) => {
    // Structured options (measures, rows, map, keys, order…) are JSON — parse
    // the raw text when it reads as JSON so list/dict options survive intact.
    const trimmed = value.trim();
    let parsed: unknown = value;
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      try {
        parsed = JSON.parse(trimmed);
      } catch {
        parsed = value;
      }
    }
    setSteps((prev) =>
      prev.map((s, i) =>
        i === index ? { ...s, options: { ...(s.options || {}), [key]: parsed } } : s,
      ),
    );
  };

  const opDetail = (op: string) => ops?.ops_detail?.find((d) => d.op === op);

  // Columns the guided condition builder can offer: every column any step
  // already names, plus keys from the preview sample rows.
  const columnOptions = useMemo(() => {
    const names = new Set<string>();
    for (const s of steps) {
      if (s.column) names.add(s.column);
      if (s.output_column) names.add(s.output_column);
      for (const v of Object.values(s.options || {})) {
        if (typeof v === "string" && /^[A-Za-z_][\w.]*$/.test(v)) names.add(v);
      }
    }
    try {
      const rows = JSON.parse(previewRows || "[]");
      if (Array.isArray(rows) && rows[0] && typeof rows[0] === "object") {
        Object.keys(rows[0] as Record<string, unknown>).forEach((k) => names.add(k));
      }
    } catch {
      /* preview JSON not parseable yet — column hints just stay sparse */
    }
    return [...names].sort().map((n) => ({ value: n, label: n }));
  }, [steps, previewRows]);

  const stepOptions = useMemo(() => ops?.ops ?? [], [ops]);

  if (editing !== null || isNew) {
    return (
      <PageShell
        wide
        title={isNew ? "New library transform" : `Edit — ${editing?.name ?? "transform"}`}
        description="A saved version a transfer can bind to. Saving publishes a new version; pinned transfers are unaffected."
      >
        <PageFrame>
          <PageToolbar
            actions={
              <>
                <Button variant="ghost" onClick={() => { setEditing(null); setIsNew(false); }}>
                  Back to Library
                </Button>
                <Button variant="primary" disabled={busy} onClick={() => void save()}>
                  {busy ? "Saving…" : isNew ? "Save recipe" : "Save new version"}
                </Button>
              </>
            }
          />
          {saveError ? <p className="df2-alert df2-alert-error" role="alert">{saveError}</p> : null}

          {whereUsed && whereUsed.length > 0 ? (
            <div className="df2-alert df2-alert-warn" role="status">
              <DtIcon name="warn" size={16} />
              <div>
                <p>
                  Bound by {whereUsed.length} schedule{whereUsed.length === 1 ? "" : "s"}:
                  {" "}{whereUsed.map((u) => u.schedule_name).join(", ")}.
                </p>
                <p className="df2-label-hint">
                  Saving creates v{(editing?.version ?? 0) + 1}; pinned bindings stay on their
                  version, follow-latest picks this up.
                </p>
              </div>
            </div>
          ) : null}

          <PageSection title="Identity">
            <div className="df2-label-hint" style={{ display: "grid", gap: 10, maxWidth: 560 }}>
              <label>
                Name
                <input className="df2-input" value={name} onChange={(e) => setName(e.target.value)} />
              </label>
              <label>
                Description
                <input className="df2-input" value={description} onChange={(e) => setDescription(e.target.value)} />
              </label>
              <label style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
                Enabled — disabled recipes refuse new bindings
              </label>
            </div>
          </PageSection>

          <PageSection
            title="Steps"
            actions={
              <Button variant="ghost" size="sm" onClick={() => setSteps((p) => [...p, EMPTY_STEP()])}>
                Add step
              </Button>
            }
          >
            <div className="df2-xform-steps" style={{ display: "grid", gap: 10 }}>
              {steps.map((step, i) => (
                <div key={i} className="df2-xform-panel" style={{ padding: 12 }}>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                    <span className="df2-xform-step-index">{i + 1}</span>
                    <select
                      aria-label={`Step ${i + 1} operation`}
                      className="df2-input"
                      value={step.op}
                      onChange={(e) => updateStep(i, { op: e.target.value })}
                    >
                      {stepOptions.map((op) => (
                        <option key={op} value={op}>
                          {opDetail(op)?.summary || op}
                        </option>
                      ))}
                    </select>
                    <input
                      aria-label={`Step ${i + 1} column`}
                      className="df2-input"
                      placeholder="column"
                      value={step.column}
                      onChange={(e) => updateStep(i, { column: e.target.value })}
                    />
                    <input
                      aria-label={`Step ${i + 1} output column`}
                      className="df2-input"
                      placeholder="→ output column (optional)"
                      value={step.output_column || ""}
                      onChange={(e) => updateStep(i, { output_column: e.target.value })}
                    />
                    <select
                      aria-label={`Step ${i + 1} error policy`}
                      className="df2-input"
                      value={step.on_error}
                      onChange={(e) => updateStep(i, { on_error: e.target.value as "fail" | "keep_original" })}
                    >
                      <option value="fail">fail</option>
                      <option value="keep_original">keep_original</option>
                    </select>
                    <label style={{ display: "flex", gap: 4, alignItems: "center" }}>
                      <input
                        type="checkbox"
                        checked={step.enabled}
                        onChange={(e) => updateStep(i, { enabled: e.target.checked })}
                      />
                      on
                    </label>
                    <button
                      type="button"
                      className="df2-btn df2-btn-ghost df2-btn-sm"
                      onClick={() => setSteps((p) => p.filter((_, j) => j !== i))}
                    >
                      Remove
                    </button>
                  </div>
                  {(opDetail(step.op)?.options?.length ?? 0) > 0 ? (
                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 8 }}>
                      {opDetail(step.op)!.options!.map((key) => {
                        const detail = opDetail(step.op)!;
                        if (key === detail.expression_option && key === "condition") {
                          return (
                            <div key={key} style={{ flexBasis: "100%" }}>
                              <FilterConditionBuilder
                                id={`lib-step-${i}-condition`}
                                field={{ name: "condition", kind: "expression", label: "Condition", hint: "", required: true }}
                                value={String(step.options?.[key] ?? "")}
                                columns={columnOptions}
                                canPlan
                                invalid={false}
                                expressionError=""
                                onChange={(v) => setStepOption(i, key, String(v ?? ""))}
                                onExpressionChange={(t) => setStepOption(i, key, t)}
                              />
                            </div>
                          );
                        }
                        if (key === detail.expression_option) {
                          return (
                            <div key={key} style={{ flexBasis: "100%" }}>
                              <label className="df2-label-hint" style={{ display: "block" }}>
                                {key}
                                <textarea
                                  className="df2-input df2-xform-code"
                                  rows={2}
                                  value={String(step.options?.[key] ?? "")}
                                  onChange={(e) => setStepOption(i, key, e.target.value)}
                                />
                              </label>
                              {(ops?.functions?.length ?? 0) > 0 && (
                                <select
                                  aria-label={`Insert function into step ${i + 1}`}
                                  className="df2-input df2-input-sm"
                                  value=""
                                  onChange={(e) => {
                                    if (!e.target.value) return;
                                    setStepOption(i, key, `${step.options?.[key] ?? ""}${e.target.value}()`);
                                  }}
                                >
                                  <option value="">Insert function…</option>
                                  {ops!.functions!.map((f) => (
                                    <option key={f.name} value={f.name}>
                                      {f.name} — {f.summary}
                                    </option>
                                  ))}
                                </select>
                              )}
                            </div>
                          );
                        }
                        return (
                          <label key={key} className="df2-label-hint" style={{ display: "block" }}>
                            {key}
                            <input
                              className="df2-input"
                              value={typeof step.options?.[key] === "object" ? JSON.stringify(step.options[key]) : String(step.options?.[key] ?? "")}
                              onChange={(e) => setStepOption(i, key, e.target.value)}
                            />
                          </label>
                        );
                      })}
                    </div>
                  ) : null}
                  {opDetail(step.op)?.summary ? (
                    <p className="df2-label-hint" style={{ marginTop: 6 }}>
                      {opDetail(step.op)!.summary}
                    </p>
                  ) : null}
                  {opDetail(step.op)?.buffered ? (
                    <p className="df2-label-hint" style={{ marginTop: 6 }}>
                      Holds the population in memory (up to 500,000 rows) and
                      emits at end-of-stream — the run fails loud past the cap.
                    </p>
                  ) : null}
                  {step.op === "lookup" && step.options?.reference ? (
                    <p className="df2-label-hint" style={{ marginTop: 6 }}>
                      Connector reference: the table is read live once at run
                      start and cached for the run — re-run to pick up changed
                      reference data.
                    </p>
                  ) : null}
                </div>
              ))}
            </div>
          </PageSection>

          <PageSection
            title="Parameters"
            actions={
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setParams((p) => [...p, { name: "", def: "", description: "" }])}
              >
                Add parameter
              </Button>
            }
          >
            <p className="df2-label-hint">
              Transfers override values only — e.g. a filter value as <code>:state</code> inside an
              option — never the shared definition.
            </p>
            {params.map((p, i) => (
              <div key={i} style={{ display: "flex", gap: 8, marginBottom: 6 }}>
                <input
                  aria-label="Parameter name"
                  className="df2-input"
                  placeholder=":name"
                  value={p.name}
                  onChange={(e) =>
                    setParams((prev) => prev.map((x, j) => (j === i ? { ...x, name: e.target.value } : x)))
                  }
                />
                <input
                  aria-label="Parameter default"
                  className="df2-input"
                  placeholder="default"
                  value={p.def}
                  onChange={(e) =>
                    setParams((prev) => prev.map((x, j) => (j === i ? { ...x, def: e.target.value } : x)))
                  }
                />
                <input
                  aria-label="Parameter description"
                  className="df2-input"
                  placeholder="description"
                  value={p.description}
                  onChange={(e) =>
                    setParams((prev) =>
                      prev.map((x, j) => (j === i ? { ...x, description: e.target.value } : x)),
                    )
                  }
                />
                <button
                  type="button"
                  className="df2-btn df2-btn-ghost df2-btn-sm"
                  onClick={() => setParams((prev) => prev.filter((_, j) => j !== i))}
                >
                  Remove
                </button>
              </div>
            ))}
          </PageSection>

          {!isNew && editing ? (
            <PageSection title="Preview on sample rows">
              <label className="df2-label-hint" style={{ display: "block" }}>
                Sample rows (JSON array of objects)
                <textarea
                  className="df2-input"
                  rows={4}
                  value={previewRows}
                  placeholder='[{"email":" A@B.COM ","qty":"12"}]'
                  onChange={(e) => setPreviewRows(e.target.value)}
                />
              </label>
              <Button variant="ghost" size="sm" disabled={!previewRows.trim()} onClick={() => void runPreview()}>
                Run preview
              </Button>
              {previewError ? (
                <p className="df2-alert df2-alert-error" role="alert">{previewError}</p>
              ) : null}
              {previewResult ? (
                <pre className="df2-xform-panel" style={{ padding: 10, overflow: "auto" }}>
                  {JSON.stringify(previewResult, null, 2)}
                </pre>
              ) : null}
            </PageSection>
          ) : null}
        </PageFrame>
      </PageShell>
    );
  }

  return (
    <PageShell
      wide
      title="Transform Library"
      description="Named, versioned transforms a transfer binds to from the Transform step — built once, reused with pinned or latest versions."
    >
      <PageFrame>
        <PageToolbar
          actions={
            <Button variant="primary" onClick={() => openEditor(null)}>
              New transform
            </Button>
          }
        />
        {loadError ? (
          <p className="df2-alert df2-alert-error" role="alert">{loadError}</p>
        ) : recipes === null ? (
          <SectionLoader title="Loading library…" />
        ) : (
          <>
            {templates.length ? (
              <PageSection title="Templates">
                <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                  {templates.map((t) => (
                    <div key={t.key} className="df2-xform-panel" style={{ padding: 12, maxWidth: 320 }}>
                      <strong>{t.name}</strong>
                      <p className="df2-label-hint">{t.description}</p>
                      <Button variant="ghost" size="sm" onClick={() => void clone(t.key)}>
                        Add to Library
                      </Button>
                    </div>
                  ))}
                </div>
              </PageSection>
            ) : null}
            <PageSection title="Saved transforms">
              {recipes.length === 0 ? (
                <EmptyState
                  icon="layers"
                  title="No transforms yet"
                  description="Create one here or add a template — then bind it from the Transform step of a transfer."
                />
              ) : (
                <table className="df2-table df2-table-compact">
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Version</th>
                      <th>Status</th>
                      <th>Steps</th>
                      <th>Params</th>
                      <th>Updated</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {recipes.map((r) => (
                      <tr key={r.id}>
                        <td>
                          <strong>{r.name}</strong>
                          {r.description ? (
                            <div className="df2-label-hint">{r.description}</div>
                          ) : null}
                        </td>
                        <td>v{r.version}</td>
                        <td>
                          <span className={`df2-badge${r.status !== "active" ? " df2-badge-warn" : ""}`}>
                            {r.status}
                            {!r.enabled ? " · disabled" : ""}
                          </span>
                          {r.issues?.length ? (
                            <div className="df2-label-hint">{r.issues[0]}</div>
                          ) : null}
                        </td>
                        <td>{r.steps?.length ?? 0}</td>
                        <td>{Object.keys(r.parameters || {}).length}</td>
                        <td>{r.updated_at ? new Date(r.updated_at).toLocaleDateString() : "—"}</td>
                        <td>
                          <Button variant="ghost" size="sm" onClick={() => openEditor(r)}>
                            Edit
                          </Button>
                          <Button variant="ghost" size="sm" onClick={() => void remove(r)}>
                            Delete
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </PageSection>
          </>
        )}
      </PageFrame>
    </PageShell>
  );
}
