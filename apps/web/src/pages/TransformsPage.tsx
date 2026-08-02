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
import {
  createTransformProject,
  deleteTransformProject,
  fetchTransformProjects,
  previewTransformPlan,
  runTransformProject,
  updateTransformProject,
  type TransformModelDef,
  type TransformPlanPreview,
  type TransformProject,
  type TransformRunResult,
} from "../lib/api";
import { formatSeconds } from "../lib/phaseProfile";
import type { Connector } from "../lib/types";

interface TransformsPageProps {
  connectors: Connector[];
}

type Materialization = TransformModelDef["materialization"];

const EMPTY_MODEL = (): TransformModelDef => ({
  name: "",
  sql: "SELECT * FROM {{ source('table_name') }}",
  materialization: "view",
  description: "",
  unique_key: "",
  incremental_strategy: "merge",
  tests: [],
  enabled: true,
});

const EMPTY_DRAFT = (): Omit<TransformProject, "id"> & { id?: string } => ({
  name: "",
  destination_connector_id: "",
  schema: "",
  models: [EMPTY_MODEL()],
  enabled: true,
  run_after_transfer: true,
  trigger_tables: [],
  description: "",
});

export function TransformsPage({ connectors }: TransformsPageProps) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const [projects, setProjects] = useState<TransformProject[]>([]);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState<(Omit<TransformProject, "id"> & { id?: string }) | null>(
    null,
  );
  const [saving, setSaving] = useState(false);
  const [runningId, setRunningId] = useState<string | null>(null);
  const [preview, setPreview] = useState<TransformPlanPreview | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [lastRun, setLastRun] = useState<TransformRunResult | null>(null);
  const [search, setSearch] = useState("");

  const destConnectors = useMemo(
    () =>
      connectors.filter((c) => {
        const role = (c.role || "both").toLowerCase();
        return role === "destination" || role === "both";
      }),
    [connectors],
  );

  const load = useCallback(async () => {
    setLoading(true);
    try {
      setProjects(await fetchTransformProjects());
    } catch (err) {
      toast({ title: err instanceof Error ? err.message : "Could not load transforms", tone: "error" });
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    void load();
  }, [load]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return projects;
    return projects.filter(
      (p) =>
        p.name.toLowerCase().includes(q) ||
        p.destination_connector_id.toLowerCase().includes(q) ||
        p.models.some((m) => m.name.toLowerCase().includes(q)),
    );
  }, [projects, search]);

  const connectorName = useCallback(
    (id: string) => connectors.find((c) => c.id === id)?.name || id,
    [connectors],
  );

  const openCreate = () => {
    setEditing(EMPTY_DRAFT());
    setPreview(null);
    setLastRun(null);
  };

  const openEdit = (project: TransformProject) => {
    setEditing({
      ...project,
      models: project.models.length ? project.models.map((m) => ({ ...m })) : [EMPTY_MODEL()],
    });
    setPreview(null);
    setLastRun(null);
  };

  const updateModel = (index: number, patch: Partial<TransformModelDef>) => {
    if (!editing) return;
    const models = editing.models.map((m, i) => (i === index ? { ...m, ...patch } : m));
    setEditing({ ...editing, models });
    setPreview(null);
  };

  const addModel = () => {
    if (!editing) return;
    setEditing({ ...editing, models: [...editing.models, EMPTY_MODEL()] });
  };

  const removeModel = (index: number) => {
    if (!editing) return;
    if (editing.models.length <= 1) {
      toast({ title: "A transform needs at least one model.", tone: "warning" });
      return;
    }
    setEditing({ ...editing, models: editing.models.filter((_, i) => i !== index) });
    setPreview(null);
  };

  const handlePreview = async () => {
    if (!editing) return;
    const dest = destConnectors.find((c) => c.id === editing.destination_connector_id);
    setPreviewing(true);
    try {
      const result = await previewTransformPlan({
        models: editing.models,
        dialect: dest?.type || "postgresql",
        schema: editing.schema || dest?.schema || "",
      });
      setPreview(result);
      toast({
        title: "Plan ready",
        message: `${result.plan.layer_count ?? 0} layer(s), ${result.plan.model_count ?? 0} model(s).`,
        tone: "success",
      });
    } catch (err) {
      setPreview(null);
      toast({ title: err instanceof Error ? err.message : "Could not compile the models", tone: "error" });
    } finally {
      setPreviewing(false);
    }
  };

  const handleSave = async () => {
    if (!editing) return;
    if (!editing.name.trim()) {
      toast({ title: "Give the transform a name.", tone: "warning" });
      return;
    }
    if (!editing.destination_connector_id) {
      toast({ title: "Pick the destination connector the models will run against.", tone: "warning" });
      return;
    }
    setSaving(true);
    try {
      const body = {
        name: editing.name.trim(),
        destination_connector_id: editing.destination_connector_id,
        schema: editing.schema,
        models: editing.models,
        enabled: editing.enabled,
        run_after_transfer: editing.run_after_transfer,
        trigger_tables: editing.trigger_tables,
        description: editing.description,
      };
      if (editing.id) {
        await updateTransformProject(editing.id, body);
        toast({ title: "Transform updated.", tone: "success" });
      } else {
        await createTransformProject(body);
        toast({ title: "Transform created.", tone: "success" });
      }
      setEditing(null);
      await load();
    } catch (err) {
      toast({ title: err instanceof Error ? err.message : "Could not save transform", tone: "error" });
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (project: TransformProject) => {
    const ok = await confirm({
      title: "Delete transform?",
      message: `“${project.name}” and its ${project.models.length} model(s) will be removed. Landed tables are unaffected.`,
      confirmLabel: "Delete",
      tone: "danger",
    });
    if (!ok) return;
    try {
      await deleteTransformProject(project.id);
      toast({ title: "Transform deleted.", tone: "success" });
      if (editing?.id === project.id) setEditing(null);
      await load();
    } catch (err) {
      toast({ title: err instanceof Error ? err.message : "Could not delete transform", tone: "error" });
    }
  };

  const handleRun = async (project: TransformProject, dryRun: boolean) => {
    setRunningId(project.id);
    try {
      const result = await runTransformProject(project.id, { dryRun });
      setLastRun(result);
      if (result.status === "success") {
        toast({
          title: dryRun ? "Dry run complete" : "Models built",
          message: dryRun
            ? `${result.model_count} model(s) compiled. Nothing was written.`
            : `${result.model_count} model(s) built successfully.`,
          tone: "success",
        });
      } else if (result.status === "partial") {
        toast({
          title: "Partial run",
          message: `${result.failed_model_count} model(s), ${result.failed_test_count} test(s) failed. Landed tables are unaffected.`,
          tone: "warning",
        });
      } else if (result.status === "skipped") {
        toast({ title: result.error || "Nothing to run.", tone: "info" });
      } else {
        toast({ title: result.error || "Transformation run failed.", tone: "error" });
      }
    } catch (err) {
      toast({ title: err instanceof Error ? err.message : "Transformation run failed", tone: "error" });
    } finally {
      setRunningId(null);
    }
  };

  return (
    <PageShell
      title="Transforms"
      description="Post-load SQL models that run at the destination after a transfer lands."
    >
      <PageFrame>
        {editing ? (
          <PageSection
            title={editing.id ? "Edit transform" : "New transform"}
            subtitle="SELECT models · runner materializes VIEW / TABLE / incremental MERGE."
            actions={
              <>
                <Button variant="ghost" onClick={() => setEditing(null)} disabled={saving}>
                  Cancel
                </Button>
                <Button
                  variant="secondary"
                  onClick={() => void handlePreview()}
                  loading={previewing}
                  disabled={saving}
                >
                  Preview plan
                </Button>
                <Button
                  variant="primary"
                  onClick={() => void handleSave()}
                  loading={saving}
                  disabled={previewing}
                >
                  Save transform
                </Button>
              </>
            }
          >
            <div className="df2-xform-form">
              <label className="df2-field">
                <span>Name</span>
                <input
                  value={editing.name}
                  onChange={(e) => setEditing({ ...editing, name: e.target.value })}
                  placeholder="Daily revenue models"
                  maxLength={120}
                />
              </label>

              <label className="df2-field">
                <span>Destination connector</span>
                <select
                  value={editing.destination_connector_id}
                  onChange={(e) =>
                    setEditing({ ...editing, destination_connector_id: e.target.value })
                  }
                >
                  <option value="">Select a warehouse…</option>
                  {destConnectors.map((c) => (
                    <option key={c.id} value={c.id}>
                      {c.name} · {c.type}
                    </option>
                  ))}
                </select>
              </label>

              <label className="df2-field">
                <span>Schema / dataset</span>
                <input
                  value={editing.schema}
                  onChange={(e) => setEditing({ ...editing, schema: e.target.value })}
                  placeholder="analytics"
                />
              </label>

              <label className="df2-field">
                <span>Trigger tables</span>
                <input
                  value={editing.trigger_tables.join(", ")}
                  onChange={(e) =>
                    setEditing({
                      ...editing,
                      trigger_tables: e.target.value
                        .split(",")
                        .map((t) => t.trim())
                        .filter(Boolean),
                    })
                  }
                  placeholder="orders, customers — empty means any table"
                />
              </label>

              <label className="df2-field df2-field-wide">
                <span>Description</span>
                <input
                  value={editing.description}
                  onChange={(e) => setEditing({ ...editing, description: e.target.value })}
                  placeholder="What these models produce for operators"
                />
              </label>

              <div className="df2-xform-toggles">
                <label>
                  <input
                    type="checkbox"
                    checked={editing.enabled}
                    onChange={(e) => setEditing({ ...editing, enabled: e.target.checked })}
                  />
                  Enabled
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={editing.run_after_transfer}
                    onChange={(e) =>
                      setEditing({ ...editing, run_after_transfer: e.target.checked })
                    }
                  />
                  Run automatically after a matching transfer
                </label>
              </div>

              <div className="df2-xform-models">
                <header>
                  <strong>Models</strong>
                  <Button variant="ghost" size="sm" leadingIcon={<DtIcon name="plus" size={14} />} onClick={addModel}>
                    Add model
                  </Button>
                </header>

                {editing.models.map((model, index) => (
                  <ModelEditor
                    key={index}
                    model={model}
                    onChange={(patch) => updateModel(index, patch)}
                    onRemove={() => removeModel(index)}
                  />
                ))}
              </div>

              {preview && <PlanPreview preview={preview} />}
            </div>
          </PageSection>
        ) : (
          <>
            <PageToolbar
              searchValue={search}
              onSearchChange={setSearch}
              searchPlaceholder="Search transforms or models…"
              actions={
                <Button variant="primary" size="sm" leadingIcon={<DtIcon name="plus" size={14} />} onClick={openCreate}>
                  New transform
                </Button>
              }
            />

            {loading ? (
              <SectionLoader title="Loading transforms" hint="Fetching post-load SQL models…" />
            ) : filtered.length === 0 ? (
              <EmptyState
                icon="layers"
                title={projects.length === 0 ? "No transforms yet" : "No matches"}
                description={
                  projects.length === 0
                    ? "After a transfer lands, define SQL models (views, tables, incremental rollups) that run at the destination warehouse."
                    : "Try a different search."
                }
                action={
                  projects.length === 0 ? (
                    <Button variant="primary" leadingIcon={<DtIcon name="plus" size={14} />} onClick={openCreate}>
                      New transform
                    </Button>
                  ) : undefined
                }
                page
              />
            ) : (
              <ul className="df2-xform-list">
                {filtered.map((project) => (
                  <li key={project.id} className="df2-xform-card">
                    <div className="df2-xform-card-head">
                      <div>
                        <h3>{project.name}</h3>
                        <p>
                          {connectorName(project.destination_connector_id)}
                          {project.schema ? ` · ${project.schema}` : ""}
                          {project.run_after_transfer
                            ? project.trigger_tables.length
                              ? ` · auto on ${project.trigger_tables.join(", ")}`
                              : " · auto after every transfer"
                            : " · manual only"}
                        </p>
                      </div>
                      <div className="df2-xform-card-actions">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => void handleRun(project, true)}
                          loading={runningId === project.id}
                        >
                          Dry run
                        </Button>
                        <Button
                          variant="secondary"
                          size="sm"
                          onClick={() => void handleRun(project, false)}
                          loading={runningId === project.id}
                        >
                          Run now
                        </Button>
                        <Button variant="ghost" size="sm" onClick={() => openEdit(project)}>
                          Edit
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => void handleDelete(project)}
                        >
                          Delete
                        </Button>
                      </div>
                    </div>
                    <ul className="df2-xform-card-models">
                      {project.models.map((m) => (
                        <li key={m.name}>
                          <DtIcon name="layers" size={12} />
                          <code>{m.name}</code>
                          <span>{m.materialization}</span>
                          {(m.refs?.length ?? 0) > 0 && (
                            <span className="df2-xform-refs">
                              → {m.refs!.join(", ")}
                            </span>
                          )}
                        </li>
                      ))}
                    </ul>
                    {project.plan?.layers && (
                      <p className="df2-xform-layers">
                        Plan:{" "}
                        {project.plan.layers
                          .map((layer, i) => `L${i + 1} ${layer.join(", ")}`)
                          .join(" · ")}
                      </p>
                    )}
                  </li>
                ))}
              </ul>
            )}

            {lastRun && <RunResultPanel result={lastRun} />}
          </>
        )}
      </PageFrame>
    </PageShell>
  );
}

function ModelEditor({
  model,
  onChange,
  onRemove,
}: {
  model: TransformModelDef;
  onChange: (patch: Partial<TransformModelDef>) => void;
  onRemove: () => void;
}) {
  return (
    <article className="df2-xform-model">
      <header>
        <input
          className="df2-xform-model-name"
          value={model.name}
          onChange={(e) => onChange({ name: e.target.value })}
          placeholder="model_name"
          maxLength={63}
        />
        <select
          value={model.materialization}
          onChange={(e) => onChange({ materialization: e.target.value as Materialization })}
        >
          <option value="view">view</option>
          <option value="table">table</option>
          <option value="incremental">incremental</option>
          <option value="ephemeral">ephemeral</option>
        </select>
        {model.materialization === "incremental" && (
          <>
            <input
              value={model.unique_key || ""}
              onChange={(e) => onChange({ unique_key: e.target.value })}
              placeholder="unique_key"
              title="Required for merge / delete_insert so re-runs stay idempotent"
            />
            <select
              value={model.incremental_strategy || "merge"}
              onChange={(e) =>
                onChange({
                  incremental_strategy: e.target.value as TransformModelDef["incremental_strategy"],
                })
              }
            >
              <option value="merge">merge (idempotent)</option>
              <option value="delete_insert">delete+insert</option>
              <option value="append">append (at-least-once)</option>
            </select>
          </>
        )}
        <Button variant="ghost" size="sm" onClick={onRemove} aria-label="Remove model">
          Remove
        </Button>
      </header>
      <textarea
        value={model.sql}
        onChange={(e) => onChange({ sql: e.target.value })}
        rows={5}
        spellCheck={false}
        placeholder={"SELECT day, SUM(amount) AS revenue\nFROM {{ ref('stg_orders') }}\nGROUP BY day"}
      />
      <p className="df2-xform-hint">
        Use <code>{"{{ ref('model') }}"}</code> for upstream models and{" "}
        <code>{"{{ source('table') }}"}</code> for the table the transfer just landed. The body
        must be a single SELECT — the runner owns CREATE / INSERT.
      </p>
    </article>
  );
}

function PlanPreview({ preview }: { preview: TransformPlanPreview }) {
  return (
    <div className="df2-xform-preview">
      <header>
        <DtIcon name="activity" size={14} />
        <strong>Compiled plan · {preview.dialect}</strong>
        <span>
          {preview.plan.layer_count} layer(s) · max parallelism {preview.plan.max_parallelism}
        </span>
      </header>
      <ol className="df2-xform-preview-layers">
        {(preview.plan.layers || []).map((layer, i) => (
          <li key={i}>
            <span>Layer {i + 1}</span>
            <code>{layer.join(", ")}</code>
          </li>
        ))}
      </ol>
      {preview.models.map((m) => (
        <article key={m.name} className={m.error ? "is-danger" : ""}>
          <header>
            <strong>{m.name}</strong>
            <span>{m.materialization}</span>
            {m.relation && <code>{m.relation}</code>}
          </header>
          {m.error && <p className="df2-xform-preview-error">{m.error}</p>}
          {m.strategy && <p className="df2-xform-hint">Strategy: {m.strategy}</p>}
          {m.statements.map((stmt, i) => (
            <pre key={i}>
              <code>{stmt}</code>
            </pre>
          ))}
        </article>
      ))}
    </div>
  );
}

function RunResultPanel({ result }: { result: TransformRunResult }) {
  return (
    <PageSection
      title="Last run"
      subtitle={`${result.status} · ${formatSeconds(result.seconds)} · ${result.model_count} model(s)`}
    >
      {result.error && <p className="df2-xform-preview-error">{result.error}</p>}
      <ul className="df2-xform-run-list">
        {result.models.map((m) => (
          <li key={m.name} className={`is-${m.status}`}>
            <DtIcon
              name={m.status === "success" ? "check" : m.status === "failed" ? "alert" : "minus"}
              size={13}
            />
            <code>{m.name}</code>
            <span>{m.materialization}</span>
            <span>{formatSeconds(m.seconds)}</span>
            {m.error && <em>{m.error}</em>}
          </li>
        ))}
      </ul>
    </PageSection>
  );
}
