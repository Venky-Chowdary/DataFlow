import { useCallback, useEffect, useMemo, useState } from "react";
import { SectionLoader } from "../components/LoadingState";
import { TransformDetailDrawer } from "../components/TransformDetailDrawer";
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
  exportTransformProjectDbt,
  runTransformProject,
  updateTransformProject,
  type TransformModelDef,
  type TransformPlanPreview,
  type TransformProject,
  type TransformRunResult,
} from "../lib/api";
import type { Connector, Screen } from "../lib/types";

interface TransformsPageProps {
  connectors: Connector[];
  onNavigate?: (screen: Screen) => void;
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
  contract_id: "",
  schema: "",
  models: [EMPTY_MODEL()],
  enabled: true,
  run_after_transfer: true,
  trigger_tables: [],
  description: "",
});

export function TransformsPage({ connectors, onNavigate }: TransformsPageProps) {
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
  const [lastRunProjectId, setLastRunProjectId] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [resumeDrawerAfterEdit, setResumeDrawerAfterEdit] = useState(false);

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

  const selectedProject = useMemo(
    () => projects.find((p) => p.id === selectedId) ?? null,
    [projects, selectedId],
  );

  const openDrawer = (id: string) => {
    setSelectedId(id);
    setDrawerOpen(true);
  };

  const closeDrawer = () => {
    setDrawerOpen(false);
  };

  const openCreate = () => {
    setResumeDrawerAfterEdit(false);
    closeDrawer();
    setEditing(EMPTY_DRAFT());
    setPreview(null);
  };

  const openEdit = (project: TransformProject) => {
    setEditing({
      ...project,
      models: project.models.length ? project.models.map((m) => ({ ...m })) : [EMPTY_MODEL()],
    });
    setPreview(null);
  };

  const cancelEdit = () => {
    const reopenId = resumeDrawerAfterEdit ? editing?.id ?? selectedId : null;
    setEditing(null);
    setPreview(null);
    setResumeDrawerAfterEdit(false);
    if (reopenId) openDrawer(reopenId);
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
        contract_id: (editing.contract_id || "").trim(),
        schema: editing.schema,
        models: editing.models,
        enabled: editing.enabled,
        run_after_transfer: editing.run_after_transfer,
        trigger_tables: editing.trigger_tables,
        description: editing.description,
      };
      if (editing.id) {
        await updateTransformProject(editing.id, {
          ...body,
          expected_version: editing.version ?? 0,
        });
        toast({ title: "Transform updated.", tone: "success" });
        const reopenId = editing.id;
        setEditing(null);
        setResumeDrawerAfterEdit(false);
        await load();
        openDrawer(reopenId);
      } else {
        const created = await createTransformProject(body);
        toast({ title: "Transform created.", tone: "success" });
        setEditing(null);
        setResumeDrawerAfterEdit(false);
        await load();
        openDrawer(created.id);
      }
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
      if (selectedId === project.id) {
        setDrawerOpen(false);
        setSelectedId(null);
      }
      if (lastRunProjectId === project.id) {
        setLastRun(null);
        setLastRunProjectId(null);
      }
      await load();
    } catch (err) {
      toast({ title: err instanceof Error ? err.message : "Could not delete transform", tone: "error" });
    }
  };


  const handleExportDbt = async (project: TransformProject) => {
    try {
      const pack = await exportTransformProjectDbt(project.id);
      const blob = new Blob([JSON.stringify(pack, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `datawrap-dbt-${project.name || project.id}.json`;
      a.click();
      URL.revokeObjectURL(url);
      toast({
        title: "dbt pack exported",
        message: `${pack.file_count} file(s). Complement hook only — not dbt Cloud.`,
        tone: "success",
      });
    } catch (err) {
      toast({ title: err instanceof Error ? err.message : "dbt export failed", tone: "error" });
    }
  };

  const handleRun = async (project: TransformProject, dryRun: boolean) => {
    setRunningId(project.id);
    try {
      const result = await runTransformProject(project.id, { dryRun });
      setLastRun(result);
      setLastRunProjectId(project.id);
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
      className="df2-page-transforms"
      title="Transforms"
      description="Post-load SQL models that run at the destination after a transfer lands — open a row for Dry run, Run, Export, or Edit."
    >
      <PageFrame>
        {editing ? (
          <PageSection
            title={editing.id ? "Edit transform" : "New transform"}
            subtitle="SELECT models · runner materializes VIEW / TABLE / incremental MERGE."
            actions={
              <>
                <Button variant="ghost" onClick={cancelEdit} disabled={saving}>
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
                <span>Linked data contract (optional)</span>
                <input
                  value={editing.contract_id || ""}
                  onChange={(e) => setEditing({ ...editing, contract_id: e.target.value })}
                  placeholder="dfc-… — when set, post-load auto-run requires SIGNED"
                />
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
            {/* With nothing to search or list, the toolbar only duplicated the
                empty state's own action. */}
            {projects.length > 0 && (
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
            )}

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
                    <>
                      <Button variant="primary" leadingIcon={<DtIcon name="plus" size={14} />} onClick={openCreate}>
                        New transform
                      </Button>
                      {/* A model runs against a landed destination table, so
                          the honest next step when nothing has landed is the
                          transfer, not another empty form. */}
                      {onNavigate && (
                        <Button variant="secondary" leadingIcon={<DtIcon name="arrow-right" size={14} />} onClick={() => onNavigate("transfer")}>
                          Run a transfer first
                        </Button>
                      )}
                    </>
                  ) : undefined
                }
                page
              />
            ) : (
              <div className="df2-pipeline-rows df2-xform-rows" role="list" aria-label="Transforms">
                <div className="df2-pipeline-rows-head df2-xform-rows-head" aria-hidden>
                  <span className="df2-pipeline-rows-head-name">Transform</span>
                  <span>Destination</span>
                  <span>Models</span>
                  <span>Trigger</span>
                  <span>Status</span>
                  <span />
                </div>
                {filtered.map((project) => {
                  const modelCount = project.models.length;
                  const trigger = project.run_after_transfer
                    ? project.trigger_tables.length
                      ? project.trigger_tables.join(", ")
                      : "Any table"
                    : "Manual";
                  return (
                    <div
                      key={project.id}
                      className={[
                        "df2-pipeline-row",
                        "df2-xform-row",
                        "df2-card-interactive",
                        project.enabled ? "is-active" : "is-paused",
                        drawerOpen && selectedId === project.id ? "selected" : "",
                      ]
                        .filter(Boolean)
                        .join(" ")}
                      role="button"
                      tabIndex={0}
                      aria-current={drawerOpen && selectedId === project.id ? true : undefined}
                      onClick={() => openDrawer(project.id)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter" || e.key === " ") {
                          e.preventDefault();
                          openDrawer(project.id);
                        }
                      }}
                    >
                      <span
                        className={`df2-health-dot ${project.enabled ? "ok" : "err"}`}
                        aria-hidden
                        title={project.enabled ? "Enabled" : "Disabled"}
                      />
                      <span className="df2-pipeline-row-icons" aria-hidden>
                        <DtIcon name="layers" size={16} />
                      </span>
                      <div className="df2-pipeline-row-identity">
                        <span className="df2-pipeline-row-name">{project.name}</span>
                        <span className="df2-pipeline-row-meta">
                          v{project.version ?? 0}
                          {project.schema ? ` · ${project.schema}` : ""}
                        </span>
                      </div>
                      <span className="df2-pipeline-row-cadence" title={project.destination_connector_id}>
                        {connectorName(project.destination_connector_id)}
                      </span>
                      <span className="df2-pipeline-row-sync">
                        {modelCount} model{modelCount === 1 ? "" : "s"}
                      </span>
                      <span className="df2-pipeline-row-signal" title={trigger}>
                        {project.run_after_transfer ? "Auto" : "Manual"}
                      </span>
                      <span
                        className={`df2-badge ${project.enabled ? "df2-badge-live" : "df2-badge-muted"}`}
                      >
                        {project.enabled ? "Enabled" : "Disabled"}
                      </span>
                      <span className="df2-pipeline-row-open" aria-hidden>
                        <DtIcon name="chevron-right" size={16} />
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
          </>
        )}

        {!editing && (
          <TransformDetailDrawer
            open={drawerOpen && Boolean(selectedProject)}
            project={selectedProject}
            destinationName={
              selectedProject
                ? connectorName(selectedProject.destination_connector_id)
                : undefined
            }
            running={Boolean(selectedProject && runningId === selectedProject.id)}
            lastRun={
              selectedProject && lastRunProjectId === selectedProject.id ? lastRun : null
            }
            onClose={closeDrawer}
            onDryRun={() => selectedProject && void handleRun(selectedProject, true)}
            onRun={() => selectedProject && void handleRun(selectedProject, false)}
            onExportDbt={() => selectedProject && void handleExportDbt(selectedProject)}
            onEdit={() => {
              if (!selectedProject) return;
              setResumeDrawerAfterEdit(true);
              closeDrawer();
              openEdit(selectedProject);
            }}
            onDelete={() => selectedProject && void handleDelete(selectedProject)}
          />
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
          {m.note && <p className="df2-xform-hint">{m.note}</p>}
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
