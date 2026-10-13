import { lazy, Suspense, useEffect, useMemo, useRef, useState, type KeyboardEvent, type PointerEvent as ReactPointerEvent, type ReactNode } from "react";
import { DtIcon } from "../DtIcon";
import { Dialog } from "../ui/Dialog";
import { Drawer } from "../ui/Drawer";
import { StudioPicker } from "../ui/StudioPicker";
import { ShapeStepFields } from "./ShapeStepFields";
import { StudioCommandPalette } from "./StudioCommandPalette";
import { checkShapeExpression, previewShapeRecipe } from "../../lib/api";
import { paletteActions } from "../../lib/canvasPalette";
import {
  chainDocument,
  documentKind,
  draftWithDefaults,
  reorderByDrag,
  setDocumentOf,
  stepsToFlow,
  stepsWithoutPlanOps,
  type FlowPoint,
  type PendingStepModel,
} from "../../lib/canvasDocument";
import {
  inspectorOperation,
  plannedSpecs,
  registryFromCatalog,
  runnableSpecs,
  setDocumentHash,
  specByOp,
  specsByFamily,
} from "../../lib/canvasRegistry";
import {
  divertMatchingRows,
  dropEmptyColumns,
  hashIdentityKey,
  parseDateColumns,
  trimTextColumns,
} from "../../lib/canvasTemplates";
import { createUndoStack, redo, sameSnapshot, undo, undoPush } from "../../lib/canvasUndo";
import {
  isBlankOption,
  moveStep,
  removeStep,
  settleExpressionCheck,
  stepFromDraft,
  type ShapeCatalog,
  type ShapeStepWire,
  type ShapeSuggestion,
} from "../../lib/shape";

const TransformFlow = lazy(() => import("./TransformFlow"));

interface TransferTransformCanvasProps {
  catalog: ShapeCatalog | null;
  steps: ShapeStepWire[];
  onChangeSteps: (steps: ShapeStepWire[]) => void;
  columns: { name: string; type: string }[];
  suggestions: ShapeSuggestion[];
  canPlan: boolean;
  disabledReason: string;
  syncRefused: boolean;
  refusalReason: string;
  activeTable: string;
  previewBanner: string;
  effectSummary: string;
  onRunPreview: () => void;
  onContinue: () => void;
  canContinue: boolean;
  onOpenSchema: () => void;
  /** Business rules, suggestions, and the rule ledger. Same elements as the classic step. */
  rules?: ReactNode;
  repairs?: ReactNode;
  ledger?: ReactNode;
  previewColumns: string[];
  previewRows: Record<string, unknown>[];
  previewStatus: string;
  /** Source sample. A node preview runs the recipe through that step only. */
  sampleRows?: Record<string, unknown>[];
  onApplySuggestion?: (suggestion: ShapeSuggestion) => void;
}

const SHORTCUTS: { keys: string; action: string }[] = [
  { keys: "?", action: "Show these shortcuts" },
  { keys: "/", action: "Filter the catalog" },
  { keys: "N", action: "Start adding a step" },
  { keys: "Delete", action: "Remove the selected step" },
  { keys: "↑ / ↓", action: "Move the selected step" },
  { keys: "Enter", action: "Open the inspector" },
  { keys: "Esc", action: "Close the inspector" },
  { keys: "Ctrl+Enter", action: "Continue when the preview matches" },
  { keys: "Ctrl+Z", action: "Undo" },
  { keys: "Ctrl+Shift+Z", action: "Redo" },
  { keys: "Ctrl+K", action: "Command palette" },
  { keys: "Ctrl+Shift+F", action: "Zoom to fit" },
  { keys: "Ctrl+Shift+L", action: "Arrange the chain" },
];

/**
 * Row-local recipe as an ordered chain. Set operations stay plan cards and
 * are not written into the recipe.
 */
export function TransferTransformCanvas(props: TransferTransformCanvasProps) {
  const {
    catalog,
    steps,
    onChangeSteps,
    columns,
    suggestions,
    canPlan,
    disabledReason,
    syncRefused,
    refusalReason,
    activeTable,
    previewBanner,
    effectSummary,
    onRunPreview,
    onContinue,
    canContinue,
    onOpenSchema,
    rules,
    repairs,
    ledger,
    previewColumns,
    previewRows,
    previewStatus,
    sampleRows = [],
    onApplySuggestion,
  } = props;
  const stackRef = useRef(createUndoStack(steps));
  const filterRef = useRef<HTMLInputElement>(null);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<number | null>(null);
  const [draftOp, setDraftOp] = useState("");
  const [draftColumn, setDraftColumn] = useState("");
  const [draftOptions, setDraftOptions] = useState<Record<string, unknown>>({});
  const [draftLabel, setDraftLabel] = useState("");
  const [draftPolicy, setDraftPolicy] = useState("refuse");
  const [expressionError, setExpressionError] = useState("");
  const [formError, setFormError] = useState("");
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const [templateExpression, setTemplateExpression] = useState("");
  const [templateFormat, setTemplateFormat] = useState("%Y-%m-%d");
  const [pendingAt, setPendingAt] = useState<number | null>(null);
  const [placed, setPlaced] = useState<{ index: number; x: number; y: number } | null>(null);
  const [linkRefusal, setLinkRefusal] = useState("");
  const [linkNote, setLinkNote] = useState("");
  const [rulesOpen, setRulesOpen] = useState(false);
  const [templatesOpen, setTemplatesOpen] = useState(false);
  const [dockHeight, setDockHeight] = useState(96);
  const [fitTick, setFitTick] = useState(0);
  const [runningIndex, setRunningIndex] = useState<number | null>(null);
  const [nodePreview, setNodePreview] = useState<{ index: number; rows: Record<string, unknown>[]; note: string } | null>(null);
  const expressionGen = useRef(0);
  const draftIndex = useRef<number | null>(null);
  const canEdit = canPlan && !syncRefused;

  useEffect(() => {
    if (!sameSnapshot(stackRef.current.present, steps)) {
      stackRef.current = createUndoStack(steps);
    }
  }, [steps]);

  const commit = (next: ShapeStepWire[]) => {
    const safe = stepsWithoutPlanOps(next, catalog?.post_load_only.operations ?? []).map((step) => (
      step.source_table || !activeTable ? step : { ...step, source_table: activeTable }
    ));
    stackRef.current = undoPush(stackRef.current, safe);
    onChangeSteps(stackRef.current.present);
  };

  const applyStack = (direction: "undo" | "redo") => {
    stackRef.current = direction === "undo" ? undo(stackRef.current) : redo(stackRef.current);
    onChangeSteps(stackRef.current.present);
    setSelected(null);
    setPendingAt(null);
    setPlaced(null);
    setLinkRefusal("");
    setLinkNote("");
  };

  const focusInspector = () => {
    window.setTimeout(() => document.getElementById("canvas-inspector")?.focus({ preventScroll: true }), 0);
  };

  const names = columns.map((column) => column.name);
  const registry = useMemo(() => registryFromCatalog(catalog), [catalog]);
  const graph = useMemo(() => chainDocument(steps), [steps]);
  const setOps = useMemo(
    () => new Set(registry.filter((spec) => spec.family === "set" || spec.placement !== "palette").map((spec) => spec.op)),
    [registry],
  );
  const flow = useMemo(
    () => stepsToFlow(steps, catalog?.operations ?? [], activeTable, syncRefused, runningIndex),
    [steps, catalog, activeTable, syncRefused, runningIndex],
  );
  const families = specsByFamily(runnableSpecs(registry, filter));
  const planned = plannedSpecs(registry);
  const plannedMatches = filter.trim() ? plannedSpecs(registry, filter) : [];
  const selectedStep = selected != null ? steps[selected] : undefined;
  const selectedSpec = specByOp(registry, selectedStep?.op ?? draftOp);
  const selectedOperation = inspectorOperation(selectedSpec) ?? catalog?.operations.find((operation) => operation.op === (selectedStep?.op ?? draftOp)) ?? null;
  const blankRequired = useMemo(() => {
    if (!selectedOperation) return new Set<string>();
    return new Set(selectedOperation.required.filter((name) => isBlankOption(name, draftOptions[name])));
  }, [selectedOperation, draftOptions]);

  const loadStep = (index: number) => {
    const step = steps[index];
    if (!step) return;
    draftIndex.current = index;
    setSelected(index);
    setDraftOp(step.op);
    setDraftColumn(step.column ?? "");
    setDraftOptions({ ...(step.options ?? {}) });
    setDraftLabel(step.label ?? "");
    setDraftPolicy(step.on_error ?? "refuse");
    setExpressionError("");
    setFormError("");
  };

  const clearPending = () => {
    setPendingAt(null);
    if (draftIndex.current == null) setDraftOp("");
  };

  const beginAdd = (op: string, atIndex = steps.length, atPoint?: FlowPoint) => {
    if (!canEdit) return;
    const spec = specByOp(registry, op);
    if (spec && !spec.executable) {
      setFormError(spec.reason);
      return;
    }
    const operation = catalog?.operations.find((item) => item.op === op);
    if (!operation) return;
    const defaults = draftWithDefaults(operation, names);
    const at = Math.max(0, Math.min(steps.length, atIndex));
    draftIndex.current = null;
    setDraftOp(op);
    setDraftColumn(defaults.column);
    setDraftOptions(defaults.options);
    setDraftLabel("");
    setDraftPolicy("refuse");
    setExpressionError("");
    setFormError("");
    setLinkRefusal("");
    setLinkNote("");
    const built = stepFromDraft(operation, defaults.column, defaults.options, "", "refuse");
    if (atPoint) setPlaced({ index: built.step ? at : -1, x: atPoint.x, y: atPoint.y });
    else setPlaced(null);
    if (built.step) {
      const next = steps.slice();
      next.splice(at, 0, built.step);
      setPendingAt(null);
      draftIndex.current = at;
      setSelected(at);
      commit(next);
      if (!atPoint) setFitTick((tick) => tick + 1);
      focusInspector();
      return;
    }
    setSelected(null);
    setPendingAt(at);
    focusInspector();
  };

  const validateExpression = async (text: string) => {
    const requestId = ++expressionGen.current;
    if (!text.trim()) {
      setExpressionError("");
      return;
    }
    try {
      const answer = await checkShapeExpression({ expression: text, source_columns: names });
      const next = settleExpressionCheck(requestId, expressionGen.current, answer);
      if (next !== undefined) setExpressionError(next);
    } catch (err) {
      const next = settleExpressionCheck(requestId, expressionGen.current, {
        error: err instanceof Error ? err.message : String(err),
      });
      if (next !== undefined) setExpressionError(next);
    }
  };

  const commitDraft = () => {
    if (!selectedOperation || !canEdit) return;
    if (expressionError) {
      setFormError(expressionError);
      return;
    }
    const built = stepFromDraft(selectedOperation, draftColumn, draftOptions, draftLabel, draftPolicy);
    if (!built.step) {
      setFormError(built.error);
      return;
    }
    const index = draftIndex.current;
    if (index != null && steps[index]) {
      const next = steps.slice();
      next[index] = { ...steps[index], ...built.step };
      commit(next);
      return;
    }
    const at = pendingAt ?? steps.length;
    const next = steps.slice();
    next.splice(at, 0, built.step);
    draftIndex.current = at;
    setPendingAt(null);
    setSelected(at);
    commit(next);
    setFormError("");
  };

  const insertTemplate = (incoming: ShapeStepWire[], error: string) => {
    setPendingAt(null);
    if (error || incoming.length === 0) {
      setFormError(error || "That template had no steps to add.");
      return;
    }
    commit([...steps, ...incoming]);
    setFormError("");
  };

  const runAction = (id: string) => {
    if (id === "preview") onRunPreview();
    else if (id === "continue" && canContinue) onContinue();
    else if (id === "schema") onOpenSchema();
    else if (id.startsWith("jump-")) loadStep(Number(id.slice(5)));
    else if (id.startsWith("add-")) beginAdd(id.slice(4));
    else if (id === "template-trim") {
      const result = trimTextColumns(columns, activeTable);
      insertTemplate(result.steps, result.error);
    } else if (id === "template-drop") {
      const result = dropEmptyColumns(suggestions, activeTable);
      insertTemplate(result.steps, result.error);
    } else if (id === "template-dates") {
      const result = parseDateColumns(columns, templateFormat, activeTable);
      insertTemplate(result.steps, result.error);
    } else if (id === "template-divert") {
      const result = divertMatchingRows(templateExpression, activeTable);
      insertTemplate(result.steps, result.error);
    } else if (id === "template-hash") {
      const key = names.slice(0, 1);
      const result = hashIdentityKey(key, activeTable);
      insertTemplate(result.steps, result.error);
    }
  };

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const target = event.target as HTMLElement;
    const typing = target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable;
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
      event.preventDefault();
      setPaletteOpen(true);
      return;
    }
    if (typing) return;
    if (event.key === "?") setHelpOpen(true);
    else if (event.key === "/") {
      event.preventDefault();
      filterRef.current?.focus();
    } else if (event.key === "n" || event.key === "N") filterRef.current?.focus();
    else if (event.key === "Escape") {
      setSelected(null);
      clearPending();
    }
    else if ((event.key === "Delete" || event.key === "Backspace") && selected != null && canEdit) {
      event.preventDefault();
      commit(removeStep(steps, selected));
      setSelected(null);
    } else if (event.key === "ArrowUp" && selected != null && canEdit) {
      event.preventDefault();
      commit(moveStep(steps, selected, -1));
    } else if (event.key === "ArrowDown" && selected != null && canEdit) {
      event.preventDefault();
      commit(moveStep(steps, selected, 1));
    } else if (event.key === "Enter" && selected != null) {
      document.getElementById("canvas-inspector")?.focus();
    } else if ((event.ctrlKey || event.metaKey) && event.key === "Enter" && canContinue) {
      event.preventDefault();
      onContinue();
    } else if ((event.ctrlKey || event.metaKey) && event.shiftKey && event.key.toLowerCase() === "z") {
      event.preventDefault();
      applyStack("redo");
    } else if ((event.ctrlKey || event.metaKey) && event.shiftKey && event.key.toLowerCase() === "f") {
      event.preventDefault();
      setFitTick((tick) => tick + 1);
    }     else if ((event.ctrlKey || event.metaKey) && event.shiftKey && event.key.toLowerCase() === "l") {
      event.preventDefault();
      arrange();
    } else if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "z") {
      event.preventDefault();
      applyStack("undo");
    }
  };

  const arrange = () => {
    setPlaced(null);
    setFitTick((tick) => tick + 1);
  };

  const previewNode = async (index: number) => {
    const step = steps[index];
    if (!step) return;
    if (sampleRows.length === 0) {
      setNodePreview({ index, rows: [], note: "No sampled rows to preview through this step." });
      return;
    }
    setRunningIndex(index);
    setNodePreview({ index, rows: [], note: "Previewing this step…" });
    try {
      const body = await previewShapeRecipe({
        recipe: { steps: steps.slice(0, index + 1) },
        sample_rows: sampleRows.slice(0, 12),
        source_columns: names,
        focus_table: activeTable,
      });
      const rows = Array.isArray(body.after) ? body.after : [];
      setNodePreview({
        index,
        rows,
        note: body.refusal?.message || `Through step ${index + 1}. This sample is not the population.`,
      });
    } catch (err) {
      setNodePreview({
        index,
        rows: [],
        note: err instanceof Error ? err.message : "Preview failed.",
      });
    } finally {
      setRunningIndex(null);
    }
  };

  const pending = useMemo<PendingStepModel | null>(() => {
    if (pendingAt == null || !selectedOperation || selected != null) return null;
    const built = stepFromDraft(selectedOperation, draftColumn, draftOptions, draftLabel, draftPolicy);
    return {
      op: selectedOperation.op,
      title: selectedOperation.summary,
      family: selectedOperation.family || "cleanse",
      reason: built.step
        ? "Ready. Add step to put it in the recipe."
        : (built.error || "Finish this step in the inspector. It is not in the recipe yet."),
      atIndex: pendingAt,
      invalid: !built.step,
    };
  }, [pendingAt, selectedOperation, selected, draftColumn, draftOptions, draftLabel, draftPolicy]);

  const actions = paletteActions({
    operations: (catalog?.operations ?? []).map((operation) => ({ op: operation.op, summary: operation.summary })),
    stepCount: steps.length,
    canContinue,
    canAdd: canEdit,
  });

  return (
    <div
      className="df2-studio-next df2-canvas"
      data-testid="transform-canvas"
      data-document={documentKind(graph)}
      data-set-hash={setDocumentHash(setDocumentOf(graph, setOps))}
      tabIndex={0}
      onKeyDown={onKeyDown}
    >
      <header className="df2-canvas-head">
        <div>
          <h3>Recipe</h3>
          <p>
            {activeTable ? `Steps for ${activeTable}. Other tables stay in the recipe, dimmed.` : "Steps run in order on the read."}
          </p>
        </div>
        <div className="df2-canvas-head-actions">
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => setRulesOpen(true)}>
            Rules and suggestions
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => setTemplatesOpen(true)}>
            Templates
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => applyStack("undo")} aria-label="Undo">
            Undo
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => applyStack("redo")} aria-label="Redo">
            Redo
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => setFitTick((tick) => tick + 1)} aria-label="Fit view">
            Fit
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={arrange} aria-label="Arrange">
            Arrange
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => setHelpOpen(true)} aria-label="Keyboard shortcuts">
            Shortcuts
          </button>
        </div>
      </header>
      {syncRefused && (
        <p className="df2-canvas-refusal" role="status">{refusalReason} Nodes stay visible and cannot be added.</p>
      )}
      {!canPlan && <p className="df2-label-hint">{disabledReason}</p>}
      {formError && <p className="df2-qw-schema-error" role="alert">{formError}</p>}

      <div className="df2-canvas-body">
        <aside className="df2-canvas-rail" aria-label="Operations">
          <input
            ref={filterRef}
            className="df2-input df2-input-sm"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
            placeholder="Search operations"
            aria-label="Search operations"
          />
          {families.map((group) => (
            <section key={group.family}>
              <h4>{group.label}</h4>
              <ul className="df2-canvas-tiles">
                {group.operations.map((operation) => (
                  <li key={operation.op}>
                    <button
                      type="button"
                      className="df2-canvas-op"
                      draggable={canEdit && operation.executable}
                      disabled={!canEdit || !operation.executable}
                      title={operation.summary}
                      aria-label={operation.summary}
                      data-tip={operation.summary}
                      data-op={operation.op}
                      data-stripe={operation.stripe}
                      data-icon={operation.icon}
                      data-executable="true"
                      onDragStart={(event) => {
                        event.dataTransfer.setData("application/x-df-op", operation.op);
                        event.dataTransfer.effectAllowed = "copy";
                      }}
                      onClick={() => beginAdd(operation.op)}
                    >
                      <span className="df2-canvas-op-icon" aria-hidden>
                        <DtIcon name={operation.icon} size={18} chrome="product" />
                      </span>
                      <span className="df2-canvas-op-copy">
                        <span className="df2-canvas-op-name">{operation.short}</span>
                        <span className="df2-canvas-op-family">{operation.familyLabel}</span>
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          ))}
          {plannedMatches.length > 0 && (
            <section>
              <h4>Not executable yet</h4>
              <ul className="df2-canvas-tiles">
                {plannedMatches.map((operation) => (
                  <li key={operation.op}>
                    <button
                      type="button"
                      className="df2-canvas-op"
                      disabled
                      title={operation.reason}
                      aria-label={operation.summary}
                      data-op={operation.op}
                      data-stripe={operation.stripe}
                      data-icon={operation.icon}
                      data-executable="false"
                    >
                      <span className="df2-canvas-op-icon" aria-hidden>
                        <DtIcon name={operation.icon} size={18} chrome="product" />
                      </span>
                      <span className="df2-canvas-op-copy">
                        <span className="df2-canvas-op-name">{operation.short}</span>
                        <span className="df2-canvas-op-family">{operation.familyLabel}</span>
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          )}
          {suggestions.slice(0, 3).length > 0 && (
            <section>
              <h4>Suggested next</h4>
              <ul>
                {suggestions.slice(0, 3).map((suggestion) => (
                  <li key={suggestion.id}>
                    <button
                      type="button"
                      className="df2-canvas-suggest"
                      disabled={!canEdit || !onApplySuggestion}
                      onClick={() => onApplySuggestion?.(suggestion)}
                    >
                      {suggestion.title}
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          )}
          {pending?.invalid && (
            <ul className="df2-canvas-tiles" data-testid="canvas-validation">
              <li>
                <button type="button" className="df2-canvas-suggest" onClick={focusInspector}>
                  {pending.reason}
                </button>
              </li>
            </ul>
          )}
          {planned.length > 0 && (
            <details className="df2-canvas-sql">
              <summary>Warehouse SQL (runs after load)</summary>
              <p className="df2-label-hint">These stay disabled until the catalog marks them executable. They are not steps on this canvas.</p>
              <ul className="df2-canvas-tiles">
                {planned.map((operation) => (
                  <li key={operation.op}>
                    <button
                      type="button"
                      className="df2-canvas-op"
                      disabled
                      title={operation.reason}
                      aria-label={operation.summary}
                      data-op={operation.op}
                      data-stripe={operation.stripe}
                      data-icon={operation.icon}
                      data-executable="false"
                    >
                      <span className="df2-canvas-op-icon" aria-hidden>
                        <DtIcon name={operation.icon} size={18} chrome="product" />
                      </span>
                      <span className="df2-canvas-op-copy">
                        <span className="df2-canvas-op-name">{operation.short}</span>
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
              <a href="#/transforms">Open SQL models</a>
            </details>
          )}
        </aside>

        <div className="df2-canvas-stage-wrap">
          <Suspense fallback={<p className="df2-label-hint" role="status" aria-busy="true">Loading the canvas…</p>}>
            <TransformFlow
              nodes={flow.nodes}
              edges={flow.edges}
              pending={pending}
              placed={placed}
              onSelect={loadStep}
              onReorder={(from, to) => {
                if (!canEdit) return;
                setPendingAt(null);
                setPlaced(null);
                commit(reorderByDrag(steps, from, to));
              }}
              onInvalidLink={(reason) => {
                setLinkNote("");
                setLinkRefusal(reason);
              }}
              onAcceptLink={(message) => {
                setLinkRefusal("");
                setLinkNote(message);
              }}
              onDropOp={(op, at, index) => beginAdd(op, index, at)}
              onPreviewNode={(index) => void previewNode(index)}
              fitTick={fitTick}
            />
          </Suspense>
          {linkNote && <p className="df2-canvas-link-note" role="status">{linkNote}</p>}
          {linkRefusal && <p className="df2-canvas-link-refusal" role="alert">{linkRefusal}</p>}
          <PreviewDock
            height={dockHeight}
            onHeight={setDockHeight}
            status={previewStatus || previewBanner}
            effect={effectSummary}
            columns={previewColumns}
            rows={previewRows}
            onRunPreview={onRunPreview}
          />
        </div>

        <aside className="df2-canvas-inspector" id="canvas-inspector" tabIndex={-1} aria-label="Step inspector">
          <h4>{selected != null ? "Edit step" : draftOp ? "New step" : "Inspector"}</h4>
          {!selectedOperation && <p className="df2-label-hint">Choose an operation, or select a step.</p>}
          {selectedOperation && (
            <>
              <p className="df2-label-hint">{selectedOperation.summary}</p>
              {selectedOperation.needs_column && (
                <StudioPicker
                  id="canvas-column"
                  label="Column"
                  value={draftColumn}
                  options={names.map((name) => ({ value: name, label: name }))}
                  placeholder="Pick a column…"
                  disabled={!canEdit}
                  required
                  onChange={setDraftColumn}
                />
              )}
              <ShapeStepFields
                operation={selectedOperation}
                columns={names}
                options={draftOptions}
                onChange={setDraftOptions}
                onCommit={commitDraft}
                canPlan={canEdit}
                expressionError={expressionError}
                onExpressionChange={(text) => void validateExpression(text)}
                idPrefix="canvas-opt"
                blankRequired={blankRequired}
              />
              <label className="df2-label" htmlFor="canvas-label">
                Step name
                <input
                  id="canvas-label"
                  className="df2-input"
                  value={draftLabel}
                  disabled={!canEdit}
                  onChange={(event) => setDraftLabel(event.target.value)}
                  onBlur={commitDraft}
                />
              </label>
              <div className="df2-canvas-inspector-actions">
                {selected != null && (
                  <button
                    type="button"
                    className="df2-btn df2-btn-ghost df2-btn-sm"
                    disabled={runningIndex != null}
                    onClick={() => void previewNode(selected)}
                  >
                    Preview this step
                  </button>
                )}
                <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" disabled={!canEdit} onClick={commitDraft}>
                  {selected != null ? "Update step" : "Add step"}
                </button>
                {pending && (
                  <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={clearPending}>
                    Discard
                  </button>
                )}
              </div>
              {nodePreview && selected === nodePreview.index && (
                <div data-testid="canvas-node-preview">
                  <p className="df2-label-hint">{nodePreview.note}</p>
                  {nodePreview.rows[0] && (
                    <p className="df2-canvas-node-summary">
                      {Object.keys(nodePreview.rows[0]).slice(0, 4).join(", ")} · {nodePreview.rows.length} row(s)
                    </p>
                  )}
                </div>
              )}
            </>
          )}
        </aside>
      </div>

      <Drawer
        open={rulesOpen}
        onClose={() => setRulesOpen(false)}
        title="Rules and suggestions"
        subtitle="The same cards as the classic editor."
      >
        {rules}
        {repairs}
        {ledger}
      </Drawer>
      <Dialog
        open={templatesOpen}
        onClose={() => setTemplatesOpen(false)}
        title="Templates"
        subtitle="Recipes made of several steps. They also appear in the command menu."
        size="md"
      >
        <label className="df2-label" htmlFor="canvas-date-format">Date format</label>
        <input
          id="canvas-date-format"
          className="df2-input df2-input-sm"
          value={templateFormat}
          onChange={(event) => setTemplateFormat(event.target.value)}
          aria-label="Explicit date format"
        />
        <label className="df2-label" htmlFor="canvas-divert">Divert condition</label>
        <input
          id="canvas-divert"
          className="df2-input df2-input-sm"
          value={templateExpression}
          onChange={(event) => setTemplateExpression(event.target.value)}
          placeholder="[status] = 'void'"
          aria-label="Divert condition"
        />
        <div className="df2-canvas-inspector-actions">
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-trim")}>Trim text columns</button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-drop")}>Drop empty columns</button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-dates")}>Parse dates</button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-divert")}>Divert rows</button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-hash")}>Hash identity key</button>
        </div>
      </Dialog>
      <StudioCommandPalette
        open={paletteOpen}
        actions={actions}
        onClose={() => setPaletteOpen(false)}
        onRun={runAction}
      />
      <Dialog open={helpOpen} onClose={() => setHelpOpen(false)} title="Keyboard shortcuts" size="md">
        <ul className="df2-canvas-shortcuts">
          {SHORTCUTS.map((row) => (
            <li key={row.keys}><kbd>{row.keys}</kbd> {row.action}</li>
          ))}
        </ul>
      </Dialog>
    </div>
  );
}

function cellText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function PreviewDock({
  height,
  onHeight,
  status,
  effect,
  columns,
  rows,
  onRunPreview,
}: {
  height: number;
  onHeight: (height: number) => void;
  status: string;
  effect: string;
  columns: string[];
  rows: Record<string, unknown>[];
  onRunPreview: () => void;
}) {
  const shown = columns.slice(0, 6);
  const gripDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    event.preventDefault();
    const startY = event.clientY;
    const startH = height;
    const move = (next: PointerEvent) => {
      onHeight(Math.min(420, Math.max(96, startH + (startY - next.clientY))));
    };
    const up = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };
  return (
    <div className="df2-canvas-dock" style={{ height }} data-testid="canvas-preview">
      <div
        className="df2-canvas-dock-grip"
        role="separator"
        aria-orientation="horizontal"
        aria-label="Resize preview"
        aria-valuemin={96}
        aria-valuemax={420}
        aria-valuenow={height}
        tabIndex={0}
        onPointerDown={gripDown}
        onKeyDown={(event) => {
          if (event.key === "ArrowUp") onHeight(Math.min(420, height + 24));
          if (event.key === "ArrowDown") onHeight(Math.max(96, height - 24));
        }}
      />
      <div className="df2-canvas-dock-body">
        <div className="df2-canvas-dock-head">
          <p data-testid="canvas-preview-status" role="status">{status}</p>
          <button type="button" className="df2-btn df2-btn-sm" onClick={onRunPreview}>Run preview</button>
        </div>
        {effect && <p className="df2-label-hint">{effect}</p>}
        {rows.length > 0 && shown.length > 0 && (
          <table>
            <thead>
              <tr>{shown.map((column) => <th key={column}>{column}</th>)}</tr>
            </thead>
            <tbody>
              {rows.slice(0, 8).map((row, index) => (
                <tr key={index}>
                  {shown.map((column) => <td key={column}>{cellText(row[column])}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
