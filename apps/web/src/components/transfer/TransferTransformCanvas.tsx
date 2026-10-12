import { lazy, Suspense, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { DtIcon } from "../DtIcon";
import { Dialog } from "../ui/Dialog";
import { StudioPicker } from "../ui/StudioPicker";
import { ShapeStepFields } from "./ShapeStepFields";
import { StudioCommandPalette } from "./StudioCommandPalette";
import { checkShapeExpression } from "../../lib/api";
import { paletteActions } from "../../lib/canvasPalette";
import {
  planCards,
  reorderOnDisconnect,
  stepsToFlow,
  stepsWithoutPlanOps,
} from "../../lib/canvasDocument";
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
  operationsByFamily,
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
  };

  const names = columns.map((column) => column.name);
  const flow = useMemo(
    () => stepsToFlow(steps, catalog?.operations ?? [], activeTable, syncRefused),
    [steps, catalog, activeTable, syncRefused],
  );
  const families = operationsByFamily(
    (catalog?.operations ?? []).filter((operation) => (
      !filter.trim()
      || operation.op.includes(filter.trim().toLowerCase())
      || operation.summary.toLowerCase().includes(filter.trim().toLowerCase())
    )),
  );
  const cards = planCards(catalog?.post_load_only.operations ?? []);
  const selectedStep = selected != null ? steps[selected] : undefined;
  const selectedOperation = catalog?.operations.find((operation) => operation.op === (selectedStep?.op ?? draftOp));
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

  const beginAdd = (op: string) => {
    if (!canEdit) return;
    draftIndex.current = null;
    setSelected(null);
    setDraftOp(op);
    setDraftColumn("");
    setDraftOptions({});
    setDraftLabel("");
    setDraftPolicy("refuse");
    setExpressionError("");
    setFormError("");
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
    draftIndex.current = steps.length;
    setSelected(steps.length);
    commit([...steps, built.step]);
    setFormError("");
  };

  const insertTemplate = (incoming: ShapeStepWire[], error: string) => {
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
    else if (event.key === "Escape") setSelected(null);
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
    } else if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "z") {
      event.preventDefault();
      applyStack("undo");
    }
  };

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
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => applyStack("undo")} aria-label="Undo">
            Undo
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => applyStack("redo")} aria-label="Redo">
            Redo
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => setHelpOpen(true)} aria-label="Keyboard shortcuts">
            Shortcuts
          </button>
        </div>
      </header>
      <p className="df2-canvas-banner" role="status">{previewBanner}</p>
      {effectSummary && <p className="df2-label-hint">{effectSummary}</p>}
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
            placeholder="Filter operations…"
            aria-label="Filter operations"
          />
          {families.map((group) => (
            <section key={group.family}>
              <h4>{group.label}</h4>
              <ul>
                {group.operations.map((operation) => (
                  <li key={operation.op}>
                    <button
                      type="button"
                      className="df2-canvas-op"
                      draggable={canEdit}
                      disabled={!canEdit}
                      onDragStart={(event) => event.dataTransfer.setData("application/x-df-op", operation.op)}
                      onClick={() => beginAdd(operation.op)}
                    >
                      <DtIcon name="layers" size={16} chrome="product" />
                      <span>{operation.summary}</span>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          ))}
          <section>
            <h4>Templates</h4>
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
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-trim")}>Trim text columns</button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-drop")}>Drop empty columns</button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-dates")}>Parse dates</button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-divert")}>Divert rows</button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" disabled={!canEdit} onClick={() => runAction("template-hash")}>Hash identity key</button>
          </section>
          {cards.length > 0 && (
            <section aria-label="After load">
              <h4>After load</h4>
              <ul>
                {cards.map((card) => (
                  <li key={card.op} className="df2-canvas-plan">
                    <p>{card.op}</p>
                    <p>{card.caption}</p>
                    <a href={card.href}>Open SQL models</a>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </aside>

        <div
          className="df2-canvas-stage"
          onDragOver={(event) => {
            if (canEdit) event.preventDefault();
          }}
          onDrop={(event) => {
            const op = event.dataTransfer.getData("application/x-df-op");
            if (op) beginAdd(op);
          }}
        >
          <Suspense fallback={<p className="df2-label-hint" role="status" aria-busy="true">Loading the canvas…</p>}>
            <TransformFlow
              nodes={flow.nodes}
              edges={flow.edges}
              onSelect={loadStep}
              onDisconnect={(index) => {
                if (!canEdit) return;
                commit(reorderOnDisconnect(steps, index));
              }}
            />
          </Suspense>
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
              <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" disabled={!canEdit} onClick={commitDraft}>
                {selected != null ? "Update step" : "Add step"}
              </button>
            </>
          )}
        </aside>
      </div>

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
