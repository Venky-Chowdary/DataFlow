import { lazy, Suspense, useEffect, useMemo, useRef, useState, type KeyboardEvent, type PointerEvent as ReactPointerEvent, type ReactNode } from "react";
import { DtIcon } from "../DtIcon";
import { ButtonGroup } from "../ui/ButtonGroup";
import { Dialog } from "../ui/Dialog";
import { Drawer } from "../ui/Drawer";
import { StudioPicker } from "../ui/StudioPicker";
import { ShapeStepFields } from "./ShapeStepFields";
import { StudioCommandPalette } from "./StudioCommandPalette";
import { checkShapeExpression, previewShapeRecipe } from "../../lib/api";
import { usePointerDrag } from "../../lib/pointerDrag";
import { paletteActions } from "../../lib/canvasPalette";
import {
  chainDocument,
  columnsBeforeStep,
  documentKind,
  stepExpressionIssues,
  draftWithDefaults,
  layoutFor,
  positionClearOf,
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
  shortName,
  specByOp,
  specsByFamily,
  statusLabel,
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
  unknownExpressionColumn,
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
  /** Source format for the step preview's value model. */
  sourceType?: string;
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
  /** Refusals for the current recipe. Index is 0-based; -1 is the recipe. */
  issues?: ReadonlyArray<{ index: number; message: string }>;
  /** When set, the parent owns which node is open. Omit it to keep selection local. */
  selectedStep?: number | null;
  onSelectedStepChange?: (index: number | null) => void;
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
  { keys: "Ctrl+Shift+L", action: "Auto-arrange" },
];

const SPLIT_KEY = "df.canvasSplit";
type CanvasView = "canvas" | "split" | "properties";
type PropertyTab = "general" | "fields" | "op" | "preview";
type PreviewTab = "data" | "compare" | "schema" | "issues";

function readSplit(): { mode: CanvasView; height: number } {
  try {
    const raw = localStorage.getItem(SPLIT_KEY);
    if (!raw) return { mode: "split", height: 168 };
    const parsed = JSON.parse(raw) as { mode?: string; height?: number };
    const mode: CanvasView = parsed.mode === "canvas" || parsed.mode === "properties" || parsed.mode === "split" ? parsed.mode : "split";
    const height = typeof parsed.height === "number" ? Math.min(420, Math.max(96, parsed.height)) : 168;
    return { mode, height };
  } catch {
    return { mode: "split", height: 168 };
  }
}

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
    sourceType = "",
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
    issues = [],
    selectedStep: selectedStepProp,
    onSelectedStepChange,
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
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewMax, setPreviewMax] = useState(false);
  const [paletteDock, setPaletteDock] = useState(true);
  const [opTip, setOpTip] = useState<{ text: string; x: number; y: number } | null>(null);
  const resizeProperties = usePointerDrag();
  const [validationOpen, setValidationOpen] = useState(false);
  const [overview, setOverview] = useState(true);
  const [menuOpen, setMenuOpen] = useState(false);
  const [viewMode, setViewMode] = useState<CanvasView>(() => readSplit().mode);
  const [propertiesHeight, setPropertiesHeight] = useState(() => readSplit().height);
  const [propertyTab, setPropertyTab] = useState<PropertyTab>("op");
  const [previewTab, setPreviewTab] = useState<PreviewTab>("data");
  const [previewFilter, setPreviewFilter] = useState("");
  const [outputGroup, setOutputGroup] = useState<"kept" | "diverted">("kept");
  const [edgeLabels, setEdgeLabels] = useState<Record<string, string>>({});
  const insertAt = useRef<number | null>(null);
  const [focusMode, setFocusMode] = useState(false);
  const [zoomCommand, setZoomCommand] = useState<{ id: number; delta: number; absolute?: number }>({ id: 0, delta: 0 });
  const [centerCommand, setCenterCommand] = useState({ id: 0, index: 0 });
  const [zoomLevel, setZoomLevel] = useState(1);
  const [fitTick, setFitTick] = useState(0);
  const [runningIndex, setRunningIndex] = useState<number | null>(null);
  const [nodePreview, setNodePreview] = useState<{
    index: number;
    rows: Record<string, unknown>[];
    before: Record<string, unknown>[];
    diverted: Record<string, unknown>[];
    columns: string[];
    types: Record<string, string>;
    note: string;
    disabled: boolean;
  } | null>(null);
  const expressionGen = useRef(0);
  const draftIndex = useRef<number | null>(null);
  const echoStep = useRef<number | null | undefined>(undefined);
  const stepsRef = useRef(steps);
  stepsRef.current = steps;
  const canEdit = canPlan && !syncRefused;

  const chooseStep = (index: number | null) => {
    echoStep.current = index;
    setSelected(index);
    onSelectedStepChange?.(index);
  };

  useEffect(() => {
    if (!sameSnapshot(stackRef.current.present, steps)) {
      stackRef.current = createUndoStack(steps);
    }
  }, [steps]);

  useEffect(() => {
    const root = document.documentElement;
    if (focusMode) root.classList.add("df2-canvas-focus");
    else root.classList.remove("df2-canvas-focus");
    return () => root.classList.remove("df2-canvas-focus");
  }, [focusMode]);

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
    chooseStep(null);
    setPendingAt(null);
    setPlaced(null);
    setLinkRefusal("");
    setLinkNote("");
  };

  const focusInspector = () => {
    window.setTimeout(() => document.getElementById("canvas-inspector")?.focus({ preventScroll: true }), 0);
  };

  const sourceNames = columns.map((column) => column.name);
  const names = columnsBeforeStep(steps, selected ?? pendingAt ?? steps.length, sourceNames);
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
  const selectedWire = selected != null ? steps[selected] : undefined;
  const selectedSpec = specByOp(registry, selectedWire?.op ?? draftOp);
  const selectedOperation = inspectorOperation(selectedSpec) ?? catalog?.operations.find((operation) => operation.op === (selectedWire?.op ?? draftOp)) ?? null;
  const blankRequired = useMemo(() => {
    if (!selectedOperation) return new Set<string>();
    return new Set(selectedOperation.required.filter((name) => isBlankOption(name, draftOptions[name])));
  }, [selectedOperation, draftOptions]);

  const loadStep = (index: number) => {
    const step = stepsRef.current[index];
    if (!step) return;
    const same = draftIndex.current === index;
    draftIndex.current = index;
    chooseStep(index);
    setDraftOp(step.op);
    setDraftColumn(step.column ?? "");
    if (!same) {
      setDraftOptions({ ...(step.options ?? {}) });
      setDraftLabel(step.label ?? "");
      setDraftPolicy(step.on_error ?? "refuse");
      setExpressionError("");
      setFormError("");
    }
    setViewMode((mode) => (mode === "canvas" ? "split" : mode));
  };

  const revealStep = (index: number) => {
    if (!stepsRef.current[index]) return;
    loadStep(index);
    setViewMode((mode) => (mode === "canvas" ? "split" : mode));
    setPropertyTab("op");
    setCenterCommand((current) => ({ id: current.id + 1, index }));
    focusInspector();
  };

  useEffect(() => {
    if (selectedStepProp === undefined) return;
    if (selectedStepProp != null && !stepsRef.current[selectedStepProp]) return;
    if (echoStep.current === selectedStepProp) return;
    echoStep.current = selectedStepProp;
    if (selectedStepProp == null) {
      setSelected(null);
      return;
    }
    revealStep(selectedStepProp);
  }, [selectedStepProp, steps.length]);

  const clearPending = () => {
    setPendingAt(null);
    if (draftIndex.current == null) setDraftOp("");
  };

  const beginAdd = (op: string, atIndex = steps.length, atPoint?: FlowPoint) => {
    if (!canEdit) return;
    setViewMode((mode) => (mode === "canvas" ? "split" : mode));
    const spec = specByOp(registry, op);
    if (spec && !spec.executable) {
      setFormError(spec.reason);
      return;
    }
    const operation = catalog?.operations.find((item) => item.op === op);
    if (!operation) return;
    const at = Math.max(0, Math.min(steps.length, atIndex));
    const defaults = draftWithDefaults(operation, columnsBeforeStep(steps, at, sourceNames));
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
      chooseStep(at);
      commit(next);
      if (!atPoint) setFitTick((tick) => tick + 1);
      focusInspector();
      return;
    }
    chooseStep(null);
    setPendingAt(at);
    focusInspector();
  };

  const validateExpression = async (text: string) => {
    const requestId = ++expressionGen.current;
    if (!text.trim()) {
      setExpressionError("");
      return;
    }
    const local = unknownExpressionColumn(text, names);
    if (local) {
      setExpressionError(local);
      return;
    }
    setExpressionError("");
    setFormError("");
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
    const expressionName = selectedOperation.expression_option;
    const expression = expressionName ? String(draftOptions[expressionName] ?? "") : "";
    const columnIssue = expression ? unknownExpressionColumn(expression, names) : "";
    if (!columnIssue && expressionError && !expressionError.startsWith("refers to column")) {
      setFormError(expressionError);
      return;
    }
    if (columnIssue) setExpressionError(columnIssue);
    const built = stepFromDraft(selectedOperation, draftColumn, draftOptions, draftLabel, draftPolicy);
    if (!built.step) {
      setFormError(built.error);
      return;
    }
    setFormError("");
    setExpressionError("");
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
    chooseStep(at);
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
      if (focusMode) {
        setFocusMode(false);
        return;
      }
      chooseStep(null);
      clearPending();
    }
    else if ((event.key === "Delete" || event.key === "Backspace") && selected != null && canEdit) {
      event.preventDefault();
      commit(removeStep(steps, selected));
      chooseStep(null);
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

  const zoomBy = (delta: number) => {
    setZoomCommand((current) => ({ id: current.id + 1, delta }));
  };

  const zoomTo = (absolute: number) => {
    setZoomCommand((current) => ({ id: current.id + 1, delta: 0, absolute }));
  };

  const zoomPercent = Math.round(zoomLevel * 100);
  const zoomStops = [50, 75, 100, 125, 150];
  const zoomChoice = zoomStops.reduce((best, stop) => (Math.abs(stop - zoomPercent) < Math.abs(best - zoomPercent) ? stop : best), 100);

  const liveIssues = useMemo(() => {
    const local = stepExpressionIssues(steps, catalog?.operations ?? [], sourceNames);
    if (selected != null && expressionError) local.unshift({ index: selected, message: expressionError });
    const covered = new Set(issues.map((issue) => issue.index));
    return [
      ...issues,
      ...local.filter((issue) => !covered.has(issue.index)),
    ];
  }, [catalog?.operations, expressionError, issues, selected, sourceNames, steps]);
  const validationRows = useMemo(() => {
    const messages = new Map<number, string[]>();
    for (const issue of liveIssues) {
      const list = messages.get(issue.index) ?? [];
      list.push(issue.message);
      messages.set(issue.index, list);
    }
    const rows = flow.nodes.map((node) => {
      const detail = (messages.get(node.index) ?? []).join(" ");
      const status = detail
        ? "error" as const
        : node.status === "error" || node.status === "warning" || node.status === "running"
          ? node.status
          : "valid" as const;
      return {
        key: node.id,
        index: node.index,
        title: node.title,
        status,
        detail: detail || statusLabel(status),
      };
    });
    for (const [index, list] of messages) {
      if (flow.nodes.some((node) => node.index === index)) continue;
      rows.push({
        key: `issue-${index}`,
        index,
        title: index >= 0 ? `Step ${index + 1}` : "Recipe",
        status: "error" as const,
        detail: list.join(" "),
      });
    }
    return rows;
  }, [flow.nodes, liveIssues]);
  const recipeInvalid = liveIssues.length > 0;
  const stepIssue = selected != null ? liveIssues.find((issue) => issue.index === selected) : undefined;
  const inspectorAlert = formError || expressionError || stepIssue?.message || "";
  const activePreview = selected != null && nodePreview?.index === selected ? nodePreview : null;
  const groupRows = activePreview?.disabled
    ? []
    : outputGroup === "diverted"
      ? (activePreview?.diverted ?? [])
      : (activePreview?.rows?.length ? activePreview.rows : previewRows);
  const groupColumns = activePreview?.columns?.length ? activePreview.columns : previewColumns;
  const previewNeedle = previewFilter.trim().toLowerCase();
  const filteredPreview = previewNeedle
    ? groupRows.filter((row) => Object.values(row).some((value) => cellText(value).toLowerCase().includes(previewNeedle)))
    : groupRows;
  const previewPopulation = Math.max(sampleRows.length, groupRows.length);

  const previewNode = async (index: number) => {
    const step = stepsRef.current[index];
    if (!step) return;
    const upstream = liveIssues.find((issue) => issue.index >= 0 && issue.index < index);
    if (upstream) {
      setNodePreview({
        index,
        rows: [],
        before: [],
        diverted: [],
        columns: [],
        types: {},
        disabled: true,
        note: `Preview is off because step ${upstream.index + 1} is invalid. ${upstream.message}`,
      });
      return;
    }
    if (sampleRows.length === 0) {
      setNodePreview({
        index,
        rows: [],
        before: [],
        diverted: [],
        columns: [],
        types: {},
        disabled: true,
        note: "No sampled rows to preview through this step.",
      });
      return;
    }
    setRunningIndex(index);
    setNodePreview({
      index,
      rows: [],
      before: [],
      diverted: [],
      columns: [],
      types: {},
      disabled: false,
      note: "Previewing this step…",
    });
    try {
      const body = await previewShapeRecipe({
        recipe: { steps: stepsRef.current.slice(0, index + 1) },
        sample_rows: sampleRows.slice(0, 50),
        source_columns: sourceNames,
        focus_table: activeTable,
        source_type: sourceType || undefined,
      });
      const rows = Array.isArray(body.after) ? body.after : [];
      const before = Array.isArray(body.before) ? body.before : [];
      const diverted = Array.isArray(body.effect?.diverted_samples) ? body.effect.diverted_samples : [];
      const labels: Record<string, string> = {};
      for (const effect of body.effect?.steps ?? []) {
        const from = effect.step - 1;
        if (from >= 0) labels[`order-${from}`] = String(effect.rows_out);
      }
      setEdgeLabels(labels);
      const columns = rows[0] ? Object.keys(rows[0]) : before[0] ? Object.keys(before[0]) : [];
      setNodePreview({
        index,
        rows,
        before,
        diverted,
        columns,
        types: body.column_types ?? {},
        disabled: false,
        note: body.refusal?.message || `Through step ${index + 1}. Showing ${Math.min(50, rows.length)} of ${sampleRows.length} rows. This sample is not the population.`,
      });
    } catch (err) {
      setNodePreview({
        index,
        rows: [],
        before: [],
        diverted: [],
        columns: [],
        types: {},
        disabled: false,
        note: err instanceof Error ? err.message : "Preview failed.",
      });
    } finally {
      setRunningIndex(null);
    }
  };

  const recipeKey = useMemo(
    () => steps.map((step) => `${step.op}:${step.column ?? ""}:${JSON.stringify(step.options ?? {})}`).join("|"),
    [steps],
  );
  useEffect(() => {
    if (selected == null) return;
    void previewNode(selected);
    // previewNode reads the latest steps and issues. The key is the recipe.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected, recipeKey]);
  useEffect(() => {
    localStorage.setItem(SPLIT_KEY, JSON.stringify({ mode: viewMode, height: propertiesHeight }));
  }, [propertiesHeight, viewMode]);
  useEffect(() => {
    const onKey = (event: globalThis.KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setPaletteOpen(true);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

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
      data-view={viewMode}
      data-testid="transform-canvas"
      data-document={documentKind(graph)}
      data-set-hash={setDocumentHash(setDocumentOf(graph, setOps))}
      tabIndex={0}
      onKeyDown={onKeyDown}
    >
      <header className="df2-canvas-head">
        <div className="df2-canvas-head-title">
          <h3>Recipe</h3>
          <span className={`df2-canvas-badge is-${recipeInvalid ? "error" : "valid"}`} data-testid="canvas-recipe-pill">
            {recipeInvalid ? "Invalid" : "Valid"}
          </span>
          <p>
            {activeTable ? `Steps for ${activeTable}. Other tables stay in the recipe, dimmed.` : "Steps run in order on the read."}
          </p>
        </div>
        <div className="df2-canvas-head-actions">
          <ButtonGroup label="Recipe actions">
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" aria-expanded={validationOpen} onClick={() => setValidationOpen((open) => !open)}>
              Issues{liveIssues.length ? ` (${liveIssues.length})` : ""}
            </button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => { setValidationOpen(true); if (selected != null) void previewNode(selected); }}>
              Validate
            </button>
            <button type="button" className="df2-btn df2-btn-sm" onClick={() => { setViewMode((mode) => (mode === "canvas" ? "split" : mode)); setPropertyTab("preview"); setPreviewOpen(true); onRunPreview(); if (selected != null) void previewNode(selected); }}>
              Run preview
            </button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-icon-btn" aria-label="More actions" aria-expanded={menuOpen} title="More actions" onClick={() => setMenuOpen((open) => !open)}>
              <DtIcon name="menu" size={16} chrome="product" />
            </button>
          </ButtonGroup>
          {menuOpen && (
            <div className="df2-canvas-menu" role="menu">
              <button type="button" role="menuitem" onClick={() => { setMenuOpen(false); setRulesOpen(true); }}>Rules and suggestions</button>
              <button type="button" role="menuitem" onClick={() => { setMenuOpen(false); setTemplatesOpen(true); }}>Templates</button>
              <button type="button" role="menuitem" aria-expanded={paletteDock} onClick={() => { setMenuOpen(false); setPaletteDock((open) => !open); }}>Palette</button>
              <button type="button" role="menuitem" onClick={() => { setMenuOpen(false); setViewMode((mode) => (mode === "split" ? "canvas" : "split")); }}>Properties</button>
              <button type="button" role="menuitem" aria-pressed={overview} onClick={() => { setMenuOpen(false); setOverview((on) => !on); }}>Overview</button>
              <button type="button" role="menuitem" onClick={() => { setMenuOpen(false); setFocusMode((on) => !on); }}>{focusMode ? "Exit focus" : "Focus canvas"}</button>
              <button type="button" role="menuitem" onClick={() => { setMenuOpen(false); setHelpOpen(true); }}>Keyboard shortcuts</button>
            </div>
          )}
        </div>
      </header>
      {syncRefused && (
        <p className="df2-canvas-refusal" role="status">{refusalReason} Nodes stay visible and cannot be added.</p>
      )}
      {!canPlan && <p className="df2-label-hint">{disabledReason}</p>}

      {suggestions[0] && canEdit && onApplySuggestion && (
        <p className="df2-canvas-inline-suggest" role="status">
          <span>Suggested</span>
          <button type="button" className="df2-btn df2-btn-sm" onClick={() => onApplySuggestion(suggestions[0])}>
            {suggestions[0].title}
          </button>
        </p>
      )}
      <div className="df2-canvas-body" style={{ ["--df-split-h" as string]: `${viewMode === "split" ? propertiesHeight : 0}px` }}>
        <aside
          className={paletteDock ? "df2-canvas-rail" : "df2-canvas-rail is-collapsed"}
          aria-label="Operations"
          onPointerOver={(event) => {
            const op = (event.target as HTMLElement).closest(".df2-canvas-op");
            const text = op?.getAttribute("data-tip") || "";
            if (!op || !text) {
              setOpTip((current) => (current == null ? current : null));
              return;
            }
            const box = op.getBoundingClientRect();
            const next = { text, x: Math.round(box.right + 8), y: Math.round(box.top) };
            setOpTip((current) => (
              current && current.text === next.text && current.x === next.x && current.y === next.y ? current : next
            ));
          }}
          onPointerLeave={() => setOpTip(null)}
        >
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
                      onClick={() => {
                        const at = insertAt.current ?? steps.length;
                        insertAt.current = null;
                        beginAdd(operation.op, at);
                      }}
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
          {opTip && (
            <div
              className="df2-canvas-op-tip"
              role="tooltip"
              style={{ ["--df-tip-x" as string]: `${opTip.x}px`, ["--df-tip-y" as string]: `${opTip.y}px` }}
            >
              {opTip.text}
            </div>
          )}
        </aside>

        <div className="df2-canvas-stage-wrap">
          <Suspense fallback={<p className="df2-label-hint" role="status" aria-busy="true">Loading the canvas…</p>}>
            <TransformFlow
              nodes={flow.nodes}
              edges={flow.edges}
              pending={pending}
              placed={placed}
              issues={liveIssues}
              zoomCommand={zoomCommand}
              centerCommand={centerCommand}
              selectedIndex={selected}
              onZoom={setZoomLevel}
              onReveal={revealStep}
              onSelect={loadStep}
              onReorder={(from, to, at) => {
                if (!canEdit) return;
                setPendingAt(null);
                const next = reorderByDrag(steps, from, to);
                const occupied = layoutFor(next.length).filter((_, index) => index !== to);
                const clear = positionClearOf(at, occupied);
                setPlaced({ index: to, x: clear.x, y: clear.y });
                commit(next);
              }}
              onPlace={(index, at) => {
                setPlaced({ index, x: at.x, y: at.y });
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
              onInsert={(afterIndex) => {
                insertAt.current = afterIndex + 1;
                setPaletteDock(true);
              }}
              onDeleteEdge={(targetIndex) => {
                if (!canEdit || !steps[targetIndex]) return;
                commit(removeStep(steps, targetIndex));
                if (selected === targetIndex) chooseStep(null);
              }}
              edgeLabels={edgeLabels}
              onPreviewNode={(index) => void previewNode(index)}
              fitTick={fitTick}
              overview={overview}
            />
          </Suspense>
          <div className="df2-canvas-edgebar" role="toolbar" aria-label="Canvas tools">
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-icon-btn" aria-label="Auto-arrange" title="Auto-arrange" onClick={arrange}>
              <DtIcon name="layers" size={16} chrome="product" />
            </button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-icon-btn" aria-label="Delete" title="Delete" disabled={selected == null || !canEdit} onClick={() => { if (selected == null || !canEdit) return; commit(removeStep(steps, selected)); chooseStep(null); }}>
              <DtIcon name="trash" size={16} chrome="product" />
            </button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-icon-btn" aria-label="Undo" title="Undo" onClick={() => applyStack("undo")}>
              <DtIcon name="arrow-left" size={16} chrome="product" />
            </button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-icon-btn" aria-label="Redo" title="Redo" onClick={() => applyStack("redo")}>
              <DtIcon name="arrow-right" size={16} chrome="product" />
            </button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-icon-btn" aria-label="Zoom out" title="Zoom out" onClick={() => zoomBy(-0.1)}>
              <DtIcon name="minus" size={16} chrome="product" />
            </button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-icon-btn" aria-label="Zoom in" title="Zoom in" onClick={() => zoomBy(0.1)}>
              <DtIcon name="plus" size={16} chrome="product" />
            </button>
            <label className="df2-canvas-zoom-label">
              <span className="df2-sr-only">Zoom percent</span>
              <select aria-label="Zoom percent" value={String(zoomChoice)} onChange={(event) => zoomTo(Number(event.target.value) / 100)}>
                {zoomStops.map((stop) => <option key={stop} value={stop}>{stop}%</option>)}
              </select>
            </label>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => zoomTo(1)}>100%</button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-canvas-icon-btn" aria-label="Fit" title="Fit" onClick={() => setFitTick((tick) => tick + 1)}>
              <DtIcon name="expand" size={16} chrome="product" />
            </button>
          </div>
          {linkNote && <p className="df2-canvas-link-note" role="status">{linkNote}</p>}
          {linkRefusal && <p className="df2-canvas-link-refusal" role="alert">{linkRefusal}</p>}
        </div>

        {validationOpen && (
          <div className="df2-canvas-validation" role="region" aria-label="Issues">
            <h4>Issues</h4>
            {validationRows.length === 0 ? (
              <p className="df2-label-hint">No transformations on the canvas.</p>
            ) : (
              <ul>
                {validationRows.map((row) => (
                  <li key={row.key}>
                    <button
                      type="button"
                      className={`is-${row.status}${selected === row.index ? " is-current" : ""}`}
                      aria-label={row.index >= 0 ? `Step ${row.index + 1} ${statusLabel(row.status)}` : `Recipe ${statusLabel(row.status)}`}
                      onClick={() => {
                        if (row.index >= 0 && steps[row.index]) {
                          revealStep(row.index);
                          return;
                        }
                        setViewMode((mode) => (mode === "canvas" ? "split" : mode));
                        focusInspector();
                      }}
                    >
                      <span className="df2-canvas-validation-mark" aria-hidden>
                        <DtIcon
                          name={row.status === "error" ? "alert" : row.status === "warning" ? "warning" : row.status === "running" ? "activity" : "check"}
                          size={16}
                          chrome="product"
                        />
                      </span>
                      <span className="df2-canvas-validation-copy">
                        <span className="df2-canvas-validation-name">{row.title}</span>
                        <span className="df2-canvas-validation-state">{row.detail}</span>
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}

        <section className="df2-canvas-properties" aria-label="Properties">
          {viewMode === "split" && (
            <div
              className="df2-canvas-split"
              role="separator"
              aria-orientation="horizontal"
              aria-label="Resize properties"
              aria-valuemin={96}
              aria-valuemax={420}
              aria-valuenow={propertiesHeight}
              tabIndex={0}
              onPointerDown={(event) => {
                const startY = event.clientY;
                const startH = propertiesHeight;
                resizeProperties(event, (next) => {
                  setPropertiesHeight(Math.min(420, Math.max(96, startH + (startY - next.clientY))));
                });
              }}
              onKeyDown={(event) => {
                if (event.key === "ArrowUp") setPropertiesHeight((height) => Math.min(420, height + 24));
                if (event.key === "ArrowDown") setPropertiesHeight((height) => Math.max(96, height - 24));
              }}
            />
          )}
          <div className="df2-canvas-prop-head">
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" aria-pressed={viewMode === "canvas"} onClick={() => setViewMode("canvas")}>Canvas only</button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" aria-pressed={viewMode === "split"} onClick={() => { setPreviewMax(false); setViewMode("split"); }}>Split</button>
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" aria-pressed={viewMode === "properties"} onClick={() => setViewMode("properties")}>Properties only</button>
          </div>
          <div className="df2-canvas-prop-row">
            <nav className="df2-canvas-tab-rail" aria-label="Step properties">
              <button type="button" aria-selected={propertyTab === "general"} onClick={() => setPropertyTab("general")}>General</button>
              <button type="button" aria-selected={propertyTab === "fields"} onClick={() => setPropertyTab("fields")}>Incoming Fields</button>
              <button type="button" aria-selected={propertyTab === "op"} onClick={() => setPropertyTab("op")}>{selectedOperation ? shortName(selectedOperation.op, selectedOperation.summary) : "Step"}</button>
              <button type="button" aria-selected={propertyTab === "preview"} onClick={() => { setPropertyTab("preview"); setPreviewOpen(true); }}>Preview</button>
            </nav>
            <div className="df2-canvas-inspector" id="canvas-inspector" tabIndex={-1} aria-label="Step inspector">
              {propertyTab === "general" && (
                <>
                  <h4>{selected != null ? "Edit step" : draftOp ? "New step" : "Properties"}</h4>
                  {inspectorAlert && <p className="df2-qw-schema-error" role="alert">{inspectorAlert}</p>}
                  {!selectedOperation && <p className="df2-label-hint">Choose an operation, or select a step.</p>}
                  {selectedOperation && <p className="df2-label-hint">{selectedOperation.summary}</p>}
                  <label className="df2-label" htmlFor="canvas-on-error">
                    When a row fails
                    <select id="canvas-on-error" className="df2-input" value={draftPolicy} disabled={!canEdit} onChange={(event) => setDraftPolicy(event.target.value)}>
                      {(catalog?.error_policies?.length ? catalog.error_policies : [{ value: "refuse", label: "Refuse the step" }]).map((entry) => (
                        <option key={entry.value} value={entry.value}>{entry.label}</option>
                      ))}
                    </select>
                  </label>
                </>
              )}
              {propertyTab === "fields" && (
                <>
                  <h4>Incoming Fields</h4>
                  {names.length === 0 ? <p className="df2-label-hint">No columns reach this step.</p> : (
                    <ul className="df2-canvas-field-list">
                      {names.map((name) => {
                        const column = columns.find((item) => item.name === name);
                        return <li key={name}><span>{name}</span><span>{column?.type || "text"}</span></li>;
                      })}
                    </ul>
                  )}
                </>
              )}
              {propertyTab === "op" && (
                <>
                  <h4>{selected != null ? "Edit step" : draftOp ? "New step" : "Properties"}</h4>
                  {inspectorAlert && <p className="df2-qw-schema-error" role="alert">{inspectorAlert}</p>}
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
                        conditionBuilder
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
                            onClick={() => { setPreviewOpen(true); void previewNode(selected); }}
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
                          {nodePreview.rows.length > 0 && (
                            <div className="df2-structure-table-wrap">
                              <table className="df2-structure-table">
                                <thead>
                                  <tr>
                                    {nodePreview.columns.slice(0, 6).map((column) => (
                                      <th key={column}>{column}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {nodePreview.rows.slice(0, 5).map((row, rowIndex) => (
                                    <tr key={rowIndex}>
                                      {nodePreview.columns.slice(0, 6).map((column) => (
                                        <td key={column}>{String(row[column] ?? "")}</td>
                                      ))}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          )}
                        </div>
                      )}
                    </>
                  )}
                </>
              )}
              {propertyTab === "preview" && (
                <p className="df2-label-hint">The sample for the selected step is in the preview under this panel.</p>
              )}
            </div>
          </div>
          <PreviewDock
            height={dockHeight}
            onHeight={setDockHeight}
            open={previewOpen}
            onOpen={setPreviewOpen}
            maximized={previewMax}
            onMaximize={() => {
              setPreviewMax((value) => {
                const next = !value;
                if (next) {
                  setPreviewOpen(true);
                  setPropertyTab("preview");
                  setViewMode("properties");
                } else {
                  setViewMode("split");
                }
                return next;
              });
            }}
            status={previewStatus || previewBanner || activePreview?.note || ""}
            effect={activePreview?.note || effectSummary}
            columns={groupColumns}
            rows={filteredPreview.slice(0, 50)}
            population={previewPopulation}
            disabledReason={activePreview?.disabled ? activePreview.note : ""}
            filter={previewFilter}
            onFilter={setPreviewFilter}
            previewTab={previewTab}
            onPreviewTab={setPreviewTab}
            outputGroup={outputGroup}
            onOutputGroup={setOutputGroup}
            showGroups={selectedWire?.op === "divert_rows"}
            beforeRows={activePreview?.before ?? []}
            types={activePreview?.types ?? {}}
            issues={selected == null ? liveIssues.map((issue) => issue.message) : liveIssues.filter((issue) => issue.index === selected).map((issue) => issue.message)}
            onRunPreview={() => {
              setPreviewOpen(true);
              setPropertyTab("preview");
              onRunPreview();
              if (selected != null) void previewNode(selected);
            }}
            onDownload={() => downloadCsv(groupColumns, filteredPreview.slice(0, 50))}
          />
        </section>
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

function downloadCsv(columns: string[], rows: Record<string, unknown>[]) {
  const line = (row: Record<string, unknown> | null, names: string[]) => names.map((name) => {
    const text = row ? cellText(row[name]) : name;
    return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
  }).join(",");
  const blob = new Blob([[line(null, columns), ...rows.map((row) => line(row, columns))].join("\n")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "step-preview.csv";
  link.click();
  URL.revokeObjectURL(url);
}

function PreviewDock({
  height,
  onHeight,
  open,
  onOpen,
  maximized,
  onMaximize,
  status,
  effect,
  columns,
  rows,
  population,
  disabledReason,
  filter,
  onFilter,
  previewTab,
  onPreviewTab,
  outputGroup,
  onOutputGroup,
  showGroups,
  beforeRows,
  types,
  issues,
  onRunPreview,
  onDownload,
}: {
  height: number;
  onHeight: (height: number) => void;
  open: boolean;
  onOpen: (open: boolean) => void;
  maximized: boolean;
  onMaximize: () => void;
  status: string;
  effect: string;
  columns: string[];
  rows: Record<string, unknown>[];
  population: number;
  disabledReason: string;
  filter: string;
  onFilter: (value: string) => void;
  previewTab: PreviewTab;
  onPreviewTab: (tab: PreviewTab) => void;
  outputGroup: "kept" | "diverted";
  onOutputGroup: (group: "kept" | "diverted") => void;
  showGroups: boolean;
  beforeRows: Record<string, unknown>[];
  types: Record<string, string>;
  issues: string[];
  onRunPreview: () => void;
  onDownload: () => void;
}) {
  const shown = columns.slice(0, 8);
  const resizeDock = usePointerDrag();
  const gripDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    const startY = event.clientY;
    const startH = height;
    resizeDock(event, (next) => {
      onHeight(Math.min(420, Math.max(96, startH + (startY - next.clientY))));
    });
  };
  const expanded = open || maximized;
  const compareColumns = shown.length > 0 ? shown : (beforeRows[0] ? Object.keys(beforeRows[0]).slice(0, 8) : []);
  return (
    <div
      className={`df2-canvas-dock${expanded ? "" : " is-collapsed"}${maximized ? " is-max" : ""}`}
      style={open && !maximized ? { height } : undefined}
      data-testid="canvas-preview"
    >
      {open && !maximized && (
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
      )}
      <div className="df2-canvas-dock-body">
        <div className="df2-canvas-dock-head">
          <p data-testid="canvas-preview-status" role="status">{status}</p>
          <button
            type="button"
            className="df2-btn df2-btn-ghost df2-btn-sm"
            onClick={() => {
              if (maximized) {
                onMaximize();
                onOpen(false);
                return;
              }
              onOpen(!open);
            }}
          >
            {expanded ? "Hide preview" : "Show preview"}
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={onMaximize}>
            {maximized ? "Restore preview" : "Maximize preview"}
          </button>
          <button type="button" className="df2-btn df2-btn-sm" onClick={() => { onOpen(true); onRunPreview(); }}>Run preview</button>
        </div>
        {expanded && (
          <div className="df2-canvas-preview-tools">
            <div className="df2-canvas-tabs" role="tablist" aria-label="Preview sections">
              {([
                ["data", "Data"],
                ["compare", "Input vs Output"],
                ["schema", "Schema"],
                ["issues", "Issues"],
              ] as const).map(([id, label]) => (
                <button key={id} type="button" role="tab" aria-selected={previewTab === id} onClick={() => onPreviewTab(id)}>{label}</button>
              ))}
            </div>
            <p className="df2-label-hint">{Math.min(50, rows.length)} of {population}</p>
            {showGroups && (
              <label className="df2-label">
                Output group
                <select className="df2-input df2-input-sm" aria-label="Output group" value={outputGroup} onChange={(event) => onOutputGroup(event.target.value === "diverted" ? "diverted" : "kept")}>
                  <option value="kept">Kept</option>
                  <option value="diverted">Diverted</option>
                </select>
              </label>
            )}
            <input className="df2-input df2-input-sm" value={filter} onChange={(event) => onFilter(event.target.value)} placeholder="Filter rows" aria-label="Filter preview rows" />
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={onDownload}>Download CSV</button>
          </div>
        )}
        {expanded && disabledReason && <p className="df2-label-hint" role="status">{disabledReason}</p>}
        {expanded && !disabledReason && effect && previewTab === "data" && <p className="df2-label-hint">{effect}</p>}
        {expanded && !disabledReason && previewTab === "data" && rows.length > 0 && shown.length > 0 && (
          <table>
            <thead>
              <tr>{shown.map((column) => <th key={column}>{column}</th>)}</tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={index}>
                  {shown.map((column) => <td key={column}>{cellText(row[column])}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {expanded && !disabledReason && previewTab === "compare" && (
          <div className="df2-canvas-compare">
            <table>
              <caption>Input</caption>
              <thead><tr>{compareColumns.map((column) => <th key={column}>{column}</th>)}</tr></thead>
              <tbody>
                {beforeRows.slice(0, 50).map((row, index) => (
                  <tr key={index}>{compareColumns.map((column) => <td key={column}>{cellText(row[column])}</td>)}</tr>
                ))}
              </tbody>
            </table>
            <table>
              <caption>Output</caption>
              <thead><tr>{shown.map((column) => <th key={column}>{column}</th>)}</tr></thead>
              <tbody>
                {rows.map((row, index) => (
                  <tr key={index}>{shown.map((column) => <td key={column}>{cellText(row[column])}</td>)}</tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {expanded && previewTab === "schema" && (
          <ul className="df2-canvas-field-list">
            {(shown.length > 0 ? shown : Object.keys(types)).map((column) => (
              <li key={column}><span>{column}</span><span>{types[column] || "text"}</span></li>
            ))}
          </ul>
        )}
        {expanded && previewTab === "issues" && (
          issues.length === 0
            ? <p className="df2-label-hint">No issues on this step.</p>
            : <ul className="df2-canvas-field-list">{issues.map((issue) => <li key={issue}>{issue}</li>)}</ul>
        )}
      </div>
    </div>
  );
}
