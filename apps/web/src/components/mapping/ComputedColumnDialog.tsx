import { useEffect, useId, useState } from "react";
import { Dialog } from "../ui/Dialog";
import { checkShapeExpression } from "../../lib/api";
import type { DestColumnCatalog } from "../../lib/destColumnCatalog";
import {
  computedStep,
  destColumnForAdvice,
  runTimeAdvice,
  type ComputedSource,
} from "../../lib/mappingCode";
import type { ShapeStepWire } from "../../lib/shape";

type Kind = "expression" | "constant" | "run_time";

interface ComputedColumnDialogProps {
  open: boolean;
  catalog: DestColumnCatalog | null;
  sourceColumns: string[];
  onAdd: (step: ShapeStepWire, target: string) => void;
  onClose: () => void;
}

const EXAMPLES = [
  "CONCAT(first_name, ' ', last_name)",
  "UPPER(TRIM(status))",
  "COALESCE(nickname, first_name)",
];

/**
 * A destination column filled by an expression or a constant instead of one
 * source column. The value is a Transform (shape) step — the one expression
 * engine — so preview, Validate (which type-checks it) and Execute (Gate-8
 * re-applies it) all compute the same thing. The expression is checked live
 * by the same compiler the engine runs.
 */
export function ComputedColumnDialog({ open, catalog, sourceColumns, onAdd, onClose }: ComputedColumnDialogProps) {
  const ids = { kind: useId(), target: useId(), expr: useId(), value: useId(), check: useId() };
  const [kind, setKind] = useState<Kind>("expression");
  const [target, setTarget] = useState("");
  const [expression, setExpression] = useState(EXAMPLES[0]);
  const [value, setValue] = useState("");
  const [check, setCheck] = useState<{ state: "idle" | "checking" | "ok" | "error"; message: string }>({
    state: "idle", message: "",
  });

  // Keyed by value: the parent hands a fresh array each render, and an
  // identity dependency would restart the debounce forever.
  const sourceKey = sourceColumns.join("\u0000");
  useEffect(() => {
    if (!open || kind !== "expression" || !expression.trim()) {
      setCheck({ state: "idle", message: "" });
      return;
    }
    let live = true;
    setCheck({ state: "checking", message: "Checking…" });
    const t = window.setTimeout(() => {
      checkShapeExpression({ expression, source_columns: sourceColumns })
        .then((r) => {
          if (!live) return;
          setCheck(
            r.valid
              ? { state: "ok", message: `Valid. Reads ${(r.columns ?? []).join(", ") || "no columns"}.` }
              : { state: "error", message: r.error || "The expression is not valid." },
          );
        })
        .catch((e: unknown) => {
          if (live) setCheck({ state: "error", message: e instanceof Error ? e.message : "Check failed" });
        });
    }, 300);
    return () => {
      live = false;
      window.clearTimeout(t);
    };
  }, [open, kind, expression, sourceKey]);

  const advice = kind === "run_time" ? runTimeAdvice(destColumnForAdvice(catalog, target)) : null;
  const ready = Boolean(target.trim()) && (
    kind === "expression" ? check.state === "ok" : kind === "constant" ? true : Boolean(advice?.ok)
  );

  const add = () => {
    if (!ready) return;
    const name = target.trim();
    const c: ComputedSource = kind === "expression"
      ? { kind, target: name, expression: expression.trim() }
      : kind === "constant"
        ? { kind, target: name, value }
        : { kind, target: name };
    onAdd(computedStep(c), name);
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      size="md"
      title="Computed destination column"
      subtitle="Fill a destination column from an expression, a constant or the run's start time. It is added to Transform, where you preview it; Validate type-checks it against the destination."
      className="df2-ms-computed"
      footer={
        <>
          <button type="button" className="df2-btn df2-btn-ghost" onClick={onClose}>Cancel</button>
          <button type="button" className="df2-btn df2-btn-primary" disabled={!ready} onClick={add}>
            Add to Transform
          </button>
        </>
      }
    >
      <div className="df2-ms-form">
        <fieldset className="df2-ms-kinds">
          <legend>Value</legend>
          {([
            ["expression", "Expression"],
            ["constant", "Constant"],
            ["run_time", "Run start time (fixed once per run)"],
          ] as const).map(([k, label]) => (
            <label key={k} className="df2-ms-radio">
              <input
                aria-label={label}
                type="radio"
                name={ids.kind}
                checked={kind === k}
                onChange={() => setKind(k)}
              />
              {label}
            </label>
          ))}
        </fieldset>

        <label htmlFor={ids.target}>Destination column</label>
        <input
          id={ids.target}
          className="df2-input"
          list={`${ids.target}-cols`}
          value={target}
          onChange={(e) => setTarget(e.target.value)}
          placeholder="full_name"
          autoComplete="off"
        />
        <datalist id={`${ids.target}-cols`}>
          {(catalog?.columns ?? []).map((c) => <option key={c.name} value={c.name} />)}
        </datalist>

        {kind === "expression" && (
          <>
            <label htmlFor={ids.expr}>Expression</label>
            <textarea
              id={ids.expr}
              className="df2-input df2-ms-expr"
              rows={3}
              value={expression}
              spellCheck={false}
              onChange={(e) => setExpression(e.target.value)}
              aria-describedby={ids.check}
            />
            <p id={ids.check} className={`df2-ms-check is-${check.state}`} role="status" aria-live="polite">
              {check.message}
            </p>
            <p className="df2-ms-panel-note">
              Columns by name, text in single quotes. Examples:{" "}
              {EXAMPLES.map((ex) => (
                <button key={ex} type="button" className="df2-ms-link" onClick={() => setExpression(ex)}>
                  {ex}
                </button>
              ))}
            </p>
          </>
        )}
        {kind === "constant" && (
          <>
            <label htmlFor={ids.value}>Constant value</label>
            <input
              id={ids.value}
              className="df2-input"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder="mysql"
            />
          </>
        )}
        {advice && (
          <p className={`df2-ms-check is-${advice.ok ? "ok" : "error"}`} role="status">{advice.message}</p>
        )}
      </div>
    </Dialog>
  );
}
