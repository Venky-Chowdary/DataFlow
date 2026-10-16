import { useEffect, useMemo, useState } from "react";
import {
  applyColumnLayout,
  columnLayoutIssue,
  layoutRowsFromSteps,
  type ColumnLayoutRow,
} from "../../lib/columnLayout";
import type { ShapeStepWire } from "../../lib/shape";

interface ClassicColumnLayoutProps {
  columns: string[];
  steps: ShapeStepWire[];
  canPlan: boolean;
  onApply: (steps: ShapeStepWire[]) => void;
}

/**
 * Keep, rename, and reorder as rename_column and keep_columns steps.
 * This is the classic wizard's column board. The canvas does not mount it.
 */
export function ClassicColumnLayout({ columns, steps, canPlan, onApply }: ClassicColumnLayoutProps) {
  const saved = useMemo(() => layoutRowsFromSteps(steps, columns), [columns, steps]);
  const [rows, setRows] = useState<ColumnLayoutRow[]>(saved);
  const [dirty, setDirty] = useState(false);

  useEffect(() => {
    if (!dirty) setRows(saved);
  }, [dirty, saved]);

  const issue = columnLayoutIssue(rows);
  const edit = (next: ColumnLayoutRow[]) => {
    setRows(next);
    setDirty(true);
  };

  return (
    <section className="df2-xform-card df2-xform-columns" aria-labelledby="xform-columns-title">
      <header className="df2-xform-card-head">
        <h3 id="xform-columns-title">Columns</h3>
      </header>
      <p className="df2-xform-note">
        Keep, rename, or reorder. A column you clear is dropped from the stream.
        Apply writes the rename and keep steps the engine already runs, after the other steps.
      </p>
      {columns.length === 0 ? (
        <p className="df2-xform-empty">Load a sample on the Source step to choose columns.</p>
      ) : (
        <ol className="df2-xform-column-list">
          {rows.map((row, index) => (
            <li key={row.source} className={row.kept ? "" : "is-dropped"}>
              <input
                type="checkbox"
                checked={row.kept}
                disabled={!canPlan}
                aria-label={`Keep ${row.source}`}
                onChange={(event) => {
                  const next = rows.slice();
                  next[index] = { ...row, kept: event.target.checked };
                  edit(next);
                }}
              />
              <input
                className="df2-input df2-studio-field"
                value={row.name}
                disabled={!canPlan || !row.kept}
                aria-label={`Name for ${row.source}`}
                onChange={(event) => {
                  const next = rows.slice();
                  next[index] = { ...row, name: event.target.value };
                  edit(next);
                }}
              />
              <div className="df2-xform-column-moves">
                <button
                  type="button"
                  className="df2-btn df2-btn-sm df2-btn-ghost"
                  disabled={!canPlan || index === 0}
                  aria-label={`Move ${row.source} up`}
                  onClick={() => {
                    const next = rows.slice();
                    const [item] = next.splice(index, 1);
                    next.splice(index - 1, 0, item);
                    edit(next);
                  }}
                >
                  ↑
                </button>
                <button
                  type="button"
                  className="df2-btn df2-btn-sm df2-btn-ghost"
                  disabled={!canPlan || index === rows.length - 1}
                  aria-label={`Move ${row.source} down`}
                  onClick={() => {
                    const next = rows.slice();
                    const [item] = next.splice(index, 1);
                    next.splice(index + 1, 0, item);
                    edit(next);
                  }}
                >
                  ↓
                </button>
              </div>
            </li>
          ))}
        </ol>
      )}
      {dirty && issue && (
        <p className="df2-xform-required" role="alert">{issue}</p>
      )}
      <div className="df2-xform-builder-actions">
        <button
          type="button"
          className="df2-btn df2-btn-sm"
          disabled={!canPlan || !dirty || Boolean(issue) || columns.length === 0}
          onClick={() => {
            const result = applyColumnLayout(steps, rows, columns);
            if (result.error) return;
            onApply(result.steps);
            setDirty(false);
          }}
        >
          Apply column layout
        </button>
        {dirty && (
          <button
            type="button"
            className="df2-btn df2-btn-ghost df2-btn-sm"
            onClick={() => {
              setRows(saved);
              setDirty(false);
            }}
          >
            Reset
          </button>
        )}
      </div>
    </section>
  );
}
