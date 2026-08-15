import { FilterTabs } from "../ui/FilterTabs";
import { SqlEditor } from "../ui/SqlEditor";
import { bindNamesFromSql, dialectOffersProcedures, procedureHint } from "../../lib/sourceReadMode";

export type DestWriteMode = "table" | "procedure";

interface DestProcedurePanelProps {
  destType: string;
  destWriteMode: DestWriteMode;
  onDestWriteMode: (mode: DestWriteMode) => void;
  destProcedureCall: string;
  onDestProcedureCall: (sql: string) => void;
  destProcedureParams: Record<string, string>;
  onDestProcedureParams: (next: Record<string, string>) => void;
  destProcedureBefore: string;
  onDestProcedureBefore: (sql: string) => void;
  destProcedureAfter: string;
  onDestProcedureAfter: (sql: string) => void;
  sourceColumns: string[];
  paramMap: Record<string, string>;
  onParamMap: (next: Record<string, string>) => void;
}

export function DestProcedurePanel({
  destType,
  destWriteMode,
  onDestWriteMode,
  destProcedureCall,
  onDestProcedureCall,
  destProcedureParams,
  onDestProcedureParams,
  destProcedureBefore,
  onDestProcedureBefore,
  destProcedureAfter,
  onDestProcedureAfter,
  sourceColumns,
  paramMap,
  onParamMap,
}: DestProcedurePanelProps) {
  if (!dialectOffersProcedures(destType)) return null;
  const binds = bindNamesFromSql(destProcedureCall);

  return (
    <div className="df2-dest-procedure">
      <div className="df2-field">
        <label className="df2-label" htmlFor="dest-write-mode">Destination write</label>
        <FilterTabs<DestWriteMode>
          ariaLabel="Destination write"
          value={destWriteMode}
          onChange={onDestWriteMode}
          items={[
            { id: "table", label: "Table" },
            { id: "procedure", label: "Stored procedure" },
          ]}
        />
        <span className="df2-label-hint">
          {destWriteMode === "procedure"
            ? "Each row is one CALL. Failed CALLs quarantine — not Informatica continue-on-error. Not CDC."
            : "Write a table or view. Optional before/after CALL hooks stay on Advanced."}
        </span>
      </div>

      {destWriteMode === "procedure" && (
        <>
          <SqlEditor
            id="dest-procedure-call"
            label="Destination CALL / EXEC"
            value={destProcedureCall}
            onChange={onDestProcedureCall}
            mode="procedure"
            dialect={destType}
            bound={destProcedureParams}
            placeholder={procedureHint(destType)}
            hint="Map each :bind to a source column. Missing binds quarantine that row."
            rows={6}
          />
          {binds.length > 0 && (
            <div className="df2-source-bind-params">
              {binds.map((name) => (
                <div className="df2-field" key={name}>
                  <label className="df2-label" htmlFor={`dest-bind-col-${name}`}>
                    :{name} ← column
                  </label>
                  <select
                    id={`dest-bind-col-${name}`}
                    className="df2-input df2-select"
                    value={paramMap[name] || ""}
                    onChange={(e) => onParamMap({ ...paramMap, [name]: e.target.value })}
                  >
                    <option value="">Select column…</option>
                    {sourceColumns.map((col) => (
                      <option key={col} value={col}>{col}</option>
                    ))}
                  </select>
                </div>
              ))}
            </div>
          )}
        </>
      )}

      <details className="df2-dest-procedure-hooks">
        <summary>Before / after write hooks</summary>
        <p className="df2-label-hint">
          Informatica Target Pre-load / Post-load. Each hook is one CALL/EXEC on the destination.
        </p>
        <SqlEditor
          id="dest-procedure-before"
          label="Before write"
          value={destProcedureBefore}
          onChange={onDestProcedureBefore}
          mode="procedure"
          dialect={destType}
          placeholder="EXEC dbo.DisableIndexes"
          rows={4}
        />
        <SqlEditor
          id="dest-procedure-after"
          label="After write"
          value={destProcedureAfter}
          onChange={onDestProcedureAfter}
          mode="procedure"
          dialect={destType}
          placeholder="CALL public.rebuild_stats()"
          rows={4}
        />
      </details>
    </div>
  );
}
