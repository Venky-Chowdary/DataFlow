import type { DataContractSummary } from "../../lib/api";
import { contractColumnRows } from "../../lib/contractReadModel";

function statusWord(status: string): string {
  const s = (status || "").toLowerCase();
  if (s === "signed") return "Signed";
  if (s === "broken") return "Broken";
  if (s === "deprecated") return "Deprecated";
  return "Draft";
}

function cell(v: unknown): string {
  if (v == null || v === "") return "—";
  return String(v);
}

/** Columns of the contract's bound schema (real names, never col_<i>). */
export function ContractColumnsTable({ contract }: { contract: Pick<DataContractSummary, "columns"> }) {
  const rows = contractColumnRows(contract);
  return (
    <div className="df2-table-wrap df2-drawer-table-wrap">
      <table className="df2-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Nullable</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const name = cell(row.name);
            const typ = cell(row.type);
            const nullable = row.nullable == null ? "—" : row.nullable ? "Yes" : "No";
            return (
              <tr key={`${name}-${i}`}>
                <td title={name}>{name}</td>
                <td className="df2-cell-mono" title={typ}>{typ}</td>
                <td>{nullable}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** Status + version facts, from the server read model. */
export function ContractStatusFacts({ contract: c }: { contract: DataContractSummary }) {
  const label = statusWord(c.status);
  return (
    <>
      <div className="df2-drawer-fact">
        <span>Status</span>
        <strong>{label}</strong>
      </div>
      <div className="df2-drawer-fact">
        <span>Version</span>
        <strong>v{c.version}</strong>
      </div>
    </>
  );
}

