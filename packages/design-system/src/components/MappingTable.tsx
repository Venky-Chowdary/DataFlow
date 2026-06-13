import { ConfidenceBar } from "./ConfidenceBar";
import type { MappingRow } from "../types";

interface MappingTableProps {
  rows: MappingRow[];
}

export function MappingTable({ rows }: MappingTableProps) {
  return (
    <div className="df-card df-card--flat df-table-panel">
      <div className="df-table-scroll">
        <table className="df-data-table">
          <thead>
            <tr>
              <th>Source column</th>
              <th>Target column</th>
              <th>AI confidence</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${row.source}-${row.target}`} className={row.needsReview ? "df-data-table-row--review" : ""}>
                <td className="df-mono">{row.source}</td>
                <td className="df-mono">{row.target}</td>
                <td>
                  <ConfidenceBar value={row.confidence} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
