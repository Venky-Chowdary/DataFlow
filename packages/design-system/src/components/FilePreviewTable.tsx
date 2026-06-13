interface FilePreviewTableProps {
  columns: string[];
  rows: string[][];
  format?: string;
  rowCount?: number;
}

/** First rows preview — plan Screen 1 spec */
export function FilePreviewTable({ columns, rows, format, rowCount }: FilePreviewTableProps) {
  if (columns.length === 0) return null;

  return (
    <div className="df-file-preview">
      <div className="df-file-preview-meta">
        <span>
          Preview · {rows.length} rows shown
          {rowCount != null && ` of ${rowCount.toLocaleString()}`}
        </span>
        {format && (
          <span className="df-mono">
            Detected: {format.toUpperCase()} · {columns.length} columns
          </span>
        )}
      </div>
      <div className="df-table-panel">
        <div className="df-table-scroll">
          <table className="df-data-table df-data-table--dense">
            <thead>
              <tr>
                {columns.map((col) => (
                  <th key={col}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i}>
                  {columns.map((_, j) => (
                    <td key={j} className="df-mono">
                      {row[j] ?? ""}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
