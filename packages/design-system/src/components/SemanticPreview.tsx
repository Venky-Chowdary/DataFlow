export interface SemanticColumnAnalysis {
  name: string;
  inferred_type: string;
  semantic_role: string;
  confidence: number;
  detection_source: string;
  description: string;
  samples: string[];
}

interface SemanticPreviewProps {
  columns: SemanticColumnAnalysis[];
  title?: string;
}

export function SemanticPreview({ columns, title = "AI semantic analysis" }: SemanticPreviewProps) {
  if (columns.length === 0) return null;

  return (
    <div className="df-semantic-preview">
      <div className="df-semantic-preview-head">
        <span className="df-semantic-preview-title">{title}</span>
        <span className="df-semantic-preview-meta">{columns.length} columns analyzed</span>
      </div>
      <div className="df-table-scroll">
        <table className="df-data-table df-data-table--compact">
          <thead>
            <tr>
              <th>Column</th>
              <th>Detected role</th>
              <th>Confidence</th>
              <th>Source</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((col) => (
              <tr key={col.name}>
                <td className="df-mono">{col.name}</td>
                <td>
                  <span className="df-semantic-role">{col.semantic_role}</span>
                  <span className="df-semantic-desc">{col.description}</span>
                </td>
                <td>{Math.round(col.confidence * 100)}%</td>
                <td className="df-semantic-source">{col.detection_source.replace("_", " ")}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
