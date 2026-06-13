interface TransformRow {
  source: string;
  target: string;
  expression: string;
}

interface TransformPreviewTableProps {
  transforms: TransformRow[];
}

export function TransformPreviewTable({ transforms }: TransformPreviewTableProps) {
  if (!transforms.length) return null;
  return (
    <div className="df-transform-preview">
      <div className="df-section-title">Transform expressions</div>
      <div className="df-table-panel">
        <div className="df-table-scroll">
          <table className="df-data-table df-data-table--dense">
            <thead>
              <tr>
                <th>Source</th>
                <th>Target</th>
                <th>Expression</th>
              </tr>
            </thead>
            <tbody>
              {transforms.map((t) => (
                <tr key={`${t.source}-${t.target}`}>
                  <td className="df-mono">{t.source}</td>
                  <td className="df-mono">{t.target}</td>
                  <td className="df-mono df-transform-expr">{t.expression}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
