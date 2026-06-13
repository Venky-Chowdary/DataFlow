import { Button } from "./Button";

export interface SavedConnectorRecord {
  id: string;
  name: string;
  type: string;
  role: string;
  connection_string: string;
  last_test_ok?: boolean;
  last_tested_at?: string | null;
}

interface SavedConnectorRegistryProps {
  connectors: SavedConnectorRecord[];
  onTest: (id: string) => void;
  onDelete: (id: string) => void;
  testingId?: string | null;
}

export function SavedConnectorRegistry({
  connectors,
  onTest,
  onDelete,
  testingId,
}: SavedConnectorRegistryProps) {
  if (connectors.length === 0) {
    return <p className="df-empty-inline">No saved connectors yet. Add one below to reuse across transfers.</p>;
  }

  return (
    <div className="df-table-scroll">
      <table className="df-data-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Engine</th>
            <th>Role</th>
            <th>Connection</th>
            <th>Status</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {connectors.map((c) => (
            <tr key={c.id}>
              <td>{c.name}</td>
              <td className="df-mono">{c.type}</td>
              <td>{c.role}</td>
              <td className="df-mono df-conn-mask">{c.connection_string || "—"}</td>
              <td>
                {c.last_test_ok ? (
                  <span className="df-badge df-badge--success">Verified</span>
                ) : c.last_tested_at ? (
                  <span className="df-badge df-badge--warn">Failed</span>
                ) : (
                  <span className="df-badge">Not tested</span>
                )}
              </td>
              <td className="df-table-actions">
                <Button variant="ghost" onClick={() => onTest(c.id)} disabled={testingId === c.id}>
                  {testingId === c.id ? "Testing…" : "Test"}
                </Button>
                <Button variant="ghost" onClick={() => onDelete(c.id)}>
                  Remove
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
