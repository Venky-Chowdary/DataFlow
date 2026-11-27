/**
 * The "?" sheet (UI pass 2, item 7): every keyboard shortcut, searchable.
 */
import { useMemo, useState } from "react";
import { SHORTCUTS } from "../../lib/commandRegistry";
import { Dialog } from "./Dialog";

export function ShortcutsSheet({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [query, setQuery] = useState("");
  const shown = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? SHORTCUTS.filter((s) => `${s.action} ${s.where} ${s.keys.join(" ")}`.toLowerCase().includes(q)) : SHORTCUTS;
  }, [query]);
  return (
    <Dialog open={open} onClose={() => { setQuery(""); onClose(); }} title="Keyboard shortcuts" size="md" ariaLabel="Keyboard shortcuts">
      <input
        className="df2-input"
        type="search"
        aria-label="Filter shortcuts"
        placeholder="Filter shortcuts"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
      />
      <table className="df2-shortcuts">
        <caption className="df2-sr-only">Keyboard shortcuts</caption>
        <thead>
          <tr>
            <th scope="col">Keys</th>
            <th scope="col">What it does</th>
            <th scope="col">Where</th>
          </tr>
        </thead>
        <tbody>
          {shown.map((s) => (
            <tr key={`${s.where}-${s.action}`}>
              <td>{s.keys.map((k, i) => <kbd key={`${k}-${i}`} className="df2-cmdk-kbd">{k}</kbd>)}</td>
              <td>{s.action}</td>
              <td>{s.where}</td>
            </tr>
          ))}
          {shown.length === 0 && (
            <tr><td colSpan={3}>No shortcut matches.</td></tr>
          )}
        </tbody>
      </table>
    </Dialog>
  );
}
