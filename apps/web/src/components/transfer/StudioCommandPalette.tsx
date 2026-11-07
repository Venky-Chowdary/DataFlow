import { useEffect, useMemo, useRef, useState } from "react";
import { filterPalette, type PaletteAction } from "../../lib/canvasPalette";
import { Dialog } from "../ui/Dialog";

interface StudioCommandPaletteProps {
  open: boolean;
  actions: PaletteAction[];
  onClose: () => void;
  onRun: (id: string) => void;
}

/** Actions only. The index is built by the caller and must not contain cell values. */
export function StudioCommandPalette({ open, actions, onClose, onRun }: StudioCommandPaletteProps) {
  const [query, setQuery] = useState("");
  const [cursor, setCursor] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const shown = useMemo(() => filterPalette(actions, query), [actions, query]);

  useEffect(() => {
    if (!open) {
      setQuery("");
      setCursor(0);
      return;
    }
    const timer = window.setTimeout(() => inputRef.current?.focus(), 0);
    return () => window.clearTimeout(timer);
  }, [open]);

  useEffect(() => {
    setCursor(0);
  }, [query, open]);

  return (
    <Dialog open={open} onClose={onClose} title="Commands" size="md" ariaLabel="Command palette">
      <input
        ref={inputRef}
        className="df2-input"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder="Add an operation, run preview, continue…"
        aria-label="Filter commands"
        role="combobox"
        aria-expanded
        aria-controls="studio-command-list"
        aria-activedescendant={shown[cursor] ? `studio-command-${shown[cursor].id}` : undefined}
        onKeyDown={(event) => {
          if (event.key === "ArrowDown") {
            event.preventDefault();
            setCursor((index) => Math.min(shown.length - 1, index + 1));
          } else if (event.key === "ArrowUp") {
            event.preventDefault();
            setCursor((index) => Math.max(0, index - 1));
          } else if (event.key === "Enter" && shown[cursor]) {
            event.preventDefault();
            onRun(shown[cursor].id);
            onClose();
          }
        }}
      />
      <ul id="studio-command-list" role="listbox" className="df2-canvas-palette-list" aria-label="Commands">
        {shown.length === 0 && <li className="df2-label-hint">No command matches.</li>}
        {shown.map((action, index) => (
          <li key={action.id} role="presentation">
            <button
              type="button"
              id={`studio-command-${action.id}`}
              role="option"
              aria-selected={index === cursor}
              className={index === cursor ? "is-active" : ""}
              onClick={() => {
                onRun(action.id);
                onClose();
              }}
            >
              <span>{action.label}</span>
              <small>{action.group}</small>
            </button>
          </li>
        ))}
      </ul>
    </Dialog>
  );
}
