import { useEffect, useId, useMemo, useRef, useState } from "react";
import { DtIcon } from "../DtIcon";

interface ObjectNameComboboxProps {
  id: string;
  label: string;
  value: string;
  onChange: (value: string) => void;
  options: string[];
  placeholder?: string;
  loading?: boolean;
  emptyHint?: string;
  objectNoun?: string;
}

/**
 * Type-or-pick control for destination table/collection names.
 * Shows a real dropdown of introspected objects (not a native datalist).
 */
export function ObjectNameCombobox({
  id,
  label,
  value,
  onChange,
  options,
  placeholder = "Pick existing or type a new name",
  loading = false,
  emptyHint,
  objectNoun = "table",
}: ObjectNameComboboxProps) {
  const listId = useId();
  const rootRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);

  const filtered = useMemo(() => {
    const q = value.trim().toLowerCase();
    if (!q) return options.slice(0, 200);
    const starts: string[] = [];
    const contains: string[] = [];
    for (const name of options) {
      const n = name.toLowerCase();
      if (n === q) continue;
      if (n.startsWith(q)) starts.push(name);
      else if (n.includes(q)) contains.push(name);
    }
    return [...starts, ...contains].slice(0, 200);
  }, [options, value]);

  const exactMatch = useMemo(() => {
    const q = value.trim().toLowerCase();
    if (!q) return false;
    return options.some((n) => n.toLowerCase() === q);
  }, [options, value]);

  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (!rootRef.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  useEffect(() => {
    setActiveIndex(0);
  }, [filtered, open]);

  const pick = (name: string) => {
    onChange(name);
    setOpen(false);
    inputRef.current?.focus();
  };

  const showCreateRow =
    value.trim().length > 0 && !exactMatch && options.length > 0;

  const canOpen = options.length > 0 || loading;

  return (
    <div className="df2-field df2-object-combobox" ref={rootRef}>
      <label className="df2-label" htmlFor={id}>
        {label}
      </label>
      <div className={`df2-object-combobox-control${open ? " is-open" : ""}`}>
        <input
          ref={inputRef}
          id={id}
          className="df2-input"
          role="combobox"
          aria-expanded={open}
          aria-controls={listId}
          aria-autocomplete="list"
          aria-busy={loading || undefined}
          autoComplete="off"
          spellCheck={false}
          value={value}
          placeholder={placeholder}
          onChange={(e) => {
            onChange(e.target.value);
            if (canOpen) setOpen(true);
          }}
          onFocus={() => {
            if (canOpen) setOpen(true);
          }}
          onKeyDown={(e) => {
            if (e.key === "Escape") {
              setOpen(false);
              return;
            }
            if (e.key === "ArrowDown") {
              e.preventDefault();
              if (!open && canOpen) setOpen(true);
              else setActiveIndex((i) => Math.min(i + 1, filtered.length - 1 + (showCreateRow ? 1 : 0)));
              return;
            }
            if (e.key === "ArrowUp") {
              e.preventDefault();
              setActiveIndex((i) => Math.max(i - 1, 0));
              return;
            }
            if (e.key === "Enter" && open) {
              const createIdx = showCreateRow ? filtered.length : -1;
              if (activeIndex === createIdx && showCreateRow) {
                e.preventDefault();
                setOpen(false);
                return;
              }
              if (filtered[activeIndex]) {
                e.preventDefault();
                pick(filtered[activeIndex]);
              }
            }
          }}
        />
        <button
          type="button"
          className="df2-object-combobox-toggle"
          tabIndex={-1}
          aria-label={open ? `Hide ${objectNoun} list` : `Show ${objectNoun} list`}
          disabled={!canOpen}
          onClick={() => {
            if (!canOpen) return;
            setOpen((v) => !v);
            inputRef.current?.focus();
          }}
        >
          {loading ? (
            <span className="df2-object-combobox-spinner" aria-hidden />
          ) : (
            <DtIcon name="chevron-down" size={14} />
          )}
        </button>
      </div>

      {open && canOpen && (
        <ul
          id={listId}
          className="df2-object-combobox-menu"
          role="listbox"
          aria-label={`Existing ${objectNoun}s`}
        >
          {loading && options.length === 0 ? (
            <li className="df2-object-combobox-empty" role="presentation">
              Loading {objectNoun}s…
            </li>
          ) : filtered.length === 0 && !showCreateRow ? (
            <li className="df2-object-combobox-empty" role="presentation">
              {options.length === 0
                ? emptyHint || `No ${objectNoun}s discovered yet — type a name to create.`
                : `No ${objectNoun}s match “${value.trim()}”.`}
            </li>
          ) : (
            <>
              {filtered.map((name, i) => (
                <li key={name} role="option" aria-selected={i === activeIndex}>
                  <button
                    type="button"
                    className={`df2-object-combobox-option${i === activeIndex ? " is-active" : ""}`}
                    onMouseEnter={() => setActiveIndex(i)}
                    onClick={() => pick(name)}
                  >
                    <span className="df2-object-combobox-option-name">{name}</span>
                    <span className="df2-object-combobox-option-meta">existing</span>
                  </button>
                </li>
              ))}
              {showCreateRow && (
                <li role="option" aria-selected={activeIndex === filtered.length}>
                  <button
                    type="button"
                    className={`df2-object-combobox-option is-create${
                      activeIndex === filtered.length ? " is-active" : ""
                    }`}
                    onMouseEnter={() => setActiveIndex(filtered.length)}
                    onClick={() => {
                      setOpen(false);
                      inputRef.current?.focus();
                    }}
                  >
                    <span className="df2-object-combobox-option-name">
                      Create “{value.trim()}”
                    </span>
                    <span className="df2-object-combobox-option-meta">new</span>
                  </button>
                </li>
              )}
            </>
          )}
        </ul>
      )}
    </div>
  );
}
