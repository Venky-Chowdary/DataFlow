import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
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
 * Menu is portaled + fixed so Transfer Studio overflow panels cannot clip
 * the list (no-scroll / missing dropdown under Destination right rail).
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
  const controlRef = useRef<HTMLDivElement>(null);
  const menuRef = useRef<HTMLUListElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const [menuBox, setMenuBox] = useState<{
    top: number;
    left: number;
    width: number;
    maxHeight: number;
  } | null>(null);

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

  const computeMenuBox = () => {
    const control = controlRef.current;
    if (!control) return null;
    const rect = control.getBoundingClientRect();
    const gap = 4;
    const spaceBelow = window.innerHeight - rect.bottom - gap - 8;
    const spaceAbove = rect.top - 8;
    const preferBelow = spaceBelow >= 140 || spaceBelow >= spaceAbove;
    const maxHeight = Math.min(280, Math.max(120, preferBelow ? spaceBelow : spaceAbove));
    const top = preferBelow
      ? rect.bottom + gap
      : Math.max(8, rect.top - gap - maxHeight);
    return {
      top,
      left: rect.left,
      width: Math.max(rect.width, 180),
      maxHeight,
    };
  };

  const placeMenu = () => {
    const box = computeMenuBox();
    if (box) setMenuBox(box);
  };

  const openMenu = () => {
    // Place immediately so the first paint is not a blank frame (menuBox null).
    const box = computeMenuBox();
    if (box) setMenuBox(box);
    setOpen(true);
  };

  const closeMenu = () => {
    setOpen(false);
    setMenuBox(null);
  };

  useLayoutEffect(() => {
    if (!open) return;
    placeMenu();
    const onReposition = () => placeMenu();
    window.addEventListener("resize", onReposition);
    // Capture scroll from Transfer Studio overflow panels.
    window.addEventListener("scroll", onReposition, true);
    return () => {
      window.removeEventListener("resize", onReposition);
      window.removeEventListener("scroll", onReposition, true);
    };
  }, [open, filtered.length, value]);

  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      const t = e.target as Node;
      if (rootRef.current?.contains(t) || menuRef.current?.contains(t)) return;
      closeMenu();
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  useEffect(() => {
    setActiveIndex(0);
  }, [filtered, open]);

  const pick = (name: string) => {
    onChange(name);
    closeMenu();
    inputRef.current?.focus();
  };

  const showCreateRow =
    value.trim().length > 0 && !exactMatch;

  // Always allow open — empty discovery must still show the create/empty hint
  // (previously options.length===0 hid the dropdown entirely → "sometimes not showing").
  const canOpen = true;
  const optionCount = filtered.length + (showCreateRow ? 1 : 0);

  const menu =
    open && canOpen && menuBox
      ? createPortal(
          <ul
            ref={menuRef}
            id={listId}
            className="df2-object-combobox-menu is-portaled"
            role="listbox"
            aria-label={`Existing ${objectNoun}s`}
            style={{
              top: menuBox.top,
              left: menuBox.left,
              width: menuBox.width,
              maxHeight: menuBox.maxHeight,
            }}
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
                      closeMenu();
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
          </ul>,
          document.body,
        )
      : null;

  return (
    <div className={`df2-field df2-object-combobox${open ? " is-open" : ""}`} ref={rootRef}>
      <label className="df2-label" htmlFor={id}>
        {label}
      </label>
      <div
        ref={controlRef}
        className={`df2-object-combobox-control${open ? " is-open" : ""}`}
      >
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
            openMenu();
          }}
          onFocus={() => {
            openMenu();
          }}
          onKeyDown={(e) => {
            if (e.key === "Escape") {
              closeMenu();
              return;
            }
            if (e.key === "ArrowDown") {
              e.preventDefault();
              if (!open) openMenu();
              else setActiveIndex((i) => Math.min(i + 1, Math.max(optionCount - 1, 0)));
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
                closeMenu();
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
          onClick={() => {
            if (open) closeMenu();
            else openMenu();
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
      {menu}
    </div>
  );
}
