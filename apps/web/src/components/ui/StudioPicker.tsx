import {
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type ReactNode,
  type RefObject,
} from "react";
import { createPortal } from "react-dom";
import { DtIcon } from "../DtIcon";
import {
  filterStudioOptions,
  groupStudioOptions,
  studioPickerSelection,
  type StudioPickerOption,
} from "../../lib/studioPicker";

export type { StudioPickerOption };

interface StudioPickerBase {
  id: string;
  label: string;
  options: StudioPickerOption[];
  placeholder?: string;
  disabled?: boolean;
  hint?: string;
  required?: boolean;
  invalid?: boolean;
  searchable?: boolean;
  emptyHint?: string;
}

interface StudioPickerProps extends StudioPickerBase {
  value: string;
  onChange: (value: string) => void;
}

interface StudioMultiPickerProps extends StudioPickerBase {
  value: string[];
  onChange: (value: string[]) => void;
}

function useMenuBox(open: boolean, controlRef: RefObject<HTMLElement | null>) {
  const [box, setBox] = useState<{ top: number; left: number; width: number; maxHeight: number } | null>(null);

  const compute = () => {
    const control = controlRef.current;
    if (!control) return null;
    const rect = control.getBoundingClientRect();
    const gap = 6;
    const spaceBelow = window.innerHeight - rect.bottom - gap - 8;
    const spaceAbove = rect.top - 8;
    const preferBelow = spaceBelow >= 180 || spaceBelow >= spaceAbove;
    const maxHeight = Math.min(320, Math.max(160, preferBelow ? spaceBelow : spaceAbove));
    return {
      top: preferBelow ? rect.bottom + gap : Math.max(8, rect.top - gap - maxHeight),
      left: rect.left,
      width: Math.max(rect.width, 240),
      maxHeight,
    };
  };

  const place = () => {
    const next = compute();
    if (next) setBox(next);
  };

  useLayoutEffect(() => {
    if (!open) {
      setBox(null);
      return;
    }
    place();
    const onMove = () => place();
    window.addEventListener("resize", onMove);
    window.addEventListener("scroll", onMove, true);
    return () => {
      window.removeEventListener("resize", onMove);
      window.removeEventListener("scroll", onMove, true);
    };
  }, [open]);

  return { box, place };
}

function PickerField({
  id,
  label,
  required,
  invalid,
  hint,
  children,
}: {
  id: string;
  label: string;
  required?: boolean;
  invalid?: boolean;
  hint?: string;
  children: ReactNode;
}) {
  return (
    <div className={`df2-field df2-studio-picker${invalid ? " is-invalid" : ""}`}>
      <label className="df2-label" htmlFor={id}>
        {label}{required ? " *" : ""}
      </label>
      {children}
      {hint ? <span className="df2-label-hint">{hint}</span> : null}
    </div>
  );
}

export function StudioPicker({
  id,
  label,
  value,
  onChange,
  options,
  placeholder = "Pick…",
  disabled = false,
  hint,
  required,
  invalid,
  searchable = true,
  emptyHint = "No matches.",
}: StudioPickerProps) {
  const listId = useId();
  const rootRef = useRef<HTMLDivElement>(null);
  const controlRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const { box } = useMenuBox(open, controlRef);

  const selected = studioPickerSelection(options, value);
  const filtered = useMemo(
    () => (searchable ? filterStudioOptions(options, query) : options),
    [options, query, searchable],
  );
  const grouped = useMemo(() => groupStudioOptions(filtered), [filtered]);

  useEffect(() => {
    if (!open) return;
    const onDoc = (event: MouseEvent) => {
      const node = event.target as Node;
      if (rootRef.current?.contains(node) || menuRef.current?.contains(node)) return;
      setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  useEffect(() => {
    setActiveIndex(0);
  }, [query, open]);

  useEffect(() => {
    if (!open) return;
    window.requestAnimationFrame(() => {
      if (searchable) searchRef.current?.focus();
      if (listRef.current) listRef.current.scrollTop = 0;
    });
  }, [open, searchable]);

  const pick = (next: string) => {
    onChange(next);
    setOpen(false);
    setQuery("");
    controlRef.current?.focus();
  };

  const onKeyDown = (event: KeyboardEvent) => {
    if (event.key === "Escape") {
      event.preventDefault();
      setOpen(false);
      controlRef.current?.focus();
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (!open) setOpen(true);
      else setActiveIndex((index) => Math.min(index + 1, Math.max(filtered.length - 1, 0)));
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((index) => Math.max(index - 1, 0));
      return;
    }
    if (event.key === "Enter" && open && filtered[activeIndex]) {
      event.preventDefault();
      pick(filtered[activeIndex].value);
    }
  };

  const menu = open && box
    ? createPortal(
        <div
          ref={menuRef}
          id={listId}
          className="df2-studio-picker-menu"
          role="listbox"
          aria-label={label}
          style={{ top: box.top, left: box.left, width: box.width, maxHeight: box.maxHeight }}
          onKeyDown={onKeyDown}
        >
          {searchable && (
            <div className="df2-studio-picker-search">
              <DtIcon name="search" size={14} />
              <input
                ref={searchRef}
                className="df2-input"
                value={query}
                placeholder="Search operations, columns…"
                aria-label={`Search ${label}`}
                autoComplete="off"
                spellCheck={false}
                onChange={(event) => setQuery(event.target.value)}
              />
            </div>
          )}
          <div ref={listRef} className="df2-studio-picker-list">
            {filtered.length === 0 ? (
              <p className="df2-studio-picker-empty">{emptyHint}</p>
            ) : grouped.map((bucket) => (
              <div key={bucket.group || "ungrouped"} className="df2-studio-picker-group">
                {bucket.group ? <p className="df2-studio-picker-group-label">{bucket.group}</p> : null}
                {bucket.options.map((opt) => {
                  const index = filtered.indexOf(opt);
                  const active = index === activeIndex;
                  const isSelected = opt.value === value;
                  return (
                    <button
                      key={opt.value}
                      type="button"
                      role="option"
                      aria-selected={isSelected}
                      className={`df2-studio-picker-option${active ? " is-active" : ""}${isSelected ? " is-selected" : ""}`}
                      onMouseEnter={() => setActiveIndex(index)}
                      onClick={() => pick(opt.value)}
                    >
                      <span className="df2-studio-picker-option-copy">
                        <strong>{opt.label}</strong>
                        {opt.hint && opt.hint !== opt.label ? <small>{opt.hint}</small> : null}
                      </span>
                      {opt.meta ? <span className="df2-studio-picker-option-meta">{opt.meta}</span> : null}
                      {isSelected ? <DtIcon name="check" size={14} /> : null}
                    </button>
                  );
                })}
              </div>
            ))}
          </div>
        </div>,
        document.body,
      )
    : null;

  return (
    <PickerField id={id} label={label} required={required} invalid={invalid} hint={hint}>
      <div ref={rootRef} className={`df2-studio-picker-root${open ? " is-open" : ""}`}>
        <button
          ref={controlRef}
          id={id}
          type="button"
          className="df2-studio-picker-trigger"
          disabled={disabled}
          aria-haspopup="listbox"
          aria-expanded={open}
          aria-controls={listId}
          aria-invalid={invalid || undefined}
          onClick={() => { if (!disabled) setOpen((next) => !next); }}
          onKeyDown={onKeyDown}
        >
          <span className={`df2-studio-picker-value${selected ? "" : " is-placeholder"}`}>
            {selected ? (
              <>
                <strong>{selected.label}</strong>
                {selected.hint && selected.hint !== selected.label ? <small>{selected.hint}</small> : null}
              </>
            ) : placeholder}
          </span>
          <span className="df2-studio-picker-chevron" aria-hidden>
            <DtIcon name="chevron-down" size={14} />
          </span>
        </button>
        {menu}
      </div>
    </PickerField>
  );
}

export function StudioMultiPicker({
  id,
  label,
  value,
  onChange,
  options,
  placeholder = "Pick columns…",
  disabled = false,
  hint,
  required,
  invalid,
  searchable = true,
  emptyHint = "No matches.",
}: StudioMultiPickerProps) {
  const listId = useId();
  const rootRef = useRef<HTMLDivElement>(null);
  const controlRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const { box } = useMenuBox(open, controlRef);
  const selected = useMemo(
    () => options.filter((opt) => value.includes(opt.value)),
    [options, value],
  );
  const filtered = useMemo(
    () => (searchable ? filterStudioOptions(options, query) : options),
    [options, query, searchable],
  );

  useEffect(() => {
    if (!open) return;
    const onDoc = (event: MouseEvent) => {
      const node = event.target as Node;
      if (rootRef.current?.contains(node) || menuRef.current?.contains(node)) return;
      setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  useEffect(() => {
    if (open && searchable) {
      window.requestAnimationFrame(() => searchRef.current?.focus());
    }
  }, [open, searchable]);

  const toggle = (next: string) => {
    if (value.includes(next)) onChange(value.filter((item) => item !== next));
    else onChange([...value, next]);
  };

  const menu = open && box
    ? createPortal(
        <div
          ref={menuRef}
          id={listId}
          className="df2-studio-picker-menu"
          role="listbox"
          aria-multiselectable="true"
          aria-label={label}
          style={{ top: box.top, left: box.left, width: box.width, maxHeight: box.maxHeight }}
        >
          {searchable && (
            <div className="df2-studio-picker-search">
              <DtIcon name="search" size={14} />
              <input
                ref={searchRef}
                className="df2-input"
                value={query}
                placeholder="Search columns…"
                aria-label={`Search ${label}`}
                autoComplete="off"
                spellCheck={false}
                onChange={(event) => setQuery(event.target.value)}
              />
            </div>
          )}
          <div className="df2-studio-picker-list">
            {filtered.length === 0 ? (
              <p className="df2-studio-picker-empty">{emptyHint}</p>
            ) : filtered.map((opt, index) => {
              const isSelected = value.includes(opt.value);
              return (
                <button
                  key={opt.value}
                  type="button"
                  role="option"
                  aria-selected={isSelected}
                  className={`df2-studio-picker-option${index === activeIndex ? " is-active" : ""}${isSelected ? " is-selected" : ""}`}
                  onMouseEnter={() => setActiveIndex(index)}
                  onClick={() => toggle(opt.value)}
                >
                  <span className={`df2-studio-picker-check${isSelected ? " is-on" : ""}`} aria-hidden />
                  <span className="df2-studio-picker-option-copy">
                    <strong>{opt.label}</strong>
                  </span>
                </button>
              );
            })}
          </div>
        </div>,
        document.body,
      )
    : null;

  return (
    <PickerField id={id} label={label} required={required} invalid={invalid} hint={hint}>
      <div ref={rootRef} className={`df2-studio-picker-root${open ? " is-open" : ""}`}>
        <button
          ref={controlRef}
          id={id}
          type="button"
          className="df2-studio-picker-trigger is-multi"
          disabled={disabled}
          aria-haspopup="listbox"
          aria-expanded={open}
          aria-controls={listId}
          aria-invalid={invalid || undefined}
          onClick={() => { if (!disabled) setOpen((next) => !next); }}
        >
          <span className={`df2-studio-picker-value${selected.length ? "" : " is-placeholder"}`}>
            {selected.length
              ? selected.map((opt) => (
                  <span key={opt.value} className="df2-studio-picker-chip">{opt.label}</span>
                ))
              : placeholder}
          </span>
          <span className="df2-studio-picker-chevron" aria-hidden>
            <DtIcon name="chevron-down" size={14} />
          </span>
        </button>
        {menu}
      </div>
    </PickerField>
  );
}
