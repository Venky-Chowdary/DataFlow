import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { createPortal } from "react-dom";
import {
  destColumnFacts,
  filterDestColumns,
  findDestColumn,
  type DestColumn,
  type DestColumnCatalog,
} from "../../lib/destColumnCatalog";
import { useVirtualWindow } from "./useVirtualWindow";

/** Fixed option height: the list is windowed, so every row is this tall (CSS agrees). */
export const COMBO_ROW_PX = 48;

type ComboOption =
  | { kind: "column"; key: string; column: DestColumn }
  | { kind: "create"; key: string; name: string }
  | { kind: "refused"; key: string; name: string }
  | { kind: "clear"; key: string };

interface DestColumnComboboxProps {
  sourceName: string;
  value: string;
  catalog: DestColumnCatalog;
  /** lower(target) → every source writing it (this row included). */
  claimedBy: Map<string, string[]>;
  onCommit: (target: string) => void;
  disabled?: boolean;
  inputRef?: (el: HTMLInputElement | null) => void;
}

function selectable(o: ComboOption | undefined): boolean {
  if (!o || o.kind === "refused") return false;
  // A generated column is listed (so the operator sees it exists) but never written.
  return o.kind !== "column" || o.column.writable;
}

/**
 * Searchable destination-column picker for one mapping row (WAI-ARIA combobox).
 *
 * Lists the existing destination columns with type, nullability, key and
 * default; marks the ones another source already writes; offers a clearly
 * labelled "will create column" only when the destination can add one. The
 * list is windowed, so a 1,000-column table costs the same as a 10-column one.
 * Typing never commits by itself: Enter or a click does, Escape or leaving the
 * field restores the current target.
 */
export function DestColumnCombobox({
  sourceName,
  value,
  catalog,
  claimedBy,
  onCommit,
  disabled = false,
  inputRef,
}: DestColumnComboboxProps) {
  const listId = useId();
  const hintId = useId();
  const optionId = (i: number) => `${listId}-opt-${i}`;
  const ownInput = useRef<HTMLInputElement | null>(null);
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<string | null>(null);
  const [active, setActive] = useState(0);
  const [box, setBox] = useState<{ top: number; left: number; width: number; maxHeight: number } | null>(null);

  const query = draft ?? "";
  const options = useMemo<ComboOption[]>(() => {
    const cols = filterDestColumns(catalog.columns, query);
    const out: ComboOption[] = cols.map((column) => ({ kind: "column", key: `c:${column.name}`, column }));
    const typed = query.trim();
    if (typed && !findDestColumn(catalog, typed)) {
      out.push(
        catalog.canAddColumns
          ? { kind: "create", key: `n:${typed}`, name: typed }
          : { kind: "refused", key: `r:${typed}`, name: typed },
      );
    }
    if (value && !typed) out.push({ kind: "clear", key: "clear" });
    return out;
  }, [catalog, query, value]);

  const win = useVirtualWindow(open ? options.length : 0, COMBO_ROW_PX, 288);

  const place = () => {
    const el = ownInput.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const below = window.innerHeight - r.bottom - 12;
    const above = r.top - 12;
    const preferBelow = below >= 160 || below >= above;
    const maxHeight = Math.min(288, Math.max(144, preferBelow ? below : above));
    setBox({
      top: preferBelow ? r.bottom + 4 : Math.max(8, r.top - 4 - maxHeight),
      left: Math.min(r.left, Math.max(8, window.innerWidth - Math.max(r.width, 300) - 8)),
      width: Math.min(Math.max(r.width, 300), window.innerWidth - 16),
      maxHeight,
    });
  };

  useLayoutEffect(() => {
    if (!open) return;
    place();
    const onMove = () => place();
    window.addEventListener("resize", onMove);
    window.addEventListener("scroll", onMove, true);
    return () => {
      window.removeEventListener("resize", onMove);
      window.removeEventListener("scroll", onMove, true);
    };
  }, [open]);

  useEffect(() => {
    if (open) win.scrollToIndex(active);
  }, [active, open]);

  const openList = (fromTyping: boolean) => {
    if (disabled) return;
    setOpen(true);
    if (!fromTyping) {
      setDraft("");
      const at = catalog.columns.findIndex((c) => c.name.toLowerCase() === value.toLowerCase());
      setActive(Math.max(0, at));
    }
  };

  const close = () => {
    setOpen(false);
    setDraft(null);
    setBox(null);
  };

  const commit = (o: ComboOption | undefined) => {
    if (!o || !selectable(o)) return;
    const next = o.kind === "column" ? o.column.name : o.kind === "create" ? o.name : "";
    close();
    if (next !== value) onCommit(next);
  };

  const step = (delta: number) => {
    if (options.length === 0) return;
    let i = active;
    for (let n = 0; n < options.length; n += 1) {
      i = Math.max(0, Math.min(options.length - 1, i + delta));
      if (selectable(options[i])) break;
      if (i === 0 || i === options.length - 1) break;
    }
    setActive(i);
  };

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      if (!open) openList(false);
      else step(e.key === "ArrowDown" ? 1 : -1);
      return;
    }
    if (!open) {
      if (e.key === "Enter") {
        e.preventDefault();
        openList(false);
      }
      return;
    }
    if (e.key === "PageDown" || e.key === "PageUp") {
      e.preventDefault();
      step(e.key === "PageDown" ? 6 : -6);
    } else if (e.key === "Enter") {
      e.preventDefault();
      commit(options[active]);
    } else if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      close();
    } else if (e.key === "Tab") {
      close();
    }
  };

  const claimNote = (name: string): string => {
    const others = (claimedBy.get(name.toLowerCase()) ?? []).filter((s) => s !== sourceName);
    return others.length ? `mapped from ${others.join(", ")}` : "";
  };

  const current = findDestColumn(catalog, value);
  const isNew = Boolean(value) && !current && catalog.columns.length > 0;
  const activeOption = open ? options[active] : undefined;

  const menu = open && box
    ? createPortal(
        <ul
          ref={win.ref}
          id={listId}
          role="listbox"
          aria-label={`Destination columns for ${sourceName}`}
          className="df2-ms-combo-menu"
          style={{ top: box.top, left: box.left, width: box.width, maxHeight: box.maxHeight }}
          // Keep focus in the input: a click on an option must not blur it first.
          onMouseDown={(e) => e.preventDefault()}
        >
          {options.length === 0 ? (
            <li className="df2-ms-combo-empty" role="presentation">
              No destination columns match “{query}”.
            </li>
          ) : (
            <>
              <li role="presentation" style={{ height: win.offsetTop }} aria-hidden="true" />
              {options.slice(win.start, win.end).map((o, k) => {
                const i = win.start + k;
                const isActive = i === active;
                const common = {
                  id: optionId(i),
                  role: "option" as const,
                  "aria-setsize": options.length,
                  "aria-posinset": i + 1,
                  className: `df2-ms-combo-option is-${o.kind}${isActive ? " is-active" : ""}`,
                  onMouseEnter: () => setActive(i),
                  onClick: () => commit(o),
                };
                if (o.kind === "column") {
                  const note = claimNote(o.column.name);
                  const selected = o.column.name.toLowerCase() === value.toLowerCase();
                  return (
                    <li
                      key={o.key}
                      {...common}
                      aria-selected={selected}
                      aria-disabled={o.column.writable ? undefined : "true"}
                    >
                      <span className="df2-ms-combo-name">
                        {o.column.name}
                        {!o.column.writable ? <span className="df2-ms-tag is-claimed">engine-filled · cannot be written</span> : null}
                        {o.column.required ? <span className="df2-ms-tag is-required">required</span> : null}
                        {note ? <span className="df2-ms-tag is-claimed">{note}</span> : null}
                      </span>
                      <span className="df2-ms-combo-facts">{destColumnFacts(o.column).join(" · ") || "type not read"}</span>
                    </li>
                  );
                }
                if (o.kind === "create") {
                  return (
                    <li key={o.key} {...common} aria-selected={false}>
                      <span className="df2-ms-combo-name">
                        <span className="df2-ms-tag is-create">will create column</span> {o.name}
                      </span>
                      <span className="df2-ms-combo-facts">{catalog.addColumnsReason}</span>
                    </li>
                  );
                }
                if (o.kind === "refused") {
                  return (
                    <li key={o.key} {...common} aria-selected={false} aria-disabled="true">
                      <span className="df2-ms-combo-name">“{o.name}” is not a destination column</span>
                      <span className="df2-ms-combo-facts">{catalog.addColumnsReason}</span>
                    </li>
                  );
                }
                return (
                  <li key={o.key} {...common} aria-selected={false}>
                    <span className="df2-ms-combo-name">Unmap {sourceName}</span>
                    <span className="df2-ms-combo-facts">Leave the destination column empty for this source</span>
                  </li>
                );
              })}
              <li
                role="presentation"
                aria-hidden="true"
                style={{ height: win.tailHeight }}
              />
            </>
          )}
        </ul>,
        document.body,
      )
    : null;

  return (
    <div className={`df2-ms-combo${isNew ? " is-new" : ""}`}>
      <input
        aria-label={`Destination column for ${sourceName}`}
        ref={(el) => {
          ownInput.current = el;
          inputRef?.(el);
        }}
        className="df2-input df2-column-target-input df2-ms-combo-input"
        role="combobox"
        aria-autocomplete="list"
        aria-expanded={open}
        aria-controls={listId}
        aria-activedescendant={activeOption && selectable(activeOption) ? optionId(active) : undefined}
        aria-describedby={hintId}
        value={draft ?? value}
        placeholder={catalog.columns.length ? "Pick a destination column" : "Destination name"}
        disabled={disabled}
        spellCheck={false}
        autoComplete="off"
        onChange={(e) => {
          setDraft(e.target.value);
          setActive(0);
          if (!open) openList(true);
        }}
        onFocus={(e) => e.currentTarget.select()}
        onClick={() => (open ? undefined : openList(false))}
        onBlur={close}
        onKeyDown={onKeyDown}
      />
      <span id={hintId} className="df2-sr-only">
        {current
          ? `${current.name}: ${destColumnFacts(current).join(", ")}.`
          : isNew
            ? `${value} is not in the destination table: it will be created.`
            : ""}
        {" "}Type to search {catalog.columns.length} destination columns; arrow keys choose, Enter maps, Escape cancels.
      </span>
      {isNew ? <span className="df2-ms-tag is-create" title={catalog.addColumnsReason}>new column</span> : null}
      {menu}
    </div>
  );
}
