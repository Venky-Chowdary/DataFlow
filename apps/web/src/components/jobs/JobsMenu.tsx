import { type KeyboardEvent, type ReactNode, useCallback, useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

export interface JobsMenuItem {
  id: string;
  label: string;
  onSelect: () => void;
  disabled?: boolean;
  /** Tooltip — also why a disabled item is disabled. */
  title?: string;
  /** Renders a checkable item (columns, density); the menu stays open on toggle. */
  checked?: boolean;
  /** `radio` for one-of-many (density), `checkbox` for independent toggles. */
  kind?: "checkbox" | "radio";
  /** Consecutive items with the same section render as one labelled group. */
  section?: string;
}

function groupItems(items: JobsMenuItem[]): { section?: string; items: JobsMenuItem[] }[] {
  const groups: { section?: string; items: JobsMenuItem[] }[] = [];
  for (const item of items) {
    const last = groups.at(-1);
    if (last && last.section === item.section) last.items.push(item);
    else groups.push({ section: item.section, items: [item] });
  }
  return groups;
}

interface JobsMenuProps {
  /** Accessible name of the trigger; shown as text unless `iconOnly`. */
  label: string;
  icon?: ReactNode;
  iconOnly?: boolean;
  items: JobsMenuItem[];
  /** Trigger classes — the app's button classes (df2-btn df2-btn-ghost df2-btn-sm). */
  triggerClassName?: string;
  triggerTabIndex?: number;
  title?: string;
  /** Visible text when it differs from the accessible label (e.g. "Density: Compact"). */
  text?: string;
  /** After the text, e.g. a chevron on a filter chip. */
  trailingIcon?: ReactNode;
}

const MENU_GAP = 4;

/**
 * Button + popup menu, after the Transfer Studio canvas "More actions" menu
 * (`df2-canvas-menu`). The popup mounts on <body>, fixed-positioned from the
 * trigger, so a scrolling grid never clips or covers it. Arrow keys move,
 * Escape returns focus.
 */
export function JobsMenu({ label, icon, iconOnly, items, triggerClassName, triggerTabIndex, title, text, trailingIcon }: JobsMenuProps) {
  const [open, setOpen] = useState(false);
  const [position, setPosition] = useState<{ top: number; left: number } | null>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const menuId = useId();

  const close = useCallback((refocus: boolean) => {
    setOpen(false);
    if (refocus) triggerRef.current?.focus();
  }, []);

  /** Place the popup under the trigger (above it near the bottom edge); false when the trigger left the viewport. */
  const place = useCallback((): boolean => {
    const trigger = triggerRef.current;
    const menu = menuRef.current;
    if (!trigger) return false;
    const rect = trigger.getBoundingClientRect();
    const viewportW = window.innerWidth || document.documentElement.clientWidth;
    const viewportH = window.innerHeight || document.documentElement.clientHeight;
    if (rect.bottom < 0 || rect.top > viewportH) return false;
    const width = menu?.offsetWidth || 220;
    const height = menu?.offsetHeight || 0;
    const left = Math.max(8, Math.min(rect.right - width, viewportW - width - 8));
    const below = rect.bottom + MENU_GAP;
    const top = height && below + height > viewportH - 8 ? Math.max(8, rect.top - height - MENU_GAP) : below;
    setPosition({ top, left });
    return true;
  }, []);

  useLayoutEffect(() => {
    if (open) place();
  }, [open, place]);

  useEffect(() => {
    if (!open) return;
    const first = menuRef.current?.querySelector<HTMLButtonElement>('[role^="menuitem"]:not([disabled])');
    first?.focus();
    const onPointer = (event: MouseEvent) => {
      const target = event.target as Node;
      if (menuRef.current?.contains(target) || triggerRef.current?.contains(target)) return;
      close(false);
    };
    // A scrolling grid moves the trigger: follow it, close once it is gone.
    const onViewportChange = () => {
      if (!place()) close(false);
    };
    document.addEventListener("mousedown", onPointer);
    window.addEventListener("resize", onViewportChange);
    window.addEventListener("scroll", onViewportChange, true);
    return () => {
      document.removeEventListener("mousedown", onPointer);
      window.removeEventListener("resize", onViewportChange);
      window.removeEventListener("scroll", onViewportChange, true);
    };
  }, [open, close, place]);

  const onMenuKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const entries = Array.from(
      menuRef.current?.querySelectorAll<HTMLButtonElement>('[role^="menuitem"]:not([disabled])') ?? [],
    );
    const index = entries.indexOf(document.activeElement as HTMLButtonElement);
    if (event.key === "Escape") {
      event.preventDefault();
      // Keep an enclosing drawer or grid from also handling this Escape.
      event.stopPropagation();
      close(true);
    } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (!entries.length) return;
      const step = event.key === "ArrowDown" ? 1 : -1;
      entries[(index + step + entries.length) % entries.length]?.focus();
    } else if (event.key === "Home" || event.key === "End") {
      event.preventDefault();
      (event.key === "Home" ? entries[0] : entries.at(-1))?.focus();
    } else if (event.key === "Tab") {
      close(false);
    }
  };

  return (
    <span className="df2-jobs-menu-anchor">
      <button
        ref={triggerRef}
        type="button"
        className={triggerClassName ?? "df2-btn df2-btn-ghost df2-btn-sm"}
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={open ? menuId : undefined}
        aria-label={iconOnly || text ? label : undefined}
        title={title ?? (iconOnly ? label : undefined)}
        tabIndex={triggerTabIndex}
        onClick={(event) => {
          event.stopPropagation();
          setOpen((value) => !value);
        }}
        onKeyDown={(event) => {
          // Enter/Space on the trigger must not also open the row it sits in.
          if (event.key === "Enter" || event.key === " " || event.key === "ArrowDown") event.stopPropagation();
          if (event.key === "ArrowDown" && !open) {
            event.preventDefault();
            setOpen(true);
          }
        }}
      >
        {icon}
        {iconOnly ? null : text ?? label}
        {trailingIcon}
      </button>
      {open && createPortal(
        <div
          ref={menuRef}
          id={menuId}
          className="df2-canvas-menu df2-jobs-menu"
          role="menu"
          aria-label={label}
          style={position ? { position: "fixed", top: position.top, left: position.left, right: "auto" } : { visibility: "hidden" }}
          onKeyDown={onMenuKeyDown}
          onClick={(event) => event.stopPropagation()}
        >
          {groupItems(items).map((group, index) => {
            const buttons = group.items.map((item) => (
              <button
                key={item.id}
                type="button"
                role={item.kind === "radio" ? "menuitemradio" : item.kind === "checkbox" ? "menuitemcheckbox" : "menuitem"}
                aria-checked={item.kind ? Boolean(item.checked) : undefined}
                disabled={item.disabled}
                title={item.title}
                tabIndex={-1}
                onClick={() => {
                  item.onSelect();
                  if (item.kind !== "checkbox") close(true);
                }}
              >
                {item.kind ? (
                  <span className="df2-jobs-menu-check" aria-hidden="true">{item.checked ? "✓" : ""}</span>
                ) : null}
                {item.label}
              </button>
            ));
            return group.section ? (
              <div key={group.section} role="group" aria-label={group.section} className="df2-jobs-menu-group">
                <div className="df2-jobs-menu-heading" aria-hidden="true">{group.section}</div>
                {buttons}
              </div>
            ) : (
              <div key={`group-${index}`} role="none" className="df2-jobs-menu-group">{buttons}</div>
            );
          })}
        </div>,
        // On <body>: a pinned grid column is its own stacking context and
        // would paint later rows' cells over a popup left inside it.
        document.body,
      )}
    </span>
  );
}
