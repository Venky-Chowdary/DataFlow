import { Fragment, type ReactNode, useCallback, useEffect, useRef, useState } from "react";
import * as Menu from "@radix-ui/react-dropdown-menu";
import { pushOverlay } from "../../lib/overlayStack";
import { DtIcon } from "../DtIcon";

export interface JobsMenuItem {
  id: string;
  label: string;
  onSelect: () => void;
  disabled?: boolean;
  /**
   * Tooltip on an enabled item. On a disabled item it is the reason, shown
   * as a second line so keyboard and touch users can read it too.
   */
  title?: string;
  /** Renders a checkable item (columns, density); the menu stays open on toggle. */
  checked?: boolean;
  /** `radio` for one-of-many (density), `checkbox` for independent toggles. */
  kind?: "checkbox" | "radio";
  /** Consecutive items with the same section render as one labelled group. */
  section?: string;
  /**
   * `danger` for destructive actions (cancel a job, clear all mappings): red
   * text, set apart by a separator, always last in its group. Confirm or
   * offer undo at the call site.
   */
  tone?: "danger";
}

interface MenuGroup { section?: string; items: JobsMenuItem[] }

function groupItems(items: JobsMenuItem[]): MenuGroup[] {
  const groups: MenuGroup[] = [];
  for (const item of items) {
    const last = groups.at(-1);
    if (last && last.section === item.section) last.items.push(item);
    else groups.push({ section: item.section, items: [item] });
  }
  // Destructive items go last in their group, behind a separator.
  for (const group of groups) {
    group.items = [...group.items.filter((i) => i.tone !== "danger"), ...group.items.filter((i) => i.tone === "danger")];
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

/** The one overflow trigger glyph: a horizontal ellipsis. */
export function OverflowIcon({ size = 16 }: { size?: number }) {
  return <DtIcon name="more" size={size} chrome="product" />;
}

const FIRST_ITEM = '[role^="menuitem"]:not([data-disabled])';

/**
 * Button + dropdown menu on Radix DropdownMenu: focus moves into the menu on
 * open, arrows/Home/End/typeahead move, Enter/Space choose, Escape closes and
 * returns focus to the trigger, Tab closes and continues from the trigger.
 * The popup is portaled to <body> and follows its trigger (a scrolling grid
 * never clips it); it hides when the trigger scrolls out of view.
 */
export function JobsMenu({ label, icon, iconOnly, items, triggerClassName, triggerTabIndex, title, text, trailingIcon }: JobsMenuProps) {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const leavingByTab = useRef(false);

  // While open, the menu is the top overlay: an enclosing dialog or drawer
  // leaves Escape and Tab to it.
  useEffect(() => {
    if (!open || !contentRef.current) return;
    return pushOverlay(contentRef.current);
  }, [open]);

  const focusFirstItem = useCallback((event: Event) => {
    event.preventDefault();
    contentRef.current?.querySelector<HTMLElement>(FIRST_ITEM)?.focus({ preventScroll: true });
  }, []);

  const groups = groupItems(items);

  const renderItem = (item: JobsMenuItem) => {
    const reason = item.disabled && item.title ? item.title : "";
    const className = `df-menu-item${item.tone === "danger" ? " is-danger" : ""}${reason ? " has-reason" : ""}`;
    const body = (
      <>
        {item.kind ? (
          <span className="df-menu-item-check" aria-hidden="true">
            {item.checked ? <DtIcon name="check" size={14} chrome="product" /> : null}
          </span>
        ) : null}
        <span className="df-menu-item-text">
          {item.label}
          {reason ? <span className="df-menu-item-reason">{reason}</span> : null}
        </span>
      </>
    );
    const common = {
      className,
      disabled: item.disabled,
      title: reason ? undefined : item.title,
      "data-menu-item-id": item.id,
    };
    if (item.kind === "checkbox") {
      return (
        <Menu.CheckboxItem
          key={item.id}
          {...common}
          checked={Boolean(item.checked)}
          onSelect={(event) => {
            event.preventDefault();
            item.onSelect();
          }}
        >
          {body}
        </Menu.CheckboxItem>
      );
    }
    if (item.kind === "radio") {
      return (
        <Menu.RadioItem key={item.id} {...common} value={item.id} onSelect={() => item.onSelect()}>
          {body}
        </Menu.RadioItem>
      );
    }
    return (
      <Menu.Item key={item.id} {...common} onSelect={() => item.onSelect()}>
        {body}
      </Menu.Item>
    );
  };

  const renderGroupItems = (group: MenuGroup) => {
    const out: ReactNode[] = [];
    let index = 0;
    while (index < group.items.length) {
      const item = group.items[index];
      if (item.tone === "danger" && index > 0 && group.items[index - 1].tone !== "danger") {
        out.push(<Menu.Separator key={`sep-${item.id}`} className="df-menu-separator" />);
      }
      if (item.kind === "radio") {
        const run: JobsMenuItem[] = [];
        while (index < group.items.length && group.items[index].kind === "radio") run.push(group.items[index++]);
        out.push(
          <Menu.RadioGroup key={`radio-${run[0].id}`} role="none" value={run.find((r) => r.checked)?.id ?? ""}>
            {run.map(renderItem)}
          </Menu.RadioGroup>,
        );
        continue;
      }
      out.push(renderItem(item));
      index += 1;
    }
    return out;
  };

  return (
    <span className="df2-jobs-menu-anchor">
      <Menu.Root open={open} onOpenChange={setOpen} modal={false}>
        <Menu.Trigger
          ref={triggerRef}
          className={triggerClassName ?? "df2-btn df2-btn-ghost df2-btn-sm"}
          aria-label={iconOnly || text ? label : undefined}
          title={title ?? (iconOnly ? label : undefined)}
          tabIndex={triggerTabIndex}
          // Open on click (pointer, keyboard-synthesised or assistive tech)
          // rather than Radix's pointerdown, so every way in behaves alike.
          onPointerDown={(event) => event.preventDefault()}
          onClick={(event) => {
            event.stopPropagation();
            setOpen((value) => !value);
          }}
          onKeyDown={(event) => {
            // Enter/Space/ArrowDown on the trigger must not also open the row it sits in.
            if (event.key === "Enter" || event.key === " " || event.key === "ArrowDown") event.stopPropagation();
          }}
        >
          {icon}
          {iconOnly ? null : text ?? label}
          {trailingIcon}
        </Menu.Trigger>
        <Menu.Portal>
          <Menu.Content
            ref={contentRef}
            className="df-menu"
            aria-label={label}
            side="bottom"
            align="end"
            sideOffset={4}
            collisionPadding={8}
            hideWhenDetached
            loop
            // Radix's MenuContent honours onOpenAutoFocus; DropdownMenu only
            // leaves it out of its prop types. The first item gets focus on
            // every open (pointer too), per the menu-button pattern.
            {...({ onOpenAutoFocus: focusFirstItem } as object)}
            onCloseAutoFocus={(event) => {
              if (leavingByTab.current) {
                leavingByTab.current = false;
                event.preventDefault();
              }
            }}
            onPointerDownOutside={(event) => {
              // A press on the trigger toggles it closed through its click.
              if (triggerRef.current?.contains(event.target as Node)) event.preventDefault();
            }}
            onKeyDownCapture={(event) => {
              if (event.key !== "Tab") return;
              // Close and hand focus back to the trigger; the browser's Tab
              // then carries on from there (no preventDefault).
              event.stopPropagation();
              leavingByTab.current = true;
              triggerRef.current?.focus();
              setOpen(false);
            }}
            // Portaled, but React events still bubble to the row it came from.
            onKeyDown={(event) => event.stopPropagation()}
            onClick={(event) => event.stopPropagation()}
          >
            {groups.map((group, index) => (
              <Fragment key={group.section ?? `group-${index}`}>
                {index > 0 ? <Menu.Separator className="df-menu-separator" /> : null}
                {group.section ? (
                  <Menu.Group aria-label={group.section} className="df-menu-group">
                    <Menu.Label className="df-menu-label" aria-hidden="true">{group.section}</Menu.Label>
                    {renderGroupItems(group)}
                  </Menu.Group>
                ) : (
                  <div role="none" className="df-menu-group">{renderGroupItems(group)}</div>
                )}
              </Fragment>
            ))}
          </Menu.Content>
        </Menu.Portal>
      </Menu.Root>
    </span>
  );
}
