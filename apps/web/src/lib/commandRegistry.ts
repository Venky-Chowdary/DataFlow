/**
 * Actions for what is selected now (UI pass 2, item 7). A page registers the
 * actions that apply to its current selection (Jobs: the selected run); the
 * command menu lists them first. A tiny external store so the menu, mounted
 * in the shell, re-renders when a page's selection changes.
 */
import { useSyncExternalStore } from "react";
import type { IconName } from "../components/icons/iconMap";

export interface CommandAction {
  id: string;
  label: string;
  icon?: IconName;
  /** Extra words cmdk matches on (never shown). */
  keywords?: string[];
  /** Danger actions are styled and still confirm in their own dialog. */
  tone?: "danger";
  run: () => void;
}

export interface SelectionActions {
  /** Section heading, e.g. "Selected job · orders nightly". */
  title: string;
  actions: CommandAction[];
}

let current: SelectionActions | null = null;
let owner: symbol | null = null;
const listeners = new Set<() => void>();

function emit() {
  for (const listener of listeners) listener();
}

/** Set the selection's actions; returns a function that clears them only if still the owner's. */
export function setSelectionActions(next: SelectionActions | null): () => void {
  const token = Symbol("selection");
  owner = token;
  current = next && next.actions.length ? next : null;
  emit();
  return () => {
    if (owner !== token) return;
    owner = null;
    current = null;
    emit();
  };
}

export function getSelectionActions(): SelectionActions | null {
  return current;
}

export function useSelectionActions(): SelectionActions | null {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => current,
    () => null,
  );
}

/** Every keyboard shortcut the product has, for the "?" sheet. */
export interface Shortcut {
  keys: string[];
  action: string;
  where: string;
}

export const SHORTCUTS: Shortcut[] = [
  { keys: ["⌘", "K"], action: "Open the command menu (Ctrl+K on Windows and Linux)", where: "Anywhere" },
  { keys: ["?"], action: "Show keyboard shortcuts", where: "Anywhere outside a text field" },
  { keys: ["Esc"], action: "Close a menu, dialog or popover", where: "Anywhere" },
  { keys: ["↑", "↓"], action: "Move between runs", where: "Jobs list" },
  { keys: ["Home", "End"], action: "First or last run", where: "Jobs list" },
  { keys: ["Enter"], action: "Open the run's details", where: "Jobs list" },
  { keys: ["J", "K"], action: "Move between checks", where: "Validate" },
  { keys: ["Enter"], action: "Run the check's action (Re-run Validate, or Explain)", where: "Validate" },
  { keys: ["E"], action: "Explain the check", where: "Validate" },
];

/** True when a key press belongs to a text field, not to a shortcut. */
export function isTypingTarget(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null;
  if (!el || typeof el.tagName !== "string") return false;
  const tag = el.tagName.toLowerCase();
  return tag === "input" || tag === "textarea" || tag === "select" || Boolean(el.isContentEditable);
}
