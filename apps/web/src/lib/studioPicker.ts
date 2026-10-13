/**
 * Closed-list picker helpers. The menu is a portaled listbox; this module
 * only ranks and groups options so the UI cannot invent a value.
 */

export interface StudioPickerOption {
  value: string;
  label: string;
  hint?: string;
  group?: string;
  meta?: string;
}

export function filterStudioOptions(
  options: StudioPickerOption[],
  query: string,
): StudioPickerOption[] {
  const q = query.trim().toLowerCase();
  if (!q) return options;
  return options
    .map((opt) => {
      const value = opt.value.toLowerCase();
      const label = opt.label.toLowerCase();
      let score = 0;
      if (value === q || label === q) score = 3;
      else if (value.startsWith(q) || label.startsWith(q)) score = 2;
      else {
        const hay = [value, label, opt.hint, opt.group, opt.meta]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();
        if (hay.includes(q)) score = 1;
      }
      return { opt, score };
    })
    .filter((row) => row.score > 0)
    .sort((a, b) => b.score - a.score || a.opt.label.localeCompare(b.opt.label))
    .map((row) => row.opt);
}

export function groupStudioOptions(
  options: StudioPickerOption[],
): Array<{ group: string; options: StudioPickerOption[] }> {
  const order: string[] = [];
  const buckets = new Map<string, StudioPickerOption[]>();
  for (const opt of options) {
    const group = opt.group || "";
    if (!buckets.has(group)) {
      buckets.set(group, []);
      order.push(group);
    }
    buckets.get(group)!.push(opt);
  }
  return order.map((group) => ({ group, options: buckets.get(group) ?? [] }));
}

export type StudioPickerKey =
  | { type: "close" }
  | { type: "open" }
  | { type: "move"; index: number }
  | { type: "choose"; index: number }
  | { type: "ignore" };

/**
 * Keyboard contract shared by the single and multi pickers.
 * Space chooses only when the caller says the target is not a text field.
 */
export function studioPickerKey(input: {
  key: string;
  open: boolean;
  activeIndex: number;
  optionCount: number;
  spaceChooses: boolean;
}): StudioPickerKey {
  const { key, open, activeIndex, optionCount, spaceChooses } = input;
  const last = Math.max(optionCount - 1, 0);
  if (key === "Escape") return { type: "close" };
  if (key === "ArrowDown") {
    if (!open) return { type: "open" };
    return { type: "move", index: Math.min(activeIndex + 1, last) };
  }
  if (key === "ArrowUp") return { type: "move", index: Math.max(activeIndex - 1, 0) };
  const choose = key === "Enter" || (key === " " && spaceChooses);
  if (choose && open && activeIndex >= 0 && activeIndex < optionCount) {
    return { type: "choose", index: activeIndex };
  }
  return { type: "ignore" };
}

export function studioPickerSelection(
  options: StudioPickerOption[],
  value: string,
): StudioPickerOption | undefined {
  const want = value.trim().toLowerCase();
  if (!want) return undefined;
  return options.find((opt) => opt.value.toLowerCase() === want);
}
