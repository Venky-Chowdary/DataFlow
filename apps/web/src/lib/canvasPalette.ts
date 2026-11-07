/**
 * Command palette index. Operation names come from the catalog.
 * Column names, cell values, and object names are not indexed.
 */

export interface PaletteAction {
  id: string;
  label: string;
  group: "Transfer" | "Steps" | "Operations" | "Templates";
}

export function paletteActions(input: {
  operations: { op: string; summary: string }[];
  stepCount: number;
  canContinue: boolean;
  canAdd: boolean;
}): PaletteAction[] {
  const actions: PaletteAction[] = [
    { id: "preview", label: "Run preview", group: "Transfer" },
    { id: "schema", label: "Open schema browser", group: "Transfer" },
  ];
  if (input.canContinue) {
    actions.push({ id: "continue", label: "Continue", group: "Transfer" });
  }
  for (let index = 0; index < input.stepCount; index += 1) {
    actions.push({ id: `jump-${index}`, label: `Jump to step ${index + 1}`, group: "Steps" });
  }
  if (input.canAdd) {
    actions.push(
      { id: "template-trim", label: "Template: trim text columns", group: "Templates" },
      { id: "template-drop", label: "Template: drop empty columns", group: "Templates" },
      { id: "template-dates", label: "Template: parse dates", group: "Templates" },
      { id: "template-divert", label: "Template: divert rows", group: "Templates" },
      { id: "template-hash", label: "Template: hash identity key", group: "Templates" },
    );
    for (const operation of input.operations) {
      actions.push({
        id: `add-${operation.op}`,
        label: `Add ${operation.summary}`,
        group: "Operations",
      });
    }
  }
  return actions;
}

export function filterPalette(actions: PaletteAction[], query: string): PaletteAction[] {
  const needle = query.trim().toLowerCase();
  if (!needle) return actions;
  return actions.filter((action) => (
    action.label.toLowerCase().includes(needle) || action.id.toLowerCase().includes(needle)
  ));
}
