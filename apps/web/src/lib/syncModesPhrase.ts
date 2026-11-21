import { SYNC_MODES } from "./transferConstants";

const WORDS = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve"];

/** How many sync modes Transfer Studio offers, as a word ("seven"). */
export const SYNC_MODE_COUNT_WORD: string = WORDS[SYNC_MODES.length] ?? String(SYNC_MODES.length);

/** "full overwrite, full append, …, and mirror": the modes Transfer Studio offers, in its order. */
export function syncModesList(): string {
  const names = SYNC_MODES.map((m) => (m.label === "CDC" || m.label.startsWith("SCD") ? m.label : m.label.toLowerCase()));
  return `${names.slice(0, -1).join(", ")}, and ${names[names.length - 1]}`;
}
