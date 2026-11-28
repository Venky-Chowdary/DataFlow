/**
 * Counted nouns without "(s)": plural(1, "table") → "1 table",
 * plural(3, "table") → "3 tables", plural(0, "table") → "0 tables".
 * Irregular words pass their plural: plural(2, "entry", "entries").
 * Counts use en-US grouping: plural(1200000, "row") → "1,200,000 rows".
 */
export function plural(count: number, singular: string, pluralForm?: string): string {
  return `${formatCount(count)} ${pluralWord(count, singular, pluralForm)}`;
}

/** Just the word for a count: pluralWord(1, "row") → "row", pluralWord(2, "row") → "rows". */
export function pluralWord(count: number, singular: string, pluralForm?: string): string {
  return Math.abs(count) === 1 ? singular : pluralForm ?? defaultPlural(singular);
}

function formatCount(count: number): string {
  return Number.isFinite(count) ? count.toLocaleString("en-US") : String(count);
}

function defaultPlural(word: string): string {
  if (/[^aeiou]y$/i.test(word)) return `${word.slice(0, -1)}ies`;
  if (/(s|x|z|ch|sh)$/i.test(word)) return `${word}es`;
  return `${word}s`;
}
