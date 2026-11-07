/**
 * Browser-local date-order detect — same contract as services.transform_engine.
 *
 * Auto: 31/12/2024 is DMY, 12/31/2024 is MDY. A lone 01/02/2024 fails closed
 * (Jan 2 vs Feb 1). Never invent a calendar from an ambiguous slash date.
 */

export type DateLocale = "" | "DMY" | "MDY";

const SLASH_DATE = /^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2,4})(?:[ T].*)?$/;

function activeLocale(explicit: DateLocale | string = ""): DateLocale {
  const loc = String(explicit || "").trim().toUpperCase();
  return loc === "DMY" || loc === "MDY" ? loc : "";
}

export function isAmbiguousMdyDmy(raw: string, dateLocale: DateLocale | string = ""): boolean {
  if (activeLocale(dateLocale)) return false;
  const text = String(raw || "").trim();
  const m = SLASH_DATE.exec(text);
  if (!m) return false;
  const first = Number(m[1]);
  const second = Number(m[2]);
  if (first > 12 || second > 12 || first === second) return false;
  return true;
}

export function ambiguousDateColumns(
  rows: Array<Record<string, unknown>> | undefined,
  columns: string[] | undefined,
  dateLocale: DateLocale | string = "",
): Array<{ column: string; samples: string[] }> {
  if (activeLocale(dateLocale) || !rows?.length) return [];
  const cols = columns?.length ? columns : Object.keys(rows[0] || {});
  const findings: Array<{ column: string; samples: string[] }> = [];
  for (const col of cols) {
    const samples: string[] = [];
    for (const row of rows) {
      const raw = String(row?.[col] ?? "").trim();
      if (!raw || !isAmbiguousMdyDmy(raw, dateLocale)) continue;
      samples.push(raw);
    }
    if (samples.length) findings.push({ column: col, samples: samples.slice(0, 5) });
  }
  return findings;
}
