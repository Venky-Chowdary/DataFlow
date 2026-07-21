/** Compact count for tight chrome (100k) with full locale string for tooltips. */
export function formatRouteRowCount(n: number): { short: string; full: string } {
  const full = `${n.toLocaleString()} rows`;
  const abs = Math.abs(n);
  if (abs < 1_000) return { short: full, full };
  const units: [number, string][] = [
    [1_000_000_000, "B"],
    [1_000_000, "M"],
    [1_000, "k"],
  ];
  for (const [div, suffix] of units) {
    if (abs >= div) {
      const scaled = n / div;
      const rounded =
        abs >= div * 10 || Number.isInteger(scaled)
          ? Math.round(scaled).toString()
          : scaled.toFixed(1).replace(/\.0$/, "");
      return { short: `${rounded}${suffix} rows`, full };
    }
  }
  return { short: full, full };
}
