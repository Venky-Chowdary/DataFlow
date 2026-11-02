/**
 * Snowflake NUMBER(p,s) pads display zeros after the decimal.
 * 9.083333 and 9.083333000000 are the same value — not a 10^n shift.
 */

export const DEST_SCALE_PADDING_HONESTY =
  "Zeros after the decimal are display scale, not a bigger number. 9.083333 and 9.083333000000 compare equal — the time did not increase. New CREATE uses the observed scale only — extra dest zeros are not invented.";

function significantDecimalText(raw: string): string {
  const text = raw.trim();
  if (!text.includes(".")) return text;
  return text.replace(/(\.\d*?)0+$/, "$1").replace(/\.$/, "");
}

export function fractionalTrailingZerosSameValue(left: string, right: string): boolean {
  return significantDecimalText(left) === significantDecimalText(right);
}
