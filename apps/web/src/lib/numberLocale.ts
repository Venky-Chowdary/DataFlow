/**
 * Browser-local number parse — same contract as services.transform_engine.
 *
 * Auto: both separators, 3+ thousand groups, and 1–2 digit last groups parse.
 * A lone 1,234 / 1.234 fails closed. $ / USD / £ / GBP imply US for that cell;
 * € / EUR imply EU. Never invent a US thousands rewrite.
 */

export type NumberLocale = "" | "US" | "EU";

const UNICODE_SPACES = /[\u00a0\u2007\u202f\u2009\u2002\u2003\u2000\u2001\u2004\u2005\u2006\u2008\u200a\u205f\u3000]/g;
const NULL_SENTINELS = new Set(["null", "none", "nil", "n/a", "na", "nan", ""]);

function activeLocale(explicit: NumberLocale | string = ""): NumberLocale {
  const loc = String(explicit || "").trim().toUpperCase();
  return loc === "US" || loc === "EU" ? loc : "";
}

export function impliedNumberLocaleFromCurrency(raw: string): NumberLocale {
  const text = String(raw || "").normalize("NFKC");
  const us = /(?<![A-Za-z])(?:USD|GBP|US\$)(?![A-Za-z])|[$£]/i.test(text);
  const eu = /(?<![A-Za-z])EUR(?![A-Za-z])|[€]/.test(text);
  if (us && !eu) return "US";
  if (eu && !us) return "EU";
  return "";
}

function normalizeNumericText(value: string): string {
  let text = value.normalize("NFKC").replace(UNICODE_SPACES, "").trim();
  if (text.endsWith("%")) text = text.slice(0, -1).trim();
  if (text.startsWith("(") && text.endsWith(")")) {
    text = `-${text.slice(1, -1).trim()}`;
  }
  if (text.endsWith("-") && text.slice(0, -1).trim()) {
    text = `-${text.slice(0, -1).trim()}`;
  }
  text = text.replace(/(?:^|\s)(?:USD|EUR|GBP|INR|JPY|CNY|CAD|AUD)(?:\s|$)/gi, " ");
  text = text.replace(/[$€£¥₹]/g, "");
  return text.trim();
}

function digitParts(parts: string[]): boolean {
  if (!parts.length || !parts[0]) return false;
  const head = parts[0].startsWith("-") ? parts[0].slice(1) : parts[0];
  if (!/^\d+$/.test(head)) return false;
  return parts.slice(1).every((part) => /^\d+$/.test(part));
}

function normalizeLocaleSeparators(text: string, numberLocale: NumberLocale = ""): string | null {
  if (NULL_SENTINELS.has(text.toLowerCase())) return null;
  if (!text) return null;

  const locale = activeLocale(numberLocale);
  text = text.replace(/[ \t]/g, "");

  if (text.includes(".") && text.includes(",")) {
    const lastDot = text.lastIndexOf(".");
    const lastComma = text.lastIndexOf(",");
    if (lastDot > lastComma) {
      const candidate = text.replace(/,/g, "");
      if ((candidate.match(/\./g) || []).length <= 1) return candidate;
      return null;
    }
    text = text.replace(/\./g, "");
    const comma = text.lastIndexOf(",");
    const candidate = `${text.slice(0, comma)}.${text.slice(comma + 1)}`;
    if (candidate.includes(",") || (candidate.match(/\./g) || []).length > 1) return null;
    return candidate;
  }

  if (text.includes(",")) {
    const parts = text.split(",");
    if (!digitParts(parts)) return null;
    if (locale === "US") {
      return parts.slice(1).every((part) => part.length === 3) ? parts.join("") : null;
    }
    if (locale === "EU") {
      if (parts.length === 2) return `${parts[0]}.${parts[1]}`;
      if (
        parts.length >= 2
        && parts.slice(1, -1).every((part) => part.length === 3)
        && parts[parts.length - 1].length >= 1
        && parts[parts.length - 1].length <= 2
      ) {
        return `${parts.slice(0, -1).join("")}.${parts[parts.length - 1]}`;
      }
      return null;
    }
    if (
      parts.length >= 3
      && parts[0]
      && !parts[0].replace(/^-/, "").startsWith("0")
      && parts.slice(1).every((part) => part.length === 3)
    ) {
      return parts.join("");
    }
    if (
      parts.length >= 2
      && parts.slice(1, -1).every((part) => part.length === 3)
      && parts[parts.length - 1].length >= 1
      && parts[parts.length - 1].length <= 2
    ) {
      return `${parts.slice(0, -1).join("")}.${parts[parts.length - 1]}`;
    }
    if (parts.length === 2 && parts[1].length > 3) {
      return `${parts[0]}.${parts[1]}`;
    }
    return null;
  }

  if (text.includes(".")) {
    const parts = text.split(".");
    if (!digitParts(parts)) return text;
    if (locale === "US") {
      return parts.length === 2 ? text : null;
    }
    if (locale === "EU") {
      if (parts.slice(1).every((part) => part.length === 3)) return parts.join("");
      if (parts.length === 2 && parts[1].length >= 1 && parts[1].length <= 2) return text;
      if (parts.length === 2 && parts[1].length > 3) return text;
      return null;
    }
    if (
      parts.length >= 3
      && parts[0]
      && !parts[0].replace(/^-/, "").startsWith("0")
      && parts.slice(1).every((part) => part.length === 3)
    ) {
      return parts.join("");
    }
    if (
      parts.length >= 2
      && parts.slice(1, -1).every((part) => part.length === 3)
      && parts[parts.length - 1].length >= 1
      && parts[parts.length - 1].length <= 2
    ) {
      return `${parts.slice(0, -1).join("")}.${parts[parts.length - 1]}`;
    }
    if (parts.length === 2 && parts[1].length >= 1 && parts[1].length <= 2) return text;
    if (parts.length === 2 && parts[1].length > 3) return text;
    return null;
  }
  return text;
}

/** Canonical decimal text the write path would bind, or null when Auto refuses. */
export function parseLocaleDecimalText(raw: unknown, numberLocale: NumberLocale | string = ""): string | null {
  if (raw == null) return null;
  if (typeof raw === "number") return Number.isFinite(raw) ? String(raw) : null;
  if (typeof raw === "bigint") return String(raw);
  const text = String(raw).trim();
  if (!text) return null;
  if (text.startsWith("(") && text.endsWith(")") && text.includes(",") && !text.includes(".")) {
    return null;
  }
  const implied = impliedNumberLocaleFromCurrency(text);
  const normalized = normalizeLocaleSeparators(normalizeNumericText(text), implied || activeLocale(numberLocale));
  if (normalized == null || normalized === "") return null;
  if (!/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(normalized)) return null;
  const n = Number(normalized);
  return Number.isFinite(n) ? normalized : null;
}

/** IEEE number for charts / local dry-run. Null when Auto grouping is ambiguous. */
export function parseLocaleNumber(raw: unknown, numberLocale: NumberLocale | string = ""): number | null {
  const text = parseLocaleDecimalText(raw, numberLocale);
  if (text == null) return null;
  const n = Number(text);
  return Number.isFinite(n) ? n : null;
}
