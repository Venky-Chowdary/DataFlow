import { parseLocaleNumber, type NumberLocale } from "./numberLocale";

/**
 * Browser-local transform used by local preflight and local file export.
 * Decimal/number casts use the write-path locale contract — Auto never invents
 * 1234 from 1,234. Boolean is the same strict wire as transform_engine.
 */
export function applyLocalTransform(
  value: unknown,
  transform?: string,
  numberLocale: NumberLocale | string = "",
): unknown {
  if (value == null || value === "") return value;
  const s = String(value);
  switch (transform) {
    case "trim":
      return s.trim();
    case "upper":
      return s.toUpperCase();
    case "lower":
      return s.toLowerCase();
    case "hash_pii": {
      let h = 5381;
      for (let i = 0; i < s.length; i += 1) h = (h * 33) ^ s.charCodeAt(i);
      return `sha256:${(h >>> 0).toString(16).padStart(8, "0")}`;
    }
    case "datetime":
    case "date_iso":
      return s;
    case "decimal":
    case "cast_number": {
      const n = parseLocaleNumber(s, numberLocale);
      return n !== null ? n : value;
    }
    case "boolean":
    case "cast_boolean": {
      const t = s.toLowerCase();
      if (["true", "t", "1"].includes(t)) return true;
      if (["false", "f", "0"].includes(t)) return false;
      return value;
    }
    default:
      return value;
  }
}
