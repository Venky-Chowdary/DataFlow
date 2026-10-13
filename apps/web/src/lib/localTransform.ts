import { parseLocaleNumber, type NumberLocale } from "./numberLocale";

/**
 * Transforms the browser must not invent a value for. Hash PII is HMAC on the
 * server. IBAN, time, and boolean are parsed by the engine, not by a local
 * approximation the operator could trust.
 */
export const SERVER_PREVIEW_TRANSFORMS = new Set([
  "hash_pii",
  "iban",
  "time",
  "boolean",
  "cast_boolean",
  "url",
  "postal",
  "uuid",
  "trim_id",
]);

export class UntrustedLocalTransform extends Error {
  readonly transform: string;

  constructor(transform: string) {
    super(
      `${transform} is applied by the server. This browser will not invent a digest or a cast.`,
    );
    this.name = "UntrustedLocalTransform";
    this.transform = transform;
  }
}

export function requiresServerPreview(transform?: string): boolean {
  return Boolean(transform && SERVER_PREVIEW_TRANSFORMS.has(transform));
}

/**
 * Browser-local transform used by local preflight and local file export.
 * Decimal/number casts use the write-path locale contract — Auto never invents
 * 1234 from 1,234. hash_pii, iban, time, and boolean refuse here so a preview
 * cannot show a toy digest or a guessed cast.
 */
export function applyLocalTransform(
  value: unknown,
  transform?: string,
  numberLocale: NumberLocale | string = "",
): unknown {
  if (requiresServerPreview(transform)) {
    throw new UntrustedLocalTransform(transform ?? "");
  }
  if (value == null || value === "") return value;
  const s = String(value);
  switch (transform) {
    case "trim":
      return s.trim();
    case "upper":
      return s.toUpperCase();
    case "lower":
      return s.toLowerCase();
    case "datetime":
    case "date_iso":
      return s;
    case "decimal":
    case "cast_number": {
      const n = parseLocaleNumber(s, numberLocale);
      return n !== null ? n : value;
    }
    default:
      return value;
  }
}
