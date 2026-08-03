/**
 * Declared carrier width / decimal scale fidelity (Map chips).
 *
 * Mirrors ``apps/api/services/type_system.py`` string_width_would_narrow and
 * DECIMAL(p,s) compare — Fivetran HVR / Airbyte class: mapping honesty depends
 * on attributes (length, precision, scale), not type names alone.
 *
 * SaaS defaults align with ``apps/api/connectors/saas_write_carriers.py`` and
 * HubSpot/Notion/Zendesk writer carriers when Describe has not stamped widths.
 */

/** Bounded VARCHAR/CHAR/NVARCHAR width, or null if unlimited/unknown. */
export function parseStringCarrierWidth(inferred: string | null | undefined): number | null {
  const text = (inferred || "").trim();
  if (!text) return null;
  if (isUnlimitedStringCarrier(text)) return null;
  const m = text.match(
    /(?:var)?(?:national\s+)?(?:character\s+varying|char(?:acter)?\s+varying|nvarchar|varchar|nchar|char|character|string)\s*\(\s*(\d+)\s*\)/i,
  );
  if (!m) return null;
  const width = Number.parseInt(m[1], 10);
  return width > 0 ? width : null;
}

/**
 * TEXT / CLOB / VARCHAR(MAX) / JSON — known unlimited carriers.
 * Bare ``string`` / ``varchar`` stay unknown (no invented narrow), matching
 * ``type_system.is_unlimited_string_carrier`` (LOGICAL_TEXT only).
 */
const MYSQL_TEXT_TIER_RANK: Record<string, number> = {
  tinytext: 1,
  text: 2,
  mediumtext: 3,
  longtext: 4,
};

function mysqlTextTierRank(inferred: string | null | undefined): number | null {
  const token = (inferred || "").trim().toLowerCase().split(/\s+/)[0] || "";
  return MYSQL_TEXT_TIER_RANK[token] ?? null;
}

export function isUnlimitedStringCarrier(inferred: string | null | undefined): boolean {
  const text = (inferred || "").trim();
  if (!text) return false;
  if (/\b(?:varchar|nvarchar|char)\s*\(\s*max\s*\)/i.test(text)) return true;
  const token = text.toLowerCase().split(/\s+/)[0] || "";
  // TINYTEXT is tight (255) — not an unlimited sink (API SSOT).
  if (token === "tinytext") return false;
  if (
    /^(?:text|ntext|clob|nclob|longtext|mediumtext|long\s+varchar|json|jsonb)\b/i.test(
      text,
    )
  ) {
    return true;
  }
  return false;
}

export function isStringFamily(inferred: string | null | undefined): boolean {
  const t = (inferred || "").trim().toLowerCase();
  if (!t) return false;
  if (isUnlimitedStringCarrier(t) || mysqlTextTierRank(t) != null) return true;
  return /\b(?:varchar|nvarchar|char|character|string|text|clob)\b/.test(t);
}

/** True when source string capacity exceeds destination VARCHAR(n). */
export function stringWidthWouldNarrow(sourceType: string, targetType: string): boolean {
  if (!isStringFamily(sourceType) || !isStringFamily(targetType)) return false;
  const srcRank = mysqlTextTierRank(sourceType);
  const tgtRank = mysqlTextTierRank(targetType);
  if (srcRank != null && tgtRank != null && srcRank > tgtRank) return true;
  if (isUnlimitedStringCarrier(targetType)) return false;
  const tgtW = parseStringCarrierWidth(targetType);
  if (tgtW == null) {
    // TINYTEXT capacity = 255 when typmod absent.
    if ((targetType || "").trim().toLowerCase().startsWith("tinytext")) {
      if (isUnlimitedStringCarrier(sourceType)) return true;
      const srcW = parseStringCarrierWidth(sourceType);
      return srcW != null && srcW > 255;
    }
    return false;
  }
  if (isUnlimitedStringCarrier(sourceType)) return true;
  const srcW = parseStringCarrierWidth(sourceType);
  if (srcW == null) return false;
  return srcW > tgtW;
}

export function parseDecimalPrecisionScale(
  inferred: string | null | undefined,
): { precision: number; scale: number } | null {
  const text = (inferred || "").trim();
  if (!text) return null;
  const m = text.match(/\b(?:decimal|numeric|number|bignumeric)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)/i);
  if (!m) return null;
  const precision = Number.parseInt(m[1], 10);
  const scale = m[2] != null ? Number.parseInt(m[2], 10) : 0;
  if (!Number.isFinite(precision) || precision <= 0) return null;
  return { precision, scale: Number.isFinite(scale) ? Math.max(0, scale) : 0 };
}

export function isDecimalFamily(inferred: string | null | undefined): boolean {
  const t = (inferred || "").trim().toLowerCase();
  return /\b(?:decimal|numeric|number|bignumeric)\b/.test(t);
}

/** True when dest DECIMAL scale/precision cannot hold source DECIMAL. */
export function decimalWouldCollapse(sourceType: string, targetType: string): boolean {
  if (!isDecimalFamily(sourceType) || !isDecimalFamily(targetType)) return false;
  const src = parseDecimalPrecisionScale(sourceType);
  const tgt = parseDecimalPrecisionScale(targetType);
  if (!src || !tgt) return false;
  if (src.scale > tgt.scale) return true;
  // Integer digits capacity: precision − scale.
  return src.precision - src.scale > tgt.precision - tgt.scale;
}

/**
 * Documented SaaS write carriers when Map destType lacks (n)/(p,s).
 * Keep in sync with Python ``saas_write_carriers`` / writer describe paths.
 */
const SAAS_FIELD_CARRIERS: Record<string, Record<string, string>> = {
  stripe: {
    email: "VARCHAR(512)",
    receipt_email: "VARCHAR(512)",
    customer_email: "VARCHAR(512)",
    name: "VARCHAR(256)",
    phone: "VARCHAR(20)",
    business_name: "VARCHAR(150)",
    individual_name: "VARCHAR(150)",
    invoice_prefix: "VARCHAR(12)",
    description: "VARCHAR(500)", // subscription-class bound; Map stamps object-specific
  },
  shopify: {
    email: "VARCHAR(255)",
    note: "VARCHAR(5000)",
    first_name: "VARCHAR(255)",
    last_name: "VARCHAR(255)",
    phone: "VARCHAR(50)",
    title: "VARCHAR(255)",
    handle: "VARCHAR(255)",
    code: "VARCHAR(255)",
  },
  hubspot: {
    email: "VARCHAR(65536)",
    phone: "VARCHAR(65536)",
    mobilephone: "VARCHAR(65536)",
  },
  zendesk: {
    subject: "VARCHAR(255)",
    description: "VARCHAR(65535)",
    comment: "VARCHAR(65535)",
    body: "VARCHAR(65535)",
    email: "VARCHAR(255)",
  },
  notion: {
    email: "VARCHAR(200)",
    url: "VARCHAR(2000)",
    phone_number: "VARCHAR(200)",
    phone: "VARCHAR(200)",
  },
  airtable: {
    email: "VARCHAR(254)",
    url: "VARCHAR(2048)",
    phone: "VARCHAR(64)",
    phonenumber: "VARCHAR(64)",
  },
  salesforce: {
    // Describe stamps real lengths; email is a common default when missing.
    email: "VARCHAR(80)",
  },
};

export function saasDefaultCarrier(
  destConnector: string | null | undefined,
  fieldName: string | null | undefined,
): string | null {
  const connector = (destConnector || "").trim().toLowerCase();
  const field = (fieldName || "").trim().toLowerCase();
  if (!connector || !field) return null;
  const catalog = SAAS_FIELD_CARRIERS[connector];
  if (!catalog) return null;
  return catalog[field] || null;
}

/**
 * Effective destination carrier for fidelity checks.
 * Prefer stamped Map destType when it already carries (n)/(p,s); else SaaS catalog.
 */
export function effectiveDestCarrier(
  destType: string | null | undefined,
  destConnector: string | null | undefined,
  fieldName: string | null | undefined,
): string {
  const stamped = (destType || "").trim();
  if (stamped) {
    const hasWidth = parseStringCarrierWidth(stamped) != null || isUnlimitedStringCarrier(stamped);
    const hasDecimal = parseDecimalPrecisionScale(stamped) != null;
    if (hasWidth || hasDecimal || !isStringFamily(stamped)) {
      return stamped;
    }
  }
  return saasDefaultCarrier(destConnector, fieldName) || stamped;
}

export function sampleExceedsStringWidth(
  sample: string | null | undefined,
  destCarrier: string,
): boolean {
  if (sample == null || sample === "") return false;
  if (isUnlimitedStringCarrier(destCarrier)) return false;
  const width = parseStringCarrierWidth(destCarrier);
  if (width == null) return false;
  // Unicode code points — matches default VARCHAR(n CHAR) honesty.
  return [...String(sample)].length > width;
}
