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
    /(?:var)?(?:national\s+)?(?:character\s+varying|char(?:acter)?\s+varying|nvarchar2|varchar2|nvarchar|varchar|nchar|bpchar|char|character|string)\s*\(\s*(\d+)\s*(?:byte|char)?\s*\)/i,
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
  // Oracle VARCHAR2/NVARCHAR2 and NCHAR/BPCHAR are string carriers too — an
  // unclassified carrier is a carrier every fidelity rule below skips.
  return /\b(?:n?varchar2|n?varchar|nchar|bpchar|char|character|string|text|clob)\b/.test(t);
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
  if (!src) return false;
  // Proven (p,s) → bare DECIMAL invents platform default — Accept risk.
  if (!tgt) return true;
  if (src.scale > tgt.scale) return true;
  // Integer digits capacity: precision − scale.
  return src.precision - src.scale > tgt.precision - tgt.scale;
}

function isTzAwareTemporal(inferred: string | null | undefined): boolean {
  const t = (inferred || "").trim().toLowerCase();
  return /\b(timestamptz|timestamp with time zone|timestamp_tz|timestamp_ltz|timetz|time with time zone|datetimeoffset)\b/.test(
    t,
  );
}

function isNtzTemporal(inferred: string | null | undefined): boolean {
  const t = (inferred || "").trim().toLowerCase();
  if (isTzAwareTemporal(t)) return false;
  return /\b(timestamp_ntz|datetime|timestamp without time zone|timestamp|time)\b/.test(t);
}

function isDocumentCarrier(inferred: string | null | undefined): boolean {
  const t = (inferred || "").trim().toLowerCase();
  return /\b(jsonb?|variant|super|object)\b/.test(t);
}

function isOpenStringCarrier(inferred: string | null | undefined): boolean {
  const t = (inferred || "").trim().toLowerCase();
  return /\b(string|text|varchar|nvarchar|char|nchar|clob)\b/.test(t) && !isDocumentCarrier(t);
}

/** Approximate signed bit width — mirrors API integer_bit_width for Map chips. */
function integerBitWidth(inferred: string | null | undefined): number | null {
  const raw = (inferred || "").trim();
  if (!raw) return null;
  // ClickHouse Int8/UInt8 — must not collide with PG INT8≡BIGINT.
  const ch = raw.match(/^(U?Int)(8|16|32|64)\b/);
  if (ch) {
    const bits = Number(ch[2]);
    return ch[1].startsWith("U") ? bits + 1 : bits;
  }
  const u = raw.toUpperCase();
  if (/\b(BIGINT|INT64|INT8|LONG|UINT64)\b/.test(u) || u.includes("BIGSERIAL")) return 64;
  if (u.includes("MEDIUMINT")) return 24;
  if (/\b(SMALLINT|INT16|INT2|UINT16|SHORT)\b/.test(u) || u.includes("SMALLSERIAL")) return 16;
  if (/\b(TINYINT|INT1|UINT8)\b/.test(u) || u.includes("TINYSERIAL")) return 8;
  if (/\b(INTEGER|INT32|INT4|UINT32)\b/.test(u) || (/\bSERIAL\b/.test(u) && !u.includes("BIG"))) return 32;
  if (/\bINT\b/.test(u)) return 32;
  return null;
}

function integerWidthWouldNarrow(sourceType: string, targetType: string): boolean {
  const srcW = integerBitWidth(sourceType);
  const tgtW = integerBitWidth(targetType);
  if (srcW == null || tgtW == null) return false;
  return srcW > tgtW;
}

/**
 * The logical domain a declared carrier belongs to, for the domain-crossing
 * rule below. Mirrors the domains ``type_system.normalize_logical_type``
 * distinguishes; anything it cannot place (specialty, interval, spatial,
 * vector, ObjectId) returns null and is left to the specific rules above.
 */
export type CarrierDomain =
  | "string"
  | "text"
  | "json"
  | "array"
  | "struct"
  | "map"
  | "integer"
  | "decimal"
  | "float"
  | "boolean"
  | "date"
  | "datetime"
  | "time"
  | "binary";

export function carrierLogicalDomain(inferred: string | null | undefined): CarrierDomain | null {
  const raw = (inferred || "").trim();
  if (!raw) return null;
  const t = raw.toLowerCase();
  // Document/container carriers first: JSON also matches the unlimited-string test.
  if (/^(?:struct|record|row)\b/.test(t) || /^(?:struct|record|row)\s*[<(]/.test(t)) return "struct";
  if (/^map\b/.test(t)) return "map";
  if (/^(?:array|list|set)\b/.test(t) || /\[\s*\]\s*$/.test(t)) return "array";
  if (/\b(?:jsonb?|variant|super)\b/.test(t)) return "json";
  if (/\b(?:interval|vector|geography|geometry|geojson|sdo_geometry|objectid|uuid|guid|uniqueidentifier|inet|cidr|macaddr|xml|xmltype|hstore|ltree|tsvector|tsquery|jsonpath|hierarchyid|sql_variant|rowversion|enum|user-defined|user_defined)\b/.test(t)) {
    return null;
  }
  if (/\b(?:binary|varbinary|blob|bytea|bytes|raw|bindata|image)\b/.test(t)) return "binary";
  if (/\b(?:boolean|bool)\b/.test(t)) return "boolean";
  if (/\b(?:timestamptz|timestamp|datetime|datetime2|smalldatetime|datetimeoffset)\b/.test(t)) {
    return "datetime";
  }
  if (/\b(?:timetz|time)\b/.test(t)) return "time";
  if (/\bdate\b/.test(t)) return "date";
  if (isDecimalFamily(t)) return "decimal";
  if (/\b(?:float|double|real|float4|float8|float16|float32|float64|half|halffloat|binary_float|binary_double)\b/.test(t)) {
    return "float";
  }
  if (integerBitWidth(raw) != null) return "integer";
  if (isUnlimitedStringCarrier(t)) return "text";
  if (isStringFamily(t)) return "string";
  return null;
}

/**
 * Domain crossings the engine treats as preserving — the allow-list in
 * ``type_system.is_lossy_coercion``. Every other crossing is a coercion the
 * engine declares lossy, so Map must ask for a Risk Contract rather than
 * offering a plain Approve that Validate then refuses.
 */
const SAFE_DOMAIN_COERCIONS: ReadonlySet<string> = new Set([
  "string>text",
  "text>string",
  "integer>decimal",
  "integer>string",
  "integer>text",
  "integer>json",
  "decimal>string",
  "decimal>text",
  "decimal>json",
  "float>string",
  "float>text",
  "float>json",
  "boolean>string",
  "boolean>text",
  "boolean>json",
  "boolean>integer",
  "boolean>decimal",
  "boolean>float",
  "date>datetime",
  "date>string",
  "date>text",
  "date>json",
  "datetime>string",
  "datetime>text",
  "datetime>json",
  "time>string",
  "time>text",
  "time>json",
]);

/** N-prefixed / NATIONAL CHARACTER carrier — its own charset, not the table default. */
function isNationalCharsetCarrier(inferred: string | null | undefined): boolean {
  return /\b(?:nchar|nvarchar|nvarchar2|nclob|ntext|national\s+char(?:acter)?)\b/i.test(
    (inferred || "").trim(),
  );
}

/** Fixed-width CHAR(n)/NCHAR(n) — blank-padded storage, not an open string. */
function isBlankPaddedCharCarrier(inferred: string | null | undefined): boolean {
  const t = (inferred || "").trim().toLowerCase();
  if (!t) return false;
  return /^(?:n?char|character|nchar\s+varying|bpchar)\s*\(\s*\d+\s*\)$/.test(t)
    || /^(?:n?char|character|bpchar)$/.test(t);
}

/** Declared-width string carrier: VARCHAR(255), STRING(50), VARCHAR2(30), CHAR(10). */
function isBoundedStringCarrier(inferred: string | null | undefined): boolean {
  return isStringFamily(inferred) && parseStringCarrierWidth(inferred) != null;
}

/**
 * The engine's document specialty carriers: a scalar written into JSONB /
 * VARIANT / SUPER acquires document-validation polarity the source never had
 * (``type_system.specialty_domain_would_invent``).
 */
function isDocumentSpecialtyCarrier(inferred: string | null | undefined): boolean {
  const t = (inferred || "").trim().toLowerCase();
  return /\b(?:jsonb|variant|super|bson)\b/.test(t);
}

/**
 * Crossings the engine refuses that a same/adjacent-domain read would call
 * preserving. Each mirrors a named ``is_lossy_coercion`` rule, so an Approve
 * offered here is an Approve Validate then refuses.
 */
function carrierShapeCoercionRisk(
  sourceType: string | null | undefined,
  targetType: string | null | undefined,
): boolean {
  const src = carrierLogicalDomain(sourceType);
  const tgt = carrierLogicalDomain(targetType);

  // Blank padding is storage semantics: entering or leaving CHAR(n) rewrites it.
  const srcChar = isBlankPaddedCharCarrier(sourceType);
  const tgtChar = isBlankPaddedCharCarrier(targetType);
  if (srcChar !== tgtChar) return true;

  // Non-string value into a declared-width string sink truncates at the width
  // its rendered form exceeds — the width is not a property of the source.
  if (
    src != null
    && src !== "string"
    && src !== "text"
    && isBoundedStringCarrier(targetType)
  ) {
    return true;
  }

  // A scaled decimal rendered as text drops the numeric domain and its scale.
  if (src === "decimal" && (tgt === "string" || tgt === "text")) {
    const ps = parseDecimalPrecisionScale(sourceType);
    if (ps == null || ps.scale > 0) return true;
  }

  // A rendered TIMESTAMPTZ drops the offset it carried; its JSON wire keeps it.
  if (isTzAwareTemporal(sourceType) && (tgt === "string" || tgt === "text")) return true;

  // Bytes have no text encoding — rendering them invents one.
  if (src === "binary" && (tgt === "string" || tgt === "text" || tgt === "json")) {
    return true;
  }

  // Scalar → native document carrier invents document validation polarity. A
  // temporal instant keeps its own JSON wire (``document_instant``), so the
  // engine preserves it and Map must not demand a contract for it.
  if (
    src != null
    && src !== "json"
    && src !== "date"
    && src !== "datetime"
    && src !== "time"
    && isDocumentSpecialtyCarrier(targetType)
  ) {
    return true;
  }

  return false;
}

/**
 * True when the declared carriers sit in different logical domains and the
 * crossing is not one the engine preserves — TEXT → DECIMAL(38,15) parses,
 * DECIMAL(12,2) → DATE reinterprets, and both can reject cells at write time.
 */
export function crossDomainCoercionRisk(
  sourceType: string | null | undefined,
  targetType: string | null | undefined,
): boolean {
  const src = carrierLogicalDomain(sourceType);
  const tgt = carrierLogicalDomain(targetType);
  if (src == null || tgt == null) return false;
  if (carrierShapeCoercionRisk(sourceType, targetType)) return true;
  if (src === tgt) return false;
  return !SAFE_DOMAIN_COERCIONS.has(`${src}>${tgt}`);
}

/**
 * Client-side Map fidelity risk when engine stamp is cleared (dest-type change).
 * Aligns with API is_lossy / timezone / document-domain honesty — never invent Approve.
 */
export function declaredCarrierFidelityRisk(
  sourceType: string | null | undefined,
  targetType: string | null | undefined,
): boolean {
  const src = (sourceType || "").trim();
  const tgt = (targetType || "").trim();
  if (!src || !tgt) return false;
  if (stringWidthWouldNarrow(src, tgt)) return true;
  if (decimalWouldCollapse(src, tgt)) return true;
  // Bare DECIMAL → DECIMAL(p,s) invents capacity (API SSOT).
  if (
    isDecimalFamily(src)
    && isDecimalFamily(tgt)
    && parseDecimalPrecisionScale(src) == null
    && parseDecimalPrecisionScale(tgt) != null
  ) {
    return true;
  }
  // DECFLOAT → fixed DECIMAL/FLOAT invent.
  if (/\bdecfloat\b/i.test(src) && !/\bdecfloat\b/i.test(tgt)) return true;
  // Bare NUMBER/DECIMAL → BIGNUMERIC invents (76,38) class.
  if (
    isDecimalFamily(src)
    && /\bbignumeric\b|\bbigdecimal\b/i.test(tgt)
    && !/\bbignumeric\b|\bbigdecimal\b/i.test(src)
  ) {
    return true;
  }
  // SMALLDATETIME minute accuracy → second-level TIMESTAMP invent.
  if (/\bsmalldatetime\b/i.test(src) && !/\bsmalldatetime\b/i.test(tgt)) return true;
  // Float ↔ fixed-point invent/drop IEEE polarity.
  const srcFloat = /\b(float|double|real|float64|float32|float4|float8|half|halffloat|float16|binary_float|binary_double)\b/i.test(src);
  const tgtFloat = /\b(float|double|real|float64|float32|float4|float8|half|halffloat|float16|binary_float|binary_double)\b/i.test(tgt);
  const srcDec = isDecimalFamily(src);
  const tgtDec = isDecimalFamily(tgt);
  if ((srcFloat && tgtDec) || (srcDec && tgtFloat)) return true;
  // IEEE mantissa narrow (DOUBLE→HALF / REAL→FLOAT16 / BINARY_DOUBLE→BINARY_FLOAT).
  const srcHalf = /\b(half|halffloat|float16)\b/i.test(src);
  const tgtHalf = /\b(half|halffloat|float16)\b/i.test(tgt);
  const srcDouble = /\b(double|float64|float8|binary_double)\b/i.test(src);
  const tgtDouble = /\b(double|float64|float8|binary_double)\b/i.test(tgt);
  const srcSingle = /\b(real|float32|float4|binary_float)\b/i.test(src);
  const tgtSingle = /\b(real|float32|float4|binary_float)\b/i.test(tgt);
  if ((srcDouble && (tgtHalf || tgtSingle)) || (srcSingle && tgtHalf)) {
    return true;
  }
  if (srcDouble && !tgtDouble && tgtFloat && !srcHalf) return true;
  // Bare DATETIME2 → DATETIME (SQL Server default precision 7 → ~3.33ms).
  if (/\bdatetime2\b/i.test(src) && /\bdatetime\b/i.test(tgt) && !/datetime2/i.test(tgt)) return true;
  // Oracle LONG text LOB → integer invent.
  if (/^long$/i.test(src.trim()) && /\b(bigint|integer|int64|int8|number|decimal|numeric)\b/i.test(tgt)) {
    return true;
  }
  // Specialty → open string (INET/XML/HSTORE/USER-DEFINED/IPv4/…).
  if (
    /\b(inet|cidr|macaddr|xmltype|xml|hstore|ltree|tsvector|tsquery|jsonpath|objectid|anydata|hllsketch|rowversion|sql_variant|hierarchyid|user-defined|user_defined|ipv4|ipv6|enum8|enum16|nothing|dynamic|aggregatefunction|simpleaggregatefunction)\b/i.test(src)
    && isOpenStringCarrier(tgt)
  ) {
    return true;
  }
  // Unsigned → signed integer invent (UInt8→SMALLINT).
  if (
    (/\bunsigned\b/i.test(src) || /\buint\d*\b/i.test(src) || /^UInt(8|16|32|64)\b/.test(src.trim()))
    && /\b(smallint|integer|int|bigint|int\d+)\b/i.test(tgt)
    && !/\bunsigned\b/i.test(tgt)
    && !/\buint\d*\b/i.test(tgt)
    && !/^UInt(8|16|32|64)\b/.test(tgt.trim())
  ) {
    return true;
  }
  // National charset collapse / invent (NCHAR↔CHAR, NATIONAL CHARACTER). The
  // target side reads through isStringFamily so a MySQL text tier
  // (LONGTEXT/MEDIUMTEXT) counts — the engine calls that crossing lossy.
  if (isNationalCharsetCarrier(src) && isStringFamily(tgt) && !isNationalCharsetCarrier(tgt)) {
    return true;
  }
  if (isStringFamily(src) && !isNationalCharsetCarrier(src) && isNationalCharsetCarrier(tgt)) {
    return true;
  }
  if (isDocumentCarrier(src) && isOpenStringCarrier(tgt)) return true;
  if (isOpenStringCarrier(src) && isDocumentCarrier(tgt)) return true;
  if (isTzAwareTemporal(src) && isNtzTemporal(tgt)) return true;
  if (isNtzTemporal(src) && isTzAwareTemporal(tgt)) return true;
  // Offset-aware → open string drops the TZ contract (API SSOT).
  if (isTzAwareTemporal(src) && isOpenStringCarrier(tgt)) return true;
  if (/\b(timetz|time\s+with\s+time\s+zone)\b/i.test(src) && isOpenStringCarrier(tgt)) {
    return true;
  }
  if (/\bdate\b/.test(src.toLowerCase()) && isTzAwareTemporal(tgt)) return true;
  if (integerWidthWouldNarrow(src, tgt)) return true;
  // MONEY / SMALLMONEY domain collapse.
  if (
    /\b(money|smallmoney|currency)\b/i.test(src)
    && !/\b(money|smallmoney)\b/i.test(tgt)
  ) {
    return true;
  }
  // INTERVAL family invent/collapse (bare↔YM↔DS).
  const intervalFamily = (t: string): string | null => {
    const u = t.toUpperCase();
    if (!/\bINTERVAL\b/.test(u)) return null;
    if (/YEAR|MONTH/.test(u) && !/DAY|SECOND|HOUR|MINUTE/.test(u.replace(/YEAR|MONTH/g, ""))) {
      return "ym";
    }
    if (/DAY|SECOND|HOUR|MINUTE/.test(u)) return "ds";
    return "bare";
  };
  const sif = intervalFamily(src);
  const tif = intervalFamily(tgt);
  if (sif != null && tif != null && sif !== tif) return true;
  if (sif != null && isOpenStringCarrier(tgt)) return true;
  // GEOGRAPHY ↔ GEOMETRY ↔ SDO polarity.
  const geoPol = (t: string): string | null => {
    if (/\b(sdo_geometry|st_geometry)\b/i.test(t)) return "sdo";
    if (/\bgeography\b/i.test(t)) return "geography";
    if (/\bgeometry\b/i.test(t)) return "geometry";
    return null;
  };
  const sg = geoPol(src);
  const tg = geoPol(tgt);
  if (sg != null && tg != null && sg !== tg) return true;
  // LONG RAW locator collapse.
  if (/\blong\s+raw\b/i.test(src) && !/\blong\s+raw\b/i.test(tgt)) return true;
  if (
    /\b(timestamp|datetime|timestamptz)\b/.test(src.toLowerCase())
    && /\bdate\b/.test(tgt.toLowerCase())
    && !/time/.test(tgt.toLowerCase().replace("timestamp", "").replace("datetime", ""))
  ) {
    return true;
  }
  // Bare ARRAY/LIST/MAP ↔ typed element invent/drop.
  const arrayTyped = (t: string): boolean | null => {
    if (/^(array|list)$/i.test(t)) return false;
    if (/^(?:array|list)\s*[<(]/i.test(t) || /\[\s*\]\s*$/.test(t)) return true;
    return null;
  };
  const sa = arrayTyped(src);
  const ta = arrayTyped(tgt);
  if (sa != null && ta != null && sa !== ta) return true;
  const mapTyped = (t: string): boolean | null => {
    if (/^map$/i.test(t)) return false;
    if (/^map\s*[<(]/i.test(t)) return true;
    return null;
  };
  const sm = mapTyped(src);
  const tm = mapTyped(tgt);
  if (sm != null && tm != null && sm !== tm) return true;
  if (crossDomainCoercionRisk(src, tgt)) return true;
  return false;
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
