/**
 * Node registry for the recipe canvas.
 *
 * The shape catalog is the list of operations the row engine will run.
 * Set-engine nodes arrive on the same payload (`canvas_nodes`, `set_operations`,
 * `set_engine.set_hash`) and stay disabled until that catalog marks them
 * executable. Ports, including field ports, live on the spec so a later
 * executable join does not need a new canvas.
 */

import {
  type ShapeCatalog,
  type ShapeOperation,
} from "./shape";

export type NodeStripe = "values" | "columns" | "rows" | "shape";
export type NodeStatus = "valid" | "warning" | "error" | "running";
export type PortRole = "in" | "out";
export type PortKind = "row" | "field";
export type NodePlacement = "palette" | "wizard" | "plan";

export interface CanvasPort {
  id: string;
  role: PortRole;
  kind: PortKind;
  label: string;
  /** False keeps the port in the model and off the card. */
  visible: boolean;
}

export interface CanvasNodeWire {
  op: string;
  summary: string;
  family: string;
  executable: boolean;
  placement: NodePlacement;
  inputs: CanvasPort[];
  outputs: CanvasPort[];
  field_ports: CanvasPort[];
}

export interface NodeSpec {
  op: string;
  summary: string;
  short: string;
  family: string;
  familyLabel: string;
  stripe: NodeStripe;
  icon: string;
  executable: boolean;
  placement: NodePlacement;
  reason: string;
  ports: CanvasPort[];
  operation: ShapeOperation | null;
}

const FAMILY_LABEL: Record<string, string> = {
  nested: "Nested JSON",
  rows: "Row count",
  structural: "Columns",
  cleanse: "Values",
  set: "Set",
  object: "Objects",
  sql: "SQL",
};

const SHORT: Record<string, string> = {
  drop_column: "Drop",
  keep_columns: "Keep",
  rename_column: "Rename",
  cast_column: "Cast",
  constant_column: "Constant",
  derive_column: "Expression",
  split_column: "Split",
  concat_columns: "Concat",
  hash_identity: "Hash",
  trim: "Trim",
  collapse_whitespace: "Collapse",
  case: "Case",
  strip_characters: "Strip",
  pad: "Pad",
  replace: "Replace",
  default_if_null: "Default",
  null_if: "Null if",
  round_number: "Round",
  truncate_number: "Truncate",
  absolute: "Absolute",
  clamp: "Clamp",
  parse_number: "Number",
  parse_date: "Date",
  parse_boolean: "Boolean",
  normalize_unicode: "Unicode",
  set_value: "Map",
  unnest_json: "Unnest",
  flatten_json: "Flatten",
  filter_rows: "Filter",
  divert_rows: "Divert",
  source: "Source",
  target: "Target",
  join: "Join",
  lookup: "Lookup",
  aggregate: "Aggregate",
  sort: "Sort",
  rank: "Rank",
  window: "Window",
  dedupe: "Dedupe",
  router: "Router",
  union: "Union",
  normalizer: "Normalizer",
  mapplet: "Mapplet",
  pivot: "Pivot",
  unpivot: "Unpivot",
  mask: "Mask",
  sql: "SQL",
};

/** One stroke icon per operation. Unknown ops fall back to the family glyph. */
const ICONS: Record<string, string> = {
  drop_column: "trash",
  keep_columns: "layers",
  rename_column: "rename",
  cast_column: "cast",
  constant_column: "plus",
  derive_column: "edit",
  split_column: "split",
  concat_columns: "concat",
  hash_identity: "hash",
  trim: "trim",
  collapse_whitespace: "collapse",
  case: "case",
  strip_characters: "strip",
  pad: "pad",
  replace: "replace",
  default_if_null: "default",
  null_if: "null",
  round_number: "round",
  truncate_number: "truncate",
  absolute: "absolute",
  clamp: "clamp",
  parse_number: "number",
  parse_date: "calendar",
  parse_boolean: "check",
  normalize_unicode: "normalizer",
  set_value: "edit",
  unnest_json: "unnest",
  flatten_json: "flatten",
  filter_rows: "filter",
  divert_rows: "git-branch",
  source: "database",
  target: "send",
  join: "join",
  lookup: "search",
  aggregate: "aggregate",
  sort: "sort",
  rank: "rank",
  window: "window",
  dedupe: "dedupe",
  router: "router",
  union: "union",
  normalizer: "normalizer",
  mapplet: "mapplet",
  pivot: "pivot",
  unpivot: "unpivot",
  mask: "mask",
  sql: "code",
};

const NOT_EXECUTABLE = "The catalog has not marked this operation executable. It stays on the canvas model and is not written into the recipe.";

/** Used when an older catalog payload has no canvas_nodes block. */
export const FALLBACK_CANVAS_NODES: CanvasNodeWire[] = [
  node("source", "Source object. The wizard owns this step.", "object", "wizard", [], [row("out", "out", "Rows")]),
  node("target", "Target object. The wizard owns this step.", "object", "wizard", [row("in", "in", "Rows")], []),
  node("join", "Combine two inputs on a key", "set", "palette", [row("left", "in", "Left"), row("right", "in", "Right")], [row("out", "out", "Joined")], [field("left-key", "in", "Left key"), field("right-key", "in", "Right key")]),
  node("lookup", "Attach columns from a reference input", "set", "palette", [row("main", "in", "Main"), row("reference", "in", "Reference")], [row("out", "out", "Looked up")], [field("key", "in", "Lookup key")]),
  node("aggregate", "Group rows and compute measures", "set", "palette", [row("in", "in", "Rows")], [row("out", "out", "Groups")], [field("group", "in", "Group by"), field("measure", "out", "Measure")]),
  node("sort", "Order rows by one or more columns", "set", "palette", [row("in", "in", "Rows")], [row("out", "out", "Ordered")]),
  node("rank", "Number rows inside a partition", "set", "palette", [row("in", "in", "Rows")], [row("out", "out", "Ranked")]),
  node("window", "Compute a value over a frame of rows", "set", "palette", [row("in", "in", "Rows")], [row("out", "out", "Windowed")], [field("frame", "in", "Frame")]),
  node("dedupe", "Keep one row per key", "set", "palette", [row("in", "in", "Rows")], [row("out", "out", "Unique"), row("duplicates", "out", "Duplicates")]),
  node("router", "Send each row to one or more outputs", "set", "palette", [row("in", "in", "Rows")], [row("out-1", "out", "Group 1"), row("out-2", "out", "Group 2"), row("default", "out", "Default")]),
  node("union", "Stack compatible inputs", "set", "palette", [row("in-1", "in", "Input 1"), row("in-2", "in", "Input 2")], [row("out", "out", "Union")]),
  node("normalizer", "Turn repeating columns into rows", "set", "palette", [row("in", "in", "Wide")], [row("out", "out", "Normalized")]),
  node("mapplet", "A reusable group of steps", "set", "palette", [row("in", "in", "In")], [row("out", "out", "Out")]),
  node("pivot", "Turn row values into columns", "set", "palette", [row("in", "in", "Rows")], [row("out", "out", "Wide")]),
  node("unpivot", "Turn columns into row values", "set", "palette", [row("in", "in", "Wide")], [row("out", "out", "Tall")]),
  node("mask", "Hide part of a value", "cleanse", "palette", [row("in", "in", "Rows")], [row("out", "out", "Masked")], [field("column", "in", "Column")]),
  node("sql", "Warehouse SQL. It runs after the load, not on this canvas.", "sql", "plan", [], []),
];

function row(id: string, role: PortRole, label: string): CanvasPort {
  return { id, role, kind: "row", label, visible: true };
}

function field(id: string, role: PortRole, label: string): CanvasPort {
  return { id, role, kind: "field", label, visible: false };
}

function node(
  op: string,
  summary: string,
  family: string,
  placement: NodePlacement,
  inputs: CanvasPort[],
  outputs: CanvasPort[],
  fieldPorts: CanvasPort[] = [],
): CanvasNodeWire {
  return {
    op,
    summary,
    family,
    executable: false,
    placement,
    inputs,
    outputs,
    field_ports: fieldPorts,
  };
}

export function stripeFor(family: string): NodeStripe {
  if (family === "structural") return "columns";
  if (family === "rows") return "rows";
  if (family === "nested" || family === "set" || family === "sql" || family === "object") return "shape";
  return "values";
}

export function iconFor(op: string, family = ""): string {
  if (ICONS[op]) return ICONS[op];
  if (family === "structural") return "layers";
  if (family === "rows") return "activity";
  if (family === "nested" || family === "sql") return "code";
  return "edit";
}

export function shortName(op: string, summary = ""): string {
  if (SHORT[op]) return SHORT[op];
  const word = summary.trim().split(/\s+/)[0];
  return word || op;
}

/** Row steps carry one visible row port each way, plus field ports that stay off the card. */
export function streamPorts(): CanvasPort[] {
  return [
    row("in", "in", "Input"),
    row("out", "out", "Output"),
    field("fields-in", "in", "Fields"),
    field("fields-out", "out", "Fields"),
  ];
}

/**
 * Divert keeps the passing rows on Output's sibling "Kept" and sends matches
 * out a second labeled port. The chain still continues from Kept.
 */
export function stepPorts(op: string): CanvasPort[] {
  if (op === "divert_rows") {
    return [
      row("in", "in", "Input"),
      row("out", "out", "Kept"),
      row("diverted", "out", "Diverted"),
      field("fields-in", "in", "Fields"),
      field("fields-out", "out", "Fields"),
    ];
  }
  return streamPorts();
}

export function portsOf(wire: CanvasNodeWire): CanvasPort[] {
  return [...wire.inputs, ...wire.outputs, ...wire.field_ports];
}

export function nodeStatus(input: {
  invalid?: boolean;
  disabled?: boolean;
  dimmed?: boolean;
  running?: boolean;
}): NodeStatus {
  if (input.running) return "running";
  if (input.invalid) return "error";
  if (input.disabled || input.dimmed) return "warning";
  return "valid";
}

export function statusLabel(status: NodeStatus): string {
  if (status === "valid") return "Valid";
  if (status === "warning") return "Warning";
  if (status === "error") return "Invalid";
  return "Running";
}

export function categoryLabel(family: string): string {
  return FAMILY_LABEL[family] || "Values";
}

function familyLabel(family: string): string {
  return categoryLabel(family);
}

function specFromOperation(operation: ShapeOperation): NodeSpec {
  const family = operation.family || "cleanse";
  return {
    op: operation.op,
    summary: operation.summary,
    short: shortName(operation.op, operation.summary),
    family,
    familyLabel: familyLabel(family),
    stripe: stripeFor(family),
    icon: iconFor(operation.op, family),
    executable: true,
    placement: "palette",
    reason: "",
    ports: streamPorts(),
    operation,
  };
}

function specFromWire(wire: CanvasNodeWire, executableSet: Set<string>, reason: string): NodeSpec {
  const executable = wire.executable || executableSet.has(wire.op);
  const family = wire.family;
  return {
    op: wire.op,
    summary: wire.summary,
    short: shortName(wire.op, wire.summary),
    family,
    familyLabel: familyLabel(wire.family),
    stripe: stripeFor(wire.family),
    icon: iconFor(wire.op, wire.family),
    executable,
    placement: wire.placement,
    reason: executable ? "" : (reason || NOT_EXECUTABLE),
    ports: portsOf(wire),
    operation: null,
  };
}

/** Certified set ops. The engine publishes objects; the canvas also accepts a name. */
function certifiedSetOps(catalog: ShapeCatalog | null | undefined): Set<string> {
  const names = new Set<string>();
  for (const entry of catalog?.set_operations ?? []) {
    if (typeof entry === "string") {
      if (entry) names.add(entry);
      continue;
    }
    if (entry.executable && entry.op) names.add(entry.op);
  }
  return names;
}

export function registryFromCatalog(catalog: ShapeCatalog | null | undefined): NodeSpec[] {
  const executableSet = certifiedSetOps(catalog);
  const reason = catalog?.post_load_only?.reason || NOT_EXECUTABLE;
  const specs: NodeSpec[] = [];
  const seen = new Set<string>();
  for (const operation of catalog?.operations ?? []) {
    specs.push(specFromOperation(operation));
    seen.add(operation.op);
  }
  const wires = catalog?.canvas_nodes?.length ? catalog.canvas_nodes : FALLBACK_CANVAS_NODES;
  for (const wire of wires) {
    if (seen.has(wire.op)) continue;
    specs.push(specFromWire(wire, executableSet, reason));
    seen.add(wire.op);
  }
  return specs;
}

export function specByOp(specs: NodeSpec[], op: string): NodeSpec | undefined {
  return specs.find((spec) => spec.op === op);
}

export function runnableSpecs(specs: NodeSpec[], query = ""): NodeSpec[] {
  return filterSpecs(specs, query).filter((spec) => spec.placement === "palette" && spec.executable);
}

export function plannedSpecs(specs: NodeSpec[], query = ""): NodeSpec[] {
  return filterSpecs(specs, query).filter((spec) => spec.placement !== "wizard" && !spec.executable);
}

export function filterSpecs(specs: NodeSpec[], query: string): NodeSpec[] {
  const needle = query.trim().toLowerCase();
  if (!needle) return specs;
  return specs.filter((spec) => (
    spec.op.includes(needle)
    || spec.short.toLowerCase().includes(needle)
    || spec.summary.toLowerCase().includes(needle)
    || spec.familyLabel.toLowerCase().includes(needle)
  ));
}

const FAMILY_ORDER = ["cleanse", "structural", "rows", "nested", "set", "sql", "object"];

export function specsByFamily(specs: NodeSpec[]): { family: string; label: string; operations: NodeSpec[] }[] {
  const buckets = new Map<string, NodeSpec[]>();
  for (const spec of specs) {
    const family = FAMILY_LABEL[spec.family] ? spec.family : "cleanse";
    const list = buckets.get(family) ?? [];
    list.push(spec);
    buckets.set(family, list);
  }
  const ordered = FAMILY_ORDER.filter((family) => (buckets.get(family) ?? []).length > 0);
  for (const family of buckets.keys()) {
    if (!ordered.includes(family)) ordered.push(family);
  }
  return ordered.map((family) => ({
    family,
    label: familyLabel(family),
    operations: buckets.get(family) ?? [],
  }));
}

/** Inspector fields stay on the shape operation. A disabled node has no form. */
export function inspectorOperation(spec: NodeSpec | undefined): ShapeOperation | null {
  if (!spec?.executable) return null;
  return spec.operation;
}

function stable(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map((item) => stable(item)).join(",")}]`;
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stable(record[key])}`).join(",")}}`;
}

function canonicalNode(nodeRecord: Record<string, unknown>): Record<string, unknown> {
  const item = JSON.parse(stable(nodeRecord)) as Record<string, unknown>;
  if (typeof item.op === "string") item.op = item.op.trim().toLowerCase();
  return item;
}

/**
 * Same identity as `set_document_hash` on the API. No nodes hashes to "".
 * Key order is not meaning. Op spelling is folded.
 */
export function setDocumentHash(document: { nodes?: Record<string, unknown>[]; main_output?: unknown; [key: string]: unknown } | null | undefined): string {
  const nodes = document?.nodes ?? [];
  if (!document || nodes.length === 0) return "";
  const body: Record<string, unknown> = {
    nodes: nodes.map((node) => canonicalNode(node)),
  };
  if (document.main_output != null && document.main_output !== "") body.main_output = document.main_output;
  for (const key of Object.keys(document)) {
    if (key === "nodes" || key === "main_output") continue;
    body[key] = JSON.parse(stable(document[key]));
  }
  return sha256Hex(stable(body)).slice(0, 16);
}

const SHA_K = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

function rotr(n: number, x: number): number {
  return (x >>> n) | (x << (32 - n));
}

/** Synchronous SHA-256 hex. Matches the API hash without a Node-only import. */
function sha256Hex(message: string): string {
  const bytes = new TextEncoder().encode(message);
  const bitLength = bytes.length * 8;
  const withOne = new Uint8Array(((bytes.length + 9 + 63) & ~63));
  withOne.set(bytes);
  withOne[bytes.length] = 0x80;
  const view = new DataView(withOne.buffer);
  view.setUint32(withOne.length - 4, bitLength, false);
  let h0 = 0x6a09e667;
  let h1 = 0xbb67ae85;
  let h2 = 0x3c6ef372;
  let h3 = 0xa54ff53a;
  let h4 = 0x510e527f;
  let h5 = 0x9b05688c;
  let h6 = 0x1f83d9ab;
  let h7 = 0x5be0cd19;
  const w = new Uint32Array(64);
  for (let offset = 0; offset < withOne.length; offset += 64) {
    for (let i = 0; i < 16; i += 1) w[i] = view.getUint32(offset + i * 4, false);
    for (let i = 16; i < 64; i += 1) {
      const s0 = rotr(7, w[i - 15]) ^ rotr(18, w[i - 15]) ^ (w[i - 15] >>> 3);
      const s1 = rotr(17, w[i - 2]) ^ rotr(19, w[i - 2]) ^ (w[i - 2] >>> 10);
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) >>> 0;
    }
    let a = h0; let b = h1; let c = h2; let d = h3;
    let e = h4; let f = h5; let g = h6; let h = h7;
    for (let i = 0; i < 64; i += 1) {
      const s1 = rotr(6, e) ^ rotr(11, e) ^ rotr(25, e);
      const ch = (e & f) ^ (~e & g);
      const temp1 = (h + s1 + ch + SHA_K[i] + w[i]) >>> 0;
      const s0 = rotr(2, a) ^ rotr(13, a) ^ rotr(22, a);
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (s0 + maj) >>> 0;
      h = g; g = f; f = e; e = (d + temp1) >>> 0;
      d = c; c = b; b = a; a = (temp1 + temp2) >>> 0;
    }
    h0 = (h0 + a) >>> 0; h1 = (h1 + b) >>> 0; h2 = (h2 + c) >>> 0; h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0; h5 = (h5 + f) >>> 0; h6 = (h6 + g) >>> 0; h7 = (h7 + h) >>> 0;
  }
  return [h0, h1, h2, h3, h4, h5, h6, h7].map((word) => word.toString(16).padStart(8, "0")).join("");
}
