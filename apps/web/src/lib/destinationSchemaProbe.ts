export interface DestinationProbeEndpoint {
  kind?: unknown;
  format?: unknown;
  connector_id?: unknown;
  host?: unknown;
  port?: unknown;
  database?: unknown;
  schema?: unknown;
  table?: unknown;
  collection?: unknown;
  warehouse?: unknown;
  connection_string?: unknown;
  username?: unknown;
  password?: unknown;
}

export const INLINE_DESTINATION_PROBE_DEBOUNCE_MS = 600;

const SECRET_SETTING_NAMES =
  "(?:password|passwd|pwd|token|access[_-]?token|refresh[_-]?token|client[_-]?secret|api[_-]?key|secret(?:[_-]?key)?|private[_-]?key|credential)";
const SECRET_SETTING_RE = new RegExp(
  `(["']?${SECRET_SETTING_NAMES}["']?\\s*[=:]\\s*)(?:"[^"]*"|'[^']*'|[^&;,\\s}]+)`,
  "gi",
);
const SECRET_QUERY_RE = new RegExp(
  `([?&]${SECRET_SETTING_NAMES}=)[^&#]*`,
  "gi",
);
const URL_CREDENTIALS_RE = /(\b[a-z][a-z\d+.-]*:\/\/)([^:/@?#]+)(?::([^@/?#]*))?@/gi;

function text(value: unknown): string {
  return typeof value === "string" || typeof value === "number"
    ? String(value).trim()
    : "";
}

function redactConnectionString(value: string): { value: string; hasSecret: boolean } {
  let hasSecret = false;
  const redacted = value
    .replace(SECRET_QUERY_RE, (_match, prefix: string) => {
      hasSecret = true;
      return `${prefix}<redacted>`;
    })
    .replace(SECRET_SETTING_RE, (_match, prefix: string) => {
      hasSecret = true;
      return `${prefix}<redacted>`;
    })
    .replace(
      URL_CREDENTIALS_RE,
      (_match, scheme: string, username: string, password: string | undefined) => {
        if (password === undefined) return `${scheme}${username}@`;
        hasSecret = true;
        return `${scheme}${username}:<redacted>@`;
      },
    );
  return { value: redacted, hasSecret };
}

export function isInlineDestinationConfigured(endpoint: DestinationProbeEndpoint): boolean {
  const engine = text(endpoint.format).toLowerCase();
  if (!engine) return false;
  if (text(endpoint.connection_string)) return true;
  if (engine === "sqlite" || engine === "duckdb") return Boolean(text(endpoint.database));
  return Boolean(text(endpoint.host) && text(endpoint.database));
}

export function destinationSchemaProbeKey(
  endpoint: DestinationProbeEndpoint,
  connectorId = "",
): string {
  const rawConnectionString = text(endpoint.connection_string);
  const connectionString = redactConnectionString(rawConnectionString);
  const embeddedPasswordPresent = connectionString.hasSecret;
  return JSON.stringify({
    connectorId: text(endpoint.connector_id) || connectorId,
    engine: text(endpoint.format).toLowerCase(),
    host: text(endpoint.host),
    port: text(endpoint.port),
    database: text(endpoint.database),
    schema: text(endpoint.schema),
    table: text(endpoint.table) || text(endpoint.collection),
    warehouse: text(endpoint.warehouse),
    connectionString: connectionString.value,
    username: text(endpoint.username),
    passwordPresent: Boolean(text(endpoint.password)) || embeddedPasswordPresent,
  });
}

export type DestinationProbeOutcome<TResult> =
  | { status: "loaded"; value: TResult }
  | { status: "cached" | "pending" | "stale" | "unconfigured" }
  | { status: "error"; error: unknown };

export interface DestinationSchemaProbeController<TResult> {
  schedule(endpoint: DestinationProbeEndpoint, options?: { force?: boolean }): Promise<DestinationProbeOutcome<TResult>>;
  cancel(): void;
}

export function createDestinationSchemaProbe<TResult>(options: {
  load: (endpoint: DestinationProbeEndpoint) => Promise<TResult>;
  delayMs?: number;
  onResult?: (result: TResult, endpoint: DestinationProbeEndpoint) => void;
  onError?: (error: unknown, endpoint: DestinationProbeEndpoint) => void;
}): DestinationSchemaProbeController<TResult> {
  const delayMs = options.delayMs ?? INLINE_DESTINATION_PROBE_DEBOUNCE_MS;
  let generation = 0;
  let currentKey = "";
  let lastSuccessKey = "";
  let timer: ReturnType<typeof setTimeout> | null = null;
  let pendingTimerResolve: ((outcome: DestinationProbeOutcome<TResult>) => void) | null = null;

  const cancel = () => {
    generation += 1;
    currentKey = "";
    if (timer !== null) clearTimeout(timer);
    timer = null;
    pendingTimerResolve?.({ status: "stale" });
    pendingTimerResolve = null;
  };

  return {
    cancel,
    schedule(endpoint, scheduleOptions = {}) {
      if (!isInlineDestinationConfigured(endpoint)) {
        cancel();
        return Promise.resolve({ status: "unconfigured" });
      }

      const key = destinationSchemaProbeKey(endpoint);
      if (!scheduleOptions.force && key === lastSuccessKey) {
        return Promise.resolve({ status: "cached" });
      }
      if (!scheduleOptions.force && key === currentKey) {
        return Promise.resolve({ status: "pending" });
      }

      cancel();
      currentKey = key;
      const requestGeneration = generation;
      return new Promise((resolve) => {
        pendingTimerResolve = resolve;
        timer = setTimeout(async () => {
          timer = null;
          pendingTimerResolve = null;
          let value: TResult;
          try {
            value = await options.load(endpoint);
          } catch (error) {
            if (requestGeneration !== generation || currentKey !== key) {
              resolve({ status: "stale" });
              return;
            }
            options.onError?.(error, endpoint);
            resolve({ status: "error", error });
            return;
          }

          if (requestGeneration !== generation || currentKey !== key) {
            resolve({ status: "stale" });
            return;
          }
          lastSuccessKey = key;
          options.onResult?.(value, endpoint);
          resolve({ status: "loaded", value });
        }, Math.max(0, delayMs));
      });
    },
  };
}
