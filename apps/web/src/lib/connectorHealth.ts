/**
 * Connector test health — single source of truth for list, drawer, dashboard.
 *
 * Never derive "failed" from a stale ``status`` string when ``last_test_ok``
 * is true. ``status`` is a display convenience; the probe result is authoritative.
 */

export type ConnectorTestHealth = "passed" | "failed" | "never_tested";

export type ConnectorHealthFields = {
  last_test_ok?: boolean | null;
  status?: string | null;
};

/** Coerce API/BSON/legacy string booleans into a tri-state. */
export function coerceLastTestOk(value: unknown): boolean | undefined {
  if (value === true || value === 1 || value === "true" || value === "1") return true;
  if (value === false || value === 0 || value === "false" || value === "0") return false;
  return undefined;
}

export function connectorTestHealth(c: ConnectorHealthFields): ConnectorTestHealth {
  const ok = coerceLastTestOk(c.last_test_ok);
  if (ok === true) return "passed";
  if (ok === false) return "failed";
  return "never_tested";
}

/** True when the last probe passed, or the connector has never been tested. */
export function connectorLooksHealthy(c: ConnectorHealthFields): boolean {
  return connectorTestHealth(c) !== "failed";
}

/** True only after a probe passed — never-tested is not healthy. */
export function connectorPassedProbe(c: ConnectorHealthFields): boolean {
  return connectorTestHealth(c) === "passed";
}

export function connectorNeedsAttention(c: ConnectorHealthFields): boolean {
  return connectorTestHealth(c) === "failed";
}

export function connectorTestLabel(c: ConnectorHealthFields): string {
  const h = connectorTestHealth(c);
  if (h === "passed") return "Test passed";
  if (h === "failed") return "Test failed";
  return "Never tested";
}

export function connectorHealthBadgeLabel(c: ConnectorHealthFields): string {
  const h = connectorTestHealth(c);
  if (h === "passed") return "Healthy";
  if (h === "failed") return "Connection error";
  return "Never tested";
}

/** Derive list ``status`` from the probe result — never keep a stale error. */
export function statusFromLastTest(lastTestOk: boolean | undefined): string {
  if (lastTestOk === false) return "error";
  return "configured";
}
