/**
 * The host the operator should read on a saved connection.
 *
 * ``dial_host`` is the server's view of the URI (an Atlas hostname wins over
 * a leftover localhost field). A connection that really is loopback says so
 * instead of looking like a usable Atlas destination.
 */

const LOOPBACK = new Set(["localhost", "127.0.0.1", "::1", "0.0.0.0", "host.docker.internal"]);

export function isLoopbackHost(host: string | undefined): boolean {
  const text = (host || "").trim().toLowerCase().replace(/^\[|\]$/g, "");
  return LOOPBACK.has(text);
}

export function connectorEndpointLabel(connector: {
  host?: string;
  port?: number;
  dial_host?: string;
  dial_host_loopback?: boolean;
}): string {
  const host = (connector.dial_host || connector.host || "").trim();
  const loopback = connector.dial_host_loopback === true || isLoopbackHost(host);
  if (!host && !loopback) return "";
  if (loopback) {
    const shown = host || "localhost";
    return `${shown} (loopback — not a cloud target)`;
  }
  const atlas = host.toLowerCase().endsWith(".mongodb.net");
  const port = connector.port && connector.port > 0 && !atlas ? `:${connector.port}` : "";
  return `${host}${port}`;
}
