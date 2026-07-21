/** Runtime environment hints for UI copy (dev vs deployed). */

export function isDevRuntime(): boolean {
  return !import.meta.env.PROD;
}

export function isLocalHost(): boolean {
  if (typeof window === "undefined") return false;
  const host = window.location.hostname;
  return host === "localhost" || host === "127.0.0.1" || host === "[::1]";
}

/** Railway / custom prod hosts — never show local npm run instructions. */
export function isDeployedHost(): boolean {
  if (typeof window === "undefined") return Boolean(import.meta.env.PROD);
  const host = window.location.hostname;
  if (isLocalHost()) return false;
  return (
    host.endsWith(".railway.app")
    || host.endsWith(".up.railway.app")
    || host.endsWith(".dataflow.app")
    || Boolean(import.meta.env.PROD)
  );
}

export function apiEnvLabel(online: boolean): string {
  if (!online) return "Unavailable";
  if (isLocalHost() && isDevRuntime()) return "Local";
  if (isDeployedHost()) return "Production";
  if (isDevRuntime()) return "Development";
  return "Production";
}

export function apiOfflineMessage(): { title: string; body: string } {
  // Only local npm workflows should see "dev:api" — never on Railway mid-transfer.
  if (isLocalHost() && !isDeployedHost()) {
    return {
      title: "Control plane offline",
      body: "Start the API with npm run dev:api — connectors and transfers need port 8001.",
    };
  }
  return {
    title: "Control plane busy or unreachable",
    body: "A health check timed out — often because a long Snowflake introspect is running. This is not a validation failure. Wait for schema load to finish, then continue.",
  };
}
