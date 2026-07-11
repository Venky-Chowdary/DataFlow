/** Runtime environment hints for UI copy (dev vs deployed). */

export function isDevRuntime(): boolean {
  return !import.meta.env.PROD;
}

export function isLocalHost(): boolean {
  if (typeof window === "undefined") return false;
  const host = window.location.hostname;
  return host === "localhost" || host === "127.0.0.1";
}

export function apiEnvLabel(online: boolean): string {
  if (!online) return "Unavailable";
  if (isDevRuntime() || isLocalHost()) return "Development";
  return "Production";
}

export function apiOfflineMessage(): { title: string; body: string } {
  if (isDevRuntime() || isLocalHost()) {
    return {
      title: "Control plane offline",
      body: "Start the API with npm run dev:api — connectors and transfers need port 8001.",
    };
  }
  return {
    title: "Control plane offline",
    body: "The API service is unreachable. Check your deployment health endpoint and network settings.",
  };
}
