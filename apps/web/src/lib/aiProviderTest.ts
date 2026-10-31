/**
 * Settings -> AI Models: what the "Test" button says (QA enterprise #42).
 *
 * Ollama has no key: its test probes the server and checks the configured
 * model is pulled, so the copy talks about a connection, not a key.
 */
export type ProviderTestResult = { ok: boolean; error?: string; base_url?: string };

export function providerHasTest(provider: { provider: string; tier: string; configured: boolean }): boolean {
  return provider.provider === "ollama" || (provider.tier === "cloud" && provider.configured);
}

export function providerTestLabel(provider: string): string {
  return provider === "ollama" ? "Test connection" : "Test key";
}

export function providerTestToast(provider: string, result: ProviderTestResult): { title: string; message: string } {
  if (provider === "ollama") {
    return result.ok
      ? { title: "Ollama is reachable", message: `The server${result.base_url ? ` at ${result.base_url}` : ""} answered and the model is pulled.` }
      : { title: "Ollama is not ready", message: result.error || "The Ollama server did not answer." };
  }
  return result.ok
    ? { title: `${provider} key works`, message: "The provider accepted the saved key." }
    : { title: `${provider} key rejected`, message: result.error || "The provider rejected the saved key." };
}
