/**
 * Settings -> SSO editor helpers (QA enterprise #13/#14/#15).
 *
 * The API refuses to enable a provider with required fields blank, persists
 * `email_domains` (which drive `/auth/sso/route`), and clears a stored client
 * secret only on an explicit `clear_client_secret` — a blank secret keeps it.
 */
import type { SsoConfig, SsoType } from "./api";

export const SSO_REQUIRED_FIELDS: Record<SsoType, readonly string[]> = {
  saml: ["entity_id", "sso_url", "x509_cert"],
  oidc: ["issuer", "client_id", "client_secret", "redirect_uri"],
  azure_ad: ["tenant_id", "client_id", "client_secret", "redirect_uri"],
};

const FIELD_LABELS: Record<string, string> = {
  entity_id: "Entity ID",
  sso_url: "SSO URL",
  x509_cert: "IdP certificate",
  issuer: "Issuer URL",
  client_id: "Client ID",
  client_secret: "Client secret",
  redirect_uri: "Redirect URI",
  tenant_id: "Tenant ID",
};

/** "acme.com, @Corp.io  acme.com" -> ["acme.com", "corp.io"]. */
export function parseEmailDomains(text: string): string[] {
  const out: string[] = [];
  for (const part of text.split(/[\s,;]+/)) {
    const domain = part.trim().toLowerCase().replace(/^@/, "").replace(/\.$/, "");
    if (domain && !out.includes(domain)) out.push(domain);
  }
  return out;
}

/**
 * Required fields still blank if this draft were enabled. A field the server
 * already holds (a saved secret or certificate shown masked) counts as set.
 */
export function missingForEnable(
  type: SsoType,
  draft: SsoConfig,
  saved: SsoConfig | undefined,
  clearSecret: boolean,
): string[] {
  return SSO_REQUIRED_FIELDS[type].filter((field) => {
    const draftValue = String((draft as Record<string, unknown>)[field] ?? "").trim();
    if (draftValue) return false;
    if (field === "client_secret" && clearSecret) return true;
    if (field === "client_secret" || field === "x509_cert") {
      return !String((saved as Record<string, unknown> | undefined)?.[field] ?? "").trim();
    }
    return true;
  }).map((field) => FIELD_LABELS[field] ?? field);
}

/** The PATCH body for one save. */
export function buildSsoPatch(
  draft: SsoConfig,
  domainsText: string,
  clearSecret: boolean,
): Partial<SsoConfig> {
  const body: Partial<SsoConfig> = { ...draft, email_domains: parseEmailDomains(domainsText) };
  if (clearSecret) {
    body.clear_client_secret = true;
    delete body.client_secret;
  }
  return body;
}
