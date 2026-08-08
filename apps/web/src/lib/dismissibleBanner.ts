/**
 * Persist operator dismissals for Overview attention / Freshness ribbons.
 * Re-shows when the signature changes (new failure count, new SLO status, etc.).
 */

const PREFIX = "df2.banner.dismiss.";

export function isBannerDismissed(id: string, signature: string): boolean {
  if (!id || !signature) return false;
  try {
    return localStorage.getItem(PREFIX + id) === signature;
  } catch {
    return false;
  }
}

export function dismissBanner(id: string, signature: string): void {
  if (!id || !signature) return;
  try {
    localStorage.setItem(PREFIX + id, signature);
  } catch {
    /* private mode */
  }
}

export function clearBannerDismissal(id: string): void {
  try {
    localStorage.removeItem(PREFIX + id);
  } catch {
    /* ignore */
  }
}
