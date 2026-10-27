import { useEffect, useRef } from "react";

/** Render options handed to Turnstile — theme follows the app theme and the
 * widget stretches to the field width (`size: "flexible"`). `action` names
 * the form so the API can refuse a token minted for a different one. Pure so
 * tests can pin the contract the widget renders with. */
export function turnstileRenderOptions(
  siteKey: string,
  appTheme: "light" | "dark",
  onToken: (token: string) => void,
  action?: string,
): Record<string, unknown> {
  return {
    sitekey: siteKey,
    theme: appTheme,
    size: "flexible",
    ...(action ? { action } : {}),
    callback: (token: string) => onToken(token),
    "expired-callback": () => onToken(""),
  };
}

/** Cloudflare Turnstile — renders only when the deployment publishes a
 *  site key; no-ops to invisible otherwise. */
export function TurnstileWidget({
  siteKey,
  onToken,
  action,
}: {
  siteKey: string;
  onToken: (token: string) => void;
  action?: string;
}) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const tokenRef = useRef(onToken);
  tokenRef.current = onToken;

  useEffect(() => {
    if (!siteKey) return;
    const render = () => {
      const turnstile = (window as unknown as { turnstile?: { render: (el: HTMLElement, opts: Record<string, unknown>) => void } }).turnstile;
      if (hostRef.current && turnstile) {
        const appTheme = document.documentElement.dataset.theme === "dark" ? "dark" : "light";
        turnstile.render(hostRef.current,
          turnstileRenderOptions(siteKey, appTheme, (t) => tokenRef.current(t), action));
      }
    };
    const existing = document.querySelector<HTMLScriptElement>('script[data-df-turnstile="1"]');
    if (existing) {
      if ((window as unknown as { turnstile?: unknown }).turnstile) render();
      else existing.addEventListener("load", render, { once: true });
      return;
    }
    const script = document.createElement("script");
    script.src = "https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit";
    script.async = true;
    script.defer = true;
    script.dataset.dfTurnstile = "1";
    script.addEventListener("load", render, { once: true });
    document.head.appendChild(script);
  }, [siteKey, action]);

  if (!siteKey) return null;
  return <div ref={hostRef} className="lp-login-turnstile" data-testid="turnstile-widget" />;
}
