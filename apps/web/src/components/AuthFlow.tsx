import { useEffect, useState } from "react";
import { resendCode, SignupApiError } from "../lib/signupApi";
import { useToast } from "./Toast";

/** Focus the first field marked invalid inside the card. */
export function focusFirstInvalid(root: HTMLElement | null) {
  root?.querySelector<HTMLElement>('[aria-invalid="true"], .lp-field.is-error .lp-input')?.focus();
}

/** Resend-with-cooldown row shared by verify + reset: muted while the
 *  countdown runs, aria-disabled, and the countdown + "New code sent"
 *  announce through an aria-live region. */
export function ResendRow({ challengeId, initialCooldown }: { challengeId: string; initialCooldown: number }) {
  const [cooldown, setCooldown] = useState(initialCooldown);
  const [resending, setResending] = useState(false);
  const [sent, setSent] = useState(false);
  const [notice, setNotice] = useState("");
  const { toast } = useToast();

  useEffect(() => {
    if (cooldown <= 0) return;
    const t = window.setTimeout(() => setCooldown((s) => s - 1), 1000);
    return () => window.clearTimeout(t);
  }, [cooldown]);

  const resend = async () => {
    setResending(true);
    setSent(false);
    setNotice("");
    try {
      const r = await resendCode(challengeId);
      setCooldown(r.resendAfterSec || 60);
      setSent(true);
      setNotice("New code sent — check your inbox.");
      toast({ title: "New code sent", message: "Check the inbox for the latest 6-digit code.", tone: "info" });
    } catch (err) {
      const msg = err instanceof SignupApiError && err.code === "resend_limited"
        ? `Too many resends — wait ${Math.max(1, Math.round(err.retryAfterSec / 60))} min.`
        : "Couldn't resend the code. Try again shortly.";
      setNotice(msg);
      toast({ title: "Resend unavailable", message: msg, tone: "warning" });
      if (err instanceof SignupApiError && err.retryAfterSec) setCooldown(err.retryAfterSec);
    } finally {
      setResending(false);
    }
  };

  const disabled = cooldown > 0 || resending;
  return (
    <div className="lp-login-resend">
      <p className="lp-login-legal">
        Didn't get it?{" "}
        <button type="button" onClick={resend} disabled={disabled} aria-disabled={disabled}>
          {cooldown > 0 ? `Resend in ${cooldown}s` : resending ? "Sending…" : "Resend code"}
        </button>
      </p>
      <p className="lp-login-resend-status" aria-live="polite">
        {notice || (cooldown > 0 ? `You can request a new code in ${cooldown} seconds.` : "")}
      </p>
    </div>
  );
}
