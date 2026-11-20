import { useEffect, useState } from "react";
import { resendRefusal } from "../lib/authFlowCopy";
import { resendCode, SignupApiError } from "../lib/signupApi";
import { AuthButton } from "./AuthButton";
import { DtIcon } from "./DtIcon";
import { useToast } from "./Toast";

/** Focus the first field marked invalid inside the card. */
export function focusFirstInvalid(root: HTMLElement | null) {
  root?.querySelector<HTMLElement>('[aria-invalid="true"], .lp-field.is-error .lp-input')?.focus();
}

/** Where to look when a code or link does not show up. Mail can land in
 *  spam or junk, especially the first time; marking it "Not spam" teaches
 *  the mailbox to deliver the next one to the inbox. */
export function SpamFolderHint({ what = "email" }: { what?: string }) {
  return (
    <p className="lp-login-legal lp-login-spam-hint">
      Can't find the {what}? Check your spam or junk folder. It can take a minute to arrive. If it's
      there, mark it "Not spam" so the next one lands in your inbox.
    </p>
  );
}

/** Resend-with-cooldown row shared by verify + reset: muted while the
 *  countdown runs, aria-disabled, and the countdown + "New code sent"
 *  announce through an aria-live region. A refused resend says why
 *  (D324); a signup that can no longer be resent offers the way out. */
export function ResendRow({ challengeId, initialCooldown, onRestart, onSignIn, accountMayExist = false }: {
  challengeId: string;
  initialCooldown: number;
  /** Signup can't say whether the address has an account (enumeration). */
  accountMayExist?: boolean;
  onRestart?: () => void;
  onSignIn?: () => void;
}) {
  const [cooldown, setCooldown] = useState(initialCooldown);
  const [resending, setResending] = useState(false);
  const [sent, setSent] = useState(false);
  const [notice, setNotice] = useState("");
  const [ended, setEnded] = useState(false);
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
    setEnded(false);
    try {
      const r = await resendCode(challengeId);
      setCooldown(r.resendAfterSec || 60);
      setSent(true);
      // The server repeats the still-valid code, so either email works.
      const msg = accountMayExist
        ? "Sent again. Check your inbox and spam folder. If this address already has an account, the email is a sign-in reminder, not a code."
        : r.sameCode
        ? "Sent again. It's the same code, so the code in either email works. Check your spam folder too."
        : "New code sent. Use the code from the newest email, and check your spam folder too.";
      setNotice(msg);
      toast({ title: r.sameCode ? "Code sent again" : "New code sent", message: msg, tone: "info" });
    } catch (err) {
      const refusal = resendRefusal(err instanceof SignupApiError ? err : {});
      setNotice(refusal.text);
      setEnded(refusal.restart);
      toast({ title: "Resend unavailable", message: refusal.text, tone: "warning" });
      if (refusal.cooldownSec) setCooldown(refusal.cooldownSec);
    } finally {
      setResending(false);
    }
  };

  return (
    <div className="lp-login-resend">
      <SpamFolderHint what="code" />
      {!ended && (
        <p className="lp-login-legal" aria-live="polite">
          Didn't get it?{" "}
          <AuthButton variant="link" onClick={resend} disabled={cooldown > 0} loading={resending} loadingLabel="Sending…">
            {cooldown > 0 ? `Resend in ${cooldown}s` : "Resend code"}
          </AuthButton>
        </p>
      )}
      {notice && (
        <p className="lp-login-resend-status" aria-live="polite">
          {notice}
        </p>
      )}
      {ended && (onRestart || onSignIn) && (
        <p className="lp-login-legal">
          {onRestart && <AuthButton variant="link" onClick={onRestart}>Start again</AuthButton>}
          {onRestart && onSignIn && " or "}
          {onSignIn && <AuthButton variant="link" onClick={onSignIn}>sign in</AuthButton>}
        </p>
      )}
    </div>
  );
}

/** Show/hide password toggle — proper eye/eye-off icon, aria-pressed, and
 *  the 44px hit target comes from .lp-input-password-toggle. */
export function PasswordToggle({ show, onToggle }: { show: boolean; onToggle: () => void }) {
  return (
    <button
      type="button"
      className="lp-input-password-toggle"
      onClick={onToggle}
      aria-label={show ? "Hide password" : "Show password"}
      aria-pressed={show}
    >
      <DtIcon name={show ? "eye-off" : "eye"} size={16} />
    </button>
  );
}
