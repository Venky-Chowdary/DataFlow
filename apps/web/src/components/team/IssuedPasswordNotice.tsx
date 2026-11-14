import { useState } from "react";

interface IssuedPasswordNoticeProps {
  email: string;
  password: string;
  onDone: () => void;
  /** Start revealed (tests / an explicit "show now" flow). */
  initiallyRevealed?: boolean;
}

/**
 * A one-time password, shown once in the operator's own session. Masked until
 * the admin reveals it, so it is not on screen (or in a screen share) by default.
 */
export function IssuedPasswordNotice({ email, password, onDone, initiallyRevealed = false }: IssuedPasswordNoticeProps) {
  const [revealed, setRevealed] = useState(initiallyRevealed);
  const [copied, setCopied] = useState<"" | "done" | "failed">("");
  // UX-38: copy without having to reveal it on screen.
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(password);
      setCopied("done");
    } catch {
      setCopied("failed");
    }
  };
  return (
    <div className="df2-team-secret">
      <div>
        <strong>One-time password for {email}</strong>
        <span>Shown once. They will be asked to change it at first sign-in.</span>
      </div>
      <code className="df2-team-secret-value" aria-label={revealed ? "One-time password" : "One-time password (hidden)"}>
        {revealed ? password : "••••••••••••"}
      </code>
      <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={() => setRevealed((v) => !v)}>
        {revealed ? "Hide" : "Reveal password"}
      </button>
      <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={() => void copy()}>
        {copied === "done" ? "Copied" : "Copy password"}
      </button>
      <button type="button" className="df2-btn df2-btn-sm" onClick={onDone}>
        Done
      </button>
      <span className="df2-sr-only" role="status" aria-live="polite">
        {copied === "done" ? "Password copied to the clipboard." : copied === "failed" ? "Copy failed: reveal the password and copy it by hand." : ""}
      </span>
    </div>
  );
}
