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
      <button type="button" className="df2-btn df2-btn-sm" onClick={onDone}>
        Done
      </button>
    </div>
  );
}
