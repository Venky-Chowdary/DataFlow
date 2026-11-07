import { useState } from "react";

interface SigningSecretNoticeProps {
  label: string;
  secret: string;
  onDone: () => void;
  initiallyRevealed?: boolean;
}

/**
 * A webhook channel's signing secret, shown once after create or rotate
 * (QA enterprise #46). Masked until revealed; the receiver verifies the
 * X-Datawrap-Signature header with it (docs/WEBHOOK_SIGNING.md).
 */
export function SigningSecretNotice({ label, secret, onDone, initiallyRevealed = false }: SigningSecretNoticeProps) {
  const [revealed, setRevealed] = useState(initiallyRevealed);
  return (
    <div className="df2-team-secret" data-testid="webhook-signing-secret">
      <div>
        <strong>Signing secret for {label}</strong>
        <span>
          Shown once. Deliveries carry X-Datawrap-Signature: t=&lt;timestamp&gt;,v1=HMAC-SHA256(secret,
          &quot;&lt;timestamp&gt;.&lt;raw body&gt;&quot;). Store it in the receiver now.
        </span>
      </div>
      <code className="df2-team-secret-value" aria-label={revealed ? "Signing secret" : "Signing secret (hidden)"}>
        {revealed ? secret : "••••••••••••"}
      </code>
      <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={() => setRevealed((v) => !v)}>
        {revealed ? "Hide" : "Reveal secret"}
      </button>
      <button type="button" className="df2-btn df2-btn-sm" onClick={onDone}>
        Done
      </button>
    </div>
  );
}
