/** Inline SVG illustrations for marketing subpages — no external assets required. */

type IllustrationKind =
  | "security"
  | "enterprise"
  | "contact"
  | "legal"
  | "customers"
  | "pricing"
  | "integrations"
  | "mapping"
  | "help";

export function MarketingIllustration({ kind, className = "" }: { kind: IllustrationKind; className?: string }) {
  const cls = `lp-mkt-illustration ${className}`.trim();

  if (kind === "security") {
    return (
      <svg className={cls} viewBox="0 0 480 280" role="img" aria-label="Security architecture diagram">
        <defs>
          <linearGradient id="lp-sec-g" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#f0fdfa" />
            <stop offset="100%" stopColor="#ecfdf5" />
          </linearGradient>
        </defs>
        <rect width="480" height="280" rx="16" fill="url(#lp-sec-g)" stroke="#ccece7" />
        <rect x="24" y="32" width="120" height="56" rx="10" fill="#fff" stroke="#99f6e4" />
        <text x="84" y="64" textAnchor="middle" fontSize="11" fill="#0f766e" fontWeight="600">Tenant</text>
        <rect x="180" y="32" width="120" height="56" rx="10" fill="#fff" stroke="#99f6e4" />
        <text x="240" y="64" textAnchor="middle" fontSize="11" fill="#0f766e" fontWeight="600">BYOK KMS</text>
        <rect x="336" y="32" width="120" height="56" rx="10" fill="#fff" stroke="#99f6e4" />
        <text x="396" y="64" textAnchor="middle" fontSize="11" fill="#0f766e" fontWeight="600">Audit log</text>
        <path d="M84 88v24M240 88v24M396 88v24" stroke="#5eead4" strokeWidth="2" strokeDasharray="4 4" />
        <rect x="72" y="112" width="336" height="72" rx="12" fill="#fff" stroke="#0d9488" strokeWidth="1.5" />
        <text x="240" y="142" textAnchor="middle" fontSize="12" fill="#0f172a" fontWeight="650">Governed transfer engine</text>
        <text x="240" y="162" textAnchor="middle" fontSize="10" fill="#64748b">Preflight · Quarantine · Reconcile</text>
        <rect x="48" y="208" width="384" height="44" rx="10" fill="#0f766e" opacity="0.08" />
        <text x="240" y="235" textAnchor="middle" fontSize="11" fill="#0f766e" fontWeight="600">SOC 2 · GDPR · HIPAA-ready posture</text>
      </svg>
    );
  }

  if (kind === "enterprise") {
    return (
      <svg className={cls} viewBox="0 0 480 280" role="img" aria-label="Enterprise deployment">
        <rect width="480" height="280" rx="16" fill="#f8fafc" stroke="#e2e8f0" />
        <rect x="40" y="48" width="400" height="48" rx="10" fill="#fff" stroke="#cbd5e1" />
        <text x="240" y="78" textAnchor="middle" fontSize="12" fill="#334155" fontWeight="600">dataflow.company.com · SSO</text>
        <rect x="40" y="116" width="180" height="120" rx="10" fill="#fff" stroke="#99f6e4" />
        <text x="130" y="148" textAnchor="middle" fontSize="11" fill="#0f766e" fontWeight="650">Workspace A</text>
        <text x="130" y="172" textAnchor="middle" fontSize="10" fill="#64748b">RBAC · Pipelines</text>
        <rect x="260" y="116" width="180" height="120" rx="10" fill="#fff" stroke="#99f6e4" />
        <text x="350" y="148" textAnchor="middle" fontSize="11" fill="#0f766e" fontWeight="650">Workspace B</text>
        <text x="350" y="172" textAnchor="middle" fontSize="10" fill="#64748b">Audit · MCP</text>
      </svg>
    );
  }

  if (kind === "contact") {
    return (
      <svg className={cls} viewBox="0 0 480 280" role="img" aria-label="Contact support">
        <rect width="480" height="280" rx="16" fill="#f0fdfa" stroke="#99f6e4" />
        <circle cx="240" cy="100" r="40" fill="#0d9488" opacity="0.15" />
        <path d="M240 72v56M212 100h56" stroke="#0f766e" strokeWidth="3" strokeLinecap="round" />
        <rect x="80" y="160" width="320" height="88" rx="12" fill="#fff" stroke="#e2e8f0" />
        <text x="240" y="192" textAnchor="middle" fontSize="12" fill="#0f172a" fontWeight="650">Pilot plan in 48 hours</text>
        <text x="240" y="214" textAnchor="middle" fontSize="10" fill="#64748b">Sources · Destinations · Compliance review</text>
        <text x="240" y="234" textAnchor="middle" fontSize="10" fill="#0f766e">Dedicated solutions engineer</text>
      </svg>
    );
  }

  if (kind === "legal") {
    return (
      <svg className={cls} viewBox="0 0 480 280" role="img" aria-label="Legal document">
        <rect width="480" height="280" rx="16" fill="#fff" stroke="#e2e8f0" />
        <rect x="120" y="24" width="240" height="232" rx="8" fill="#f8fafc" stroke="#cbd5e1" />
        {[0, 1, 2, 3, 4, 5].map((i) => (
          <rect key={i} x="148" y={48 + i * 28} width={i % 2 === 0 ? 184 : 140} height="8" rx="4" fill="#e2e8f0" />
        ))}
        <circle cx="240" cy="220" r="20" fill="#0d9488" opacity="0.12" />
        <path d="M230 220l8 8 16-16" stroke="#0f766e" strokeWidth="2.5" fill="none" strokeLinecap="round" />
      </svg>
    );
  }

  if (kind === "customers") {
    return (
      <svg className={cls} viewBox="0 0 480 280" role="img" aria-label="Customer outcomes">
        <rect width="480" height="280" rx="16" fill="#f8fafb" stroke="#e2e8f0" />
        {[
          { x: 60, label: "Retail", stat: "12k migrations" },
          { x: 190, label: "Healthcare", stat: "HIPAA paths" },
          { x: 320, label: "SaaS", stat: "Agent MCP" },
        ].map((c) => (
          <g key={c.label}>
            <rect x={c.x} y="48" width="100" height="184" rx="12" fill="#fff" stroke="#e2e8f0" />
            <text x={c.x + 50} y="88" textAnchor="middle" fontSize="11" fill="#0f172a" fontWeight="650">{c.label}</text>
            <text x={c.x + 50} y="200" textAnchor="middle" fontSize="10" fill="#0f766e">{c.stat}</text>
          </g>
        ))}
      </svg>
    );
  }

  if (kind === "pricing") {
    return (
      <svg className={cls} viewBox="0 0 480 280" role="img" aria-label="Pricing tiers">
        <rect width="480" height="280" rx="16" fill="#f8fafc" stroke="#e2e8f0" />
        {[
          { x: 48, h: 160, featured: false, label: "Starter" },
          { x: 176, h: 200, featured: true, label: "Team" },
          { x: 304, h: 160, featured: false, label: "Enterprise" },
        ].map((t) => (
          <g key={t.label}>
            <rect
              x={t.x}
              y={280 - t.h - 40}
              width="128"
              height={t.h}
              rx="12"
              fill="#fff"
              stroke={t.featured ? "#0d9488" : "#e2e8f0"}
              strokeWidth={t.featured ? 2 : 1}
            />
            <text x={t.x + 64} y={280 - t.h - 16} textAnchor="middle" fontSize="11" fill={t.featured ? "#0f766e" : "#64748b"} fontWeight="650">
              {t.label}
            </text>
          </g>
        ))}
      </svg>
    );
  }

  if (kind === "mapping") {
    const src = ["order_amt", "cust_email", "order_id", "ts"];
    const dst = ["payment_amount", "email", "order_key", "created_at"];
    return (
      <svg className={cls} viewBox="0 0 480 280" role="img" aria-label="Semantic column mapping">
        <rect width="480" height="280" rx="16" fill="#f8fafc" stroke="#e2e8f0" />
        <text x="96" y="34" textAnchor="middle" fontSize="11" fill="#64748b" fontWeight="700">SOURCE</text>
        <text x="384" y="34" textAnchor="middle" fontSize="11" fill="#0f766e" fontWeight="700">DESTINATION</text>
        {src.map((s, i) => {
          const y = 60 + i * 50;
          return (
            <g key={s}>
              <rect x="24" y={y} width="144" height="36" rx="8" fill="#fff" stroke="#cbd5e1" />
              <text x="96" y={y + 23} textAnchor="middle" fontSize="12" fill="#334155" fontFamily="ui-monospace, monospace">{s}</text>
              <rect x="312" y={y} width="144" height="36" rx="8" fill="#fff" stroke="#99f6e4" />
              <text x="384" y={y + 23} textAnchor="middle" fontSize="12" fill="#0f766e" fontFamily="ui-monospace, monospace">{dst[i]}</text>
              <path d={`M168 ${y + 18}C220 ${y + 18} 260 ${y + 18} 312 ${y + 18}`} stroke="#0d9488" strokeWidth="2" fill="none" strokeDasharray="5 4" />
              <circle cx="240" cy={y + 18} r="11" fill="#ecfdf5" stroke="#0d9488" />
              <text x="240" y={y + 22} textAnchor="middle" fontSize="9" fill="#0f766e" fontWeight="700">{96 - i * 3}</text>
            </g>
          );
        })}
      </svg>
    );
  }

  if (kind === "help") {
    return (
      <svg className={cls} viewBox="0 0 480 280" role="img" aria-label="Documentation guides">
        <rect width="480" height="280" rx="16" fill="#f0fdfa" stroke="#99f6e4" />
        {[0, 1, 2].map((i) => (
          <g key={i}>
            <rect x={48 + i * 140} y="48" width="120" height="184" rx="12" fill="#fff" stroke="#e2e8f0" />
            <rect x={64 + i * 140} y="72" width="88" height="8" rx="4" fill="#e2e8f0" />
            <rect x={64 + i * 140} y="92" width="72" height="6" rx="3" fill="#f1f5f9" />
            <rect x={64 + i * 140} y="108" width="80" height="6" rx="3" fill="#f1f5f9" />
            <circle cx={108 + i * 140} cy="200" r="16" fill="#0d9488" opacity="0.15" />
            <path d={`M${102 + i * 140} 200l6 6 12-12`} stroke="#0f766e" strokeWidth="2" fill="none" strokeLinecap="round" />
          </g>
        ))}
      </svg>
    );
  }

  return (
    <svg className={cls} viewBox="0 0 480 280" role="img" aria-label="Connector ecosystem">
      <rect width="480" height="280" rx="16" fill="#f0fdfa" stroke="#99f6e4" />
      <circle cx="240" cy="140" r="36" fill="#0d9488" opacity="0.2" />
      <text x="240" y="145" textAnchor="middle" fontSize="11" fill="#0f766e" fontWeight="700">DataFlow</text>
      {[0, 60, 120, 180, 240, 300].map((deg, i) => {
        const rad = (deg * Math.PI) / 180;
        const x = 240 + Math.cos(rad) * 100;
        const y = 140 + Math.sin(rad) * 80;
        return (
          <g key={i}>
            <line x1="240" y1="140" x2={x} y2={y} stroke="#5eead4" strokeWidth="1.5" />
            <circle cx={x} cy={y} r="14" fill="#fff" stroke="#0d9488" />
          </g>
        );
      })}
    </svg>
  );
}
