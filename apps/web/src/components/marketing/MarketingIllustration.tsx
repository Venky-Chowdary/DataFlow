/** Inline SVG illustrations for marketing subpages — no external assets required. */

import type { CSSProperties } from "react";

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
      <svg className={`${cls} lp-mkt-illustration--contact`} viewBox="0 0 480 300" role="img" aria-label="Sales engagement network">
        <defs>
          <linearGradient id="lp-contact-bg" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#042f2e" />
            <stop offset="55%" stopColor="#0f2744" />
            <stop offset="100%" stopColor="#07111f" />
          </linearGradient>
          <radialGradient id="lp-contact-glow" cx="50%" cy="40%" r="50%">
            <stop offset="0%" stopColor="#14b8a6" stopOpacity="0.45" />
            <stop offset="100%" stopColor="#14b8a6" stopOpacity="0" />
          </radialGradient>
        </defs>
        <rect width="480" height="300" rx="18" fill="url(#lp-contact-bg)" />
        <ellipse cx="240" cy="120" rx="160" ry="90" fill="url(#lp-contact-glow)" className="lp-contact-pulse" />
        <circle cx="240" cy="140" r="78" fill="none" stroke="#14b8a6" strokeOpacity="0.25" strokeWidth="1" className="lp-contact-ring" />
        <circle cx="240" cy="140" r="112" fill="none" stroke="#5eead4" strokeOpacity="0.18" strokeWidth="1" strokeDasharray="4 8" className="lp-contact-ring lp-contact-ring--slow" />
        <circle cx="240" cy="140" r="36" fill="#0d9488" />
        <text x="240" y="136" textAnchor="middle" fontSize="10" fill="#ecfdf5" fontWeight="700">SALES</text>
        <text x="240" y="152" textAnchor="middle" fontSize="9" fill="#99f6e4">48h pilot</text>
        {[
          { x: 96, y: 72, label: "Sources" },
          { x: 384, y: 72, label: "Dest" },
          { x: 88, y: 210, label: "SSO" },
          { x: 392, y: 210, label: "BYOK" },
          { x: 240, y: 248, label: "Pilot" },
        ].map((n) => (
          <g key={n.label}>
            <line x1="240" y1="140" x2={n.x} y2={n.y} stroke="#5eead4" strokeOpacity="0.35" strokeWidth="1.5" className="lp-contact-spoke" />
            <circle cx={n.x} cy={n.y} r="22" fill="#0f172a" stroke="#14b8a6" strokeWidth="1.5" />
            <text x={n.x} y={n.y + 4} textAnchor="middle" fontSize="9" fill="#ecfdf5" fontWeight="650">{n.label}</text>
          </g>
        ))}
        <rect x="140" y="18" width="200" height="28" rx="14" fill="#042f2e" stroke="#14b8a6" strokeOpacity="0.5" />
        <text x="240" y="36" textAnchor="middle" fontSize="11" fill="#99f6e4" fontWeight="650">Solutions engineer · 1 business day</text>
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
      <svg className={`${cls} lp-mkt-illustration--customers`} viewBox="0 0 480 300" role="img" aria-label="Customer proof mosaic">
        <defs>
          <linearGradient id="lp-cust-bg" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="#f0fdfa" />
            <stop offset="100%" stopColor="#ecfdf5" />
          </linearGradient>
        </defs>
        <rect width="480" height="300" rx="18" fill="url(#lp-cust-bg)" stroke="#99f6e4" />
        <g className="lp-cust-tile" style={{ "--i": 0 } as CSSProperties}>
          <rect x="24" y="24" width="200" height="120" rx="14" fill="#fff" stroke="#ccece7" />
          <text x="40" y="54" fontSize="11" fill="#0f766e" fontWeight="700">RETAIL</text>
          <text x="40" y="88" fontSize="22" fill="#0f172a" fontWeight="700">12k+</text>
          <text x="40" y="112" fontSize="12" fill="#64748b">migrations governed</text>
        </g>
        <g className="lp-cust-tile" style={{ "--i": 1 } as CSSProperties}>
          <rect x="240" y="24" width="216" height="120" rx="14" fill="#0f766e" />
          <text x="256" y="54" fontSize="11" fill="#99f6e4" fontWeight="700">HEALTHCARE</text>
          <text x="256" y="88" fontSize="22" fill="#fff" fontWeight="700">99.2%</text>
          <text x="256" y="112" fontSize="12" fill="#ccfbf1">preflight pass rate</text>
        </g>
        <g className="lp-cust-tile" style={{ "--i": 2 } as CSSProperties}>
          <rect x="24" y="160" width="140" height="116" rx="14" fill="#fff" stroke="#ccece7" />
          <text x="40" y="196" fontSize="11" fill="#0f766e" fontWeight="700">SAAS</text>
          <text x="40" y="228" fontSize="18" fill="#0f172a" fontWeight="700">MCP</text>
          <text x="40" y="250" fontSize="11" fill="#64748b">agent-native ops</text>
        </g>
        <g className="lp-cust-tile" style={{ "--i": 3 } as CSSProperties}>
          <rect x="180" y="160" width="140" height="116" rx="14" fill="#fff" stroke="#ccece7" />
          <text x="196" y="196" fontSize="11" fill="#0f766e" fontWeight="700">FINANCE</text>
          <text x="196" y="228" fontSize="18" fill="#0f172a" fontWeight="700">0</text>
          <text x="196" y="250" fontSize="11" fill="#64748b">silent data loss</text>
        </g>
        <g className="lp-cust-tile" style={{ "--i": 4 } as CSSProperties}>
          <rect x="336" y="160" width="120" height="116" rx="14" fill="#042f2e" />
          <text x="352" y="196" fontSize="11" fill="#5eead4" fontWeight="700">PROOF</text>
          <text x="352" y="228" fontSize="16" fill="#ecfdf5" fontWeight="700">Checksum</text>
          <text x="352" y="250" fontSize="11" fill="#99f6e4">every load</text>
        </g>
      </svg>
    );
  }

  if (kind === "pricing") {
    return (
      <svg className={`${cls} lp-mkt-illustration--pricing`} viewBox="0 0 480 300" role="img" aria-label="Plan scale visualization">
        <defs>
          <linearGradient id="lp-price-bg" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#07111f" />
            <stop offset="100%" stopColor="#0f2744" />
          </linearGradient>
        </defs>
        <rect width="480" height="300" rx="18" fill="url(#lp-price-bg)" />
        <text x="40" y="42" fontSize="12" fill="#94a3b8" fontWeight="650">PLAN SCALE</text>
        {[
          { x: 40, w: 120, h: 140, label: "Starter", sub: "Free", featured: false },
          { x: 180, w: 140, h: 180, label: "Team", sub: "Custom", featured: true },
          { x: 340, w: 100, h: 160, label: "Ent", sub: "Custom", featured: false },
        ].map((t, i) => (
          <g key={t.label} className="lp-price-bar" style={{ "--i": i } as CSSProperties}>
            <rect
              x={t.x}
              y={260 - t.h}
              width={t.w}
              height={t.h}
              rx="12"
              fill={t.featured ? "#0d9488" : "#1e293b"}
              stroke={t.featured ? "#5eead4" : "#334155"}
              strokeWidth={t.featured ? 2 : 1}
            />
            <text x={t.x + t.w / 2} y={260 - t.h + 28} textAnchor="middle" fontSize="13" fill={t.featured ? "#ecfdf5" : "#e2e8f0"} fontWeight="700">
              {t.label}
            </text>
            <text x={t.x + t.w / 2} y={260 - t.h + 52} textAnchor="middle" fontSize="11" fill={t.featured ? "#99f6e4" : "#94a3b8"}>
              {t.sub}
            </text>
          </g>
        ))}
        <text x="40" y="286" fontSize="11" fill="#64748b">Start free · Scale when pipelines &amp; SSO matter</text>
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
