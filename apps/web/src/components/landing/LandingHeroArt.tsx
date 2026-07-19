/** Azure-style right-panel data design — animated schema flow, not photos. */

export function LandingHeroArt() {
  return (
    <div className="lp-hero-art" aria-hidden>
      <div className="lp-hero-art-stage">
        <svg className="lp-hero-art-svg" viewBox="0 0 560 520" role="img">
          <defs>
            <linearGradient id="lp-art-stage" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="#0f766e" />
              <stop offset="42%" stopColor="#0d9488" />
              <stop offset="100%" stopColor="#134e4a" />
            </linearGradient>
            <linearGradient id="lp-art-glass" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" stopColor="rgba(255,255,255,0.96)" />
              <stop offset="100%" stopColor="rgba(240,253,250,0.92)" />
            </linearGradient>
            <linearGradient id="lp-art-wire" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor="#5eead4" />
              <stop offset="100%" stopColor="#99f6e4" />
            </linearGradient>
            <filter id="lp-art-glow" x="-20%" y="-20%" width="140%" height="140%">
              <feGaussianBlur stdDeviation="10" result="b" />
              <feMerge>
                <feMergeNode in="b" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          {/* Deep teal stage — Azure-weight right panel */}
          <rect x="0" y="0" width="560" height="520" rx="28" fill="url(#lp-art-stage)" />
          <circle cx="460" cy="80" r="120" fill="rgba(255,255,255,0.06)" />
          <circle cx="80" cy="420" r="100" fill="rgba(15,23,42,0.12)" />
          <path
            d="M40 180 C120 120, 200 240, 280 180 S440 120, 520 200"
            fill="none"
            stroke="rgba(255,255,255,0.08)"
            strokeWidth="40"
          />

          {/* Source */}
          <g className="lp-hero-art-float lp-hero-art-float--a">
            <rect x="36" y="56" width="168" height="100" rx="18" fill="url(#lp-art-glass)" />
            <circle cx="64" cy="84" r="10" fill="#0d9488" />
            <text x="84" y="89" fontSize="12" fontWeight="700" fill="#0f172a">PostgreSQL</text>
            <text x="52" y="118" fontSize="11" fill="#64748b">orders · live</text>
            <text x="52" y="138" fontSize="18" fontWeight="750" fill="#0f766e">12,480</text>
          </g>

          {/* Destination */}
          <g className="lp-hero-art-float lp-hero-art-float--b">
            <rect x="356" y="56" width="168" height="100" rx="18" fill="url(#lp-art-glass)" />
            <circle cx="384" cy="84" r="10" fill="#0f766e" />
            <text x="404" y="89" fontSize="12" fontWeight="700" fill="#0f172a">Snowflake</text>
            <text x="372" y="118" fontSize="11" fill="#64748b">FACT_ORDERS</text>
            <text x="372" y="138" fontSize="18" fontWeight="750" fill="#0f766e">ready</text>
          </g>

          {/* Flow wire */}
          <path
            className="lp-hero-art-path"
            d="M204 106 H268 Q292 106 292 130 V200 Q292 224 316 224 H356"
            fill="none"
            stroke="url(#lp-art-wire)"
            strokeWidth="3.5"
            strokeLinecap="round"
          />
          <circle className="lp-hero-art-particle" r="6" fill="#ecfdf5" filter="url(#lp-art-glow)" />

          {/* Mapping panel */}
          <g className="lp-hero-art-float lp-hero-art-float--c">
            <rect x="88" y="200" width="384" height="200" rx="20" fill="url(#lp-art-glass)" />
            <text x="280" y="236" textAnchor="middle" fontSize="13" fontWeight="750" fill="#0f172a">
              Semantic map · 96% confidence
            </text>
            <rect x="116" y="256" width="328" height="36" rx="10" fill="#f0fdfa" />
            <text x="136" y="279" fontSize="12" fill="#0f766e">order_amt</text>
            <text x="250" y="279" fontSize="12" fill="#94a3b8">→</text>
            <text x="280" y="279" fontSize="12" fontWeight="700" fill="#0f172a">payment_amount</text>
            <rect x="116" y="302" width="328" height="36" rx="10" fill="#f8fafc" />
            <text x="136" y="325" fontSize="12" fill="#0f766e">cust_id</text>
            <text x="250" y="325" fontSize="12" fill="#94a3b8">→</text>
            <text x="280" y="325" fontSize="12" fontWeight="700" fill="#0f172a">customer_key</text>
            <rect x="116" y="350" width="140" height="28" rx="8" fill="#ccfbf1" />
            <text x="186" y="369" textAnchor="middle" fontSize="11" fontWeight="700" fill="#0f766e">8 / 8 gates</text>
            <rect x="272" y="350" width="172" height="28" rx="8" fill="#ecfdf5" />
            <text x="358" y="369" textAnchor="middle" fontSize="11" fontWeight="700" fill="#059669">checksum OK</text>
          </g>

          {/* Proof chip */}
          <g className="lp-hero-art-float lp-hero-art-float--d">
            <rect x="120" y="432" width="320" height="48" rx="14" fill="#042f2e" />
            <circle cx="152" cy="456" r="7" fill="#34d399" />
            <text x="280" y="461" textAnchor="middle" fontSize="13" fontWeight="650" fill="#ecfdf5">
              Reconciled · 12,480 = 12,480
            </text>
          </g>
        </svg>
      </div>
    </div>
  );
}
