/** Official-style brand icons for connector catalog */

export const BrandIcons: Record<string, React.FC<{ size?: number }>> = {
  postgresql: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M23.56 14.68c-.18-.09-1.6-.87-1.88-.97-.29-.1-.5-.14-.71.15-.21.28-.82 1.03-1 1.24-.19.21-.37.24-.69.08-.31-.16-1.32-.49-2.52-1.56-.93-.83-1.56-1.86-1.74-2.17-.18-.31-.02-.48.14-.64.14-.14.31-.37.47-.56.16-.18.21-.31.31-.52.1-.21.05-.39-.03-.55-.08-.16-.71-1.71-.97-2.34-.26-.63-.52-.54-.71-.55h-.61c-.21 0-.55.08-.84.39-.29.31-1.1 1.08-1.1 2.62 0 1.55 1.13 3.04 1.29 3.25.16.21 2.22 3.39 5.38 4.76.75.32 1.34.52 1.8.66.76.24 1.45.21 1.99.13.61-.09 1.88-.77 2.14-1.51.26-.74.26-1.38.18-1.51-.08-.13-.29-.21-.6-.36z" fill="#336791"/>
      <path d="M28.78 9.64c-.72-1.77-1.92-3.31-3.42-4.49-1.5-1.18-3.27-1.97-5.16-2.32-1.89-.35-3.86-.25-5.7.31-1.84.56-3.51 1.55-4.83 2.89-1.33 1.33-2.33 2.98-2.92 4.79-.59 1.81-.77 3.75-.52 5.65.25 1.9.92 3.72 1.97 5.31 1.05 1.59 2.46 2.91 4.1 3.87l.13 3.35c.02.47.28.89.69 1.13.41.23.91.24 1.33.02l3.05-1.6c1.69.23 3.42.11 5.05-.35 1.63-.46 3.13-1.27 4.39-2.37 1.26-1.1 2.25-2.47 2.89-4.01.64-1.53.92-3.2.8-4.86-.11-1.66-.61-3.27-1.45-4.68-.24-.4-.51-.79-.8-1.16-.14-.18-.29-.36-.45-.53l-.15-.05z" stroke="#336791" strokeWidth="1.5"/>
    </svg>
  ),
  mysql: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <circle cx="16" cy="16" r="12" fill="#00758F"/>
      <path d="M12 11h2v10h-2zm6 0h2v10h-2z" fill="white"/>
    </svg>
  ),
  mongodb: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M16.62 4.41c-.36-.53-.57-.53-.73-.79-.16-.25-.3-.52-.3-.52s-.05.75-.18 1.05c-.51 1.17-3.49 4.36-4.51 8.26-.58 2.22-.22 4.12.73 5.85 1.32 2.4 2.62 3.34 2.77 4.86.03.32.05.64.05.97 0 .12.18.12.22 0 .15-1.52.38-2.61.86-3.63.68-1.44 4.57-4.76 4.97-8.91.27-2.77-1.66-5.57-3.88-7.14z" fill="#00ED64"/>
    </svg>
  ),
  snowflake: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M16 4v24M4 16h24M7.76 7.76l16.48 16.48M24.24 7.76L7.76 24.24" stroke="#29B5E8" strokeWidth="2" strokeLinecap="round"/>
      <circle cx="16" cy="16" r="3" fill="#29B5E8"/>
    </svg>
  ),
  dynamodb: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <rect x="4" y="6" width="24" height="20" rx="3" fill="#4053D6"/>
      <path d="M10 12h12v2H10v-2zm0 4h12v2H10v-2zm0 4h8v2h-8v-2z" fill="white"/>
    </svg>
  ),
  elasticsearch: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <circle cx="16" cy="16" r="12" fill="#FEC514"/>
      <path d="M12 11h8l-2 10h-4l2-10z" fill="#343741"/>
    </svg>
  ),
  clickhouse: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <rect x="4" y="4" width="24" height="24" rx="4" fill="#FFCC01"/>
      <path d="M10 10h4v12h-4V10zm8 0h4v12h-4V10z" fill="#1A1A1A"/>
    </svg>
  ),
  redshift: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M16 4l12 7v10l-12 7L4 21V11l12-7z" fill="#8C4FFF"/>
    </svg>
  ),
  bigquery: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M6 8v16l10 4V12L6 8z" fill="#4386FA"/>
      <path d="M16 12v16l10-4V8l-10 4z" fill="#3B78E7"/>
    </svg>
  ),
  s3: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M16 4l10 3v18l-10 3-10-3V7l10-3z" fill="#569A31"/>
      <path d="M16 4v24l10-3V7l-10-3z" fill="#4B8A29"/>
    </svg>
  ),
  salesforce: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M13.3 8.2c1.1-1.2 2.7-1.9 4.4-1.9 2.1 0 4 1.1 5.1 2.7.9-.4 1.9-.6 2.9-.6 4 0 7.3 3.3 7.3 7.3s-3.3 7.3-7.3 7.3c-.5 0-1-.1-1.5-.2-.9 1.6-2.6 2.7-4.6 2.7-1 0-2-.3-2.8-.8-1 1.4-2.6 2.3-4.4 2.3-2.1 0-3.9-1.2-4.8-2.9-.5.1-1 .2-1.5.2-3.3 0-6-2.7-6-6s2.7-6 6-6c.8 0 1.5.2 2.2.4.8-2.4 3.1-4.2 5.7-4.2.3-.1.6-.1.9-.1l-.6-.2z" fill="#00A1E0"/>
    </svg>
  ),
  kafka: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <circle cx="16" cy="16" r="12" fill="#231F20"/>
      <circle cx="16" cy="11" r="2.5" fill="white"/>
      <circle cx="12" cy="18" r="2.5" fill="white"/>
      <circle cx="20" cy="18" r="2.5" fill="white"/>
    </svg>
  ),
  redis: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M28 16.5c0 2.5-5.37 4.5-12 4.5S4 19 4 16.5 9.37 12 16 12s12 2 12 4.5z" fill="#A41E11"/>
      <path d="M28 13c0 2.5-5.37 4.5-12 4.5S4 15.5 4 13s5.37-4.5 12-4.5S28 10.5 28 13z" fill="#D82C20"/>
    </svg>
  ),
  csv: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M6 4h12l8 8v16a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2z" fill="#22A565"/>
      <path d="M8 16h3v2H8v-2zm5 0h3v2h-3v-2zm5 0h3v2h-3v-2z" fill="white"/>
    </svg>
  ),
  json: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <path d="M6 4h12l8 8v16a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2z" fill="#F7B93E"/>
    </svg>
  ),
  generic_sql: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <ellipse cx="16" cy="10" rx="10" ry="4" fill="#2D65F7"/>
      <path d="M6 10v12c0 2.2 4.5 4 10 4s10-1.8 10-4V10" stroke="#2D65F7" strokeWidth="2"/>
      <path d="M6 14c0 2.2 4.5 4 10 4s10-1.8 10-4" stroke="#2D65F7" strokeWidth="2"/>
      <path d="M6 18c0 2.2 4.5 4 10 4s10-1.8 10-4" stroke="#2D65F7" strokeWidth="2"/>
    </svg>
  ),
  default: ({ size = 32 }) => (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden>
      <rect x="4" y="4" width="24" height="24" rx="4" fill="#78716c"/>
      <path d="M12 12h8M12 16h8M12 20h5" stroke="white" strokeWidth="2" strokeLinecap="round"/>
    </svg>
  ),
};

export function ConnectorIcon({ id, size = 32 }: { id: string; size?: number }) {
  const Icon = BrandIcons[id] || BrandIcons.default;
  return <Icon size={size} />;
}
