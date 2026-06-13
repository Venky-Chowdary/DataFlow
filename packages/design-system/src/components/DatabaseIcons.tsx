import type { SVGProps } from "react";

type IconProps = SVGProps<SVGSVGElement> & { size?: number };

const iconBase = (size: number): SVGProps<SVGSVGElement> => ({
  width: size,
  height: size,
  viewBox: "0 0 24 24",
  fill: "none",
  xmlns: "http://www.w3.org/2000/svg",
});

export function PostgresIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <path
        d="M12 2C8.13 2 5 4.46 5 7.5v9c0 3.04 3.13 5.5 7 5.5s7-2.46 7-5.5v-9C19 4.46 15.87 2 12 2z"
        fill="#336791"
      />
      <ellipse cx="12" cy="7.5" rx="7" ry="3.5" fill="#fff" opacity="0.3" />
      <path
        d="M12 11c-3.87 0-7-1.57-7-3.5V16c0 1.93 3.13 3.5 7 3.5s7-1.57 7-3.5V7.5c0 1.93-3.13 3.5-7 3.5z"
        fill="#fff"
        opacity="0.15"
      />
      <path
        d="M15.5 10c-.83 0-1.5.67-1.5 1.5v4c0 .83.67 1.5 1.5 1.5s1.5-.67 1.5-1.5v-4c0-.83-.67-1.5-1.5-1.5z"
        fill="#fff"
      />
    </svg>
  );
}

export function SnowflakeIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <circle cx="12" cy="12" r="10" fill="#29B5E8" />
      <path
        d="M12 4v16M4 12h16M6.34 6.34l11.32 11.32M17.66 6.34L6.34 17.66"
        stroke="#fff"
        strokeWidth="2"
        strokeLinecap="round"
      />
      <circle cx="12" cy="12" r="2" fill="#fff" />
      <circle cx="12" cy="6" r="1.5" fill="#fff" />
      <circle cx="12" cy="18" r="1.5" fill="#fff" />
      <circle cx="6" cy="12" r="1.5" fill="#fff" />
      <circle cx="18" cy="12" r="1.5" fill="#fff" />
    </svg>
  );
}

export function MongoIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <path
        d="M12 2C9 2 7 5 7 9c0 4 2 8 4 11l1 3 1-3c2-3 4-7 4-11 0-4-2-7-5-7z"
        fill="#13AA52"
      />
      <path
        d="M12 6c-.5 0-1 .5-1 1.5 0 1 .5 1.5 1 1.5s1-.5 1-1.5c0-1-.5-1.5-1-1.5z"
        fill="#fff"
      />
      <path d="M11.5 10h1v8h-1z" fill="#B8C4C2" />
    </svg>
  );
}

export function MySQLIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <circle cx="12" cy="12" r="10" fill="#00758F" />
      <path
        d="M7 8l2 8h1l1.5-6 1.5 6h1l2-8h-1.5l-1 5-1.5-5h-1l-1.5 5-1-5H7z"
        fill="#F29111"
      />
    </svg>
  );
}

export function SQLServerIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <rect x="3" y="3" width="18" height="18" rx="2" fill="#CC2927" />
      <path
        d="M7 8h4v2H7v4h4v2H7V8zM13 8h4v2h-4v4h4v2h-4V8z"
        fill="#fff"
        opacity="0.9"
      />
    </svg>
  );
}

export function BigQueryIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <path
        d="M4 6v12c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2H6c-1.1 0-2 .9-2 2z"
        fill="#4285F4"
      />
      <path d="M8 10h2v6H8zM11 8h2v8h-2zM14 12h2v4h-2z" fill="#fff" />
    </svg>
  );
}

export function RedisIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <path
        d="M21.68 12.6c-.6.4-3.72 1.6-4.4 1.96-.68.36-1.04.36-1.6.04-.56-.32-3.88-1.6-4.48-1.92-.6-.32-.56-.56.04-.88.6-.32 3.92-1.64 4.4-1.88.48-.24 1.16-.28 1.64-.04.48.24 3.88 1.56 4.4 1.84.52.28.6.48 0 .88z"
        fill="#A41E11"
      />
      <path
        d="M21.68 9.8c-.6.4-3.72 1.6-4.4 1.96-.68.36-1.04.36-1.6.04-.56-.32-3.88-1.6-4.48-1.92-.6-.32-.56-.56.04-.88.6-.32 3.92-1.64 4.4-1.88.48-.24 1.16-.28 1.64-.04.48.24 3.88 1.56 4.4 1.84.52.28.6.48 0 .88z"
        fill="#D82C20"
      />
      <path
        d="M21.68 7c-.6.4-3.72 1.6-4.4 1.96-.68.36-1.04.36-1.6.04-.56-.32-3.88-1.6-4.48-1.92-.6-.32-.56-.56.04-.88.6-.32 3.92-1.64 4.4-1.88.48-.24 1.16-.28 1.64-.04.48.24 3.88 1.56 4.4 1.84.52.28.6.48 0 .88z"
        fill="#FF4438"
      />
    </svg>
  );
}

export function DatabricksIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <path d="M12 2L2 7l10 5 10-5-10-5z" fill="#FF3621" />
      <path d="M2 12l10 5 10-5-10-5-10 5z" fill="#FF3621" opacity="0.7" />
      <path d="M2 17l10 5 10-5-10-5-10 5z" fill="#FF3621" opacity="0.5" />
    </svg>
  );
}

export function OracleIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <rect x="2" y="6" width="20" height="12" rx="6" fill="#F80000" />
      <text
        x="12"
        y="14"
        fontSize="6"
        fontWeight="bold"
        fill="#fff"
        textAnchor="middle"
        dominantBaseline="middle"
      >
        O
      </text>
    </svg>
  );
}

export function FileIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <path
        d="M6 2h8l6 6v12c0 1.1-.9 2-2 2H6c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2z"
        fill="#4CAF50"
      />
      <path d="M14 2v6h6" fill="#81C784" />
      <path d="M8 13h8M8 16h5" stroke="#fff" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

export function ApiIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <rect x="3" y="3" width="18" height="18" rx="3" fill="#9C27B0" />
      <path
        d="M7 9h2v6H7zM10 9h2v6h-2zM15 9h2v6h-2zM13 9h.5v6H13z"
        fill="#fff"
        opacity="0.9"
      />
    </svg>
  );
}

export function TransferIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <circle cx="6" cy="12" r="4" fill="var(--df-brand-orange, #ff4d00)" />
      <circle cx="18" cy="12" r="4" fill="var(--df-brand-mint, #00b87a)" />
      <path
        d="M10 10h4l-1.5-2M10 14h4l1.5 2"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export function SparkIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <path
        d="M12 2l2.4 7.4H22l-6.2 4.5 2.4 7.4L12 16.8l-6.2 4.5 2.4-7.4L2 9.4h7.6L12 2z"
        fill="#E25A1C"
      />
    </svg>
  );
}

export function CloudIcon({ size = 24, ...props }: IconProps) {
  return (
    <svg {...iconBase(size)} {...props}>
      <path
        d="M19.35 10.04A7.49 7.49 0 0012 4C9.11 4 6.6 5.64 5.35 8.04A5.994 5.994 0 000 14c0 3.31 2.69 6 6 6h13c2.76 0 5-2.24 5-5 0-2.64-2.05-4.78-4.65-4.96z"
        fill="#2196F3"
      />
    </svg>
  );
}

const DB_ICONS: Record<string, typeof PostgresIcon> = {
  postgresql: PostgresIcon,
  postgres: PostgresIcon,
  snowflake: SnowflakeIcon,
  mongodb: MongoIcon,
  mongo: MongoIcon,
  mysql: MySQLIcon,
  sqlserver: SQLServerIcon,
  mssql: SQLServerIcon,
  bigquery: BigQueryIcon,
  redis: RedisIcon,
  databricks: DatabricksIcon,
  oracle: OracleIcon,
  file: FileIcon,
  csv: FileIcon,
  api: ApiIcon,
  spark: SparkIcon,
  cloud: CloudIcon,
};

export function DatabaseIcon({
  type,
  size = 24,
  ...props
}: IconProps & { type: string }) {
  const Icon = DB_ICONS[type.toLowerCase()] ?? PostgresIcon;
  return <Icon size={size} {...props} />;
}

export { DB_ICONS };
