/**
 * Product icon — a thin wrapper over lucide-react with one explicit map.
 *
 * Icon guide (the whole app follows it):
 * - One library: lucide-react, through this component or `ICONS`.
 * - One stroke: 2 (lucide's default, also used where lucide is imported directly).
 * - One size scale: 14 dense rows and chips, 16 buttons and menus, 20 nav and
 *   headings; 24/32 only for empty-state art. Other sizes snap to the scale.
 * - One meaning per name (see components/icons/iconMap.ts): `copy` copies,
 *   `refresh` reloads, `ai` is only AI/Pilot, `verify` is only verification.
 * - Arrows and checks are icons, never "→", "←", "✓" or "▾" in label text.
 * - An unknown name renders nothing (and warns in development); it never
 *   falls back to another glyph.
 */
import { ICONS, ICON_STROKE, isIconName, snapIconSize, type IconName } from "./icons/iconMap";

export type { IconName } from "./icons/iconMap";

const warned = new Set<string>();

function warnUnknown(name: string) {
  const env = (import.meta as { env?: { DEV?: boolean; MODE?: string } }).env;
  if (!env?.DEV || env.MODE === "test" || warned.has(name)) return;
  warned.add(name);
  console.warn(`DtIcon: unknown icon "${name}" — add it to components/icons/iconMap.ts`);
}

export function DtIcon({
  name,
  size = 20,
  chrome = "nav",
  className,
}: {
  /** A name from `ICONS` (components/icons/iconMap.ts). */
  name: IconName;
  size?: number;
  /**
   * `nav` keeps the historical `.dt-nav-icon` wrapper span that existing
   * layouts size against; `product` renders the bare svg.
   */
  chrome?: "nav" | "product";
  className?: string;
}) {
  if (!isIconName(name)) {
    warnUnknown(name);
    return null;
  }
  const Icon = ICONS[name];
  const px = snapIconSize(size);
  const svg = (
    <Icon size={px} strokeWidth={ICON_STROKE} aria-hidden="true" focusable="false" className={className} />
  );
  if (chrome === "product") return svg;
  return <span className="dt-nav-icon">{svg}</span>;
}
