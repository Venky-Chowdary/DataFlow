/**
 * The product icon map: one name, one meaning, one lucide glyph.
 *
 * Rules (see the icon guide in DtIcon.tsx):
 * - Every name here means one thing. Pick the name for what the control does,
 *   not for the picture (`copy`, not `layers`).
 * - `ai` (Sparkles) is reserved for AI / Pilot. `verify` (ShieldCheck) is
 *   reserved for verification and gates. `live` (Activity) is reserved for
 *   live telemetry; refresh, test and schedules have their own names.
 * - Transform operators that lucide lacks are drawn with `createLucideIcon`
 *   so they share lucide's grid, stroke and class names.
 */
import {
  Activity,
  ArrowDownUp,
  ArrowLeft,
  ArrowLeftRight,
  ArrowRight,
  ArrowUpRight,
  AppWindow,
  Ban,
  Bell,
  Binary,
  BookOpen,
  Cable,
  Calendar,
  CalendarClock,
  CaseSensitive,
  Check,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronUp,
  CircleAlert,
  CircleCheck,
  CircleDot,
  CircleHelp,
  CircleX,
  Clock,
  Cloud,
  Code,
  Columns3,
  Combine,
  Copy,
  CopyMinus,
  Cpu,
  Database,
  Download,
  Ellipsis,
  Eye,
  EyeOff,
  File,
  FileText,
  FoldVertical,
  Folder,
  FolderOpen,
  Funnel,
  Gauge,
  GitBranch,
  Globe,
  Grid3x3,
  Hash,
  Info,
  KeyRound,
  Layers,
  LayoutDashboard,
  Library,
  Link2,
  ListChecks,
  ListCollapse,
  ListOrdered,
  ListTree,
  LoaderCircle,
  Lock,
  LogOut,
  Maximize2,
  Menu,
  Merge,
  Minus,
  Moon,
  Package,
  PanelLeft,
  PanelRight,
  Pause,
  Pencil,
  Play,
  Plus,
  PlugZap,
  RectangleEllipsis,
  Redo2,
  RefreshCw,
  RemoveFormatting,
  Replace,
  Rows3,
  ScanSearch,
  Scissors,
  ScissorsLineDashed,
  ScrollText,
  Search,
  Send,
  Server,
  Settings,
  Shapes,
  Shield,
  ShieldCheck,
  Sigma,
  Signpost,
  SlidersHorizontal,
  Space,
  Sparkles,
  Split,
  Sun,
  Table2,
  TextCursorInput,
  Timer,
  Trash2,
  TrendingUp,
  TriangleAlert,
  Type,
  Undo2,
  Ungroup,
  Upload,
  Users,
  X,
  Zap,
  createLucideIcon,
  type LucideIcon,
} from "lucide-react";

/* Operators lucide has no glyph for: same 24 grid, drawn as lucide nodes. */
const AbsoluteValue = createLucideIcon("absolute-value", [
  ["path", { d: "M6 4v16", key: "a" }],
  ["path", { d: "M18 4v16", key: "b" }],
  ["path", { d: "m9 15 6-6", key: "c" }],
]);
const Clamp = createLucideIcon("clamp", [
  ["path", { d: "M4 6h16", key: "a" }],
  ["path", { d: "M4 18h16", key: "b" }],
  ["path", { d: "m9 10 3-3 3 3", key: "c" }],
  ["path", { d: "m9 14 3 3 3-3", key: "d" }],
]);
const RoundNumber = createLucideIcon("round-number", [
  ["circle", { cx: "12", cy: "12", r: "8", key: "a" }],
  ["path", { d: "M12 8v4l3 2", key: "b" }],
]);

export const ICONS = {
  /* ── Navigation and shell ─────────────────────────────── */
  dashboard: LayoutDashboard,
  transfer: ArrowLeftRight,
  connectors: Cable,
  contract: ScrollText,
  jobs: ListChecks,
  schedule: CalendarClock,
  transform: Shapes,
  library: Library,
  help: CircleHelp,
  settings: Settings,
  "sign-out": LogOut,
  menu: Menu,
  "panel-left": PanelLeft,
  "panel-right": PanelRight,
  bell: Bell,
  users: Users,
  key: KeyRound,
  globe: Globe,
  server: Server,
  cpu: Cpu,
  sun: Sun,
  moon: Moon,

  /* ── Data entities ────────────────────────────────────── */
  cloud: Cloud,
  database: Database,
  file: File,
  document: FileText,
  folder: Folder,
  "folder-open": FolderOpen,
  table: Table2,
  "git-branch": GitBranch,

  /* ── Actions ──────────────────────────────────────────── */
  copy: Copy,
  plus: Plus,
  minus: Minus,
  edit: Pencil,
  trash: Trash2,
  upload: Upload,
  download: Download,
  send: Send,
  play: Play,
  pause: Pause,
  refresh: RefreshCw,
  "test-connection": PlugZap,
  search: Search,
  inspect: ScanSearch,
  filter: Funnel,
  sliders: SlidersHorizontal,
  expand: Maximize2,
  undo: Undo2,
  redo: Redo2,
  more: Ellipsis,
  check: Check,
  x: X,
  eye: Eye,
  "eye-off": EyeOff,
  lock: Lock,
  code: Code,
  map: Columns3,
  "arrow-right": ArrowRight,
  "arrow-left": ArrowLeft,
  "arrow-up-right": ArrowUpRight,
  "chevron-left": ChevronLeft,
  "chevron-right": ChevronRight,
  "chevron-down": ChevronDown,
  "chevron-up": ChevronUp,

  /* ── Status (pair with text; colour comes from the parent) ── */
  info: Info,
  success: CircleCheck,
  warning: TriangleAlert,
  error: CircleX,
  attention: CircleAlert,
  verify: ShieldCheck,
  security: Shield,
  live: Activity,
  ai: Sparkles,
  clock: Clock,
  timer: Timer,
  calendar: Calendar,
  trend: TrendingUp,
  speed: Gauge,
  zap: Zap,
  docs: BookOpen,
  spinner: LoaderCircle,

  /* ── Transform operators (recipe canvas) ──────────────── */
  cast: Type,
  trim: Scissors,
  mask: RectangleEllipsis,
  hash: Hash,
  dedupe: CopyMinus,
  split: Split,
  unnest: ListTree,
  join: Merge,
  aggregate: Sigma,
  sort: ArrowDownUp,
  rank: ListOrdered,
  window: AppWindow,
  router: Signpost,
  union: Combine,
  normalizer: Ungroup,
  mapplet: Package,
  flatten: FoldVertical,
  rename: TextCursorInput,
  concat: Link2,
  collapse: ListCollapse,
  case: CaseSensitive,
  strip: RemoveFormatting,
  pad: Space,
  replace: Replace,
  default: CircleDot,
  null: Ban,
  round: RoundNumber,
  truncate: ScissorsLineDashed,
  absolute: AbsoluteValue,
  clamp: Clamp,
  number: Binary,
  pivot: Grid3x3,
  unpivot: Rows3,

  /* ── Deprecated names: one meaning each, kept so older call sites render.
        Prefer the name in the comment for new code. ── */
  /** = verify */
  gate: ShieldCheck,
  /** = security (Contracts use `contract`, Sign out uses `sign-out`) */
  shield: Shield,
  /** = ai */
  sparkle: Sparkles,
  /** = live */
  activity: Activity,
  /** = warning */
  alert: TriangleAlert,
  /** = docs (Library uses `library`, Help uses `help`) */
  book: BookOpen,
  /** = inspect */
  scan: ScanSearch,
  /** A stack of layers. Not for copy, map, transform or continue. */
  layers: Layers,
} satisfies Record<string, LucideIcon>;

export type IconName = keyof typeof ICONS;

export const ICON_NAMES = Object.freeze(Object.keys(ICONS)) as readonly IconName[];

export function isIconName(name: string): name is IconName {
  return Object.prototype.hasOwnProperty.call(ICONS, name);
}

/** The size scale: 14 dense UI and chips, 16 buttons and menus, 20 nav and
 *  headings. 24 and 32 are display sizes for empty states only. */
export const ICON_SIZES = [14, 16, 20, 24, 32] as const;
export type IconSize = (typeof ICON_SIZES)[number];

export function snapIconSize(size: number): IconSize {
  if (size <= 14) return 14;
  if (size <= 17) return 16;
  if (size <= 21) return 20;
  if (size <= 26) return 24;
  return 32;
}

/** One stroke for every icon: lucide's default, matching direct lucide use. */
export const ICON_STROKE = 2;
