/** Window over a long list. Used once the schema object list passes 200. */

export const VIRTUALIZE_AFTER = 200;

export interface VirtualRange {
  start: number;
  end: number;
  offsetTop: number;
  totalHeight: number;
}

export function virtualRange(
  count: number,
  scrollTop: number,
  viewportHeight: number,
  rowHeight: number,
  overscan = 6,
): VirtualRange {
  if (count <= 0 || rowHeight <= 0 || viewportHeight <= 0) {
    return { start: 0, end: 0, offsetTop: 0, totalHeight: Math.max(0, count * Math.max(rowHeight, 0)) };
  }
  const start = Math.max(0, Math.floor(Math.max(0, scrollTop) / rowHeight) - overscan);
  const visible = Math.ceil(viewportHeight / rowHeight) + overscan * 2;
  const end = Math.min(count, start + visible);
  return {
    start,
    end,
    offsetTop: start * rowHeight,
    totalHeight: count * rowHeight,
  };
}

export function shouldVirtualize(count: number, expanded: boolean): boolean {
  return !expanded && count > VIRTUALIZE_AFTER;
}
