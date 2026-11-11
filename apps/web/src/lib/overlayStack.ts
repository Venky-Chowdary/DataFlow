/**
 * Open modal overlays (drawers, dialogs) in the order they opened.
 *
 * Escape and focus trapping act on the topmost one only. Open order is the
 * truth: DOM order is not, because a drawer can mount inside the app shell,
 * which precedes every <body>-portaled overlay however recently it opened.
 */
const stack: HTMLElement[] = [];

export function pushOverlay(element: HTMLElement): () => void {
  stack.push(element);
  return () => {
    const index = stack.lastIndexOf(element);
    if (index >= 0) stack.splice(index, 1);
  };
}

export function isTopOverlay(element: HTMLElement | null): boolean {
  return Boolean(element) && stack[stack.length - 1] === element;
}
