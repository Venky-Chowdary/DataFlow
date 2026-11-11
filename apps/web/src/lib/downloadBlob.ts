/**
 * Save a fetched Blob as a file: the one download path the evidence exports use
 * (signed proof pack, migration certificate, parity evidence pack).
 *
 * The anchor is attached before the click so Firefox honours it, and the object
 * URL is revoked a moment later rather than at once, so the save starts first.
 */
export function downloadBlob(filename: string, blob: Blob): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.rel = "noopener";
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1500);
}
