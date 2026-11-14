import { CopyIdChip } from "./ui/CopyIdChip";

/**
 * The support reference of a failure, as its own copyable chip (ERR-40).
 *
 * It sits outside the clamped / ellipsised message text, so a long message can
 * never clip it: the whole reference is visible and one click copies it.
 */
export function ErrorReferenceChip({ reference, className = "" }: { reference: string; className?: string }) {
  if (!reference) return null;
  return (
    <CopyIdChip
      id={reference}
      label="Reference"
      className={`df2-error-reference ${className}`.trim()}
    />
  );
}
