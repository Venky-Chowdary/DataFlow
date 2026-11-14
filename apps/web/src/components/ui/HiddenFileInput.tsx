import { forwardRef } from "react";

/**
 * One owner for browser file pick. Visually hidden (not `hidden` / display:none)
 * so a `<label htmlFor>` opens the picker without a JS `.click()`, and
 * Playwright `setInputFiles` can target the input without unhiding it.
 */
export const HiddenFileInput = forwardRef<
  HTMLInputElement,
  {
    id: string;
    accept?: string;
    disabled?: boolean;
    /** -1 when a menu item (not a label) opens the picker: no invisible tab stop. */
    tabIndex?: number;
    onChange: React.ChangeEventHandler<HTMLInputElement>;
  }
>(function HiddenFileInput({ id, accept, disabled, tabIndex, onChange }, ref) {
  return (
    <input
      id={id}
      ref={ref}
      type="file"
      accept={accept}
      disabled={disabled}
      tabIndex={tabIndex}
      onChange={onChange}
      className="df2-sr-only"
    />
  );
});

HiddenFileInput.displayName = "HiddenFileInput";
