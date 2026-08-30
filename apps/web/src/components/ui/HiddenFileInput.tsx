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
    onChange: React.ChangeEventHandler<HTMLInputElement>;
  }
>(function HiddenFileInput({ id, accept, disabled, onChange }, ref) {
  return (
    <input
      id={id}
      ref={ref}
      type="file"
      accept={accept}
      disabled={disabled}
      onChange={onChange}
      className="df2-sr-only"
    />
  );
});

HiddenFileInput.displayName = "HiddenFileInput";
