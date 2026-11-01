import { useEffect, useState } from "react";
import QRCode from "qrcode";

/**
 * Render a value (usually an otpauth:// URI) as a scannable QR code.
 * Generated entirely in the browser — the setup secret is never sent to a
 * third-party QR service.
 */
export function QrCodeImage({ value, label, size = 180 }: { value: string; label: string; size?: number }) {
  const [src, setSrc] = useState("");

  useEffect(() => {
    let cancelled = false;
    void QRCode.toDataURL(value, { width: size * 2, margin: 1, errorCorrectionLevel: "M" }).then((url) => {
      if (!cancelled) setSrc(url);
    });
    return () => {
      cancelled = true;
    };
  }, [value, size]);

  if (!src) {
    return <div className="df2-qr-placeholder" style={{ width: size, height: size }} aria-hidden="true" />;
  }
  return <img src={src} width={size} height={size} alt={label} className="df2-qr" />;
}
