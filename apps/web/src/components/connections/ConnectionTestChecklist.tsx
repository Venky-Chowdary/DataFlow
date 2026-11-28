/**
 * Connection test as a step checklist (UI pass 2, item 4): Reach host, Sign
 * in, Read catalog, Privileges, each passed / failed / not run / checked
 * elsewhere, in words and glyph. Below it, the grants this engine needs as
 * SQL a DBA can copy (shown open when the test stopped at a permission).
 */
import { useState } from "react";
import { connectionTestSteps, grantFixes, type TestStepState } from "../../lib/connectionTestSteps";
import type { IconName } from "../icons/iconMap";
import { DtIcon } from "../DtIcon";

const STATE_ICON: Record<TestStepState, IconName> = {
  passed: "success",
  failed: "error",
  not_run: "minus",
  not_checked: "verdict-not-checked",
  unknown: "warning",
};
const STATE_WORD: Record<TestStepState, string> = {
  passed: "Passed",
  failed: "Failed",
  not_run: "Not run",
  not_checked: "Not checked by Test",
  unknown: "Failed",
};

function CopySql({ sql, label }: { sql: string; label: string }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1800);
    } catch {
      setCopied(false);
    }
  };
  return (
    <button type="button" className="df2-ctest-copy" onClick={() => void copy()} aria-label={`Copy ${label}`}>
      <DtIcon name={copied ? "check" : "copy"} size={14} chrome="product" />
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

export function ConnectionTestChecklist({
  result,
  connection,
}: {
  result: { success: boolean; code?: string | null; message?: string };
  connection: { type: string; username?: string; database?: string; schema?: string; warehouse?: string; role?: string };
}) {
  const steps = connectionTestSteps(result);
  const fixes = grantFixes({ ...connection, message: result.message });
  const grantFailed = steps.some((s) => (s.id === "privileges" || s.id === "catalog") && s.state === "failed");
  return (
    <div className="df2-ctest">
      <ol className="df2-ctest-steps" aria-label="Connection test steps">
        {steps.map((s) => (
          <li key={s.id} className="df2-ctest-step" data-state={s.state}>
            <span className="df2-ctest-icon" aria-hidden="true">
              <DtIcon name={STATE_ICON[s.state]} size={16} chrome="product" />
            </span>
            <span className="df2-ctest-label">{s.label}</span>
            <span className="df2-ctest-word">{STATE_WORD[s.state]}</span>
            <span className="df2-ctest-detail">{s.detail}</span>
          </li>
        ))}
      </ol>
      {fixes.length > 0 && (
        <details className="df2-ctest-grants" open={grantFailed}>
          <summary>{grantFailed ? "Grants to fix this" : "Grants a transfer needs"}</summary>
          <p className="df2-ctest-grants-note">Run as an administrator of the database. Values you typed are filled in; replace anything in angle brackets.</p>
          {fixes.map((fix) => (
            <div key={fix.purpose} className="df2-ctest-grant">
              <div className="df2-ctest-grant-head">
                <span>{fix.purpose}</span>
                <CopySql sql={fix.sql} label={`grants ${fix.purpose.toLowerCase()}`} />
              </div>
              <pre className="df2-ctest-sql"><code>{fix.sql}</code></pre>
              {fix.note && <p className="df2-ctest-grants-note">{fix.note}</p>}
            </div>
          ))}
        </details>
      )}
    </div>
  );
}
