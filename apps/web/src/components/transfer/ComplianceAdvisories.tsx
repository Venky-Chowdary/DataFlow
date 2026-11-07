import { DtIcon } from "../DtIcon";
import type { ComplianceAdvisory } from "../../lib/types";

/**
 * Non-blocking compliance notes from preflight (QA D7 review 3): a numeric
 * column with 1-4 card numbers in a small sample is not a hold, but it must not
 * pass silently either.
 */
export function ComplianceAdvisories({ advisories }: { advisories?: ComplianceAdvisory[] | null }) {
  const rows = (advisories || []).filter((a) => a && a.note);
  if (!rows.length) return null;
  return (
    <div className="df2-vd-enterprise-trust" role="note" aria-label="Compliance advisories">
      <DtIcon name="shield" size={15} />
      <div>
        <strong>Compliance advisory · not blocking</strong>
        <ul>
          {rows.map((a) => (
            <li key={`${a.column}:${a.category}`}>
              <code>{a.column}</code> — {a.category === "possible_pci_pan" ? "possible card numbers" : a.category}: {a.note}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
