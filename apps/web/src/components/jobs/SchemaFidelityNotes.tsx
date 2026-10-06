import { readSchemaFidelity, type JobEvidenceCarrier, type SchemaFidelityView } from "../../lib/jobEvidence";

function fidelityHeadline(view: SchemaFidelityView): string {
  const parts: string[] = [];
  if (view.unsupported > 0) {
    parts.push(
      `${view.unsupported} not carried on CREATE TABLE`,
    );
  }
  if (view.unknown > 0) {
    parts.push(`${view.unknown} unmeasured`);
  }
  if (!parts.length) parts.push("findings recorded");
  return `Schema fidelity: ${parts.join(" · ")}`;
}

function findingLine(item: SchemaFidelityView["items"][number]): string {
  const name = item.name && item.name !== "*" ? ` ${item.name}` : "";
  const why = item.reason ? ` — ${item.reason}` : "";
  return `${item.aspect || "aspect"}${name}: ${item.status}${why}`;
}

/**
 * Create-new schema fidelity that is not a quiet carried or measured-absent
 * row. The engine already stamps this on destination_summary.schema_fidelity.
 */
export function SchemaFidelityNotes({ job }: { job: JobEvidenceCarrier | null | undefined }) {
  const view = readSchemaFidelity(job);
  if (!view || view.items.length === 0) return null;
  return (
    <section className="df2-result-warnings-block" role="status" aria-label="Schema fidelity">
      <p className="df2-result-warnings-note">{fidelityHeadline(view)}</p>
      <ul className="df2-result-warnings">
        {view.items.map((item, index) => (
          <li key={`${item.aspect}-${item.name}-${index}`}>{findingLine(item)}</li>
        ))}
      </ul>
    </section>
  );
}
