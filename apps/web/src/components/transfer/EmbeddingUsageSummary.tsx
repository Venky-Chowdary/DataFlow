type EmbeddingUsage = Record<string, unknown>;

function finiteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

export function EmbeddingUsageSummary({
  destinationSummary,
}: {
  destinationSummary: Record<string, unknown> | null | undefined;
}) {
  const rawUsage = destinationSummary?.embedding_usage;
  if (!rawUsage || typeof rawUsage !== "object" || Array.isArray(rawUsage)) return null;
  const usage = rawUsage as EmbeddingUsage;
  const tokens = finiteNumber(usage.input_tokens ?? usage.tokens);
  const calls = finiteNumber(usage.calls);
  const cost = finiteNumber(usage.estimated_cost_usd);
  if (tokens === null && calls === null && cost === null) return null;

  const currency = cost === null
    ? "Unavailable"
    : new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        minimumFractionDigits: 4,
        maximumFractionDigits: 6,
      }).format(cost);

  return (
    <section className="df2-result-proof df2-embedding-usage" aria-label="Embedding usage estimate">
      <header className="df2-result-proof-head">
        <strong>Embedding usage</strong>
        <span className="df2-badge">Estimate</span>
      </header>
      <dl className="df2-result-proof-dl">
        <div>
          <dt>Input tokens</dt>
          <dd>
            {tokens === null
              ? "Unavailable"
              : Math.trunc(tokens).toLocaleString()}
            {usage.token_count_estimated === true ? " (estimate)" : ""}
          </dd>
        </div>
        <div>
          <dt>Provider calls</dt>
          <dd>{calls === null ? "Unavailable" : Math.trunc(calls).toLocaleString()}</dd>
        </div>
        <div>
          <dt>Estimated cost</dt>
          <dd>{currency} · estimate</dd>
        </div>
      </dl>
    </section>
  );
}
