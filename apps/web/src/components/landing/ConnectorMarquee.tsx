import { ConnectorIcon } from "../../app/brand-icons";

const ROW_A = [
  "postgresql", "snowflake", "mysql", "mongodb", "bigquery", "redshift",
  "amazon_s3", "json", "csv___tsv", "dynamodb", "elasticsearch", "redis",
  "oracle", "sql_server", "salesforce", "stripe",
];

const ROW_B = [
  "hubspot", "shopify", "kafka", "parquet", "excel", "azure_blob",
  "google_cloud_storage", "teradata", "db2", "sap", "netsuite", "workday",
  "snowflake", "postgresql", "bigquery", "mysql",
];

function MarqueeRow({ ids, reverse }: { ids: string[]; reverse?: boolean }) {
  const track = [...ids, ...ids];
  return (
    <div className={`lp-marquee-row ${reverse ? "reverse" : ""}`}>
      <div className="lp-marquee-track">
        {track.map((id, i) => (
          <div key={`${id}-${i}`} className="lp-marquee-item" title={id}>
            <ConnectorIcon id={id} size={26} />
          </div>
        ))}
      </div>
    </div>
  );
}

export function ConnectorMarquee() {
  return (
    <section className="lp-marquee-band" aria-label="Supported integrations">
      <p className="lp-marquee-eyebrow">Trusted connector catalog</p>
      <MarqueeRow ids={ROW_A} />
      <MarqueeRow ids={ROW_B} reverse />
    </section>
  );
}
