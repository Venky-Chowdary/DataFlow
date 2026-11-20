/**
 * The run summary: the identity every counted load has to close,
 * read = landed + held + skipped, on one recorded run.
 */
/* Every figure comes from one recorded run in the hosted
   workspace: Orders Postgres (demo) public.sales_orders into Warehouse
   Snowflake (demo) DW_DEMO.SALES_ORDERS_REPORTING. The screenshots in
   WORKSPACE_SHOT were captured on the same run. */
export const RUN = {
  read: "5,100",
  written: "5,100",
  held: "0",
  skipped: "0",
  counted: "5,100",
  columns: "11 of 11",
  checks: "23",
  checksDetail: "17 passed, 6 not applicable",
};

export function RunLedger() {
  return (
    <figure className="mk-ledger" aria-labelledby="mk-ledger-title">
      <div className="mk-ledger-head">
        <p id="mk-ledger-title" className="mk-ledger-title">
          Run summary
        </p>
        <p className="mk-ledger-route">
          <code>public.sales_orders</code>
          <span> to </span>
          <code>DW_DEMO.SALES_ORDERS_REPORTING</code>
        </p>
      </div>
      <dl className="mk-ledger-rows">
        <div>
          <dt>Read from PostgreSQL</dt>
          <dd>{RUN.read}</dd>
        </div>
        <div>
          <dt>Written to Snowflake</dt>
          <dd>{RUN.written}</dd>
        </div>
        <div className="is-held">
          <dt>Held in quarantine</dt>
          <dd>{RUN.held}</dd>
        </div>
        <div className="is-total">
          <dt>Counted by Snowflake after the write</dt>
          <dd>{RUN.counted}</dd>
        </div>
      </dl>
      <p className="mk-ledger-identity" aria-label={`${RUN.read} rows read equals ${RUN.counted} in the destination, plus ${RUN.held} held, plus ${RUN.skipped} skipped`}>
        <span>
          <b>{RUN.read}</b> read
        </span>
        <i aria-hidden>=</i>
        <span>
          <b>{RUN.counted}</b> landed
        </span>
        <i aria-hidden>+</i>
        <span>
          <b>{RUN.held}</b> held
        </span>
        <i aria-hidden>+</i>
        <span>
          <b>{RUN.skipped}</b> skipped
        </span>
      </p>
      <dl className="mk-ledger-meta">
        <div>
          <dt>Columns mapped</dt>
          <dd>{RUN.columns}</dd>
        </div>
        <div>
          <dt>Checks before the write</dt>
          <dd>
            {RUN.checks} <span>({RUN.checksDetail})</span>
          </dd>
        </div>
        <div>
          <dt>Comparison</dt>
          <dd>Full checksum, every row</dd>
        </div>
      </dl>
      <div className="mk-ledger-verdict">
        <span className="mk-verdict mk-verdict--proven">Proven</span>
        <p>Every row was compared and none differs.</p>
      </div>
    </figure>
  );
}

