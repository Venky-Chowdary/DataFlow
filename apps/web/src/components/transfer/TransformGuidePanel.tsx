import { DtIcon } from "../DtIcon";

/**
 * What Transform (pre-load) is, stated on the screen where it is used.
 *
 * The step's rules are not guessable from its controls — that it runs on the
 * read and never touches the source, that removing rows here is not data loss
 * but a named ledger term, that relational work is deliberately absent, and
 * that the recipe carries an identity Execute is held to. Operators were left
 * to infer all of it; it is written down here instead.
 */
export function TransformGuidePanel({ postLoadOnly }: { postLoadOnly: string[] }) {
  return (
    <div className="df2-xform-guide" role="region" aria-label="How Transform (pre-load) works">
      <ol className="df2-xform-guide-list">
        <li>
          <span className="df2-xform-guide-num">1</span>
          <div>
            <strong>It runs on the read, before anything is written.</strong>
            <p>
              Each step is applied row by row as the source is streamed, in the order listed.
              Your file, table or sheet is never modified — this is a recipe, not an edit.
            </p>
          </div>
        </li>
        <li>
          <span className="df2-xform-guide-num">2</span>
          <div>
            <strong>Map, Validate and the writer all see the transformed columns.</strong>
            <p>
              That is why this step comes before Map: carriers, narrowing contracts and the
              destination DDL are decided from the columns you leave here.
            </p>
          </div>
        </li>
        <li>
          <span className="df2-xform-guide-num">3</span>
          <div>
            <strong>A row this step removes is counted as removed, not as loss.</strong>
            <p>
              Filtered and diverted rows land in the conservation ledger under their own terms,
              so the run still proves every source row is accounted for.
            </p>
          </div>
        </li>
        <li>
          <span className="df2-xform-guide-num">4</span>
          <div>
            <strong>Only row-local, deterministic work is allowed in flight.</strong>
            <p>
              No clock, no randomness, no SQL — the same row always yields the same result.
              {postLoadOnly.length > 0 && (
                <>
                  {" "}
                  Not available here and refused by name: <code>{postLoadOnly.join(", ")}</code> —
                  they need the whole table, so they belong to a post-load transform.
                </>
              )}
            </p>
          </div>
        </li>
        <li>
          <span className="df2-xform-guide-num">5</span>
          <div>
            <strong>Hash identity is how Gate-8 aligns rows without a natural PK.</strong>
            <p>
              File loads like flights CSV have repeating dates. Add{" "}
              <code>hash_identity</code> on the columns that make a row unique —
              Datawrap writes a stable SHA-256 key and uses it for dest read-back.
              Airbyte/Fivetran hash-all-columns or require a warehouse PK; this is
              the operator-chosen composite.
            </p>
          </div>
        </li>
        <li>
          <span className="df2-xform-guide-num">6</span>
          <div>
            <strong>The recipe is pinned by identity.</strong>
            <p>
              The <code>recipe</code> hash shown above is approved with the plan and re-checked
              before Execute: if the steps change afterwards the run is refused rather than
              silently running a different recipe. Every step also reports what it did — cells
              changed, nulls introduced, rows removed — on the sample and again on the real run.
            </p>
          </div>
        </li>
      </ol>
      <p className="df2-xform-guide-foot">
        <DtIcon name="alert" size={14} />
        Charts and previews below are a <strong>sample</strong>. Validate re-checks every row of the
        population before the destination is touched.
      </p>
    </div>
  );
}
