import { ConnectorIcon } from "../../app/brand-icons";
import { CONNECTOR_CLAIMS, MARKETING_STACK } from "../../lib/provenEvidence";

/** Connector families from the CI evidence file: proven names in bold. */
export function ConnectorFamilies() {
  return (
    <>
      <table className="mk-families">
        <caption>Connector families</caption>
        <thead>
          <tr>
            <th scope="col">Family</th>
            <th scope="col">Members</th>
            <th scope="col">Status</th>
          </tr>
        </thead>
        <tbody>
          {MARKETING_STACK.map((row) => {
            const family = CONNECTOR_CLAIMS.families.find((f) => f.family === row.family);
            return (
              <tr key={row.family}>
                <th scope="row">{row.family}</th>
                <td>
                  <ul className="mk-family-members">
                    {(family?.items ?? []).map((item) => (
                      <li key={item.driver} className={item.proven ? "is-proven" : undefined}>
                        <ConnectorIcon id={item.driver} size={16} />
                        {item.name}
                        {item.proven ? <span className="mk-sr-only"> (proven in CI)</span> : null}
                      </li>
                    ))}
                  </ul>
                </td>
                <td>{row.badge}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <p className="mk-caption">Names marked CI have a green CI run behind them; the others are implemented.</p>
    </>
  );
}
