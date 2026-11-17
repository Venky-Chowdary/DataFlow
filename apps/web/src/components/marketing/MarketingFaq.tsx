import { useState } from "react";

/**
 * Single-column question row. Airbyte and Fivetran use this instead of a
 * card grid: the question is a button, the answer opens under it.
 */
export function MarketingFaq({
  items,
}: {
  items: { q: string; a: string; where?: string }[];
}) {
  const [open, setOpen] = useState(0);

  return (
    <div className="lp-faq">
      {items.map((item, i) => {
        const on = open === i;
        return (
          <div key={item.q} className={on ? "is-open" : ""}>
            <h3>
              <button type="button" aria-expanded={on} onClick={() => setOpen(on ? -1 : i)}>
                <span>{item.q}</span>
                <i aria-hidden />
              </button>
            </h3>
            <div className="lp-faq-panel">
              <div>
                <p>{item.a}</p>
                {item.where ? <span>{item.where}</span> : null}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
