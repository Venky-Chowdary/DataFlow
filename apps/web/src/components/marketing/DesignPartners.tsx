/**
 * The design partner programme. There are no named partners to show, so this
 * block says what the programme offers and asks, and how to apply.
 */
export function DesignPartners({ headingId = "mk-partners-title" }: { headingId?: string }) {
  return (
    <section className="mk-section mk-partners" aria-labelledby={headingId}>
      <div className="mk-wrap mk-partners-grid">
        <header>
          <h2 id={headingId}>Design partner programme</h2>
          <p>
            We are working with a small number of data teams on real migrations and syncs. Partners
            shape what we build next.
          </p>
          <a
            className="mk-btn mk-btn--primary mk-btn--lg"
            href="mailto:sales@datawrap.io?subject=Design%20partner%20application"
          >
            Apply to be a design partner
          </a>
          <p className="mk-caption">Applications go to sales@datawrap.io.</p>
        </header>
        <div className="mk-partners-terms">
          <div>
            <h3>What partners get</h3>
            <ul className="mk-facts">
              <li>Early access to new connectors and features</li>
              <li>A direct line to the engineers who build Datawrap</li>
              <li>Influence on the roadmap</li>
            </ul>
          </div>
          <div>
            <h3>What we ask</h3>
            <ul className="mk-facts">
              <li>A real migration or sync to run on Datawrap</li>
              <li>Regular, candid feedback on what works and what does not</li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  );
}
