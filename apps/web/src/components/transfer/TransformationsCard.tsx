import { useState } from "react";
import { DtIcon } from "../DtIcon";
import { formatSeconds } from "../../lib/phaseProfile";
import type { TransformationsReport, TransformModelResult } from "../../lib/types";

const STATUS_TONE: Record<string, string> = {
  success: "is-ok",
  partial: "is-warn",
  failed: "is-danger",
  skipped: "is-muted",
};

function modelIcon(status: string): "check" | "alert" | "minus" {
  if (status === "success") return "check";
  if (status === "failed") return "alert";
  return "minus";
}

/**
 * Post-load SQL models built after the rows landed.
 *
 * The distinction this card has to make unmistakable: a transformation failure
 * does not mean the transfer failed. The data is at the destination; the
 * derived models are stale. Conflating the two would send an operator hunting
 * for lost rows that were never lost.
 */
export function TransformationsCard({ report }: { report?: TransformationsReport | null }) {
  const [expanded, setExpanded] = useState<string>("");

  if (!report || (!report.ran && report.status !== "failed")) return null;

  const models: TransformModelResult[] = report.projects.flatMap((p) => p.models || []);
  const failedTests = models.flatMap((m) =>
    (m.tests || []).filter((t) => !t.passed && t.severity === "error")
  );

  return (
    <section
      className={`df2-result-transforms ${STATUS_TONE[report.status] || ""}`}
      aria-label="Post-load transformations"
    >
      <header>
        <DtIcon name="git-branch" size={14} />
        <strong>Transformations</strong>
        <span>{report.message}</span>
      </header>

      {models.length > 0 && (
        <ul className="df2-result-transform-list">
          {models.map((model) => {
            const open = expanded === model.name;
            const modelFailedTests = (model.tests || []).filter((t) => !t.passed);
            return (
              <li
                key={model.name}
                className={`df2-result-transform ${STATUS_TONE[model.status] || ""}`}
              >
                <button
                  type="button"
                  className="df2-result-transform-head"
                  onClick={() => setExpanded(open ? "" : model.name)}
                  aria-expanded={open}
                >
                  <DtIcon name={modelIcon(model.status)} size={13} />
                  <span className="df2-result-transform-name">{model.name}</span>
                  <span className="df2-result-transform-mat">{model.materialization}</span>
                  {model.rows_affected >= 0 && (
                    <span className="df2-result-transform-rows">
                      {model.rows_affected.toLocaleString()} rows
                    </span>
                  )}
                  <span className="df2-result-transform-time">
                    {formatSeconds(model.seconds)}
                  </span>
                  <DtIcon name={open ? "chevron-up" : "chevron-down"} size={13} />
                </button>

                {model.error && <p className="df2-result-transform-error">{model.error}</p>}

                {modelFailedTests.length > 0 && (
                  <ul className="df2-result-transform-tests">
                    {modelFailedTests.map((test, i) => (
                      <li
                        key={`${test.test_type}-${test.column}-${i}`}
                        className={test.severity === "error" ? "is-danger" : "is-warn"}
                      >
                        <DtIcon name="alert" size={12} />
                        <span>{test.message || `${test.test_type} failed`}</span>
                        <span className="df2-result-transform-sev">{test.severity}</span>
                      </li>
                    ))}
                  </ul>
                )}

                {open && (
                  <div className="df2-result-transform-detail">
                    {model.relation && (
                      <p>
                        <span>Relation</span>
                        <code>{model.relation}</code>
                      </p>
                    )}
                    {model.strategy && (
                      <p>
                        <span>Strategy</span>
                        <code>{model.strategy}</code>
                      </p>
                    )}
                    {model.sql && (
                      <pre className="df2-result-transform-sql">
                        <code>{model.sql}</code>
                      </pre>
                    )}
                  </div>
                )}
              </li>
            );
          })}
        </ul>
      )}

      {failedTests.length > 0 && (
        <p className="df2-result-transform-foot">
          {failedTests.length} blocking data test
          {failedTests.length === 1 ? "" : "s"} failed. The loaded tables are
          unaffected — re-run the models after fixing the source data.
        </p>
      )}
    </section>
  );
}
