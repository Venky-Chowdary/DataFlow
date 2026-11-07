/**
 * Prints the Map risk-contract verdict for the pairs on stdin (one "src|tgt" per
 * line) so it can be diffed against `services.type_system.is_lossy_coercion` —
 * a route the engine refuses must be a route Map offers a contract on.
 */
import { readFileSync } from "node:fs";
import { mappingRequiresRiskAck } from "../src/lib/mapping";

const lines = readFileSync(0, "utf8")
  .split(/\r?\n/)
  .filter((l: string) => l.trim());
for (const line of lines) {
  const [src, tgt] = line.split("|");
  const verdict = mappingRequiresRiskAck({
    source: "c",
    target: "c",
    confidence: 0.95,
    approved: false,
    inferredType: src,
    destType: tgt,
  });
  console.log(`${src}|${tgt}|${verdict}`);
}
