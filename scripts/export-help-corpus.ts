/**
 * Export the shipped operator help articles as a retrieval corpus the API can read.
 *
 * The help text has one owner (`apps/web/src/lib/helpDocs.ts`) and the API image
 * only carries `apps/api`, so Pilot cannot answer a product question from the
 * article the operator is reading unless that text is emitted into the API tree.
 * The JSON is generated, never hand-edited; `npm run help-corpus:check` fails when
 * it drifts from the articles.
 */

import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

import {
  listAllHelpDocs,
  type HelpDocArticle,
  type HelpDocSection,
} from "../apps/web/src/lib/helpDocs";

export interface HelpCorpusChunk {
  id: string;
  doc_id: string;
  doc_slug: string;
  doc_title: string;
  category: string;
  section_id: string;
  section_title: string;
  text: string;
}

export interface HelpCorpus {
  version: 1;
  generated_from: string;
  chunk_count: number;
  chunks: HelpCorpusChunk[];
}

const OUT_PATH = resolve(
  import.meta.dirname,
  "../apps/api/src/ai/rag/help_corpus.json",
);

function sectionText(section: HelpDocSection): string {
  const parts: string[] = [section.title, section.body];
  for (const step of section.steps ?? []) parts.push(step);
  for (const step of section.workflow ?? []) {
    parts.push(step.title, step.body);
    if (step.pin) parts.push(`Where: ${step.pin}`);
    if (step.tip) parts.push(`Tip: ${step.tip}`);
  }
  if (section.tip) parts.push(`Tip: ${section.tip}`);
  if (section.code) parts.push(section.code);
  if (section.figure?.caption) parts.push(section.figure.caption);
  return parts
    .map((p) => String(p).trim())
    .filter(Boolean)
    .join("\n");
}

export function buildHelpCorpus(articles: HelpDocArticle[] = listAllHelpDocs()): HelpCorpus {
  const chunks: HelpCorpusChunk[] = [];
  for (const article of articles) {
    for (const section of article.sections) {
      const text = sectionText(section);
      if (!text) continue;
      chunks.push({
        id: `${article.id}#${section.id}`,
        doc_id: article.id,
        doc_slug: article.slug,
        doc_title: article.title,
        category: article.category,
        section_id: section.id,
        section_title: section.title,
        text,
      });
    }
  }
  return {
    version: 1,
    generated_from: "apps/web/src/lib/helpDocs.ts",
    chunk_count: chunks.length,
    chunks,
  };
}

export function serializeHelpCorpus(corpus: HelpCorpus): string {
  return `${JSON.stringify(corpus, null, 2)}\n`;
}

export function helpCorpusDigest(corpus: HelpCorpus): string {
  return createHash("sha256").update(serializeHelpCorpus(corpus)).digest("hex");
}

function main(): void {
  const corpus = buildHelpCorpus();
  if (corpus.chunk_count === 0) {
    throw new Error("help corpus is empty — refusing to write a corpus Pilot cannot answer from");
  }
  mkdirSync(dirname(OUT_PATH), { recursive: true });
  writeFileSync(OUT_PATH, serializeHelpCorpus(corpus), "utf-8");
  process.stdout.write(
    `help corpus: ${corpus.chunk_count} chunks → ${OUT_PATH}\n`,
  );
}

if (process.argv[1] && process.argv[1].endsWith("export-help-corpus.ts")) {
  main();
}
