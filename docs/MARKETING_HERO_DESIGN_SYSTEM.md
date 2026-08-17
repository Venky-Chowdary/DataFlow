# Marketing hero design system

Why this document exists: every public page carried the same hero composition — a dark
slab, an aurora glow, and a white browser-chrome card containing a small UI mock. Two
pages shipped the *identical* artwork (`solutions/migrations` reused Transfer Studio's
mock; `solutions/warehouse` reused Job Theater's). A visitor could not tell the pages
apart from the fold, and a fake UI screenshot is the weakest possible argument for a
product whose claim is *evidence*.

The fix is not new gradients. It is a drawing system: one geometric idea per page, drawn
to scale on a shared canvas, in the existing `--df-*` token palette.

## Rules

1. **One idea per page, and it must be the page's own argument.** If the drawing could be
   moved to another page without changing its meaning, it is wrong.
2. **Draw the mechanism, not a screenshot of the mechanism.** No browser chrome, no fake
   window dots, no invented metrics presented as product UI.
3. **Geometry over decoration.** Every shape is on the 20px canvas grid; no blobs, no
   free-floating aurora, no gradient that does not describe depth or direction.
4. **Nothing is claimed that the engine does not do.** Labels come from real gate names,
   real modes, real refusal wording.
5. **Fluid by construction.** A single `viewBox` (1000×640) scales without breakpoints.
   Below 720px the frame switches to the drawing's declared focal region so the labels
   stay legible instead of shrinking with the canvas.
6. **One motion per hero, and it stops.** Motion illustrates direction (a packet, a
   waterline, a sweep) and is disabled under `prefers-reduced-motion`.

## Shared canvas

`HeroArtFrame` owns the parts that must never diverge: the viewBox (1000×640 in a column
beside the copy, or the 1700×440 band when a drawing runs the full hero width), the
engineering grid, the ink field, the directional light, the teal/amber accents drawn from
tokens, the stroke scale (1 / 1.5 / 2), the label type scale (11/13/16/22), the caption
line, and the `role="img"` + `aria-label` contract. Art files provide only geometry.

## Per-page concepts

| Page | Idea | Composition |
| --- | --- | --- |
| Home | **The proof loop.** A write is not the end; the destination reread closes the circle. | Left→right lane through the engine into the destination, then a return arc *underneath* flowing back to a seal. The only closed shape in the system. |
| Transfer Studio | **The gate comb.** Nine blades a row must pass. | Nine vertical blades of stepped height, rows threading through as horizontal filaments; one blade lit amber holds a row at G4. |
| Job Theater | **The run spine.** Every phase is on the record. | A spine of phase nodes with a quarantine tray branching down and a proof seal terminating the line. |
| Pipelines | **The cadence dial.** Recurrence is a rhythm, not a DAG. | A tick ring; each tick drops a run bar radially outward; one tick amber for drift. |
| Datawrap Pilot | **Sentence → governed action.** Language resolves into a staged, confirmable act. | A typed line breaks into tokens that assemble a resolved action card behind a lock that only Confirm opens. |
| MCP | **One door.** Human and agent enter through the same policy wall. | Two callers on the left, a wall with a single aperture, the engine on the right; credential glyphs stop dead at the wall. |
| Query | **The read-only lens.** Inspect without touching. | A lens over a typed cell grid magnifying values; write arrows deflect off the rim. |
| Migrations | **The cutover runway.** Two schemas that were never 1:1. | Two combs of different tooth counts joined by mapping ribbons; checkpoint flags along the runway; a checksum seal at the far end. |
| Warehouse loading | **Typed delivery into layers.** | Stacked warehouse slabs in light perspective; typed columns descending with their type badges intact — never as strings. |
| Sync / CDC | **The watermark.** Only what is new moves. | A table with a rising waterline; rows above the line in motion, rows below at rest; a drift marker off to one side. |
| Enterprise | **The governed perimeter.** | Concentric rings — identity, RBAC, audit — around a workspace core; region pin on the outer ring. |
| Security | **The vault wall.** Secrets do not travel with the data. | A key lattice that terminates at the vault face while the data lane passes outside it. |
| Integrations | **The lattice.** Connectors are a graph, not a wheel. | Nodes on the measured grid with typed lanes between families (file, OLTP, warehouse, object store). |
| Pricing | **The plan ladder on one rail.** Tiers climb on cadence and security; the proof engine is not an upgrade. | Three rising step plates joined by dashed lifts, all standing on a single teal rail labelled `in every plan`; a struck-through `monthly active rows` states the meter we do not bill on. |
| Customers | **Named evidence, and one plate we refuse to fill.** | Three plates carrying the recorded case counts (`48` / `43` / `14`) with their scope, the measured engines, a reread seal — and an empty dashed plate marked `logo wall · no invented marks`. |
| Contact | **The pilot route.** A request becomes a scoped pilot on your own stack and ends in an artifact you keep. | A full-width band under the copy and the form: the caller's request plate, a dashed branch dead-ended at a cross (`no nurture queue`), then checkpoints `01 discovery` → `02 scoped pilot` → `03 reconcile`, terminating in a sealed `artifact you keep` plate. Annotated `skip_preflight is never set from this form`. |

**Contact** is the one band drawing: the form is the only control that matters, so the
route runs the full width *beneath* it rather than competing for the right column, and
below 960px — where band type would fall under 7px — the drawing is replaced by the same
three steps as a text list. **Help / docs** still carries no schematic: it leads with the
document index it is, with real product screenshots. **Privacy** and **Terms** are legal
text behind a thin band. All use the same ink field, typography, and pill treatment.

## Background and typography

The hero band itself is rebuilt rather than recoloured: a single deep-ink field with a
fine 32px engineering grid, one directional wash from the upper left (the light source
the art is drawn against), and a soft plate behind the artwork so it reads as a mounted
schematic. Display type moves to a fluid `clamp()` scale with tightened tracking; the
lead is capped at 62 characters for a measured line; the copy column and the art column
share one baseline.

Under 960px the art keeps its aspect and moves below the copy; under 560px the frame
switches to its compact form (caption below, labels promoted one step) so nothing scales
below legibility, and the page has no horizontal overflow at 390px.

## Verification

Measured in a real browser, not eyeballed. For every public route at 1920 / 1440 / 1280 /
1024 / 390: `document.scrollWidth == clientWidth` (no horizontal overflow), the artwork
rects inside the hero bounds, the H1 and both CTAs visible. Every `<text>` node in every
hero drawing is checked against the active `viewBox` and pairwise against the others:
zero out-of-bounds and zero overlaps across all fifteen drawn routes. Under
`prefers-reduced-motion: reduce` every packet is `display: none` and no sweep animates.
Each drawing exposes `role="img"`, an `aria-label` naming the mechanism, and a
caption that says when the drawing is schematic.
