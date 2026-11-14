# Studio end-to-end tests

Playwright projects are defined in `../playwright.config.ts`. CI runs them in
the `studio-ui`, `studio-ui-next` and `plan-conflict-e2e` jobs of the PR
Workflow (`.github/workflows/ci.yml`) through `.github/actions/studio-surface`.

## Visual baselines

Pixel baselines live next to their spec in `*.spec.ts-snapshots/` and are
compared with `maxDiffPixelRatio: 0`. They are rendered on the CI runner image
(fonts and anti-aliasing differ elsewhere), so never regenerate them on a
laptop.

When a change is meant to move pixels:

1. Push the change with `[refresh-visual-baselines]` in the head commit
   message, on a `release/**` branch.
2. The `Refresh visual baselines` workflow renders the `wizard` and
   `studio-next` projects with `--update-snapshots=changed` and commits only
   the PNGs that differ, as `github-actions[bot]`. A cancelled run commits
   nothing.
3. Review every changed PNG against the one it replaces before merging. Name
   the cause of each difference. A baseline that captured a bug (for example,
   a control painted over another) is replaced; anything you cannot explain
   is a regression, not a baseline.
4. A bot push starts no workflow. The next human push runs the PR Workflow
   against the refreshed baselines.

## Which CI jobs a push runs

The `changes` job (`.github/scripts/ci_backend_reuse.py`) looks for the newest
ancestor of the PR head whose backend checks all passed. If every file changed
since then is web-only (`apps/web/`, `packages/design-system/`, the root npm
manifests, `docs/`, top-level Markdown) and the base branch gained no backend
change, the backend jobs are skipped and the `Required PR gate` accepts the
earlier green results. The studio and plan-conflict jobs always run. Anything
else, including any change under `.github/`, runs everything.
