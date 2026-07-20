# DataFlow CLI (GitOps)

## Install / run

```bash
# Repo root
export PYTHONPATH=apps/api:apps/cli
python -m dataflow_cli --help

# Or
npm run dataflow -- --help
```

## Commands

| Command | Purpose | Exit |
|---|---|---|
| `validate -f FILE` | Shape check only | `1` on bad kind / empty |
| `plan -f FILE [--local\|--api]` | Dry-run create/update/skip | `0` |
| `apply -f FILE --yes […]` | Apply (needs `--yes`) | `2` without `--yes`; `1` if apply failures |
| `export [-o FILE] […]` | Write fleet `dataflow.yaml` | `0` |

## CI sketch

GitHub Actions job ``gitops`` (see `.github/workflows/ci.yml`) runs on every PR:

```bash
python -m dataflow_cli validate -f examples/gitops/dataflow.yaml
python -m dataflow_cli plan -f examples/gitops/dataflow.yaml --local
pytest apps/api/tests/test_gitops_plan_apply.py apps/api/tests/test_dataflow_cli.py
```

Local equivalent: `npm run test:gitops`

Against a live API (CD / staging):

```bash
# Plan always
python -m dataflow_cli plan -f examples/gitops/dataflow.staging.yaml \
  --api "$DATAFLOW_API_BASE" --token "$DATAFLOW_API_TOKEN"

# Apply only with signed-contract enforcement (schedules must reference SIGNED contracts)
python -m dataflow_cli apply -f examples/gitops/dataflow.staging.yaml \
  --api "$DATAFLOW_API_BASE" --token "$DATAFLOW_API_TOKEN" \
  --yes --require-signed-contracts
```

CI: job ``gitops`` validates both example manifests. Optional job ``gitops-cd-staging``
plans against ``vars.DATAFLOW_STAGING_API_BASE`` and applies on main when
``vars.DATAFLOW_STAGING_APPLY=1`` (token: ``secrets.DATAFLOW_STAGING_API_TOKEN``).

Contracts import as **DRAFT**. CDC remains **at-least-once**.
