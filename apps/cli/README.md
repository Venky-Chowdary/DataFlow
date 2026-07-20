# DataFlow GitOps CLI

Plan / apply / export / validate for `dataflow.yaml`.

```bash
# From repo root
export PYTHONPATH=apps/api:apps/cli

python -m dataflow_cli validate -f dataflow.yaml
python -m dataflow_cli plan -f dataflow.yaml --local
python -m dataflow_cli apply -f dataflow.yaml --local --yes
# CD / staging: refuse schedules unless contract_id is SIGNED
python -m dataflow_cli apply -f dataflow.staging.yaml --local --yes --require-signed-contracts
python -m dataflow_cli export --local -o dataflow.yaml

# Against a running API
python -m dataflow_cli plan -f dataflow.yaml --api http://127.0.0.1:8001/api/v1
python -m dataflow_cli apply -f dataflow.yaml --api http://127.0.0.1:8001/api/v1 --yes
```

Or via npm: `npm run dataflow -- plan -f dataflow.yaml --local`

CI runs the same checks on every PR (`gitops` job) against [examples/gitops/dataflow.yaml](../../examples/gitops/dataflow.yaml). Locally: `npm run test:gitops`.

**Honesty**
- Imported contracts are **DRAFT** until signed.
- CDC delivery remains **at-least-once** upsert — GitOps does not change that.
