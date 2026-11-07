# Enterprise service topology

Principal-architect reading of how this product actually runs, which drawbacks
block a multi-replica deployment, and which processes should be separate.
Capability evidence stays in `docs/ENTERPRISE_ARCHITECTURE_AUDIT.md`. This
document is the process and state plan. It does not claim a soak, an SLA, or
a compliance certification.

## Status

Steps 1–4 of the order of work below are implemented on this branch. Step 5
(process pool or columnar batch, then the 1M-row harness) is not. Proof is
`apps/api/tests/test_process_topology.py` and the claim/reclaim cases in
`apps/api/tests/test_worker_fleet.py`. That is a unit fixture, not a cluster
soak and not a production deploy.

Railway does not need a new service. `deploy/railway/api.toml` stays one API
replica and keeps the cadence loop until an operator sets `PROCESS_ROLE`.
`deploy/railway/scheduler.toml` is there for the day cadence should leave the API.

What changed:

| Process | Command | What it does now |
| --- | --- | --- |
| API | `scripts/start-api.sh` | With `WORKER_FLEET=1`, `API_CLAIM_LOOP=0`, `SCHEDULE_LOOP=0` it enqueues and still re-enqueues orphans. It does not start the thread-pool executor or the cadence loop. Single-process installs that leave those variables unset keep today's local executor. |
| Scheduler | `python -m src.scheduler_main` | Cadence only. Helm Deployment `scheduler-deployment.yaml`, compose service `scheduler`. Replica count 1. Not on the HPA. |
| Batch worker | `python -m src.worker_main` with `WORKER_MODE=batch` | Claims every queued row except `workload=cdc`. |
| CDC worker | same module, `WORKER_MODE=cdc` | Claims only CDC. Fixed replicas. Not on the batch HPA. |
| Confirm acks | `ACK_BACKEND=mongo` | `pilot_acks` collection. Two processes, one claim wins, a replay returns the first result. |
| File bytes | `BLOB_ROOT` or S3 | `stage_bytes` writes the shared root first when `BLOB_ROOT` is set and returns `s3://local/…`. The worker hydrates into its own upload dir. Helm keeps `DATAFLOW_S3_BUCKET` and does not set `BLOB_ROOT`. |

`MULTI_REPLICA=1` refuses to serve (API) or exits (worker, scheduler) until the API has stopped executing, the scheduler is the only cadence, and acks are mongo. Mapping, preflight, quarantine, reconcile, and dest-exists-by-column-name stay inside the worker. There is still one transfer engine.

CDC remains at-least-once upsert. Redis is still not the job queue.

## What ran before this change

One Python image. Three intended roles. Only two of them are deployed, and
the API role still does the work of the other two.

| Role | What the chart says | What the process does |
| --- | --- | --- |
| Web | Separate deployment, static UI | Separate. This split is real. |
| API | 3 replicas, `DATAFLOW_WORKER_FLEET=0` | HTTP, MCP, mapping, preflight, **and** the pipeline scheduler, **and** a local `ThreadPoolExecutor` (default 8) that runs transfers. Every replica starts `run_schedule_loop()` from `apps/api/src/main.py`. |
| Worker | 2 replicas, `DATAFLOW_WORKER_FLEET=1`, `python -m src.worker_main` | Claims jobs from Mongo `transfer_job_queue`. The API, forced to fleet `0`, never enqueues there (`apps/api/src/transfer/background.py`). The workers wait on an empty queue while the API pods execute the transfers. |
| Scheduler | `values.yaml` and `values-production.yaml` set `scheduler.enabled` and a replica count | **No Deployment template.** `deploy/enterprise/helm/dataflow/templates/` has api, web, and worker only. |
| Worker mode `cdc,batch` | Set as `DATAFLOW_WORKER_MODE` | **No reader in the API.** The variable does not change which jobs a worker claims. |

`docker-compose.prod.yml` is closer to the intended shape: API plus a worker
command, shared volumes for `/data`. It still starts the pipeline scheduler
inside the API, and the API image is not told to stop executing transfers.

Shared state:

| State | Owner today | Safe across replicas |
| --- | --- | --- |
| Transfer jobs, checkpoints | MongoDB (`transfer_job_queue`, job documents) | Yes, when `MONGODB_URI` is a real server |
| Pipeline schedules | Mongo when the client connects, else `schedules.json` | Only on Mongo. File fallback is per pod. |
| Saved connectors | Mongo when detected, else `connectors.json`, secrets via `secret_vault` | Only on Mongo |
| Pilot confirm acks | `pilot_acks.json` on local disk (`ack_ledger.py`) | **No.** An ack staged on pod A cannot be confirmed on pod B. |
| Uploads, SQLite roots, spill | Local `DATAFLOW_UPLOAD_DIR` / `DATAFLOW_SQLITE_ROOT` | **No** in the Helm chart. The templates mount no volume and no object store. Compose shares a Docker volume; Kubernetes does not. |
| Redis | Named in Helm values and the Terraform comment | **Unused by the queue.** The claim queue is Mongo. Deploying Redis does not move a job. |

The schedule beat does take a Mongo lock (`schedule_locks` / `global_scheduler_lock`) when a real Mongo client is up, and it fails closed when multi-replica mode requires that lock and Mongo is down. That lock is a leader election living inside every API pod. It is not a scheduler service. A lock expiry of `SCHEDULER_LOCK_TTL` (default 300s) during a long beat can let a second pod start the same pass.

Transfer execution on the API path uses a thread pool. The 1M-row profile already showed the busy time inside per-cell Python. Threads do not add cores. Horizontal pod autoscaling of the API scales HTTP and accidentally scales in-process transfers, which is the wrong knob.

## Drawbacks, in the order they hurt a customer

1. **Control plane and data plane are the same process.** A large transfer, a CDC reader, or RAG warm-up on the API event loop's thread pool competes with login, mapping, and MCP. Readiness stays true after warm-up even while the pool is saturated. An API deploy or HPA scale-down kills in-flight transfers that were never handed to a worker.

2. **The worker fleet the chart deploys does not receive the work.** `DATAFLOW_WORKER_FLEET=0` on the API selects `scheduler_mode=local`. Jobs run in the API pod. The worker Deployment is idle. Scaling workers does not raise throughput. Scaling API replicas runs more local executors and more orphan-resume scans (`list_jobs` limit 200 on every API start).

3. **The scheduler the chart describes is not installed.** Every API replica runs the cadence loop. The Mongo lock reduces double-fires. It does not give the cadence its own lifecycle, its own budget, or a single place to look when pipelines sit on "Next run" with zero runs.

4. **Confirm, files, and SQLite are node-local.** MCP `confirm_action` and the Pilot confirm card read `pilot_acks.json`. Two API replicas make confirm a coin flip. A file upload accepted by pod A is invisible to a worker on pod B. The Helm chart has object-store values and no volume that carries bytes to the worker.

5. **File fallbacks are still legal in a multi-replica process.** Connector and schedule stores fall back to JSON files when Mongo is absent. Production must refuse to boot in that mode. A silent file fallback on one replica is a split brain, not a degraded mode.

6. **CDC and batch share one undifferentiated worker.** CDC holds a replication slot, a binlog position, or a change-stream cursor for the life of the source. Batch is burst CPU and memory. One HPA on "worker" will evict a capture to place a batch pod, or starve batch behind a quiet capture. `DATAFLOW_WORKER_MODE=cdc,batch` does not split them.

7. **Throughput does not scale with cores or with pods until the transform leaves the GIL.** More API replicas running a thread pool copy the same ceiling. This is a worker-internal change (process pool or a columnar batch), not a new network service.

8. **Redis, DocumentDB, and RDS are named before they have a job.** RDS in the chart is not the product's job store. DocumentDB can be the Mongo-compatible job store. Redis is not on the claim path. Standing them up without an owner creates a second control plane nobody reads.

## What to deploy separately

Same image, different command. Do not split mapping, preflight, and the writer into network services. `services.semantic_mapper.map_columns` and `services.shape_contract.classify_dest_exists_shape` stay in-process in the worker. A second mapper behind an RPC is how dest-exists starts writing by position.

| Service | Replicas | Command / duty | Must not do |
| --- | --- | --- | --- |
| Web | 2+ | Static UI, already separate | Call warehouses |
| Control-plane API | 2+, HPA on request CPU | Auth, connectors, schedules CRUD, mapping preview, preflight, MCP, confirm. On start, re-enqueue orphans (`run_transfer_async` upserts the queue row; it does not execute). | Run `run_schedule_loop`. Run `ThreadPoolExecutor` transfers. Run the API claim loop when workers are deployed. |
| Scheduler | 1 (a second replica may run only as lock standby) | The cadence loop only. Enqueue due pipelines onto `transfer_job_queue`. | Execute a transfer. Serve HTTP traffic. |
| Batch workers | HPA on queue depth, not API CPU | `src.worker_main` claiming batch jobs | Hold a CDC slot |
| CDC workers | Fixed small pool, one owner per source slot, **not** on the batch HPA | Claim only CDC jobs. Slot, LSN, and binlog stay with the lease. | Steal batch jobs to fill idle CPU |
| Mongo-compatible store | Managed (DocumentDB or MongoDB) | Jobs, queue, schedule lock, schedules, connectors, **acks** | — |
| Object store | S3-compatible | Uploads, spill, SQLite files, proof packs | — |

Do **not** deploy a separate MCP server. MCP is a door onto the control plane. A second process would grow a second confirm path and a second transfer engine.

Do **not** deploy Redis until a specific owner needs it (short-TTL rate limit or pub/sub). The queue owner is Mongo. Adding Redis beside it splits the lease story.

Do **not** move the product database to the chart's RDS just to match the Terraform comment. Customer warehouses stay customer warehouses. The control plane stays on the job store that already has the claim, the fence, and the schedule lock.

Postgres in `docker-compose.prod.yml` under the `full` profile is a sample warehouse, not a service this product requires to boot.

## State that has to move off the pod disk before the split is real

1. **Ack ledger → Mongo** (same database as jobs), with the existing one-shot consume and replay-of-result. Until that lands, API replicas must stay at 1 or confirm is wrong.
2. **Uploads and spill → object store**, keyed by workspace and file id. The worker hydrates from that key. A shared PVC is acceptable for a single-node compose install and is not acceptable for the EKS shape in `values-production.yaml`.
3. **Boot guard.** If `DATAFLOW_MULTI_REPLICA=1` or production: refuse file backends for connectors, schedules, and acks. Refuse `WORKER_FLEET=0` on the API when a worker Deployment is enabled.
4. **Chart must match the process.** Remove `scheduler` values until a Deployment exists, or add the Deployment and set API `WORKER_FLEET=0` **and** stop the in-process schedule loop and the local executor. `DATAFLOW_WORKER_MODE` is either read by the claim filter or deleted from the chart.

## Order of work

Each step is done when the named proof is green. Chat confidence is not the proof.

1. **Make the fleet honest.** Done for the enqueue path. `test_run_transfer_async_enqueues_and_does_not_execute` asserts the API thread pool is not called and the queue payload is `workload=cdc` for `cdc_incremental`. Helm API env is `WORKER_FLEET=1`, `API_CLAIM_LOOP=0`, `SCHEDULE_LOOP=0`, `PROCESS_ROLE=api`, `ACK_BACKEND=mongo`. A second worker not receiving the same job is the claim lease (`test_claim_next_job_acquires_lease_and_requeues_on_lease_fail`), not a second OS process in this fixture.
2. **Move the cadence loop out of the API.** Done as a process boundary. `schedule_loop_enabled()` is false on the honest API and true only on `src.scheduler_main`. `test_scheduler_without_a_loop_is_refused` fails closed when that process is told not to run. This fixture does not kill a live scheduler pod.
3. **Put acks and uploads where both roles can see them.** Done. `test_two_api_processes_share_one_ack` (first claim wins, replay is idempotent). `test_blob_root_is_readable_from_another_upload_dir` deletes the API upload path and reads the bytes from a different upload directory via `s3://local/`.
4. **Split CDC from batch.** Done. `test_batch_worker_skips_cdc_and_claims_batch`, `test_cdc_worker_claims_only_cdc`, `test_reclaim_leaves_a_held_cdc_lease`, `test_batch_reclaim_does_not_touch_cdc_rows`. A held CDC lease is not requeued. A batch worker does not reclaim a CDC row.
5. **Only then** raise worker parallelism with a process pool or a columnar batch, and re-run the existing 1M-row harness. Publish before and after on that fixture. Do not quote the new number as an SLA. Not this change.

Mapping, preflight, quarantine, reconcile, and dest-exists-by-column-name stay inside the worker that already calls them. The enterprise gap is which process is allowed to call them, and where the bytes and the acks live — not a new mapper.
