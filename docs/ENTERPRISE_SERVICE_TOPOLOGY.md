# Enterprise service topology

Principal-architect reading of how this product actually runs, which drawbacks
block a multi-replica deployment, and which processes should be separate.
Capability evidence stays in `docs/ENTERPRISE_ARCHITECTURE_AUDIT.md`. This
document is the process and state plan. It does not claim a soak, an SLA, or
a compliance certification.

## What runs today

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
| Control-plane API | 2+, HPA on request CPU | Auth, connectors, schedules CRUD, mapping preview, preflight, MCP, confirm | Run `run_schedule_loop`. Run `ThreadPoolExecutor` transfers. Start RAG training. Scan and resume orphans. |
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

1. **Make the fleet honest.** API enqueues and does not execute when workers are deployed. One integration test: two processes, one queue, the API thread pool does not run the job, one worker does, a second worker does not. Helm API env matches that mode.
2. **Move the cadence loop out of the API.** One scheduler process. Two API replicas with the loop disabled do not start a pipeline. The Mongo lock remains for the standby. Proof: kill the scheduler, no new run starts; restart it, the due pipeline enqueues once.
3. **Put acks and uploads where both roles can see them.** Confirm an ack staged on process A from process B. A file staged on the API is read by the worker. No shared local disk in that test.
4. **Split CDC from batch** by claim filter, with a test that a batch HPA scale-down does not drop a held CDC lease.
5. **Only then** raise worker parallelism with a process pool or a columnar batch, and re-run the existing 1M-row harness. Publish before and after on that fixture. Do not quote the new number as an SLA.

Mapping, preflight, quarantine, reconcile, and dest-exists-by-column-name stay inside the worker that already calls them. The enterprise gap is which process is allowed to call them, and where the bytes and the acks live — not a new mapper.
