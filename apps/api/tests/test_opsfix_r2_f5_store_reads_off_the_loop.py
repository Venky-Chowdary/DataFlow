"""Ops review round 2, High 5: control-plane reads must not run on the event loop.

The API serves every request on one event loop. Tenant, BYOK, notification,
ACL, preflight-run and audit-anchor reads are synchronous database calls (up
to the 1 s control-plane bound each, plus a retry budget that sleeps), and
several ``async`` routes and the auth middleware's MFA gate made them on the
loop, so one slow read stalled ``/health`` and every other request. Those
routes now run on the thread pool (plain ``def`` handlers) or hand the store
call to it.
"""

from __future__ import annotations

import ast
import os
import asyncio
import time
from pathlib import Path

import httpx
import pytest

API = Path(__file__).resolve().parents[1]
STORE_MODULES = {
    "tenant_store", "resource_acl", "byok_key_manager", "notification_store", "shape_draft_store",
    "preflight_run_store", "cdc_mapping_review", "mcp_invocation_log", "audit_anchor",
    "session_policy", "sso_enforcement", "ip_allowlist_policy", "mfa_enforcement",
    # Transfer plans: file I/O plus a cross-process lock wait of up to 30 s (PS-1).
    "transfer_plan_store",
}
#: Stores whose helpers are followed through any depth of sync calls.
TRANSITIVE_STORE_MODULES = {"transfer_plan_store"}
OFFLOAD = {"to_thread", "run_in_threadpool", "run_in_executor", "run_sync"}
#: Startup work, before the API serves anything.
ALLOWED = {("main", "lifespan")}
BLOCK_SECONDS = 0.4
#: The stall a warm, unloaded loop is allowed while nine store reads wait.
STALL_FLOOR = BLOCK_SECONDS / 2
#: A loop that ran one store read on itself stalls for BLOCK_SECONDS (less one
#: 10 ms tick), so no limit at or above this can let that regression through.
STALL_CEILING = BLOCK_SECONDS * 3 / 4


def _stall_limit(baseline: float) -> float:
    """STALL_FLOOR, widened for a slow runner by its own unblocked jitter.

    CI (api-tests 2/6, PR 412) failed this case with "the event loop stalled
    0.21s" against a 0.20 s bar: the late tick was the first /health request
    building the app's lazily materialised route table on the loop (the work
    ``services.route_table_warmup`` does at startup, which an ASGI transport
    without lifespan skips), about 85 ms here and over 200 ms on a loaded
    runner. The limit scales with the same burst measured with the store not
    blocking (4x its worst tick) but never reaches STALL_CEILING, so a store
    read on the loop still fails at any runner speed.
    """
    return min(STALL_CEILING, max(STALL_FLOOR, 4 * baseline))


def _tree(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return None


def _imports(tree: ast.Module) -> dict[str, tuple[str, str | None]]:
    out: dict[str, tuple[str, str | None]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                out[alias.asname or alias.name] = (node.module.split(".")[-1], alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                out[alias.asname or alias.name.split(".")[-1]] = (alias.name.split(".")[-1], None)
    return out


def _target(call: ast.Call, imports: dict[str, tuple[str, str | None]]) -> tuple[str, str] | None:
    func = call.func
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        module = (imports.get(func.value.id) or (func.value.id, None))[0]
        return module, func.attr
    if isinstance(func, ast.Name) and func.id in imports:
        module, name = imports[func.id]
        return module, name or func.id
    return None


def _store_reads_on_the_loop() -> list[str]:
    """Async defs that call a store function directly, or a sync helper that does, without offloading it."""
    store_funcs = set()
    for module in STORE_MODULES:
        tree = _tree(API / "services" / f"{module}.py")
        for node in tree.body if tree else []:
            if isinstance(node, ast.FunctionDef):
                store_funcs.add((module, node.name))
    files = [p for p in list((API / "src").rglob("*.py")) + list((API / "services").rglob("*.py"))]
    trees = {p: _tree(p) for p in files}
    # Each sync function's call targets: imported (module, name) and same-file names.
    callees: dict[tuple[str, str], set[tuple[str, str]]] = {}
    imported: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for path, tree in trees.items():
        if tree is None:
            continue
        imports = _imports(tree)
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef):
                continue
            key = (path.stem, fn.name)
            targets, via_import = callees.setdefault(key, set()), imported.setdefault(key, set())
            for c in ast.walk(fn):
                if not isinstance(c, ast.Call):
                    continue
                target = _target(c, imports)
                if target:
                    targets.add(target)
                    via_import.add(target)
                if isinstance(c.func, ast.Name):
                    targets.add((path.stem, c.func.id))
    # Control-plane stores: a sync helper that calls one directly.
    helpers = {key for key, targets in imported.items() if targets & store_funcs}
    # The transfer-plan store, transitively: route -> assert_body_ids ->
    # plan_owner -> get_plan counts, to a fixed point. (The other stores keep
    # the one-level rule: transitively they reach many pre-existing sites.)
    reaching = {f for f in store_funcs if f[0] in TRANSITIVE_STORE_MODULES}
    while True:
        found = {key for key, targets in callees.items() if key not in reaching and targets & reaching}
        if not found:
            break
        reaching |= found
    helpers |= reaching
    hits = []
    for path, tree in trees.items():
        if tree is None:
            continue
        imports = _imports(tree)
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.AsyncFunctionDef) or (path.stem, fn.name) in ALLOWED:
                continue
            offloaded = {
                id(node)
                for call in ast.walk(fn)
                if isinstance(call, ast.Call) and getattr(call.func, "attr", getattr(call.func, "id", "")) in OFFLOAD
                for node in ast.walk(call)
            }
            nested = {id(n) for d in ast.walk(fn) if isinstance(d, ast.FunctionDef) for n in ast.walk(d)}
            for call in ast.walk(fn):
                if not isinstance(call, ast.Call) or id(call) in offloaded or id(call) in nested:
                    continue
                target = _target(call, imports)
                local = (path.stem, call.func.id) if isinstance(call.func, ast.Name) else None
                if target in store_funcs or target in helpers or local in helpers:
                    hits.append(f"{path.relative_to(API)}:{call.lineno} async {fn.name}() -> {ast.unparse(call.func)}")
    return sorted(set(hits))


def test_no_async_route_or_middleware_reads_a_control_plane_store_on_the_loop():
    assert _store_reads_on_the_loop() == []


@pytest.fixture
def blocking_stores(monkeypatch):
    """Every control-plane read blocks its thread, like a slow database."""
    from services.shared_doc_store import SharedLog
    from services.shared_record_store import SharedRecordSet

    calls = {"n": 0, "blocking": False}
    real_load, real_backend = SharedRecordSet.load, SharedLog.backend

    def slow_load(self, *args, **kwargs):
        if calls["blocking"]:
            calls["n"] += 1
            time.sleep(BLOCK_SECONDS)
        return real_load(self, *args, **kwargs)

    def slow_backend(self, *args, **kwargs):
        if calls["blocking"]:
            calls["n"] += 1
            time.sleep(BLOCK_SECONDS)
        return real_backend(self, *args, **kwargs)

    monkeypatch.setattr(SharedRecordSet, "load", slow_load)
    monkeypatch.setattr(SharedLog, "backend", slow_backend)
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    return calls


ROUTES = [
    ("GET", "/api/v1/workspace/tenants", None),
    ("GET", "/api/v1/workspace/tenant", None),
    ("POST", "/api/v1/workspace/tenant", {"workspace_id": "ws-loop", "name": "Loop"}),
    ("GET", "/api/v1/workspace/tenant/byok-keys", None),
    ("GET", "/api/v1/workspace/notifications", None),
    ("GET", "/api/v1/workspace/security/posture", None),
    ("GET", "/api/v1/preflight/runs", None),
    ("GET", "/api/v1/preflight/runs/pf_missing", None),
    ("GET", "/api/v1/audit/tip", None),
]


def test_health_and_the_loop_stay_responsive_while_store_routes_wait_on_the_database(blocking_stores):
    from services.route_table_warmup import warm_route_table
    from src.main import app

    async def burst(client: httpx.AsyncClient) -> tuple[float, float, list[int]]:
        """Every store route at once, then /health: the worst tick and /health's latency."""
        lags: list[float] = []

        async def ticker() -> None:
            while True:
                started = time.perf_counter()
                await asyncio.sleep(0.01)
                lags.append(time.perf_counter() - started - 0.01)

        tick = asyncio.create_task(ticker())
        await asyncio.sleep(0.05)
        lags.clear()
        calls = [client.request(m, p, json=b) for m, p, b in ROUTES]
        routes = asyncio.gather(*calls)
        await asyncio.sleep(0.05)
        started = time.perf_counter()
        health = await client.get("/health")
        health_seconds = time.perf_counter() - started
        assert health.status_code == 200
        responses = await routes
        tick.cancel()
        return max(lags), health_seconds, [r.status_code for r in responses]

    async def scenario() -> tuple[float, float, float, list[int]]:
        # What the API's startup does before it serves anything (an ASGI
        # transport skips lifespan): the route table is built off the request path.
        await asyncio.to_thread(warm_route_table, app)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1", timeout=60) as client:
            # Warm up (first-request imports and schema builds are not the finding)...
            for method, path, body in ROUTES:
                await client.request(method, path, json=body)
            await client.get("/health")
            # ...take this runner's own jitter for the same burst, then block the store.
            baseline, _, _ = await burst(client)
            blocking_stores["blocking"] = True
            max_lag, health_seconds, statuses = await burst(client)
        return baseline, max_lag, health_seconds, statuses

    baseline, max_lag, health_seconds, statuses = asyncio.run(scenario())
    assert blocking_stores["n"] >= len(ROUTES), "every route reached the (blocking) store"
    assert all(code < 500 for code in statuses), statuses
    assert health_seconds < BLOCK_SECONDS, f"/health waited {health_seconds:.2f}s behind store reads"
    limit = _stall_limit(baseline)
    assert max_lag < limit, f"the event loop stalled {max_lag:.2f}s (limit {limit:.2f}s, unblocked {baseline:.3f}s)"


def test_the_stall_limit_still_fails_a_blocked_loop():
    """A warm runner keeps the 0.2 s bar; a slow one is judged against itself,
    and one store read on the loop (BLOCK_SECONDS) fails at any runner speed."""
    assert _stall_limit(0.005) == STALL_FLOOR
    assert _stall_limit(0.06) == pytest.approx(0.24)
    assert _stall_limit(5.0) == STALL_CEILING < BLOCK_SECONDS - 0.01


# ---------------------------------------------------------------------------
# Transfer plans (PS-1): file reads plus a cross-process lock wait of up to 30 s
# ---------------------------------------------------------------------------


@pytest.fixture
def plan_store(tmp_path, monkeypatch):
    """An isolated plan store holding one mapped plan; reads can be made slow."""
    from services import transfer_plan_store as plans

    monkeypatch.setattr(plans, "STORE_PATH", tmp_path / "transfer_plans.json")
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    plan = plans.create_plan({"name": "loop", "source_columns": ["id"], "target_columns": ["id"]})
    plans.sync_ui_mappings(plan.id, [{"source": "id", "target": "id", "confidence": 1.0}])
    state = {"plan_id": plan.id, "slow": False, "reads": 0}
    real_load = plans._load_all

    def slow_load():
        if state["slow"]:
            state["reads"] += 1
            time.sleep(BLOCK_SECONDS)
        return real_load()

    monkeypatch.setattr(plans, "_load_all", slow_load)
    return state


def _plan_routes(plan_id: str) -> list[tuple[str, str, dict | None]]:
    base = f"/api/v1/transfer/plans/{plan_id}"
    return [
        ("GET", "/api/v1/transfer/plans", None),
        ("POST", "/api/v1/transfer/plans", {"name": "loop-2"}),
        ("GET", base, None),
        ("PATCH", base, {"name": "renamed"}),
        ("POST", f"{base}/mappings", {"mappings": [{"source": "id", "target": "id", "confidence": 1.0}]}),
        ("POST", f"{base}/approve", None),
        ("GET", f"{base}/run-payload", None),
        ("GET", f"{base}/preflight", None),
        ("GET", f"{base}/reversibility", None),
        ("POST", "/api/v1/transfer/plans/00000000-0000-4000-8000-000000000000/map", {}),
    ]


async def _plan_burst(client: httpx.AsyncClient, routes, *, while_waiting=None):
    """Every plan route at once, then /health: the worst loop tick, /health's latency, statuses."""
    lags: list[float] = []

    async def ticker() -> None:
        while True:
            started = time.perf_counter()
            await asyncio.sleep(0.01)
            lags.append(time.perf_counter() - started - 0.01)

    tick = asyncio.create_task(ticker())
    await asyncio.sleep(0.05)
    lags.clear()
    pending = asyncio.gather(*[client.request(m, p, json=b) for m, p, b in routes])
    await asyncio.sleep(0.05)
    started = time.perf_counter()
    health = await client.get("/health")
    health_seconds = time.perf_counter() - started
    assert health.status_code == 200
    max_lag = max(lags)
    if while_waiting is not None:
        while_waiting()
    responses = await pending
    tick.cancel()
    return max_lag, health_seconds, [(r.request.url.path, r.status_code) for r in responses]


def _run_plan_scenario(plan_store, block):
    from services.route_table_warmup import warm_route_table
    from src.main import app

    routes = _plan_routes(plan_store["plan_id"])

    async def scenario():
        await asyncio.to_thread(warm_route_table, app)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1", timeout=60) as client:
            for method, path, body in routes:
                await client.request(method, path, json=body)
            await client.get("/health")
            baseline, _, _ = await _plan_burst(client, routes)
            release = block()
            return baseline, *(await _plan_burst(client, routes, while_waiting=release))

    return asyncio.run(scenario())


def test_health_answers_while_plan_routes_wait_on_a_slow_plan_store(plan_store):
    def block():
        plan_store["slow"] = True
        return None

    baseline, max_lag, health_seconds, statuses = _run_plan_scenario(plan_store, block)
    assert plan_store["reads"] >= len(_plan_routes("x")), "every plan route reached the (slow) store"
    assert all(code < 500 for _path, code in statuses), statuses
    assert health_seconds < BLOCK_SECONDS, f"/health waited {health_seconds:.2f}s behind plan-store reads"
    limit = _stall_limit(baseline)
    assert max_lag < limit, f"the event loop stalled {max_lag:.2f}s (limit {limit:.2f}s, unblocked {baseline:.3f}s)"


def test_health_answers_while_plan_writes_wait_for_the_plan_store_lock(plan_store, monkeypatch):
    """Another process holds the plan store: writers wait for it off the loop."""
    import fcntl

    from services import transfer_plan_store as plans

    monkeypatch.setattr(plans, "LOCK_TIMEOUT_SECONDS", 10.0)
    lock_path = plans.STORE_PATH.with_name(plans.STORE_PATH.name + ".lock")
    holder = open(lock_path, "a+")  # noqa: SIM115 - held across the burst, closed below

    def block():
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX)

        def release():
            fcntl.flock(holder.fileno(), fcntl.LOCK_UN)

        return release

    try:
        baseline, max_lag, health_seconds, statuses = _run_plan_scenario(plan_store, block)
    finally:
        holder.close()
    assert all(code < 500 for _path, code in statuses), statuses
    assert health_seconds < BLOCK_SECONDS, f"/health waited {health_seconds:.2f}s behind a plan write's lock wait"
    limit = _stall_limit(baseline)
    assert max_lag < limit, f"the event loop stalled {max_lag:.2f}s (limit {limit:.2f}s, unblocked {baseline:.3f}s)"


def test_plan_writes_waiting_on_the_lock_do_not_starve_reads_or_the_shared_thread_pool(plan_store, monkeypatch):
    """Writers parked on a held plan-store lock must not use up the default executor.

    The loop's default executor is pinned to 4 threads here; 8 PATCHes wait
    for a lock another process holds. GET /plans (no lock) and an unrelated
    asyncio.to_thread call must still answer at once.
    """
    import concurrent.futures
    import fcntl

    from services import transfer_plan_store as plans
    from services.route_table_warmup import warm_route_table
    from src.main import app

    monkeypatch.setattr(plans, "LOCK_TIMEOUT_SECONDS", 20.0)
    plan_id = plan_store["plan_id"]
    lock_path = plans.STORE_PATH.with_name(plans.STORE_PATH.name + ".lock")

    async def scenario():
        asyncio.get_running_loop().set_default_executor(
            concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="test-default")
        )
        await asyncio.to_thread(warm_route_table, app)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1", timeout=60) as client:
            await client.get("/api/v1/transfer/plans")
            fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)  # "another process" holds the plan store
                writes = asyncio.gather(*[
                    client.patch(f"/api/v1/transfer/plans/{plan_id}", json={"name": f"n{i}"})
                    for i in range(8)
                ])
                await asyncio.sleep(0.3)  # the writers are now parked on the lock
                started = time.perf_counter()
                listed = await client.get("/api/v1/transfer/plans")
                read_seconds = time.perf_counter() - started
                started = time.perf_counter()
                await asyncio.wait_for(asyncio.to_thread(lambda: None), timeout=10)
                shared_seconds = time.perf_counter() - started
                started = time.perf_counter()
                health = await client.get("/health")
                health_seconds = time.perf_counter() - started
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
            statuses = [r.status_code for r in await writes]
        return listed.status_code, read_seconds, shared_seconds, health.status_code, health_seconds, statuses

    listed, read_seconds, shared_seconds, health, health_seconds, statuses = asyncio.run(scenario())
    assert listed == 200 and health == 200
    assert read_seconds < 1.0, f"GET /plans waited {read_seconds:.2f}s behind parked plan writers"
    assert shared_seconds < 1.0, f"the shared thread pool was starved for {shared_seconds:.2f}s"
    assert health_seconds < 1.0
    assert statuses == [200] * 8, statuses


def test_map_and_validate_final_writes_wait_for_the_lock_on_the_plan_pool(plan_store, monkeypatch):
    """Map's and sync Validate's long work runs on the default executor; their final
    plan write waits for a held lock on the plan-write pool, not on a shared thread."""
    import concurrent.futures
    import fcntl
    from unittest.mock import patch

    from services import transfer_plan_store as plans
    from services.route_table_warmup import warm_route_table
    from src.main import app

    monkeypatch.setattr(plans, "LOCK_TIMEOUT_SECONDS", 20.0)
    plan = plans.create_plan(
        {
            "name": "pool",
            "source": {"kind": "file", "format": "csv"},
            "destination": {"kind": "database", "format": "postgresql", "table": "people"},
            "source_columns": ["id"],
            "source_schema": {"id": "INTEGER"},
            "target_columns": ["id"],
            "target_schema": {"id": "INTEGER"},
            "sample_rows": [{"id": 1}],
        }
    )
    plans.sync_ui_mappings(plan.id, [{"source": "id", "target": "id", "confidence": 1.0}])
    lock_path = plans.STORE_PATH.with_name(plans.STORE_PATH.name + ".lock")
    mapped = {"mappings": [{"source": "id", "target": "id", "confidence": 1.0}], "validation": {"passed": True, "issues": []}}
    engines = (
        lambda pf, *_a, **_k: pf,
        lambda mode: 0.85,
        lambda **_k: {"connected": True, "table_exists": True, "can_create_table": True,
                      "db_type": "postgresql", "column_types": {"id": "INTEGER"}, "message": "ok"},
        lambda **_k: {"passed": True, "passed_count": 1, "total_gates": 1, "gates": [], "blockers": []},
        lambda **_k: [],
    )

    async def scenario():
        asyncio.get_running_loop().set_default_executor(
            concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="test-default")
        )
        await asyncio.to_thread(warm_route_table, app)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1", timeout=60) as client:
            await client.get("/api/v1/transfer/plans")
            fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)  # "another process" holds the plan store
                base = f"/api/v1/transfer/plans/{plan.id}"
                writers = asyncio.gather(
                    *[client.post(f"{base}/map", json={"use_llm": False}) for _ in range(3)],
                    *[client.post(f"{base}/preflight", json={}) for _ in range(3)],
                )
                await asyncio.sleep(0.5)  # their compute is done; the final writes are parked
                started = time.perf_counter()
                listed = await client.get("/api/v1/transfer/plans")
                read_seconds = time.perf_counter() - started
                started = time.perf_counter()
                await asyncio.wait_for(asyncio.to_thread(lambda: None), timeout=10)
                shared_seconds = time.perf_counter() - started
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
            responses = await writers
        return listed.status_code, read_seconds, shared_seconds, [(r.request.url.path, r.status_code) for r in responses]

    with patch("services.mapping_pipeline.run_mapping_pipeline", return_value=mapped), patch(
        "services.transfer_plan_service._preflight", return_value=engines
    ), patch("src.transfer.adapters.read_source_database", side_effect=Exception("skip")):
        listed, read_seconds, shared_seconds, statuses = asyncio.run(scenario())
    assert listed == 200
    assert read_seconds < 1.0, f"GET /plans waited {read_seconds:.2f}s behind parked Map/Validate writes"
    assert shared_seconds < 1.0, f"the shared thread pool was starved for {shared_seconds:.2f}s"
    # Map and Validate ran against the same head, so only one Map may append;
    # the others answer 409 (the plan changed while they ran) — none fails.
    assert all(code in (200, 409) for _path, code in statuses), statuses
    assert sum(1 for path, code in statuses if path.endswith("/preflight") and code == 200) == 3
