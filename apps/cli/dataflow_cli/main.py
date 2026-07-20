"""DataFlow GitOps CLI — plan / apply / export / validate.

Usage (from repo root, with apps/api on PYTHONPATH)::

    PYTHONPATH=apps/api:apps/cli python -m dataflow_cli plan -f dataflow.yaml --local
    PYTHONPATH=apps/api:apps/cli python -m dataflow_cli apply -f dataflow.yaml --local --yes
    PYTHONPATH=apps/api:apps/cli python -m dataflow_cli export --local -o dataflow.yaml

Remote (CI against a running API)::

    python -m dataflow_cli plan -f dataflow.yaml --api https://api.example/api/v1
    python -m dataflow_cli apply -f dataflow.yaml --api https://api.example/api/v1 --token "$DF_TOKEN" --yes

Honesty: applying schedules/contracts does not change CDC at-least-once delivery.
Imported contracts are DRAFT until signed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


def _ensure_api_path() -> None:
    """Allow ``--local`` to import ``services.*`` from apps/api."""
    here = Path(__file__).resolve()
    api_root = here.parents[2] / "api"  # apps/cli/dataflow_cli -> apps/api
    if api_root.is_dir():
        p = str(api_root)
        if p not in sys.path:
            sys.path.insert(0, p)


def _load_file(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".json"}:
        data = json.loads(text)
    else:
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise SystemExit("PyYAML required for YAML manifests (pip install pyyaml)") from exc
        data = yaml.safe_load(text)
    if not isinstance(data, (dict, list)):
        raise SystemExit(f"{path}: manifest must be a mapping or list")
    if isinstance(data, list):
        return {"apiVersion": "dataflow.space/v1", "kind": "DataFlowManifest", "resources": data}
    return data


def _print_plan(plan: dict[str, Any]) -> None:
    print(
        f"Plan: {plan.get('creates', 0)} create · "
        f"{plan.get('updates', 0)} update · "
        f"{plan.get('skips', 0)} skip "
        f"({plan.get('resource_count', 0)} resources)"
    )
    for action in plan.get("actions") or []:
        kind = action.get("kind") or "?"
        act = action.get("action") or "?"
        name = action.get("name") or action.get("id") or ""
        reason = action.get("reason")
        line = f"  - {act:6} {kind} {name}".rstrip()
        if reason:
            line += f" ({reason})"
        print(line)


def _print_apply(result: dict[str, Any]) -> None:
    print(
        f"Apply: {result.get('applied', 0)} ok · "
        f"{result.get('failed', 0)} failed "
        f"({result.get('resource_count', 0)} resources)"
    )
    for row in result.get("results") or []:
        status = "ok" if row.get("ok") else "FAIL"
        kind = row.get("kind") or "?"
        act = row.get("action") or "?"
        name = row.get("name") or row.get("id") or ""
        err = row.get("error") or row.get("reason") or ""
        line = f"  - [{status}] {act:6} {kind} {name}".rstrip()
        if err:
            line += f" — {err}"
        print(line)


def _http_json(
    method: str,
    url: str,
    *,
    token: str = "",
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("httpx required for --api mode (pip install httpx)") from exc

    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with httpx.Client(timeout=60.0) as client:
        res = client.request(method, url, headers=headers, json=body)
        if res.status_code >= 400:
            raise SystemExit(f"HTTP {res.status_code}: {res.text[:500]}")
        if res.headers.get("content-type", "").startswith("application/json"):
            return res.json()
        return {"raw": res.text}


def _http_bytes(url: str, *, token: str = "") -> bytes:
    import httpx

    headers = {"Accept": "application/x-yaml, application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with httpx.Client(timeout=60.0) as client:
        res = client.get(url, headers=headers)
        if res.status_code >= 400:
            raise SystemExit(f"HTTP {res.status_code}: {res.text[:500]}")
        return res.content


def cmd_validate(path: Path) -> int:
    data = _load_file(path)
    kind = str(data.get("kind") or "")
    if kind == "DataFlowManifest":
        resources = data.get("resources") or []
        if not isinstance(resources, list) or not resources:
            print("validate: FAIL — DataFlowManifest has no resources", file=sys.stderr)
            return 1
        bad = [r for r in resources if not isinstance(r, dict) or not r.get("kind")]
        if bad:
            print(f"validate: FAIL — {len(bad)} resource(s) missing kind", file=sys.stderr)
            return 1
        print(f"validate: ok — {len(resources)} resource(s)")
        return 0
    if kind in {"PipelineSchedule", "DataContract"}:
        print(f"validate: ok — single {kind}")
        return 0
    print(f"validate: FAIL — unsupported kind {kind!r}", file=sys.stderr)
    return 1


def cmd_plan(path: Path, *, api: str, token: str, local: bool) -> int:
    data = _load_file(path)
    if local:
        _ensure_api_path()
        from services.gitops_manifest import plan_manifest

        plan = plan_manifest(data)
    else:
        base = api.rstrip("/")
        plan = _http_json("POST", f"{base}/schedules/gitops/plan", token=token, body=data)
    _print_plan(plan)
    return 0


def cmd_apply(
    path: Path,
    *,
    api: str,
    token: str,
    local: bool,
    yes: bool,
    require_signed_contracts: bool = False,
) -> int:
    data = _load_file(path)
    if local:
        _ensure_api_path()
        from services.gitops_manifest import apply_manifest, plan_manifest

        plan = plan_manifest(data)
        _print_plan(plan)
        if not yes:
            print("Re-run with --yes to apply.", file=sys.stderr)
            return 2
        result = apply_manifest(
            data,
            dry_run=False,
            require_signed_contracts=require_signed_contracts,
        )
    else:
        base = api.rstrip("/")
        plan = _http_json("POST", f"{base}/schedules/gitops/plan", token=token, body=data)
        _print_plan(plan)
        if not yes:
            print("Re-run with --yes to apply.", file=sys.stderr)
            return 2
        qs = "require_signed_contracts=true" if require_signed_contracts else ""
        url = f"{base}/schedules/gitops/apply"
        if qs:
            url = f"{url}?{qs}"
        result = _http_json(
            "POST",
            url,
            token=token,
            body=data,
        )
    _print_apply(result)
    failed = int(result.get("failed") or 0)
    return 1 if failed else 0


def cmd_export(*, api: str, token: str, local: bool, output: Path) -> int:
    if local:
        _ensure_api_path()
        import yaml
        from services.gitops_manifest import build_dataflow_manifest

        artifact = build_dataflow_manifest()
        text = yaml.safe_dump(artifact, sort_keys=False, default_flow_style=False)
        output.write_text(text, encoding="utf-8")
    else:
        base = api.rstrip("/")
        raw = _http_bytes(f"{base}/schedules/export/dataflow?format=yaml", token=token)
        output.write_bytes(raw)
    print(f"Wrote {output}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dataflow",
        description="DataFlow GitOps CLI (plan / apply / export / validate)",
    )

    sub = p.add_subparsers(dest="command", required=True)

    def add_conn_flags(sp: argparse.ArgumentParser) -> None:
        sp.add_argument(
            "--api",
            default=os.getenv("DATAFLOW_API_BASE", "http://127.0.0.1:8001/api/v1"),
            help="API base including /api/v1 (default: DATAFLOW_API_BASE or localhost)",
        )
        sp.add_argument(
            "--token",
            default=os.getenv("DATAFLOW_API_TOKEN", ""),
            help="Bearer token (or DATAFLOW_API_TOKEN)",
        )
        sp.add_argument(
            "--local",
            action="store_true",
            help="Use in-process stores (apps/api services) instead of HTTP",
        )

    v = sub.add_parser("validate", help="Check manifest shape without contacting the API")
    v.add_argument("-f", "--file", type=Path, required=True)

    pl = sub.add_parser("plan", help="Dry-run create/update/skip against live state")
    pl.add_argument("-f", "--file", type=Path, required=True)
    add_conn_flags(pl)

    ap = sub.add_parser("apply", help="Apply manifest (contracts land as DRAFT)")
    ap.add_argument("-f", "--file", type=Path, required=True)
    ap.add_argument("-y", "--yes", action="store_true", help="Apply without interactive confirm")
    ap.add_argument(
        "--require-signed-contracts",
        action="store_true",
        help="CD/staging: every schedule must reference a SIGNED contract",
    )
    add_conn_flags(ap)

    ex = sub.add_parser("export", help="Export fleet dataflow.yaml")
    ex.add_argument("-o", "--output", type=Path, default=Path("dataflow.yaml"))
    add_conn_flags(ex)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            return cmd_validate(args.file)
        if args.command == "plan":
            return cmd_plan(args.file, api=args.api, token=args.token, local=args.local)
        if args.command == "apply":
            return cmd_apply(
                args.file,
                api=args.api,
                token=args.token,
                local=args.local,
                yes=args.yes,
                require_signed_contracts=bool(
                    getattr(args, "require_signed_contracts", False)
                ),
            )
        if args.command == "export":
            return cmd_export(
                api=args.api,
                token=args.token,
                local=args.local,
                output=args.output,
            )
    except SystemExit as exc:
        code = exc.code
        return int(code) if isinstance(code, int) else 1
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
