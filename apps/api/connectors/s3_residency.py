"""Data residency at the S3 write itself (security review round 2, H3).

Residency judges the region boto3 signs for, but an S3 bucket's own region
decides where the bytes land, and botocore's ``S3RegionRedirectorv2`` (on
every S3 client) re-sends a request to the bucket's region after S3 answers
with a redirect. For a client that dials AWS in a workspace with a data
region, :func:`install_bucket_region_guard`:

- checks each bucket's real region before the first write to it
  (``GetBucketLocation``, else ``HeadBucket``'s ``x-amz-bucket-region``) and
  refuses a bucket outside the fenced region, or one whose region cannot be
  read;
- refuses a write that S3 answers with a redirect to another region instead
  of letting the redirector re-send it (the bucket was re-created elsewhere
  between the check and the write).

A custom endpoint (an S3-compatible store) has no AWS bucket region: its
platform-verified region stamp speaks for it (``services.region_verification``).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

#: Operations that put bytes into a bucket.
WRITE_OPERATIONS = frozenset({
    "PutObject", "CreateMultipartUpload", "UploadPart", "UploadPartCopy",
    "CompleteMultipartUpload", "CopyObject",
})


class S3ResidencyError(RuntimeError):
    """An S3 write that would land outside the workspace's data region."""


#: The fence when no workspace can be named while some workspace is fenced.
_UNBOUND = "(workspace cannot be determined)"
#: The fence when the workspaces named for one write are fenced to different regions.
_CONFLICT = "(conflicting workspace regions)"


def _writing_workspace() -> str:
    """The workspace of the run or request making this write (``""`` = none bound)."""
    from services.egress_guard import current_egress_workspace
    from services.export_object_store import bound_job_workspace

    return current_egress_workspace() or bound_job_workspace()


def _writing_workspaces(named: tuple[str, ...]) -> tuple[str, ...]:
    """Every workspace this write is made for: the ones named when the client was
    built (the caller's, the saved connector's) and the one bound now."""
    return tuple(dict.fromkeys(w for w in (*named, _writing_workspace()) if w))


def _fence_for(workspaces: tuple[str, ...]) -> str:
    from services.data_residency import any_fence, enforced_region

    if not workspaces:
        # Review round 3 (H4): an unbound write is not an unfenced one.
        return _UNBOUND if any_fence() else ""
    fences = {enforced_region(w) for w in workspaces} - {""}
    if len(fences) > 1:
        return _CONFLICT
    return fences.pop() if fences else ""


def _fence_region(cache: dict[tuple[str, ...], str], named: tuple[str, ...] = ()) -> str:
    """The data region enforced for this write (``""`` = none), read once per client.

    A workspace named in ``cfg`` can add a fence but never remove the bound one.
    """
    workspaces = _writing_workspaces(named)
    if workspaces not in cache:
        cache[workspaces] = _fence_for(workspaces)
    return cache[workspaces]


def _location_region(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "us-east-1"  # GetBucketLocation's answer for us-east-1
    return "eu-west-1" if text == "eu" else text


def _header_region(response: Any) -> str:
    headers = ((response or {}).get("ResponseMetadata") or {}).get("HTTPHeaders") or {}
    return str(headers.get("x-amz-bucket-region") or "").strip().lower()


def bucket_region(client: Any, bucket: str) -> str:
    """The bucket's region, or ``""`` when neither S3 call can read it."""
    from botocore.exceptions import BotoCoreError, ClientError

    try:
        return _location_region(client.get_bucket_location(Bucket=bucket).get("LocationConstraint"))
    except (ClientError, BotoCoreError):
        pass
    try:
        return _header_region(client.head_bucket(Bucket=bucket))
    except ClientError as exc:
        return _header_region(getattr(exc, "response", None))
    except BotoCoreError:
        return ""


def _refusal(bucket: str, found: str, fence: str) -> S3ResidencyError:
    if fence in (_UNBOUND, _CONFLICT):
        why = (
            "the workspace making it cannot be determined while a data region is enforced "
            "on this deployment" if fence == _UNBOUND
            else "the workspaces it is made for are fenced to different regions"
        )
        return S3ResidencyError(
            f"Data residency violation: S3 write to bucket {bucket!r} refused because {why}; "
            "nothing was written"
        )
    where = f"is in {found}" if found else "has a region that cannot be read"
    return S3ResidencyError(
        f"Data residency violation: S3 bucket {bucket!r} {where}, outside this "
        f"workspace's data region {fence}; nothing was written"
    )


def _dials_aws(cfg: dict[str, Any]) -> bool:
    from connectors.aws_common import resolve_endpoint_url

    url = resolve_endpoint_url(cfg)
    host = (urlparse(url).hostname or "").lower() if url else ""
    return not url or host.endswith(".amazonaws.com")


def install_bucket_region_guard(client: Any, cfg: dict[str, Any], *, workspace_id: str = "") -> Any:
    """Arm ``client`` (an S3 client) with the bucket-region check; returns it.

    ``workspace_id`` is the workspace the client writes for; the resolved
    connector's own ``cfg["workspace_id"]`` and the job or egress workspace
    bound at write time are judged too (review round 3, H4).
    """
    if not _dials_aws(cfg):
        return client
    named = (str(workspace_id or "").strip(), str(cfg.get("workspace_id") or "").strip())
    checked: dict[str, str] = {}
    fences: dict[tuple[str, ...], str] = {}

    def before_write(params: dict[str, Any] | None = None, **_kwargs: Any) -> None:
        fence = _fence_region(fences, named)
        if not fence:
            return
        bucket = str((params or {}).get("Bucket") or "")
        if fence in (_UNBOUND, _CONFLICT):
            raise _refusal(bucket, "", fence)
        if bucket not in checked:
            checked[bucket] = bucket_region(client, bucket)
        if checked[bucket] != fence:
            raise _refusal(bucket, checked[bucket], fence)

    def redirected_write(response: Any = None, operation: Any = None, **_kwargs: Any) -> None:
        if getattr(operation, "name", "") not in WRITE_OPERATIONS or not response:
            return
        http, parsed = response
        moved = str((getattr(http, "headers", None) or {}).get("x-amz-bucket-region") or "").lower()
        status = int(getattr(http, "status_code", 0) or 0)
        fence = _fence_region(fences, named) if moved and status in (301, 307, 400) else ""
        if fence and moved != fence:
            bucket = str(((parsed or {}).get("Error") or {}).get("BucketName") or "")
            raise _refusal(bucket or "(this bucket)", moved, fence)

    events = client.meta.events
    for name in WRITE_OPERATIONS:
        events.register(f"before-parameter-build.s3.{name}", before_write)
    events.register_first("needs-retry.s3", redirected_write)
    return client


__all__ = ["S3ResidencyError", "WRITE_OPERATIONS", "bucket_region", "install_bucket_region_guard"]
