"""REL-3: a file export that hits the size limit leaves no truncated file.

Before the fix a CSV export under RLIMIT_FSIZE failed with a raw "[Errno 27]
File too large" and left export_<job>.csv cut mid-row at the download name.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

API = Path(__file__).resolve().parents[1]

_SCRIPT = textwrap.dedent(
    """
    import os, resource, signal, sys
    sys.path[:0] = [{api!r}, {src!r}]
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    from types import SimpleNamespace
    from services import file_export_append as fea
    from services.data_errors import DataPathError
    out = sys.argv[1]
    os.environ["DATAFLOW_EXPORTS_DIR"] = out
    fea.local_destination_refusal = None
    import services.local_file_policy as lfp
    lfp.local_destination_refusal = lambda *a, **k: ""
    import services.export_object_store as eos
    eos.exports_use_object_store = lambda: False
    eos.job_export_workspace = lambda job_id: ""
    import services.export_paths as ep
    ep.generated_export_path = lambda name, workspace_id="": os.path.join(out, name)
    dest = SimpleNamespace(format="csv", output_path={output!r})
    payload = b"id,v\\n" + b"".join(b"%d,%s\\n" % (i, b"x" * 90) for i in range(30000))
    if {prior!r}:
        open(os.path.join(out, {output!r}), "wb").write(b"id,v\\n1,old\\n")
    resource.setrlimit(resource.RLIMIT_FSIZE, (1_000_000, 1_000_000))
    try:
        fea.land_export_bytes(dest, payload, job_id="w2datjob", sync_mode={mode!r}, export_name="export.csv")
    except DataPathError as exc:
        print("CODE", exc.code)
    """
)


def _run(tmp_path, *, output: str, mode: str, prior: bool) -> str:
    script = _SCRIPT.format(api=str(API), src=str(API / "src"), output=output, mode=mode, prior=prior)
    done = subprocess.run([sys.executable, "-c", script, str(tmp_path)], capture_output=True, text=True,
                          timeout=120)
    return done.stdout + done.stderr


def test_generated_export_leaves_no_partial_file(tmp_path):
    out = _run(tmp_path, output="", mode="full_refresh_overwrite", prior=False)
    assert "CODE EXPORT_WRITE_FAILED" in out, out
    assert [p.name for p in tmp_path.iterdir()] == [], out


def test_named_overwrite_and_append_keep_the_previous_file(tmp_path):
    for mode in ("full_refresh_overwrite", "full_refresh_append"):
        out = _run(tmp_path, output="w2dat_named.csv", mode=mode, prior=True)
        assert "CODE EXPORT_WRITE_FAILED" in out, (mode, out)
        assert (tmp_path / "w2dat_named.csv").read_bytes() == b"id,v\n1,old\n", mode
        assert sorted(p.name for p in tmp_path.iterdir()) == ["w2dat_named.csv"], mode
