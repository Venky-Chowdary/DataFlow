"""C03/X-SFTP1 — SFTP overwrite must never destroy the destination file.

On servers without ``posix-rename@openssh.com`` the writer fell back to
``remove(final)`` + ``rename(temp, final)``: the destination was gone the
moment the upload finished staging, and a failed second rename left
nothing at the path at all. The fallback is now a rename pair — park the
final at a trash name, rename the staged file in, restore the parked file
if the second rename fails, and only then delete the trash.
"""

from __future__ import annotations

import pytest

from connectors.sftp_writer import _replace_remote


class _NoPosixRename:
    """SFTP stub that refuses posix_rename and records the fallback steps."""

    def __init__(self, *, final_exists: bool = True, fail_second_rename: bool = False):
        self.calls: list[tuple[str, str, str]] = []
        self.final_exists = final_exists
        self.fail_second_rename = fail_second_rename

    def posix_rename(self, a: str, b: str) -> None:
        import errno

        raise OSError(errno.EOPNOTSUPP, "Operation unsupported")

    def rename(self, a: str, b: str) -> None:
        self.calls.append(("rename", a, b))
        if b.endswith(".trash") and not self.final_exists:
            raise OSError(2, "No such file")
        if a.endswith(".tmp") and self.fail_second_rename:
            raise OSError(13, "Permission denied")

    def remove(self, path: str) -> None:
        self.calls.append(("remove", path, ""))


def test_fallback_parks_then_swaps_then_drops_trash():
    sftp = _NoPosixRename()
    _replace_remote(sftp, "/dst/orders.csv.dataflow-aaa.tmp", "/dst/orders.csv")
    ops = sftp.calls
    assert ops[0][0] == "rename" and ops[0][1] == "/dst/orders.csv"
    assert ops[0][2].endswith(".trash")
    assert ops[1] == ("rename", "/dst/orders.csv.dataflow-aaa.tmp", "/dst/orders.csv")
    assert ops[2][0] == "remove" and ops[2][1].endswith(".trash")
    # The final path is never deleted while a consumer could read it.


def test_failed_second_rename_restores_the_original():
    sftp = _NoPosixRename(fail_second_rename=True)
    with pytest.raises(OSError):
        _replace_remote(sftp, "/dst/orders.csv.dataflow-aaa.tmp", "/dst/orders.csv")
    ops = sftp.calls
    # Park, failed rename, restore — the original file is put back.
    assert ops[0][0] == "rename" and ops[0][1] == "/dst/orders.csv"
    assert ops[1] == ("rename", "/dst/orders.csv.dataflow-aaa.tmp", "/dst/orders.csv")
    assert ops[2][0] == "rename" and ops[2][2] == "/dst/orders.csv"
    assert ops[2][1].endswith(".trash")


def test_no_existing_final_skips_park_and_delete():
    sftp = _NoPosixRename(final_exists=False)
    _replace_remote(sftp, "/dst/new.csv.dataflow-bbb.tmp", "/dst/new.csv")
    ops = sftp.calls
    # Park attempt is made and refused (ENOENT), then the temp lands.
    assert ops[-1] == ("rename", "/dst/new.csv.dataflow-bbb.tmp", "/dst/new.csv",)
    assert all(op[0] != "remove" for op in ops)


def test_posix_rename_success_takes_no_fallback():
    class _Posix(_NoPosixRename):
        def posix_rename(self, a: str, b: str) -> None:
            self.calls.append(("posix_rename", a, b))

    sftp = _Posix()
    _replace_remote(sftp, "/dst/x.tmp", "/dst/x")
    assert sftp.calls == [("posix_rename", "/dst/x.tmp", "/dst/x")]
