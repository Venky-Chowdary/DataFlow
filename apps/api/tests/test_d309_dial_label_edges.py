"""D309 AC-2/AC-3 and edge cases: the stall label names every dialled host, never a credential.

The PR's test covers one host on port 38403. This adds the story's other
criteria and edges: a multi-host replica-set URI lists each host (no
crash on ``h1:p,h2:p``), an SRV URI names the cluster, an IPv6 literal is
bracketed (``2001:db8::1:38403`` read as a different address, the round-4
cosmetic), and a password, even percent-encoded, never appears.
"""

from __future__ import annotations

import pytest

from src.ai.copilot.tool_deadline import describe_target


@pytest.mark.parametrize(
    ("uri", "label"),
    [
        ("mongodb://reader:secret@a.example:38403,b.example:38404,c.example/app?replicaSet=rs0",
         "a.example:38403,b.example:38404,c.example:27017"),
        ("mongodb+srv://reader:secret@cluster.example.net/app", "cluster.example.net"),
        ("mongodb://reader:secret@[2001:db8::1]:38403/app?directConnection=true", "[2001:db8::1]:38403"),
        ("mongodb://reader:p%40ss-secret@host.example:38403/app", "host.example:38403"),
    ],
)
def test_the_label_names_the_dialled_hosts_and_no_credential(uri, label):
    out = describe_target({"type": "mongodb", "host": "form.example", "port": 27017, "connection_string": uri})
    assert out.startswith(label), out
    assert "secret" not in out and "reader" not in out and "p@ss" not in out
