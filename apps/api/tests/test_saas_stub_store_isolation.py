"""CI-1 isolation regression — each SaaS stub server owns its store.

Pre-fix: ``start_saas_stub()`` reset the module-global ``STORE`` while an
already-running singleton server kept serving it. Under pytest-randomly
ordering, ``desktop_lab_untested._run_saas_stub`` started a second stub
mid-suite and wiped the rows the universal-matrix SaaS legs then read —
~52 ``rest_api``/``stripe``/``salesforce`` legs came back ``success=True,
records_transferred=0`` on faithful data.
"""

from __future__ import annotations

import json
import urllib.request


def test_second_stub_does_not_wipe_first_server_store():
    from tests.saas_desktop_stub import seed_tabular_fixture, start_saas_stub

    server1, url1 = start_saas_stub()
    try:
        seed_tabular_fixture(server1)
        body = urllib.request.urlopen(f"{url1}/v1/customers", timeout=5).read()
        assert len(json.loads(body)["data"]) == 2

        # A second stub starting mid-suite must not touch server1's rows.
        server2, url2 = start_saas_stub()
        try:
            body = urllib.request.urlopen(f"{url1}/v1/customers", timeout=5).read()
            assert len(json.loads(body)["data"]) == 2
            # The new server starts empty, not holding a stale shared store.
            body = urllib.request.urlopen(f"{url2}/v1/customers", timeout=5).read()
            assert json.loads(body)["data"] == []
        finally:
            server2.shutdown()
    finally:
        server1.shutdown()


def test_seed_tabular_fixture_defaults_to_module_store():
    from tests.saas_desktop_stub import STORE, seed_tabular_fixture

    seed_tabular_fixture()
    assert STORE.rows["customers"][0]["id"] == "1"
