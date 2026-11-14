"""A test's swapped Pilot dataset analyst ends with that test.

Route tests point Pilot at their own upload folder by swapping the process's
dataset analyst (``data_analyst._analyst``) for one bound to that folder. The
Pilot tool executor (``tools._tools``) and the Pilot agent
(``pilot_agent._pilot``) are process singletons that take the analyst once,
when they are built. A route test that built them during its swap left them
holding its analyst after monkeypatch restored the real one:
test_mxd04_upload_decimal_exact_carrier (an MCP transfer through
``mcp_client``) ran before test_pilot_truth_datasets in CI shard 5 of PR 412,
and every Pilot answer in that file listed the earlier test's four sample
uploads while the analyst the file had configured saw none ("You have 0
uploaded datasets in this workspace"). The other way round, a swap made after
the singletons were built never reached them.
"""

from __future__ import annotations

import pytest

from tests import r4_route_harness as h


@pytest.fixture
def pilot(monkeypatch):
    """The Pilot modules, with whatever this test builds dropped at its end."""
    import src.ai.copilot.pilot_agent as pilot_agent
    import src.ai.copilot.tools as tools
    from src.ai.copilot.data_analyst import get_data_analyst

    monkeypatch.setattr(tools, "_tools", None)
    monkeypatch.setattr(pilot_agent, "_pilot", None)
    return tools, pilot_agent, get_data_analyst


def test_pilot_built_during_a_swap_does_not_keep_the_swapped_analyst(pilot, tmp_path):
    tools, pilot_agent, get_data_analyst = pilot
    process_analyst = get_data_analyst()

    with pytest.MonkeyPatch.context() as earlier_test:
        h.use_upload_dir(earlier_test, tmp_path)
        swapped = get_data_analyst()
        assert swapped is not process_analyst
        assert tools.get_pilot_tools().analyst is swapped
        assert pilot_agent.get_pilot_agent().analyst is swapped

    assert get_data_analyst() is process_analyst
    assert tools.get_pilot_tools().analyst is process_analyst
    assert pilot_agent.get_pilot_agent().analyst is process_analyst
    assert pilot_agent.get_pilot_agent().tools is tools.get_pilot_tools()


def test_a_swap_reaches_a_pilot_built_before_it(pilot, monkeypatch, tmp_path):
    tools, pilot_agent, get_data_analyst = pilot
    monkeypatch.setattr(tools, "_tools", tools.DataPilotTools())
    monkeypatch.setattr(pilot_agent, "_pilot", pilot_agent.DataPilotAgent())

    with pytest.MonkeyPatch.context() as later_test:
        h.use_upload_dir(later_test, tmp_path)
        assert tools.get_pilot_tools().analyst is get_data_analyst()
        assert pilot_agent.get_pilot_agent().analyst is get_data_analyst()
        assert [str(tmp_path)] == get_data_analyst().feeder.upload_dirs
