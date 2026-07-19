"""Re-export Singer tap bridge for ``from connectors.sdk.singer_bridge import …``."""

from connectors.sdk import SingerTapBridge, test_singer_tap

__all__ = ["SingerTapBridge", "test_singer_tap"]
