"""
DataTransfer.space — Copilot Agent

Thin façade: customer chat delegates to DataPilotAgent.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CopilotMessage:
    role: str
    content: str


@dataclass
class CopilotResponse:
    answer: str
    intent: str
    confidence: float
    method: str
    reasoning: str = ""
    suggested_actions: list[dict] = field(default_factory=list)
    # Mutations / run-now style actions — UI must Confirm before applying.
    pending_actions: list[dict] = field(default_factory=list)
    needs_clarification: str = ""
    suggested_prompts: list[str] = field(default_factory=list)
    sources: list[dict] = field(default_factory=list)
    data_insight: dict | None = None
    tools_used: list[dict] = field(default_factory=list)


class DataTransferCopilotAgent:
    """Talks about your actual data in natural language."""

    def chat(
        self,
        message: str,
        history: list[dict] | None = None,
        data_context: dict | None = None,
    ) -> CopilotResponse:
        """Delegate to Data Pilot — Anthropic/Cursor-style agent with tools."""
        from .pilot_agent import get_pilot_agent
        return get_pilot_agent().chat(message, history, data_context)

    def get_suggested_prompts(self) -> list[str]:
        from .pilot_agent import get_pilot_agent
        return get_pilot_agent().get_suggested_prompts()


_agent: DataTransferCopilotAgent | None = None


def get_copilot_agent() -> DataTransferCopilotAgent:
    global _agent
    if _agent is None:
        _agent = DataTransferCopilotAgent()
    return _agent
