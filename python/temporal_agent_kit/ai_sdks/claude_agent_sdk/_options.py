"""Claude Agent SDK options for Temporal Agent Kit streaming."""

from dataclasses import dataclass

from claude_agent_sdk import ClaudeAgentOptions


@dataclass(kw_only=True)
class TemporalAgentKitOptions(ClaudeAgentOptions):
    """Claude Agent SDK options that stream Claude's replies and thinking to Agent Kit."""

    include_partial_messages: bool = True

    def __post_init__(self) -> None:
        # Without a display, Claude Code streams thinking with no text to show.
        # thinking takes precedence over max_thinking_tokens, so keep either one.
        if self.thinking is None and self.max_thinking_tokens is None:
            self.thinking = {"type": "adaptive", "display": "summarized"}
