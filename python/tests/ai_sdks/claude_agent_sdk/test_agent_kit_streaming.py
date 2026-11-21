from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from claude_agent_sdk import Transport
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import ActivityEnvironment

from temporal_agent_kit.agent_protocol import (
    ContextCompactionDelta,
    ModelInteractionEnded,
    ModelInteractionStarted,
    ReplyDelta,
    TextAnnotationDelta,
    ThoughtSummaryDelta,
    TokenUsage,
    ToolInputDelta,
)
from temporal_agent_kit.agent_workflow import AgentWorkflowRunner
from temporal_agent_kit.ai_sdks.claude_agent_sdk import TemporalAgentKitOptions
from temporal_agent_kit.ai_sdks.claude_agent_sdk._activities import ConnectionActivities
from temporal_agent_kit.ai_sdks.claude_agent_sdk._types import (
    ConnectActivityInput,
    Connector,
    ReadActivityInput,
    WriteActivityInput,
)
from temporal_agent_kit.stream_context import TurnStreamContext

CONTEXT = TurnStreamContext(agent_id="agent", turn_id="turn", turn_number=1, message_id="message")


def _stream_event(event: dict[str, Any]) -> dict[str, Any]:
    return {"type": "stream_event", "event": event}


def _delta(delta: dict[str, Any], index: int = 0) -> dict[str, Any]:
    return _stream_event({"type": "content_block_delta", "index": index, "delta": delta})


def _published(*messages: dict[str, Any]) -> list[Any]:
    """The Agent Kit events published for Claude Code's messages."""
    activities = ConnectionActivities(AsyncMock(spec=Connector), "test")
    publisher = Mock()
    for message in messages:
        activities._publish_stream_item(message, publisher)
    return [call.args[0] for call in publisher.publish.call_args_list]


# Mapping Claude Code's stream events to Agent Kit events


@pytest.mark.parametrize(
    ("delta", "index", "expected"),
    [
        ({"type": "text_delta", "text": "Hi"}, 0, ReplyDelta(text="Hi")),
        ({"type": "input_json_delta", "partial_json": '{"a":'}, 1, ToolInputDelta(block_index=1, partial_json='{"a":')),
        (
            {"type": "thinking_delta", "thinking": "Hmm"},
            0,
            ThoughtSummaryDelta(delta={"type": "thinking_delta", "text": "Hmm"}),
        ),
        (
            {"type": "signature_delta", "signature": "sig"},
            0,
            ThoughtSummaryDelta(delta={"type": "signature_delta", "signature": "sig"}),
        ),
        (
            {"type": "citations_delta", "citation": {"cited_text": "Hi"}},
            0,
            TextAnnotationDelta(delta={"annotations": [{"cited_text": "Hi"}]}),
        ),
        (
            {"type": "compaction_delta", "content": "Earlier.", "encrypted_content": "opaque"},
            2,
            ContextCompactionDelta(block_index=2, content="Earlier.", encrypted_content="opaque"),
        ),
        (
            {"type": "compaction_delta", "content": 1},
            2,
            ContextCompactionDelta(block_index=2, content=None, encrypted_content=None),
        ),
    ],
)
def test_content_deltas_publish_agent_events(delta: dict[str, Any], index: int, expected: Any) -> None:
    assert _published(_delta(delta, index)) == [expected]


@pytest.mark.parametrize(
    "message",
    [
        {"type": "assistant"},
        {"type": "stream_event", "event": "not a dict"},
        _stream_event({"type": "content_block_start"}),
        _stream_event({"type": "content_block_delta", "index": 0}),
        _delta({"type": "unknown_delta"}),
        _delta({"type": "text_delta", "text": 1}),
        _delta({"type": "input_json_delta", "partial_json": '{"a":'}, index=None),  # type: ignore[arg-type]
    ],
)
def test_other_messages_publish_nothing(message: dict[str, Any]) -> None:
    assert _published(message) == []


def test_model_call_reports_its_model_and_usage_when_it_stops() -> None:
    start = {
        "model": "claude-test",
        "usage": {"input_tokens": 10, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 5, "output_tokens": 1},
    }

    assert _published(
        _stream_event({"type": "message_start", "message": start}),
        _stream_event({"type": "message_delta", "usage": {"output_tokens": 42, "server_tool_use": None}}),
        _stream_event({"type": "message_stop"}),
    ) == [
        ModelInteractionStarted(),
        ModelInteractionEnded(
            model="claude-test",
            usage=TokenUsage(input_tokens=115, output_tokens=42, cached_tokens=100, total_tokens=157),
        ),
    ]


def test_model_call_without_usage_reports_none() -> None:
    assert _published(
        _stream_event({"type": "message_start", "message": {}}),
        _stream_event({"type": "message_stop"}),
    ) == [ModelInteractionStarted(), ModelInteractionEnded(model=None, usage=None)]


def test_each_model_call_starts_with_fresh_usage() -> None:
    first_usage = {"input_tokens": 10, "output_tokens": 5}
    events = _published(
        _stream_event({"type": "message_start", "message": {"model": "a", "usage": first_usage}}),
        _stream_event({"type": "message_stop"}),
        _stream_event({"type": "message_start", "message": {"model": "b"}}),
        _stream_event({"type": "message_stop"}),
    )

    assert events[-1] == ModelInteractionEnded(model="b", usage=None)


# Where the read Activity publishes


@pytest.fixture
async def connection() -> tuple[ActivityEnvironment, ConnectionActivities]:
    output = [_delta({"type": "text_delta", "text": "Hi"}), {"type": "assistant"}]

    async def read_messages():
        for message in output:
            yield message

    transport = AsyncMock(spec=Transport)
    transport.read_messages = read_messages
    connector = AsyncMock(spec=Connector)
    connector.connect.return_value = transport
    activities = ConnectionActivities(connector, "test")
    env = ActivityEnvironment()
    await env.run(activities.get_available_task_queue)
    await env.run(activities.connect, ConnectActivityInput(args=[], env={}, cli_path=None))
    return env, activities


async def test_read_publishes_to_the_active_turn(
    monkeypatch: pytest.MonkeyPatch, connection: tuple[ActivityEnvironment, ConnectionActivities]
) -> None:
    env, activities = connection
    publisher = Mock()
    published_for: list[TurnStreamContext] = []

    @asynccontextmanager
    async def publisher_from_activity(context: TurnStreamContext):
        published_for.append(context)
        yield publisher

    monkeypatch.setattr(AgentWorkflowRunner, "publisher_from_activity", publisher_from_activity)
    topic_client = Mock()
    monkeypatch.setattr(WorkflowStreamClient, "from_within_activity", topic_client)

    await env.run(activities.write, WriteActivityInput(text='{"type": "user"}\n', stream_context=CONTEXT))
    result = await env.run(activities.read, ReadActivityInput(streaming_topic="claude-events"))

    assert len(result.output) == 2
    assert published_for == [CONTEXT]
    assert [call.args[0] for call in publisher.publish.call_args_list] == [ReplyDelta(text="Hi")]
    topic_client.assert_not_called()
    await env.run(activities.close)


async def test_read_without_a_turn_publishes_stream_events_to_the_topic(
    monkeypatch: pytest.MonkeyPatch, connection: tuple[ActivityEnvironment, ConnectionActivities]
) -> None:
    env, activities = connection
    client = Mock()

    @asynccontextmanager
    async def from_within_activity():
        yield client

    monkeypatch.setattr(WorkflowStreamClient, "from_within_activity", from_within_activity)

    result = await env.run(activities.read, ReadActivityInput(streaming_topic="claude-events"))

    client.topic.assert_called_once_with("claude-events")
    assert [call.args[0] for call in client.topic.return_value.publish.call_args_list] == [result.output[0]]
    await env.run(activities.close)


def test_thinking_defaults_to_a_summarized_display() -> None:
    assert TemporalAgentKitOptions().thinking == {"type": "adaptive", "display": "summarized"}
    assert TemporalAgentKitOptions(thinking={"type": "disabled"}).thinking == {"type": "disabled"}
    assert TemporalAgentKitOptions(max_thinking_tokens=1000).thinking is None
