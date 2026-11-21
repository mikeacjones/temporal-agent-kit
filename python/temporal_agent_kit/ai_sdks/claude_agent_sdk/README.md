# Claude Agent SDK Temporal Integration


Install with `uv add 'temporal-agent-kit[claude-agent-sdk]'`. Import `ClaudeAgentSDKPlugin` and
`TemporalTransport` from `temporal_agent_kit.ai_sdks.claude_agent_sdk` when following the
[upstream usage guide](https://docs.temporal.io/develop/python/integrations/claude-agent-sdk).

In addition to the upstream guide, use `TemporalAgentKitOptions` in place of
`ClaudeAgentOptions`, pass the same options to `ClaudeSDKClient` and `TemporalTransport`, and
pass your runner to `TemporalTransport`. Claude's replies, thinking, and model calls then stream
to the Agent Kit turn:

```python
options = TemporalAgentKitOptions()
transport = TemporalTransport(options, runner=self._runner)
client = ClaudeSDKClient(options=options, transport=transport)
```

See the [Claude Agent SDK Hello example](../../../examples/claude_agent_sdk_hello).

To run Activities on the selected Worker, such as downloading files from S3
before Claude Code starts, register them on the plugin:

```python
worker = Worker(
    client,
    task_queue="my-agent",
    workflows=[MyAgentWorkflow],
    plugins=[ClaudeAgentSDKPlugin(hook_activities=[stage_files]), AgentKitPlugin()],
)
```

Then call them from a transport hook, which receives the Worker's task queue:

```python
async def stage(task_queue: str) -> None:
    await workflow.execute_activity(
        stage_files,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(minutes=5),
    )

transport = TemporalTransport(options, hooks={"before_connect": stage})
```
