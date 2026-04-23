import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from run_agent import AIAgent


class _FakeMemoryStore:
    def __init__(self, memory_block: str = "", user_block: str = ""):
        self.memory_block = memory_block
        self.user_block = user_block

    def format_for_system_prompt(self, target: str) -> str:
        if target == "memory":
            return self.memory_block
        if target == "user":
            return self.user_block
        return ""


class _FakeMemoryManager:
    def __init__(self, *, external_block: str = "", prefetch_result: str = ""):
        self.external_block = external_block
        self.prefetch_result = prefetch_result
        self.providers = [SimpleNamespace(name="builtin"), SimpleNamespace(name="byterover")]
        self.turn_starts = []
        self.prefetch_calls = []
        self.sync_calls = []
        self.queue_calls = []
        self.tool_calls = []

    def build_system_prompt(self) -> str:
        return self.external_block

    def on_turn_start(self, turn_number: int, message: str, **kwargs) -> None:
        self.turn_starts.append((turn_number, message, kwargs))

    def prefetch_all(self, query: str, *, session_id: str = "") -> str:
        self.prefetch_calls.append((query, session_id))
        return self.prefetch_result

    def sync_all(self, user_content: str, assistant_content: str, *, session_id: str = "") -> None:
        self.sync_calls.append((user_content, assistant_content, session_id))

    def queue_prefetch_all(self, query: str, *, session_id: str = "") -> None:
        self.queue_calls.append((query, session_id))

    def has_tool(self, tool_name: str) -> bool:
        return tool_name == "brv_query"

    def handle_tool_call(self, tool_name: str, args: dict, **kwargs) -> str:
        self.tool_calls.append((tool_name, args, kwargs))
        return json.dumps({"handled": tool_name, "args": args})

    def on_memory_write(self, action: str, target: str, content: str) -> None:
        return None


@pytest.fixture()
def agent_with_memory_hooks():
    tool_defs = [
        {
            "type": "function",
            "function": {
                "name": name,
                "description": f"{name} tool",
                "parameters": {"type": "object", "properties": {}},
            },
        }
        for name in ("web_search", "memory", "session_search")
    ]
    with (
        patch("run_agent.get_tool_definitions", return_value=tool_defs),
        patch("run_agent.check_toolset_requirements", return_value={}),
        patch("run_agent.OpenAI"),
    ):
        agent = AIAgent(
            api_key="test-key-1234567890",
            base_url="https://openrouter.ai/api/v1",
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
        )
    agent.client = MagicMock()
    agent._cached_system_prompt = "You are helpful."
    agent._use_prompt_caching = False
    agent.tool_delay = 0
    agent.compression_enabled = False
    agent.save_trajectories = False
    return agent


def _mock_response(content: str = "Done", finish_reason: str = "stop"):
    assistant_message = SimpleNamespace(
        content=content,
        tool_calls=None,
        reasoning=None,
        reasoning_content=None,
        reasoning_details=None,
    )
    choice = SimpleNamespace(message=assistant_message, finish_reason=finish_reason)
    return SimpleNamespace(choices=[choice], model="test/model", usage=None)


def test_build_system_prompt_emits_memory_inject_hook(agent_with_memory_hooks):
    agent = agent_with_memory_hooks
    agent._memory_store = _FakeMemoryStore("Memory block", "User block")
    agent._memory_enabled = True
    agent._user_profile_enabled = True
    agent._memory_manager = _FakeMemoryManager(external_block="External block")

    hook_calls = []

    def _record_hook(name, **kwargs):
        hook_calls.append((name, kwargs))
        return []

    with patch("hermes_cli.plugins.invoke_hook", side_effect=_record_hook):
        prompt = agent._build_system_prompt()

    assert "Memory block" in prompt
    assert "User block" in prompt
    assert "External block" in prompt

    inject_calls = [kwargs for name, kwargs in hook_calls if name == "on_memory_inject"]
    assert len(inject_calls) == 1
    inject = inject_calls[0]
    assert inject["session_id"] == agent.session_id
    assert inject["memory_chars"] == len("Memory block")
    assert inject["user_chars"] == len("User block")
    assert inject["external_chars"] == len("External block")
    assert inject["total_chars"] == len("Memory block") + len("User block") + len("External block")


def test_run_conversation_starts_session_before_memory_inject(agent_with_memory_hooks):
    agent = agent_with_memory_hooks
    agent._cached_system_prompt = None
    agent._memory_store = _FakeMemoryStore("Memory block", "User block")
    agent._memory_enabled = True
    agent._user_profile_enabled = True
    agent._memory_manager = _FakeMemoryManager(external_block="External block")
    agent.client.chat.completions.create.return_value = _mock_response(content="Final answer")

    hook_calls = []

    def _record_hook(name, **kwargs):
        hook_calls.append(name)
        return []

    with (
        patch("hermes_cli.plugins.invoke_hook", side_effect=_record_hook),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        result = agent.run_conversation("hello memory")

    assert result["final_response"] == "Final answer"
    assert "on_session_start" in hook_calls
    assert "on_memory_inject" in hook_calls
    assert hook_calls.index("on_session_start") < hook_calls.index("on_memory_inject")


def test_run_conversation_emits_parent_session_id_in_hooks(agent_with_memory_hooks):
    agent = agent_with_memory_hooks
    agent._cached_system_prompt = None
    agent._parent_session_id = "parent-session-1"
    agent._memory_store = _FakeMemoryStore("Memory block", "User block")
    agent._memory_enabled = True
    agent._user_profile_enabled = True
    agent._memory_manager = _FakeMemoryManager(external_block="External block")
    agent.client.chat.completions.create.return_value = _mock_response(content="Final answer")

    hook_calls = []

    def _record_hook(name, **kwargs):
        hook_calls.append((name, kwargs))
        return []

    with (
        patch("hermes_cli.plugins.invoke_hook", side_effect=_record_hook),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        result = agent.run_conversation("hello delegation")

    assert result["final_response"] == "Final answer"
    relevant = {
        name: kwargs
        for name, kwargs in hook_calls
        if name in {"on_session_start", "pre_llm_call", "pre_api_request", "post_api_request"}
    }
    assert relevant["on_session_start"]["parent_session_id"] == "parent-session-1"
    assert relevant["pre_llm_call"]["parent_session_id"] == "parent-session-1"
    assert relevant["pre_api_request"]["parent_session_id"] == "parent-session-1"
    assert relevant["post_api_request"]["parent_session_id"] == "parent-session-1"


def test_run_conversation_emits_memory_recall_and_sync_hooks(agent_with_memory_hooks):
    agent = agent_with_memory_hooks
    agent._memory_store = _FakeMemoryStore()
    agent._memory_enabled = False
    agent._user_profile_enabled = False
    agent._memory_manager = _FakeMemoryManager(prefetch_result="Remembered context")
    agent.client.chat.completions.create.return_value = _mock_response(content="Final answer")

    hook_calls = []

    def _record_hook(name, **kwargs):
        hook_calls.append((name, kwargs))
        return []

    with (
        patch("hermes_cli.plugins.invoke_hook", side_effect=_record_hook),
        patch.object(agent, "_persist_session"),
        patch.object(agent, "_save_trajectory"),
        patch.object(agent, "_cleanup_task_resources"),
    ):
        result = agent.run_conversation("hello memory")

    assert result["final_response"] == "Final answer"

    recall_calls = [kwargs for name, kwargs in hook_calls if name == "on_memory_recall"]
    sync_calls = [kwargs for name, kwargs in hook_calls if name == "on_memory_sync"]

    assert len(recall_calls) == 1
    assert recall_calls[0]["session_id"] == agent.session_id
    assert recall_calls[0]["query"] == "hello memory"
    assert recall_calls[0]["result_chars"] == len("Remembered context")
    assert recall_calls[0]["has_result"] is True

    assert len(sync_calls) == 1
    assert sync_calls[0]["session_id"] == agent.session_id
    assert sync_calls[0]["user_chars"] == len("hello memory")
    assert sync_calls[0]["assistant_chars"] == len("Final answer")


def test_invoke_tool_memory_provider_path_records_precheck_and_post_hook(agent_with_memory_hooks):
    agent = agent_with_memory_hooks
    agent._memory_manager = _FakeMemoryManager()

    post_calls = []
    precheck = MagicMock(return_value=None)

    def _record_hook(name, **kwargs):
        post_calls.append((name, kwargs))
        return []

    with (
        patch("hermes_cli.plugins.get_pre_tool_call_block_message", precheck),
        patch("hermes_cli.plugins.invoke_hook", side_effect=_record_hook),
    ):
        result = agent._invoke_tool("brv_query", {"query": "opik"}, "task-1", tool_call_id="tool-1")

    assert json.loads(result) == {"handled": "brv_query", "args": {"query": "opik"}}
    precheck.assert_called_once_with(
        "brv_query",
        {"query": "opik"},
        task_id="task-1",
        session_id=agent.session_id,
        tool_call_id="tool-1",
        parent_session_id="",
    )
    post = [kwargs for name, kwargs in post_calls if name == "post_tool_call"]
    assert len(post) == 1
    assert post[0]["session_id"] == agent.session_id
    assert post[0]["tool_call_id"] == "tool-1"
    assert post[0]["tool_name"] == "brv_query"


def test_execute_tool_calls_sequential_session_search_records_precheck_and_post_hook(agent_with_memory_hooks):
    agent = agent_with_memory_hooks
    agent._session_db = MagicMock()

    precheck = MagicMock(return_value=None)
    hook_calls = []
    tool_call = SimpleNamespace(
        id="tool-2",
        type="function",
        function=SimpleNamespace(name="session_search", arguments='{"query": "last time"}'),
    )
    assistant_message = SimpleNamespace(content="", tool_calls=[tool_call])
    messages = []

    def _record_hook(name, **kwargs):
        hook_calls.append((name, kwargs))
        return []

    with (
        patch("hermes_cli.plugins.get_pre_tool_call_block_message", precheck),
        patch("hermes_cli.plugins.invoke_hook", side_effect=_record_hook),
        patch("tools.session_search_tool.session_search", return_value='{"success": true, "results": []}'),
    ):
        agent._execute_tool_calls_sequential(assistant_message, messages, "task-2")

    precheck.assert_called_once_with(
        "session_search",
        {"query": "last time"},
        task_id="task-2",
        session_id=agent.session_id,
        tool_call_id="tool-2",
        parent_session_id="",
    )
    post = [kwargs for name, kwargs in hook_calls if name == "post_tool_call"]
    assert len(post) == 1
    assert post[0]["session_id"] == agent.session_id
    assert post[0]["tool_call_id"] == "tool-2"
    assert post[0]["tool_name"] == "session_search"
    assert len(messages) == 1
    assert messages[0]["tool_call_id"] == "tool-2"
